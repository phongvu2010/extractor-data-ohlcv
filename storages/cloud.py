"""Module cung cấp cơ chế lưu trữ dữ liệu chứng khoán lên Google Cloud Storage (GCS) và Google Cloud BigQuery."""

from __future__ import annotations

from datetime import date, datetime
import gc
import io
import json
import logging
import re
from typing import Any
import uuid

from google.cloud import bigquery, storage
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import Config
from .base import BaseStorage


class CustomJSONEncoder(json.JSONEncoder):
    """Bộ mã hóa JSON tùy chỉnh để xử lý an toàn các kiểu dữ liệu NumPy và Pandas."""

    def default(self, obj: Any) -> Any:
        """Kiểm tra và ánh xạ kiểu dữ liệu tùy chỉnh về chuẩn JSON.

        Args:
            obj (Any): Đối tượng cần mã hóa.

        Returns:
            Any: Đối tượng đã chuẩn hóa tương thích với JSON.
        """
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if pd.isna(obj):
            return None
        return super().default(obj)


class CloudStorage(BaseStorage):
    """Chuyên trách việc lưu trữ dữ liệu an toàn ra Google Cloud Storage (GCS) và BigQuery (BQ)."""

    client: storage.Client
    bq_client: bigquery.Client
    bucket: storage.Bucket

    def __init__(self, logger: logging.Logger) -> None:
        """Khởi tạo bộ lưu trữ dữ liệu và kết nối đến GCS & BigQuery.

        Args:
            logger (logging.Logger): Đối tượng Logger dùng để ghi nhận tiến trình.

        Raises:
            Exception: Phát sinh khi không thể thiết lập kết nối đến dịch vụ GCP.
        """
        super().__init__(logger)
        try:
            # self.client = storage.Client.from_service_account_json(
            #     "../secrets/credentials.json"
            # )
            self.client = storage.Client()
            # self.bq_client = bigquery.Client.from_service_account_json(
            #     "../secrets/credentials.json"
            # )
            self.bq_client = bigquery.Client()
            self.bucket = self.client.bucket(Config.GCS_BUCKET_NAME)
            self.logger.info(
                "☁️ [GCS] Kết nối thành công bằng Application Default Credentials (ADC)."
            )
            self.logger.info(f"☁️ [GCS] Bucket: {Config.GCS_BUCKET_NAME}")
            self.logger.info(f"📊 [BigQuery] Dự án: {self.bq_client.project}")
        except Exception as e:
            self.logger.error(
                f"🛑 [Storage] Lỗi khởi tạo kết nối Cloud Services qua ADC: {e}"
            )
            raise e

        # Tự động quét và dọn dẹp các bảng staging rác của phiên trước
        self.cleanup_stale_staging_tables()

    def cleanup_stale_staging_tables(self) -> None:
        """Tìm và dọn dẹp các bảng staging BigQuery còn sót lại từ các lần chạy trước."""
        dataset_ref: bigquery.DatasetReference = self.bq_client.dataset(
            Config.BQ_DATASET
        )
        try:
            self.bq_client.get_dataset(dataset_ref)
        except Exception:
            return

        prefix: str = f"{Config.BQ_ADJ_TABLE}_staging_"
        self.logger.info(
            f"🧹 [BigQuery] Đang quét các bảng staging còn sót lại với tiền tố '{prefix}' bằng metadata query..."
        )

        try:
            query: str = f"""
                SELECT table_name
                FROM `{self.bq_client.project}.{Config.BQ_DATASET}.INFORMATION_SCHEMA.TABLES`
                WHERE table_name LIKE @prefix
            """
            query_config: bigquery.QueryJobConfig = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("prefix", "STRING", f"{prefix}%"),
                ]
            )
            query_job: bigquery.QueryJob = self.bq_client.query(
                query, job_config=query_config
            )
            rows: bigquery.table.RowIterator = query_job.result()

            stale_tables: list[str] = [row.table_name for row in rows]

            if not stale_tables:
                self.logger.info(
                    "✨ [BigQuery] Không phát hiện bảng staging nào còn sót lại."
                )
                return

            self.logger.warning(
                f"🧹 [BigQuery] Phát hiện {len(stale_tables)} bảng staging còn sót lại: {stale_tables}"
            )
            for table_id in stale_tables:
                table_ref: str = (
                    f"{self.bq_client.project}.{Config.BQ_DATASET}.{table_id}"
                )
                try:
                    self.bq_client.delete_table(table_ref, not_found_ok=True)
                    self.logger.info(f"🗑️ [BigQuery] Đã xóa bảng staging: {table_id}")
                except Exception as del_err:
                    self.logger.warning(
                        f"⚠️ [BigQuery] Không thể xóa bảng staging {table_id}: {del_err}"
                    )
        except Exception as e:
            self.logger.error(
                f"❌ [BigQuery] Lỗi trong quá trình quét dọn bảng staging: {e}"
            )

    def _df_to_parquet_bytes(self, df: pd.DataFrame, suffix: str) -> io.BytesIO:
        """Chuyển đổi DataFrame sang dữ liệu nén Parquet dạng BytesIO một cách tối ưu hiệu năng.

        Đối với dữ liệu 'adj', chuyển đổi cột giá sang kiểu decimal128 ở tầng PyArrow
        thay vì dùng apply(Decimal) ở tầng Pandas nhằm tối đa hóa hiệu năng.

        Args:
            df (pd.DataFrame): DataFrame dữ liệu chứng khoán cần chuyển đổi.
            suffix (str): Hậu tố để phân loại định dạng ('raw' hoặc 'adj').

        Returns:
            io.BytesIO: Đối tượng BytesIO chứa luồng dữ liệu Parquet đã được nén Snappy.
        """
        df_write: pd.DataFrame = df.copy()

        # Ép kiểu trading_date về Date thuần túy để khớp hoàn hảo với kiểu DATE của BigQuery
        if "trading_date" in df_write.columns:
            df_write["trading_date"] = pd.to_datetime(df_write["trading_date"]).dt.date

        price_cols: list[str] = ["open_price", "high_price", "low_price", "close_price"]
        if suffix == "raw":
            df_write[price_cols] = df_write[price_cols].round(0).astype("Int64")
        else:
            # Giữ kiểu float64 và làm tròn 2 chữ số thập phân ở Pandas trước khi cast trong Arrow
            df_write[price_cols] = df_write[price_cols].round(2).astype("float64")

        if "total_volume" in df_write.columns:
            df_write["total_volume"] = df_write["total_volume"].astype("Int64")

        for col in ["symbol", "exchange", "source"]:
            if col in df_write.columns:
                df_write[col] = df_write[col].astype(str)

        # Chuyển đổi sang PyArrow Table
        table: pa.Table = pa.Table.from_pandas(df_write, preserve_index=False)

        # Cast các cột giá sang decimal128(18, 2) ở tầng Arrow (C++ layer) cực nhanh
        if suffix != "raw":
            for col in price_cols:
                if col in table.column_names:
                    idx: int = table.column_names.index(col)
                    casted_col: pa.ChunkedArray = table.column(col).cast(
                        pa.decimal128(18, 2)
                    )
                    table = table.set_column(idx, col, casted_col)

        bio: io.BytesIO = io.BytesIO()
        pq.write_table(
            table,
            bio,
            compression="snappy",
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )
        bio.seek(0)
        return bio

    def save_parquet(
        self,
        df: pd.DataFrame,
        date_ref: datetime,
        suffix: str = "raw",
        partition: bool = False,
    ) -> str | None:
        """Ghi dữ liệu nén Parquet trực tiếp lên Google Cloud Storage (GCS).

        Args:
            df (pd.DataFrame): DataFrame dữ liệu chứng khoán cần lưu.
            date_ref (datetime): Mốc thời gian mặc định của tệp dữ liệu.
            suffix (str): Hậu tố định danh loại dữ liệu ('raw' hoặc 'adj').
            partition (bool): True để lưu phân mảnh theo năm/tháng, False để lưu file gộp tĩnh.

        Returns:
            Optional[str]: Đường dẫn GCS của file nếu lưu thành công, ngược lại là None.

        Raises:
            Exception: Phát sinh khi ghi dữ liệu lên GCS thất bại.
        """
        if df is None or df.empty:
            return None

        # Tối ưu: Lấy ngày thực tế lớn nhất từ cột trading_date để đặt tên thư mục/file nếu có
        if "trading_date" in df.columns:
            max_date: Any = pd.to_datetime(df["trading_date"]).max()
            if not pd.isna(max_date):
                date_ref = max_date

        gcs_path: str
        if partition:
            year_str: str = date_ref.strftime("%Y")
            month_str: str = date_ref.strftime("%m")
            date_str: str = date_ref.strftime("%Y%m%d")
            gcs_path = f"{Config.GCS_PARQUET_PREFIX}/{suffix}/year={year_str}/month={month_str}/daily_{date_str}.parquet"
        else:
            gcs_path = (
                f"{Config.GCS_PARQUET_PREFIX}/{suffix}/cafef_historical_all.parquet"
            )

        try:
            self.logger.info(
                f"💾 ☁️ [GCS] Đang ghi dữ liệu nén Parquet: gs://{Config.GCS_BUCKET_NAME}/{gcs_path}"
            )

            bio: io.BytesIO = self._df_to_parquet_bytes(df, suffix)

            blob: storage.Blob = self.bucket.blob(gcs_path)
            blob.upload_from_file(bio, content_type="application/octet-stream")

            self.logger.info(
                f"🎉 ☁️ [GCS] File lưu trữ thành công tại GCS: gs://{Config.GCS_BUCKET_NAME}/{gcs_path}"
            )
            return gcs_path
        except Exception as e:
            self.logger.error(
                f"❌ ☁️ [GCS] Lỗi trong quá trình ghi file Parquet lên GCS: {e}"
            )
            raise e

    def save_symbol_history(
        self, df: pd.DataFrame, symbol: str, suffix: str = "adj"
    ) -> None:
        """Ghi toàn bộ lịch sử giá của một mã cổ phiếu cụ thể ra file Parquet riêng biệt trên GCS.

        Args:
            df (pd.DataFrame): DataFrame dữ liệu lịch sử đầy đủ của mã cổ phiếu.
            symbol (str): Mã cổ phiếu cần lưu (ví dụ: FPT).
            suffix (str): Tiền tố thư mục lưu trữ ('raw' hoặc 'adj').

        Raises:
            Exception: Phát sinh khi ghi dữ liệu lên GCS thất bại.
        """
        if df is None or df.empty:
            return

        gcs_path: str = (
            f"{Config.GCS_PARQUET_PREFIX}/{suffix}/reloaded/{symbol.upper()}.parquet"
        )

        try:
            self.logger.info(
                f"💾 ☁️ [GCS] Đang ghi dữ liệu lịch sử cho mã {symbol.upper()} "
                f"tại: gs://{Config.GCS_BUCKET_NAME}/{gcs_path}"
            )

            bio: io.BytesIO = self._df_to_parquet_bytes(df, suffix)

            blob: storage.Blob = self.bucket.blob(gcs_path)
            blob.upload_from_file(bio, content_type="application/octet-stream")

            self.logger.info(
                f"🎉 ☁️ [GCS] File lịch sử mã {symbol.upper()} "
                f"lưu trữ thành công tại GCS: gs://{Config.GCS_BUCKET_NAME}/{gcs_path}"
            )
        except Exception as e:
            self.logger.error(
                f"❌ ☁️ [GCS] Lỗi khi ghi file lịch sử cho mã {symbol.upper()} lên GCS: {e}"
            )
            raise e

    def _ensure_table_exists(self, table_name: str) -> None:
        """Kiểm tra và tự động khởi tạo Dataset/Table BigQuery với phân vùng Month và Cluster nếu chưa tồn tại.

        Args:
            table_name (str): Tên bảng BigQuery cần kiểm tra/khởi tạo.
        """
        dataset_ref: bigquery.DatasetReference = self.bq_client.dataset(
            Config.BQ_DATASET
        )
        try:
            self.bq_client.get_dataset(dataset_ref)
        except Exception:
            self.logger.info(
                f"✨ [BigQuery] Đang khởi tạo dataset mới: {Config.BQ_DATASET}"
            )
            self.bq_client.create_dataset(bigquery.Dataset(dataset_ref))

        table_ref: str = f"{self.bq_client.project}.{Config.BQ_DATASET}.{table_name}"
        try:
            self.bq_client.get_table(table_ref)
        except Exception:
            self.logger.info(
                f"✨ [BigQuery] Đang tạo bảng mới {table_name} với Monthly Partitioning & Clustering..."
            )
            # Xác định kiểu dữ liệu giá là INTEGER cho bảng raw và NUMERIC cho các bảng khác (tránh lỗi lệch kiểu khi tạo mới)
            price_type: str = (
                "INTEGER" if table_name == Config.BQ_RAW_TABLE else "NUMERIC"
            )
            schema: list[bigquery.SchemaField] = [
                bigquery.SchemaField("symbol", "STRING"),
                bigquery.SchemaField("trading_date", "DATE"),
                bigquery.SchemaField("open_price", price_type),
                bigquery.SchemaField("high_price", price_type),
                bigquery.SchemaField("low_price", price_type),
                bigquery.SchemaField("close_price", price_type),
                bigquery.SchemaField("total_volume", "INTEGER"),
                bigquery.SchemaField("exchange", "STRING"),
                bigquery.SchemaField("source", "STRING"),
            ]
            table: bigquery.Table = bigquery.Table(table_ref, schema=schema)
            table.expires = None
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH, field="trading_date"
            )
            table.clustering_fields = ["symbol", "exchange"]
            self.bq_client.create_table(table)

    def sync_adjusted_symbol_to_bigquery(
        self, symbol: str, min_date: date | None = None
    ) -> None:
        """Đồng bộ lịch sử điều chỉnh của một mã chứng khoán duy nhất lên BigQuery qua Staging Table.

        Sử dụng cơ chế Staging Table để đảm bảo tính an toàn dữ liệu (nếu nạp file từ GCS lỗi
        sẽ không ảnh hưởng bảng chính) và tối ưu hóa DML Delete bằng bộ lọc phân vùng động.

        Args:
            symbol (str): Mã chứng khoán cần đồng bộ.
            min_date (Optional[date]): Ngày nhỏ nhất của dữ liệu điều chỉnh để tối ưu hóa phạm vi xóa.

        Raises:
            Exception: Phát sinh khi load job hoặc giao dịch SQL đồng bộ gặp lỗi.
        """
        symbol_upper: str = symbol.upper()
        gcs_uri: str = (
            f"gs://{Config.GCS_BUCKET_NAME}/"
            f"{Config.GCS_PARQUET_PREFIX}/adj/reloaded/{symbol_upper}.parquet"
        )
        self.logger.info(
            f"⚡ [BigQuery] Bắt đầu đồng bộ lịch sử điều chỉnh (Staging Mode) cho mã {symbol_upper}..."
        )

        target_table_ref: str = (
            f"{self.bq_client.project}.{Config.BQ_DATASET}.{Config.BQ_ADJ_TABLE}"
        )
        # Thay thế ký tự không hợp lệ cho tên bảng BigQuery (chỉ giữ lại chữ cái, số và dấu gạch dưới)
        clean_symbol: str = re.sub(r"[^a-zA-Z0-9_]", "_", symbol_upper)
        run_id: str = uuid.uuid4().hex[:8]
        staging_table_name: str = (
            f"{Config.BQ_ADJ_TABLE}_staging_{clean_symbol}_{run_id}"
        )
        staging_table_ref: str = (
            f"{self.bq_client.project}.{Config.BQ_DATASET}.{staging_table_name}"
        )

        # Đảm bảo bảng đích đã sẵn sàng hoạt động
        self._ensure_table_exists(Config.BQ_ADJ_TABLE)

        # 1. Nạp dữ liệu từ GCS vào bảng Staging (Tạm thời)
        job_config: bigquery.LoadJobConfig = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE",  # Đảm bảo ghi đè bảng tạm cũ nếu tồn tại
        )

        try:
            self.logger.info(
                f"⚡ [BigQuery] Bước 1: Nạp file Parquet từ GCS vào bảng Staging: {staging_table_name}..."
            )
            load_job: bigquery.LoadJob = self.bq_client.load_table_from_uri(
                gcs_uri, staging_table_ref, job_config=job_config
            )
            load_job.result()
            self.logger.info(
                f"🎉 [BigQuery] Nạp thành công vào bảng tạm {staging_table_name}. "
                f"Đã nạp {load_job.output_rows} dòng."
            )

            # 1.5. Xác định ngày giao dịch nhỏ nhất để làm mốc cắt tỉa phân vùng tĩnh
            if min_date is None:
                self.logger.info(
                    f"⚡ [BigQuery] Đang lấy ngày giao dịch nhỏ nhất từ bảng tạm {staging_table_name}..."
                )
                min_date_query: str = (
                    f"SELECT MIN(trading_date) as min_date FROM `{staging_table_ref}`"
                )
                min_date_job: bigquery.QueryJob = self.bq_client.query(min_date_query)
                min_date_result: bigquery.table.RowIterator = min_date_job.result()
                min_date_rows: list[bigquery.Row] = list(min_date_result)
                min_date = (
                    min_date_rows[0].min_date
                    if min_date_rows and min_date_rows[0].min_date
                    else datetime.now(Config.VN_TZ).date()
                )
                self.logger.info(
                    f"📅 [BigQuery] Ngày giao dịch nhỏ nhất phát hiện từ staging: {min_date}"
                )
            else:
                self.logger.info(
                    f"📅 [BigQuery] Sử dụng ngày giao dịch nhỏ nhất truyền từ RAM: {min_date}"
                )

            # 2. Chạy giao dịch SQL để xóa dữ liệu cũ (có phân vùng tĩnh) và chèn dữ liệu mới từ bảng tạm
            self.logger.info(
                f"⚡ [BigQuery] Bước 2: Thực thi giao dịch SQL đồng bộ sang bảng chính {Config.BQ_ADJ_TABLE}..."
            )
            sync_query: str = f"""
            BEGIN TRANSACTION;

            # Xóa lịch sử cũ từ ngày nhỏ nhất của dữ liệu mới để pruning partitions
            DELETE FROM `{target_table_ref}`
            WHERE symbol = @symbol
              AND trading_date >= @min_date;

            # Sao chép toàn bộ dữ liệu từ bảng tạm sang bảng chính
            INSERT INTO `{target_table_ref}` (
              symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange, source
            )
            SELECT symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange, source
            FROM `{staging_table_ref}`;

            COMMIT TRANSACTION;
            """
            query_config: bigquery.QueryJobConfig = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("symbol", "STRING", symbol_upper),
                    bigquery.ScalarQueryParameter("min_date", "DATE", min_date),
                ]
            )
            query_job: bigquery.QueryJob = self.bq_client.query(
                sync_query, job_config=query_config
            )
            query_job.result()
            self.logger.info(
                f"🎉 [BigQuery] Đồng bộ hoàn tất giao dịch SQL cho mã {symbol_upper}."
            )

        except Exception as e:
            self.logger.error(
                f"❌ [BigQuery] Lỗi đồng bộ lịch sử điều chỉnh cho mã {symbol_upper}: {e}"
            )
            raise e
        finally:
            # 3. Dọn dẹp bảng Staging kể cả khi chạy thành công hay thất bại
            try:
                self.logger.info(
                    f"🧹 [BigQuery] Bước 3: Dọn dẹp dứt điểm bảng tạm {staging_table_name}..."
                )
                self.bq_client.delete_table(staging_table_ref, not_found_ok=True)
            except Exception as clean_err:
                self.logger.warning(
                    f"⚠️ [BigQuery] Không thể xóa bảng tạm {staging_table_name}: {clean_err}"
                )

    def sync_adjusted_symbols_to_bigquery(self, symbols: list[str]) -> None:
        """Đồng bộ lịch sử điều chỉnh của danh sách các mã chứng khoán lên BigQuery qua một Staging Table duy nhất.

        Sử dụng cơ chế Staging Table gộp để giảm thiểu chi phí DDL (tạo/xóa bảng tạm)
        và tối ưu hóa DML Delete bằng bộ lọc phân vùng động.

        Args:
            symbols (list[str]): Danh sách các mã chứng khoán cần đồng bộ.

        Raises:
            Exception: Phát sinh khi load job hoặc giao dịch SQL đồng bộ gặp lỗi.
        """
        if not symbols:
            return

        symbols_upper: list[str] = [s.upper() for s in symbols]

        valid_symbols: list[str] = []
        valid_gcs_uris: list[str] = []
        for sym in symbols_upper:
            blob_key: str = (
                f"{Config.GCS_PARQUET_PREFIX}/adj/reloaded/{sym}.parquet"
            )
            if self.bucket.blob(blob_key).exists():
                valid_symbols.append(sym)
                valid_gcs_uris.append(
                    f"gs://{Config.GCS_BUCKET_NAME}/{blob_key}"
                )
            else:
                self.logger.warning(
                    f"⚠️ [BigQuery] Bỏ qua mã {sym} do không tìm thấy file Parquet trên GCS: "
                    f"gs://{Config.GCS_BUCKET_NAME}/{blob_key}"
                )

        if not valid_gcs_uris:
            self.logger.warning(
                "⚠️ [BigQuery] Không có mã nào có file Parquet hợp lệ trên GCS. Hủy đồng bộ gộp."
            )
            return

        self.logger.info(
            f"⚡ [BigQuery] Bắt đầu đồng bộ gộp lịch sử điều chỉnh cho {len(valid_symbols)} mã..."
        )

        target_table_ref: str = (
            f"{self.bq_client.project}.{Config.BQ_DATASET}.{Config.BQ_ADJ_TABLE}"
        )
        run_id: str = uuid.uuid4().hex[:8]
        staging_table_name: str = f"{Config.BQ_ADJ_TABLE}_staging_batch_{run_id}"
        staging_table_ref: str = (
            f"{self.bq_client.project}.{Config.BQ_DATASET}.{staging_table_name}"
        )

        # Đảm bảo bảng đích đã sẵn sàng hoạt động
        self._ensure_table_exists(Config.BQ_ADJ_TABLE)

        # 1. Nạp dữ liệu từ các file GCS vào bảng Staging (Tạm thời)
        job_config: bigquery.LoadJobConfig = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE",  # Đảm bảo ghi đè bảng tạm cũ nếu tồn tại
        )

        try:
            self.logger.info(
                f"⚡ [BigQuery] Bước 1: Nạp các file Parquet từ GCS vào bảng Staging chung: {staging_table_name}..."
            )
            load_job: bigquery.LoadJob = self.bq_client.load_table_from_uri(
                valid_gcs_uris, staging_table_ref, job_config=job_config
            )
            load_job.result()
            self.logger.info(
                f"🎉 [BigQuery] Nạp thành công vào bảng tạm {staging_table_name}. "
                f"Đã nạp {load_job.output_rows} dòng."
            )

            # 1.5. Xác định ngày giao dịch nhỏ nhất để làm mốc cắt tỉa phân vùng tĩnh
            self.logger.info(
                "⚡ [BigQuery] Đang lấy ngày giao dịch nhỏ nhất từ bảng tạm..."
            )
            min_date_query: str = (
                f"SELECT MIN(trading_date) as min_date FROM `{staging_table_ref}`"
            )
            min_date_job: bigquery.QueryJob = self.bq_client.query(min_date_query)
            min_date_result: bigquery.table.RowIterator = min_date_job.result()
            min_date_rows: list[bigquery.Row] = list(min_date_result)
            min_date: date = (
                min_date_rows[0].min_date
                if min_date_rows and min_date_rows[0].min_date
                else datetime.now(Config.VN_TZ).date()
            )
            self.logger.info(
                f"📅 [BigQuery] Ngày giao dịch nhỏ nhất phát hiện từ staging: {min_date}"
            )

            # 2. Chạy giao dịch SQL để xóa dữ liệu cũ (có phân vùng tĩnh) và chèn dữ liệu mới từ bảng tạm
            self.logger.info(
                f"⚡ [BigQuery] Bước 2: Thực thi giao dịch SQL đồng bộ sang bảng chính {Config.BQ_ADJ_TABLE}..."
            )
            sync_query: str = f"""
            BEGIN TRANSACTION;

            # Xóa lịch sử cũ của các mã reloaded từ ngày nhỏ nhất
            DELETE FROM `{target_table_ref}`
            WHERE symbol IN UNNEST(@symbols)
              AND trading_date >= @min_date;

            # Sao chép toàn bộ dữ liệu từ bảng tạm sang bảng chính
            INSERT INTO `{target_table_ref}` (
              symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange, source
            )
            SELECT symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange, source
            FROM `{staging_table_ref}`;

            COMMIT TRANSACTION;
            """
            query_config: bigquery.QueryJobConfig = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("symbols", "STRING", valid_symbols),
                    bigquery.ScalarQueryParameter("min_date", "DATE", min_date),
                ]
            )
            query_job: bigquery.QueryJob = self.bq_client.query(
                sync_query, job_config=query_config
            )
            query_job.result()
            self.logger.info(
                f"🎉 [BigQuery] Đồng bộ hoàn tất giao dịch SQL gộp cho {len(valid_symbols)} mã."
            )

        except Exception as e:
            self.logger.error(f"❌ [BigQuery] Lỗi đồng bộ gộp lịch sử điều chỉnh: {e}")
            raise e
        finally:
            # 3. Dọn dẹp bảng Staging kể cả khi chạy thành công hay thất bại
            try:
                self.logger.info(
                    f"🧹 [BigQuery] Bước 3: Dọn dẹp dứt điểm bảng tạm {staging_table_name}..."
                )
                self.bq_client.delete_table(staging_table_ref, not_found_ok=True)
            except Exception as clean_err:
                self.logger.warning(
                    f"⚠️ [BigQuery] Không thể xóa bảng tạm {staging_table_name}: {clean_err}"
                )

    def load_parquet_to_bigquery(
        self, gcs_path: str, table_name: str, write_disposition: str = "WRITE_APPEND"
    ) -> None:
        """Nạp trực tiếp tệp Parquet từ GCS vào BigQuery.

        Args:
            gcs_path (str): Đường dẫn lưu trữ tương đối trên GCS bucket.
            table_name (str): Tên bảng đích BigQuery.
            write_disposition (str): Chế độ ghi bảng ('WRITE_APPEND' hoặc 'WRITE_TRUNCATE').

        Raises:
            Exception: Phát sinh khi load job thất bại.
        """
        gcs_uri: str = f"gs://{Config.GCS_BUCKET_NAME}/{gcs_path}"
        table_ref: str = f"{self.bq_client.project}.{Config.BQ_DATASET}.{table_name}"

        self.logger.info(
            f"⚡ [BigQuery] Đang nạp dữ liệu từ {gcs_uri} vào {table_ref} ({write_disposition})..."
        )

        self._ensure_table_exists(table_name)

        job_config: bigquery.LoadJobConfig = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
        )

        try:
            load_job: bigquery.LoadJob = self.bq_client.load_table_from_uri(
                gcs_uri, table_ref, job_config=job_config
            )
            load_job.result()
            self.logger.info(
                f"🎉 [BigQuery] Nạp thành công vào {table_name}. Đã chèn {load_job.output_rows} dòng."
            )
        except Exception as e:
            self.logger.error(
                f"❌ [BigQuery] Lỗi khi nạp dữ liệu từ {gcs_path} vào {table_name}: {e}"
            )
            raise e

    def sync_partition_to_bigquery(
        self, gcs_path: str, table_name: str, date_ref: date
    ) -> None:
        """Đồng bộ hóa dữ liệu phân vùng một ngày từ GCS vào BigQuery sử dụng bảng tạm và transaction.

        Đảm bảo tính toàn vẹn dữ liệu (idempotency và atomicity) của bảng đích.

        Args:
            gcs_path (str): Đường dẫn tương đối của tệp Parquet trên GCS.
            table_name (str): Tên bảng đích trong BigQuery.
            date_ref (date): Ngày giao dịch của phân vùng cần đồng bộ.

        Raises:
            Exception: Phát sinh khi quá trình nạp hoặc giao dịch SQL gặp sự cố.
        """
        gcs_uri: str = f"gs://{Config.GCS_BUCKET_NAME}/{gcs_path}"
        date_str: str = date_ref.strftime("%Y-%m-%d")
        self.logger.info(
            f"⚡ [BigQuery] Bắt đầu đồng bộ phân vùng (Staging Mode) cho bảng {table_name} ngày {date_str}..."
        )

        target_table_ref: str = (
            f"{self.bq_client.project}.{Config.BQ_DATASET}.{table_name}"
        )

        run_id: str = uuid.uuid4().hex[:8]
        clean_date_str: str = date_ref.strftime("%Y%m%d")
        staging_table_name: str = f"{table_name}_staging_{clean_date_str}_{run_id}"
        staging_table_ref: str = (
            f"{self.bq_client.project}.{Config.BQ_DATASET}.{staging_table_name}"
        )

        self._ensure_table_exists(table_name)

        job_config: bigquery.LoadJobConfig = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE",
        )

        try:
            self.logger.info(
                f"⚡ [BigQuery] Bước 1: Nạp file Parquet từ GCS vào bảng Staging: {staging_table_name}..."
            )
            load_job: bigquery.LoadJob = self.bq_client.load_table_from_uri(
                gcs_uri, staging_table_ref, job_config=job_config
            )
            load_job.result()
            self.logger.info(
                f"🎉 [BigQuery] Nạp thành công vào bảng tạm {staging_table_name}. "
                f"Đã nạp {load_job.output_rows} dòng."
            )

            # 2. Giao dịch SQL đồng bộ sang bảng chính
            self.logger.info(
                f"⚡ [BigQuery] Bước 2: Thực thi giao dịch SQL đồng bộ sang bảng chính {table_name}..."
            )
            sync_query: str = f"""
            BEGIN TRANSACTION;

            # Xóa dữ liệu cũ ngày hôm đó
            DELETE FROM `{target_table_ref}`
            WHERE trading_date = @target_date;

            # Sao chép toàn bộ dữ liệu từ bảng tạm sang bảng chính
            INSERT INTO `{target_table_ref}` (
              symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange, source
            )
            SELECT symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange, source
            FROM `{staging_table_ref}`;

            COMMIT TRANSACTION;
            """
            query_config: bigquery.QueryJobConfig = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("target_date", "DATE", date_str),
                ]
            )
            query_job: bigquery.QueryJob = self.bq_client.query(
                sync_query, job_config=query_config
            )
            query_job.result()
            self.logger.info(
                f"🎉 [BigQuery] Đồng bộ hoàn tất giao dịch SQL cho bảng {table_name} ngày {date_str}."
            )
        except Exception as e:
            self.logger.error(
                f"❌ [BigQuery] Lỗi đồng bộ phân vùng ngày {date_str} cho bảng {table_name}: {e}"
            )
            raise e
        finally:
            try:
                self.logger.info(
                    f"🧹 [BigQuery] Bước 3: Dọn dẹp bảng tạm {staging_table_name}..."
                )
                self.bq_client.delete_table(staging_table_ref, not_found_ok=True)
            except Exception as clean_err:
                self.logger.warning(
                    f"⚠️ [BigQuery] Không thể xóa bảng tạm {staging_table_name}: {clean_err}"
                )

    def sync_daily_adjusted_prices(
        self, dates: list[datetime | date], excluded_symbols: list[str]
    ) -> None:
        """Đồng bộ hóa hàng loạt dữ liệu từ raw_price sang adjusted_price cho danh sách các ngày.

        Loại bỏ các mã có sự kiện điều chỉnh giá (đã được tải lại toàn bộ lịch sử riêng biệt)
        để tránh ghi đè dữ liệu lịch sử điều chỉnh chính xác.

        Args:
            dates (list[Union[datetime, date]]): Danh sách các ngày cần đồng bộ.
            excluded_symbols (list[str]): Các mã chứng khoán có sự kiện chia tách/cổ tức (cần loại trừ).

        Raises:
            Exception: Phát sinh khi truy vấn DML trong giao dịch BigQuery gặp lỗi.
        """
        if not dates:
            return

        date_strings: list[str] = [
            d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in dates
        ]

        raw_table_ref: str = (
            f"`{self.bq_client.project}.{Config.BQ_DATASET}.{Config.BQ_RAW_TABLE}`"
        )
        adj_table_ref: str = (
            f"`{self.bq_client.project}.{Config.BQ_DATASET}.{Config.BQ_ADJ_TABLE}`"
        )

        self._ensure_table_exists(Config.BQ_RAW_TABLE)
        self._ensure_table_exists(Config.BQ_ADJ_TABLE)
        self.logger.info(
            f"⚡ [BigQuery] Đang sao chép giá từ raw sang adjusted cho các ngày: {date_strings}..."
        )

        query_params: list[Any] = [
            bigquery.ArrayQueryParameter("dates", "DATE", date_strings),
        ]

        exclude_clause: str = ""
        if excluded_symbols:
            exclude_clause = "AND symbol NOT IN UNNEST(@excluded_symbols)"
            query_params.append(
                bigquery.ArrayQueryParameter(
                    "excluded_symbols", "STRING", [s.upper() for s in excluded_symbols]
                )
            )

        query: str = f"""
        BEGIN TRANSACTION;

        # Xóa dữ liệu cũ nếu đã tồn tại để tránh trùng lặp dữ liệu khi chạy lại
        DELETE FROM {adj_table_ref}
        WHERE trading_date IN UNNEST(@dates) {exclude_clause};

        # Chèn giá thô ngày T từ raw_price sang adjusted_price đối với các mã bình thường
        INSERT INTO {adj_table_ref}
          (symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange, source)
        SELECT
          symbol,
          trading_date,
          open_price,
          high_price,
          low_price,
          close_price,
          total_volume,
          exchange,
          source
        FROM {raw_table_ref}
        WHERE trading_date IN UNNEST(@dates) {exclude_clause};

        COMMIT TRANSACTION;
        """

        try:
            query_config: bigquery.QueryJobConfig = bigquery.QueryJobConfig(
                query_parameters=query_params
            )
            query_job: bigquery.QueryJob = self.bq_client.query(
                query, job_config=query_config
            )
            query_job.result()
            self.logger.info(
                f"🎉 [BigQuery] Hòn tất đồng bộ adjusted_price cho {len(date_strings)} ngày."
            )
        except Exception as e:
            self.logger.error(
                f"❌ [BigQuery] Lỗi đồng bộ adjusted_price số lượng lớn ngày: {e}"
            )
            raise e

    def delete_by_date(self, table_name: str, date_ref: datetime | date | str) -> None:
        """Xóa toàn bộ bản ghi của một ngày cụ thể trong bảng BigQuery.

        Giúp đảm bảo tính idempotent của pipeline (chạy lại nhiều lần không sinh bản ghi thừa).

        Args:
            table_name (str): Tên bảng đích BigQuery.
            date_ref (Union[datetime, date, str]): Ngày giao dịch cần xóa.

        Raises:
            Exception: Phát sinh khi câu lệnh DELETE bị lỗi.
        """
        date_str: str = (
            date_ref.strftime("%Y-%m-%d")
            if hasattr(date_ref, "strftime")
            else str(date_ref)
        )
        table_ref: str = f"`{self.bq_client.project}.{Config.BQ_DATASET}.{table_name}`"

        self._ensure_table_exists(table_name)
        self.logger.info(
            f"🗑️ [BigQuery] Đang xóa dữ liệu cũ ngày {date_str} từ bảng {table_ref}..."
        )

        query: str = f"DELETE FROM {table_ref} WHERE trading_date = @target_date"
        query_config: bigquery.QueryJobConfig = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("target_date", "DATE", date_str),
            ]
        )
        try:
            query_job: bigquery.QueryJob = self.bq_client.query(
                query, job_config=query_config
            )
            query_job.result()
            self.logger.info(f"🎉 [BigQuery] Đã dọn dẹp xong dữ liệu ngày {date_str}.")
        except Exception as e:
            self.logger.error(
                f"❌ [BigQuery] Lỗi khi dọn dẹp dữ liệu ngày {date_str}: {e}"
            )
            raise e

    def read_checkpoint(self) -> dict[str, Any]:
        """Đọc tệp snapshot thị trường EOD được lưu trữ trước đó trên GCS.

        Returns:
            dict[str, Any]: Dict chứa thông tin metadata và trạng thái snapshots của các mã.
                Trả về dict rỗng nếu không tồn tại hoặc lỗi đọc file.
        """
        try:
            blob: storage.Blob = self.bucket.blob(Config.GCS_CHECKPOINT_KEY)
            if blob.exists():
                json_str: str = blob.download_as_text(encoding="utf-8")
                result: dict[str, Any] = json.loads(json_str)
                return result
        except Exception as e:
            self.logger.warning(
                f"⚠️ [GCS] Không thể đọc file checkpoint từ GCS do lỗi: {e}. Tiến hành khởi tạo mới."
            )
            return {}
        return {}

    def save_checkpoint(
        self,
        df: pd.DataFrame,
        active_symbols: set[str] | None = None,
        pending_adjusted_reloads: list[str] | None = None,
    ) -> None:
        """Trích xuất và cập nhật trạng thái thị trường EOD (Snapshot) trực tiếp lên GCS.

        Args:
            df (pd.DataFrame): DataFrame dữ liệu tổng hợp của ngày chạy hiện tại.
            active_symbols (Optional[set[str]]): Danh sách các mã cổ phiếu đang niêm yết thực tế trên thị trường.
            pending_adjusted_reloads (Optional[list[str]]): Danh sách các mã lỗi cần chạy lại lịch sử điều chỉnh lần sau.
        """
        if df is None or df.empty:
            return

        self.logger.info("⚡ [Snapshot] Đang trích xuất trạng thái thị trường EOD...")

        # 1. Lọc lấy bản ghi mới nhất của ngày hôm nay cho từng mã
        df_latest: pd.DataFrame = df.drop_duplicates(
            subset=["symbol"], keep="last"
        ).copy()

        # Tính toán giá trung bình nhanh chóng bằng toán tử cột
        price_cols: list[str] = ["open_price", "high_price", "low_price", "close_price"]
        if "average_price" not in df_latest.columns:
            df_latest["average_price"] = df_latest[price_cols].mean(axis=1)

        # Chuẩn hóa kiểu dữ liệu số thực về int (Raw prices) và float64 (average_price) để lưu sạch trên JSON
        for col in price_cols:
            df_latest[col] = df_latest[col].astype(float).round(0).astype(int)
        df_latest["average_price"] = df_latest["average_price"].astype(float).round(1)

        # Chuẩn hóa cột ngày sang chuỗi YYYY-MM-DD
        df_latest["trading_date"] = df_latest["trading_date"].dt.strftime("%Y-%m-%d")

        # Lấy ngày chạy lớn nhất để lưu metadata
        max_date_str: str = str(df_latest["trading_date"].max())

        # Tự động tính toán xem dữ liệu này đã được chốt phiên cuối ngày (EOD) chưa
        vn_now: datetime = datetime.now(Config.VN_TZ)
        today_str: str = vn_now.strftime("%Y-%m-%d")
        is_eod: bool
        if max_date_str < today_str:
            is_eod = True
        else:
            # Thị trường chứng khoán Việt Nam chốt phiên lúc 15:00 và dữ liệu hoàn tất sau 15:15
            is_eod = vn_now.hour > 15 or (vn_now.hour == 15 and vn_now.minute >= 15)

        # Đảm bảo index symbol là chuỗi thông thường (không phải categorical) để xuất dict sạch sẽ
        if isinstance(df_latest["symbol"].dtype, pd.CategoricalDtype):
            df_latest["symbol"] = df_latest["symbol"].astype(str)
        df_latest.set_index("symbol", inplace=True)

        # Chỉ lấy các cột cần thiết, ép kiểu chuẩn về dict nguyên bản của Python để gom JSON
        cols_to_extract: list[str] = [
            "exchange",
            "trading_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "average_price",
            "total_volume",
        ]
        current_data_dict: dict[str, dict[str, Any]] = df_latest[
            cols_to_extract
        ].to_dict(orient="index")

        # 2. Đọc dữ liệu lịch sử cũ từ file checkpoint trên GCS
        old_checkpoint: dict[str, Any] = self.read_checkpoint()
        merged_snapshots: dict[str, dict[str, Any]] = old_checkpoint.get(
            "snapshots", {}
        )
        old_metadata: dict[str, Any] = old_checkpoint.get("metadata") or {}
        old_pending: list[str] = old_metadata.get("pending_adjusted_reloads") or []

        # Hợp nhất pending_adjusted_reloads cũ nếu không được truyền vào mới
        if pending_adjusted_reloads is None:
            pending_adjusted_reloads = old_pending

        # 3. Tiến hành Hợp nhất (Upsert) - Chỉ cập nhật snapshots khi đã chốt phiên EOD thực tế
        if is_eod:
            for sym, new_row in current_data_dict.items():
                if not sym:
                    continue
                old_row: dict[str, Any] | None = merged_snapshots.get(sym)
                # Nếu mã chưa có hoặc có ngày mới hơn/bằng ngày cũ -> Cập nhật thông tin mới nhất
                if not old_row or new_row["trading_date"] >= old_row["trading_date"]:
                    merged_snapshots[sym] = new_row
        else:
            self.logger.info(
                "ℹ️ [Snapshot] Đang chạy trong phiên (Chưa chốt EOD). "
                "Giữ nguyên dữ liệu snapshots lịch sử từ phiên EOD trước."
            )

        # Chuẩn hóa toàn bộ dữ liệu trong merged_snapshots để dọn dẹp các tàn dư float32 cũ
        for sym, row in merged_snapshots.items():
            for col in price_cols:
                if col in row and isinstance(row[col], (int, float)):
                    row[col] = int(round(float(row[col])))
            if "average_price" in row and isinstance(
                row["average_price"], (int, float)
            ):
                row["average_price"] = round(float(row["average_price"]), 1)

        # 4. Áp dụng bộ lọc active_symbols & Sắp xếp Alphabet gọn gàng
        final_snapshots: dict[str, dict[str, Any]] = {}
        for sym in sorted(merged_snapshots.keys()):
            if active_symbols and sym not in active_symbols:
                continue
            final_snapshots[sym] = merged_snapshots[sym]

        # 5. Cấu trúc JSON cuối cùng
        final_json_structure: dict[str, Any] = {
            "metadata": {
                "last_successful_run": max_date_str,
                "is_eod": is_eod,
                "total_tickers": len(final_snapshots),
                "pending_adjusted_reloads": pending_adjusted_reloads,
            },
            "snapshots": final_snapshots,
        }

        # 6. Upload JSON trực tiếp lên GCS
        try:
            json_str: str = json.dumps(
                final_json_structure,
                cls=CustomJSONEncoder,
                ensure_ascii=False,
                indent=2,
            )
            blob: storage.Blob = self.bucket.blob(Config.GCS_CHECKPOINT_KEY)
            blob.upload_from_string(json_str, content_type="application/json")
            self.logger.info(
                f"💾 ☁️ [Snapshot Thành Công] Đã cập nhật tổng cộng {len(final_snapshots)} mã "
                f"tại GCS: gs://{Config.GCS_BUCKET_NAME}/{Config.GCS_CHECKPOINT_KEY}"
            )
        except Exception as e:
            self.logger.error(
                f"🛑 [GCS] Ghi tệp snapshot trạng thái lên GCS thất bại: {e}"
            )
        finally:
            # Giải phóng các cấu trúc dữ liệu lớn thủ công để tối ưu RAM
            del current_data_dict, merged_snapshots, final_snapshots
            gc.collect()

    def read_blacklist(self) -> set[str]:
        """Đọc tệp danh sách đen (blacklist.txt) từ GCS, nếu không có hoặc lỗi thì fallback về file cục bộ.

        Returns:
            set[str]: Set chứa các mã chứng khoán viết hoa thuộc danh sách đen.
        """
        blacklist_key: str = Config.GCS_BLACKLIST_KEY
        blob: storage.Blob = self.bucket.blob(blacklist_key)

        if blob.exists():
            try:
                content: str = blob.download_as_text(encoding="utf-8")
                blacklist: set[str] = set()
                for line in content.splitlines():
                    line = line.strip().upper()
                    if line and not line.startswith("#"):
                        blacklist.add(line)
                self.logger.info(
                    f"🎉 [GCS] Đã tải danh sách đen gồm {len(blacklist)} mã "
                    f"từ GCS: gs://{Config.GCS_BUCKET_NAME}/{blacklist_key}"
                )
                return blacklist
            except Exception as e:
                self.logger.warning(
                    f"⚠️ [GCS] Lỗi khi tải blacklist từ GCS: {e}. Thử đọc file cục bộ..."
                )

        # Fallback đọc từ file cục bộ
        try:
            with open(Config.GCS_BLACKLIST_KEY, "r", encoding="utf-8") as file:
                blacklist: set[str] = {
                    line.strip().upper()
                    for line in file
                    if line.strip() and not line.strip().startswith("#")
                }
                self.logger.info(
                    f"📂 [Local] Đã tải danh sách đen gồm {len(blacklist)} mã từ file cục bộ."
                )
                return blacklist
        except FileNotFoundError:
            self.logger.warning(
                f"⚠️ [Local] Không tìm thấy file '{Config.GCS_BLACKLIST_KEY}' cục bộ. Bỏ qua bộ lọc danh sách đen."
            )
            return set()

    def export_interested_tickers_data(self) -> dict[str, Any] | None:
        """Trích xuất dữ liệu giá thô và giá điều chỉnh theo số năm cấu hình cho các mã cổ phiếu quan tâm.

        Danh sách các mã cổ phiếu được lấy từ file text (.txt) lưu trên GCS.
        Dữ liệu trích xuất sẽ được lưu lại dưới dạng Parquet riêng biệt cho từng mã trên GCS.

        Returns:
            Optional[dict[str, Any]]: Dict chứa tóm tắt kết quả xuất dữ liệu (số lượng mã, dòng thô, dòng điều chỉnh đã xuất),
                hoặc None nếu không tìm thấy danh sách mã quan tâm hoặc danh sách trống.
        """
        # 1. Đọc tệp cấu hình danh sách mã quan tâm từ GCS
        tickers_key: str = Config.GCS_EXPORT_TICKERS_KEY
        blob: storage.Blob = self.bucket.blob(tickers_key)

        if not blob.exists():
            self.logger.info(
                f"ℹ️ Không tìm thấy file danh sách mã cổ phiếu quan tâm tại "
                f"gs://{Config.GCS_BUCKET_NAME}/{tickers_key}. Bỏ qua trích xuất."
            )
            return None

        content: str = blob.download_as_text(encoding="utf-8")
        tickers: list[str] = []
        ticker_pattern: re.Pattern = re.compile(r"^[A-Z0-9]{3,10}$")
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Cho phép các định dạng phân cách bằng khoảng trắng hoặc dấu phẩy
            parts: list[str] = [
                p.strip().upper() for p in line.replace(",", " ").split() if p.strip()
            ]
            valid_parts: list[str] = [p for p in parts if ticker_pattern.match(p)]
            invalid_parts: list[str] = [p for p in parts if not ticker_pattern.match(p)]
            if invalid_parts:
                self.logger.warning(
                    f"⚠️ [Export] Phát hiện mã không hợp lệ bị loại bỏ khỏi danh sách xuất: {invalid_parts}"
                )
            tickers.extend(valid_parts)

        tickers = sorted(list(set(tickers)))
        if not tickers:
            self.logger.info(
                f"ℹ️ File danh sách mã cổ phiếu quan tâm tại gs://{Config.GCS_BUCKET_NAME}/{tickers_key} trống. "
                "Bỏ qua trích xuất."
            )
            return None

        self.logger.info(
            f"🚀 Bắt đầu trích xuất dữ liệu cho {len(tickers)} mã quan tâm bằng truy vấn gộp..."
        )

        # 2. Tính toán khoảng thời gian (trọn vẹn các năm trước + năm hiện tại)
        current_year: int = datetime.now(Config.VN_TZ).year
        start_year: int = current_year - Config.GCS_EXPORT_YEARS
        start_date: date = date(start_year, 1, 1)
        self.logger.info(
            f"📅 Khoảng thời gian trích xuất: Từ {start_date} đến nay (Cấu hình {Config.GCS_EXPORT_YEARS} năm)."
        )

        try:
            export_prefix: str = Config.GCS_EXPORT_PREFIX
            raw_count: int = 0
            adj_count: int = 0
            exported_raw_tickers: set[str] = set()
            exported_adj_tickers: set[str] = set()

            # Chia nhỏ danh sách mã để truy vấn theo từng nhóm (mỗi nhóm tối đa 150 mã) để tối ưu hóa hiệu năng BigQuery
            batch_size: int = 150

            # Bước 1: Trích xuất dữ liệu Giá Thô (Raw)
            self.logger.info(
                "📥 [Batch Export] Đang truy vấn và ghi dữ liệu Giá Thô theo nhóm..."
            )
            for i in range(0, len(tickers), batch_size):
                batch_tickers: list[str] = tickers[i : i + batch_size]
                self.logger.info(
                    f"   ↳ Đang xử lý nhóm Giá Thô {i // batch_size + 1}: {len(batch_tickers)} mã..."
                )
                raw_query: str = f"""
                    SELECT symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange, source
                    FROM `{self.bq_client.project}.{Config.BQ_DATASET}.{Config.BQ_RAW_TABLE}`
                    WHERE symbol IN UNNEST(@tickers)
                      AND trading_date >= @start_date
                    ORDER BY trading_date ASC
                """
                raw_job_config: bigquery.QueryJobConfig = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter(
                            "tickers", "STRING", batch_tickers
                        ),
                        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                    ]
                )
                df_raw_batch: pd.DataFrame = self.bq_client.query(
                    raw_query, job_config=raw_job_config
                ).to_dataframe()

                if not df_raw_batch.empty:
                    for ticker, group in df_raw_batch.groupby("symbol"):
                        ticker_str: str = str(ticker).strip().upper()
                        gcs_path: str = f"{export_prefix}/raw/{ticker_str}.parquet"

                        bio: io.BytesIO = self._df_to_parquet_bytes(group, suffix="raw")

                        blob: storage.Blob = self.bucket.blob(gcs_path)
                        blob.upload_from_file(
                            bio, content_type="application/octet-stream"
                        )
                        raw_count += len(group)
                        exported_raw_tickers.add(ticker_str)

                # Giải phóng bộ nhớ của lô hiện tại trước khi chuyển sang lô tiếp theo
                del df_raw_batch
                gc.collect()

            # Bước 2: Trích xuất dữ liệu Giá Điều Chỉnh (Adj)
            self.logger.info(
                "📥 [Batch Export] Đang truy vấn và ghi dữ liệu Giá Điều Chỉnh theo nhóm..."
            )
            for i in range(0, len(tickers), batch_size):
                batch_tickers = tickers[i : i + batch_size]
                self.logger.info(
                    f"   ↳ Đang xử lý nhóm Giá Điều Chỉnh {i // batch_size + 1}: {len(batch_tickers)} mã..."
                )
                adj_query: str = f"""
                    SELECT symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange, source
                    FROM `{self.bq_client.project}.{Config.BQ_DATASET}.{Config.BQ_ADJ_TABLE}`
                    WHERE symbol IN UNNEST(@tickers)
                      AND trading_date >= @start_date
                    ORDER BY trading_date ASC
                """
                adj_job_config: bigquery.QueryJobConfig = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter(
                            "tickers", "STRING", batch_tickers
                        ),
                        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                    ]
                )
                df_adj_batch: pd.DataFrame = self.bq_client.query(
                    adj_query, job_config=adj_job_config
                ).to_dataframe()

                if not df_adj_batch.empty:
                    for ticker, group in df_adj_batch.groupby("symbol"):
                        ticker_str = str(ticker).strip().upper()
                        gcs_path = f"{export_prefix}/adj/{ticker_str}.parquet"

                        bio = self._df_to_parquet_bytes(group, suffix="adj")

                        blob = self.bucket.blob(gcs_path)
                        blob.upload_from_file(
                            bio, content_type="application/octet-stream"
                        )
                        adj_count += len(group)
                        exported_adj_tickers.add(ticker_str)

                # Giải phóng bộ nhớ của lô hiện tại trước khi chuyển sang lô tiếp theo
                del df_adj_batch
                gc.collect()

            exported_tickers_count: int = len(
                exported_raw_tickers.union(exported_adj_tickers)
            )
            self.logger.info("✅ Hoàn tất xuất dữ liệu lên GCS cho các mã quan tâm.")
            self.logger.info(
                f"📊 Chi tiết: {exported_tickers_count} mã được xuất, "
                f"Tổng số dòng raw: {raw_count}, adj: {adj_count}"
            )

            return {
                "tickers_count": len(tickers),
                "exported_count": exported_tickers_count,
                "raw_rows": raw_count,
                "adj_rows": adj_count,
            }

        except Exception as e:
            self.logger.error(
                f"❌ Lỗi trong quá trình truy vấn hoặc ghi dữ liệu xuất lên GCS: {e}",
                exc_info=True,
            )
            raise e

    def save_corporate_events(self, events: list[dict[str, Any]]) -> None:
        """Ghi nhận sự kiện doanh nghiệp (Không thực hiện ở phiên bản CloudStorage).

        Args:
            events (list[dict[str, Any]]): Danh sách các sự kiện doanh nghiệp.
        """
        pass

    def save_companies(self, df_companies: pd.DataFrame) -> None:
        """Ghi nhận danh sách công ty (Không thực hiện ở phiên bản CloudStorage).

        Args:
            df_companies (pd.DataFrame): DataFrame chứa thông tin danh sách các công ty.
        """
        pass
