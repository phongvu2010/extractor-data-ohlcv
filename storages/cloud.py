"""Module cung cấp cơ chế lưu trữ dữ liệu chứng khoán lên Google Cloud Storage (GCS) và Google Cloud BigQuery."""

from __future__ import annotations

from datetime import date, datetime
import gc
import io
import json
import logging
import math
import re
import time
from typing import Any
import uuid

import google.api_core.exceptions
from google.cloud import bigquery, storage
import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from config import Config
from .base import BaseStorage


class CustomJSONEncoder(json.JSONEncoder):
    """Bộ mã hóa JSON tùy chỉnh để xử lý an toàn các kiểu dữ liệu NumPy và Python native."""

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

        if obj is None or (isinstance(obj, float) and math.isnan(obj)):
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
                "✅ [GCS] Kết nối thành công qua Application Default Credentials (ADC)."
            )
            self.logger.info(f"ℹ️ [GCS] Bucket: {Config.GCS_BUCKET_NAME}")
            self.logger.info(f"ℹ️ [BigQuery] Dự án: {self.bq_client.project}")
        except Exception as e:
            self.logger.error(f"❌ [Storage] Lỗi khởi tạo kết nối Cloud Services: {e}")
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

        prefix_raw: str = f"{Config.BQ_RAW_TABLE}_staging_"
        prefix_adj: str = f"{Config.BQ_ADJ_TABLE}_staging_"
        self.logger.info(
            f"🧹 [BigQuery] Đang quét các bảng staging còn sót lại với tiền tố '{prefix_raw}' và '{prefix_adj}'..."
        )

        try:
            query: str = f"""
                SELECT table_name
                FROM `{self.bq_client.project}.{Config.BQ_DATASET}.INFORMATION_SCHEMA.TABLES`
                WHERE table_name LIKE @prefix_raw OR table_name LIKE @prefix_adj
            """
            query_config: bigquery.QueryJobConfig = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "prefix_raw", "STRING", f"{prefix_raw}%"
                    ),
                    bigquery.ScalarQueryParameter(
                        "prefix_adj", "STRING", f"{prefix_adj}%"
                    ),
                ]
            )
            query_job: bigquery.QueryJob = self.bq_client.query(
                query, job_config=query_config
            )
            rows: bigquery.table.RowIterator = query_job.result()

            stale_tables: list[str] = [row.table_name for row in rows]

            if not stale_tables:
                self.logger.info(
                    "✅ [BigQuery] Không phát hiện bảng staging nào còn sót lại."
                )
                return

            self.logger.warning(
                f"⚠️ [BigQuery] Phát hiện {len(stale_tables)} bảng staging còn sót lại: {stale_tables}"
            )
            for table_id in stale_tables:
                table_ref: str = (
                    f"{self.bq_client.project}.{Config.BQ_DATASET}.{table_id}"
                )
                try:
                    self.bq_client.delete_table(table_ref, not_found_ok=True)
                    self.logger.info(f"✅ [BigQuery] Đã xóa bảng staging: {table_id}")
                except Exception as del_err:
                    self.logger.warning(
                        f"⚠️ [BigQuery] Không thể xóa bảng staging {table_id}: {del_err}"
                    )
        except Exception as e:
            self.logger.error(
                f"❌ [BigQuery] Lỗi trong quá trình quét dọn bảng staging: {e}"
            )

    def _df_to_parquet_bytes(self, df: pl.DataFrame, suffix: str) -> io.BytesIO:
        """Chuyển đổi DataFrame sang dữ liệu nén Parquet dạng BytesIO một cách tối ưu hiệu năng.

        Đối với dữ liệu 'adj', chuyển đổi cột giá sang kiểu decimal128 ở tầng PyArrow
        nhằm tối đa hóa hiệu năng.

        Args:
            df (pl.DataFrame): DataFrame dữ liệu chứng khoán cần chuyển đổi.
            suffix (str): Hậu tố để phân loại định dạng ('raw' hoặc 'adj').

        Returns:
            io.BytesIO: Đối tượng BytesIO chứa luồng dữ liệu Parquet đã được nén Snappy.
        """
        df_write: pl.DataFrame = df.clone()

        # Ép kiểu trading_date về Date thuần túy để khớp hoàn hảo với kiểu DATE của BigQuery
        if "trading_date" in df_write.columns:
            df_write = df_write.with_columns(pl.col("trading_date").cast(pl.Date))

        price_cols: list[str] = ["open_price", "high_price", "low_price", "close_price"]
        if suffix == "raw":
            df_write = df_write.with_columns(
                [pl.col(col).round(0).cast(pl.Int64) for col in price_cols]
            )
        else:
            # Giữ kiểu float64 và làm tròn 2 chữ số thập phân ở Polars trước khi cast trong Arrow
            df_write = df_write.with_columns(
                [pl.col(col).round(2).cast(pl.Float64) for col in price_cols]
            )

        if "total_volume" in df_write.columns:
            df_write = df_write.with_columns(pl.col("total_volume").cast(pl.Int64))

        for col in ["symbol", "exchange", "source"]:
            if col in df_write.columns:
                df_write = df_write.with_columns(pl.col(col).cast(pl.String))

        # Chuyển đổi sang PyArrow Table
        table: pa.Table = df_write.to_arrow()

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
        df: pl.DataFrame,
        date_ref: datetime,
        suffix: str = "raw",
        partition: bool = False,
    ) -> str | None:
        """Ghi dữ liệu nén Parquet trực tiếp lên Google Cloud Storage (GCS).

        Args:
            df (pl.DataFrame): DataFrame dữ liệu chứng khoán cần lưu.
            date_ref (datetime): Mốc thời gian mặc định của tệp dữ liệu.
            suffix (str): Hậu tố định danh loại dữ liệu ('raw' hoặc 'adj').
            partition (bool): True để lưu phân mảnh theo năm/tháng, False để lưu file gộp tĩnh.

        Returns:
            Optional[str]: Đường dẫn GCS của file nếu lưu thành công, ngược lại là None.

        Raises:
            Exception: Phát sinh khi ghi dữ liệu lên GCS thất bại.
        """
        if df is None or df.is_empty():
            return None

        # Tối ưu: Lấy ngày thực tế lớn nhất từ cột trading_date để đặt tên thư mục/file nếu có
        if "trading_date" in df.columns:
            max_date: Any = df["trading_date"].max()
            if max_date is not None:
                if isinstance(max_date, date):
                    date_ref = datetime.combine(max_date, datetime.min.time())
                else:
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
                f"💾 [GCS] Đang ghi dữ liệu nén Parquet: gs://{Config.GCS_BUCKET_NAME}/{gcs_path}"
            )

            bio: io.BytesIO = self._df_to_parquet_bytes(df, suffix)

            blob: storage.Blob = self.bucket.blob(gcs_path)
            blob.upload_from_file(bio, content_type="application/octet-stream")

            self.logger.info(
                f"✅ [GCS] File lưu trữ thành công tại GCS: gs://{Config.GCS_BUCKET_NAME}/{gcs_path}"
            )
            return gcs_path
        except Exception as e:
            self.logger.error(f"❌ [GCS] Lỗi ghi file Parquet lên GCS: {e}")
            raise e

    def save_symbol_history(
        self, df: pl.DataFrame, symbol: str, suffix: str = "adj"
    ) -> None:
        """Ghi toàn bộ lịch sử giá của một mã cổ phiếu cụ thể ra file Parquet riêng biệt trên GCS.

        Args:
            df (pl.DataFrame): DataFrame dữ liệu lịch sử đầy đủ của mã cổ phiếu.
            symbol (str): Mã cổ phiếu cần lưu (ví dụ: FPT).
            suffix (str): Tiền tố thư mục lưu trữ ('raw' hoặc 'adj').

        Raises:
            Exception: Phát sinh khi ghi dữ liệu lên GCS thất bại.
        """
        if df is None or df.is_empty():
            return

        gcs_path: str = (
            f"{Config.GCS_PARQUET_PREFIX}/{suffix}/reloaded/{symbol.upper()}.parquet"
        )

        try:
            self.logger.info(
                f"💾 [GCS] Đang ghi dữ liệu lịch sử cho mã {symbol.upper()} "
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
            self.logger.info(f"✅ [BigQuery] Đang tạo dataset mới: {Config.BQ_DATASET}")
            self.bq_client.create_dataset(bigquery.Dataset(dataset_ref))

        table_ref: str = f"{self.bq_client.project}.{Config.BQ_DATASET}.{table_name}"
        try:
            self.bq_client.get_table(table_ref)
        except Exception:
            self.logger.info(
                f"✅ [BigQuery] Đang tạo bảng mới {table_name} với Monthly Partitioning & Clustering..."
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
            f"⚡ [BigQuery] Bắt đầu đồng bộ lịch sử điều chỉnh cho mã {symbol_upper}..."
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
                f"⚡ [BigQuery] [Step 1] Nạp file Parquet từ GCS vào bảng Staging: {staging_table_name}..."
            )
            load_job: bigquery.LoadJob = self.bq_client.load_table_from_uri(
                gcs_uri, staging_table_ref, job_config=job_config
            )
            load_job.result()
            self.logger.info(
                f"✅ [BigQuery] Nạp thành công vào bảng tạm {staging_table_name} "
                f"({load_job.output_rows} dòng)."
            )

            # 1.5. Xác định ngày giao dịch nhỏ nhất để làm mốc cắt tỉa phân vùng tĩnh
            if min_date is None:
                self.logger.info(
                    f"⚡ [BigQuery] Đang xác định ngày giao dịch nhỏ nhất từ bảng tạm {staging_table_name}..."
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
                    f"ℹ️ [BigQuery] Ngày giao dịch nhỏ nhất từ staging: {min_date}"
                )
            else:
                self.logger.info(
                    f"ℹ️ [BigQuery] Sử dụng ngày giao dịch nhỏ nhất truyền từ RAM: {min_date}"
                )

            # 2. Chạy giao dịch SQL để xóa dữ liệu cũ (có phân vùng tĩnh) và chèn dữ liệu mới từ bảng tạm
            self.logger.info(
                f"⚡ [BigQuery] [Step 2] Thực thi giao dịch SQL đồng bộ sang bảng chính {Config.BQ_ADJ_TABLE}..."
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
                f"✅ [BigQuery] Đồng bộ hoàn tất giao dịch SQL cho mã {symbol_upper}."
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
                    f"🧹 [BigQuery] [Step 3] Dọn dẹp bảng tạm {staging_table_name}..."
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
            blob_key: str = f"{Config.GCS_PARQUET_PREFIX}/adj/reloaded/{sym}.parquet"
            if self.bucket.blob(blob_key).exists():
                valid_symbols.append(sym)
                valid_gcs_uris.append(f"gs://{Config.GCS_BUCKET_NAME}/{blob_key}")
            else:
                self.logger.warning(
                    f"⚠️ [BigQuery] Bỏ qua mã {sym} do không tìm thấy file Parquet trên GCS: "
                    f"gs://{Config.GCS_BUCKET_NAME}/{blob_key}"
                )

        if not valid_gcs_uris:
            self.logger.warning(
                "⚠️ [BigQuery] Không có mã nào có file Parquet hợp lệ trên GCS. Hủy đồng bộ."
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
                f"⚡ [BigQuery] [Step 1] Nạp các file Parquet từ GCS vào bảng Staging: {staging_table_name}..."
            )
            load_job: bigquery.LoadJob = self.bq_client.load_table_from_uri(
                valid_gcs_uris, staging_table_ref, job_config=job_config
            )
            load_job.result()
            self.logger.info(
                f"✅ [BigQuery] Nạp thành công vào bảng tạm {staging_table_name} "
                f"({load_job.output_rows} dòng)."
            )

            # 1.5. Xác định ngày giao dịch nhỏ nhất để làm mốc cắt tỉa phân vùng tĩnh
            self.logger.info(
                "⚡ [BigQuery] Đang xác định ngày giao dịch nhỏ nhất từ bảng tạm..."
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
                f"ℹ️ [BigQuery] Ngày giao dịch nhỏ nhất từ staging: {min_date}"
            )

            # 2. Chạy giao dịch SQL để xóa dữ liệu cũ (có phân vùng tĩnh) và chèn dữ liệu mới từ bảng tạm
            self.logger.info(
                f"⚡ [BigQuery] [Step 2] Thực thi giao dịch SQL đồng bộ sang bảng chính {Config.BQ_ADJ_TABLE}..."
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
                f"✅ [BigQuery] Đồng bộ hoàn tất giao dịch SQL gộp cho {len(valid_symbols)} mã."
            )

        except Exception as e:
            self.logger.error(f"❌ [BigQuery] Lỗi đồng bộ gộp lịch sử điều chỉnh: {e}")
            raise e
        finally:
            # 3. Dọn dẹp bảng Staging kể cả khi chạy thành công hay thất bại
            try:
                self.logger.info(
                    f"🧹 [BigQuery] [Step 3] Dọn dẹp bảng tạm {staging_table_name}..."
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
        df: pl.DataFrame,
        active_symbols: set[str] | None = None,
        pending_adjusted_reloads: list[str] | None = None,
    ) -> None:
        """Trích xuất và cập nhật trạng thái thị trường EOD (Snapshot) trực tiếp lên GCS.

        Args:
            df (pl.DataFrame): DataFrame dữ liệu tổng hợp của ngày chạy hiện tại.
            active_symbols (Optional[set[str]]): Danh sách các mã cổ phiếu đang niêm yết thực tế trên thị trường.
            pending_adjusted_reloads (Optional[list[str]]): Danh sách các mã lỗi cần chạy lại lịch sử điều chỉnh lần sau.
        """
        if df is None or df.is_empty():
            return

        self.logger.info("⚡ [Snapshot] Đang trích xuất trạng thái thị trường EOD...")

        # Xây dựng cấu trúc JSON snapshot qua phương thức dùng chung trên BaseStorage
        old_checkpoint: dict[str, Any] = self.read_checkpoint()
        final_json_structure: dict[str, Any] = self._build_eod_snapshot(
            df=df,
            active_symbols=active_symbols,
            pending_adjusted_reloads=pending_adjusted_reloads,
            old_checkpoint=old_checkpoint,
        )
        final_snapshots: dict[str, Any] = final_json_structure["snapshots"]

        # Upload JSON trực tiếp lên GCS
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
                f"☁️ [Snapshot Thành Công] 💾 Đã cập nhật tổng cộng {len(final_snapshots)} mã "
                f"tại GCS: gs://{Config.GCS_BUCKET_NAME}/{Config.GCS_CHECKPOINT_KEY}"
            )
        except Exception as e:
            self.logger.error(
                f"🛑 [GCS] Ghi tệp snapshot trạng thái lên GCS thất bại: {e}"
            )
        finally:
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

    def save_corporate_events(self, events: list[dict[str, Any]]) -> None:
        """Ghi nhận sự kiện doanh nghiệp (chia tách, cổ tức...) lên BigQuery.

        Args:
            events (list[dict[str, Any]]): Danh sách các sự kiện doanh nghiệp.
        """
        if not events:
            return

        self.logger.info(
            f"📥 [BigQuery] Đang lưu {len(events)} sự kiện doanh nghiệp..."
        )
        target_table_name = "corporate_events"
        target_table_ref = (
            f"{self.bq_client.project}.{Config.BQ_DATASET}.{target_table_name}"
        )

        # 1. Đảm bảo bảng đích đã tồn tại (khởi tạo schema nếu chưa có)
        try:
            self.bq_client.get_table(target_table_ref)
        except Exception:
            self.logger.info(f"⚡ [BigQuery] Khởi tạo bảng mới: {target_table_name}")
            schema = [
                bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("ex_date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("record_date", "DATE"),
                bigquery.SchemaField("ratio", "STRING"),
            ]
            table = bigquery.Table(target_table_ref, schema=schema)
            self.bq_client.create_table(table)

        # 2. Đẩy dữ liệu vào bảng tạm (Staging)
        run_id = uuid.uuid4().hex[:8]
        staging_table_name = f"corporate_events_staging_{run_id}"
        staging_table_ref = (
            f"{self.bq_client.project}.{Config.BQ_DATASET}.{staging_table_name}"
        )

        try:
            # Chuyển đổi events thành Polars DataFrame rồi sang Parquet bytes
            df_events = pl.DataFrame(events)

            # Ép kiểu Date chặt chẽ trước khi đẩy lên BigQuery
            if "ex_date" in df_events.columns:
                df_events = df_events.with_columns(pl.col("ex_date").cast(pl.Date))
            if "record_date" in df_events.columns:
                df_events = df_events.with_columns(pl.col("record_date").cast(pl.Date))

            bio = io.BytesIO()
            df_events.write_parquet(bio, compression="snappy")
            bio.seek(0)

            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                source_format=bigquery.SourceFormat.PARQUET,
            )
            load_job = self.bq_client.load_table_from_file(
                bio, staging_table_ref, job_config=job_config
            )
            load_job.result()

            # 3. MERGE để chống trùng lặp dựa trên khóa phức hợp (symbol, event_type, ex_date)
            merge_query = f"""
            MERGE `{target_table_ref}` T
            USING `{staging_table_ref}` S
            ON T.symbol = S.symbol AND T.event_type = S.event_type AND T.ex_date = S.ex_date
            WHEN MATCHED THEN
              UPDATE SET
                record_date = COALESCE(S.record_date, T.record_date),
                ratio = COALESCE(S.ratio, T.ratio)
            WHEN NOT MATCHED THEN
              INSERT (symbol, event_type, ex_date, record_date, ratio)
              VALUES (S.symbol, S.event_type, S.ex_date, S.record_date, S.ratio)
            """
            query_job = self.bq_client.query(merge_query)
            query_job.result()

            self.logger.info("💾 [BigQuery] Lưu sự kiện doanh nghiệp thành công.")
        except Exception as e:
            self.logger.error(
                f"  [BigQuery] Lỗi khi lưu sự kiện doanh nghiệp: {e}", exc_info=True
            )
        finally:
            # 4. Dọn dẹp bảng tạm
            try:
                self.bq_client.delete_table(staging_table_ref, not_found_ok=True)
            except Exception as clean_err:
                self.logger.warning(
                    f"  [BigQuery] Không thể dọn bảng tạm {staging_table_name}: {clean_err}"
                )

    def get_state(self, key: str) -> Any:
        """Đọc trạng thái tùy ý từ tệp JSON checkpoints/state.json trên GCS.

        Args:
            key (str): Khóa của trạng thái cần đọc.

        Returns:
            Any: Giá trị của trạng thái, hoặc None nếu không tồn tại hoặc lỗi.
        """
        try:
            blob = self.bucket.blob("checkpoints/state.json")
            if blob.exists():
                json_str = blob.download_as_text(encoding="utf-8")
                data = json.loads(json_str)
                return data.get(key)
        except Exception as e:
            self.logger.warning(f"⚠️ [GCS] Lỗi khi đọc trạng thái '{key}' từ GCS: {e}")
        return None

    def save_state(self, key: str, value: Any) -> None:
        """Lưu trạng thái tùy ý vào tệp JSON checkpoints/state.json trên GCS.

        Sử dụng cơ chế Optimistic Concurrency Control (Optimistic Locking) thông qua
        generation của GCS và retry logic với exponential backoff để ngăn ngừa triệt để
        rủi ro Race Condition khi có nhiều container chạy đồng thời.

        Args:
            key (str): Khóa của trạng thái cần lưu.
            value (Any): Giá trị của trạng thái cần lưu.
        """
        max_retries: int = 5
        backoff_delay: float = 0.5

        for attempt in range(1, max_retries + 1):
            try:
                blob = self.bucket.blob("checkpoints/state.json")
                data = {}
                generation = 0

                # Tải metadata mới nhất của blob bao gồm generation hiện tại
                try:
                    blob.reload()
                    if blob.exists():
                        generation = blob.generation
                        json_str = blob.download_as_text(encoding="utf-8")
                        data = json.loads(json_str)
                except google.api_core.exceptions.NotFound:
                    # Nếu file chưa tồn tại, generation match = 0 đảm bảo chỉ tạo file nếu không ai tạo trước đó
                    generation = 0
                except Exception:
                    pass

                data[key] = value

                # Upload với điều kiện generation khớp
                blob.upload_from_string(
                    json.dumps(data, ensure_ascii=False, indent=2),
                    content_type="application/json",
                    if_generation_match=generation,
                )
                return
            except google.api_core.exceptions.PreconditionFailed:
                # Lỗi 412: Có tiến trình khác đã cập nhật file state.json trước đó
                if attempt == max_retries:
                    self.logger.error(
                        f"🛑 [GCS] Gặp xung đột Race Condition liên tục khi lưu trạng thái '{key}'. "
                        f"Đã thử lại {max_retries} lần nhưng vẫn thất bại."
                    )
                    raise
                self.logger.warning(
                    f"⚠️ [GCS] Phát hiện xung đột Race Condition khi lưu trạng thái '{key}' (lần {attempt}/{max_retries}). "
                    f"Đang thử lại sau {backoff_delay * attempt}s..."
                )
                time.sleep(backoff_delay * attempt)
            except Exception as e:
                self.logger.error(
                    f"❌ [GCS] Lỗi khi lưu trạng thái '{key}' lên GCS: {e}"
                )
                raise e

    def save_icb_industries(self, df_icb: pl.DataFrame) -> None:
        """Ghi nhận danh mục ngành ICB lên BigQuery (Ghi đè hoàn toàn).

        Args:
            df_icb (pl.DataFrame): DataFrame chứa thông tin danh mục phân loại ngành ICB.
        """
        if df_icb is None or df_icb.is_empty():
            return
        self.logger.info(f"💾 [BigQuery] Đang lưu {len(df_icb)} ngành ICB...")
        table_ref = f"{self.bq_client.project}.{Config.BQ_DATASET}.icb_industries"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            source_format=bigquery.SourceFormat.PARQUET,
        )
        try:
            # Ghi trực tiếp từ Polars sang Parquet trong RAM
            bio = io.BytesIO()
            df_icb.write_parquet(bio, compression="snappy")
            bio.seek(0)

            job = self.bq_client.load_table_from_file(
                bio, table_ref, job_config=job_config
            )
            job.result()
            self.logger.info("🎉 [BigQuery] Ghi danh mục ngành ICB thành công.")
        except Exception as e:
            self.logger.error(f"❌ [BigQuery] Gặp lỗi khi lưu danh mục ngành ICB: {e}")
            raise e

    def save_companies(self, df_companies: pl.DataFrame) -> None:
        """Ghi nhận danh sách công ty lên BigQuery.

        Args:
            df_companies (pl.DataFrame): DataFrame chứa thông tin danh sách các công ty.
        """
        if df_companies is None or df_companies.is_empty():
            return

        self.logger.info(
            f"📥 [BigQuery] Đang đồng bộ {len(df_companies)} thông tin công ty..."
        )
        target_table_name = "companies"
        target_table_ref = (
            f"{self.bq_client.project}.{Config.BQ_DATASET}.{target_table_name}"
        )

        run_id = uuid.uuid4().hex[:8]
        staging_table_name = f"companies_staging_{run_id}"
        staging_table_ref = (
            f"{self.bq_client.project}.{Config.BQ_DATASET}.{staging_table_name}"
        )

        # 1. Đảm bảo bảng đích đã tồn tại (nếu chưa có thì tạo mới)
        try:
            self.bq_client.get_table(target_table_ref)
        except Exception:
            self.logger.info(
                f"🔍 [BigQuery] Không tìm thấy bảng {target_table_name}, đang tạo mới..."
            )
            schema = [
                bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("exchange", "STRING"),
                bigquery.SchemaField("company_name", "STRING"),
                bigquery.SchemaField("icb_code", "STRING"),
                bigquery.SchemaField("com_type_code", "STRING"),
                bigquery.SchemaField("type", "STRING"),
                bigquery.SchemaField("status", "STRING"),
            ]
            table = bigquery.Table(target_table_ref, schema=schema)
            self.bq_client.create_table(table)

        # 2. Đẩy dữ liệu mới vào bảng tạm (Staging)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            source_format=bigquery.SourceFormat.PARQUET,
        )
        try:
            # Ghi trực tiếp từ Polars sang Parquet trong RAM
            bio = io.BytesIO()
            df_companies.write_parquet(bio, compression="snappy")
            bio.seek(0)

            load_job = self.bq_client.load_table_from_file(
                bio, staging_table_ref, job_config=job_config
            )
            load_job.result()

            # 3. Thực thi câu lệnh MERGE để cập nhật dữ liệu
            merge_query = f"""
            MERGE `{target_table_ref}` T
            USING `{staging_table_ref}` S
            ON T.symbol = S.symbol
            WHEN MATCHED THEN
              UPDATE SET
                exchange = COALESCE(S.exchange, T.exchange),
                company_name = COALESCE(S.company_name, T.company_name),
                icb_code = COALESCE(S.icb_code, T.icb_code),
                com_type_code = COALESCE(S.com_type_code, T.com_type_code),
                type = COALESCE(S.type, T.type),
                status = 'active'
            WHEN NOT MATCHED THEN
              INSERT (symbol, exchange, company_name, icb_code, com_type_code, type, status)
              VALUES (S.symbol, S.exchange, S.company_name, S.icb_code, S.com_type_code, S.type, S.status)
            WHEN NOT MATCHED BY SOURCE THEN
              UPDATE SET status = 'delisted'
            """
            query_job = self.bq_client.query(merge_query)
            query_job.result()

            self.logger.info("🎉 [BigQuery] Đồng bộ thông tin công ty thành công.")
        except Exception as e:
            self.logger.error(f"❌ [BigQuery] Gặp lỗi khi lưu thông tin công ty: {e}")
            raise e
        finally:
            # 4. Dọn dẹp bảng tạm
            try:
                self.bq_client.delete_table(staging_table_ref, not_found_ok=True)
            except Exception as clean_err:
                self.logger.warning(
                    f"❌ [BigQuery] Không thể dọn bảng tạm {staging_table_name}: {clean_err}"
                )
