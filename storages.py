import gc
import io
import json
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Set, Union
import numpy as np
import pandas as pd
from google.cloud import storage, bigquery

from config import Config


class CustomJSONEncoder(json.JSONEncoder):
    """Bộ mã hóa JSON tùy chỉnh để xử lý an toàn các kiểu dữ liệu NumPy và Pandas."""

    def default(self, obj: Any) -> Any:
        """Kiểm tra và ánh xạ kiểu dữ liệu tùy chỉnh về chuẩn JSON.

        Args:
            obj: Đối tượng cần mã hóa.

        Returns:
            Đối tượng đã chuẩn hóa tương thích với JSON.
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


class Storage:
    """Chuyên trách việc lưu trữ dữ liệu an toàn ra Google Cloud Storage (GCS) và BigQuery (BQ)."""

    def __init__(self, logger: logging.Logger) -> None:
        """Khởi tạo bộ lưu trữ dữ liệu và kết nối đến GCS & BigQuery.

        Args:
            logger: Đối tượng Logger dùng để ghi nhận tiến trình.

        Raises:
            Exception: Phát sinh khi không thể thiết lập kết nối đến dịch vụ GCP.
        """
        self.logger: logging.Logger = logger
        try:
            self.client: storage.Client = storage.Client()
            self.bq_client: bigquery.Client = bigquery.Client()
            self.bucket: storage.Bucket = self.client.bucket(Config.GCS_BUCKET_NAME)
            self.logger.info("☁️ [GCS] Kết nối thành công bằng Application Default Credentials (ADC).")
            self.logger.info(f"☁️ [GCS] Bucket: {Config.GCS_BUCKET_NAME}")
            self.logger.info(f"📊 [BigQuery] Dự án: {self.bq_client.project}")
        except Exception as e:
            self.logger.error(f"🛑 [Storage] Lỗi khởi tạo kết nối Cloud Services qua ADC: {e}")
            raise e

    def save_parquet(
        self,
        df: pd.DataFrame,
        date_ref: datetime,
        suffix: str = "raw",
        partition: bool = False
    ) -> Optional[str]:
        """Ghi dữ liệu nén Parquet trực tiếp lên Google Cloud Storage (GCS).

        Args:
            df: DataFrame dữ liệu chứng khoán cần lưu.
            date_ref: Mốc thời gian mặc định của tệp dữ liệu.
            suffix: Hậu tố định danh loại dữ liệu ('raw' hoặc 'adj').
            partition: True để lưu phân mảnh theo năm/tháng, False để lưu file gộp tĩnh.

        Returns:
            Đường dẫn GCS của file nếu lưu thành công, ngược lại là None.

        Raises:
            Exception: Phát sinh khi ghi dữ liệu lên GCS thất bại.
        """
        if df is None or df.empty:
            return None

        # Tối ưu: Lấy ngày thực tế lớn nhất từ cột trading_date để đặt tên thư mục/file nếu có
        if "trading_date" in df.columns:
            max_date = pd.to_datetime(df["trading_date"]).max()
            if not pd.isna(max_date):
                date_ref = max_date

        if partition:
            year_str: str = date_ref.strftime("%Y")
            month_str: str = date_ref.strftime("%m")
            date_str: str = date_ref.strftime("%Y%m%d")
            gcs_path: str = f"{Config.GCS_PARQUET_PREFIX}/{suffix}/year={year_str}/month={month_str}/daily_{date_str}.parquet"
        else:
            gcs_path = f"{Config.GCS_PARQUET_PREFIX}/{suffix}/cafef_historical_all.parquet"

        try:
            self.logger.info(f"💾 ☁️ [GCS] Đang ghi dữ liệu nén Parquet: gs://{Config.GCS_BUCKET_NAME}/{gcs_path}")

            bio: io.BytesIO = io.BytesIO()
            df_write: pd.DataFrame = df.copy()

            # Ép kiểu trading_date về Date thuần túy để khớp hoàn hảo với kiểu DATE của BigQuery
            if "trading_date" in df_write.columns:
                df_write["trading_date"] = pd.to_datetime(df_write["trading_date"]).dt.date
            for col in ["symbol", "exchange"]:
                if col in df_write.columns:
                    df_write[col] = df_write[col].astype(str)

            df_write.to_parquet(
                bio,
                compression="snappy",
                index=False,
                coerce_timestamps="us",
                allow_truncated_timestamps=True
            )
            bio.seek(0)

            blob: storage.Blob = self.bucket.blob(gcs_path)
            blob.upload_from_file(bio, content_type="application/octet-stream")

            self.logger.info(f"🎉 ☁️ [GCS] File lưu trữ thành công tại GCS: gs://{Config.GCS_BUCKET_NAME}/{gcs_path}")
            return gcs_path
        except Exception as e:
            self.logger.error(f"❌ ☁️ [GCS] Lỗi trong quá trình ghi file Parquet lên GCS: {e}")
            raise e

    def save_symbol_history(
        self,
        df: pd.DataFrame,
        symbol: str,
        suffix: str = "adj"
    ) -> None:
        """Ghi toàn bộ lịch sử giá của một mã cổ phiếu cụ thể ra file Parquet riêng biệt trên GCS.

        Args:
            df: DataFrame dữ liệu lịch sử đầy đủ của mã cổ phiếu.
            symbol: Mã cổ phiếu cần lưu (ví dụ: FPT).
            suffix: Tiền tố thư mục lưu trữ ('raw' hoặc 'adj').

        Raises:
            Exception: Phát sinh khi ghi dữ liệu lên GCS thất bại.
        """
        if df is None or df.empty:
            return

        gcs_path: str = f"{Config.GCS_PARQUET_PREFIX}/{suffix}/reloaded/{symbol.upper()}.parquet"

        try:
            self.logger.info(f"💾 ☁️ [GCS] Đang ghi dữ liệu lịch sử cho mã {symbol.upper()} tại: gs://{Config.GCS_BUCKET_NAME}/{gcs_path}")

            bio: io.BytesIO = io.BytesIO()
            df_write: pd.DataFrame = df.copy()

            if "trading_date" in df_write.columns:
                df_write["trading_date"] = pd.to_datetime(df_write["trading_date"]).dt.date
            for col in ["symbol", "exchange"]:
                if col in df_write.columns:
                    df_write[col] = df_write[col].astype(str)

            df_write.to_parquet(
                bio,
                compression="snappy",
                index=False,
                coerce_timestamps="us",
                allow_truncated_timestamps=True
            )
            bio.seek(0)

            blob: storage.Blob = self.bucket.blob(gcs_path)
            blob.upload_from_file(bio, content_type="application/octet-stream")

            self.logger.info(f"🎉 ☁️ [GCS] File lịch sử mã {symbol.upper()} lưu trữ thành công tại GCS: gs://{Config.GCS_BUCKET_NAME}/{gcs_path}")
        except Exception as e:
            self.logger.error(f"❌ ☁️ [GCS] Lỗi khi ghi file lịch sử cho mã {symbol.upper()} lên GCS: {e}")
            raise e

    def _ensure_table_exists(self, table_name: str) -> None:
        """Kiểm tra và tự động khởi tạo Dataset/Table BigQuery với phân vùng Month và Cluster nếu chưa tồn tại.

        Args:
            table_name: Tên bảng BigQuery cần kiểm tra/khởi tạo.
        """
        dataset_ref: bigquery.DatasetReference = self.bq_client.dataset(Config.BQ_DATASET)
        try:
            self.bq_client.get_dataset(dataset_ref)
        except Exception:
            self.logger.info(f"✨ [BigQuery] Đang khởi tạo dataset mới: {Config.BQ_DATASET}")
            self.bq_client.create_dataset(bigquery.Dataset(dataset_ref))

        table_ref: str = f"{self.bq_client.project}.{Config.BQ_DATASET}.{table_name}"
        try:
            self.bq_client.get_table(table_ref)
        except Exception:
            self.logger.info(f"✨ [BigQuery] Đang tạo bảng mới {table_name} với Monthly Partitioning & Clustering...")
            schema: List[bigquery.SchemaField] = [
                bigquery.SchemaField("symbol", "STRING"),
                bigquery.SchemaField("trading_date", "DATE"),
                bigquery.SchemaField("open_price", "FLOAT64"),
                bigquery.SchemaField("high_price", "FLOAT64"),
                bigquery.SchemaField("low_price", "FLOAT64"),
                bigquery.SchemaField("close_price", "FLOAT64"),
                bigquery.SchemaField("total_volume", "INTEGER"),
                bigquery.SchemaField("exchange", "STRING"),
            ]
            table: bigquery.Table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field="trading_date"
            )
            table.clustering_fields = ["symbol", "exchange"]
            self.bq_client.create_table(table)

    def sync_adjusted_symbol_to_bigquery(self, symbol: str) -> None:
        """Đồng bộ lịch sử điều chỉnh của một mã chứng khoán duy nhất lên BigQuery qua Staging Table.

        Sử dụng cơ chế Staging Table để đảm bảo tính an toàn dữ liệu (nếu nạp file từ GCS lỗi
        sẽ không ảnh học bảng chính) và tối ưu hóa DML Delete bằng bộ lọc phân vùng động.

        Args:
            symbol: Mã chứng khoán cần đồng bộ.

        Raises:
            Exception: Phát sinh khi load job hoặc giao dịch SQL đồng bộ gặp lỗi.
        """
        symbol_upper: str = symbol.upper()
        gcs_uri: str = f"gs://{Config.GCS_BUCKET_NAME}/{Config.GCS_PARQUET_PREFIX}/adj/reloaded/{symbol_upper}.parquet"
        self.logger.info(f"⚡ [BigQuery] Bắt đầu đồng bộ lịch sử điều chỉnh (Staging Mode) cho mã {symbol_upper}...")

        target_table_ref: str = f"{self.bq_client.project}.{Config.BQ_DATASET}.{Config.BQ_ADJ_TABLE}"
        staging_table_name: str = f"{Config.BQ_ADJ_TABLE}_staging_{symbol_upper}"
        staging_table_ref: str = f"{self.bq_client.project}.{Config.BQ_DATASET}.{staging_table_name}"

        # Đảm bảo bảng đích đã sẵn sàng hoạt động
        self._ensure_table_exists(Config.BQ_ADJ_TABLE)

        # 1. Nạp dữ liệu từ GCS vào bảng Staging (Tạm thời)
        job_config: bigquery.LoadJobConfig = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE",  # Đảm bảo ghi đè bảng tạm cũ nếu tồn tại
        )

        try:
            self.logger.info(f"⚡ [BigQuery] Bước 1: Nạp file Parquet từ GCS vào bảng Staging: {staging_table_name}...")
            load_job: bigquery.LoadJob = self.bq_client.load_table_from_uri(
                gcs_uri, staging_table_ref, job_config=job_config
            )
            load_job.result()
            self.logger.info(f"🎉 [BigQuery] Nạp thành công vào bảng tạm {staging_table_name}. Đã nạp {load_job.output_rows} dòng.")

            # 2. Chạy giao dịch SQL để xóa dữ liệu cũ (có phân vùng) và chèn dữ liệu mới từ bảng tạm
            self.logger.info(f"⚡ [BigQuery] Bước 2: Thực thi giao dịch SQL đồng bộ sang bảng chính {Config.BQ_ADJ_TABLE}...")
            sync_query: str = f"""
            DECLARE min_date DATE;
            SET min_date = (SELECT MIN(trading_date) FROM `{staging_table_ref}`);
            
            BEGIN TRANSACTION;
            
            # Xóa lịch sử cũ từ ngày nhỏ nhất của dữ liệu mới để pruning partitions
            DELETE FROM `{target_table_ref}`
            WHERE symbol = '{symbol_upper}'
              AND trading_date >= min_date;
              
            # Sao chép toàn bộ dữ liệu từ bảng tạm sang bảng chính
            INSERT INTO `{target_table_ref}` (symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange)
            SELECT symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange
            FROM `{staging_table_ref}`;
            
            COMMIT TRANSACTION;
            """
            query_job: bigquery.QueryJob = self.bq_client.query(sync_query)
            query_job.result()
            self.logger.info(f"🎉 [BigQuery] Đồng bộ hoàn tất giao dịch SQL cho mã {symbol_upper}.")

        except Exception as e:
            self.logger.error(f"❌ [BigQuery] Lỗi đồng bộ lịch sử điều chỉnh cho mã {symbol_upper}: {e}")
            raise e
        finally:
            # 3. Dọn dẹp bảng Staging kể cả khi chạy thành công hay thất bại
            try:
                self.logger.info(f"🧹 [BigQuery] Bước 3: Dọn dẹp dứt điểm bảng tạm {staging_table_name}...")
                self.bq_client.delete_table(staging_table_ref, not_found_ok=True)
            except Exception as clean_err:
                self.logger.warning(f"⚠️ [BigQuery] Không thể xóa bảng tạm {staging_table_name}: {clean_err}")

    def load_parquet_to_bigquery(
        self,
        gcs_path: str,
        table_name: str,
        write_disposition: str = "WRITE_APPEND"
    ) -> None:
        """Nạp trực tiếp tệp Parquet từ GCS vào BigQuery.

        Args:
            gcs_path: Đường dẫn lưu trữ tương đối trên GCS bucket.
            table_name: Tên bảng đích BigQuery.
            write_disposition: Chế độ ghi bảng ('WRITE_APPEND' hoặc 'WRITE_TRUNCATE').

        Raises:
            Exception: Phát sinh khi load job thất bại.
        """
        gcs_uri: str = f"gs://{Config.GCS_BUCKET_NAME}/{gcs_path}"
        table_ref: str = f"{self.bq_client.project}.{Config.BQ_DATASET}.{table_name}"

        self.logger.info(f"⚡ [BigQuery] Đang nạp dữ liệu từ {gcs_uri} vào {table_ref} ({write_disposition})...")

        self._ensure_table_exists(table_name)

        job_config: bigquery.LoadJobConfig = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
        )

        try:
            load_job: bigquery.LoadJob = self.bq_client.load_table_from_uri(
                gcs_uri,
                table_ref,
                job_config=job_config
            )
            load_job.result()
            self.logger.info(f"🎉 [BigQuery] Nạp thành công vào {table_name}. Đã chèn {load_job.output_rows} dòng.")
        except Exception as e:
            self.logger.error(f"❌ [BigQuery] Lỗi khi nạp dữ liệu từ {gcs_path} vào {table_name}: {e}")
            raise e

    def sync_daily_adjusted_prices(
        self,
        dates: List[Union[datetime, date]],
        excluded_symbols: List[str]
    ) -> None:
        """Đồng bộ hóa hàng loạt dữ liệu từ raw_price sang adjusted_price cho danh sách các ngày.

        Loại bỏ các mã có sự kiện điều chỉnh giá (đã được tải lại toàn bộ lịch sử riêng biệt)
        để tránh ghi đè dữ liệu lịch sử điều chỉnh chính xác.

        Args:
            dates: Danh sách các ngày cần đồng bộ.
            excluded_symbols: Các mã chứng khoán có sự kiện chia tách/cổ tức (cần loại trừ).

        Raises:
            Exception: Phát sinh khi truy vấn DML trong giao dịch BigQuery gặp lỗi.
        """
        if not dates:
            return

        date_strings: List[str] = [
            d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in dates
        ]
        formatted_dates: str = ", ".join([f"'{d_str}'" for d_str in date_strings])

        raw_table_ref: str = f"`{self.bq_client.project}.{Config.BQ_DATASET}.{Config.BQ_RAW_TABLE}`"
        adj_table_ref: str = f"`{self.bq_client.project}.{Config.BQ_DATASET}.{Config.BQ_ADJ_TABLE}`"

        self.logger.info(f"⚡ [BigQuery] Đang sao chép giá từ raw sang adjusted cho các ngày: {date_strings}...")

        exclude_clause: str = ""
        if excluded_symbols:
            formatted_symbols: str = ", ".join([f"'{s.upper()}'" for s in excluded_symbols])
            exclude_clause = f"AND symbol NOT IN ({formatted_symbols})"

        query: str = f"""
        BEGIN TRANSACTION;
        
        # Xóa dữ liệu cũ nếu đã tồn tại để tránh trùng lặp dữ liệu khi chạy lại
        DELETE FROM {adj_table_ref}
        WHERE trading_date IN ({formatted_dates}) {exclude_clause};
        
        # Chèn giá thô ngày T từ raw_price sang adjusted_price đối với các mã bình thường
        INSERT INTO {adj_table_ref}
          (symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange)
        SELECT 
          symbol, 
          trading_date, 
          open_price, 
          high_price, 
          low_price, 
          close_price, 
          total_volume, 
          exchange
        FROM {raw_table_ref}
        WHERE trading_date IN ({formatted_dates}) {exclude_clause};
        
        COMMIT TRANSACTION;
        """

        try:
            query_job: bigquery.QueryJob = self.bq_client.query(query)
            query_job.result()
            self.logger.info(f"🎉 [BigQuery] Hoàn tất đồng bộ adjusted_price cho {len(date_strings)} ngày.")
        except Exception as e:
            self.logger.error(f"❌ [BigQuery] Lỗi đồng bộ adjusted_price số lượng lớn ngày: {e}")
            raise e

    def delete_by_date(self, table_name: str, date_ref: Union[datetime, date, str]) -> None:
        """Xóa toàn bộ bản ghi của một ngày cụ thể trong bảng BigQuery.

        Giúp đảm bảo tính idempotent của pipeline (chạy lại nhiều lần không sinh bản ghi thừa).

        Args:
            table_name: Tên bảng đích BigQuery.
            date_ref: Ngày giao dịch cần xóa.

        Raises:
            Exception: Phát sinh khi câu lệnh DELETE bị lỗi.
        """
        date_str: str = date_ref.strftime("%Y-%m-%d") if hasattr(date_ref, "strftime") else str(date_ref)
        table_ref: str = f"`{self.bq_client.project}.{Config.BQ_DATASET}.{table_name}`"

        self.logger.info(f"🗑️ [BigQuery] Đang xóa dữ liệu cũ ngày {date_str} từ bảng {table_ref}...")

        query: str = f"DELETE FROM {table_ref} WHERE trading_date = '{date_str}'"
        try:
            query_job: bigquery.QueryJob = self.bq_client.query(query)
            query_job.result()
            self.logger.info(f"🎉 [BigQuery] Đã dọn dẹp xong dữ liệu ngày {date_str}.")
        except Exception as e:
            self.logger.error(f"❌ [BigQuery] Lỗi khi dọn dẹp dữ liệu ngày {date_str}: {e}")
            raise e

    def read_checkpoint(self) -> Dict[str, Any]:
        """Đọc tệp snapshot thị trường EOD được lưu trữ trước đó trên GCS.

        Returns:
            Dict chứa thông tin metadata và trạng thái snapshots của các mã. Trả về dict rỗng nếu không tồn tại.
        """
        blob: storage.Blob = self.bucket.blob(Config.GCS_CHECKPOINT_KEY)
        if blob.exists():
            try:
                json_str: str = blob.download_as_text(encoding="utf-8")
                result: Dict[str, Any] = json.loads(json_str)
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
        active_symbols: Optional[Set[str]] = None,
        pending_adjusted_reloads: Optional[List[str]] = None
    ) -> None:
        """Trích xuất và cập nhật trạng thái thị trường EOD (Snapshot) trực tiếp lên GCS.

        Args:
            df: DataFrame dữ liệu tổng hợp của ngày chạy hiện tại.
            active_symbols: Danh sách các mã cổ phiếu đang niêm yết thực tế trên thị trường.
            pending_adjusted_reloads: Danh sách các mã lỗi cần chạy lại lịch sử điều chỉnh lần sau.
        """
        if df is None or df.empty:
            return

        self.logger.info("⚡ [Snapshot] Đang trích xuất trạng thái thị trường EOD...")

        # 1. Lọc lấy bản ghi mới nhất của ngày hôm nay cho từng mã
        df_latest: pd.DataFrame = df.drop_duplicates(subset=["symbol"], keep="last").copy()

        # Tính toán giá trung bình nhanh chóng bằng toán tử cột
        price_cols: List[str] = ["open_price", "high_price", "low_price", "close_price"]
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
        if max_date_str < today_str:
            is_eod: bool = True
        else:
            # Thị trường chứng khoán Việt Nam chốt phiên lúc 15:00 và dữ liệu hoàn tất sau 15:15
            is_eod = vn_now.hour > 15 or (vn_now.hour == 15 and vn_now.minute >= 15)

        # Đảm bảo index symbol là chuỗi thông thường (không phải categorical) để xuất dict sạch sẽ
        if isinstance(df_latest["symbol"].dtype, pd.CategoricalDtype):
            df_latest["symbol"] = df_latest["symbol"].astype(str)
        df_latest.set_index("symbol", inplace=True)

        # Chỉ lấy các cột cần thiết, ép kiểu chuẩn về dict nguyên bản của Python để gom JSON
        cols_to_extract: List[str] = [
            "exchange", "trading_date", "open_price", "high_price",
            "low_price", "close_price", "average_price", "total_volume"
        ]
        current_data_dict: Dict[str, Dict[str, Any]] = df_latest[cols_to_extract].to_dict(orient="index")

        # 2. Đọc dữ liệu lịch sử cũ từ file checkpoint trên GCS
        old_checkpoint: Dict[str, Any] = self.read_checkpoint()
        merged_snapshots: Dict[str, Dict[str, Any]] = old_checkpoint.get("snapshots", {})
        old_metadata: Dict[str, Any] = old_checkpoint.get("metadata") or {}
        old_pending: List[str] = old_metadata.get("pending_adjusted_reloads") or []

        # Hợp nhất pending_adjusted_reloads cũ nếu không được truyền vào mới
        if pending_adjusted_reloads is None:
            pending_adjusted_reloads = old_pending

        # 3. Tiến hành Hợp nhất (Upsert) - Chỉ cập nhật snapshots khi đã chốt phiên EOD thực tế
        if is_eod:
            for sym, new_row in current_data_dict.items():
                if not sym:
                    continue
                old_row: Optional[Dict[str, Any]] = merged_snapshots.get(sym)
                # Nếu mã chưa có hoặc có ngày mới hơn/bằng ngày cũ -> Cập nhật thông tin mới nhất
                if not old_row or new_row["trading_date"] >= old_row["trading_date"]:
                    merged_snapshots[sym] = new_row
        else:
            self.logger.info(
                "ℹ️ [Snapshot] Đang chạy trong phiên (Chưa chốt EOD). Giữ nguyên dữ liệu snapshots lịch sử từ phiên EOD trước."
            )

        # Chuẩn hóa toàn bộ dữ liệu trong merged_snapshots để dọn dẹp các tàn dư float32 cũ
        for sym, row in merged_snapshots.items():
            for col in price_cols:
                if col in row and isinstance(row[col], (int, float)):
                    row[col] = int(round(float(row[col])))
            if "average_price" in row and isinstance(row["average_price"], (int, float)):
                row["average_price"] = round(float(row["average_price"]), 1)

        # 4. Áp dụng bộ lọc active_symbols & Sắp xếp Alphabet gọn gàng
        final_snapshots: Dict[str, Dict[str, Any]] = {}
        for sym in sorted(merged_snapshots.keys()):
            if active_symbols and sym not in active_symbols:
                continue
            final_snapshots[sym] = merged_snapshots[sym]

        # 5. Cấu trúc JSON cuối cùng
        final_json_structure: Dict[str, Any] = {
            "metadata": {
                "last_successful_run": max_date_str,
                "is_eod": is_eod,
                "total_tickers": len(final_snapshots),
                "pending_adjusted_reloads": pending_adjusted_reloads
            },
            "snapshots": final_snapshots
        }

        # 6. Upload JSON trực tiếp lên GCS
        try:
            json_str: str = json.dumps(final_json_structure, cls=CustomJSONEncoder, ensure_ascii=False, indent=2)
            blob: storage.Blob = self.bucket.blob(Config.GCS_CHECKPOINT_KEY)
            blob.upload_from_string(json_str, content_type="application/json")
            self.logger.info(
                f"💾 ☁️ [Snapshot Thành Công] Đã cập nhật tổng cộng {len(final_snapshots)} mã tại GCS: gs://{Config.GCS_BUCKET_NAME}/{Config.GCS_CHECKPOINT_KEY}"
            )
        except Exception as e:
            self.logger.error(f"🛑 [GCS] Ghi tệp snapshot trạng thái lên GCS thất bại: {e}")
        finally:
            # Giải phóng các cấu trúc dữ liệu lớn thủ công để tối ưu RAM
            del current_data_dict, merged_snapshots, final_snapshots
            gc.collect()
