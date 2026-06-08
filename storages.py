import gc
import io
import json
import logging
import os
import numpy as np
import pandas as pd
import shutil
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from google.cloud import storage

from config import Config


class CustomJSONEncoder(json.JSONEncoder):
    """Bộ mã hóa JSON tùy chỉnh để xử lý an sau các kiểu dữ liệu NumPy và Pandas."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        return super().default(obj)



class Storage:
    """Chuyên trách việc lưu trữ dữ liệu an toàn ra Google Cloud Storage (GCS)."""

    def __init__(self, logger: logging.Logger) -> None:
        """Khởi tạo bộ lưu trữ dữ liệu và kết nối đến GCS.

        Args:
            logger: Đối tượng Logger dùng để ghi nhận tiến trình.
        """
        self.logger: logging.Logger = logger
        try:
            if os.path.exists(Config.GCS_CREDENTIALS_FILE):
                self.client = storage.Client.from_service_account_json(Config.GCS_CREDENTIALS_FILE)
            else:
                self.logger.warning(
                    f"⚠️ Không tìm thấy file credentials tại {Config.GCS_CREDENTIALS_FILE}. "
                    "Sử dụng Default Credentials của hệ thống."
                )
                self.client = storage.Client()
            self.bucket = self.client.bucket(Config.GCS_BUCKET_NAME)
            self.logger.info(f"☁️ [GCS] Kết nối thành công đến bucket: {Config.GCS_BUCKET_NAME}")
        except Exception as e:
            self.logger.error(f"🛑 [GCS] Lỗi khởi tạo kết nối Cloud Storage: {e}")
            raise e

    def save_parquet(
        self,
        df: pd.DataFrame,
        date_ref: datetime,
        suffix: str = "raw",
        partition: bool = False
    ) -> None:
        """Ghi dữ liệu nén Parquet trực tiếp lên Google Cloud Storage (GCS).

        Args:
            df: DataFrame dữ liệu lịch sử cần ghi dữ liệu.
            date_ref: Mốc thời gian của tệp dữ liệu.
            suffix: Hậu tố định danh loại dữ liệu ('raw' hoặc 'adj').
            partition: True để lưu phân mảnh theo năm/tháng, False để lưu file gộp tĩnh.

        Raises:
            Exception: Phát sinh khi ghi dữ liệu lên GCS thất bại.
        """
        if df is None or df.empty:
            return

        # Tối ưu: Lấy ngày thực tế lớn nhất từ cột trading_date để đặt tên thư mục/file nếu có
        if "trading_date" in df.columns:
            max_date = pd.to_datetime(df["trading_date"]).max()
            if not pd.isna(max_date):
                date_ref = max_date

        if partition:
            year_str = date_ref.strftime("%Y")
            month_str = date_ref.strftime("%m")
            date_str = date_ref.strftime("%Y%m%d")
            gcs_path = f"{Config.GCS_PARQUET_PREFIX}/{suffix}/year={year_str}/month={month_str}/daily_{date_str}.parquet"
        else:
            gcs_path = f"{Config.GCS_PARQUET_PREFIX}/{suffix}/cafef_historical_all.parquet"

        try:
            self.logger.info(f"💾 ☁️ [GCS] Đang ghi dữ liệu nén Parquet: gs://{Config.GCS_BUCKET_NAME}/{gcs_path}")
            
            bio = io.BytesIO()
            df.to_parquet(
                bio,
                compression="snappy",
                index=False,
                coerce_timestamps="us",
                allow_truncated_timestamps=True
            )
            bio.seek(0)

            # Khởi tạo blob và upload
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_file(bio, content_type="application/octet-stream")

            self.logger.info(f"🎉 ☁️ [GCS] File lưu trữ thành công tại GCS: gs://{Config.GCS_BUCKET_NAME}/{gcs_path}")
        except Exception as e:
            self.logger.error(f"❌ ☁️ [GCS] Lỗi trong quá trình ghi file Parquet lên GCS: {e}")
            raise e

    def read_checkpoint(self) -> Dict[str, Dict[str, Any]]:
        """Đọc tệp snapshot cũ trực tiếp từ GCS. Trả về dict rỗng nếu không tồn tại."""
        blob = self.bucket.blob(Config.GCS_CHECKPOINT_KEY)
        if blob.exists():
            try:
                json_str = blob.download_as_text(encoding="utf-8")
                return json.loads(json_str)
            except Exception as e:
                self.logger.warning(f"⚠️ [GCS] Không thể đọc file checkpoint từ GCS do lỗi: {e}. Tiến hành khởi tạo mới.")
                return {}
        return {}

    def save_checkpoint(self, df: pd.DataFrame, active_symbols: Optional[Set[str]] = None) -> None:
        """Trích xuất và cập nhật trạng thái thị trường EOD (Snapshot) trực tiếp lên GCS.

        Args:
            df: DataFrame dữ liệu tổng hợp của ngày chạy hiện tại.
            active_symbols: Danh sách các mã cổ phiếu đang niêm yết thực tế trên thị trường (Tùy chọn).
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
        vn_now = datetime.now(Config.VN_TZ)
        today_str = vn_now.strftime("%Y-%m-%d")
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
        cols_to_extract = ["exchange", "trading_date", "open_price", "high_price", "low_price", "close_price", "average_price", "total_volume"]
        current_data_dict: Dict[str, Dict[str, Any]] = df_latest[cols_to_extract].to_dict(orient="index")

        # 2. Đọc dữ liệu lịch sử cũ từ file checkpoint trên GCS
        merged_snapshots: Dict[str, Dict[str, Any]] = self.read_checkpoint().get("snapshots", {})

        # 3. Tiến hành Hợp nhất (Upsert) O(1)
        for sym, new_row in current_data_dict.items():
            if not sym: 
                continue
            old_row = merged_snapshots.get(sym)
            # Nếu mã chưa có hoặc có ngày mới hơn/bằng ngày cũ -> Cập nhật thông tin mới nhất
            if not old_row or new_row["trading_date"] >= old_row["trading_date"]:
                merged_snapshots[sym] = new_row

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
                "total_tickers": len(final_snapshots)
            },
            "snapshots": final_snapshots
        }

        # 6. Upload JSON trực tiếp lên GCS
        try:
            json_str = json.dumps(final_json_structure, cls=CustomJSONEncoder, ensure_ascii=False, indent=2)
            blob = self.bucket.blob(Config.GCS_CHECKPOINT_KEY)
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
