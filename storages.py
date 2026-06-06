import logging
import json
import os
import pandas as pd
import shutil
from datetime import datetime
from typing import Any, Dict,List

from config import Config


class Storage:
    """Chuyên trách việc lưu trữ dữ liệu an toàn ra đĩa cứng (Parquet, JSON)."""

    def __init__(self, logger: logging.Logger) -> None:
        """Khởi tạo bộ lưu trữ dữ liệu.

        Args:
            logger: Đối tượng Logger dùng để ghi nhận tiến trình.
        """
        self.logger: logging.Logger = logger

    def save_parquet(self, df: pd.DataFrame, date_ref: datetime, suffix: str = "raw") -> None:
        """Ghi dữ liệu nén Parquet áp dụng cơ chế Staging an toàn (Atomic Write).

        Args:
            df: DataFrame dữ liệu lịch sử cần ghi dữ liệu.
            date_ref: Mốc thời gian của tệp dữ liệu.

        Raises:
            Exception: Phát sinh khi có lỗi I/O hoặc ghi tệp đĩa cứng thất bại.
        """
        if df is None or df.empty:
            return

        output_dir: str = os.path.join(Config.INPUT_BASE_DIR, "historical")
        staging_dir: str = os.path.join(Config.INPUT_BASE_DIR, "historical_staging_tmp")

        # Reset thư mục tạm
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)
        os.makedirs(staging_dir, exist_ok=True)

        try:
            file_name: str = f"historical_{suffix}_upto_{date_ref.strftime('%Y%m%d')}.parquet"
            staging_file_path: str = os.path.join(staging_dir, file_name)

            self.logger.info(f"💾 Đang ghi dữ liệu nén Parquet: {file_name}")
            df.to_parquet(staging_file_path, compression="snappy", index=False)

            os.makedirs(output_dir, exist_ok=True)
            target_file_path: str = os.path.join(output_dir, file_name)

            if os.path.exists(target_file_path):
                os.remove(target_file_path)

            shutil.move(staging_file_path, output_dir)
            self.logger.info(f"🎉 File lưu trữ thành công tại: {target_file_path}")
        except Exception as e:
            self.logger.error(f"❌ Lỗi trong quá trình ghi file Parquet: {e}")
            raise e
        finally:
            if os.path.exists(staging_dir):
                shutil.rmtree(staging_dir)

    def save_checkpoint(self, df: pd.DataFrame) -> None:
        """Trích xuất và lưu trạng thái thị trường EOD (Snapshot) của toàn bộ các mã chứng khoán.

        Args:
            df: DataFrame dữ liệu tổng hợp.
        """
        if df is None or df.empty:
            return

        self.logger.info("⚡ [Snapshot] Đang trích xuất trạng thái thị trường EOD...")

        # 1. Tìm ngày chạy lớn nhất toàn hệ thống bằng phép toán Vector cực nhanh O(N)
        max_date_str: str = pd.to_datetime(df["trading_date"]).max().strftime("%Y-%m-%d")

        # 2. Tối ưu hóa Big O: Nhóm và lấy bản ghi cuối cùng của từng mã: O(N)
        df_latest: pd.DataFrame = df.drop_duplicates(subset=["symbol"], keep="last").copy()

        # 3. Tính toán giá trung bình bằng Vectorization an toàn trên bản sao độc lập
        price_cols_origin = ["open_price", "high_price", "low_price", "close_price"]
        if "average_price" not in df_latest.columns:
            df_latest["average_price"] = df_latest[price_cols_origin].mean(axis=1).round(2)

        # 4. Đồng bộ hóa định dạng hiển thị chuỗi ngày và sàn giao dịch
        df_latest["exchange"] = df_latest["exchange"].astype(str)
        df_latest["trading_date"] = pd.to_datetime(df_latest["trading_date"]).dt.strftime("%Y-%m-%d")

        # 5. Chuyển đổi sang cấu trúc dữ liệu định dạng JSON
        target_cols: List[str] = [
            "symbol",
            "exchange",
            "trading_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "average_price",
            "total_volume",
        ]
        market_data_dict: Dict[str, Any] = df_latest[target_cols].set_index("symbol").to_dict(orient="index")

        final_json_structure: Dict[str, Any] = {
            "last_successful_run": max_date_str,
            "market_data": market_data_dict,
        }

        checkpoint_path: str = os.path.join(Config.INPUT_BASE_DIR, "latest_state.json")
        os.makedirs(Config.INPUT_BASE_DIR, exist_ok=True)

        try:
            with open(checkpoint_path, "w", encoding="utf-8") as f:
                # Nếu file cực lớn, cân nhắc bỏ indent=4 để giảm dung lượng file text xuống 30%
                json.dump(final_json_structure, f, indent=4, ensure_ascii=False)
            self.logger.info(
                f"💾 Đã lưu snapshot toàn bộ {len(market_data_dict)} mã thành công tại: {checkpoint_path}"
            )
        except Exception as e:
            self.logger.error(f"❌ Không thể ghi file Checkpoint JSON: {e}")
