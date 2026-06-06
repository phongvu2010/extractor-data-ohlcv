import logging
import json
import os
import pandas as pd
import shutil
from datetime import datetime
from typing import Any, Dict, List, Optional

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

    def _load_checkpoint(self, checkpoint_path: str) -> Dict[str, Any]:
        """Đọc và khôi phục dữ liệu từ file checkpoint cũ thành cấu trúc Dict để phục vụ xử lý.

        Args:
            checkpoint_path: Đường dẫn tới file checkpoint JSON.

        Returns:
            Dict[str, Any]: Bản đồ dữ liệu với key là mã chứng khoán (symbol).
        """
        market_data_dict: Dict[str, Any] = {}

        if not os.path.exists(checkpoint_path):
            return market_data_dict

        try:
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                old_data = json.load(f)

                # Trường hợp 1: File cấu trúc mới dạng rút gọn (columns và data)
                if "columns" in old_data and "data" in old_data:
                    cols = old_data["columns"]
                    for row in old_data["data"]:
                        symbol = row[0]
                        # map zip từng phần tử của dòng với tên cột tương ứng (bỏ qua cột 'symbol' làm key)
                        market_data_dict[symbol] = dict(zip(cols[1:], row[1:]))

                # Trường hợp 2: Tương thích ngược với file cấu trúc cũ (orient="index") nếu có
                else:
                    market_data_dict = old_data.get("market_data", {})

            self.logger.info(f"📂 Đã đọc thành công file trạng thái cũ để chuẩn bị gộp dữ liệu.")
        except Exception as e:
            self.logger.warning(f"⚠️ Không thể phân tích file checkpoint cũ ({e}). Hệ thống sẽ khởi tạo vùng snapshot mới.")

        return market_data_dict

    def save_checkpoint(self, df: pd.DataFrame) -> None:
        """Trích xuất và cập nhật trạng thái thị trường EOD (Snapshot)
        của các mã chứng khoán theo cơ chế Upsert an toàn và tối ưu dung lượng.

        Args:
            df: DataFrame dữ liệu tổng hợp của ngày chạy hiện tại.
        """
        if df is None or df.empty:
            return

        self.logger.info("⚡ [Snapshot] Đang trích xuất trạng thái thị trường EOD...")

        # 1. Tìm ngày chạy lớn nhất toàn hệ thống bằng phép toán Vector O(N)
        max_date_str: str = pd.to_datetime(df["trading_date"]).max().strftime("%Y-%m-%d")

        # 2. Nhóm và lấy bản ghi cuối cùng của từng mã trong ngày: O(N)
        df_latest: pd.DataFrame = df.drop_duplicates(subset=["symbol"], keep="last").copy()

        # 3. Tính toán giá trung bình bằng Vectorization an toàn
        price_cols_origin = ["open_price", "high_price", "low_price", "close_price"]
        if "average_price" not in df_latest.columns:
            df_latest["average_price"] = df_latest[price_cols_origin].mean(axis=1).round(2)

        # 4. Đồng bộ hóa định dạng hiển thị chuỗi ngày và sàn giao dịch
        df_latest["exchange"] = df_latest["exchange"].astype(str)
        df_latest["trading_date"] = pd.to_datetime(df_latest["trading_date"]).dt.strftime("%Y-%m-%d")

        # 5. Chuyển đổi dữ liệu ngày hôm nay sang cấu trúc dict tạm thời
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
        current_market_data: Dict[str, Any] = df_latest[target_cols].set_index("symbol").to_dict(orient="index")

        # 6. Tái sử dụng hàm đọc checkpoint để lấy dữ liệu lịch sử
        checkpoint_path: str = os.path.join(Config.INPUT_BASE_DIR, "latest_state.json")
        os.makedirs(Config.INPUT_BASE_DIR, exist_ok=True)

        final_market_data: Dict[str, Any] = self._load_checkpoint(checkpoint_path)

        # 7. Cơ chế UPSERT: Ghi đè dữ liệu mới của ngày hôm nay vào dữ liệu tổng lịch sử
        final_market_data.update(current_market_data)

        # 8. TỐI ƯU DUNG LƯỢNG: Chuyển đổi dữ liệu sang định dạng rút gọn (Matrix phẳng)
        data_rows: List[List[Any]] = []
        for symbol, metrics in final_market_data.items():
            row = [
                symbol,
                metrics["exchange"],
                metrics["trading_date"],
                metrics["open_price"],
                metrics["high_price"],
                metrics["low_price"],
                metrics["close_price"],
                metrics["average_price"],
                metrics["total_volume"],
            ]
            data_rows.append(row)

        final_json_structure: Dict[str, Any] = {
            "last_successful_run": max_date_str,
            "columns": target_cols,
            "data": data_rows,
        }

        # 9. Ghi dữ liệu an toàn xuống đĩa cứng (Bỏ indent=4 để tối ưu dung lượng)
        try:
            with open(checkpoint_path, "w", encoding="utf-8") as f:
                json.dump(final_json_structure, f, ensure_ascii=False)
            self.logger.info(
                f"💾 [Upsert Thành Công] Đã cập nhật snapshot tổng cộng {len(final_market_data)} mã tại: {checkpoint_path}"
            )
        except Exception as e:
            self.logger.error(f"❌ Không thể ghi file Checkpoint JSON: {e}")
