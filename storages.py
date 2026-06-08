import gc
import json
import logging
import os
import pandas as pd
import shutil
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

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

        output_dir: str = os.path.join(Config.INPUT_BASE_DIR, Config.HISTORICAL_DIR)
        # Tối ưu: Tạo staging riêng biệt theo suffix để an toàn khi chạy song song
        staging_dir: str = os.path.join(Config.INPUT_BASE_DIR, f"{Config.STAGING_DIR}_{suffix}")

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

            # Ghi đè nguyên tử (Atomic Replace) nếu cùng phân vùng đĩa, fallback shutil.move nếu khác phân vùng
            try:
                os.replace(staging_file_path, target_file_path)
            except OSError:
                if os.path.exists(target_file_path):
                    os.remove(target_file_path)
                shutil.move(staging_file_path, target_file_path)

            self.logger.info(f"🎉 File lưu trữ thành công tại: {target_file_path}")
        except Exception as e:
            self.logger.error(f"❌ Lỗi trong quá trình ghi file Parquet: {e}")
            raise e
        finally:
            if os.path.exists(staging_dir):
                shutil.rmtree(staging_dir)

    def save_checkpoint(self, df: pd.DataFrame, active_symbols: Optional[Set[str]] = None) -> None:
        """Trích xuất và cập nhật trạng thái thị trường EOD (Snapshot) với Vectorization tăng tốc x5 lần.

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

        # Chuẩn hóa kiểu dữ liệu số thực về float64 và làm tròn để tránh lỗi hiển thị trên JSON
        for col in price_cols + ["average_price"]:
            df_latest[col] = df_latest[col].astype(float).round(1)

        # Chuẩn hóa cột ngày sang chuỗi YYYY-MM-DD
        df_latest["trading_date"] = df_latest["trading_date"].dt.strftime("%Y-%m-%d")

        # Lấy ngày chạy lớn nhất để lưu metadata
        max_date_str: str = str(df_latest["trading_date"].max())

        # Đảm bảo index symbol là chuỗi thông thường (không phải categorical) để xuất dict sạch sẽ
        if isinstance(df_latest["symbol"].dtype, pd.CategoricalDtype):
            df_latest["symbol"] = df_latest["symbol"].astype(str)
        df_latest.set_index("symbol", inplace=True)

        # Chỉ lấy các cột cần thiết, ép kiểu chuẩn về dict nguyên bản của Python để gom JSON
        cols_to_extract = ["exchange", "trading_date", "open_price", "high_price", "low_price", "close_price", "average_price", "total_volume"]
        current_data_dict: Dict[str, Dict[str, Any]] = df_latest[cols_to_extract].to_dict(orient="index")

        # 2. Đọc dữ liệu lịch sử cũ từ file checkpoint
        checkpoint_path: str = os.path.join(Config.INPUT_BASE_DIR, Config.CHECKPOINT_FILE)
        os.makedirs(Config.INPUT_BASE_DIR, exist_ok=True)

        merged_snapshots: Dict[str, Dict[str, Any]] = {}

        if os.path.exists(checkpoint_path):
            try:
                with open(checkpoint_path, "r", encoding="utf-8") as f:
                    old_json = json.load(f)
                merged_snapshots = old_json.get("snapshots", {})
            except Exception as e:
                self.logger.warning(f"⚠️ Không thể đọc file checkpoint cũ do lỗi: {e}. Tiến hành khởi tạo mới.")
                merged_snapshots = {}

        # 3. Tiến hành Hợp nhất (Upsert) O(1)
        for sym, new_row in current_data_dict.items():
            if not sym: 
                continue
            old_row = merged_snapshots.get(sym)
            # Nếu mã chưa có hoặc có ngày mới hơn/bằng ngày cũ -> Cập nhật thông tin mới nhất
            if not old_row or new_row["trading_date"] >= old_row["trading_date"]:
                merged_snapshots[sym] = new_row

        # Chuẩn hóa toàn bộ dữ liệu số thực trong merged_snapshots để dọn dẹp các tàn dư float32 cũ
        for sym, row in merged_snapshots.items():
            for col in price_cols + ["average_price"]:
                if col in row and isinstance(row[col], (int, float)):
                    row[col] = round(float(row[col]), 1)

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
                "total_tickers": len(final_snapshots)
            },
            "snapshots": final_snapshots
        }

        # 6. Ghi đè nguyên tử (Atomic Write) cho Checkpoint JSON
        temp_checkpoint_path = f"{checkpoint_path}.tmp"
        try:
            with open(temp_checkpoint_path, "w", encoding="utf-8") as f:
                json.dump(final_json_structure, f, ensure_ascii=False, indent=2)
            os.replace(temp_checkpoint_path, checkpoint_path)
            self.logger.info(
                f"💾 [Snapshot Thành Công] Đã cập nhật tổng cộng {len(final_snapshots)} mã tại: {checkpoint_path}"
            )
        except Exception as e:
            self.logger.error(f"🛑 Ghi tệp snapshot trạng thái thất bại: {e}")
            if os.path.exists(temp_checkpoint_path):
                os.remove(temp_checkpoint_path)
        finally:
            # Giải phóng các cấu trúc dữ liệu lớn thủ công để tối ưu RAM
            del current_data_dict, merged_snapshots, final_snapshots
            gc.collect()
