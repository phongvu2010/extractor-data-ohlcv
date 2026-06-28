"""Module định nghĩa lớp trừu tượng cơ sở (Base Class) cho các bộ lưu trữ dữ liệu."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date, datetime
import logging
from typing import Any

import polars as pl

from config import Config


class BaseStorage(ABC):
    """Abstract Base Class định nghĩa giao diện chung cho các cơ chế lưu trữ dữ liệu."""

    logger: logging.Logger

    def __init__(self, logger: logging.Logger) -> None:
        """Khởi tạo BaseStorage với logger.

        Args:
            logger (logging.Logger): Đối tượng Logger để ghi log.
        """
        self.logger = logger

    @abstractmethod
    def save_parquet(
        self,
        df: pl.DataFrame,
        date_ref: datetime,
        suffix: str = "raw",
        partition: bool = False,
    ) -> str | None:
        """Lưu trữ DataFrame dưới dạng tệp nén Parquet.

        Args:
            df (pl.DataFrame): DataFrame dữ liệu cần lưu trữ.
            date_ref (datetime): Mốc thời gian của tệp dữ liệu.
            suffix (str): Tiền tố thư mục/định dạng ('raw' hoặc 'adj').
            partition (bool): True để phân vùng theo năm/tháng, False để lưu file tổng hợp.

        Returns:
            str | None: Đường dẫn của tệp đã lưu hoặc None.
        """
        pass

    @abstractmethod
    def save_symbol_history(
        self, df: pl.DataFrame, symbol: str, suffix: str = "adj"
    ) -> None:
        """Lưu trữ toàn bộ lịch sử giá của một mã cổ phiếu.

        Args:
            df (pl.DataFrame): DataFrame lịch sử đầy đủ của mã cổ phiếu.
            symbol (str): Mã cổ phiếu.
            suffix (str): Tiền tố thư mục ('raw' hoặc 'adj').
        """
        pass

    @abstractmethod
    def sync_partition_to_bigquery(
        self, path: str, table_name: str, date_ref: date
    ) -> None:
        """Đồng bộ hóa dữ liệu phân vùng một ngày từ file đã lưu vào database.

        Args:
            path (str): Đường dẫn tệp dữ liệu đã lưu (GCS URI hoặc Local Path).
            table_name (str): Tên bảng đích.
            date_ref (date): Ngày giao dịch của phân vùng.
        """
        pass

    @abstractmethod
    def sync_adjusted_symbols_to_bigquery(self, symbols: list[str]) -> None:
        """Đồng bộ lịch sử điều chỉnh của danh sách mã chứng khoán vào database.

        Args:
            symbols (list[str]): Danh sách mã chứng khoán.
        """
        pass

    @abstractmethod
    def sync_daily_adjusted_prices(
        self, dates: list[datetime | date], excluded_symbols: list[str]
    ) -> None:
        """Đồng bộ hóa dữ liệu giá từ raw sang adjusted cho danh sách các ngày.

        Args:
            dates (list[datetime | date]): Danh sách các ngày cần đồng bộ.
            excluded_symbols (list[str]): Các mã cần loại trừ.
        """
        pass

    @abstractmethod
    def save_checkpoint(
        self,
        df: pl.DataFrame,
        active_symbols: set[str] | None = None,
        pending_adjusted_reloads: list[str] | None = None,
    ) -> None:
        """Trích xuất và cập nhật checkpoint trạng thái thị trường EOD.

        Args:
            df (pl.DataFrame): DataFrame dữ liệu của ngày chạy hiện tại.
            active_symbols (set[str] | None): Các mã đang niêm yết thực tế.
            pending_adjusted_reloads (list[str] | None): Danh sách các mã bị lỗi
                reload cần chạy lại lần sau.
        """
        pass

    @abstractmethod
    def read_checkpoint(self) -> dict[str, Any]:
        """Đọc checkpoint snapshot thị trường đã lưu trữ gần nhất.

        Returns:
            dict[str, Any]: Dict chứa metadata và trạng thái snapshots của các mã.
        """
        pass

    @abstractmethod
    def read_blacklist(self) -> set[str]:
        """Tải danh sách các mã chứng khoán thuộc danh sách đen.

        Returns:
            set[str]: Set chứa các mã chứng khoán viết hoa.
        """
        pass

    @abstractmethod
    def save_corporate_events(self, events: list[dict[str, Any]]) -> None:
        """Lưu danh sách chi tiết sự kiện doanh nghiệp.

        Args:
            events (list[dict[str, Any]]): Danh sách các dict chi tiết sự kiện doanh nghiệp.
        """
        pass

    @abstractmethod
    def get_state(self, key: str) -> Any:
        """Đọc một trạng thái tùy ý từ kho lưu trữ.

        Args:
            key (str): Khóa của trạng thái cần đọc.

        Returns:
            Any: Giá trị của trạng thái, hoặc None nếu không tồn tại.
        """
        pass

    @abstractmethod
    def save_state(self, key: str, value: Any) -> None:
        """Lưu một trạng thái tùy ý vào kho lưu trữ.

        Args:
            key (str): Khóa của trạng thái cần lưu.
            value (Any): Giá trị của trạng thái (cần tương thích JSON).
        """
        pass

    @abstractmethod
    def save_icb_industries(self, df_icb: pl.DataFrame) -> None:
        """Lưu thông tin danh mục phân loại ngành ICB.

        Args:
            df_icb (pl.DataFrame): DataFrame chứa thông tin danh mục ngành ICB.
        """
        pass

    @abstractmethod
    def save_companies(self, df_companies: pl.DataFrame) -> None:
        """Lưu thông tin danh sách các công ty.

        Args:
            df_companies (pl.DataFrame): DataFrame chứa danh sách các công ty.
        """
        pass

    @abstractmethod
    def load_parquet_to_bigquery(
        self,
        gcs_path: str,
        table_name: str,
        write_disposition: str = "WRITE_APPEND",
    ) -> None:
        """Nạp trực tiếp tệp Parquet vào cơ sở dữ liệu.

        Args:
            gcs_path (str): Đường dẫn tệp Parquet.
            table_name (str): Tên bảng đích.
            write_disposition (str): Chế độ ghi ('WRITE_APPEND' hoặc 'WRITE_TRUNCATE').
        """
        pass

    def _build_eod_snapshot(
        self,
        df: pl.DataFrame,
        active_symbols: set[str] | None,
        pending_adjusted_reloads: list[str] | None,
        old_checkpoint: dict[str, Any],
    ) -> dict[str, Any]:
        """Xây dựng cấu trúc JSON checkpoint EOD từ DataFrame ngày chạy hiện tại.

        Đây là phần logic tính toán thuần túy (không phụ thuộc backend),
        dùng chung cho cả CloudStorage (GCS) và LocalStorage (PostgreSQL).
        Phần lưu trữ kết quả do từng subclass tự thực hiện trong ``save_checkpoint()``.

        Args:
            df (pl.DataFrame): DataFrame dữ liệu tổng hợp ngày chạy hiện tại.
            active_symbols (set[str] | None): Tập hợp mã đang niêm yết để lọc snapshot.
                Nếu None thì giữ tất cả mã.
            pending_adjusted_reloads (list[str] | None): Mã cần tải lại lịch sử lần sau.
                Nếu None thì kế thừa từ checkpoint cũ.
            old_checkpoint (dict[str, Any]): Checkpoint hiện tại được đọc từ backend.

        Returns:
            dict[str, Any]: Cấu trúc JSON chuẩn gồm hai khóa::

                {
                    "metadata": { "last_successful_run", "is_eod", "total_tickers",
                                  "pending_adjusted_reloads" },
                    "snapshots": { symbol: { exchange, trading_date, OHLCV, average_price } }
                }
        """
        # 1. Lấy bản ghi mới nhất theo mã
        df_latest: pl.DataFrame = df.sort(["symbol", "trading_date"]).unique(
            subset=["symbol"], keep="last"
        )

        # 2. Tính average_price nếu chưa có
        price_cols: list[str] = ["open_price", "high_price", "low_price", "close_price"]
        if "average_price" not in df_latest.columns:
            df_latest = df_latest.with_columns(
                (
                    (
                        pl.col("open_price")
                        + pl.col("high_price")
                        + pl.col("low_price")
                        + pl.col("close_price")
                    )
                    / 4.0
                ).alias("average_price")
            )

        # 3. Chuẩn hóa kiểu số: giá → int, average_price → float64 (1 decimal)
        df_latest = df_latest.with_columns(
            [
                pl.col(col).cast(pl.Float64).round(0).cast(pl.Int64).alias(col)
                for col in price_cols
            ]
            + [pl.col("average_price").cast(pl.Float64).round(1).alias("average_price")]
        )

        # 4. Format ngày và xác định is_eod
        df_latest = df_latest.with_columns(
            pl.col("trading_date").dt.strftime("%Y-%m-%d").alias("trading_date")
        )
        max_date_str: str = str(df_latest["trading_date"].max())

        vn_now: datetime = datetime.now(Config.VN_TZ)
        today_str: str = vn_now.strftime("%Y-%m-%d")
        if max_date_str < today_str:
            is_eod: bool = True
        else:
            is_eod = vn_now.hour > Config.EOD_HOUR or (
                vn_now.hour == Config.EOD_HOUR and vn_now.minute >= Config.EOD_MINUTE
            )

        # 5. Xây dựng dict hiện tại
        df_latest = df_latest.with_columns(pl.col("symbol").cast(pl.String))
        cols_to_extract: list[str] = [
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
        current_data_dict: dict[str, dict[str, Any]] = {}
        for row in df_latest.select(cols_to_extract).iter_rows(named=True):
            sym: str = row["symbol"]
            current_data_dict[sym] = {
                "exchange": row["exchange"],
                "trading_date": row["trading_date"],
                "open_price": row["open_price"],
                "high_price": row["high_price"],
                "low_price": row["low_price"],
                "close_price": row["close_price"],
                "average_price": row["average_price"],
                "total_volume": row["total_volume"],
            }

        # 6. Hợp nhất (upsert) vào snapshot cũ
        merged_snapshots: dict[str, dict[str, Any]] = old_checkpoint.get(
            "snapshots", {}
        )
        old_metadata: dict[str, Any] = old_checkpoint.get("metadata") or {}
        if pending_adjusted_reloads is None:
            pending_adjusted_reloads = (
                old_metadata.get("pending_adjusted_reloads") or []
            )

        if is_eod:
            for sym, new_row in current_data_dict.items():
                if not sym:
                    continue
                old_row: dict[str, Any] | None = merged_snapshots.get(sym)
                if not old_row or new_row["trading_date"] >= old_row["trading_date"]:
                    merged_snapshots[sym] = new_row
        else:
            self.logger.info(
                "ℹ️ [Snapshot] Đang chạy trong phiên (Chưa chốt EOD). "
                "Giữ nguyên dữ liệu snapshots cũ."
            )

        # 7. Chuẩn hóa lại các số float32 tồn dư từ dữ liệu cũ
        for _sym, row in merged_snapshots.items():
            for col in price_cols:
                if col in row and isinstance(row[col], (int, float)):
                    row[col] = int(round(float(row[col])))
            if "average_price" in row and isinstance(
                row["average_price"], (int, float)
            ):
                row["average_price"] = round(float(row["average_price"]), 1)

        # 8. Lọc theo active_symbols và sắp xếp
        final_snapshots: dict[str, dict[str, Any]] = {
            sym: merged_snapshots[sym]
            for sym in sorted(merged_snapshots.keys())
            if not active_symbols or sym in active_symbols
        }

        return {
            "metadata": {
                "last_successful_run": max_date_str,
                "is_eod": is_eod,
                "total_tickers": len(final_snapshots),
                "pending_adjusted_reloads": pending_adjusted_reloads,
            },
            "snapshots": final_snapshots,
        }
