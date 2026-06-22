"""Module định nghĩa lớp trừu tượng cơ sở (Base Class) cho các bộ lưu trữ dữ liệu."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date, datetime
import logging
from typing import Any

import pandas as pd


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
        df: pd.DataFrame,
        date_ref: datetime,
        suffix: str = "raw",
        partition: bool = False,
    ) -> str | None:
        """Lưu trữ DataFrame dưới dạng tệp nén Parquet.

        Args:
            df (pd.DataFrame): DataFrame dữ liệu cần lưu trữ.
            date_ref (datetime): Mốc thời gian của tệp dữ liệu.
            suffix (str): Tiền tố thư mục/định dạng ('raw' hoặc 'adj').
            partition (bool): True để phân vùng theo năm/tháng, False để lưu file tổng hợp.

        Returns:
            str | None: Đường dẫn của tệp đã lưu hoặc None.
        """
        pass

    @abstractmethod
    def save_symbol_history(
        self, df: pd.DataFrame, symbol: str, suffix: str = "adj"
    ) -> None:
        """Lưu trữ toàn bộ lịch sử giá của một mã cổ phiếu.

        Args:
            df (pd.DataFrame): DataFrame lịch sử đầy đủ của mã cổ phiếu.
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
            dates (list[Union[datetime, date]]): Danh sách các ngày cần đồng bộ.
            excluded_symbols (list[str]): Các mã cần loại trừ.
        """
        pass

    @abstractmethod
    def save_checkpoint(
        self,
        df: pd.DataFrame,
        active_symbols: set[str] | None = None,
        pending_adjusted_reloads: list[str] | None = None,
    ) -> None:
        """Trích xuất và cập nhật checkpoint trạng thái thị trường EOD.

        Args:
            df (pd.DataFrame): DataFrame dữ liệu của ngày chạy hiện tại.
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
    def save_companies(self, df_companies: pd.DataFrame) -> None:
        """Lưu thông tin danh sách các công ty.

        Args:
            df_companies (pd.DataFrame): DataFrame chứa danh sách các công ty.
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

    def export_interested_tickers_data(self) -> dict[str, Any] | None:
        """Trích xuất dữ liệu các mã cổ phiếu quan tâm. (Chỉ áp dụng cho CloudStorage)

        Returns:
            dict[str, Any] | None: Dict chứa tóm tắt kết quả xuất dữ liệu, hoặc None.
        """
        return None
