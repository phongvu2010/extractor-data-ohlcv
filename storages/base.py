from abc import ABC, abstractmethod
import logging
import pandas as pd
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Set, Union

class BaseStorage(ABC):
    """Abstract Base Class định nghĩa giao diện chung cho các cơ chế lưu trữ dữ liệu."""

    logger: logging.Logger

    def __init__(self, logger: logging.Logger) -> None:
        """Khởi tạo BaseStorage với logger.

        Args:
            logger: Đối tượng Logger để ghi log.
        """
        self.logger = logger

    @abstractmethod
    def save_parquet(
        self,
        df: pd.DataFrame,
        date_ref: datetime,
        suffix: str = "raw",
        partition: bool = False
    ) -> Optional[str]:
        """Lưu trữ DataFrame dưới dạng tệp nén Parquet.

        Args:
            df: DataFrame dữ liệu cần lưu trữ.
            date_ref: Mốc thời gian của tệp dữ liệu.
            suffix: Tiền tố thư mục/định dạng ('raw' hoặc 'adj').
            partition: True để phân vùng theo năm/tháng, False để lưu file tổng hợp.

        Returns:
            Đường dẫn của tệp đã lưu hoặc None.
        """
        pass

    @abstractmethod
    def save_symbol_history(
        self,
        df: pd.DataFrame,
        symbol: str,
        suffix: str = "adj"
    ) -> None:
        """Lưu trữ toàn bộ lịch sử giá của một mã cổ phiếu.

        Args:
            df: DataFrame lịch sử đầy đủ của mã cổ phiếu.
            symbol: Mã cổ phiếu.
            suffix: Tiền tố thư mục ('raw' hoặc 'adj').
        """
        pass

    @abstractmethod
    def sync_partition_to_bigquery(
        self,
        path: str,
        table_name: str,
        date_ref: date
    ) -> None:
        """Đồng bộ hóa dữ liệu phân vùng một ngày từ file đã lưu vào database.

        Args:
            path: Đường dẫn tệp dữ liệu đã lưu (GCS URI hoặc Local Path).
            table_name: Tên bảng đích.
            date_ref: Ngày giao dịch của phân vùng.
        """
        pass

    @abstractmethod
    def sync_adjusted_symbols_to_bigquery(self, symbols: List[str]) -> None:
        """Đồng bộ lịch sử điều chỉnh của danh sách mã chứng khoán vào database.

        Args:
            symbols: Danh sách mã chứng khoán.
        """
        pass

    @abstractmethod
    def sync_daily_adjusted_prices(
        self,
        dates: List[Union[datetime, date]],
        excluded_symbols: List[str]
    ) -> None:
        """Đồng bộ hóa dữ liệu giá từ raw sang adjusted cho danh sách các ngày.

        Args:
            dates: Danh sách các ngày cần đồng bộ.
            excluded_symbols: Các mã cần loại trừ.
        """
        pass

    @abstractmethod
    def save_checkpoint(
        self,
        df: pd.DataFrame,
        active_symbols: Optional[Set[str]] = None,
        pending_adjusted_reloads: Optional[List[str]] = None
    ) -> None:
        """Trích xuất và cập nhật checkpoint trạng thái thị trường EOD.

        Args:
            df: DataFrame dữ liệu của ngày chạy hiện tại.
            active_symbols: Các mã đang niêm yết thực tế.
            pending_adjusted_reloads: Danh sách các mã bị lỗi reload cần chạy lại lần sau.
        """
        pass

    @abstractmethod
    def read_checkpoint(self) -> Dict[str, Any]:
        """Đọc checkpoint snapshot thị trường đã lưu trữ gần nhất.

        Returns:
            Dict chứa metadata và trạng thái snapshots của các mã.
        """
        pass

    @abstractmethod
    def read_blacklist(self) -> Set[str]:
        """Tải danh sách các mã chứng khoán thuộc danh sách đen.

        Returns:
            Set chứa các mã chứng khoán viết hoa.
        """
        pass

    @abstractmethod
    def save_corporate_events(self, events: List[Dict[str, Any]]) -> None:
        """Lưu danh sách chi tiết sự kiện doanh nghiệp.

        Args:
            events: Danh sách các dict chi tiết sự kiện doanh nghiệp.
        """
        pass

    @abstractmethod
    def save_companies(self, df_companies: pd.DataFrame) -> None:
        """Lưu thông tin danh sách các công ty.

        Args:
            df_companies: DataFrame chứa danh sách các công ty.
        """
        pass

    def export_interested_tickers_data(self) -> Optional[Dict[str, Any]]:
        """Trích xuất dữ liệu các mã cổ phiếu quan tâm. (Chỉ áp dụng cho CloudStorage)

        Returns:
            Dict chứa tóm tắt kết quả xuất dữ liệu, hoặc None.
        """
        return None
