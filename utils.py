from datetime import datetime, timezone
import json
import logging
import os
from typing import Any, Callable, Dict, Optional
import time

from rich.console import Console
from rich.logging import RichHandler
from rich.text import Text

from config import Config


def vn_time_converter(*args: Any) -> time.struct_time:
    """Bộ chuyển đổi thời gian sang múi giờ Việt Nam cho logging.

    Args:
        *args: Các tham số truyền vào từ logging.

    Returns:
        Thời gian cấu trúc đại diện cho thời gian hiện tại ở Việt Nam.
    """
    return datetime.now(Config.VN_TZ).timetuple()


# Ghi đè converter mặc định của logging để bất kỳ formatter nào dùng asctime cũng dùng múi giờ Việt Nam
logging.Formatter.converter = vn_time_converter


class GCPJSONFormatter(logging.Formatter):
    """Bộ định dạng log dạng JSON cho Google Cloud Logging (Cloud Run).

    Tự động trích xuất các trường thông tin cần thiết và làm sạch Rich markup.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Định dạng bản ghi log thành chuỗi JSON tương thích với Google Cloud Logging.

        Args:
            record: Đối tượng bản ghi log cần định dạng.

        Returns:
            Chuỗi định dạng JSON của log.
        """
        message: Any = record.getMessage()
        if isinstance(message, str):
            try:
                message = Text.from_markup(message).plain
            except Exception:
                pass

        # Phân loại cấp độ log cho GCP
        severity: str = record.levelname

        log_payload: Dict[str, Any] = {
            "severity": severity,
            "message": message,
            "logger.name": record.name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if record.exc_info:
            log_payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_payload, ensure_ascii=False)


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Khởi tạo và cấu hình hệ thống log.

    Tự động chuyển đổi định dạng sạch (Structured JSON) trên Cloud Run
    và sử dụng Rich terminal logging khi chạy ở môi trường cục bộ.

    Args:
        name: Tên của đối tượng Logger cần tạo.
        level: Cấp độ ghi nhận vết log (mặc định là logging.INFO).

    Returns:
        Logger đã được cấu hình phù hợp với môi trường thực thi (Local / Cloud Run).
    """
    logger: logging.Logger = logging.getLogger(name)
    logger.setLevel(level)

    # Ngăn chặn việc truyền log lên các handler cha mặc định
    logger.propagate = False

    # Xóa sạch các handler cũ đề phòng lỗi trùng lặp khi khởi chạy lại nhiều lần
    if logger.hasHandlers():
        logger.handlers.clear()

    # Nhận diện nếu đang chạy trên Google Cloud Run
    is_cloud_run: bool = "K_SERVICE" in os.environ

    if is_cloud_run:
        # Sử dụng Structured Logging trên Cloud Run để Google Cloud Logging thu thập sạch sẽ
        handler: logging.StreamHandler = logging.StreamHandler()
        formatter: GCPJSONFormatter = GCPJSONFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    else:
        # Chạy cục bộ -> Sử dụng Rich Handler trực quan
        custom_console: Console = Console(force_terminal=True)
        rich_handler: RichHandler = RichHandler(
            console=custom_console,
            rich_tracebacks=True,  # Hiển thị lỗi traceback chi tiết và trực quan
            markup=True,           # Hỗ trợ định dạng chữ nghệ thuật (markup)
            show_path=False,       # Ẩn đường dẫn file để màn hình console gọn gàng hơn
            show_time=True,
        )
        logger.addHandler(rich_handler)

    return logger


def normalize_exchange(exchange_code: str) -> str:
    """Chuẩn hóa tên sàn giao dịch chứng khoán Việt Nam về dạng thống nhất.

    Args:
        exchange_code: Tên sàn hoặc mã sàn chưa được chuẩn hóa từ nguồn dữ liệu.

    Returns:
        Tên sàn chuẩn hóa thuộc một trong các nhóm: "HoSE", "HNX", "UPCoM", "Unknown".
    """
    clean_code: str = str(exchange_code).strip().upper()
    if "HSX" in clean_code or "HOSE" in clean_code:
        return "HoSE"
    if "UPCOM" in clean_code:
        return "UPCoM"
    if "HNX" in clean_code:
        return "HNX"
    return "Unknown"


class SmartRateLimiter:
    """Bộ điều tiết tần suất cuộc gọi API để tránh bị khóa IP/tài khoản."""

    logger: logging.Logger
    limit: int
    window: float
    micro_sleep: float
    count: int
    start_time: float
    last_request_time: float

    def __init__(
        self,
        logger: logging.Logger,
        limit: int,
        window: float,
        micro_sleep: float = 3.5
    ) -> None:
        """Khởi tạo bộ giới hạn tốc độ yêu cầu API.

        Args:
            logger: Đối tượng Logger dùng để theo dõi trạng thái.
            limit: Ngưỡng số lượng cuộc gọi API tối đa trong một chu kỳ window.
            window: Độ dài khung thời gian làm mát tính bằng giây.
            micro_sleep: Độ trễ tối thiểu bắt buộc giữa hai yêu cầu liên tiếp.
        """
        self.logger = logger
        self.limit = limit
        self.window = window
        self.micro_sleep = micro_sleep
        self.count = 0
        self.start_time = time.time()
        self.last_request_time = 0.0

    def hit(self) -> None:
        """Đánh dấu một cuộc gọi API và thực hiện các biện pháp delay làm mát nếu cần."""
        now: float = time.time()
        if now - self.start_time >= self.window:
            self.reset()

        if self.is_threshold_reached():
            self.logger.warning(
                f"⚠️ Đạt giới hạn API ({self.limit} req/{self.window}s). Tiến hành làm mát..."
            )
            self.wait_if_needed()

        self.count += 1
        now = time.time()
        elapsed: float = now - self.last_request_time

        # Ép buộc thời gian nghỉ tối thiểu giữa các cuộc gọi API liên tiếp
        if elapsed < self.micro_sleep:
            time.sleep(self.micro_sleep - elapsed)

        self.last_request_time = time.time()

    def is_threshold_reached(self) -> bool:
        """Kiểm tra xem số lượt yêu cầu đã chạm ngưỡng giới hạn hay chưa.

        Returns:
            True nếu số lượng cuộc gọi chạm hoặc vượt giới hạn, ngược lại False.
        """
        return self.count >= self.limit

    def wait_if_needed(self, io_task: Optional[Callable[[], None]] = None) -> None:
        """Thực hiện dừng luồng để làm mát API, có thể kết hợp chạy tác vụ phụ song song.

        Args:
            io_task: Tác vụ phụ cần chạy tranh thủ trong thời gian chờ (ví dụ: ghi file).
        """
        if io_task:
            io_task()

        if self.count == 0:
            self.reset()
            return

        elapsed: float = time.time() - self.start_time
        remaining: float = self.window - elapsed

        if remaining > 0:
            self.logger.info(
                f"⏳ Tác vụ I/O đã xong. Làm mát API phần còn lại: {remaining:.2f}s..."
            )
            time.sleep(remaining)
        else:
            self.logger.info(
                f"⚡ Tác vụ I/O mất {elapsed:.2f}s. Đã qua {self.window}s, chạy luồng mới ngay lập tức!"
            )

        self.reset()

    def reset(self) -> None:
        """Khởi động lại bộ đếm cuộc gọi API cho chu kỳ làm mát mới."""
        self.count = 0
        self.start_time = time.time()
