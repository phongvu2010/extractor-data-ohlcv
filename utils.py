import logging
import time
from rich.console import Console
from rich.logging import RichHandler
from typing import Callable


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Khởi tạo và cấu hình hệ thống ghi chú (logging) sử dụng Rich.

    Returns:
        logging.Logger: Đối tượng Logger đã được cấu hình hoàn chỉnh.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Ngăn chặn log bị đẩy ngược lên hệ thống mặc định
    logger.propagate = False

    # Xóa các handler cũ nếu chạy lại nhiều lần
    if logger.hasHandlers():
        logger.handlers.clear()

    # Rich Console
    custom_console = Console(force_terminal=True)
    rich_handler = RichHandler(
        console=custom_console,
        rich_tracebacks=True,  # Tự động làm đẹp các đoạn lỗi (Traceback)
        markup=True,           # Cho phép dùng thẻ markup của Rich
        show_path=False,       # Ẩn đường dẫn file để console gọn gàng hơn
        show_time=True,
    )
    logger.addHandler(rich_handler)

    return logger


def normalize_exchange(exchange_code: str) -> str:
    clean_code: str = str(exchange_code).strip().upper()
    if "HSX" in clean_code or "HOSE" in clean_code:
        return "HoSE"
    if "UPCOM" in clean_code:
        return "UPCoM"
    if "HNX" in clean_code:
        return "HNX"
    return "Unknown"


class SmartRateLimiter:

    def __init__(
        self, logger: logging.Logger, limit: int, window: float, micro_sleep: float = 3.5
    ) -> None:
        self.logger: logging.Logger = logger
        self.limit: int = limit                 # Ngưỡng số lượng request tối đa (VD: 18)
        self.window: float = window             # Khung thời gian làm mát (VD: 60s)
        self.micro_sleep: float = micro_sleep   # Giãn cách tối thiểu giữa 2 request liên tiếp
        self.count: int = 0
        self.start_time: time = time.time()
        self.last_request_time: float = 0.0

    def hit(self) -> None:
        self.count += 1
        now = time.time()
        elapsed = now - self.last_request_time

        # Đảm bảo các request cách nhau ít nhất `micro_sleep` giây
        if elapsed < self.micro_sleep:
            time.sleep(self.micro_sleep - elapsed)

        self.last_request_time = time.time()

    def is_threshold_reached(self) -> bool:
        return self.count >= self.limit

    def wait_if_needed(self, io_task: Callable = None) -> None:
        if io_task:
            io_task()  # Tranh thủ ghi file nháp trong lúc chờ

        if self.count == 0:
            self.reset()
            return

        elapsed = time.time() - self.start_time
        remaining = self.window - elapsed

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
        self.count = 0
        self.start_time = time.time()
