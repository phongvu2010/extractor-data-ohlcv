import logging
from rich.console import Console
from rich.logging import RichHandler


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
