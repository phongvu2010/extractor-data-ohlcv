"""Module khởi tạo và cung cấp Factory Method để lấy bộ lưu trữ phù hợp (Local / Cloud)."""

import logging

from .base import BaseStorage
from .cloud import CloudStorage


def get_storage(env: str, logger: logging.Logger) -> BaseStorage:
    """Factory Method trả về bộ lưu trữ tương ứng dựa trên cấu hình môi trường.

    Args:
        env (str): Tên môi trường ('local' hoặc 'cloud').
        logger (logging.Logger): Đối tượng Logger ghi log.

    Returns:
        BaseStorage: Instance của lớp kế thừa từ BaseStorage.
    """
    clean_env: str = str(env).strip().lower()
    if clean_env == "local":
        from .local import LocalStorage

        return LocalStorage(logger)
    return CloudStorage(logger)
