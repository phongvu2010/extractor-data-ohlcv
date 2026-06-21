import logging
from .base import BaseStorage
from .cloud import CloudStorage
from .local import LocalStorage

def get_storage(env: str, logger: logging.Logger) -> BaseStorage:
    """Factory Method trả về bộ lưu trữ tương ứng dựa trên cấu hình môi trường DEPLOYMENT_ENV.

    Args:
        env: Tên môi trường ('local' hoặc 'cloud').
        logger: Đối tượng Logger ghi log.

    Returns:
        Instance của lớp kế thừa từ BaseStorage.
    """
    clean_env = str(env).strip().lower()
    if clean_env == "local":
        return LocalStorage(logger)
    else:
        return CloudStorage(logger)
