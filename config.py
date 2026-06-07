class Config:
    """Quản lý tập trung toàn bộ cấu hình và hằng số của hệ thống."""

    # Các hằng số hệ thống cố định
    URL_CAFEF: str = "https://cafef1.mediacdn.vn/data/ami_data/"
    NETWORK_TIMEOUT: int = 30
    PRICE_MULTIPLIER: int = 1000
    CHUNK_SIZE: int = 150000
    INPUT_BASE_DIR: str = "tmp"
    DEFAULT_LOGGER_NAME: str = "ETL_Pipeline"

    HISTORICAL_DIR: str = "historical"
    STAGING_DIR: str = "historical_staging_tmp"
    CHECKPOINT_FILE: str = "latest_state.json"
