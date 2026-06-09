import holidays
import pytz
from datetime import datetime
from typing import List


class Config:
    """Quản lý tập trung toàn bộ cấu hình và hằng số của hệ thống."""

    # Thiết lập múi giờ Việt Nam để đồng bộ thời gian giao dịch sàn nội địa
    VN_TZ: pytz.tzinfo.BaseTzInfo = pytz.timezone("Asia/Ho_Chi_Minh")
    CURRENT_TIME: datetime = datetime.now(VN_TZ)

    # Lịch nghỉ lễ Việt Nam
    CURRENT_YEAR: int = CURRENT_TIME.year
    VN_HOLIDAYS_OBJ: holidays.VN = holidays.VN(years=[CURRENT_YEAR - 1, CURRENT_YEAR, CURRENT_YEAR + 1])
    VN_HOLIDAY_DATES: List = [str(date) for date in VN_HOLIDAYS_OBJ.keys()]

    # Cấu hình Google Cloud Storage (GCS)
    GCS_CREDENTIALS_FILE: str = "secrets/credentials.json"
    GCS_BUCKET_NAME: str = "vn-stock"
    GCS_CHECKPOINT_KEY: str = "checkpoints/latest_state.json"
    GCS_PARQUET_PREFIX: str = "market_data"

    # Cấu hình Google Cloud BigQuery (BQ)
    BQ_DATASET: str = "vn_stock_dataset"
    BQ_RAW_TABLE: str = "raw_price"
    BQ_ADJ_TABLE: str = "adjusted_price"

    # Các hằng số hệ thống cố định
    URL_CAFEF: str = "https://cafef1.mediacdn.vn/data/ami_data/"
    NETWORK_TIMEOUT: int = 30
    PRICE_MULTIPLIER: int = 1000
    CHUNK_SIZE: int = 150000
    DEFAULT_LOGGER_NAME: str = "ETL_Pipeline"
    API_REQUEST_THRESHOLD: int = 18
    API_RATE_LIMIT_WINDOW: float = 60.0
    API_MICRO_SLEEP: float = 3.5
    BACKFILL_LIMIT: int = -1
