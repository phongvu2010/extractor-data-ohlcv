import os
import holidays
import pytz
from datetime import datetime
from typing import List
from dotenv import load_dotenv

# Load variables from .env file (if present)
load_dotenv()


def _get_env_int(key: str, default: int) -> int:
    val = os.getenv(key)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        return default


def _get_env_float(key: str, default: float) -> float:
    val = os.getenv(key)
    if val is None:
        return default
    try:
        return float(val)
    except ValueError:
        return default


class Config:
    """Quản lý tập trung toàn bộ cấu hình và hằng số của hệ thống."""

    # Thiết lập múi giờ Việt Nam để đồng bộ thời gian giao dịch sàn nội địa
    VN_TZ: pytz.tzinfo.BaseTzInfo = pytz.timezone("Asia/Ho_Chi_Minh")
    # Lịch nghỉ lễ Việt Nam
    CURRENT_YEAR: int = datetime.now(VN_TZ).year
    VN_HOLIDAYS_OBJ: holidays.VN = holidays.VN(years=[CURRENT_YEAR - 1, CURRENT_YEAR, CURRENT_YEAR + 1])
    VN_HOLIDAY_DATES: List = [str(date) for date in VN_HOLIDAYS_OBJ.keys()]

    # Cấu hình Google Cloud Storage (GCS)
    GCS_BUCKET_NAME: str = os.getenv("GCS_BUCKET_NAME", "vn-stock")
    GCS_CHECKPOINT_KEY: str = os.getenv("GCS_CHECKPOINT_KEY", "checkpoints/latest_state.json")
    GCS_PARQUET_PREFIX: str = os.getenv("GCS_PARQUET_PREFIX", "market_data")

    # Cấu hình Google Cloud BigQuery (BQ)
    BQ_DATASET: str = os.getenv("BQ_DATASET", "vn_stock_dataset")
    BQ_RAW_TABLE: str = os.getenv("BQ_RAW_TABLE", "raw_price")
    BQ_ADJ_TABLE: str = os.getenv("BQ_ADJ_TABLE", "adjusted_price")

    # Các hằng số hệ thống cố định
    URL_CAFEF: str = os.getenv("URL_CAFEF", "https://cafef1.mediacdn.vn/data/ami_data/")
    NETWORK_TIMEOUT: int = _get_env_int("NETWORK_TIMEOUT", 30)
    PRICE_MULTIPLIER: int = _get_env_int("PRICE_MULTIPLIER", 1000)
    CHUNK_SIZE: int = _get_env_int("CHUNK_SIZE", 150000)
    DEFAULT_LOGGER_NAME: str = os.getenv("DEFAULT_LOGGER_NAME", "ETL_Pipeline")
    API_REQUEST_THRESHOLD: int = _get_env_int("API_REQUEST_THRESHOLD", 18)
    API_RATE_LIMIT_WINDOW: float = _get_env_float("API_RATE_LIMIT_WINDOW", 60.0)
    API_MICRO_SLEEP: float = _get_env_float("API_MICRO_SLEEP", 3.5)
    BACKFILL_LIMIT: int = _get_env_int("BACKFILL_LIMIT", -1)

    # Cảnh báo qua Telegram
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # Cấu hình bỏ qua chốt chặn cuối tuần
    FORCE_RUN_WEEKEND: bool = os.getenv("FORCE_RUN_WEEKEND", "false").lower() == "true"



