from datetime import date, datetime
import logging
import os
import re

from dotenv import load_dotenv
import holidays
import pytz

# Tải trước các biến môi trường từ tệp .env nhằm phục vụ cấu hình động
load_dotenv()


def _get_env_int(key: str, default: int) -> int:
    """Lấy giá trị cấu hình số nguyên từ biến môi trường.

    Args:
        key (str): Tên biến môi trường cần lấy.
        default (int): Giá trị mặc định nếu biến môi trường
            không tồn tại hoặc lỗi định dạng.

    Returns:
        int: Giá trị số nguyên tương ứng.
    """
    if not isinstance(default, int) or isinstance(default, bool):
        raise TypeError(
            f"Default value for {key} must be an int, got {type(default).__name__}"
        )
    val: str | None = os.getenv(key)
    if val is None:
        return default
    val = val.strip()
    if val == "":
        return default
    try:
        return int(val)
    except ValueError:
        logger = logging.getLogger(os.getenv("DEFAULT_LOGGER_NAME", "ETL_Pipeline"))
        logger.warning(
            f"⚠️ Environment variable '{key}' has value {val!r} which cannot be parsed to an int. "
            f"Using default value: {default}"
        )
        return default


def _get_env_float(key: str, default: float) -> float:
    """Lấy giá trị cấu hình số thực từ biến môi trường.

    Args:
        key (str): Tên biến môi trường cần lấy.
        default (float): Giá trị mặc định nếu biến môi trường
            không tồn tại hoặc lỗi định dạng.

    Returns:
        float: Giá trị số thực tương ứng.
    """
    if not isinstance(default, (int, float)) or isinstance(default, bool):
        raise TypeError(
            f"Default value for {key} must be a float or int, got {type(default).__name__}"
        )
    val: str | None = os.getenv(key)
    if val is None:
        return float(default)
    val = val.strip()
    if val == "":
        return float(default)
    try:
        return float(val)
    except ValueError:
        logger = logging.getLogger(os.getenv("DEFAULT_LOGGER_NAME", "ETL_Pipeline"))
        logger.warning(
            f"⚠️ Environment variable '{key}' has value {val!r} which cannot be parsed to a float. "
            f"Using default value: {default}"
        )
        return float(default)


def _get_secret(key: str, default: str = "") -> str:
    """Lấy cấu hình bảo mật từ Secret Manager hoặc biến môi trường.

    Args:
        key (str): Tên secret cần lấy.
        default (str): Giá trị mặc định.

    Returns:
        str: Giá trị cấu hình chuỗi tương ứng.
    """
    # Cloud Run hỗ trợ mount secrets dưới dạng file tại đường dẫn /secrets/<SECRET_NAME>
    secret_path: str = f"/secrets/{key}"
    if os.path.exists(secret_path):
        try:
            with open(secret_path, "r", encoding="utf-8") as f:
                return f.read().strip()
        except OSError:
            pass
    return os.getenv(key, default)


class Config:
    """Quản lý tập trung toàn bộ cấu hình và hằng số của hệ thống."""

    # Thiết lập múi giờ Việt Nam để đồng bộ thời gian giao dịch sàn nội địa
    VN_TZ: pytz.tzinfo.BaseTzInfo = pytz.timezone("Asia/Ho_Chi_Minh")

    _cached_year: int | None = None
    _cached_holidays: list[str] | None = None

    @classmethod
    def get_vn_holiday_dates(cls) -> list[str]:
        """Lấy danh sách các ngày nghỉ lễ của Việt Nam.

        Kết hợp ngày lễ quốc gia và cấu hình thủ công qua
        CUSTOM_HOLIDAYS, sử dụng cơ chế cache tránh tính toán lặp lại.

        Returns:
            list[str]: Danh sách ngày nghỉ lễ định dạng YYYY-MM-DD.
        """
        current_year: int = datetime.now(cls.VN_TZ).year
        if cls._cached_year != current_year or cls._cached_holidays is None:
            # Xác định lịch nghỉ lễ Việt Nam xung quanh năm hiện hành để lọc ngày đóng cửa thị trường
            vn_holidays_obj: holidays.HolidayBase = holidays.country_holidays(
                "VN", years=[current_year - 1, current_year, current_year + 1]
            )

            # Danh sách ngày nghỉ lễ bổ sung được cấu hình thủ công qua
            # biến môi trường (dạng YYYY-MM-DD, cách nhau bởi dấu phẩy)
            custom_holidays_raw: str = os.getenv("CUSTOM_HOLIDAYS", "")
            custom_holiday_list: list[str] = []
            for d in custom_holidays_raw.split(","):
                d_clean: str = d.strip()
                if not d_clean:
                    continue
                # Chuẩn hóa dấu gạch chéo thành gạch ngang (ví dụ 2026/06/15 -> 2026-06-15)
                d_normalized: str = d_clean.replace("/", "-")
                try:
                    valid_date: date = datetime.strptime(
                        d_normalized, "%Y-%m-%d"
                    ).date()
                    custom_holiday_list.append(valid_date.strftime("%Y-%m-%d"))
                except ValueError:
                    logger = logging.getLogger(cls.DEFAULT_LOGGER_NAME)
                    logger.warning(
                        f"Environment variable 'CUSTOM_HOLIDAYS' contains invalid entry {d_clean!r}. "
                        "Expected format: YYYY-MM-DD. Ignoring this entry."
                    )

            cls._cached_holidays = sorted(
                list(
                    set(
                        [str(dt) for dt in vn_holidays_obj.keys()] + custom_holiday_list
                    )
                )
            )
            cls._cached_year = current_year
        return cls._cached_holidays

    # Môi trường chạy hệ thống (cloud / local)
    DEPLOYMENT_ENV: str = os.getenv("DEPLOYMENT_ENV", "cloud").strip().lower()
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")

    # Cấu hình Google Cloud Storage (GCS)
    GCS_BUCKET_NAME: str = os.getenv("GCS_BUCKET_NAME", "vn-stock")
    GCS_CHECKPOINT_KEY: str = os.getenv(
        "GCS_CHECKPOINT_KEY", "checkpoints/latest_state.json"
    )
    GCS_PARQUET_PREFIX: str = os.getenv("GCS_PARQUET_PREFIX", "market_data")
    BLACKLIST_PATH_KEY: str = os.getenv("BLACKLIST_PATH_KEY", "configs/blacklist.txt")

    # Cấu hình Google Cloud BigQuery (BQ)
    BQ_DATASET: str = os.getenv("BQ_DATASET", "vn_stock_dataset")
    BQ_RAW_TABLE: str = os.getenv("BQ_RAW_TABLE", "raw_price")
    BQ_ADJ_TABLE: str = os.getenv("BQ_ADJ_TABLE", "adjusted_price")

    # Các hằng số hệ thống cố định
    URL_CAFEF: str = os.getenv("URL_CAFEF", "https://cafef1.mediacdn.vn/data/ami_data/")
    URL_EVENTS: str = os.getenv(
        "URL_EVENTS",
        "https://iq.vietcap.com.vn/api/iq-insight-service/v1/events",
    )
    NETWORK_TIMEOUT: int = _get_env_int("NETWORK_TIMEOUT", 30)
    PRICE_MULTIPLIER: int = _get_env_int("PRICE_MULTIPLIER", 1000)
    CHUNK_SIZE: int = _get_env_int("CHUNK_SIZE", 150000)
    DEFAULT_LOGGER_NAME: str = os.getenv("DEFAULT_LOGGER_NAME", "ETL_Pipeline")
    API_REQUEST_THRESHOLD: int = _get_env_int("API_REQUEST_THRESHOLD", 18)
    API_RATE_LIMIT_WINDOW: float = _get_env_float("API_RATE_LIMIT_WINDOW", 60.0)
    API_MICRO_SLEEP: float = _get_env_float("API_MICRO_SLEEP", 3.5)

    # Cấu hình vnstock API nguồn mặc định và dự phòng
    VNSTOCK_DEFAULT_SOURCE: str = os.getenv("VNSTOCK_DEFAULT_SOURCE", "VCI")

    # Cảnh báo qua Telegram (Hỗ trợ Secret Manager mount file bí mật hoặc fallback sang env)
    TELEGRAM_BOT_TOKEN: str = _get_secret("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = _get_secret("TELEGRAM_CHAT_ID", "")

    # Cấu hình bỏ qua các chốt chặn ngày nghỉ (cuối tuần / lễ) khi cần chạy ép buộc
    FORCE_RUN: bool = os.getenv("FORCE_RUN", "false").lower() == "true"

    # Cấu hình ngày bắt đầu tải lịch sử mặc định khi reload giá điều chỉnh
    HISTORICAL_START_DATE: str = os.getenv(
        "HISTORICAL_START_DATE", "2000-01-01"
    ).strip()

    # Cấu hình giờ chốt phiên EOD (giờ và phút)
    EOD_HOUR: int = _get_env_int("EOD_HOUR", 16)
    EOD_MINUTE: int = _get_env_int("EOD_MINUTE", 30)

    # Danh sách mã cổ phiếu benchmark kiểm định đơn vị giá
    BENCHMARK_TICKERS: list[str] = [
        t.strip().upper()
        for t in os.getenv("BENCHMARK_TICKERS", "FPT,HPG,VNM,VIC").split(",")
        if t.strip()
    ]

    # Cấu hình kích thước lô (batch size) khi tải bảng giá T0 và reload giá điều chỉnh
    PRICE_BOARD_BATCH_SIZE: int = _get_env_int("PRICE_BOARD_BATCH_SIZE", 500)
    RELOAD_BATCH_SIZE: int = _get_env_int("RELOAD_BATCH_SIZE", 10)

    # Ngưỡng phát hiện lỗi nghiêm trọng liên tiếp khi tải ohlcv
    CRITICAL_FAILURE_THRESHOLD: int = _get_env_int("CRITICAL_FAILURE_THRESHOLD", 5)

    # Kích thước lô ghi dữ liệu (Bulk Upsert) vào cơ sở dữ liệu
    DB_UPSERT_CHUNK_SIZE: int = _get_env_int("DB_UPSERT_CHUNK_SIZE", 5000)

    @classmethod
    def validate_config(cls) -> None:
        """Kiểm tra cấu hình hệ thống và đưa ra các cảnh báo cần thiết.

        Kiểm tra xem các biến môi trường quan trọng như DATABASE_URL (cho local)
        hoặc GCS_BUCKET_NAME, BQ_DATASET (cho cloud) có đầy đủ không, đồng thời
        kiểm định thông tin Telegram cấu hình và tính hợp lệ của các mã benchmark.
        """
        missing_critical: list[str] = []
        if cls.DEPLOYMENT_ENV == "local":
            if not cls.DATABASE_URL:
                missing_critical.append("DATABASE_URL")
        elif cls.DEPLOYMENT_ENV == "cloud":
            if not cls.GCS_BUCKET_NAME:
                missing_critical.append("GCS_BUCKET_NAME")
            if not cls.BQ_DATASET:
                missing_critical.append("BQ_DATASET")

        logger = logging.getLogger(cls.DEFAULT_LOGGER_NAME)
        if missing_critical:
            error_msg: str = (
                f"🛑 [Cấu hình] Thiếu các biến môi trường quan trọng cho "
                f"chế độ '{cls.DEPLOYMENT_ENV}': {', '.join(missing_critical)}. "
                f"Không thể khởi chạy hệ thống."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Cảnh báo nếu cấu hình Telegram bị thiếu 1 trong 2 trường
        if (cls.TELEGRAM_BOT_TOKEN and not cls.TELEGRAM_CHAT_ID) or (
            cls.TELEGRAM_CHAT_ID and not cls.TELEGRAM_BOT_TOKEN
        ):
            logger.warning(
                "⚠️ [Cấu hình] Thiếu TELEGRAM_BOT_TOKEN hoặc "
                "TELEGRAM_CHAT_ID. Hệ thống sẽ không gửi được thông báo "
                "cảnh báo qua Telegram."
            )

        # Kiểm định định dạng các mã benchmark
        valid_benchmarks: list[str] = []
        for ticker in cls.BENCHMARK_TICKERS:
            if re.match(r"^[A-Z0-9]{3,10}$", ticker):
                valid_benchmarks.append(ticker)
            else:
                logger.warning(
                    f"⚠️ [Cấu hình] Mã benchmark '{ticker}' không đúng định dạng. Bỏ qua mã này."
                )
        if not valid_benchmarks:
            logger.warning(
                "⚠️ [Cấu hình] Không có mã benchmark nào hợp lệ. Sử dụng danh sách mặc định ['FPT', 'HPG', 'VNM', 'VIC']."
            )
            cls.BENCHMARK_TICKERS = ["FPT", "HPG", "VNM", "VIC"]
        else:
            cls.BENCHMARK_TICKERS = valid_benchmarks
