from datetime import date, datetime
import logging
import os
import re
from typing import Any

from dotenv import load_dotenv
import holidays
from pydantic import Field, PrivateAttr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import pytz

# Tải trước các biến môi trường từ tệp .env nhằm phục vụ cấu hình động
load_dotenv()


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


class Settings(BaseSettings):
    """Quản lý tập trung toàn bộ cấu hình và hằng số của hệ thống sử dụng Pydantic BaseSettings."""

    # Cấu hình nạp dữ liệu môi trường qua Pydantic Settings
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        arbitrary_types_allowed=True,
    )

    # Thiết lập múi giờ Việt Nam để đồng bộ thời gian giao dịch sàn nội địa
    VN_TZ: pytz.tzinfo.BaseTzInfo = Field(
        default_factory=lambda: pytz.timezone("Asia/Ho_Chi_Minh")
    )

    # Các thuộc tính private để lưu vết cache ngày nghỉ lễ Việt Nam
    _cached_year: int | None = PrivateAttr(default=None)
    _cached_holidays: list[str] | None = PrivateAttr(default=None)

    # Môi trường chạy hệ thống (cloud / local)
    DEPLOYMENT_ENV: str = Field(default="cloud")
    DATABASE_URL: str = Field(default="")

    # Cấu hình Google Cloud Storage (GCS)
    GCS_BUCKET_NAME: str = Field(default="vn-stock")
    GCS_CHECKPOINT_KEY: str = Field(default="checkpoints/latest_state.json")
    GCS_PARQUET_PREFIX: str = Field(default="market_data")
    BLACKLIST_PATH_KEY: str = Field(default="configs/blacklist.txt")

    # Cấu hình Google Cloud BigQuery (BQ)
    BQ_DATASET: str = Field(default="vn_stock_dataset")
    BQ_RAW_TABLE: str = Field(default="raw_price")
    BQ_ADJ_TABLE: str = Field(default="adj_price")

    # Các hằng số hệ thống cố định
    URL_CAFEF: str = Field(default="https://cafef1.mediacdn.vn/data/ami_data/")
    URL_EVENTS: str = Field(
        default="https://iq.vietcap.com.vn/api/iq-insight-service/v1/events"
    )
    NETWORK_TIMEOUT: int = Field(default=30)
    PRICE_MULTIPLIER: int = Field(default=1000)
    CHUNK_SIZE: int = Field(default=150000)
    DEFAULT_LOGGER_NAME: str = Field(default="ETL_Pipeline")
    API_REQUEST_THRESHOLD: int = Field(default=18)
    API_RATE_LIMIT_WINDOW: float = Field(default=60.0)
    API_MICRO_SLEEP: float = Field(default=3.5)

    # Cấu hình vnstock API nguồn mặc định và dự phòng
    VNSTOCK_DEFAULT_SOURCE: str = Field(default="VCI")

    # Cảnh báo qua Telegram (Hỗ trợ Secret Manager hoặc fallback sang env)
    TELEGRAM_BOT_TOKEN: str = Field(default="")
    TELEGRAM_CHAT_ID: str = Field(default="")

    # Cấu hình bỏ qua các chốt chặn ngày nghỉ (cuối tuần / lễ) khi cần chạy ép buộc
    FORCE_RUN: bool = Field(default=False)

    # Cấu hình ngày bắt đầu tải lịch sử mặc định khi reload giá điều chỉnh
    HISTORICAL_START_DATE: str = Field(default="2000-01-01")

    # Cấu hình giờ chốt phiên EOD (giờ và phút)
    EOD_HOUR: int = Field(default=16)
    EOD_MINUTE: int = Field(default=30)

    # Danh sách mã cổ phiếu benchmark kiểm định đơn vị giá
    BENCHMARK_TICKERS: list[str] = Field(
        default_factory=lambda: ["FPT", "HPG", "VNM", "VIC"]
    )

    # Cấu hình kích thước lô (batch size) khi tải bảng giá T0 và reload giá điều chỉnh
    PRICE_BOARD_BATCH_SIZE: int = Field(default=500)
    RELOAD_BATCH_SIZE: int = Field(default=10)

    # Ngưỡng phát hiện lỗi nghiêm trọng liên tiếp khi tải ohlcv
    CRITICAL_FAILURE_THRESHOLD: int = Field(default=5)

    # Kích thước lô ghi dữ liệu (Bulk Upsert) vào cơ sở dữ liệu
    DB_UPSERT_CHUNK_SIZE: int = Field(default=5000)

    # Cấu hình bổ sung ngày nghỉ lễ qua biến môi trường (dạng YYYY-MM-DD, cách nhau bởi dấu phẩy)
    CUSTOM_HOLIDAYS: str = Field(default="")

    @field_validator("DEPLOYMENT_ENV", mode="before")
    @classmethod
    def clean_deployment_env(cls, v: Any) -> str:
        if isinstance(v, str):
            return v.strip().lower()
        return v

    @field_validator("HISTORICAL_START_DATE", mode="before")
    @classmethod
    def clean_historical_start_date(cls, v: Any) -> str:
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator("BENCHMARK_TICKERS", mode="before")
    @classmethod
    def parse_benchmark_tickers(cls, v: Any) -> list[str]:
        if isinstance(v, str):
            return [t.strip().upper() for t in v.split(",") if t.strip()]
        if isinstance(v, list):
            return [str(t).strip().upper() for t in v if str(t).strip()]
        return v

    @model_validator(mode="before")
    @classmethod
    def load_secrets(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Check secret files from Secret Manager (/secrets/<SECRET_NAME>) first
            for key in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
                secret_path = f"/secrets/{key}"
                if os.path.exists(secret_path):
                    try:
                        with open(secret_path, "r", encoding="utf-8") as f:
                            data[key] = f.read().strip()
                            continue
                    except OSError:
                        pass
                # Nếu không có file và cũng chưa được định nghĩa trong data nạp từ env/dotenv,
                # thực hiện fallback lấy trực tiếp từ os.getenv
                if key not in data or data[key] is None:
                    data[key] = os.getenv(key, "")
        return data

    def get_vn_holiday_dates(self) -> list[str]:
        """Lấy danh sách các ngày nghỉ lễ của Việt Nam.

        Kết hợp ngày lễ quốc gia và cấu hình thủ công qua
        CUSTOM_HOLIDAYS, sử dụng cơ chế cache tránh tính toán lặp lại.

        Returns:
            list[str]: Danh sách ngày nghỉ lễ định dạng YYYY-MM-DD.
        """
        current_year: int = datetime.now(self.VN_TZ).year
        if self._cached_year != current_year or self._cached_holidays is None:
            # Xác định lịch nghỉ lễ Việt Nam xung quanh năm hiện hành để lọc ngày đóng cửa thị trường
            vn_holidays_obj: holidays.HolidayBase = holidays.country_holidays(
                "VN", years=[current_year - 1, current_year, current_year + 1]
            )

            # Danh sách ngày nghỉ lễ bổ sung được cấu hình thủ công qua
            # biến môi trường/cấu hình (dạng YYYY-MM-DD, cách nhau bởi dấu phẩy)
            custom_holidays_raw: str = self.CUSTOM_HOLIDAYS or ""
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
                    logger = logging.getLogger(self.DEFAULT_LOGGER_NAME)
                    logger.warning(
                        f"Environment variable 'CUSTOM_HOLIDAYS' contains invalid entry {d_clean!r}. "
                        "Expected format: YYYY-MM-DD. Ignoring this entry."
                    )

            self._cached_holidays = sorted(
                list(
                    set(
                        [str(dt) for dt in vn_holidays_obj.keys()] + custom_holiday_list
                    )
                )
            )
            self._cached_year = current_year
        return self._cached_holidays

    def validate_config(self) -> None:
        """Kiểm tra cấu hình hệ thống và đưa ra các cảnh báo cần thiết.

        Kiểm tra xem các biến môi trường quan trọng như DATABASE_URL (cho local)
        hoặc GCS_BUCKET_NAME, BQ_DATASET (cho cloud) có đầy đủ không, đồng thời
        kiểm định thông tin Telegram cấu hình và tính hợp lệ của các mã benchmark.
        """
        missing_critical: list[str] = []
        if self.DEPLOYMENT_ENV == "local":
            if not self.DATABASE_URL:
                missing_critical.append("DATABASE_URL")
        elif self.DEPLOYMENT_ENV == "cloud":
            if not self.GCS_BUCKET_NAME:
                missing_critical.append("GCS_BUCKET_NAME")
            if not self.BQ_DATASET:
                missing_critical.append("BQ_DATASET")

        logger = logging.getLogger(self.DEFAULT_LOGGER_NAME)
        if missing_critical:
            error_msg: str = (
                f"🛑 [Cấu hình] Thiếu các biến môi trường quan trọng cho "
                f"chế độ '{self.DEPLOYMENT_ENV}': {', '.join(missing_critical)}. "
                f"Không thể khởi chạy hệ thống."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Cảnh báo nếu cấu hình Telegram bị thiếu 1 trong 2 trường
        if (self.TELEGRAM_BOT_TOKEN and not self.TELEGRAM_CHAT_ID) or (
            self.TELEGRAM_CHAT_ID and not self.TELEGRAM_BOT_TOKEN
        ):
            logger.warning(
                "⚠️ [Cấu hình] Thiếu TELEGRAM_BOT_TOKEN hoặc "
                "TELEGRAM_CHAT_ID. Hệ thống sẽ không gửi được thông báo "
                "cảnh báo qua Telegram."
            )

        # Kiểm định định dạng các mã benchmark
        valid_benchmarks: list[str] = []
        for ticker in self.BENCHMARK_TICKERS:
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
            self.BENCHMARK_TICKERS = ["FPT", "HPG", "VNM", "VIC"]
        else:
            self.BENCHMARK_TICKERS = valid_benchmarks


# Tạo instance duy nhất của cấu hình để import và sử dụng trong toàn bộ hệ thống
Config = Settings()
