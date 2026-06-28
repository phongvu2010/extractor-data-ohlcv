"""Module thực hiện tải, xử lý và làm sạch dữ liệu chứng khoán từ nguồn Vnstock API."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import logging
import math
import re
import time
from typing import Any

import pandera.errors as pa_errors
import polars as pl
import requests
from vnstock import Reference, Trading
from vnstock.core.utils.user_agent import get_headers
from vnstock.ui import Market

from config import Config
from notifier import Notifier
from schemas import OHLCVSchema
from storages import BaseStorage, get_storage
from utils import (
    setup_logger,
    SmartRateLimiter,
    get_exchange_normalization_expr,
    sanitize_price_columns,
    build_price_invalid_mask,
)

# Suppress spam logging from vnstock internal company explorer module
logging.getLogger("vnstock.explorer.vci.company").setLevel(logging.CRITICAL)


class DataProcessor:
    """Chuyên trách việc làm sạch, biến đổi và tối ưu hóa dữ liệu chứng khoán từ vnstock."""

    logger: logging.Logger
    source: str
    reference_api: Reference
    trading_api: Trading
    market_api: Market
    rate_limiter: SmartRateLimiter

    def __init__(self, logger: logging.Logger, source: str | None = None) -> None:
        """Khởi tạo bộ xử lý dữ liệu và thiết lập các cổng kết nối API Vnstock.

        Args:
            logger (logging.Logger): Đối tượng Logger dùng để ghi nhận tiến trình.
            source (Optional[str]): Nguồn cung cấp dữ liệu chứng khoán mặc định. Nếu None, lấy từ Config.VNSTOCK_DEFAULT_SOURCE.
        """
        self.logger = logger
        self.source = source or Config.VNSTOCK_DEFAULT_SOURCE

        self.reference_api = Reference()
        self.trading_api = Trading()
        self.market_api = Market()

        # Cấu hình bộ điều tiết tần suất cuộc gọi API để bảo vệ hệ thống tránh bị chặn
        self.rate_limiter = SmartRateLimiter(
            logger=logger,
            limit=Config.API_REQUEST_THRESHOLD,
            window=Config.API_RATE_LIMIT_WINDOW,
            micro_sleep=Config.API_MICRO_SLEEP,
        )

        self.ohlcv_multiplier = 1000.0
        self.board_multiplier = 1.0

    def get_symbols_with_exchange(self) -> dict[str, str]:
        """Tải danh sách mã cổ phiếu đang hoạt động cùng với sàn giao dịch tương ứng.

        Returns:
            dict[str, str]: Dict mapping giữa mã chứng khoán (symbol) và sàn niêm yết (exchange).
        """
        self.rate_limiter.hit()
        try:
            df_symbols_pd = self.reference_api.equity().list_by_exchange()
            df_symbols = pl.from_pandas(df_symbols_pd)
            # Loại trừ trái phiếu, trái phiếu doanh nghiệp và chứng khoán phái sinh
            df_symbols = df_symbols.filter(
                ~pl.col("type").is_in(["corpbond", "bond", "future"])
            )
            return dict(zip(df_symbols["symbol"], df_symbols["exchange"]))
        except Exception as e:
            self.logger.error(f"❌ [Vnstock] Không thể lấy danh sách symbol: {e}")
            return {}

    def fetch_entire_market_t0(self, symbols: list[str]) -> pl.DataFrame:
        """Tải dữ liệu bảng giá ngày hiện hành T0 hàng loạt cho tất cả các mã chứng khoán.

        Args:
            symbols (list[str]): Danh sách mã cổ phiếu cần tải thông tin bảng giá.

        Returns:
            pl.DataFrame: DataFrame dữ liệu giao dịch T0 hoàn chỉnh của toàn bộ thị trường.
        """
        self.logger.info(
            f"📥 [Vnstock] [Bulk Fetch] Đang tải bảng giá T0 cho {len(symbols)} mã..."
        )

        if not symbols:
            self.logger.warning("⚠️ [Vnstock] Danh sách symbols trống. Bỏ qua tải T0.")
            return pl.DataFrame()

        dfs: list[pl.DataFrame] = []
        batch_size: int = Config.PRICE_BOARD_BATCH_SIZE
        for i in range(0, len(symbols), batch_size):
            batch: list[str] = symbols[i : i + batch_size]

            max_retries: int = 3
            initial_delay: float = 10.0
            backoff_factor: float = 2.0
            delay: float = initial_delay
            df_quote: pl.DataFrame | None = None
            success: bool = False

            for attempt in range(1, max_retries + 1):
                try:
                    self.rate_limiter.hit()
                    df_quote_pd = self.trading_api.price_board(batch)
                    if df_quote_pd is not None and not df_quote_pd.empty:
                        df_quote = pl.from_pandas(df_quote_pd)
                        success = True
                        break
                    raise ValueError("Dữ liệu bảng giá trống hoặc None")
                except requests.exceptions.Timeout as e:
                    self.logger.error(
                        f"⏳ [Vnstock] Lỗi Timeout khi tải lô {i} (lần {attempt}/{max_retries}): {e}."
                    )
                except requests.exceptions.ConnectionError as e:
                    self.logger.error(
                        f"🔌 [Vnstock] Lỗi kết nối mạng khi tải lô {i} (lần {attempt}/{max_retries}): {e}."
                    )
                except Exception as e:
                    self.logger.error(
                        f"⚠️ [Vnstock] Lỗi không xác định khi tải lô {i} (lần {attempt}/{max_retries}): {e}"
                    )

                if attempt < max_retries:
                    self.logger.info(f"⏳ [Vnstock] Thử lại lô {i} sau {delay} giây...")
                    time.sleep(delay)
                    delay *= backoff_factor

            if not success or df_quote is None or df_quote.is_empty():
                raise RuntimeError(
                    f"❌ Tất cả các lần thử tải bảng giá T0 cho lô từ {i} đến {min(i + batch_size, len(symbols))} đều thất bại. "
                    "Hủy pipeline để tránh thiếu hụt dữ liệu."
                )

            dfs.append(df_quote)

        df_all: pl.DataFrame = pl.concat(dfs)

        # Chuẩn hóa tên sàn giao dịch bằng biểu thức dùng chung
        df_all = df_all.with_columns(get_exchange_normalization_expr("exchange"))

        # Chuyển đổi timestamp Unix mili-giây sang ngày giờ Việt Nam chuẩn không có múi giờ
        df_all = df_all.with_columns(
            pl.col("time")
            .cast(pl.Datetime("ms"))
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone("Asia/Ho_Chi_Minh")
            .cast(pl.Date)
            .alias("trading_date")
        )
        df_all = df_all.with_columns(pl.col("volume_accumulated").alias("total_volume"))

        # Làm sạch cột giá: thay Inf/NaN/Null và loại dòng null
        df_all = sanitize_price_columns(df_all)
        df_all = df_all.drop_nulls(subset=["total_volume", "trading_date"])

        # Chuẩn bị cột source và exchange
        df_all = df_all.with_columns(
            [
                pl.col("symbol").cast(pl.String).str.strip_chars().str.to_uppercase(),
                pl.col("exchange").cast(pl.Categorical),
                pl.lit(self.source.lower()).alias("source").cast(pl.String),
            ]
        )

        # Gom các cột mục tiêu
        target_cols = [
            "symbol",
            "trading_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "total_volume",
            "exchange",
            "source",
        ]

        df_to_validate = df_all.select(target_cols)

        # Lọc bỏ các dòng lỗi logic giá hoặc giá trị <= 0 để tránh làm sập pipeline khi chạy validation
        df_to_validate = df_to_validate.filter(~build_price_invalid_mask())

        # Áp dụng Pandera Data Contract

        try:
            df_validated = OHLCVSchema.validate(df_to_validate)

            # Khôi phục thêm các cột reference nếu cần cho phiên T0 (nằm ngoài contract OHLCV)
            # bằng cách join ngược lại hoặc validate riêng
            df_final = df_validated.join(
                df_all.select(
                    ["symbol", "trading_date", "reference_price", "average_price"]
                ),
                on=["symbol", "trading_date"],
                how="left",
            )
            return df_final
        except pa_errors.SchemaError as e:
            self.logger.error(
                f"❌ [Vnstock] Dữ liệu T0 không hợp lệ, vi phạm Data Contract: {e}"
            )
            raise e

    def fetch_ohlcv(
        self, symbol: str, start_date: str, end_date: str, limit: int = 100
    ) -> pl.DataFrame | None:
        """Tải dữ liệu lịch sử OHLCV cho một mã chứng khoán trong khoảng thời gian xác định.

        Args:
            symbol (str): Mã cổ phiếu cần tải (ví dụ: 'FPT').
            start_date (str): Ngày bắt đầu định dạng YYYY-MM-DD.
            end_date (str): Ngày kết thúc định dạng YYYY-MM-DD.
            limit (int): Số dòng tối đa cho phép tải về.

        Returns:
            Optional[pl.DataFrame]: DataFrame chứa lịch sử OHLCV của mã cổ phiếu, trả về None nếu tất cả các nguồn thất bại.
        """
        # Tạo danh sách các nguồn để quét thử nếu nguồn chính gặp sự cố
        sources: list[str] = [self.source, "kbs", "msn"]
        # Loại bỏ các phần tử trùng lặp nhưng vẫn giữ nguyên thứ tự ưu tiên
        sources = list(dict.fromkeys([s for s in sources if s]))

        for src in sources:
            self.rate_limiter.hit()
            try:
                self.logger.info(
                    f"📥 [Vnstock] Đang tải dữ liệu OHLCV mã {symbol} từ nguồn: {src}"
                )
                df_ohclv_pd = self.market_api.equity(symbol).ohlcv(
                    start=start_date,
                    end=end_date,
                    source=src,
                    count=limit,
                )
                if df_ohclv_pd is not None and not df_ohclv_pd.empty:
                    df_ohclv = pl.from_pandas(df_ohclv_pd)
                    df_ohclv = df_ohclv.rename(
                        {
                            "time": "trading_date",
                            "open": "open_price",
                            "high": "high_price",
                            "low": "low_price",
                            "close": "close_price",
                            "volume": "total_volume",
                        }
                    )

                    # Làm sạch cột giá: thay Inf/NaN/Null và loại dòng null
                    df_ohclv = sanitize_price_columns(df_ohclv)

                    # Quy đổi đơn vị giá Vnstock về đồng giống file CafeF dựa trên hệ số nhân động
                    price_cols: list[str] = list(
                        ["open_price", "high_price", "low_price", "close_price"]
                    )
                    df_ohclv = df_ohclv.with_columns(
                        [
                            (pl.col(col) * self.ohlcv_multiplier).alias(col)
                            for col in price_cols
                        ]
                    )
                    df_ohclv = df_ohclv.with_columns(
                        pl.lit(src.lower()).alias("source")
                    )

                    # Lọc bỏ các dòng lỗi logic giá hoặc giá trị <= 0
                    df_ohclv = df_ohclv.filter(~build_price_invalid_mask())

                    return df_ohclv

            except Exception as e:
                self.logger.warning(
                    f"⚠️ [Vnstock] Nguồn {src} gặp lỗi khi tải mã {symbol}: {e}. Đang thử nguồn khác..."
                )

        self.logger.error(
            f"❌ [Vnstock] Tất cả các nguồn dữ liệu OHLCV đều thất bại cho mã {symbol}."
        )
        return None

    def detect_corporate_actions_via_api(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> list[dict[str, Any]]:
        """Quét và phát hiện lịch sự kiện doanh nghiệp của một nhóm mã chứng khoán thông qua API.

        Args:
            symbols (list[str]): Danh sách các mã cổ phiếu cần quét.
            start_date (date): Mốc thời gian bắt đầu quét sự kiện.
            end_date (date): Mốc thời gian kết thúc quét sự kiện.

        Returns:
            list[dict[str, Any]]: Danh sách chứa thông tin chi tiết các sự kiện doanh nghiệp.
        """
        if not symbols:
            return []

        start_str: str = start_date.strftime("%Y%m%d")
        end_str: str = (end_date + timedelta(days=1)).strftime("%Y%m%d")

        self.logger.info(
            f"🔍 [Events] Quét sự kiện từ {start_date} đến {end_date} cho {len(symbols)} mã..."
        )

        headers: dict[str, str] = get_headers(data_source="VCI")

        all_dfs: list[pl.DataFrame] = []
        batch_size: int = 150

        for i in range(0, len(symbols), batch_size):
            batch: list[str] = symbols[i : i + batch_size]
            tickers_str: str = ",".join(batch)
            url: str = (
                f"{Config.URL_EVENTS}"
                f"?ticker={tickers_str}&fromDate={start_str}&toDate={end_str}"
                f"&eventCode=DIV,ISS&page=0&size=1000"
            )

            max_retries: int = 3
            df_batch: pl.DataFrame | None = None
            for attempt in range(1, max_retries + 1):
                try:
                    self.rate_limiter.hit()
                    response = requests.get(url, headers=headers, timeout=30)
                    if response.status_code == 200:
                        data = response.json()
                        events_list = data.get("data", {}).get("content", [])
                        if events_list:
                            df_batch = pl.DataFrame(events_list)
                        break
                    else:
                        raise ValueError(f"HTTP status {response.status_code}")
                except Exception as e:
                    self.logger.warning(
                        f"⚠️ [Events] Lỗi tải sự kiện lô {i} (lần {attempt}/{max_retries}): {e}"
                    )
                    if attempt < max_retries:
                        time.sleep(2.0 * attempt)

            if df_batch is not None and not df_batch.is_empty():
                all_dfs.append(df_batch)

        if not all_dfs:
            return []

        df_events: pl.DataFrame = pl.concat(all_dfs, how="diagonal")
        rename_map: dict[str, str] = {}
        if "exrightDate" in df_events.columns:
            rename_map["exrightDate"] = "exright_date"
        if "organCode" in df_events.columns:
            rename_map["organCode"] = "organ_code"

        if rename_map:
            df_events = df_events.rename(rename_map)

        if "exright_date" not in df_events.columns:
            return []

        df_events = df_events.filter(pl.col("exright_date").is_not_null())
        if df_events.is_empty():
            return []

        df_events = df_events.with_columns(
            pl.col("exright_date")
            .cast(pl.String)
            .str.slice(0, 10)
            .str.to_date("%Y-%m-%d", strict=False)
            .alias("ex_date")
        )

        # Chỉ lọc lấy các sự kiện nằm hoàn toàn trong khoảng thời gian cần kiểm tra
        df_filtered: pl.DataFrame = df_events.filter(
            (pl.col("ex_date") >= start_date) & (pl.col("ex_date") <= end_date)
        )

        if df_filtered.is_empty():
            return []

        # Ưu tiên lấy sự kiện sớm nhất nếu có nhiều sự kiện phát sinh cho một mã
        df_filtered = df_filtered.sort("ex_date")

        # Tối ưu hóa bằng vector hóa trên Polars thay vì duyệt từng dòng (iter_rows)
        cols_present = df_filtered.columns
        ticker_col = pl.col("ticker") if "ticker" in cols_present else pl.lit(None)
        organ_code_col = (
            pl.col("organ_code") if "organ_code" in cols_present else pl.lit(None)
        )
        record_date_col = (
            pl.col("recordDate") if "recordDate" in cols_present else pl.lit(None)
        )
        ratio_col = (
            pl.col("exerciseRatio") if "exerciseRatio" in cols_present else pl.lit(None)
        )
        event_code_col = (
            pl.col("eventCode") if "eventCode" in cols_present else pl.lit(None)
        )

        symbol_expr = (
            pl.when(
                ticker_col.cast(pl.String)
                .str.strip_chars()
                .str.to_uppercase()
                .is_in(["", "NAN", "NONE", "<NA>"])
                | ticker_col.is_null()
            )
            .then(
                organ_code_col.cast(pl.String)
                .str.strip_chars()
                .str.to_uppercase()
                .fill_null("")
            )
            .otherwise(
                ticker_col.cast(pl.String)
                .str.strip_chars()
                .str.to_uppercase()
                .fill_null("")
            )
            .alias("symbol")
        )

        df_processed = df_filtered.with_columns(
            [
                symbol_expr,
                record_date_col.cast(pl.String)
                .str.slice(0, 10)
                .str.to_date("%Y-%m-%d", strict=False)
                .alias("record_date"),
                ratio_col.cast(pl.String).alias("ratio"),
                event_code_col.fill_null("DIV").cast(pl.String).alias("event_type"),
            ]
        )

        df_processed = df_processed.filter(
            pl.col("symbol").str.contains(r"^[A-Z0-9]{3,10}$")
            & pl.col("ex_date").is_not_null()
        )

        events: list[dict[str, Any]] = df_processed.select(
            ["symbol", "event_type", "ex_date", "record_date", "ratio"]
        ).to_dicts()

        return events


class VnstockExtractorETL:
    """Bộ điều phối chính của Vnstock Daily Pipeline."""

    logger: logging.Logger
    processor: DataProcessor
    storage: BaseStorage

    def __init__(
        self,
        logger_name: str = Config.DEFAULT_LOGGER_NAME,
        storage: BaseStorage | None = None,
        logger: logging.Logger | None = None,
        notifier: Notifier | None = None,
    ) -> None:
        """Khởi tạo các phân lớp phục vụ xử lý và lưu trữ dữ liệu.

        Args:
            logger_name (str): Tên Logger chung của hệ thống.
            storage (BaseStorage | None): Đối tượng Storage chia sẻ kết nối.
            logger (logging.Logger | None): Logger hệ thống bên ngoài truyền vào.
            notifier (Notifier | None): Đối tượng Notifier dùng chung.
        """
        self.logger = logger or setup_logger(logger_name)
        self.storage = storage or get_storage(Config.DEPLOYMENT_ENV, self.logger)
        self.notifier = notifier or Notifier(self.logger)
        self.processor = DataProcessor(self.logger)

    def _initialize_run_dates(
        self, df_t0: pl.DataFrame
    ) -> tuple[date, list[date], list[str], dict[str, Any]] | None:
        """Phân tích checkpoint và kiểm tra tính hợp lệ về ngày chạy dữ liệu mới.

        Args:
            df_t0 (pl.DataFrame): DataFrame dữ liệu phiên T0 tải về.

        Returns:
            Optional[tuple[date, list[date], list[str], dict[str, Any]]]: Tuple chứa các thông tin
                (t0_max_date, missing_dates, pending_reloads, latest_state)
                hoặc None nếu hệ thống dừng chạy do trùng lẫn hoặc không có lịch sử mới.
        """
        t0_max_date: date = df_t0["trading_date"].max()

        latest_state: dict[str, Any] = self.storage.read_checkpoint()
        metadata: dict[str, Any] = latest_state.get("metadata") or {}
        last_run_str: str | None = metadata.get("last_successful_run")
        is_eod: bool = metadata.get("is_eod", False)
        pending_reloads: list[str] = metadata.get("pending_adjusted_reloads") or []

        date_latest_state: date | None = None
        if last_run_str:
            try:
                date_latest_state = datetime.strptime(last_run_str, "%Y-%m-%d").date()
                self.logger.info(
                    f"ℹ️ [Vnstock] Ngày chạy daily cuối cùng thành công: {date_latest_state} "
                    f"({'Đã chốt EOD' if is_eod else 'Chưa chốt EOD'})"
                )
            except Exception as e:
                self.logger.warning(
                    f"⚠️ [Vnstock] Không thể phân tích ngày chạy cuối cùng {last_run_str}: {e}"
                )

        # Chốt chặn thời gian: Dừng chạy nếu ngày tải về cũ hơn ngày đã chạy thành công gần nhất
        if date_latest_state and t0_max_date < date_latest_state:
            self.logger.warning(
                f"⚠️ [Vnstock] Ngày giao dịch lớn nhất tải về ({t0_max_date}) "
                f"cũ hơn ngày đã chạy thành công gần nhất ({date_latest_state}). Dừng pipeline."
            )
            return None

        if date_latest_state and t0_max_date == date_latest_state and is_eod:
            self.logger.info(
                f"ℹ️ [Vnstock] Ngày giao dịch {t0_max_date} đã chạy thành công và chốt EOD trước đó. Dừng pipeline."
            )
            return None

        if date_latest_state and t0_max_date == date_latest_state and not is_eod:
            self.logger.warning(
                f"🔔 [Vnstock] Chạy lại ngày {t0_max_date} do phiên trước chưa chốt EOD. Cập nhật dữ liệu..."
            )

        # Tính toán danh sách các ngày làm việc bị thiếu để tiến hành bù dữ liệu (Backfill)
        missing_dates: list[date] = []
        if date_latest_state:
            start_offset: int = 1 if is_eod else 0
            current_date: date = date_latest_state + timedelta(days=start_offset)
            while current_date < t0_max_date:
                if current_date.weekday() < 5:
                    current_date_str: str = current_date.strftime("%Y-%m-%d")
                    if current_date_str not in Config.get_vn_holiday_dates():
                        missing_dates.append(current_date)
                current_date += timedelta(days=1)

        return t0_max_date, missing_dates, pending_reloads, latest_state

    def _detect_corporate_actions_today(
        self, df_t0: pl.DataFrame, today_date: date
    ) -> dict[str, date]:
        """Phát hiện các sự kiện doanh nghiệp của phiên T0 thông qua quét API.

        Args:
            df_t0 (pl.DataFrame): DataFrame dữ liệu T0 để lấy danh sách các mã giao dịch cần quét.
            today_date (date): Ngày giao dịch hôm nay.

        Returns:
            dict[str, date]: Dict chứa các mã cổ phiếu và ngày có sự kiện tương ứng.
        """
        symbols: list[str] = [str(s) for s in df_t0["symbol"].unique().to_list() if s]
        self.logger.info(
            f"🔍 [Events] Đang quét trực tiếp API sự kiện hôm nay cho {len(symbols)} mã..."
        )
        events = self.processor.detect_corporate_actions_via_api(
            symbols, today_date, today_date
        )
        self.storage.save_corporate_events(events)
        return {e["symbol"]: e["ex_date"] for e in events}

    def _backfill_missing_history(self, missing_dates: list[date]) -> None:
        """Bù lại toàn bộ các ngày giao dịch bị thiếu thông qua extractor CafeF.

        Args:
            missing_dates (list[date]): Danh sách các ngày cần backfill.
        """
        self.logger.info(
            f"🚀 [Backfill] Phát hiện {len(missing_dates)} ngày thiếu cần backfill. Chạy qua CafeF..."
        )
        from extractors.extractor_cafef import CafeFExtractorETL

        cafe_etl: CafeFExtractorETL = CafeFExtractorETL(
            logger_name=self.logger.name, storage=self.storage, notifier=self.notifier
        )

        for m_date in sorted(missing_dates):
            self.logger.info(
                f"📥 [CafeF] [Backfill] Đang tải dữ liệu thô cho ngày {m_date}..."
            )
            dt_ref: datetime = datetime.combine(m_date, datetime.min.time())
            try:
                res: pl.DataFrame | None = cafe_etl.run(
                    dt_ref, is_raw=True, partition=True, save_checkpoint=False
                )
                if res is None:
                    # Cảnh báo mềm qua log và Telegram, không quăng lỗi làm sập hệ thống
                    warn_msg: str = (
                        f"⚠️ [CafeF] [Backfill] Không tải được dữ liệu cho ngày {m_date.strftime('%Y-%m-%d')}. "
                        "Có thể đây là ngày nghỉ giao dịch đột xuất hoặc lỗi CDN CafeF. Bỏ qua ngày này."
                    )
                    self.logger.warning(warn_msg)
                    try:
                        self.notifier.send_alert("Cảnh báo Backfill", warn_msg)
                    except Exception as notify_err:
                        self.logger.error(
                            f"❌ Không thể gửi thông báo lỗi: {notify_err}"
                        )
            except Exception as e:
                err_msg: str = (
                    f"❌ [CafeF] [Backfill] Sự cố nghiêm trọng khi chạy backfill ngày {m_date.strftime('%Y-%m-%d')}: {e}"
                )
                self.logger.error(err_msg, exc_info=True)
                try:
                    self.notifier.send_alert("Lỗi Backfill Nghiêm Trọng", err_msg)
                except Exception as notify_err:
                    self.logger.error(
                        f"❌ [Telegram] Không thể gửi thông báo lỗi: {notify_err}"
                    )

    def _reload_adjusted_history(
        self,
        ticker: str,
        today_date: date,
        symbols_map: dict[str, str],
        df_t0: pl.DataFrame,
    ) -> bool:
        """Tải lại toàn bộ lịch sử giá điều chỉnh (từ năm 2000) của một mã và đồng bộ lên GCS/BigQuery.

        Args:
            ticker (str): Mã chứng khoán cần xử lý.
            today_date (date): Ngày giao dịch của phiên chạy hiện tại.
            symbols_map (dict[str, str]): Bản đồ mã chứng khoán sang sàn giao dịch tương ứng.
            df_t0 (pl.DataFrame): DataFrame dữ liệu T0.

        Returns:
            bool: True nếu quá trình reload và đồng bộ hoàn tất thành công, ngược lại False.
        """
        try:
            df_hist_adj: pl.DataFrame | None = self.processor.fetch_ohlcv(
                ticker,
                start_date=Config.HISTORICAL_START_DATE,
                end_date=datetime.now(Config.VN_TZ).strftime("%Y-%m-%d"),
                limit=15000,
            )

            if df_hist_adj is None or df_hist_adj.is_empty():
                self.logger.error(f"❌ Không thể tải lịch sử giá cho mã {ticker}")
                return False

            df_hist_adj = df_hist_adj.clone()
            df_hist_adj = df_hist_adj.with_columns(
                [pl.lit(ticker).alias("symbol"), pl.col("trading_date").cast(pl.Date)]
            )
            exchange_val = symbols_map.get(ticker, "Unknown")
            df_hist_adj = df_hist_adj.with_columns(
                pl.lit(exchange_val).alias("exchange").cast(pl.Categorical)
            )

            price_cols: list[str] = [
                "open_price",
                "high_price",
                "low_price",
                "close_price",
            ]
            df_hist_adj = df_hist_adj.with_columns(
                [pl.col(col).cast(pl.Float32) for col in price_cols]
                + [pl.col("total_volume").cast(pl.Int64)]
            )

            target_cols: list[str] = [
                "symbol",
                "trading_date",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "total_volume",
                "exchange",
                "source",
            ]
            df_hist_adj = df_hist_adj.select(target_cols)

            # Khắc phục rủi ro lag dữ liệu T0 từ API lịch sử bằng cách tự động bù từ bảng giá T0
            max_hist_date: date = df_hist_adj["trading_date"].max()
            if max_hist_date < today_date:
                self.logger.warning(
                    f"⚠️ Dữ liệu lịch sử điều chỉnh tải về cho mã {ticker} bị thiếu ngày hôm nay ({today_date}). "
                    "Tiến hành tự động bù dữ liệu T0..."
                )
                df_t0_sym: pl.DataFrame = df_t0.filter(pl.col("symbol") == ticker)
                if not df_t0_sym.is_empty():
                    t0_row: dict[str, Any] = df_t0_sym.row(0, named=True)
                    df_t0_append: pl.DataFrame = pl.DataFrame(
                        [
                            {
                                "symbol": ticker,
                                "trading_date": today_date,
                                "open_price": t0_row["open_price"],
                                "high_price": t0_row["high_price"],
                                "low_price": t0_row["low_price"],
                                "close_price": t0_row["close_price"],
                                "total_volume": t0_row["total_volume"],
                                "exchange": symbols_map.get(ticker, "Unknown"),
                                "source": t0_row.get("source")
                                or self.processor.source.lower(),
                            }
                        ],
                        schema={
                            "symbol": pl.String,
                            "trading_date": pl.Date,
                            "open_price": pl.Float32,
                            "high_price": pl.Float32,
                            "low_price": pl.Float32,
                            "close_price": pl.Float32,
                            "total_volume": pl.Int64,
                            "exchange": pl.Categorical,
                            "source": pl.String,
                        },
                    )

                    df_hist_adj = pl.concat([df_hist_adj, df_t0_append])
                    self.logger.info(f"✅ Đã bù thành công dữ liệu T0 cho mã {ticker}.")
                else:
                    self.logger.error(
                        f"❌ Không tìm thấy dữ liệu T0 của mã {ticker} trong df_t0 để bù."
                    )

            # Khắc phục trùng lặp ngày trong dữ liệu lịch sử điều chỉnh do lag API
            df_hist_adj = df_hist_adj.unique(subset=["trading_date"], keep="last")

            # Lưu trữ lên GCS Parquet
            self.storage.save_symbol_history(df_hist_adj, ticker, suffix="adj")
            return True
        except Exception as e:
            self.logger.error(
                f"❌ [Vnstock] Lỗi tải và lưu lên GCS lịch sử mã {ticker}: {e}",
                exc_info=True,
            )
            return False

    def verify_price_units(self, symbols_map: dict[str, str]) -> tuple[float, float]:
        """Kiểm tra sự đồng nhất đơn vị giá và xác định hệ số nhân động cho bảng giá T0 và OHLCV.

        Duyệt qua danh sách các mã cổ phiếu benchmark dự phòng để đảm bảo độ tin cậy.

        Args:
            symbols_map (dict[str, str]): Bản đồ mã chứng khoán sang sàn giao dịch tương ứng.

        Returns:
            tuple[float, float]: (board_multiplier, ohlcv_multiplier)
        """
        default_board_multiplier: float = 1.0
        default_ohlcv_multiplier: float = 1000.0
        if not symbols_map:
            return default_board_multiplier, default_ohlcv_multiplier

        # Tạo danh sách các mã để thử nghiệm từ config
        candidate_tickers: list[str] = [
            t for t in Config.BENCHMARK_TICKERS if t in symbols_map
        ]

        # Fallback nếu không có mã nào trong config nằm trong symbols_map
        if not candidate_tickers:
            fallback_ticker = next((s for s in symbols_map.keys() if len(s) == 3), "")
            if fallback_ticker:
                candidate_tickers.append(fallback_ticker)

        if not candidate_tickers:
            return default_board_multiplier, default_ohlcv_multiplier

        board_multipliers: list[float] = []
        ohlcv_multipliers: list[float] = []

        for ticker in candidate_tickers:
            try:
                self.logger.info(
                    f"🔍 [Vnstock] [Unit Check] Đang thử mã benchmark: {ticker}"
                )

                # 1. Lấy giá thô từ bảng giá T0
                self.processor.rate_limiter.hit()
                df_board_pd = self.processor.trading_api.price_board([ticker])
                if df_board_pd is not None and not df_board_pd.empty:
                    board_price = float(df_board_pd.iloc[0]["close_price"])
                    if not math.isnan(board_price) and board_price > 0:
                        # FPT, HPG, VNM, VIC luôn > 10,000đ. Nếu giá thô trả về < 1000.0 thì đơn vị là nghìn đồng -> nhân 1000.
                        bm = 1000.0 if board_price < 1000.0 else 1.0
                        board_multipliers.append(bm)

                # 2. Lấy giá thô ohlcv lịch sử gần đây (gọi trực tiếp API vnstock chưa nhân multiplier)
                today_str: str = datetime.now(Config.VN_TZ).strftime("%Y-%m-%d")
                start_date: str = (
                    datetime.now(Config.VN_TZ) - timedelta(days=7)
                ).strftime("%Y-%m-%d")

                self.processor.rate_limiter.hit()
                df_ohlcv_pd = self.processor.market_api.equity(ticker).ohlcv(
                    start=start_date,
                    end=today_str,
                    source=self.processor.source,
                    count=5,
                )
                if df_ohlcv_pd is not None and not df_ohlcv_pd.empty:
                    ohlcv_price = float(df_ohlcv_pd.iloc[-1]["close"])
                    if not math.isnan(ohlcv_price) and ohlcv_price > 0:
                        om = 1000.0 if ohlcv_price < 1000.0 else 1.0
                        ohlcv_multipliers.append(om)

            except Exception as e:
                self.logger.error(
                    f"❌ [Vnstock] [Unit Check] Lỗi kiểm tra đơn vị giá trên mã {ticker}: {e}"
                )

        # Lấy giá trị xuất hiện nhiều nhất (mode)
        final_board_multiplier = default_board_multiplier
        if board_multipliers:
            final_board_multiplier = max(
                set(board_multipliers), key=board_multipliers.count
            )

        final_ohlcv_multiplier = default_ohlcv_multiplier
        if ohlcv_multipliers:
            final_ohlcv_multiplier = max(
                set(ohlcv_multipliers), key=ohlcv_multipliers.count
            )

        self.logger.info(
            f"🎉 [Vnstock] [Unit Check] Kết quả đơn vị giá nhất quán - "
            f"Hệ số nhân T0: {final_board_multiplier} | Hệ số nhân OHLCV: {final_ohlcv_multiplier}"
        )
        return final_board_multiplier, final_ohlcv_multiplier

    def run(self) -> pl.DataFrame | None:
        """Khởi chạy toàn bộ quy trình ETL tải dữ liệu daily, backfill,
        xử lý sự kiện và đồng bộ lên GCP.

        Returns:
            Optional[pl.DataFrame]: DataFrame dữ liệu giao dịch T0.
        """
        symbols_map: dict[str, str] = self.processor.get_symbols_with_exchange()
        if not symbols_map:
            self.logger.warning(
                "⚠️ [Vnstock] Không thể tải danh sách mã từ API. Thử khôi phục từ cache..."
            )
            symbols_map = self.storage.get_state("active_symbols_cache") or {}
            if not symbols_map:
                err_msg = "Không tìm thấy danh sách mã trong cache và API đều thất bại."
                self.logger.error(f"❌ [Vnstock] {err_msg}")
                raise ValueError(err_msg)
            self.logger.info(
                f"✅ [Vnstock] Khôi phục thành công {len(symbols_map)} mã từ cache."
            )
        else:
            # Cập nhật cache mới nhất vào storage
            self.storage.save_state("active_symbols_cache", symbols_map)

        # Đồng bộ danh sách công ty từ vnstock vào DB (áp dụng ở cả Cloud và Local)
        # Kiểm tra xem có cần đồng bộ danh sách công ty & ngành hay không (tối thiểu 7 ngày/lần)
        last_sync_str = self.storage.get_state("last_company_sync_date")
        should_sync = True
        today_date = datetime.now(Config.VN_TZ).date()
        if last_sync_str:
            try:
                last_sync_date = datetime.strptime(last_sync_str, "%Y-%m-%d").date()
                if (
                    today_date - last_sync_date
                ).days < Config.COMPANY_SYNC_INTERVAL_DAYS:
                    should_sync = False
                    self.logger.info(
                        f"ℹ️ Danh sách công ty & ngành đã được đồng bộ vào {last_sync_str} (dưới {Config.COMPANY_SYNC_INTERVAL_DAYS} ngày trước). Bỏ qua đồng bộ hôm nay."
                    )
            except Exception as e:
                self.logger.warning(
                    f"⚠️ [Vnstock] Không thể phân tích ngày đồng bộ công ty cuối cùng {last_sync_str}: {e}"
                )

        if should_sync:
            try:
                self.logger.info("🏢 [Vnstock] Bắt đầu cào danh sách công ty...")
                df_symbols: pl.DataFrame = pl.from_pandas(
                    self.processor.reference_api.equity().list_by_exchange()
                ).filter(~pl.col("type").is_in(["corpbond", "bond", "future"]))

                # Tải danh sách ngành tổng hợp từ vnstock
                df_industry: pl.DataFrame = pl.from_pandas(
                    self.processor.reference_api.equity().list_by_industry()
                )

                # -----------------------------------------------------------------
                # KHỐI REFACTOR CHUẨN HÓA DANH MỤC NGÀNH ICB (Dựa trên icb_code)
                # -----------------------------------------------------------------
                # Bước 1: Tạo bảng tra cứu từ icb_code sang icb_name sạch sẽ, không trùng lặp
                icb_lookup: pl.DataFrame = df_industry.select(
                    ["icb_code", "icb_name"]
                ).unique(subset=["icb_code"], keep="first")
                icb_lookup_dict: dict[str, str] = dict(
                    zip(
                        icb_lookup["icb_code"].to_list(),
                        icb_lookup["icb_name"].to_list(),
                    )
                )

                # Bước 2: Xác định cấp độ lá (leaf level - icb_level lớn nhất) cho mỗi symbol
                # Polars: sort theo icb_level desc rồi lấy first row mỗi nhóm symbol
                leaf_cols = ["symbol", "icb_code", "icb_level"]
                if "com_type_code" in df_industry.columns:
                    leaf_cols.append("com_type_code")

                df_leaf: pl.DataFrame = (
                    df_industry.select(leaf_cols)
                    .sort("icb_level", descending=True)
                    .unique(subset=["symbol"], keep="first")
                    .rename({"icb_code": "icb_code_leaf"})
                )

                if "com_type_code" not in df_leaf.columns:
                    df_leaf = df_leaf.with_columns(
                        pl.lit(None).cast(pl.Utf8).alias("com_type_code")
                    )

                # Bước 3: Phân rã mã ngành từ mã lá dựa trên độ dài chuỗi quy chuẩn ICB kinh điển
                # (Cấp 1: 2 ký tự | Cấp 2: 4 ký tự | Cấp 3: 6 ký tự | Cấp 4: 8+ ký tự)
                # Kỹ thuật vector hóa này giúp tránh việc merge lặp lại qua symbol
                df_leaf = df_leaf.with_columns(
                    pl.col("icb_code_leaf")
                    .cast(pl.Utf8)
                    .str.strip_chars()
                    .alias("icb_code_leaf")
                ).with_columns(
                    # Cấp 1: Chữ số đầu tiên + "000". Nếu bắt đầu bằng "0" thì trả về "0001" (Dầu khí)
                    pl.when(pl.col("icb_code_leaf").str.starts_with("0"))
                    .then(pl.lit("0001"))
                    .otherwise(pl.col("icb_code_leaf").str.slice(0, 1) + "000")
                    .alias("icb_l1_code"),
                    # Cấp 2: 2 chữ số đầu + "00"
                    (pl.col("icb_code_leaf").str.slice(0, 2) + "00").alias(
                        "icb_l2_code"
                    ),
                    # Cấp 3: 3 chữ số đầu + "0"
                    (pl.col("icb_code_leaf").str.slice(0, 3) + "0").alias(
                        "icb_l3_code"
                    ),
                    # Cấp 4 (Lá)
                    pl.col("icb_code_leaf").alias("icb_code"),
                )

                # Bước 4: Ánh xạ tên ngành (icb_name) từ bảng tra cứu từ điển (Cực nhanh và không rò rỉ RAM)
                df_leaf = df_leaf.with_columns(
                    pl.col("icb_l1_code")
                    .replace(icb_lookup_dict, default="Unknown")
                    .alias("icb_l1_name"),
                    pl.col("icb_l2_code")
                    .replace(icb_lookup_dict, default="Unknown")
                    .alias("icb_l2_name"),
                    pl.col("icb_l3_code")
                    .replace(icb_lookup_dict, default="Unknown")
                    .alias("icb_l3_name"),
                    pl.col("icb_code")
                    .replace(icb_lookup_dict, default="Unknown")
                    .alias("icb_name"),
                )

                # Bước 5: Tạo bảng phẳng icb_industries duy nhất để lưu danh mục gốc
                df_icb_industries: pl.DataFrame = df_leaf.select(
                    [
                        "icb_code",
                        "icb_l1_code",
                        "icb_l1_name",
                        "icb_l2_code",
                        "icb_l2_name",
                        "icb_l3_code",
                        "icb_l3_name",
                        "icb_name",
                    ]
                ).unique(subset=["icb_code"], keep="first")

                # Lưu danh mục ngành ICB phẳng vào database trước
                self.storage.save_icb_industries(df_icb_industries)

                # Bước 6: Tiến hành Merge danh mục ngành lá vào danh sách cổ phiếu symbols gốc
                df_merged: pl.DataFrame = df_symbols.join(
                    df_leaf.select(["symbol", "icb_code", "com_type_code"]),
                    on="symbol",
                    how="left",
                )

                df_companies: pl.DataFrame = df_merged.select(
                    [
                        pl.col("symbol")
                        .str.strip_chars()
                        .str.to_uppercase()
                        .alias("symbol"),
                        get_exchange_normalization_expr("exchange"),
                        pl.col("organ_name")
                        .str.strip_chars()
                        .fill_null("Unknown")
                        .alias("company_name"),
                        pl.col("icb_code"),
                        pl.col("com_type_code"),
                        pl.col("type"),
                        pl.lit("active").alias("status"),
                    ]
                ).unique(subset=["symbol"], keep="first")
                self.storage.save_companies(df_companies)
                self.logger.info(
                    f"✅ [Vnstock] Đã đồng bộ {df_companies.height} công ty và ngành ICB vào database."
                )

                # Cập nhật ngày đồng bộ thành công vào DB
                self.storage.save_state(
                    "last_company_sync_date", today_date.strftime("%Y-%m-%d")
                )
            except Exception as e:
                self.logger.error(
                    f"❌ [Vnstock] Lỗi khi đồng bộ danh sách công ty và phân cấp ICB: {e}",
                    exc_info=True,
                )

        board_mult, ohlcv_mult = self.verify_price_units(symbols_map)
        self.processor.board_multiplier = board_mult
        self.processor.ohlcv_multiplier = ohlcv_mult

        symbols: list[str] = list(symbols_map.keys())

        df_t0: pl.DataFrame = self.processor.fetch_entire_market_t0(symbols)
        if df_t0.is_empty():
            err_msg: str = (
                "Không lấy được bất kỳ dữ liệu bảng giá T0 nào từ thị trường. "
                "API nguồn có thể gặp sự cố hoặc bị chặn."
            )
            self.logger.error(f"🛑 {err_msg}")
            raise ValueError(err_msg)

        # Áp dụng hệ số nhân đơn vị giá động sau khi tự động kiểm tra
        if board_mult != 1.0:
            self.logger.info(
                f"⚡ [Vnstock] Áp dụng hệ số nhân {board_mult} cho bảng giá T0 để đồng nhất đơn vị."
            )
            price_cols: list[str] = [
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "reference_price",
                "average_price",
            ]
            df_t0 = df_t0.with_columns(
                [pl.col(c) * board_mult for c in price_cols if c in df_t0.columns]
            )

        # 1. Khởi tạo và kiểm tra trạng thái lịch chạy gần nhất
        init_res: tuple[date, list[date], list[str], dict[str, Any]] | None = (
            self._initialize_run_dates(df_t0)
        )
        if init_res is None:
            return None
        today_date, missing_dates, pending_reloads, latest_state = init_res

        # 2. Phát hiện các sự kiện chia cổ tức/phát hành thêm cổ phiếu trong phiên T0
        detected_corporate_actions: dict[str, date] = {}
        try:
            detected_corporate_actions = self._detect_corporate_actions_today(
                df_t0, today_date
            )
        except Exception as e:
            self.logger.error(
                f"⚠️ [Events] Thất bại khi quét sự kiện doanh nghiệp hôm nay: {e}"
            )
            try:
                Notifier(self.logger).send_alert(
                    "Cảnh báo Quét Sự Kiện Doanh Nghiệp T0",
                    f"Không thể quét sự kiện hôm nay do lỗi API: {e}. "
                    "Hệ thống vẫn tiếp tục chạy tải dữ liệu T0.",
                )
            except Exception as notify_err:
                self.logger.error(
                    f"❌ [Telegram] Không thể gửi thông báo: {notify_err}"
                )

        # 3. Phát hiện sự kiện doanh nghiệp của các ngày còn thiếu (Backfill) thông qua quét API
        if missing_dates:
            try:
                backfill_start: date = min(missing_dates)
                backfill_end: date = max(missing_dates)
                detected_backfill_events = (
                    self.processor.detect_corporate_actions_via_api(
                        symbols, backfill_start, backfill_end
                    )
                )
                self.storage.save_corporate_events(detected_backfill_events)
                detected_backfill_map = {
                    e["symbol"]: e["ex_date"] for e in detected_backfill_events
                }
                if detected_backfill_map:
                    self.logger.warning(
                        f"⚠️ [Events] [Backfill] Phát hiện {len(detected_backfill_map)} mã có sự kiện: "
                        f"{list(detected_backfill_map.keys())}"
                    )
                    detected_corporate_actions.update(detected_backfill_map)
            except Exception as e:
                self.logger.error(
                    f"⚠️ [Events] Lỗi quét sự kiện doanh nghiệp Backfill qua API: {e}",
                    exc_info=True,
                )

        # Hợp nhất danh sách mã lỗi của phiên chạy trước để thử lại trong hôm nay
        if pending_reloads:
            self.logger.warning(
                f"🔄 [Events] Phát hiện {len(pending_reloads)} mã lỗi reload phiên trước, "
                f"đưa vào chạy lại hôm nay: {pending_reloads}"
            )
            ticker_pattern: re.Pattern[str] = re.compile(r"^[A-Z0-9]{3,10}$")
            for p_ticker in pending_reloads:
                p_ticker_clean: str = str(p_ticker).strip().upper()
                if ticker_pattern.match(p_ticker_clean):
                    detected_corporate_actions[p_ticker_clean] = today_date
                else:
                    self.logger.warning(
                        f"⚠️ [Events] Loại bỏ mã lỗi reload không hợp lệ: {p_ticker}"
                    )

        # 4. Thực hiện chạy bù dữ liệu thô (Backfill) cho những ngày bị thiếu
        if missing_dates:
            self._backfill_missing_history(missing_dates)

        # 5. Lưu dữ liệu thô T0 lên GCS và nạp vào BigQuery
        df_t0_parquet: pl.DataFrame = df_t0.drop(
            [c for c in ["reference_price", "average_price"] if c in df_t0.columns]
        )
        gcs_path_t0: str | None = self.storage.save_parquet(
            df_t0_parquet,
            datetime.combine(today_date, datetime.min.time()),
            partition=True,
        )

        if gcs_path_t0:
            self.storage.sync_partition_to_bigquery(
                gcs_path_t0, Config.BQ_RAW_TABLE, today_date
            )

        # 6. Tải lại toàn bộ lịch sử Giá điều chỉnh cho các mã có sự kiện doanh nghiệp
        failed_reloads: list[str] = []
        successful_reloads: list[str] = []
        if detected_corporate_actions:
            tickers_list: list[str] = sorted(list(detected_corporate_actions.keys()))
            self.logger.warning(
                f"🚀 [Events] Bắt đầu tải lại lịch sử giá điều chỉnh cho {len(tickers_list)} mã: "
                f"{tickers_list}..."
            )

            # Chia nhỏ danh sách mã cần reload thành các lô để tránh rủi ro OOM và giải phóng RAM sớm
            reload_batch_size: int = Config.RELOAD_BATCH_SIZE

            # === THÊM BIẾN ĐẾM THẤT BẠI LIÊN TIẾP ===
            consecutive_failures: int = 0
            CRITICAL_FAILURE_THRESHOLD: int = Config.CRITICAL_FAILURE_THRESHOLD
            api_blocked: bool = False

            for b_idx in range(0, len(tickers_list), reload_batch_size):
                if api_blocked:
                    self.logger.error(
                        "🛑 Dừng toàn bộ lượt reload giá điều chỉnh còn lại do API Vnstock lỗi liên tục."
                    )
                    break

                batch_tickers: list[str] = tickers_list[
                    b_idx : b_idx + reload_batch_size
                ]
                self.logger.info(
                    f"⚡ [Events] [Reload Batch] Đang xử lý lô {b_idx // reload_batch_size + 1} "
                    f"({len(batch_tickers)} mã): {batch_tickers}"
                )

                successful_batch: list[str] = []
                for ticker in batch_tickers:
                    success: bool = self._reload_adjusted_history(
                        ticker, today_date, symbols_map, df_t0
                    )
                    if not success:
                        failed_reloads.append(ticker)
                        # === XỬ LÝ LOGIC CẢNH BÁO CRITICAL ===
                        consecutive_failures += 1
                        if consecutive_failures >= CRITICAL_FAILURE_THRESHOLD:
                            alert_msg: str = (
                                f"Hàm fetch_ohlcv đã trả về None liên tục cho {consecutive_failures} mã. "
                                f"Ngừng toàn bộ tiến trình reload lịch sử hôm nay để tránh spam và bảo vệ IP."
                            )
                            self.logger.critical(f"🚨 [Vnstock] {alert_msg}")
                            try:
                                self.notifier.send_alert(
                                    "CRITICAL: Lỗi Vnstock API", alert_msg
                                )
                            except Exception as notify_err:
                                self.logger.error(
                                    f"❌ [Telegram] Không thể gửi cảnh báo khẩn cấp: {notify_err}"
                                )
                            api_blocked = True
                            break
                    else:
                        successful_batch.append(ticker)
                        successful_reloads.append(ticker)
                        consecutive_failures = (
                            0  # Reset bộ đếm nếu có 1 mã fetch thành công
                        )

                # Đồng bộ gộp dữ liệu của lô hiện tại lên database ngay lập tức
                if successful_batch:
                    self.logger.info(
                        f"⚡ [Events] Đồng bộ gộp lên database cho các mã thành công của lô: {successful_batch}"
                    )
                    self.storage.sync_adjusted_symbols_to_bigquery(successful_batch)
        else:
            self.logger.info(
                "ℹ️ [Events] Không phát hiện mã cổ phiếu nào cần tải lại lịch sử giá điều chỉnh."
            )

        # 7. Đồng bộ dữ liệu giá từ bảng raw sang bảng adjusted (tránh các mã có sự kiện)
        all_processing_dates: list[date] = sorted(missing_dates + [today_date])
        all_excluded_symbols: list[str] = list(detected_corporate_actions.keys())
        # Tối ưu hóa: Gọi lệnh SQL bulk đồng bộ tất cả các ngày trong một Transaction duy nhất
        self.storage.sync_daily_adjusted_prices(
            all_processing_dates, all_excluded_symbols
        )

        # 8. Cập nhật Checkpoint Snapshot EOD và lưu danh sách mã bị lỗi để xử lý sau
        self.storage.save_checkpoint(
            df=df_t0,
            active_symbols=set(symbols),
            pending_adjusted_reloads=failed_reloads,
        )
        self.logger.info(
            "✅ [Vnstock] Đã lưu checkpoint trạng thái thị trường EOD thành công."
        )

        # 8.5. Trích xuất dữ liệu của các mã cổ phiếu quan tâm lên GCS
        export_summary: dict[str, Any] | None = None
        try:
            export_summary = self.storage.export_interested_tickers_data()
        except Exception as export_err:
            self.logger.error(
                f"❌ [Vnstock] Lỗi khi trích xuất các mã cổ phiếu quan tâm: {export_err}",
                exc_info=True,
            )

        # 9. Gửi báo cáo thông báo trạng thái kết thúc phiên chạy qua Telegram
        try:
            vn_now: datetime = datetime.now(Config.VN_TZ)
            today_str_check: str = vn_now.strftime("%Y-%m-%d")
            t0_date_str: str = today_date.strftime("%Y-%m-%d")
            run_is_eod: bool = (
                t0_date_str < today_str_check
                or vn_now.hour > Config.EOD_HOUR
                or (
                    vn_now.hour == Config.EOD_HOUR
                    and vn_now.minute >= Config.EOD_MINUTE
                )
            )
            reloaded_symbols: list[str] = [
                s for s in detected_corporate_actions.keys() if s not in failed_reloads
            ]

            self.notifier.send_summary(
                date_str=t0_date_str,
                total_processed=df_t0.height,
                is_eod=run_is_eod,
                missing_dates=missing_dates,
                reloaded_symbols=reloaded_symbols,
                failed_reloads=failed_reloads,
                export_summary=export_summary,
            )
        except Exception as notify_err:
            self.logger.error(
                f"❌ [Telegram] Không thể gửi báo cáo chạy daily: {notify_err}"
            )

        return df_t0
