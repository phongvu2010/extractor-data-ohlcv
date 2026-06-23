"""Module thực hiện tải, xử lý và làm sạch dữ liệu chứng khoán từ nguồn Vnstock API."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import gc
import logging
import re
import time
from typing import Any

import numpy as np
import pandas as pd
import requests
from vnstock import Reference, Trading
from vnstock.core.utils.user_agent import get_headers
from vnstock.ui import Market

from config import Config
from notifier import Notifier
from storages import BaseStorage, get_storage
from utils import normalize_exchange, setup_logger, SmartRateLimiter

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

    def get_symbols_with_exchange(self) -> dict[str, str]:
        """Tải danh sách mã cổ phiếu đang hoạt động cùng với sàn giao dịch tương ứng.

        Returns:
            dict[str, str]: Dict mapping giữa mã chứng khoán (symbol) và sàn niêm yết (exchange).
        """
        self.rate_limiter.hit()
        try:
            df_symbols: pd.DataFrame = self.reference_api.equity().list_by_exchange()
            # Loại trừ trái phiếu, trái phiếu doanh nghiệp và chứng khoán phái sinh
            df_symbols = df_symbols[
                ~df_symbols["type"].isin(["corpbond", "bond", "future"])
            ]
            return dict(zip(df_symbols["symbol"], df_symbols["exchange"]))
        except Exception as e:
            self.logger.error(f"🛑 Không thể lấy danh sách symbol từ vnstock: {e}")
            return {}

    def fetch_entire_market_t0(self, symbols: list[str]) -> pd.DataFrame:
        """Tải dữ liệu bảng giá ngày hiện hành T0 hàng loạt cho tất cả các mã chứng khoán.

        Args:
            symbols (list[str]): Danh sách mã cổ phiếu cần tải thông tin bảng giá.

        Returns:
            pd.DataFrame: DataFrame dữ liệu giao dịch T0 hoàn chỉnh của toàn bộ thị trường.
        """
        self.logger.info(
            f"📥 [Bulk Fetch] Đang kéo bảng giá T0 cho {len(symbols)} mã vào RAM..."
        )

        if not symbols:
            self.logger.warning(
                "⚠️ Danh sách symbols trống. Không thể tải bảng giá T0."
            )
            return pd.DataFrame()

        dfs: list[pd.DataFrame] = []
        batch_size: int = 500
        for i in range(0, len(symbols), batch_size):
            batch: list[str] = symbols[i : i + batch_size]

            max_retries: int = 3
            initial_delay: float = 10.0
            backoff_factor: float = 2.0
            delay: float = initial_delay
            df_quote: pd.DataFrame | None = None
            success: bool = False

            for attempt in range(1, max_retries + 1):
                try:
                    self.rate_limiter.hit()
                    df_quote = self.trading_api.price_board(batch)
                    if df_quote is not None and not df_quote.empty:
                        success = True
                        break
                    raise ValueError("Dữ liệu bảng giá trống hoặc None")
                except requests.exceptions.Timeout as e:
                    self.logger.error(
                        f"⏳ Lỗi Timeout từ Vnstock khi kéo lô {i} (lần thử {attempt}/{max_retries}): {e}."
                    )
                except requests.exceptions.ConnectionError as e:
                    self.logger.error(
                        f"🔌 Lỗi kết nối mạng khi kéo lô {i} từ Vnstock (lần thử {attempt}/{max_retries}): {e}."
                    )
                except Exception as e:
                    self.logger.error(
                        f"⚠️ Lỗi không xác định khi kéo bảng giá T0 lô {i} (lần thử {attempt}/{max_retries}): {e}"
                    )

                if attempt < max_retries:
                    self.logger.info(f"⏳ Thử lại lô {i} sau {delay} giây...")
                    time.sleep(delay)
                    delay *= backoff_factor

            if not success or df_quote is None or df_quote.empty:
                raise RuntimeError(
                    f"❌ Tất cả các lần thử tải bảng giá T0 cho lô từ {i} đến {min(i + batch_size, len(symbols))} đều thất bại. "
                    "Hủy pipeline để tránh thiếu hụt dữ liệu."
                )

            dfs.append(df_quote)

        df_all: pd.DataFrame = pd.concat(dfs, ignore_index=True)
        df_all["exchange"] = df_all["exchange"].apply(normalize_exchange)

        # Chuyển đổi timestamp Unix mili-giây sang ngày giờ Việt Nam chuẩn không có múi giờ
        df_all["trading_date"] = (
            pd.to_datetime(df_all["time"], unit="ms")
            .dt.tz_localize("UTC")
            .dt.tz_convert("Asia/Ho_Chi_Minh")
            .dt.normalize()
            .dt.tz_localize(None)
        )
        df_all["total_volume"] = df_all["volume_accumulated"]

        # Loại bỏ các giá trị vô cực (inf) hoặc NaN trước khi tiến hành các phép lọc số học
        price_cols: list[str] = ["open_price", "high_price", "low_price", "close_price"]
        df_all[price_cols] = df_all[price_cols].replace([np.inf, -np.inf], np.nan)
        df_all = df_all.dropna(subset=price_cols + ["total_volume", "trading_date"])
        df_all = df_all[
            ~(
                (df_all["open_price"] <= 0)
                | (df_all["high_price"] <= 0)
                | (df_all["low_price"] <= 0)
                | (df_all["close_price"] <= 0)
                | (df_all["total_volume"] <= 0)
            )
        ]

        df_all["symbol"] = (
            df_all["symbol"].astype(str).str.strip().str.upper().astype("category")
        )
        df_all["exchange"] = df_all["exchange"].astype(
            pd.CategoricalDtype(categories=["HoSE", "HNX", "UPCoM", "Unknown"])
        )

        df_all[price_cols] = df_all[price_cols].astype("float32")
        df_all["reference_price"] = df_all["reference_price"].astype("float32")
        df_all["average_price"] = df_all["average_price"].astype("float32")
        df_all["total_volume"] = df_all["total_volume"].astype("Int64")
        df_all["source"] = self.source.lower()

        return df_all[
            [
                "symbol",
                "trading_date",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "total_volume",
                "exchange",
                "source",
                "reference_price",
                "average_price",
            ]
        ]

    def fetch_ohlcv(
        self, symbol: str, start_date: str, end_date: str, limit: int = 100
    ) -> pd.DataFrame | None:
        """Tải dữ liệu lịch sử OHLCV cho một mã chứng khoán trong khoảng thời gian xác định.

        Args:
            symbol (str): Mã cổ phiếu cần tải (ví dụ: 'FPT').
            start_date (str): Ngày bắt đầu định dạng YYYY-MM-DD.
            end_date (str): Ngày kết thúc định dạng YYYY-MM-DD.
            limit (int): Số dòng tối đa cho phép tải về.

        Returns:
            Optional[pd.DataFrame]: DataFrame chứa lịch sử OHLCV của mã cổ phiếu, trả về None nếu tất cả các nguồn thất bại.
        """
        # Tạo danh sách các nguồn để quét thử nếu nguồn chính gặp sự cố
        sources: list[str] = [self.source, "kbs", "msn"]
        # Loại bỏ các phần tử trùng lặp nhưng vẫn giữ nguyên thứ tự ưu tiên
        sources = list(dict.fromkeys([s for s in sources if s]))

        for src in sources:
            self.rate_limiter.hit()
            try:
                self.logger.info(
                    f"📥 Đang tải dữ liệu ohlcv cho mã {symbol} từ nguồn: {src}"
                )
                df_ohclv: pd.DataFrame | None = self.market_api.equity(symbol).ohlcv(
                    start=start_date,
                    end=end_date,
                    source=src,
                    count=limit,
                )
                if df_ohclv is not None and not df_ohclv.empty:
                    df_ohclv = df_ohclv.copy()
                    df_ohclv.rename(
                        columns={
                            "time": "trading_date",
                            "open": "open_price",
                            "high": "high_price",
                            "low": "low_price",
                            "close": "close_price",
                            "volume": "total_volume",
                        },
                        inplace=True,
                    )

                    # Loại bỏ các giá trị vô cực (inf) hoặc NaN trước khi nhân để tránh lỗi tràn số (overflow)
                    price_cols: list[str] = [
                        "open_price",
                        "high_price",
                        "low_price",
                        "close_price",
                    ]
                    df_ohclv[price_cols] = df_ohclv[price_cols].replace(
                        [np.inf, -np.inf], np.nan
                    )
                    df_ohclv = df_ohclv.dropna(subset=price_cols)

                    # Quy đổi đơn vị giá Vnstock (nghìn đồng) về đồng giống file CafeF
                    df_ohclv[price_cols] *= Config.PRICE_MULTIPLIER
                    df_ohclv["source"] = src.lower()

                    return df_ohclv
            except Exception as e:
                self.logger.warning(
                    f"⚠️ Nguồn {src} gặp lỗi khi tải mã {symbol}: {e}. Đang thử nguồn tiếp theo..."
                )

        self.logger.error(
            f"❌ Tất cả các nguồn dữ liệu ohlcv đều thất bại cho mã {symbol}."
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
            f"🔍 [Corporate Actions API] Quét sự kiện từ {start_date} đến {end_date} cho {len(symbols)} mã..."
        )

        headers: dict[str, str] = get_headers(data_source="VCI")

        all_dfs: list[pd.DataFrame] = []
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
            df_batch: pd.DataFrame | None = None
            for attempt in range(1, max_retries + 1):
                try:
                    self.rate_limiter.hit()
                    response = requests.get(url, headers=headers, timeout=30)
                    if response.status_code == 200:
                        data = response.json()
                        events_list = data.get("data", {}).get("content", [])
                        if events_list:
                            df_batch = pd.DataFrame(events_list)
                        break
                    else:
                        raise ValueError(f"HTTP status {response.status_code}")
                except Exception as e:
                    self.logger.warning(
                        f"⚠️ Lỗi khi tải sự kiện lô {i} (lần {attempt}/{max_retries}): {e}"
                    )
                    if attempt < max_retries:
                        time.sleep(2.0 * attempt)

            if df_batch is not None and not df_batch.empty:
                all_dfs.append(df_batch)

        if not all_dfs:
            return []

        df_events: pd.DataFrame = pd.concat(all_dfs, ignore_index=True)
        rename_map: dict[str, str] = {
            "exrightDate": "exright_date",
            "organCode": "organ_code",
        }
        df_events = df_events.rename(columns=rename_map)

        if "exright_date" not in df_events.columns:
            return []

        df_events = df_events[df_events["exright_date"].notna()]
        if df_events.empty:
            return []

        df_events["ex_date"] = pd.to_datetime(df_events["exright_date"]).dt.date

        # Chỉ lọc lấy các sự kiện nằm hoàn toàn trong khoảng thời gian cần kiểm tra
        df_filtered: pd.DataFrame = df_events[
            (df_events["ex_date"] >= start_date) & (df_events["ex_date"] <= end_date)
        ]

        if df_filtered.empty:
            return []

        # Ưu tiên lấy sự kiện sớm nhất nếu có nhiều sự kiện phát sinh cho một mã
        df_filtered = df_filtered.sort_values(by="ex_date")

        events: list[dict[str, Any]] = []
        ticker_pattern: re.Pattern = re.compile(r"^[A-Z0-9]{3,10}$")

        for _, row in df_filtered.iterrows():
            ticker: str = str(row.get("ticker", "")).strip().upper()
            if not ticker or ticker in ["", "NAN", "NONE", "<NA>"]:
                ticker = str(row.get("organ_code", "")).strip().upper()

            ex_date: Any = row["ex_date"]
            if ticker and ticker_pattern.match(ticker) and isinstance(ex_date, date):
                rec_date_val = None
                if "recordDate" in row and pd.notna(row["recordDate"]):
                    try:
                        rec_date_val = pd.to_datetime(row["recordDate"]).date()
                    except Exception:
                        pass
                ratio_val = (
                    str(row.get("exerciseRatio", ""))
                    if pd.notna(row.get("exerciseRatio"))
                    else None
                )
                events.append(
                    {
                        "symbol": ticker,
                        "event_type": row.get("eventCode", "DIV"),
                        "ex_date": ex_date,
                        "record_date": rec_date_val,
                        "ratio": ratio_val,
                    }
                )

        return events


class VnstockExtractorETL:
    """Bộ điều phối chính của Vnstock Daily Pipeline."""

    logger: logging.Logger
    processor: DataProcessor
    storage: BaseStorage

    def __init__(self, logger_name: str = Config.DEFAULT_LOGGER_NAME) -> None:
        """Khởi tạo các phân lớp phục vụ xử lý và lưu trữ dữ liệu.

        Args:
            logger_name (str): Tên Logger chung của hệ thống.
        """
        self.logger = setup_logger(logger_name)
        self.processor = DataProcessor(self.logger)
        self.storage = get_storage(Config.DEPLOYMENT_ENV, self.logger)

    def _initialize_run_dates(
        self, df_t0: pd.DataFrame
    ) -> tuple[date, list[date], list[str], dict[str, Any]] | None:
        """Phân tích checkpoint và kiểm tra tính hợp lệ về ngày chạy dữ liệu mới.

        Args:
            df_t0 (pd.DataFrame): DataFrame dữ liệu phiên T0 tải về.

        Returns:
            Optional[tuple[date, list[date], list[str], dict[str, Any]]]: Tuple chứa các thông tin
                (t0_max_date, missing_dates, pending_reloads, latest_state)
                hoặc None nếu hệ thống dừng chạy do trùng lẫn hoặc không có lịch sử mới.
        """
        t0_max_date: date = df_t0["trading_date"].dt.date.max()

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
                    f"📅 Ngày chạy daily cuối cùng thành công: {date_latest_state} "
                    f"({'Đã chốt phiên EOD' if is_eod else 'Chưa chốt phiên EOD'})"
                )
            except Exception as e:
                self.logger.warning(
                    f"⚠️ Không thể phân tích ngày chạy cuối cùng {last_run_str}: {e}"
                )

        # Chốt chặn thời gian: Dừng chạy nếu ngày tải về cũ hơn ngày đã chạy thành công gần nhất
        if date_latest_state and t0_max_date < date_latest_state:
            self.logger.warning(
                f"ℹ️ Ngày giao dịch lớn nhất của dữ liệu mới tải ({t0_max_date}) "
                f"cũ hơn ngày đã chạy thành công gần nhất ({date_latest_state}). Dừng pipeline."
            )
            return None

        if date_latest_state and t0_max_date == date_latest_state and is_eod:
            self.logger.info(
                f"ℹ️ Ngày giao dịch {t0_max_date} đã được chạy thành công và chốt phiên EOD trước đó. Dừng pipeline."
            )
            return None

        if date_latest_state and t0_max_date == date_latest_state and not is_eod:
            self.logger.warning(
                f"🔔 Chạy lại ngày {t0_max_date} do phiên chạy trước chưa chốt EOD. Cập nhật dữ liệu..."
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
        self, df_t0: pd.DataFrame, today_date: date
    ) -> dict[str, date]:
        """Phát hiện các sự kiện doanh nghiệp của phiên T0 thông qua quét API.

        Args:
            df_t0 (pd.DataFrame): DataFrame dữ liệu T0 để lấy danh sách các mã giao dịch cần quét.
            today_date (date): Ngày giao dịch hôm nay.

        Returns:
            dict[str, date]: Dict chứa các mã cổ phiếu và ngày có sự kiện tương ứng.
        """
        symbols: list[str] = [str(s) for s in df_t0["symbol"].unique() if s]
        self.logger.info(
            f"🔍 [Corporate Actions T0] Đang quét trực tiếp API sự kiện hôm nay cho {len(symbols)} mã..."
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
            f"🚀 Phát hiện {len(missing_dates)} ngày thiếu cần backfill. Tiến hành tải qua CafeF..."
        )
        from extractors.extractor_cafef import CafeFExtractorETL

        cafe_etl: CafeFExtractorETL = CafeFExtractorETL(
            logger_name=self.logger.name, storage=self.storage
        )

        for m_date in sorted(missing_dates):
            self.logger.info(
                f"📅 [Backfill CafeF] Đang tải dữ liệu thô cho ngày {m_date}..."
            )
            dt_ref: datetime = datetime.combine(m_date, datetime.min.time())
            try:
                res: pd.DataFrame | None = cafe_etl.run(
                    dt_ref, is_raw=True, partition=True, save_checkpoint=False
                )
                if res is None:
                    # Cảnh báo mềm qua log và Telegram, không quăng lỗi làm sập hệ thống
                    warn_msg: str = (
                        f"⚠️ [Backfill CafeF] Không tải được dữ liệu cho ngày {m_date.strftime('%Y-%m-%d')}. "
                        "Có thể đây là ngày nghỉ giao dịch đột xuất hoặc lỗi CDN CafeF. Bỏ qua ngày này."
                    )
                    self.logger.warning(warn_msg)
                    try:
                        Notifier(self.logger).send_alert("Cảnh báo Backfill", warn_msg)
                    except Exception as notify_err:
                        self.logger.error(
                            f"❌ Không thể gửi thông báo lỗi: {notify_err}"
                        )
            except Exception as e:
                err_msg: str = (
                    f"❌ Gặp sự cố nghiêm trọng khi chạy backfill cho ngày {m_date.strftime('%Y-%m-%d')}: {e}"
                )
                self.logger.error(err_msg, exc_info=True)
                try:
                    Notifier(self.logger).send_alert(
                        "Lỗi Backfill Nghiêm Trọng", err_msg
                    )
                except Exception as notify_err:
                    self.logger.error(f"❌ Không thể gửi thông báo lỗi: {notify_err}")

    def _reload_adjusted_history(
        self,
        ticker: str,
        today_date: date,
        symbols_map: dict[str, str],
        df_t0: pd.DataFrame,
    ) -> bool:
        """Tải lại toàn bộ lịch sử giá điều chỉnh (từ năm 2000) của một mã và đồng bộ lên GCS/BigQuery.

        Args:
            ticker (str): Mã chứng khoán cần xử lý.
            today_date (date): Ngày giao dịch của phiên chạy hiện tại.
            symbols_map (dict[str, str]): Bản đồ mã chứng khoán sang sàn giao dịch tương ứng.
            df_t0 (pd.DataFrame): DataFrame dữ liệu T0.

        Returns:
            bool: True nếu quá trình reload và đồng bộ hoàn tất thành công, ngược lại False.
        """
        try:
            df_hist_adj: pd.DataFrame | None = self.processor.fetch_ohlcv(
                ticker,
                start_date="2000-01-01",
                end_date=datetime.now(Config.VN_TZ).strftime("%Y-%m-%d"),
                limit=15000,
            )

            if df_hist_adj is None or df_hist_adj.empty:
                self.logger.error(f"❌ Không thể tải lịch sử giá cho mã {ticker}")
                return False

            df_hist_adj = df_hist_adj.copy()
            df_hist_adj["symbol"] = ticker
            df_hist_adj["exchange"] = symbols_map.get(ticker, "Unknown")
            df_hist_adj["symbol"] = (
                df_hist_adj["symbol"]
                .astype(str)
                .str.strip()
                .str.upper()
                .astype("category")
            )
            df_hist_adj["exchange"] = (
                df_hist_adj["exchange"]
                .apply(normalize_exchange)
                .astype(
                    pd.CategoricalDtype(categories=["HoSE", "HNX", "UPCoM", "Unknown"])
                )
            )
            df_hist_adj["trading_date"] = pd.to_datetime(
                df_hist_adj["trading_date"]
            ).dt.normalize()
            df_hist_adj = df_hist_adj.dropna(subset=["trading_date"])

            price_cols: list[str] = [
                "open_price",
                "high_price",
                "low_price",
                "close_price",
            ]
            df_hist_adj[price_cols] = df_hist_adj[price_cols].astype("float32")
            df_hist_adj["total_volume"] = df_hist_adj["total_volume"].astype("Int64")

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
            df_hist_adj = df_hist_adj[target_cols]

            # Khắc phục rủi ro lag dữ liệu T0 từ API lịch sử bằng cách tự động bù từ bảng giá T0
            max_hist_date: date = df_hist_adj["trading_date"].dt.date.max()
            if max_hist_date < today_date:
                self.logger.warning(
                    f"⚠️ Dữ liệu lịch sử điều chỉnh tải về cho mã {ticker} bị thiếu ngày hôm nay ({today_date}). "
                    "Tiến hành tự động bù dữ liệu T0..."
                )
                df_t0_sym: pd.DataFrame = df_t0[df_t0["symbol"] == ticker]
                if not df_t0_sym.empty:
                    t0_row: pd.Series = df_t0_sym.iloc[0]
                    df_t0_append: pd.DataFrame = pd.DataFrame(
                        [
                            {
                                "symbol": ticker,
                                "trading_date": pd.to_datetime(today_date),
                                "open_price": t0_row["open_price"],
                                "high_price": t0_row["high_price"],
                                "low_price": t0_row["low_price"],
                                "close_price": t0_row["close_price"],
                                "total_volume": t0_row["total_volume"],
                                "exchange": symbols_map.get(ticker, "Unknown"),
                                "source": t0_row.get("source", self.source.lower()),
                            }
                        ]
                    )
                    df_t0_append["symbol"] = (
                        df_t0_append["symbol"]
                        .astype(str)
                        .str.strip()
                        .str.upper()
                        .astype("category")
                    )
                    df_t0_append["exchange"] = (
                        df_t0_append["exchange"]
                        .apply(normalize_exchange)
                        .astype(
                            pd.CategoricalDtype(
                                categories=["HoSE", "HNX", "UPCoM", "Unknown"]
                            )
                        )
                    )
                    df_t0_append["trading_date"] = pd.to_datetime(
                        df_t0_append["trading_date"]
                    ).dt.normalize()
                    df_t0_append[price_cols] = df_t0_append[price_cols].astype(
                        "float32"
                    )
                    df_t0_append["total_volume"] = df_t0_append["total_volume"].astype(
                        "Int64"
                    )

                    df_hist_adj = pd.concat(
                        [df_hist_adj, df_t0_append], ignore_index=True
                    )
                    self.logger.info(f"✅ Đã bù thành công dữ liệu T0 cho mã {ticker}.")
                else:
                    self.logger.error(
                        f"❌ Không tìm thấy dữ liệu T0 của mã {ticker} trong df_t0 để bù."
                    )

            # Khắc phục trùng lặp ngày trong dữ liệu lịch sử điều chỉnh do lag API
            df_hist_adj.drop_duplicates(
                subset=["trading_date"], keep="last", inplace=True
            )

            # Lưu trữ lên GCS Parquet
            self.storage.save_symbol_history(df_hist_adj, ticker, suffix="adj")
            return True
        except Exception as e:
            self.logger.error(
                f"❌ Lỗi khi tải và lưu lên GCS lịch sử mã {ticker}: {e}", exc_info=True
            )
            return False

    def verify_price_units(self, symbols_map: dict[str, str]) -> float:
        """Kiểm tra sự đồng nhất đơn vị giá giữa price_board và ohlcv.

        Args:
            symbols_map (dict[str, str]): Bản đồ mã chứng khoán sang sàn giao dịch tương ứng.

        Returns:
            float: Hệ số nhân cần áp dụng cho bảng giá T0 (thường là 1000.0 nếu lệch hoặc 1.0 nếu đồng nhất).
        """
        default_multiplier: float = 1.0
        if not symbols_map:
            return default_multiplier

        # Chọn mã benchmark (ưu tiên FPT, hoặc lấy mã đầu tiên có 3 ký tự)
        ticker: str = "FPT"
        if ticker not in symbols_map:
            ticker = next((s for s in symbols_map.keys() if len(s) == 3), "")

        if not ticker:
            return default_multiplier

        try:
            # 1. Lấy giá từ bảng giá T0
            self.processor.rate_limiter.hit()
            df_board: pd.DataFrame | None = self.processor.trading_api.price_board(
                [ticker]
            )
            if df_board is None or df_board.empty:
                self.logger.warning(
                    f"⚠️ [Unit Check] Không thể lấy bảng giá {ticker} để kiểm tra đơn vị. "
                    "Sử dụng hệ số mặc định 1.0"
                )
                return default_multiplier
            board_price: float = float(df_board.iloc[0]["close_price"])

            # 2. Lấy giá ohlcv lịch sử gần đây (sử dụng hàm fetch_ohlcv đã qua xử lý nhân multiplier)
            today_str: str = datetime.now(Config.VN_TZ).strftime("%Y-%m-%d")
            start_date: str = (datetime.now(Config.VN_TZ) - timedelta(days=7)).strftime(
                "%Y-%m-%d"
            )

            df_ohlcv: pd.DataFrame | None = self.processor.fetch_ohlcv(
                ticker, start_date=start_date, end_date=today_str, limit=5
            )
            if df_ohlcv is None or df_ohlcv.empty:
                self.logger.warning(
                    f"⚠️ [Unit Check] Không thể lấy dữ liệu OHLCV {ticker} để kiểm tra đơn vị. "
                    "Sử dụng hệ số mặc định 1.0"
                )
                return default_multiplier

            ohlcv_price: float = float(df_ohlcv.iloc[-1]["close_price"])

            if (
                pd.isna(board_price)
                or pd.isna(ohlcv_price)
                or board_price <= 0
                or ohlcv_price <= 0
            ):
                self.logger.warning(
                    f"⚠️ [Unit Check] Giá benchmark bị NaN hoặc không hợp lệ "
                    f"(Bảng giá: {board_price} | OHLCV: {ohlcv_price}). Sử dụng hệ số mặc định 1.0"
                )
                return default_multiplier

            ratio: float = board_price / ohlcv_price
            self.logger.info(
                f"🔍 [Price Unit Check] Giá {ticker} - Bảng giá: {board_price:,.2f} | "
                f"OHLCV đã xử lý: {ohlcv_price:,.2f} | Tỷ lệ: {ratio:.4f}"
            )

            # Đánh giá sự đồng nhất đơn vị
            if 0.0008 <= ratio <= 0.0012:
                self.logger.warning(
                    f"⚠️ LỆCH ĐƠN VỊ GIÁ: Phát hiện giá bảng điện tử ({board_price:,.2f}) nhỏ hơn 1000 lần "
                    f"so với dữ liệu OHLCV ({ohlcv_price:,.2f}). Sẽ tự động áp dụng hệ số nhân 1000 cho bảng giá T0."
                )
                return 1000.0
            elif 0.8 <= ratio <= 1.2:
                self.logger.info(
                    "✅ Xác nhận đơn vị giá nhất quán giữa Bảng giá và dữ liệu OHLCV. Hệ số nhân: 1.0"
                )
                return 1.0
            else:
                self.logger.warning(
                    f"⚠️ [Unit Check] Tỷ lệ giá trị bất thường: {ratio:.4f}. "
                    f"(Bảng giá: {board_price} vs OHLCV: {ohlcv_price}). Sử dụng hệ số mặc định 1.0"
                )
                return default_multiplier
        except Exception as e:
            self.logger.error(
                f"❌ Lỗi trong quá trình kiểm tra tự động đơn vị giá: {e}",
                exc_info=True,
            )
            return default_multiplier

    def run(self) -> pd.DataFrame | None:
        """Khởi chạy toàn bộ quy trình ETL tải dữ liệu daily, backfill, xử lý sự kiện và đồng bộ lên GCP.

        Returns:
            Optional[pd.DataFrame]: DataFrame dữ liệu giao dịch T0 ngày hôm nay nếu thành công, ngược lại trả về None.
        """
        symbols_map: dict[str, str] = self.processor.get_symbols_with_exchange()

        # Đồng bộ danh sách công ty từ vnstock vào DB (chỉ áp dụng ở môi trường hỗ trợ lưu trữ thông tin công ty - Local)
        if Config.DEPLOYMENT_ENV == "local":
            # Kiểm tra xem có cần đồng bộ danh sách công ty & ngành hay không (tối thiểu 7 ngày/lần)
            last_sync_str = self.storage.get_state("last_company_sync_date")
            should_sync = True
            today_date = datetime.now(Config.VN_TZ).date()
            if last_sync_str:
                try:
                    last_sync_date = datetime.strptime(last_sync_str, "%Y-%m-%d").date()
                    if (today_date - last_sync_date).days < 7:
                        should_sync = False
                        self.logger.info(
                            f"ℹ️ Danh sách công ty & ngành đã được đồng bộ vào {last_sync_str} (dưới 7 ngày trước). Bỏ qua đồng bộ hôm nay."
                        )
                except Exception as e:
                    self.logger.warning(
                        f"⚠️ Không thể phân tích ngày đồng bộ công ty cuối cùng {last_sync_str}: {e}"
                    )

            if should_sync:
                try:
                    self.logger.info("🏢 Bắt đầu cào danh sách công ty từ vnstock...")
                    df_symbols = self.processor.reference_api.equity().list_by_exchange()
                    df_symbols = df_symbols[
                        ~df_symbols["type"].isin(["corpbond", "bond", "future"])
                    ]

                    # Tải danh sách ngành từ vnstock
                    df_industry = self.processor.reference_api.equity().list_by_industry()

                    # Phân tách 4 cấp độ ngành dựa trên symbol
                    df_l1 = df_industry[df_industry["icb_level"] == 1][["symbol", "icb_code", "icb_name"]].rename(
                        columns={"icb_code": "icb_l1_code", "icb_name": "icb_l1_name"}
                    )
                    df_l2 = df_industry[df_industry["icb_level"] == 2][["symbol", "icb_code", "icb_name"]].rename(
                        columns={"icb_code": "icb_l2_code", "icb_name": "icb_l2_name"}
                    )
                    df_l3 = df_industry[df_industry["icb_level"] == 3][["symbol", "icb_code", "icb_name"]].rename(
                        columns={"icb_code": "icb_l3_code", "icb_name": "icb_l3_name"}
                    )
                    df_l4 = df_industry[df_industry["icb_level"] == 4][["symbol", "icb_code", "icb_name"]].rename(
                        columns={"icb_code": "icb_code", "icb_name": "icb_name"}
                    )

                    # Xác định cấp độ lá (leaf level) thực tế cho mỗi symbol (max icb_level)
                    idx_max = df_industry.groupby("symbol")["icb_level"].idxmax()
                    if "com_type_code" in df_industry.columns:
                        df_leaf = df_industry.loc[idx_max][["symbol", "icb_code", "icb_level", "com_type_code"]]
                    else:
                        df_leaf = df_industry.loc[idx_max][["symbol", "icb_code", "icb_level"]].copy()
                        df_leaf["com_type_code"] = None

                    # Xây dựng bảng phẳng danh mục ngành icb_industries
                    df_paths = df_leaf.rename(columns={"icb_code": "icb_code_leaf"})
                    df_paths = pd.merge(df_paths, df_l1, on="symbol", how="left")
                    df_paths = pd.merge(df_paths, df_l2, on="symbol", how="left")
                    df_paths = pd.merge(df_paths, df_l3, on="symbol", how="left")
                    df_paths = pd.merge(df_paths, df_l4, on="symbol", how="left")

                    # Chuẩn bị DataFrame icb_industries
                    df_icb_industries = df_paths[[
                        "icb_code_leaf",
                        "icb_l1_code", "icb_l1_name",
                        "icb_l2_code", "icb_l2_name",
                        "icb_l3_code", "icb_l3_name",
                        "icb_name"
                    ]].copy()
                    df_icb_industries.rename(columns={"icb_code_leaf": "icb_code"}, inplace=True)

                    # Điền các giá trị thiếu (NaN) với giá trị hợp lý
                    df_icb_industries["icb_l1_code"] = df_icb_industries["icb_l1_code"].fillna("Unknown")
                    df_icb_industries["icb_l1_name"] = df_icb_industries["icb_l1_name"].fillna("Unknown")
                    df_icb_industries["icb_l2_code"] = df_icb_industries["icb_l2_code"].fillna("Unknown")
                    df_icb_industries["icb_l2_name"] = df_icb_industries["icb_l2_name"].fillna("Unknown")
                    df_icb_industries["icb_l3_code"] = df_icb_industries["icb_l3_code"].fillna("Unknown")
                    df_icb_industries["icb_l3_name"] = df_icb_industries["icb_l3_name"].fillna("Unknown")
                    df_icb_industries["icb_name"] = df_icb_industries["icb_name"].fillna(df_icb_industries["icb_l3_name"])

                    df_icb_industries.drop_duplicates(subset=["icb_code"], inplace=True)

                    # Lưu danh mục ngành ICB vào database trước
                    self.storage.save_icb_industries(df_icb_industries)

                    # Merge với danh sách cổ phiếu
                    df_merged = pd.merge(
                        df_symbols,
                        df_leaf[["symbol", "icb_code", "com_type_code"]],
                        on="symbol",
                        how="left",
                    )
                    df_companies = pd.DataFrame(
                        {
                            "symbol": df_merged["symbol"].str.strip().str.upper(),
                            "exchange": df_merged["exchange"].apply(normalize_exchange),
                            "company_name": df_merged["organ_name"].str.strip().fillna("Unknown"),
                            "icb_code": df_merged["icb_code"].where(df_merged["icb_code"].notna(), None),
                            "com_type_code": df_merged["com_type_code"].where(df_merged["com_type_code"].notna(), None),
                            "type": df_merged["type"].where(df_merged["type"].notna(), None),
                            "status": "active",
                        }
                    )
                    df_companies.drop_duplicates(subset=["symbol"], inplace=True)
                    self.storage.save_companies(df_companies)
                    self.logger.info(
                        f"✅ Đã cào và đồng bộ {len(df_companies)} công ty vào database."
                    )

                    # Cập nhật ngày đồng bộ thành công vào DB
                    self.storage.save_state("last_company_sync_date", today_date.strftime("%Y-%m-%d"))
                except Exception as e:
                    self.logger.error(
                        f"❌ Lỗi khi đồng bộ danh sách công ty: {e}", exc_info=True
                    )
        else:
            self.logger.info(
                "☁️ Đang chạy ở chế độ Cloud - Bỏ qua việc cào danh sách công ty và phân loại ngành ICB."
            )

        t0_multiplier: float = self.verify_price_units(symbols_map)
        symbols: list[str] = list(symbols_map.keys())

        df_t0: pd.DataFrame = self.processor.fetch_entire_market_t0(symbols)
        if df_t0.empty:
            err_msg: str = (
                "Không lấy được bất kỳ dữ liệu bảng giá T0 nào từ thị trường. "
                "API nguồn có thể gặp sự cố hoặc bị chặn."
            )
            self.logger.error(f"🛑 {err_msg}")
            raise ValueError(err_msg)

        # Áp dụng hệ số nhân đơn vị giá động sau khi tự động kiểm tra
        if t0_multiplier != 1.0:
            self.logger.info(
                f"⚡ Áp dụng hệ số nhân {t0_multiplier} cho dữ liệu giá T0 nhằm đồng nhất đơn vị."
            )
            price_cols: list[str] = [
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "reference_price",
                "average_price",
            ]
            df_t0[price_cols] *= t0_multiplier

        # 1. Khởi tạo và kiểm tra trạng thái lịch chạy gần nhất
        init_res: tuple[date, list[date], list[str], dict[str, Any]] | None = (
            self._initialize_run_dates(df_t0)
        )
        if init_res is None:
            return None
        today_date, missing_dates, pending_reloads, latest_state = init_res

        # 2. Phát hiện các sự kiện chia cổ tức/phát hành thêm cổ phiếu trong phiên T0
        detected_corporate_actions: dict[str, date] = (
            self._detect_corporate_actions_today(df_t0, today_date)
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
                        f"🔔 [Backfill] Phát hiện {len(detected_backfill_map)} mã có sự kiện trong thời gian backfill: "
                        f"{list(detected_backfill_map.keys())}"
                    )
                    detected_corporate_actions.update(detected_backfill_map)
            except Exception as e:
                self.logger.error(
                    f"⚠️ Lỗi khi quét sự kiện doanh nghiệp Backfill qua API: {e}",
                    exc_info=True,
                )

        # Hợp nhất danh sách mã lỗi của phiên chạy trước để thử lại trong hôm nay
        if pending_reloads:
            self.logger.warning(
                f"🔄 Phát hiện {len(pending_reloads)} mã bị lỗi reload phiên trước, "
                f"tự động đưa vào danh sách chạy lại hôm nay: {pending_reloads}"
            )
            ticker_pattern: re.Pattern[str] = re.compile(r"^[A-Z0-9]{3,10}$")
            for p_ticker in pending_reloads:
                p_ticker_clean: str = str(p_ticker).strip().upper()
                if ticker_pattern.match(p_ticker_clean):
                    detected_corporate_actions[p_ticker_clean] = today_date
                else:
                    self.logger.warning(
                        f"⚠️ Loại bỏ mã lỗi reload không hợp lệ khỏi hàng đợi chạy lại: {p_ticker}"
                    )

        # 4. Thực hiện chạy bù dữ liệu thô (Backfill) cho những ngày bị thiếu
        if missing_dates:
            self._backfill_missing_history(missing_dates)

        # 5. Lưu dữ liệu thô T0 lên GCS và nạp vào BigQuery
        df_t0_parquet: pd.DataFrame = df_t0.drop(
            columns=["reference_price", "average_price"], errors="ignore"
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
                f"🚀 Bắt đầu tải lại toàn bộ lịch sử Giá điều chỉnh cho {len(tickers_list)} mã "
                f"phát hiện được: {tickers_list}..."
            )

            # Chia nhỏ danh sách mã cần reload thành các lô tối đa 10 mã để tránh rủi ro OOM và giải phóng RAM sớm
            reload_batch_size: int = 10
            for b_idx in range(0, len(tickers_list), reload_batch_size):
                batch_tickers: list[str] = tickers_list[
                    b_idx : b_idx + reload_batch_size
                ]
                self.logger.info(
                    f"📦 [Reload Batch] Đang xử lý lô {b_idx // reload_batch_size + 1} "
                    f"gồm {len(batch_tickers)} mã: {batch_tickers}"
                )

                successful_batch: list[str] = []
                for ticker in batch_tickers:
                    success: bool = self._reload_adjusted_history(
                        ticker, today_date, symbols_map, df_t0
                    )
                    if not success:
                        failed_reloads.append(ticker)
                    else:
                        successful_batch.append(ticker)
                        successful_reloads.append(ticker)
                    # Dọn dẹp bộ nhớ chủ động cho từng mã
                    gc.collect()

                # Đồng bộ gộp dữ liệu của lô hiện tại lên database ngay lập tức
                if successful_batch:
                    self.logger.info(
                        f"⚡ [Reload Batch] Đồng bộ gộp lên database cho các mã thành công của lô: {successful_batch}"
                    )
                    self.storage.sync_adjusted_symbols_to_bigquery(successful_batch)

                # Giải phóng RAM chủ động sau mỗi lô
                del successful_batch
                gc.collect()
        else:
            self.logger.info(
                "ℹ️ Không phát hiện mã cổ phiếu nào cần tải lại lịch sử Giá điều chỉnh."
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
            "🎉 Đã lưu checkpoint cập nhật trạng thái thị trường EOD thành công."
        )

        # 8.5. Trích xuất dữ liệu của các mã cổ phiếu quan tâm lên GCS
        export_summary: dict[str, Any] | None = None
        try:
            export_summary = self.storage.export_interested_tickers_data()
        except Exception as export_err:
            self.logger.error(
                f"❌ Lỗi khi trích xuất dữ liệu các mã cổ phiếu quan tâm: {export_err}",
                exc_info=True,
            )

        # 9. Gửi báo cáo thông báo trạng thái kết thúc phiên chạy qua Telegram
        try:
            vn_now: datetime = datetime.now(Config.VN_TZ)
            today_str_check: str = vn_now.strftime("%Y-%m-%d")
            t0_date_str: str = today_date.strftime("%Y-%m-%d")
            run_is_eod: bool = (
                t0_date_str < today_str_check
                or vn_now.hour > 15
                or (vn_now.hour == 15 and vn_now.minute >= 15)
            )
            reloaded_symbols: list[str] = [
                s for s in detected_corporate_actions.keys() if s not in failed_reloads
            ]

            Notifier(self.logger).send_summary(
                date_str=t0_date_str,
                total_processed=df_t0.shape[0],
                is_eod=run_is_eod,
                missing_dates=missing_dates,
                reloaded_symbols=reloaded_symbols,
                failed_reloads=failed_reloads,
                export_summary=export_summary,
            )
        except Exception as notify_err:
            self.logger.error(f"❌ Không thể gửi báo cáo chạy daily: {notify_err}")

        return df_t0
