import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
import numpy as np
import pandas as pd
import requests
import time

from vnstock import Reference, Trading
from vnstock.ui import Market
from vnstock.api.company import Company

from config import Config
from storages import Storage
from utils import normalize_exchange, setup_logger, SmartRateLimiter
from notifier import Notifier

# Suppress spam logging from vnstock internal company explorer module
logging.getLogger("vnstock.explorer.vci.company").setLevel(logging.CRITICAL)


class DataProcessor:
    """Chuyên trách việc làm sạch, biến đổi và tối ưu hóa dữ liệu chứng khoán từ vnstock."""

    def __init__(self, logger: logging.Logger, source: str = "VCI") -> None:
        """Khởi tạo bộ xử lý dữ liệu và thiết lập các cổng kết nối API Vnstock.

        Args:
            logger: Đối tượng Logger dùng để ghi nhận tiến trình.
            source: Nguồn cung cấp dữ liệu chứng khoán mặc định (ví dụ: 'VCI').
        """
        self.logger: logging.Logger = logger
        self.source: str = source

        self.reference_api: Reference = Reference()
        self.trading_api: Trading = Trading()
        self.market_api: Market = Market()

        # Cấu hình bộ điều tiết tần suất cuộc gọi API để bảo vệ hệ thống tránh bị chặn
        self.rate_limiter: SmartRateLimiter = SmartRateLimiter(
            logger=logger,
            limit=Config.API_REQUEST_THRESHOLD,
            window=Config.API_RATE_LIMIT_WINDOW,
            micro_sleep=Config.API_MICRO_SLEEP,
        )

    def get_symbols_with_exchange(self) -> Dict[str, str]:
        """Tải danh sách mã cổ phiếu đang hoạt động cùng với sàn giao dịch tương ứng.

        Returns:
            Dict mapping giữa mã chứng khoán (symbol) và sàn niêm yết (exchange).
        """
        self.rate_limiter.hit()
        try:
            df_symbols: pd.DataFrame = self.reference_api.equity().list_by_exchange()
            # Loại trừ trái phiếu, trái phiếu doanh nghiệp và chứng khoán phái sinh
            df_symbols = df_symbols[~df_symbols["type"].isin(["corpbond", "bond", "future"])]
            return dict(zip(df_symbols["symbol"], df_symbols["exchange"]))
        except Exception as e:
            self.logger.error(f"🛑 Không thể lấy danh sách symbol từ vnstock: {e}")
            return {}

    def fetch_entire_market_t0(self, symbols: List[str]) -> pd.DataFrame:
        """Tải dữ liệu bảng giá ngày hiện hành T0 hàng loạt cho tất cả các mã chứng khoán.

        Args:
            symbols: Danh sách mã cổ phiếu cần tải thông tin bảng giá.

        Returns:
            DataFrame dữ liệu giao dịch T0 hoàn chỉnh của toàn bộ thị trường.
        """
        self.logger.info(
            f"📥 [Bulk Fetch] Đang kéo bảng giá T0 cho {len(symbols)} mã vào RAM..."
        )

        dfs: List[pd.DataFrame] = []
        batch_size: int = 500
        for i in range(0, len(symbols), batch_size):
            batch: List[str] = symbols[i : i + batch_size]

            max_retries: int = 3
            initial_delay: float = 10.0
            backoff_factor: float = 2.0
            delay: float = initial_delay
            df_quote: Optional[pd.DataFrame] = None
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

            if success and df_quote is not None and not df_quote.empty:
                dfs.append(df_quote)

        if not dfs:
            return pd.DataFrame()

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

        # Loại bỏ các dòng chứa giá trị lỗi hoặc thiếu tham số giao dịch quan trọng
        df_all = df_all.dropna(subset=["open_price", "high_price", "low_price", "close_price", "total_volume"])
        df_all = df_all[
            ~(
                (df_all["open_price"] <= 0)
                | (df_all["high_price"] <= 0)
                | (df_all["low_price"] <= 0)
                | (df_all["close_price"] <= 0)
                | (df_all["total_volume"] <= 0)
            )
        ]

        df_all["symbol"] = df_all["symbol"].astype(str).str.strip().str.upper().astype("category")
        df_all["exchange"] = df_all["exchange"].astype(
            pd.CategoricalDtype(categories=["HoSE", "HNX", "UPCoM", "Unknown"])
        )

        price_cols: List[str] = ["open_price", "high_price", "low_price", "close_price"]
        df_all[price_cols] = df_all[price_cols].astype("float32")
        df_all["reference_price"] = df_all["reference_price"].astype("float32")
        df_all["average_price"] = df_all["average_price"].astype("float32")
        df_all["total_volume"] = df_all["total_volume"].astype("Int32")

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
                "reference_price",
                "average_price",
            ]
        ]

    def fetch_ohlcv(
        self, symbol: str, start_date: str, end_date: str, limit: int = 100
    ) -> Optional[pd.DataFrame]:
        """Tải dữ liệu lịch sử OHLCV cho một mã chứng khoán trong khoảng thời gian xác định.

        Args:
            symbol: Mã cổ phiếu cần tải (ví dụ: 'FPT').
            start_date: Ngày bắt đầu định dạng YYYY-MM-DD.
            end_date: Ngày kết thúc định dạng YYYY-MM-DD.
            limit: Số dòng tối đa cho phép tải về.

        Returns:
            DataFrame chứa lịch sử OHLCV của mã cổ phiếu, trả về None nếu có lỗi xảy ra.
        """
        self.rate_limiter.hit()
        try:
            df_ohclv: Optional[pd.DataFrame] = self.market_api.equity(symbol).ohlcv(
                start=start_date,
                end=end_date,
                source=self.source,
                count=limit,
            )
            if df_ohclv is None or df_ohclv.empty:
                return None

            df_ohclv = df_ohclv.copy()
            df_ohclv.rename(columns={
                "time": "trading_date",
                "open": "open_price",
                "high": "high_price",
                "low": "low_price",
                "close": "close_price",
                "volume": "total_volume",
            }, inplace=True)

            # Quy đổi đơn vị giá Vnstock (nghìn đồng) về đồng giống file CafeF
            price_cols: List[str] = ["open_price", "high_price", "low_price", "close_price"]
            df_ohclv[price_cols] *= Config.PRICE_MULTIPLIER

            return df_ohclv
        except Exception as e:
            self.logger.error(f"⚠️ Lỗi khi kéo ohlcv cho mã {symbol}: {e}")
            return None

    def detect_corporate_actions_via_api(
        self, symbols: List[str], start_date: date, end_date: date
    ) -> Dict[str, date]:
        """Quét và phát hiện lịch sự kiện doanh nghiệp của một nhóm mã chứng khoán thông qua API.

        Args:
            symbols: Danh sách các mã cổ phiếu cần quét.
            start_date: Mốc thời gian bắt đầu quét sự kiện.
            end_date: Mốc thời gian kết thúc quét sự kiện.

        Returns:
            Dict chứa thông tin ánh xạ giữa mã chứng khoán (symbol) và ngày ex_date có hiệu lực sự kiện.
        """
        if not symbols:
            return {}

        start_str: str = start_date.strftime("%Y%m%d")
        end_str: str = (end_date + timedelta(days=1)).strftime("%Y%m%d")

        self.logger.info(
            f"🔍 [Corporate Actions API] Quét sự kiện từ {start_date} đến {end_date} cho {len(symbols)} mã..."
        )

        c: Company = Company(symbol='', source='VCI')
        all_events: List[Dict[str, Any]] = []
        batch_size: int = 300

        for i in range(0, len(symbols), batch_size):
            batch: List[str] = symbols[i : i + batch_size]
            c.provider.symbol = ",".join(batch)

            max_retries: int = 3
            events: Optional[List[Dict[str, Any]]] = []
            for attempt in range(1, max_retries + 1):
                try:
                    self.rate_limiter.hit()
                    events = c._fetch_events(
                        from_date=start_str,
                        to_date=end_str,
                        event_codes="DIV,ISS",
                        size=1000
                    )
                    break
                except Exception as e:
                    self.logger.warning(
                        f"⚠️ Lỗi khi tải sự kiện lô {i} (lần {attempt}/{max_retries}): {e}"
                    )
                    if attempt < max_retries:
                        time.sleep(2.0 * attempt)

            if events:
                all_events.extend(events)

        if not all_events:
            return {}

        df_events: pd.DataFrame = pd.DataFrame(all_events)
        if "exrightDate" not in df_events.columns:
            return {}

        df_events = df_events[df_events["exrightDate"].notna()]
        if df_events.empty:
            return {}

        df_events["ex_date"] = pd.to_datetime(df_events["exrightDate"]).dt.date

        # Chỉ lọc lấy các sự kiện nằm hoàn toàn trong khoảng thời gian cần kiểm tra
        df_filtered: pd.DataFrame = df_events[
            (df_events["ex_date"] >= start_date) &
            (df_events["ex_date"] <= end_date)
        ]

        if df_filtered.empty:
            return {}

        # Ưu tiên lấy sự kiện sớm nhất nếu có nhiều sự kiện phát sinh cho một mã
        df_filtered = df_filtered.sort_values(by="ex_date")

        detected_map: Dict[str, date] = {}
        for _, row in df_filtered.iterrows():
            ticker: str = str(row["ticker"]).strip().upper()
            ex_date: Any = row["ex_date"]
            if ticker and ticker not in ["", "NAN", "NONE"] and isinstance(ex_date, date):
                if ticker not in detected_map:
                    detected_map[ticker] = ex_date

        return detected_map


class VnstockExtractorETL:
    """Bộ điều phối chính của Vnstock Daily Pipeline."""

    def __init__(self, logger_name: str = Config.DEFAULT_LOGGER_NAME) -> None:
        """Khởi tạo các phân lớp phục vụ xử lý và lưu trữ dữ liệu.

        Args:
            logger_name: Tên Logger chung của hệ thống.
        """
        self.logger: logging.Logger = setup_logger(logger_name)
        self.processor: DataProcessor = DataProcessor(self.logger)
        self.storage: Storage = Storage(self.logger)

    def _initialize_run_dates(
        self,
        df_t0: pd.DataFrame
    ) -> Optional[Tuple[date, List[date], List[str], Dict[str, Any]]]:
        """Phân tích checkpoint và kiểm tra tính hợp lệ về ngày chạy dữ liệu mới.

        Args:
            df_t0: DataFrame dữ liệu phiên T0 tải về.

        Returns:
            Tuple chứa các thông tin (t0_max_date, missing_dates, pending_reloads, latest_state)
            hoặc None nếu hệ thống dừng chạy do trùng lắp hoặc không có lịch sử mới.
        """
        t0_max_date: date = df_t0["trading_date"].dt.date.max()

        latest_state: Dict[str, Any] = self.storage.read_checkpoint()
        metadata: Dict[str, Any] = latest_state.get("metadata") or {}
        last_run_str: Optional[str] = metadata.get("last_successful_run")
        is_eod: bool = metadata.get("is_eod", False)
        pending_reloads: List[str] = metadata.get("pending_adjusted_reloads") or []

        date_latest_state: Optional[date] = None
        if last_run_str:
            try:
                date_latest_state = datetime.strptime(last_run_str, "%Y-%m-%d").date()
                self.logger.info(
                    f"📅 Ngày chạy daily cuối cùng thành công: {date_latest_state} "
                    f"({'Đã chốt phiên EOD' if is_eod else 'Chưa chốt phiên EOD'})"
                )
            except Exception as e:
                self.logger.warning(f"⚠️ Không thể phân tích ngày chạy cuối cùng {last_run_str}: {e}")

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
        missing_dates: List[date] = []
        if date_latest_state:
            start_offset: int = 1 if is_eod else 0
            current_date: date = date_latest_state + timedelta(days=start_offset)
            while current_date < t0_max_date:
                if current_date.weekday() < 5:
                    current_date_str: str = current_date.strftime("%Y-%m-%d")
                    if current_date_str not in Config.VN_HOLIDAY_DATES:
                        missing_dates.append(current_date)
                current_date += timedelta(days=1)

        return t0_max_date, missing_dates, pending_reloads, latest_state

    def _detect_corporate_actions_today(
        self,
        df_t0: pd.DataFrame,
        latest_state: Dict[str, Any],
        today_date: date
    ) -> Dict[str, date]:
        """Phát hiện các sự kiện doanh nghiệp của phiên T0 dựa vào biến động giá tham chiếu so với checkpoint.

        Args:
            df_t0: DataFrame dữ liệu T0.
            latest_state: Trạng thái checkpoint cũ đọc từ GCS.
            today_date: Ngày giao dịch hôm nay.

        Returns:
            Dict chứa các mã cổ phiếu và ngày có sự kiện tương ứng.
        """
        detected_corporate_actions: Dict[str, date] = {}
        suspected_tickers: List[str] = []

        if not latest_state or "snapshots" not in latest_state:
            self.logger.info("ℹ️ Checkpoint cũ trống, bỏ qua quét sự kiện T0.")
            return {}

        old_snapshots: Dict[str, Dict[str, Any]] = latest_state["snapshots"]

        # 1. Phát hiện sơ bộ các mã bị lệch giá tham chiếu so với giá chốt phiên hôm trước
        for _, row in df_t0.iterrows():
            sym: str = str(row["symbol"]).strip().upper()
            if not sym or sym not in old_snapshots:
                continue

            snap: Dict[str, Any] = old_snapshots[sym]
            exch: Optional[str] = snap.get("exchange")
            ref_price: Optional[float] = row.get("reference_price")

            if ref_price is None or pd.isna(ref_price) or ref_price <= 0:
                continue

            # Sàn HoSE/HNX: So sánh tỉ lệ phần trăm lệch giá tham chiếu T0 so với giá đóng cửa phiên trước
            if exch in ["HoSE", "HNX"]:
                prev_close: Optional[float] = snap.get("close_price")
                if prev_close and prev_close > 0:
                    deviation: float = abs(ref_price - prev_close) / prev_close
                    if deviation > Config.PRICE_DEV_THRESHOLD_HOSE_HNX:
                        suspected_tickers.append(sym)
            # Sàn UPCoM: So sánh tỉ lệ phần trăm lệch giá tham chiếu T0 so với giá trung bình phiên trước
            elif exch == "UPCoM":
                prev_avg: Optional[float] = snap.get("average_price")
                if prev_avg and prev_avg > 0:
                    deviation: float = abs(ref_price - prev_avg) / prev_avg
                    if deviation > Config.PRICE_DEV_THRESHOLD_UPCOM:
                        suspected_tickers.append(sym)

        # 2. Xác thực chính xác các mã lệch giá bằng cách gọi API lịch sự kiện doanh nghiệp
        if suspected_tickers:
            self.logger.warning(
                f"🔔 Phát hiện {len(suspected_tickers)} mã nghi ngờ lệch giá: {suspected_tickers}. Tiến hành gọi API xác minh..."
            )
            today_formatted_str: str = today_date.strftime("%Y%m%d")
            c: Company = Company(symbol='', source='VCI')
            c.provider.symbol = ",".join(suspected_tickers)

            self.processor.rate_limiter.hit()
            events: Optional[List[Dict[str, Any]]] = c._fetch_events(
                from_date=today_formatted_str,
                to_date=today_formatted_str,
                event_codes="DIV,ISS",
                size=1000
            )

            if events:
                for ev in events:
                    ticker: str = str(ev.get("ticker")).strip().upper()
                    if ticker in suspected_tickers:
                        self.logger.warning(f"✅ [Xác thực thành công] Mã {ticker} thực sự có sự kiện doanh nghiệp hôm nay.")
                        detected_corporate_actions[ticker] = today_date
            else:
                self.logger.info("ℹ️ Không tìm thấy sự kiện khớp trên API. Sự lệch giá do sai số làm tròn hoặc dữ liệu lag.")
        else:
            self.logger.info("ℹ️ Không phát hiện mã nào lệch giá bất thường hôm nay.")

        return detected_corporate_actions

    def _backfill_missing_history(self, missing_dates: List[date]) -> None:
        """Bù lại toàn bộ các ngày giao dịch bị thiếu thông qua extractor CafeF.

        Args:
            missing_dates: Danh sách các ngày cần backfill.

        Raises:
            RuntimeError: Phát sinh khi một trong các ngày backfill bị lỗi để tránh lủng dữ liệu.
        """
        self.logger.info(f"🚀 Phát hiện {len(missing_dates)} ngày thiếu cần backfill. Tiến hành tải qua CafeF...")
        from extractors.extractor_cafef import CafeFExtractorETL
        cafe_etl: CafeFExtractorETL = CafeFExtractorETL(logger_name=self.logger.name)

        for m_date in sorted(missing_dates):
            self.logger.info(f"📅 [Backfill CafeF] Đang tải dữ liệu thô cho ngày {m_date}...")
            dt_ref: datetime = datetime.combine(m_date, datetime.min.time())
            res: Optional[pd.DataFrame] = cafe_etl.run(
                dt_ref, is_raw=True, partition=True, save_checkpoint=False
            )
            if res is None:
                raise RuntimeError(
                    f"❌ Chạy backfill CafeF thất bại cho ngày {m_date.strftime('%Y-%m-%d')}. "
                    "Dừng pipeline để tránh mất mát dữ liệu lịch sử."
                )

    def _reload_adjusted_history(
        self,
        ticker: str,
        today_date: date,
        symbols_map: Dict[str, str],
        df_t0: pd.DataFrame
    ) -> bool:
        """Tải lại toàn bộ lịch sử giá điều chỉnh (từ năm 2000) của một mã và đồng bộ lên GCS/BigQuery.

        Args:
            ticker: Mã chứng khoán cần xử lý.
            today_date: Ngày giao dịch của phiên chạy hiện tại.
            symbols_map: Bản đồ mã chứng khoán sang sàn giao dịch tương ứng.
            df_t0: DataFrame dữ liệu T0.

        Returns:
            True nếu quá trình reload và đồng bộ hoàn tất thành công, ngược lại False.
        """
        try:
            df_hist_adj: Optional[pd.DataFrame] = self.processor.fetch_ohlcv(
                ticker,
                start_date="2000-01-01",
                end_date=datetime.now(Config.VN_TZ).strftime("%Y-%m-%d"),
                limit=15000
            )

            if df_hist_adj is None or df_hist_adj.empty:
                self.logger.error(f"❌ Không thể tải lịch sử giá cho mã {ticker}")
                return False

            df_hist_adj = df_hist_adj.copy()
            df_hist_adj["symbol"] = ticker
            df_hist_adj["exchange"] = symbols_map.get(ticker, "Unknown")
            df_hist_adj["symbol"] = df_hist_adj["symbol"].astype(str).str.strip().str.upper().astype("category")
            df_hist_adj["exchange"] = df_hist_adj["exchange"].apply(normalize_exchange).astype(
                pd.CategoricalDtype(categories=["HoSE", "HNX", "UPCoM", "Unknown"])
            )
            df_hist_adj["trading_date"] = pd.to_datetime(df_hist_adj["trading_date"]).dt.normalize()

            price_cols: List[str] = ["open_price", "high_price", "low_price", "close_price"]
            df_hist_adj[price_cols] = df_hist_adj[price_cols].astype("float32")
            df_hist_adj["total_volume"] = df_hist_adj["total_volume"].astype("Int32")

            target_cols: List[str] = ["symbol", "trading_date", "open_price", "high_price", "low_price", "close_price", "total_volume", "exchange"]
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
                    df_t0_append: pd.DataFrame = pd.DataFrame([{
                        "symbol": ticker,
                        "trading_date": pd.to_datetime(today_date),
                        "open_price": t0_row["open_price"],
                        "high_price": t0_row["high_price"],
                        "low_price": t0_row["low_price"],
                        "close_price": t0_row["close_price"],
                        "total_volume": t0_row["total_volume"],
                        "exchange": symbols_map.get(ticker, "Unknown")
                    }])
                    df_t0_append["symbol"] = df_t0_append["symbol"].astype(str).str.strip().str.upper().astype("category")
                    df_t0_append["exchange"] = df_t0_append["exchange"].apply(normalize_exchange).astype(
                        pd.CategoricalDtype(categories=["HoSE", "HNX", "UPCoM", "Unknown"])
                    )
                    df_t0_append["trading_date"] = pd.to_datetime(df_t0_append["trading_date"]).dt.normalize()
                    df_t0_append[price_cols] = df_t0_append[price_cols].astype("float32")
                    df_t0_append["total_volume"] = df_t0_append["total_volume"].astype("Int32")

                    df_hist_adj = pd.concat([df_hist_adj, df_t0_append], ignore_index=True)
                    self.logger.info(f"✅ Đã bù thành công dữ liệu T0 cho mã {ticker}.")
                else:
                    self.logger.error(f"❌ Không tìm thấy dữ liệu T0 của mã {ticker} trong df_t0 để bù.")

            # Lưu trữ và đồng bộ hóa lên BigQuery
            self.storage.save_symbol_history(df_hist_adj, ticker, suffix="adj")
            self.storage.sync_adjusted_symbol_to_bigquery(ticker)
            return True
        except Exception as e:
            self.logger.error(f"❌ Lỗi khi tải lại lịch sử mã {ticker}: {e}", exc_info=True)
            return False

    def run(self) -> Optional[pd.DataFrame]:
        """Khởi chạy toàn bộ quy trình ETL tải dữ liệu daily, backfill, xử lý sự kiện và đồng bộ lên GCP.

        Returns:
            DataFrame dữ liệu giao dịch T0 ngày hôm nay nếu thành công, ngược lại trả về None.
        """
        symbols_map: Dict[str, str] = self.processor.get_symbols_with_exchange()
        symbols: List[str] = list(symbols_map.keys())

        df_t0: pd.DataFrame = self.processor.fetch_entire_market_t0(symbols)
        if df_t0.empty:
            self.logger.warning("⚠️ Không lấy được dữ liệu T0. Dừng pipeline.")
            return None

        # 1. Khởi tạo và kiểm tra trạng thái lịch chạy gần nhất
        init_res = self._initialize_run_dates(df_t0)
        if init_res is None:
            return None
        today_date, missing_dates, pending_reloads, latest_state = init_res

        # 2. Phát hiện các sự kiện chia cổ tức/phát hành thêm cổ phiếu trong phiên T0
        detected_corporate_actions: Dict[str, date] = self._detect_corporate_actions_today(
            df_t0, latest_state, today_date
        )

        # 3. Phát hiện sự kiện doanh nghiệp của các ngày còn thiếu (Backfill) thông qua quét API
        if missing_dates:
            try:
                backfill_start: date = min(missing_dates)
                backfill_end: date = max(missing_dates)
                detected_backfill_map: Dict[str, date] = self.processor.detect_corporate_actions_via_api(
                    symbols, backfill_start, backfill_end
                )
                if detected_backfill_map:
                    self.logger.warning(
                        f"🔔 [Backfill] Phát hiện {len(detected_backfill_map)} mã có sự kiện trong thời gian backfill: "
                        f"{list(detected_backfill_map.keys())}"
                    )
                    detected_corporate_actions.update(detected_backfill_map)
            except Exception as e:
                self.logger.error(f"⚠️ Lỗi khi quét sự kiện doanh nghiệp Backfill qua API: {e}", exc_info=True)

        # Hợp nhất danh sách mã lỗi của phiên chạy trước để thử lại trong hôm nay
        if pending_reloads:
            self.logger.warning(
                f"🔄 Phát hiện {len(pending_reloads)} mã bị lỗi reload phiên trước, tự động đưa vào danh sách chạy lại hôm nay: {pending_reloads}"
            )
            for p_ticker in pending_reloads:
                detected_corporate_actions[p_ticker.upper()] = today_date

        # 4. Thực hiện chạy bù dữ liệu thô (Backfill) cho những ngày bị thiếu
        if missing_dates:
            self._backfill_missing_history(missing_dates)

        # 5. Lưu dữ liệu thô T0 lên GCS và nạp vào BigQuery
        df_t0_parquet: pd.DataFrame = df_t0.drop(columns=["reference_price", "average_price"], errors="ignore")
        gcs_path_t0: Optional[str] = self.storage.save_parquet(
            df_t0_parquet, datetime.combine(today_date, datetime.min.time()), partition=True
        )

        if gcs_path_t0:
            dt_today: datetime = datetime.combine(today_date, datetime.min.time())
            self.storage.delete_by_date(Config.BQ_RAW_TABLE, dt_today)
            self.storage.load_parquet_to_bigquery(gcs_path_t0, Config.BQ_RAW_TABLE, write_disposition="WRITE_APPEND")

        # 6. Tải lại toàn bộ lịch sử giá điều chỉnh cho các mã có sự kiện doanh nghiệp
        failed_reloads: List[str] = []
        if detected_corporate_actions:
            tickers_list: List[str] = sorted(list(detected_corporate_actions.keys()))
            self.logger.warning(
                f"🚀 Bắt đầu tải lại toàn bộ lịch sử Giá điều chỉnh cho {len(tickers_list)} mã phát hiện được: {tickers_list}..."
            )
            for ticker in tickers_list:
                success: bool = self._reload_adjusted_history(ticker, today_date, symbols_map, df_t0)
                if not success:
                    failed_reloads.append(ticker)
        else:
            self.logger.info("ℹ️ Không phát hiện mã cổ phiếu nào cần tải lại lịch sử Giá điều chỉnh.")

        # 7. Đồng bộ dữ liệu giá từ bảng raw sang bảng adjusted (tránh các mã có sự kiện)
        all_processing_dates: List[date] = sorted(missing_dates + [today_date])
        all_excluded_symbols: List[str] = list(detected_corporate_actions.keys())
        # Tối ưu hóa: Gọi lệnh SQL bulk đồng bộ tất cả các ngày trong một Transaction duy nhất
        self.storage.sync_daily_adjusted_prices(all_processing_dates, all_excluded_symbols)

        # 8. Cập nhật Checkpoint Snapshot EOD và lưu danh sách mã bị lỗi để xử lý sau
        self.storage.save_checkpoint(
            df=df_t0,
            active_symbols=set(symbols),
            pending_adjusted_reloads=failed_reloads
        )
        self.logger.info("🎉 Đã lưu checkpoint cập nhật trạng thái thị trường EOD thành công.")

        # 9. Gửi báo cáo thông báo trạng thái kết thúc phiên chạy qua Telegram
        try:
            vn_now: datetime = datetime.now(Config.VN_TZ)
            today_str_check: str = vn_now.strftime("%Y-%m-%d")
            t0_date_str: str = today_date.strftime("%Y-%m-%d")
            run_is_eod: bool = (
                t0_date_str < today_str_check or
                vn_now.hour > 15 or
                (vn_now.hour == 15 and vn_now.minute >= 15)
            )
            reloaded_symbols: List[str] = [s for s in detected_corporate_actions.keys() if s not in failed_reloads]

            Notifier(self.logger).send_summary(
                date_str=t0_date_str,
                total_processed=df_t0.shape[0],
                is_eod=run_is_eod,
                missing_dates=missing_dates,
                reloaded_symbols=reloaded_symbols,
                failed_reloads=failed_reloads
            )
        except Exception as notify_err:
            self.logger.error(f"❌ Không thể gửi báo cáo chạy daily: {notify_err}")

        return df_t0
