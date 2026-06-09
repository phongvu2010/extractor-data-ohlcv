import logging
import numpy as np
import pandas as pd
import requests
import time
from datetime import datetime, timedelta, date
from typing import List, Optional, Dict
from vnstock import Reference, Trading
from vnstock.ui import Market

from config import Config
from storages import Storage
from utils import setup_logger, normalize_exchange, SmartRateLimiter
from notifier import Notifier


class DataProcessor:
    """Chuyên trách việc làm sạch, biến đổi và tối ưu hóa dữ liệu chứng khoán."""

    def __init__(self, logger: logging.Logger, source: str = "VCI") -> None:
        """Khởi tạo bộ xử lý dữ liệu và tải danh sách đen (Blacklist).

        Args:
            logger: Đối tượng Logger dùng để ghi nhận tiến trình.
        """
        self.logger: logging.Logger = logger
        self.source: str = source

        self.reference_api: Reference = Reference()
        self.trading_api: Trading = Trading()
        self.market_api: Market = Market()

        # Khởi tạo Rate Limiter
        self.rate_limiter = SmartRateLimiter(
            logger=logger,
            limit=Config.API_REQUEST_THRESHOLD,
            window=Config.API_RATE_LIMIT_WINDOW,
            micro_sleep=Config.API_MICRO_SLEEP,
        )


    def get_symbols_with_exchange(self) -> dict:
        self.rate_limiter.hit()
        try:
            df_symbols = self.reference_api.equity().list_by_exchange()
            df_symbols = df_symbols[~df_symbols["type"].isin(["corpbond", "bond", "future"])]
            return dict(zip(df_symbols["symbol"], df_symbols["exchange"]))
        except Exception as e:
            self.logger.error(f"🛑 Không thể lấy danh sách symbol từ vnstock: {e}")
            return {}

    def fetch_entire_market_t0(self, symbols: list) -> pd.DataFrame:
        self.logger.info(
            f"📥 [Bulk Fetch] Đang kéo bảng giá T0 cho {len(symbols)} mã vào RAM..."
        )

        dfs = []
        batch_size = 500
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i : i + batch_size]

            max_retries = 3
            initial_delay = 10
            backoff_factor = 2
            delay = initial_delay
            df_quote = None
            success = False

            for attempt in range(1, max_retries + 1):
                try:
                    self.rate_limiter.hit()
                    df_quote = self.trading_api.price_board(batch)
                    if df_quote is not None and not df_quote.empty:
                        success = True
                        break
                    else:
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

        df_all = pd.concat(dfs, ignore_index=True)
        df_all["exchange"] = df_all["exchange"].apply(normalize_exchange)
        df_all["trading_date"] = (
            pd.to_datetime(df_all["time"], unit="ms")
            .dt.tz_localize("UTC")
            .dt.tz_convert("Asia/Ho_Chi_Minh")
            .dt.normalize()
            .dt.tz_localize(None)
        )
        df_all["total_volume"] = df_all["volume_accumulated"]

        # Loại bỏ các dòng chứa giá trị NaN trước khi kiểm tra điều kiện logic
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

        # Chuẩn hóa kiểu dữ liệu để đồng nhất hoàn toàn với file Parquet lịch sử
        df_all["symbol"] = df_all["symbol"].astype(str).str.strip().str.upper().astype("category")
        df_all["exchange"] = df_all["exchange"].astype(
            pd.CategoricalDtype(categories=["HoSE", "HNX", "UPCoM", "Unknown"])
        )
        price_cols = ["open_price", "high_price", "low_price", "close_price"]
        df_all[price_cols] = df_all[price_cols].astype("float32")
        df_all["reference_price"] = df_all["reference_price"].astype("float32")
        df_all["average_price"] = df_all["average_price"].astype("float32")
        df_all["total_volume"] = df_all["total_volume"].astype("Int32")

        # Trả về các cột cần thiết (bao gồm cả giá tham chiếu và trung bình để xử lý nghiệp vụ)
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
        self.rate_limiter.hit()
        try:
            df_ohclv = self.market_api.equity(symbol).ohlcv(
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

            # Đồng bộ giá (Vnstock ohlcv trả về đơn vị nghìn đồng, cần nhân 1000 giống CafeF)
            price_cols = ["open_price", "high_price", "low_price", "close_price"]
            df_ohclv[price_cols] *= Config.PRICE_MULTIPLIER

            return df_ohclv
        except Exception as e:
            self.logger.error(f"⚠️ Lỗi khi kéo ohlcv cho mã {symbol}: {e}")
            return None

    def detect_corporate_actions_via_api(
        self, symbols: List[str], start_date: date, end_date: date
    ) -> Dict[str, date]:
        """Quét sự kiện doanh nghiệp trên VCI cho toàn bộ danh sách symbols trong khoảng ngày.

        Args:
            symbols: Danh sách mã cổ phiếu cần quét.
            start_date: Ngày bắt đầu.
            end_date: Ngày kết thúc.

        Returns:
            Dict mapping mã cổ phiếu -> ngày không hưởng quyền (ex_date) của sự kiện điều chỉnh giá (DIV, ISS).
        """
        if not symbols:
            return {}

        start_str = start_date.strftime("%Y%m%d")
        end_str = (end_date + timedelta(days=1)).strftime("%Y%m%d")

        self.logger.info(
            f"🔍 [Corporate Actions API] Quét sự kiện từ {start_date} đến {end_date} cho {len(symbols)} mã..."
        )

        from vnstock.api.company import Company
        import logging

        # Thiết lập logger của VCI về CRITICAL để tránh spam log
        logging.getLogger("vnstock.explorer.vci.company").setLevel(logging.CRITICAL)

        c = Company(symbol='', source='VCI')

        all_events = []
        batch_size = 300
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i : i + batch_size]
            c.provider.symbol = ",".join(batch)

            max_retries = 3
            events = []
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
                        time.sleep(2 * attempt)

            if events:
                all_events.extend(events)

        if not all_events:
            return {}

        df_events = pd.DataFrame(all_events)

        if "exrightDate" not in df_events.columns:
            return {}

        df_events = df_events[df_events["exrightDate"].notna()]
        if df_events.empty:
            return {}

        df_events["ex_date"] = pd.to_datetime(df_events["exrightDate"]).dt.date

        # Lọc các sự kiện có ex_date nằm trong khoảng [start_date, end_date]
        df_filtered = df_events[
            (df_events["ex_date"] >= start_date) & 
            (df_events["ex_date"] <= end_date)
        ]

        if df_filtered.empty:
            return {}

        # Sắp xếp để lấy ngày ex_date nhỏ nhất nếu có nhiều sự kiện
        df_filtered = df_filtered.sort_values(by="ex_date")
        
        detected_map = {}
        for idx, row in df_filtered.iterrows():
            ticker = str(row["ticker"]).strip().upper()
            ex_date = row["ex_date"]
            if ticker and ticker not in ["", "NAN", "NONE"] and isinstance(ex_date, date):
                if ticker not in detected_map:
                    detected_map[ticker] = ex_date

        return detected_map


class VnstockExtractorETL:
    """Bộ điều phối chính (Orchestrator), quản lý vòng đời và luồng chạy của toàn bộ Pipeline."""

    def __init__(self, logger_name: str = Config.DEFAULT_LOGGER_NAME) -> None:
        """Khởi tạo và kết nối các thành phần độc lập của hệ thống ETL.

        Args:
            logger_name: Tên của Logger hệ thống.
        """
        self.logger: logging.Logger = setup_logger(logger_name)

        # Dependency Injection (SOLID: Tách biệt hoàn toàn trách nhiệm)
        self.processor = DataProcessor(self.logger)
        self.storage = Storage(self.logger)

    def run(self) -> Optional[pd.DataFrame]:
        # 1. Lấy danh sách ký hiệu và sàn tương ứng
        symbols_map = self.processor.get_symbols_with_exchange()
        symbols = list(symbols_map.keys())
        
        # 2. Tải dữ liệu T0 ngày hôm nay (bao gồm giá tham chiếu và trung bình tạm thời)
        df_t0 = self.processor.fetch_entire_market_t0(symbols)

        if df_t0.empty:
            self.logger.warning("⚠️ Không lấy được dữ liệu T0. Dừng pipeline.")
            return None

        # Lấy ngày lớn nhất trong df_t0 làm mốc chặn trên
        t0_max_date = df_t0["trading_date"].dt.date.max()

        # 3. Lấy checkpoint để kiểm tra ngày chạy cuối cùng thành công
        latest_state = self.storage.read_checkpoint()
        metadata = latest_state.get("metadata") or {}
        last_run_str = metadata.get("last_successful_run")
        is_eod = metadata.get("is_eod", False)
        pending_reloads = metadata.get("pending_adjusted_reloads") or []
        
        date_latest_state = None
        if last_run_str:
            try:
                date_latest_state = datetime.strptime(last_run_str, "%Y-%m-%d").date()
                self.logger.info(
                    f"📅 Ngày chạy daily cuối cùng thành công: {date_latest_state} "
                    f"({'Đã chốt phiên EOD' if is_eod else 'Chưa chốt phiên EOD'})"
                )
            except Exception as e:
                self.logger.warning(f"⚠️ Không thể phân tích ngày chạy cuối cùng {last_run_str}: {e}")

        # --- CHỐT CHẶN: KIỂM TRA NGÀY GIAO DỊCH MỚI ---
        if date_latest_state and t0_max_date < date_latest_state:
            self.logger.warning(
                f"ℹ️ Ngày giao dịch lớn nhất của dữ liệu mới tải ({t0_max_date}) "
                f"cũ hơn ngày đã chạy thành công gần nhất ({date_latest_state}). Dừng pipeline."
            )
            return None
        elif date_latest_state and t0_max_date == date_latest_state and is_eod:
            self.logger.info(
                f"ℹ️ Ngày giao dịch {t0_max_date} đã được chạy thành công và chốt phiên EOD trước đó. Dừng pipeline."
            )
            return None
        elif date_latest_state and t0_max_date == date_latest_state and not is_eod:
            self.logger.warning(
                f"🔔 Chạy lại ngày {t0_max_date} do phiên chạy trước chưa chốt EOD. Đang tiến hành cập nhật dữ liệu EOD chính thức..."
            )

        # 4. Tính toán các ngày giao dịch bị thiếu để chạy backfill
        missing_dates = []
        if date_latest_state:
            start_offset = 1 if is_eod else 0
            current_date = date_latest_state + timedelta(days=start_offset)
            while current_date < t0_max_date:
                if current_date.weekday() < 5:
                    current_date_str = current_date.strftime("%Y-%m-%d")
                    if current_date_str not in Config.VN_HOLIDAY_DATES:
                        missing_dates.append(current_date)
                current_date += timedelta(days=1)

        # 4.5. Phát hiện sự kiện chia cổ tức/sự kiện doanh nghiệp (Corporate Actions) trước khi chạy Backfill
        detected_corporate_actions = {} 
        suspected_tickers = []
        today_date = t0_max_date
        today_str = today_date.strftime("%Y-%m-%d")
        
        try:
            self.logger.info(f"🔍 [Corporate Actions T0] Đang quét chênh lệch giá cục bộ cho ngày {today_str}...")
            
            if latest_state and "snapshots" in latest_state:
                old_snapshots = latest_state["snapshots"]
                for idx, row in df_t0.iterrows():
                    sym = str(row["symbol"]).strip().upper()
                    if not sym or sym not in old_snapshots:
                        continue
                        
                    snap = old_snapshots[sym]
                    exch = snap.get("exchange")
                    ref_price = row.get("reference_price")
                    if ref_price is None or pd.isna(ref_price) or ref_price <= 0:
                        continue
                        
                    if exch in ["HoSE", "HNX"]:
                        prev_close = snap.get("close_price")
                        if prev_close and abs(ref_price - prev_close) > 10:
                            suspected_tickers.append(sym)
                    elif exch == "UPCoM":
                        prev_avg = snap.get("average_price")
                        if prev_avg and abs(ref_price - prev_avg) > 300:
                            suspected_tickers.append(sym)
            
                if suspected_tickers:
                    self.logger.warning(f"🔔 Phát hiện {len(suspected_tickers)} mã nghi ngờ lệch giá: {suspected_tickers}. Tiến hành gọi API dự phòng...")
                    today_formatted_str = today_date.strftime("%Y%m%d")
                    from vnstock.api.company import Company
                    import logging
                    logging.getLogger("vnstock.explorer.vci.company").setLevel(logging.CRITICAL)
                    c = Company(symbol='', source='VCI')
                    c.provider.symbol = ",".join(suspected_tickers)
                    self.processor.rate_limiter.hit()
                    events = c._fetch_events(
                        from_date=today_formatted_str,
                        to_date=today_formatted_str,
                        event_codes="DIV,ISS",
                        size=1000
                    )
                    if events:
                        for ev in events:
                            ticker = str(ev.get("ticker")).strip().upper()
                            if ticker in suspected_tickers:
                                self.logger.warning(f"✅ [Xác thực thành công] Mã {ticker} thực sự có sự kiện doanh nghiệp hôm nay.")
                                detected_corporate_actions[ticker] = today_date
                    else:
                        self.logger.info("ℹ️ Không tìm thấy sự kiện khớp trên API. Lệch giá do sai số làm tròn.")
                else:
                    self.logger.info("ℹ️ Không phát hiện mã nào lệch giá bất thường hôm nay.")
            else:
                self.logger.info("ℹ️ Checkpoint cũ trống, bỏ qua quét sự kiện T0.")
        except Exception as e:
            self.logger.error(f"⚠️ Lỗi khi quét sự kiện doanh nghiệp T0: {e}", exc_info=True)
 
        if missing_dates:
            try:
                backfill_start = min(missing_dates)
                backfill_end = max(missing_dates)
                self.logger.info(f"🔍 [Corporate Actions Backfill] Quét sự kiện từ {backfill_start} đến {backfill_end} bằng API...")
                detected_backfill_map = self.processor.detect_corporate_actions_via_api(symbols, backfill_start, backfill_end)
                if detected_backfill_map:
                    self.logger.warning(f"🔔 [Backfill] Phát hiện {len(detected_backfill_map)} mã có sự kiện trong thời gian backfill: {list(detected_backfill_map.keys())}")
                    detected_corporate_actions.update(detected_backfill_map)
            except Exception as e:
                self.logger.error(f"⚠️ Lỗi khi quét sự kiện doanh nghiệp Backfill qua API: {e}", exc_info=True)
 
        if pending_reloads:
            self.logger.warning(f"🔄 Phát hiện {len(pending_reloads)} mã bị lỗi reload phiên trước, tự động đưa vào danh sách chạy lại hôm nay: {pending_reloads}")
            for p_ticker in pending_reloads:
                detected_corporate_actions[p_ticker.upper()] = t0_max_date
 
        if missing_dates:
            self.logger.info(f"🚀 Phát hiện {len(missing_dates)} ngày thiếu cần backfill. Tiến hành tải và xử lý qua CafeF...")
            from extractors.extractor_cafef import CafeFExtractorETL
            cafe_etl = CafeFExtractorETL(logger_name=self.logger.name)
            for m_date in sorted(missing_dates):
                self.logger.info(f"📅 [Backfill CafeF] Đang tải dữ liệu thô cho ngày {m_date}...")
                dt_ref = datetime.combine(m_date, datetime.min.time())
                res = cafe_etl.run(dt_ref, is_raw=True, partition=True, save_checkpoint=False)
                if res is None:
                    raise RuntimeError(
                        f"❌ Chạy backfill CafeF thất bại cho ngày {m_date.strftime('%Y-%m-%d')}. "
                        "Dừng pipeline để tránh mất mát/lủng dữ liệu lịch sử."
                    )
 
        df_t0_parquet = df_t0.drop(columns=["reference_price", "average_price"], errors="ignore")
        gcs_path_t0 = self.storage.save_parquet(df_t0_parquet, datetime.combine(today_date, datetime.min.time()), partition=True)
        
        if gcs_path_t0:
            dt_today = datetime.combine(today_date, datetime.min.time())
            self.storage.delete_by_date(Config.BQ_RAW_TABLE, dt_today)
            self.storage.load_parquet_to_bigquery(gcs_path_t0, Config.BQ_RAW_TABLE, write_disposition="WRITE_APPEND")

        failed_reloads = []
        if detected_corporate_actions:
            tickers_list = sorted(list(detected_corporate_actions.keys()))
            self.logger.warning(f"🚀 Bắt đầu tải lại toàn bộ lịch sử Giá điều chỉnh cho {len(tickers_list)} mã phát hiện được: {tickers_list}...")
            for ticker in tickers_list:
                try:
                    df_hist_adj = self.processor.fetch_ohlcv(ticker, start_date="2000-01-01", end_date=datetime.now(Config.VN_TZ).strftime("%Y-%m-%d"), limit=15000)
                    if df_hist_adj is not None and not df_hist_adj.empty:
                        df_hist_adj = df_hist_adj.copy()
                        df_hist_adj["symbol"] = ticker
                        df_hist_adj["exchange"] = symbols_map.get(ticker, "Unknown")
                        df_hist_adj["symbol"] = df_hist_adj["symbol"].astype(str).str.strip().str.upper().astype("category")
                        df_hist_adj["exchange"] = df_hist_adj["exchange"].apply(normalize_exchange).astype(pd.CategoricalDtype(categories=["HoSE", "HNX", "UPCoM", "Unknown"]))
                        df_hist_adj["trading_date"] = pd.to_datetime(df_hist_adj["trading_date"]).dt.normalize()
                        price_cols = ["open_price", "high_price", "low_price", "close_price"]
                        df_hist_adj[price_cols] = df_hist_adj[price_cols].astype("float32")
                        df_hist_adj["total_volume"] = df_hist_adj["total_volume"].astype("Int32")
                        target_cols = ["symbol", "trading_date", "open_price", "high_price", "low_price", "close_price", "total_volume", "exchange"]
                        df_hist_adj = df_hist_adj[target_cols]

                        # --- GIẢI PHÁP KHẮC PHỤC RỦI RO THIẾU T0 ---
                        # Kiểm tra xem dòng cuối của df_hist_adj có trùng với ngày hôm nay (today_date) không
                        max_hist_date = df_hist_adj["trading_date"].dt.date.max()
                        if max_hist_date < today_date:
                            self.logger.warning(f"⚠️ Dữ liệu lịch sử điều chỉnh tải về cho mã {ticker} bị thiếu ngày hôm nay ({today_date}). Tiến hành tự động bù dữ liệu T0...")
                            # Tìm dòng dữ liệu T0 tương ứng trong df_t0
                            df_t0_sym = df_t0[df_t0["symbol"] == ticker]
                            if not df_t0_sym.empty:
                                t0_row = df_t0_sym.iloc[0].copy()
                                # Tạo DataFrame chứa dữ liệu T0
                                df_t0_append = pd.DataFrame([{
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
                                df_t0_append["exchange"] = df_t0_append["exchange"].apply(normalize_exchange).astype(pd.CategoricalDtype(categories=["HoSE", "HNX", "UPCoM", "Unknown"]))
                                df_t0_append["trading_date"] = pd.to_datetime(df_t0_append["trading_date"]).dt.normalize()
                                df_t0_append[price_cols] = df_t0_append[price_cols].astype("float32")
                                df_t0_append["total_volume"] = df_t0_append["total_volume"].astype("Int32")

                                # Hợp nhất dữ liệu
                                df_hist_adj = pd.concat([df_hist_adj, df_t0_append], ignore_index=True)
                                self.logger.info(f"✅ Đã bù thành công dữ liệu T0 cho mã {ticker}.")
                            else:
                                self.logger.error(f"❌ Không tìm thấy dữ liệu T0 của mã {ticker} trong df_t0 để bù.")
                        # --------------------------------------------
                        self.storage.save_symbol_history(df_hist_adj, ticker, suffix="adj")
                        self.storage.sync_adjusted_symbol_to_bigquery(ticker)
                    else:
                        self.logger.error(f"❌ Không thể tải lịch sử giá cho mã {ticker}")
                        failed_reloads.append(ticker)
                except Exception as e:
                    self.logger.error(f"❌ Lỗi khi tải lại lịch sử mã {ticker}: {e}", exc_info=True)
                    failed_reloads.append(ticker)
        else:
            self.logger.info("ℹ️ Không phát hiện mã cổ phiếu nào cần tải lại lịch sử Giá điều chỉnh.")

        # (Lặp qua cả ngày hôm nay và các ngày chạy backfill nếu có)
        all_processing_dates = sorted(missing_dates + [today_date])
        all_excluded_symbols = list(detected_corporate_actions.keys())
        for p_date in all_processing_dates:
            self.storage.sync_daily_adjusted_prices(p_date, all_excluded_symbols)

        # Cập nhật Checkpoint Snapshot EOD và lưu danh sách các mã lỗi chưa reload xong
        self.storage.save_checkpoint(
            df=df_t0,
            active_symbols=set(symbols),
            pending_adjusted_reloads=failed_reloads
        )
        self.logger.info("🎉 Đã lưu checkpoint cập thái trạng trường EOD thành công.")

        # Gửi báo cáo kết quả chạy hàng ngày qua Telegram
        try:
            vn_now = datetime.now(Config.VN_TZ)
            today_str_check = vn_now.strftime("%Y-%m-%d")
            t0_date_str = today_date.strftime("%Y-%m-%d")
            if t0_date_str < today_str_check:
                run_is_eod = True
            else:
                run_is_eod = vn_now.hour > 15 or (vn_now.hour == 15 and vn_now.minute >= 15)

            reloaded_symbols = [s for s in detected_corporate_actions.keys() if s not in failed_reloads]

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

