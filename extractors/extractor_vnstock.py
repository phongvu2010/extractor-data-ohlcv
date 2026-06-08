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

    def get_symbols(self):
        self.rate_limiter.hit()
        df_symbols = self.reference_api.equity().list_by_exchange()
        df_symbols = df_symbols[~df_symbols["type"].isin(["corpbond", "bond", "future"])]

        return df_symbols["symbol"].tolist()

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
        df_all["trading_date"] = pd.to_datetime(df_all["time"], unit="ms").dt.normalize()
        df_all["total_volume"] = df_all["volume_accumulated"]

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
                        event_codes="DIV,ISS"
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

    def get_raw_to_adjusted_ratio(self, symbol: str, latest_state: dict, ex_date: date) -> float:
        """Tính toán tỷ lệ chuyển đổi từ giá điều chỉnh sang giá thô dựa trên checkpoint trước sự kiện.

        Tỷ lệ phục hồi (recovery ratio) R = Raw_close_prev / Adj_close_prev.
        Phục hồi giá thô lịch sử: Raw_price(t) = Adj_price(t) * R với mọi ngày t trước ex_date.
        """
        try:
            if not latest_state or "snapshots" not in latest_state:
                return 1.0
            snapshots = latest_state["snapshots"]
            if symbol not in snapshots:
                return 1.0
                
            snap = snapshots[symbol]
            prev_raw_close = snap.get("close_price")
            prev_date_str = snap.get("trading_date")
            
            if not prev_raw_close or not prev_date_str:
                return 1.0
                
            # Đảm bảo ngày checkpoint là trước ngày không hưởng quyền
            prev_date = datetime.strptime(prev_date_str, "%Y-%m-%d").date()
            if prev_date >= ex_date:
                self.logger.warning(
                    f"⚠️ [Ratio Calc] Ngày checkpoint {prev_date_str} của {symbol} không nằm trước ex_date {ex_date}."
                )
                return 1.0
                
            # Tải giá điều chỉnh tại ngày checkpoint từ vnstock
            self.logger.info(
                f"🔄 [Ratio Calc] Đang tải giá điều chỉnh ngày {prev_date_str} của {symbol} để tính tỷ lệ phục hồi..."
            )
            df_prev = self.processor.fetch_ohlcv(symbol, prev_date_str, prev_date_str, limit=1)
            if df_prev is not None and not df_prev.empty:
                df_prev_filtered = df_prev[pd.to_datetime(df_prev["trading_date"]).dt.date == prev_date]
                if df_prev_filtered.empty:
                    last_row = df_prev.iloc[-1]
                    if pd.to_datetime(last_row["trading_date"]).date() == prev_date:
                        prev_adj_close = float(last_row["close_price"])
                    else:
                        prev_adj_close = 0.0
                else:
                    prev_adj_close = float(df_prev_filtered["close_price"].iloc[0])

                if prev_adj_close > 0:
                    ratio = float(prev_raw_close) / prev_adj_close
                    self.logger.info(
                        f"📊 [Ratio Calc] Tỷ lệ phục hồi giá thô cho {symbol}: {ratio:.6f} "
                        f"(Raw={prev_raw_close}, Adj={prev_adj_close})"
                    )
                    return ratio
            self.logger.warning(f"⚠️ Không thể tải giá điều chỉnh ngày {prev_date_str} cho {symbol} để tính tỷ lệ.")
        except Exception as e:
            self.logger.error(f"❌ Lỗi khi tính tỷ lệ phục hồi giá cho {symbol}: {e}", exc_info=True)
        return 1.0

    def run(self) -> Optional[pd.DataFrame]:
        # 1. Lấy danh sách ký hiệu và sàn tương ứng
        symbols_map = self.processor.get_symbols_with_exchange()
        symbols = list(symbols_map.keys())
        
        # 2. Tải dữ liệu T0 ngày hôm nay (bao gồm giá tham chiếu và trung bình tạm thời)
        df_t0 = self.processor.fetch_entire_market_t0(symbols)

        if df_t0.empty:
            self.logger.warning("⚠️ Không lấy được dữ liệu T0. Dừng pipeline.")
            return None

        # 3. Lấy checkpoint để kiểm tra ngày chạy cuối cùng thành công
        latest_state = self.storage.read_checkpoint()
        metadata = latest_state.get("metadata") or {}
        last_run_str = metadata.get("last_successful_run")
        is_eod = metadata.get("is_eod", False)
        
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

        # 4. Tính toán các ngày giao dịch bị thiếu để chạy backfill
        missing_dates = []
        if date_latest_state:
            # Lấy ngày lớn nhất trong df_t0 làm mốc chặn trên
            t0_max_date = df_t0["trading_date"].dt.date.max()
            
            # Quét qua các ngày tính từ ngày checkpoint chạy thành công. 
            # Nếu ngày checkpoint cũ đã chốt phiên EOD -> Bắt đầu quét từ ngày tiếp theo.
            # Nếu chưa chốt phiên EOD -> Bắt đầu quét từ chính ngày đó để chạy bù dữ liệu EOD chuẩn.
            start_offset = 1 if is_eod else 0
            current_date = date_latest_state + timedelta(days=start_offset)
            while current_date < t0_max_date:
                # Bỏ qua cuối tuần
                if current_date.weekday() < 5:
                    # Bỏ qua ngày lễ Việt Nam
                    current_date_str = current_date.strftime("%Y-%m-%d")
                    if current_date_str not in Config.VN_HOLIDAY_DATES:
                        missing_dates.append(current_date)
                current_date += timedelta(days=1)

        # 4.5. Phát hiện sự kiện chia cổ tức/sự kiện doanh nghiệp (Corporate Actions) trước khi chạy Backfill
        detected_corporate_actions = {} # Map symbol -> ex_date
        
        # A. Quét sự kiện của ngày hôm nay (T0) bằng cách so sánh giá tham chiếu hôm nay với giá chốt phiên hôm qua
        try:
            today_str = df_t0["trading_date"].dt.strftime("%Y-%m-%d").max()
            today_date = df_t0["trading_date"].dt.date.max()
            self.logger.info(f"🔍 [Corporate Actions T0] Đang quét chênh lệch giá cho ngày {today_str}...")
            
            # Đọc snapshot từ checkpoint cũ
            if latest_state and "snapshots" in latest_state:
                old_snapshots = latest_state["snapshots"]
                
                # Duyệt qua các cổ phiếu giao dịch hôm nay
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
                            self.logger.warning(
                                f"🔔 [T0] Phát hiện biến động giá cho {sym} ({exch}): "
                                f"Ref Price={ref_price:.0f}, Prev Close={prev_close:.0f}. "
                                f"Chênh lệch={abs(ref_price - prev_close):.0f} VND."
                            )
                            detected_corporate_actions[sym] = today_date
                    elif exch == "UPCoM":
                        prev_avg = snap.get("average_price")
                        if prev_avg and abs(ref_price - prev_avg) > 100:
                            self.logger.warning(
                                f"🔔 [T0] Phát hiện biến động giá cho {sym} ({exch}): "
                                f"Ref Price={ref_price:.0f}, Prev Avg={prev_avg:.1f}. "
                                f"Chênh lệch={abs(ref_price - prev_avg):.1f} VND."
                            )
                            detected_corporate_actions[sym] = today_date
            else:
                self.logger.info("ℹ️ Checkpoint cũ trống, bỏ qua quét sự kiện T0 qua so sánh giá.")
        except Exception as e:
            self.logger.error(f"⚠️ Lỗi khi quét sự kiện doanh nghiệp T0 qua so sánh giá: {e}", exc_info=True)

        # B. Quét sự kiện của các ngày chạy backfill (nếu có) bằng API
        if missing_dates:
            try:
                backfill_start = min(missing_dates)
                backfill_end = max(missing_dates)
                self.logger.info(
                    f"🔍 [Corporate Actions Backfill] Quét sự kiện từ {backfill_start} đến {backfill_end} bằng API..."
                )
                detected_backfill_map = self.processor.detect_corporate_actions_via_api(
                    symbols, backfill_start, backfill_end
                )
                if detected_backfill_map:
                    self.logger.warning(
                        f"🔔 [Backfill] Phát hiện {len(detected_backfill_map)} mã có sự kiện trong thời gian backfill: {list(detected_backfill_map.keys())}"
                    )
                    detected_corporate_actions.update(detected_backfill_map)
            except Exception as e:
                self.logger.error(f"⚠️ Lỗi khi quét sự kiện doanh nghiệp Backfill qua API: {e}", exc_info=True)

        # 5. Tiến hành chạy backfill nếu phát hiện ngày bị thiếu
        if missing_dates:
            start_date_str = min(missing_dates).strftime("%Y-%m-%d")
            end_date_str = max(missing_dates).strftime("%Y-%m-%d")
            self.logger.info(
                f"🚀 Phát hiện {len(missing_dates)} ngày thiếu cần backfill: {missing_dates}. "
                f"Bắt đầu kéo dữ liệu từ {start_date_str} đến {end_date_str}..."
            )
            
            backfill_dfs = []
            
            # Tính toán limit động bên ngoài vòng lặp để tránh bị thiếu dòng nếu khoảng backfill lớn
            num_days = (max(missing_dates) - min(missing_dates)).days + 5
            backfill_limit_count = max(100, num_days)
            
            # Giới hạn số lượng mã kéo thử nếu cấu hình chế độ test (BACKFILL_LIMIT)
            limit_symbols = symbols
            if Config.BACKFILL_LIMIT > 0:
                limit_symbols = symbols[:Config.BACKFILL_LIMIT]
                self.logger.warning(f"⚠️ [Test Mode] Chỉ thực hiện backfill cho {Config.BACKFILL_LIMIT} mã đầu tiên.")
            
            for idx, symbol in enumerate(limit_symbols):
                if idx > 0 and idx % 50 == 0:
                    self.logger.info(f"⏳ Tiến trình backfill: {idx}/{len(limit_symbols)} mã...")
                
                df_ohclv = self.processor.fetch_ohlcv(symbol, start_date_str, end_date_str, limit=backfill_limit_count)
                if df_ohclv is not None and not df_ohclv.empty:
                    df_ohclv = df_ohclv.copy()
                    
                    # ⚠️ PHỤC HỒI GIÁ THÔ CHO DỮ LIỆU BACKFILL NẾU CÓ CỔ TỨC/CHIA TÁCH
                    if symbol in detected_corporate_actions:
                        ex_date = detected_corporate_actions[symbol]
                        ratio = self.get_raw_to_adjusted_ratio(symbol, latest_state, ex_date)
                        if ratio != 1.0:
                            # Lọc các ngày trong df_ohclv nằm trước ngày không hưởng quyền (ex_date)
                            mask = df_ohclv["trading_date"].dt.date < ex_date
                            if mask.any():
                                self.logger.warning(
                                    f"✏️ [Backfill Raw Price Recovery] Đang phục hồi Giá thô cho {symbol} "
                                    f"cho {mask.sum()} ngày trước ex_date {ex_date} bằng tỷ lệ {ratio:.6f}..."
                                )
                                price_cols = ["open_price", "high_price", "low_price", "close_price"]
                                # Nhân tỷ lệ và làm tròn về số nguyên
                                df_ohclv.loc[mask, price_cols] = (df_ohclv.loc[mask, price_cols] * ratio).round(0).astype("float32")
                    
                    # Lọc chỉ lấy các ngày thực sự bị thiếu
                    df_ohclv = df_ohclv[df_ohclv["trading_date"].dt.date.isin(missing_dates)]
                    if not df_ohclv.empty:
                        df_ohclv["symbol"] = symbol
                        df_ohclv["exchange"] = symbols_map[symbol]
                        backfill_dfs.append(df_ohclv)
            
            if backfill_dfs:
                df_backfill = pd.concat(backfill_dfs, ignore_index=True)
                
                # Đồng bộ dtypes và schema cho df_backfill giống CafeF
                df_backfill["symbol"] = df_backfill["symbol"].astype(str).str.strip().str.upper().astype("category")
                df_backfill["exchange"] = df_backfill["exchange"].apply(normalize_exchange).astype(
                    pd.CategoricalDtype(categories=["HoSE", "HNX", "UPCoM", "Unknown"])
                )
                df_backfill["trading_date"] = pd.to_datetime(df_backfill["trading_date"]).dt.normalize()
                
                price_cols = ["open_price", "high_price", "low_price", "close_price"]
                df_backfill[price_cols] = df_backfill[price_cols].astype("float32")
                df_backfill["total_volume"] = df_backfill["total_volume"].astype("Int32")
                
                target_cols = ["symbol", "trading_date", "open_price", "high_price", "low_price", "close_price", "total_volume", "exchange"]
                df_backfill = df_backfill[target_cols]
                
                # Phân nhóm theo trading_date và lưu từng file phân mảnh lên GCS theo thứ tự thời gian tăng dần
                for date_group, group_df in sorted(df_backfill.groupby(df_backfill["trading_date"].dt.date), key=lambda x: x[0]):
                    date_ref_group = datetime.combine(date_group, datetime.min.time())
                    self.logger.info(f"💾 ☁️ [Backfill] Đang lưu phân mảnh GCS cho ngày {date_group}...")
                    self.storage.save_parquet(group_df, date_ref_group, partition=True)
            else:
                self.logger.warning("⚠️ Không thể tải dữ liệu backfill cho bất kỳ mã nào.")

        # 6. Ghi đè phân mảnh Parquet cho ngày hôm nay (T0) (chỉ lưu 8 cột CafeF)
        df_t0_parquet = df_t0.drop(columns=["reference_price", "average_price"], errors="ignore")
        self.storage.save_parquet(df_t0_parquet, datetime.now(), partition=True)

        # 7. Lưu checkpoint snapshot EOD cho ngày hôm nay (T0)
        # Note: Chúng ta truyền df_t0 chứa cột average_price thực tế từ bảng giá để checkpoint lưu giá trung bình chuẩn
        self.storage.save_checkpoint(df_t0, set(symbols))

        # 8. Thực hiện reload toàn bộ lịch sử Giá điều chỉnh cho các mã phát hiện được
        if detected_corporate_actions:
            tickers_list = sorted(list(detected_corporate_actions.keys()))
            self.logger.warning(
                f"🚀 Bắt đầu tải lại toàn bộ lịch sử Giá điều chỉnh cho {len(tickers_list)} mã phát hiện được: {tickers_list}..."
            )
            for ticker in tickers_list:
                try:
                    # Tải lịch sử giá điều chỉnh (Vnstock ohlcv trả về giá điều chỉnh mặc định, limit=15000 để lấy toàn bộ từ đầu)
                    df_hist_adj = self.processor.fetch_ohlcv(
                        ticker, 
                        start_date="2000-01-01", 
                        end_date=datetime.now(Config.VN_TZ).strftime("%Y-%m-%d"), 
                        limit=15000
                    )
                    if df_hist_adj is not None and not df_hist_adj.empty:
                        # Đồng bộ schema và kiểu dữ liệu giống CafeF
                        df_hist_adj = df_hist_adj.copy()
                        df_hist_adj["symbol"] = ticker
                        if ticker in symbols_map:
                            df_hist_adj["exchange"] = symbols_map[ticker]
                        else:
                            df_hist_adj["exchange"] = "Unknown"
                            
                        df_hist_adj["symbol"] = df_hist_adj["symbol"].astype(str).str.strip().str.upper().astype("category")
                        df_hist_adj["exchange"] = df_hist_adj["exchange"].apply(normalize_exchange).astype(
                            pd.CategoricalDtype(categories=["HoSE", "HNX", "UPCoM", "Unknown"])
                        )
                        df_hist_adj["trading_date"] = pd.to_datetime(df_hist_adj["trading_date"]).dt.normalize()
                        
                        price_cols = ["open_price", "high_price", "low_price", "close_price"]
                        df_hist_adj[price_cols] = df_hist_adj[price_cols].astype("float32")
                        df_hist_adj["total_volume"] = df_hist_adj["total_volume"].astype("Int32")
                        
                        target_cols = ["symbol", "trading_date", "open_price", "high_price", "low_price", "close_price", "total_volume", "exchange"]
                        df_hist_adj = df_hist_adj[target_cols]
                        
                        # Lưu đè lịch sử giá điều chỉnh của mã này lên GCS
                        self.storage.save_symbol_history(df_hist_adj, ticker, suffix="adj")
                    else:
                        self.logger.error(f"❌ Không thể tải lịch sử giá cho mã {ticker}")
                except Exception as e:
                    self.logger.error(f"❌ Lỗi khi tải lại lịch sử mã {ticker}: {e}", exc_info=True)
        else:
            self.logger.info("ℹ️ Không phát hiện mã cổ phiếu nào cần tải lại lịch sử Giá điều chỉnh.")

        return df_t0
