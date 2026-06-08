import logging
import numpy as np
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from typing import List, Optional
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
        df_all["total_volume"] = df_all["total_volume"].astype("Int32")

        # Khớp chính xác 8 cột theo đúng thứ tự của CafeF
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
        
        # 2. Tải dữ liệu T0 ngày hôm nay
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

        # 5. Tiến hành chạy backfill nếu phát hiện ngày bị thiếu
        if missing_dates:
            start_date_str = min(missing_dates).strftime("%Y-%m-%d")
            end_date_str = max(missing_dates).strftime("%Y-%m-%d")
            self.logger.info(
                f"🚀 Phát hiện {len(missing_dates)} ngày thiếu cần backfill: {missing_dates}. "
                f"Bắt đầu kéo dữ liệu từ {start_date_str} đến {end_date_str}..."
            )
            
            backfill_dfs = []
            
            # Giới hạn số lượng mã kéo thử nếu cấu hình chế độ test (BACKFILL_LIMIT)
            limit_symbols = symbols
            if Config.BACKFILL_LIMIT > 0:
                limit_symbols = symbols[:Config.BACKFILL_LIMIT]
                self.logger.warning(f"⚠️ [Test Mode] Chỉ thực hiện backfill cho {Config.BACKFILL_LIMIT} mã đầu tiên.")
            
            for idx, symbol in enumerate(limit_symbols):
                if idx > 0 and idx % 50 == 0:
                    self.logger.info(f"⏳ Tiến trình backfill: {idx}/{len(limit_symbols)} mã...")
                
                df_ohclv = self.processor.fetch_ohlcv(symbol, start_date_str, end_date_str)
                if df_ohclv is not None and not df_ohclv.empty:
                    df_ohclv = df_ohclv.copy()
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

        # 6. Ghi đè phân mảnh Parquet cho ngày hôm nay (T0)
        self.storage.save_parquet(df_t0, datetime.now(), partition=True)

        # 7. Lưu checkpoint snapshot EOD cho ngày hôm nay (T0)
        self.storage.save_checkpoint(df_t0, set(symbols))

        return df_t0
