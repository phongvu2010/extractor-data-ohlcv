import contextlib
import gc
import io
import logging
import pandas as pd
import random
import requests
import time
import zipfile
from datetime import datetime
from requests.adapters import HTTPAdapter
from typing import Any, Dict, List, Optional, Set
from urllib3.util import Retry

from config import Config
from storages import Storage
from utils import normalize_exchange, setup_logger


class Downloader:
    """Chuyên trách việc giao tiếp mạng, tải tệp và trích xuất luồng dữ liệu từ CafeF."""

    def __init__(self, logger: logging.Logger) -> None:
        """Khởi tạo bộ tải dữ liệu.

        Args:
            logger: Đối tượng Logger dùng để ghi nhận tiến trình.
        """
        self.logger: logging.Logger = logger
        self.session: requests.Session = requests.Session()

        # 1. Giả lập Header để vượt qua các bộ lọc Bot cơ bản của CDN
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "Anonymized/7cb0ab2238 AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

        # Cấu hình tự động tải lại 3 lần nếu gặp lỗi server hoặc timeout
        retries = Retry(
            total=3,
            backoff_factor=2, # Tăng lên 2s để giãn cách các lần retry tự động (2s, 4s, 8s)
            status_forcelist=[500, 502, 503, 504]
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def __enter__(self) -> "Downloader":
        """Hỗ trợ Context Manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Tự động giải phóng connection pool khi thoát khối lệnh 'with'."""
        if self.session:
            self.session.close()
            self.logger.info("🔌 [Downloader] Đã đóng tài nguyên kết nối mạng an toàn.")

    def download_zip_stream(self, date_ref: datetime, is_raw: bool) -> Optional[io.BytesIO]:
        """Tải tệp zip từ CafeF về bộ nhớ RAM dưới dạng BytesIO stream bằng cơ chế kiểm tra an toàn.

        Args:
            date_ref: Ngày cần tải dữ liệu.
            is_raw: True nếu là dữ liệu thô, False nếu là dữ liệu điều chỉnh.

        Returns:
            Một luồng dữ liệu io.BytesIO nếu tải thành công, ngược lại là None.
        """
        raw_prefix: str = "Raw." if is_raw else ""
        date_str_file: str = date_ref.strftime("%d%m%Y")
        date_str_url: str = date_ref.strftime("%Y%m%d")

        file_name: str = f"CafeF.SolieuGD.{raw_prefix}Upto{date_str_file}.zip"
        url: str = f"{Config.URL_CAFEF}{date_str_url}/{file_name}"

        # 2. Cơ chế Jitter: Tránh spam request liên tục khi chạy cào lịch sử (Init)
        # Nghỉ ngẫu nhiên từ 1.5 đến 3.5 giây trước khi thực hiện request thực tế
        sleep_time = random.uniform(1.5, 3.5)
        self.logger.info(f"⏳ [Downloader] Chờ {sleep_time:.2f}s để giãn cách tần suất request...")
        time.sleep(sleep_time)

        self.logger.info(
            f"📥 [CafeF] Đang tải file lịch sử từ URL: [blue underline]{url}[/blue underline]",
            extra={"markup": True},
        )

        try:
            # Sử dụng stream=True để trì hoãn việc tải xuống phần thân (body) của file
            response: requests.Response = self.session.get(url, timeout=Config.NETWORK_TIMEOUT, stream=True)
            response.raise_for_status()

            # 1. Phòng thủ vòng 1: Kiểm tra Content-Length (nếu có) hoặc loại nội dung
            content_type = response.headers.get("Content-Type", "")
            if "text/html" in content_type:
                self.logger.error("❌ [CafeF] URL trả về trang HTML (có thể là trang lỗi ẩn của CDN), không phải file ZIP.")
                return None

            # 2. Phòng thủ vòng 2: Kiểm tra Magic Bytes (Ký hiệu nhận diện file ZIP)
            # File ZIP chuẩn luôn bắt đầu bằng cụm 'PK\x03\x04' (Hex: 50 4B 03 04)
            ZIP_MAGIC_START = b"PK\x03\x04"

            # Chỉ đọc đúng 4 bytes đầu tiên từ luồng mạng để xác thực
            first_4_bytes = next(response.iter_content(chunk_size=4), b"")

            if first_4_bytes != ZIP_MAGIC_START:
                self.logger.error(
                    f"❌ [CafeF] File tải về không phải định dạng ZIP hợp lệ. Magic bytes nhận được: {first_4_bytes}"
                )
                return None

            # 3. Tiến hành tải phần còn lại khi đã xác nhận file an toàn
            # Khởi tạo luồng BytesIO và nạp 4 bytes đã đọc trước đó vào vị trí đầu tiên
            zip_stream = io.BytesIO()
            zip_stream.write(first_4_bytes)

            # Đọc cuốn chiếu các phần còn lại theo từng block 64KB để ghi vào RAM, tránh phình bộ nhớ đột ngột
            for chunk in response.iter_content(chunk_size=65536):
                if chunk:
                    zip_stream.write(chunk)

            # Đưa con trỏ stream về lại vị trí xuất phát (0) để sẵn sàng cho thư viện zipfile đọc tiếp
            zip_stream.seek(0)
            return zip_stream
        except requests.exceptions.Timeout as e:
            self.logger.error(f"⏳ [CafeF] Yêu cầu bị Timeout sau {Config.NETWORK_TIMEOUT} giây: {e}")
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                self.logger.warning(
                    f"🚫 [CafeF] Không tìm thấy file lịch sử cho ngày {date_str_file}. "
                    "Có thể là ngày lễ hoặc chưa có dữ liệu."
                )
            else:
                self.logger.error(f"❌ [CafeF] Lỗi HTTP: {e}")
        except Exception as e:
            self.logger.error(f"❌ [CafeF] Lỗi tải file từ hệ thống mạng: {e}")

        return None


class DataProcessor:
    """Chuyên trách việc làm sạch, biến đổi và tối ưu hóa dữ liệu chứng khoán."""

    def __init__(self, logger: logging.Logger) -> None:
        """Khởi tạo bộ xử lý dữ liệu và tải danh sách đen (Blacklist).

        Args:
            logger: Đối tượng Logger dùng để ghi nhận tiến trình.
        """
        self.logger: logging.Logger = logger
        self.blacklist: Set[str] = self._load_blacklist()

    def _load_blacklist(self) -> Set[str]:
        """Tải danh sách các mã chứng khoán bị loại bỏ (Blacklist) từ tệp cấu hình.

        Returns:
            Tập hợp (Set) chứa các mã chứng khoán viết hoa cần loại bỏ.
        """
        try:
            with open("blacklist.txt", "r", encoding="utf-8") as file:
                return {line.strip().upper() for line in file if line.strip()}
        except FileNotFoundError:
            self.logger.warning("⚠️ [CafeF] Không tìm thấy file 'blacklist.txt'. Bỏ qua lọc blacklist.")
            return set()

    def _get_column_names(self, include_exchange: bool = True) -> List[str]:
        """Tạo danh bạ tên cột động dựa trên hậu tố raw/adj.

        Args:
            include_exchange: Có bao gồm cột 'exchange' ở vị trí thứ 2 hay không.

        Returns:
            Danh sách chuỗi tên các cột dữ liệu theo đúng thứ tự.
        """
        cols: List[str] = ["symbol", "trading_date", "open_price", "high_price", "low_price", "close_price", "total_volume"]
        if include_exchange:
            cols.append("exchange")
        return cols

    def _clean_chunk(self, chunk: pd.DataFrame, exchange_name: str) -> pd.DataFrame:
        """Làm sạch sâu dữ liệu của một phân đoạn (Chunk) bằng toán tử Vectorization.

        Args:
            chunk: Phân đoạn DataFrame thô vừa đọc từ CSV.
            exchange_name: Tên sàn giao dịch của phân đoạn này.

        Returns:
            DataFrame đã lọc bỏ lỗi logic và mã thuộc danh sách đen.
        """
        # Tránh Deep Nesting bằng Guard Clauses (KISS)
        chunk = chunk.dropna(subset=["symbol", "trading_date"])
        if chunk.empty:
            return chunk

        # Khắc phục SettingWithCopyWarning bằng cách tạo một bản sao độc lập dữ liệu
        chunk = chunk.copy()

        # 1. Chuẩn hóa chuỗi và ép kiểu tối ưu danh mục
        chunk["symbol"] = chunk["symbol"].astype(str).str.strip().str.upper()

        if self.blacklist:
            chunk = chunk[~chunk["symbol"].isin(self.blacklist)]
            if chunk.empty:
                return chunk

        # Định nghĩa sẵn các danh mục để tránh Pandas tự suy luận dynamic theo từng chunk
        chunk["exchange"] = pd.Categorical(
            [exchange_name] * len(chunk), 
            categories=["HoSE", "HNX", "UPCoM", "Unknown"]
        )

        # 2. Đồng bộ đơn vị giá (Vectorization)
        price_cols: List[str] = ["open_price", "high_price", "low_price", "close_price"]
        chunk[price_cols] *= Config.PRICE_MULTIPLIER

        # 3. Ép kiểu dữ liệu số về mức dung lượng thấp hơn (Downcasting)
        # Giá chứng khoán Việt Nam sau nhân 1000 không vượt quá mức Float32, Volume không vượt quá Int32
        chunk[price_cols] = chunk[price_cols].astype("float32")
        chunk["total_volume"] = chunk["total_volume"].astype("int32")

        # 4. Lọc lỗi logic toán học
        valid_mask: pd.Series = (
            (chunk[price_cols] >= 0).all(axis=1)
            & (chunk["total_volume"] >= 0)
            & (chunk["high_price"] >= chunk["low_price"])
            & (chunk["high_price"] >= chunk["open_price"])
            & (chunk["high_price"] >= chunk["close_price"])
            & (chunk["low_price"] <= chunk["open_price"])
            & (chunk["low_price"] <= chunk["close_price"])
        )
        return chunk[valid_mask].copy()

    def process_zip_content(self, zip_data: io.BytesIO) -> pd.DataFrame:
        """Đọc phân đoạn, làm sạch và hợp nhất toàn bộ nội dung bên trong file zip.

        Args:
            zip_data: Luồng bytes của file zip nằm trong bộ nhớ RAM.
            suffix: Hậu tố định danh loại cột ('raw' hoặc 'adj').

        Returns:
            DataFrame tổng hợp đã được làm sạch và tối ưu hóa bộ nhớ.
        """
        cols_order: List[str] = self._get_column_names(include_exchange=False)
        final_cols_order: List[str] = self._get_column_names(include_exchange=True)

        # Định nghĩa kiểu dữ liệu tối giản lúc đọc từ CSV gốc
        # Đọc trading_date như chuỗi (str) tạm thời để tăng tốc độ phân tích cú pháp C-Engine
        csv_dtypes: Dict[str, str] = {
            "symbol": "str",
            "open_price": "float32",
            "high_price": "float32",
            "low_price": "float32",
            "close_price": "float32",
            "total_volume": "float32",
        }

        dfs: List[pd.DataFrame] = []
        with zipfile.ZipFile(zip_data) as z:
            csv_files: List[str] = [name for name in z.namelist() if name.endswith(".csv")]
            if not csv_files:
                self.logger.warning("📭 Tệp Zip trống hoặc không chứa file '.csv' nào.")
                return pd.DataFrame(columns=final_cols_order)

            for name in csv_files:
                self.logger.info(f"📂 Đang trích xuất & xử lý stream sàn: [yellow]{name}[/yellow]")
                detected_exchange: str = normalize_exchange(name)

                with z.open(name) as f:
                    with io.TextIOWrapper(f, encoding="utf-8") as text_stream:
                        # Tối ưu: Parse trực tiếp ngày tháng tại C-Engine bằng cách chỉ định vị trí cột (index 1)
                        chunks = pd.read_csv(
                            text_stream,
                            header=0,
                            names=cols_order,
                            dtype=csv_dtypes,
                            engine="c",
                            on_bad_lines="skip",
                            chunksize=Config.CHUNK_SIZE,
                            parse_dates=[1], 
                            date_format="%Y%m%d"
                        )
                        for chunk in chunks:
                            # List Comprehension kết hợp generator lọc bỏ dataframe rỗng cực nhanh
                            clean_df = self._clean_chunk(chunk, detected_exchange)
                            if not clean_df.empty:
                                dfs.append(clean_df)
                            # Giải phóng phân đoạn hiện tại ngay lập tức
                            del chunk 

        if not dfs:
            return pd.DataFrame(columns=final_cols_order)

        self.logger.info("🧱 Đang gộp các phân đoạn dữ liệu lịch sử và tối ưu RAM...")

        # Gộp dữ liệu (Lúc này các chunk đều đã rất nhẹ do áp dụng categorical & downcast từ trước)
        result_df: pd.DataFrame = pd.concat(dfs, ignore_index=True)

        # Giải phóng mảng tạm lập tức nhằm tối ưu hóa bộ nhớ cho các bước xử lý sau
        del dfs
        gc.collect()

        # Không cần bước pd.to_datetime diện rộng ở đây nữa vì dữ liệu đã chuẩn hóa từ chunk
        result_df["exchange"] = result_df["exchange"].astype(
            pd.CategoricalDtype(categories=["HoSE", "HNX", "UPCoM", "Unknown"])
        )

        # Sắp xếp và loại bỏ trùng lặp (Giữ lại bản ghi cuối cùng của ngày)
        result_df.sort_values(by=["symbol", "trading_date"], inplace=True, ignore_index=True)
        result_df.drop_duplicates(subset=["symbol", "trading_date"], keep="last", inplace=True)

        return result_df[final_cols_order]


class CafeFExtractorETL:
    """Bộ điều phối chính (Orchestrator), quản lý vòng đời và luồng chạy của toàn bộ Pipeline."""

    def __init__(self, logger_name: str = Config.DEFAULT_LOGGER_NAME) -> None:
        """Khởi tạo và kết nối các thành phần độc lập của hệ thống ETL.

        Args:
            logger_name: Tên của Logger hệ thống.
        """
        self.logger: logging.Logger = setup_logger(logger_name)
        self.processor = DataProcessor(self.logger)
        self.storage = Storage(self.logger)

    def run(self, date_ref: datetime, is_raw: bool = True) -> Optional[pd.DataFrame]:
        """Khởi chạy toàn bộ quy trình ETL tải, xử lý và lưu trữ dữ liệu lịch sử.

        Args:
            date_ref: Mốc ngày cần tải dữ liệu lịch sử của CafeF.
            is_raw: True để xử lý file giá thô (Raw), False để xử lý file giá điều chỉnh (Adj).

        Returns:
            DataFrame kết quả cuối cùng nếu thành công, ngược lại trả về None.
        """
        suffix: str = "raw" if is_raw else "adj"
        self.logger.info(f"🚀 Khởi chạy Pipeline CafeF [{suffix.upper()}] ngày: {date_ref.strftime('%Y-%m-%d')}")

        # Khởi tạo Downloader thông qua cấu trúc Context Manager an toàn
        with Downloader(self.logger) as downloader:
            with contextlib.closing(downloader.download_zip_stream(date_ref, is_raw=is_raw)) as zip_stream:
                if not zip_stream or zip_stream.getvalue() == b"":
                    self.logger.error("🛑 Pipeline kết thúc sớm do không tải được tệp tin zip từ máy chủ.")
                    return None

                try:
                    # Bước 2: Phân tích, làm sạch dữ liệu
                    df: pd.DataFrame = self.processor.process_zip_content(zip_stream)

                    if df.empty:
                        self.logger.error("🛑 Pipeline kết thúc sớm do tập dữ liệu sau khi làm sạch trống rỗng.")
                        return None

                    self.logger.info(f"✅ Tải và làm sạch dữ liệu thành công! Kích thước: {df.shape}")

                    # Bước 3: Ghi file dữ liệu nén Parquet chuyên dụng
                    self.storage.save_parquet(df, date_ref, suffix)

                    # Bước 4: Lưu dữ liệu trạng thái EOD Snapshot thị trường (Chỉ áp dụng với luồng giá Raw)
                    if is_raw:
                        # Truyền thêm tham số check xem có phải chạy nạp lịch sử (Init) hay không
                        # Nếu date_ref quá cũ so với hiện tại, hệ thống tự động nhận biết để lưu snapshot thông minh hơn
                        self.storage.save_checkpoint(df)

                    self.logger.info(f"🏁 Pipeline hoàn thành xuất sắc! Tổng số dòng xử lý: {df.shape[0]:,}")
                    return df
                except Exception as e:
                    self.logger.error(f"❌ Hệ thống ETL gặp sự cố nghiêm trọng bất ngờ: {e}", exc_info=True)
                    return None
