import gc
import io
import logging
import pandas as pd
import requests
import zipfile
from datetime import datetime
from typing import Dict, List, Optional, Set

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

    def download_zip_stream(self, date_ref: datetime, is_raw: bool) -> Optional[io.BytesIO]:
        """Tải tệp zip từ CafeF về bộ nhớ RAM dưới dạng BytesIO stream.

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

        self.logger.info(
            f"📥 [CafeF] Đang tải file lịch sử từ URL: [blue underline]{url}[/blue underline]",
            extra={"markup": True},
        )

        try:
            response: requests.Response = self.session.get(url, timeout=Config.NETWORK_TIMEOUT)
            response.raise_for_status()

            # Kiểm tra nhanh xem bytes trả về có phải cấu trúc ZIP hợp lệ không
            zip_stream: io.BytesIO = io.BytesIO(response.content)
            if not zipfile.is_zipfile(zip_stream):
                self.logger.error(f"❌ [CafeF] File tải về không phải định dạng ZIP hợp lệ.")
                return None

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

        # 1. Chuẩn hóa chuỗi và ép kiểu tối ưu danh mục ngay lập tức
        chunk["symbol"] = chunk["symbol"].astype(str).str.strip().str.upper()

        # Định nghĩa sẵn các danh mục để tránh Pandas tự suy luận dynamic theo từng chunk
        chunk["exchange"] = pd.Categorical(
            [exchange_name] * len(chunk),
            categories=["HoSE", "HNX", "UPCoM", "Unknown"]
        )

        if self.blacklist:
            chunk = chunk[~chunk["symbol"].isin(self.blacklist)]
            if chunk.empty:
                return chunk

        # 2. Đồng bộ đơn vị giá (Vectorization)
        price_cols: List[str] = ["open_price", "high_price", "low_price", "close_price"]
        chunk[price_cols] *= Config.PRICE_MULTIPLIER

        # 3. Ép kiểu dữ liệu số về mức dung lượng thấp hơn (Downcasting)
        # Giá chứng khoán Việt Nam sau nhân 1000 không vượt quá mức Float32, Volume không vượt quá Int32
        chunk[price_cols] = chunk[price_cols].astype("float32")
        chunk[f"total_volume"] = chunk[f"total_volume"].astype("int32")

        # 4. Lọc lỗi logic toán học
        valid_mask: pd.Series = (
            (chunk[price_cols] > 0).all(axis=1)
            & (chunk[f"total_volume"] >= 0)
            & (chunk[f"high_price"] >= chunk[f"open_price"])
            & (chunk[f"high_price"] >= chunk[f"close_price"])
            & (chunk[f"low_price"] <= chunk[f"open_price"])
            & (chunk[f"low_price"] <= chunk[f"close_price"])
        )
        return chunk[valid_mask]

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
        csv_dtypes: Dict[str, str] = {
            "symbol": "str",
            f"open_price": "float32",
            f"high_price": "float32",
            f"low_price": "float32",
            f"close_price": "float32",
            f"total_volume": "float32",
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
                    text_stream = io.TextIOWrapper(f, encoding="utf-8")

                    # Đọc trực tiếp bằng Generator để tránh tích lũy chunk rỗng
                    for chunk in pd.read_csv(
                        text_stream,
                        header=0,
                        names=cols_order,
                        dtype=csv_dtypes,
                        parse_dates=["trading_date"],
                        date_format="%Y%m%d",
                        engine="c",
                        on_bad_lines="skip",
                        chunksize=Config.CHUNK_SIZE,
                    ):
                        # List Comprehension kết hợp generator lọc bỏ dataframe rỗng cực nhanh
                        clean_df = self._clean_chunk(chunk, detected_exchange)
                        if not clean_df.empty:
                            dfs.append(clean_df)

        if not dfs:
            return pd.DataFrame(columns=final_cols_order)

        self.logger.info("🧱 Đang gộp các phân đoạn dữ liệu lịch sử và tối ưu RAM...")

        # Gộp dữ liệu (Lúc này các chunk đều đã rất nhẹ do áp dụng categorical & downcast từ trước)
        result_df: pd.DataFrame = pd.concat(dfs, ignore_index=True)

        # Giải phóng mảng tạm lập tức nhằm tối ưu hóa bộ nhớ cho các bước xử lý sau
        del dfs
        gc.collect()

        # Đảm bảo tính toàn vẹn của kiểu dữ liệu sau khi gộp
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

        # Dependency Injection (SOLID: Tách biệt hoàn toàn trách nhiệm)
        self.downloader = Downloader(self.logger)
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

        # Bước 1: Tải dữ liệu luồng mạng về RAM
        zip_stream: Optional[io.BytesIO] = self.downloader.download_zip_stream(date_ref, is_raw=is_raw)
        if not zip_stream:
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
                self.storage.save_checkpoint(df)

            self.logger.info(f"🏁 Pipeline hoàn thành xuất sắc! Tổng số dòng xử lý: {df.shape[0]:,}")
            return df
        except Exception as e:
            self.logger.error(f"❌ Hệ thống ETL gặp sự cố nghiêm trọng bất ngờ: {e}", exc_info=True)
            return None
