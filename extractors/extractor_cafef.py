import io
import json
import logging
import os
import shutil
import zipfile
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import requests

from logger import setup_logger


class Config:
    """Quản lý tập trung toàn bộ cấu hình và hằng số của hệ thống."""

    URL_CAFEF: str = "https://cafef1.mediacdn.vn/data/ami_data/"
    NETWORK_TIMEOUT: int = 30
    INPUT_BASE_DIR: str = "tmp"
    CHUNK_SIZE: int = 150000
    PRICE_MULTIPLIER: int = 1000
    DEFAULT_LOGGER_NAME: str = "ETL_Pipeline"

    # Định nghĩa cấu trúc cột cố định để tái sử dụng (DRY)
    BASE_COLUMNS: List[str] = ["symbol", "trading_date", "open", "high", "low", "close", "total_volume"]


class CafeFDownloader:
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


class CafeFDataProcessor:
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

    def _normalize_exchange(self, file_name: str) -> str:
        """Nhận diện và chuẩn hóa tên sàn giao dịch dựa trên tên tệp CSV.

        Args:
            file_name: Tên tệp tin CSV nằm trong tệp zip.

        Returns:
            Tên sàn giao dịch đã chuẩn hóa ('HoSE', 'HNX', 'UPCoM', hoặc 'Unknown').
        """
        clean_code: str = str(file_name).strip().upper()
        if "HSX" in clean_code or "HOSE" in clean_code:
            return "HoSE"
        if "UPCOM" in clean_code:
            return "UPCoM"
        if "HNX" in clean_code:
            return "HNX"
        return "Unknown"

    def get_column_names(self, suffix: str, include_exchange: bool = True) -> List[str]:
        """Tạo danh bạ tên cột động dựa trên hậu tố raw/adj.

        Args:
            suffix: Hậu tố phân loại dữ liệu ('raw' hoặc 'adj').
            include_exchange: Có bao gồm cột 'exchange' ở vị trí thứ 2 hay không.

        Returns:
            Danh sách chuỗi tên các cột dữ liệu theo đúng thứ tự.
        """
        cols: List[str] = [
            f"{col}_{suffix}" if col not in ["symbol", "trading_date"] else col for col in Config.BASE_COLUMNS
        ]
        if include_exchange:
            cols.insert(1, "exchange")
        return cols

    def _clean_chunk(self, chunk: pd.DataFrame, suffix: str, exchange_name: str) -> pd.DataFrame:
        """Làm sạch sâu dữ liệu của một phân đoạn (Chunk) bằng toán tử Vectorization.

        Args:
            chunk: Phân đoạn DataFrame thô vừa đọc từ CSV.
            suffix: Hậu tố tên cột ('raw' hoặc 'adj').
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

        # Chuẩn hóa văn bản bằng Vectorization
        chunk["symbol"] = chunk["symbol"].astype(str).str.strip().str.upper()
        chunk["exchange"] = exchange_name

        # Lọc sát ván với Blacklist bằng toán tử hashset siêu tốc O(1)
        if self.blacklist:
            chunk = chunk[~chunk["symbol"].isin(self.blacklist)]
            if chunk.empty:
                return chunk

        # Đồng bộ đơn vị giá của CafeF (Nhân vector không dùng vòng lặp)
        price_cols: List[str] = [f"{p}_{suffix}" for p in ["open", "high", "low", "close"]]
        chunk[price_cols] *= Config.PRICE_MULTIPLIER

        # Lọc song song tất cả các lỗi logic toán học của biên độ giá
        valid_mask: pd.Series = (
            (chunk[price_cols] > 0).all(axis=1)
            & (chunk[f"total_volume_{suffix}"] >= 0)
            & (chunk[f"high_{suffix}"] >= chunk[f"open_{suffix}"])
            & (chunk[f"high_{suffix}"] >= chunk[f"close_{suffix}"])
            & (chunk[f"low_{suffix}"] <= chunk[f"open_{suffix}"])
            & (chunk[f"low_{suffix}"] <= chunk[f"close_{suffix}"])
        )
        return chunk[valid_mask]

    def process_zip_content(self, zip_data: io.BytesIO, suffix: str) -> pd.DataFrame:
        """Đọc phân đoạn, làm sạch và hợp nhất toàn bộ nội dung bên trong file zip.

        Args:
            zip_data: Luồng bytes của file zip nằm trong bộ nhớ RAM.
            suffix: Hậu tố định danh loại cột ('raw' hoặc 'adj').

        Returns:
            DataFrame tổng hợp đã được làm sạch và tối ưu hóa bộ nhớ.
        """
        cols_order: List[str] = self.get_column_names(suffix, include_exchange=False)
        final_cols_order: List[str] = self.get_column_names(suffix, include_exchange=True)

        # Sử dụng float64 tạm thời cho volume để tối ưu hóa tốc độ parse của engine C trong CsvReader
        csv_dtypes: Dict[str, str] = {
            "symbol": "str",
            f"open_{suffix}": "float32",
            f"high_{suffix}": "float32",
            f"low_{suffix}": "float32",
            f"close_{suffix}": "float32",
            f"total_volume_{suffix}": "float64",
        }

        dfs: List[pd.DataFrame] = []

        with zipfile.ZipFile(zip_data) as z:
            csv_files: List[str] = [name for name in z.namelist() if name.endswith(".csv")]
            if not csv_files:
                self.logger.warning("📭 Tệp Zip trống hoặc không chứa file '.csv' nào.")
                return pd.DataFrame(columns=final_cols_order)

            for name in csv_files:
                self.logger.info(f"📂 Đang trích xuất và xử lý stream sàn: [yellow]{name}[/yellow]")
                detected_exchange: str = self._normalize_exchange(name)

                with z.open(name) as f:
                    text_stream = io.TextIOWrapper(f, encoding="utf-8")
                    chunks = pd.read_csv(
                        text_stream,
                        header=0,
                        names=cols_order,
                        dtype=csv_dtypes,
                        parse_dates=["trading_date"],
                        date_format="%Y%m%d",
                        engine="c",
                        on_bad_lines="skip",
                        chunksize=Config.CHUNK_SIZE,
                    )

                    # List Comprehension kết hợp generator lọc bỏ dataframe rỗng cực nhanh
                    clean_chunks = (self._clean_chunk(chunk, suffix, detected_exchange) for chunk in chunks)
                    dfs.extend([df for df in clean_chunks if not df.empty])

        if not dfs:
            return pd.DataFrame(columns=final_cols_order)

        self.logger.info("🧱 Đang gộp các phân đoạn dữ liệu lịch sử...")
        result_df: pd.DataFrame = pd.concat(dfs, ignore_index=True)

        # Ép kiểu dữ liệu tối ưu bộ nhớ (Memory Optimization)
        result_df["exchange"] = result_df["exchange"].astype("category")
        result_df[f"total_volume_{suffix}"] = result_df[f"total_volume_{suffix}"].astype("int64")

        # Drop trùng lặp toàn cục dựa trên cặp khóa chính (Primary Key)
        result_df = result_df.drop_duplicates(subset=["symbol", "trading_date"], keep="last")

        return result_df[final_cols_order]


class CafeFStorage:
    """Chuyên trách việc lưu trữ dữ liệu an toàn ra đĩa cứng (Parquet, JSON)."""

    def __init__(self, logger: logging.Logger) -> None:
        """Khởi tạo bộ lưu trữ dữ liệu.

        Args:
            logger: Đối tượng Logger dùng để ghi nhận tiến trình.
        """
        self.logger: logging.Logger = logger

    def save_parquet(self, df: pd.DataFrame, date_ref: datetime, is_raw: bool = True) -> None:
        """Ghi dữ liệu nén Parquet áp dụng cơ chế Staging an toàn (Atomic Write).

        Args:
            df: DataFrame dữ liệu lịch sử cần ghi dữ liệu.
            date_ref: Mốc thời gian của tệp dữ liệu.

        Raises:
            Exception: Phát sinh khi có lỗi I/O hoặc ghi tệp đĩa cứng thất bại.
        """
        if df is None or df.empty:
            return

        output_dir: str = os.path.join(Config.INPUT_BASE_DIR, "historical")
        staging_dir: str = os.path.join(Config.INPUT_BASE_DIR, "historical_staging_tmp")

        # Reset thư mục tạm
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)
        os.makedirs(staging_dir, exist_ok=True)

        try:
            suffix: str = "raw" if is_raw else "adj"
            file_name: str = f"historical_{suffix}_upto_{date_ref.strftime('%Y%m%d')}.parquet"
            staging_file_path: str = os.path.join(staging_dir, file_name)

            self.logger.info(f"💾 Đang ghi dữ liệu nén Parquet: {file_name}")
            df_renamed = df.rename(columns={
                f"open_{suffix}": "open",
                f"high_{suffix}": "high",
                f"low_{suffix}": "low",
                f"close_{suffix}": "close",
                f"total_volume_{suffix}": "total_volume"
            })
            df_renamed.to_parquet(staging_file_path, compression="snappy", index=False)

            os.makedirs(output_dir, exist_ok=True)
            target_file_path: str = os.path.join(output_dir, file_name)

            if os.path.exists(target_file_path):
                os.remove(target_file_path)

            shutil.move(staging_file_path, output_dir)
            self.logger.info(f"🎉 File lưu trữ thành công tại: {target_file_path}")
        except Exception as e:
            self.logger.error(f"❌ Lỗi trong quá trình ghi file Parquet: {e}")
            raise e
        finally:
            if os.path.exists(staging_dir):
                shutil.rmtree(staging_dir)

    def save_checkpoint(self, df: pd.DataFrame, suffix: str) -> None:
        """Trích xuất và lưu trạng thái thị trường EOD (Snapshot) của toàn bộ các mã chứng khoán.

        Args:
            df: DataFrame dữ liệu tổng hợp.
            suffix: Hậu tố cột dữ liệu ('raw' hoặc 'adj') để tự động khớp tên cột động.
        """
        if df is None or df.empty:
            return

        self.logger.info("⚡ [Snapshot] Đang trích xuất trạng thái thị trường EOD cho TOÀN BỘ cổ phiếu...")

        # Tìm ngày chạy lớn nhất toàn hệ thống bằng phép toán Vector cực nhanh O(N)
        max_date_str: str = pd.to_datetime(df["trading_date"]).max().strftime("%Y-%m-%d")

        # Tối ưu hóa Big O: Sắp xếp theo cặp khóa chính tăng dần để dòng mới nhất luôn ở cuối bản ghi của mã đó
        df_sorted: pd.DataFrame = df.sort_values(by=["symbol", "trading_date"], ascending=[True, True])
        df_latest: pd.DataFrame = df_sorted.drop_duplicates(subset=["symbol"], keep="last").copy()

        # Tính toán giá trung bình bằng toán tử Vector hóa trên tập rút gọn (Chỉ tính trên dòng mới nhất)
        price_cols_origin: List[str] = [f"{p}_{suffix}" for p in ["open", "high", "low", "close"]]
        df_latest["average_price"] = df_latest[price_cols_origin].mean(axis=1).round(2)

        # Đồng bộ hóa định dạng hiển thị chuỗi ngày
        df_latest["exchange"] = df_latest["exchange"].astype(str)
        df_latest["trading_date"] = pd.to_datetime(df_latest["trading_date"]).dt.strftime("%Y-%m-%d")

        # Áp dụng Dictionary Comprehension để đổi tên cột động theo tiêu chuẩn lưu checkpoint (DRY)
        rename_mapping: Dict[str, str] = {f"{col}_{suffix}": f"{col}_price" for col in ["open", "high", "low", "close"]}
        rename_mapping[f"total_volume_{suffix}"] = "total_volume"
        df_latest = df_latest.rename(columns=rename_mapping)

        # Chuyển đổi sang cấu trúc dữ liệu định dạng JSON
        target_cols: List[str] = [
            "exchange",
            "trading_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "average_price",
            "total_volume",
        ]
        market_data_dict: Dict[str, Any] = df_latest.set_index("symbol")[target_cols].to_dict(orient="index")

        final_json_structure: Dict[str, Any] = {
            "last_successful_run": max_date_str,
            "market_data": market_data_dict,
        }

        checkpoint_path: str = os.path.join(Config.INPUT_BASE_DIR, "latest_state.json")
        os.makedirs(Config.INPUT_BASE_DIR, exist_ok=True)

        try:
            with open(checkpoint_path, "w", encoding="utf-8") as f:
                json.dump(final_json_structure, f, indent=4, ensure_ascii=False)
            self.logger.info(
                f"💾 Đã lưu snapshot toàn bộ {len(market_data_dict)} mã thành công tại: {checkpoint_path}"
            )
        except Exception as e:
            self.logger.error(f"❌ Không thể ghi file Checkpoint JSON: {e}")


class CafeFExtractorETL:
    """Bộ điều phối chính (Orchestrator), quản lý vòng đời và luồng chạy của toàn bộ Pipeline."""

    def __init__(self, logger_name: str = Config.DEFAULT_LOGGER_NAME) -> None:
        """Khởi tạo và kết nối các thành phần độc lập của hệ thống ETL.

        Args:
            logger_name: Tên của Logger hệ thống.
        """
        self.logger: logging.Logger = setup_logger(logger_name)

        # Dependency Injection (SOLID: Tách biệt hoàn toàn trách nhiệm)
        self.downloader = CafeFDownloader(self.logger)
        self.processor = CafeFDataProcessor(self.logger)
        self.storage = CafeFStorage(self.logger)

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
            df: pd.DataFrame = self.processor.process_zip_content(zip_stream, suffix)

            if df.empty:
                self.logger.error("🛑 Pipeline kết thúc sớm do tập dữ liệu sau khi làm sạch trống rỗng.")
                return None

            self.logger.info(f"✅ Tải và làm sạch dữ liệu thành công! Kích thước: {df.shape}")

            # Bước 3: Ghi file dữ liệu nén Parquet chuyên dụng
            self.storage.save_parquet(df, date_ref, is_raw)

            # Bước 4: Lưu dữ liệu trạng thái EOD Snapshot thị trường (Chỉ áp dụng với luồng giá Raw)
            if is_raw:
                self.storage.save_checkpoint(df, suffix)

            self.logger.info(f"🏁 Pipeline hoàn thành xuất sắc! Tổng số dòng xử lý: {df.shape[0]:,}")
            return df
        except Exception as e:
            self.logger.error(f"❌ Hệ thống ETL gặp sự cố nghiêm trọng bất ngờ: {e}", exc_info=True)
            return None
