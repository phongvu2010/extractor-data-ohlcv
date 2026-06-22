"""Module thực hiện tải và làm sạch dữ liệu chứng khoán dạng tệp ZIP lịch sử từ nguồn CafeF."""

from __future__ import annotations

import contextlib
from datetime import datetime
import gc
import io
import logging
import random
import time
from typing import Any, Iterator
import zipfile

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from config import Config
from notifier import Notifier
from storages import BaseStorage, get_storage
from utils import normalize_exchange, setup_logger


class Downloader:
    """Chuyên trách việc giao tiếp mạng, tải tệp và trích xuất luồng dữ liệu ZIP từ CafeF."""

    logger: logging.Logger
    session: requests.Session

    def __init__(self, logger: logging.Logger) -> None:
        """Khởi tạo bộ tải dữ liệu.

        Args:
            logger (logging.Logger): Đối tượng Logger dùng để ghi nhận tiến trình.
        """
        self.logger = logger
        self.session = requests.Session()

        # Giả lập Header thông dụng để tránh bị chặn bởi các cơ chế bot filter cơ bản của CDN
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "Anonymized/7cb0ab2238 AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }
        )

        # Cấu hình tự động retry 3 lần nếu gặp sự cố HTTP server hoặc timeout đột xuất
        retries: Retry = Retry(
            total=3,
            backoff_factor=2,  # Khoảng giãn cách giữa các lần thử lại là 2s, 4s, 8s
            status_forcelist=[500, 502, 503, 504],
        )
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def __enter__(self) -> Downloader:
        """Hỗ trợ cơ chế quản lý ngữ cảnh (Context Manager).

        Returns:
            Downloader: Đối tượng Downloader hiện tại.
        """
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Tự động đóng session giải phóng connection pool khi thoát khối lệnh with.

        Args:
            exc_type (Any): Loại ngoại lệ phát sinh nếu có.
            exc_val (Any): Giá trị ngoại lệ phát sinh nếu có.
            exc_tb (Any): Traceback ngoại lệ phát sinh nếu có.
        """
        if self.session:
            self.session.close()
            self.logger.info(
                "🔌 [Downloader] Đã đóng tài nguyên kết nối mạng an toàn sau khi sử dụng."
            )

    def download_zip_stream(
        self, date_ref: datetime, is_raw: bool
    ) -> io.BytesIO | None:
        """Tải tệp zip chứa dữ liệu lịch sử từ CafeF về bộ nhớ đệm RAM.

        Args:
            date_ref (datetime): Ngày cần tải dữ liệu giao dịch.
            is_raw (bool): True để tải giá thô (Raw), False để tải giá điều chỉnh (Adj).

        Returns:
            io.BytesIO | None: Luồng BytesIO của tệp ZIP nếu thành công, ngược lại trả về None.
        """
        raw_prefix: str = "Raw." if is_raw else ""
        date_str_file: str = date_ref.strftime("%d%m%Y")
        date_str_url: str = date_ref.strftime("%Y%m%d")

        file_name: str = f"CafeF.SolieuGD.{raw_prefix}Upto{date_str_file}.zip"
        url: str = f"{Config.URL_CAFEF}{date_str_url}/{file_name}"

        # Áp dụng cơ chế Jitter ngẫu nhiên tránh spam request hàng loạt khi chạy khởi tạo lịch sử
        sleep_time: float = random.uniform(1.5, 3.5)
        self.logger.info(
            f"⏳ [Downloader] Chờ {sleep_time:.2f}s để giãn cách tần suất request..."
        )
        time.sleep(sleep_time)

        self.logger.info(
            f"📥 [CafeF] Đang tải file lịch sử từ URL: [blue underline]{url}[/blue underline]",
            extra={"markup": True},
        )

        try:
            # Dùng stream=True để trì hoãn việc tải body giúp tối ưu hóa kiểm tra trước
            with self.session.get(
                url, timeout=Config.NETWORK_TIMEOUT, stream=True
            ) as response:
                response.raise_for_status()

                # Vòng kiểm tra 1: Xác nhận không phải trang HTML lỗi của Cloudflare/CDN
                content_type: str = response.headers.get("Content-Type", "")
                if "text/html" in content_type:
                    self.logger.error(
                        "❌ [CafeF] URL trả về trang HTML, không phải file ZIP hợp lệ."
                    )
                    return None

                # Vòng kiểm tra 2: Kiểm tra Magic Bytes đầu tiên của định dạng file ZIP (Hex: 50 4B 03 04)
                zip_magic_start: bytes = b"PK\x03\x04"
                stream_iterator: Iterator[bytes] = response.iter_content(
                    chunk_size=65536
                )

                try:
                    first_chunk: bytes = next(stream_iterator)
                except StopIteration:
                    first_chunk = b""

                if not first_chunk.startswith(zip_magic_start):
                    self.logger.error(
                        f"❌ [CafeF] Định dạng file không khớp ZIP. Magic bytes nhận được: {first_chunk[:4]}"
                    )
                    return None

                # Tiến hành nạp dữ liệu cuốn chiếu (chunk-by-chunk) vào RAM để chống cạn kiệt bộ nhớ
                zip_stream: io.BytesIO = io.BytesIO()
                zip_stream.write(first_chunk)

                for chunk in stream_iterator:
                    if chunk:
                        zip_stream.write(chunk)

                zip_stream.seek(0)
                return zip_stream

        except requests.exceptions.Timeout as e:
            self.logger.error(
                f"⏳ [CafeF] Yêu cầu bị Timeout sau {Config.NETWORK_TIMEOUT} giây: {e}"
            )
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                self.logger.warning(
                    f"🚫 [CafeF] Không tìm thấy file lịch sử cho ngày {date_str_file}. Có thể là ngày nghỉ."
                )
            else:
                self.logger.error(f"❌ [CafeF] Lỗi HTTP: {e}")
        except Exception as e:
            self.logger.error(f"❌ [CafeF] Lỗi hệ thống mạng khi tải file: {e}")

        return None


class DataProcessor:
    """Chuyên trách việc làm sạch, chuẩn hóa và ép kiểu dữ liệu chứng khoán lịch sử từ CSV."""

    logger: logging.Logger
    storage: BaseStorage | None
    blacklist: set[str]

    def __init__(
        self, logger: logging.Logger, storage: BaseStorage | None = None
    ) -> None:
        """Khởi tạo bộ xử lý dữ liệu và tải danh sách đen (Blacklist) loại bỏ các mã không hợp lệ.

        Args:
            logger (logging.Logger): Đối tượng Logger dùng để ghi nhận tiến trình.
            storage (BaseStorage | None): Đối tượng Storage dùng để đọc blacklist từ GCS.
        """
        self.logger = logger
        self.storage = storage
        self.blacklist = self._load_blacklist()

    def _load_blacklist(self) -> set[str]:
        """Tải tập hợp danh sách các mã rác, mã ảo cần loại bỏ.

        Returns:
            set[str]: Set chứa các mã chứng khoán viết hoa thuộc danh sách đen.
        """
        if self.storage:
            try:
                return self.storage.read_blacklist()
            except Exception as e:
                self.logger.warning(
                    f"⚠️ [CafeF] Lỗi khi đọc blacklist qua Storage: {e}. Thử đọc trực tiếp file cục bộ..."
                )

        try:
            with open(Config.GCS_BLACKLIST_KEY, "r", encoding="utf-8") as file:
                return {
                    line.strip().upper()
                    for line in file
                    if line.strip() and not line.strip().startswith("#")
                }
        except FileNotFoundError:
            self.logger.warning(
                f"⚠️ [CafeF] Không tìm thấy file '{Config.GCS_BLACKLIST_KEY}' cục bộ. Bỏ qua bộ lọc danh sách đen."
            )
            return set()

    def _get_column_names(
        self, include_exchange: bool = True, include_source: bool = False
    ) -> list[str]:
        """Lấy danh sách tên các cột dữ liệu theo thứ tự chuẩn.

        Args:
            include_exchange (bool): Có bao gồm cột thông tin sàn giao dịch 'exchange' hay không.
            include_source (bool): Có bao gồm cột thông tin nguồn dữ liệu 'source' hay không.

        Returns:
            list[str]: Danh sách tên cột.
        """
        cols: list[str] = [
            "symbol",
            "trading_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "total_volume",
        ]
        if include_exchange:
            cols.append("exchange")
        if include_source:
            cols.append("source")
        return cols

    def _clean_chunk(self, chunk: pd.DataFrame, exchange_name: str) -> pd.DataFrame:
        """Làm sạch và downcast kiểu dữ liệu cho một phân đoạn (Chunk) bằng vector hóa.

        Args:
            chunk (pd.DataFrame): Phân đoạn DataFrame thô đọc từ CSV.
            exchange_name (str): Tên sàn giao dịch của phân đoạn.

        Returns:
            pd.DataFrame: DataFrame đã xử lý lỗi logic, ép kiểu dữ liệu và lọc danh sách đen.
        """
        chunk = chunk.dropna(subset=["symbol", "trading_date"])
        if chunk.empty:
            return chunk

        chunk = chunk.copy()
        chunk["symbol"] = chunk["symbol"].astype(str).str.strip().str.upper()

        if self.blacklist:
            chunk = chunk[~chunk["symbol"].isin(self.blacklist)]
            if chunk.empty:
                return chunk

        chunk["exchange"] = pd.Series(exchange_name, index=chunk.index).astype(
            pd.CategoricalDtype(categories=["HoSE", "HNX", "UPCoM", "Unknown"])
        )
        chunk["source"] = "cafef"

        # Loại bỏ các giá trị vô cực (inf) hoặc NaN trước khi nhân để tránh lỗi tràn số (overflow)
        price_cols: list[str] = ["open_price", "high_price", "low_price", "close_price"]
        chunk[price_cols] = chunk[price_cols].replace([np.inf, -np.inf], np.nan)
        chunk = chunk.dropna(subset=price_cols)

        # Nhân giá thô với hệ số quy đổi (thường là 1000) để khớp đơn vị
        chunk[price_cols] *= Config.PRICE_MULTIPLIER

        # Ép kiểu dữ liệu dung lượng nhỏ để tối ưu hóa bộ nhớ RAM
        chunk[price_cols] = chunk[price_cols].round(2).astype("float32")
        chunk["total_volume"] = chunk["total_volume"].astype("Int64")

        # Áp dụng chốt chặn logic toán học để loại bỏ các dòng bị lỗi cấu trúc dữ liệu
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
        """Trích xuất, xử lý làm sạch và hợp nhất các file CSV bên trong tệp ZIP.

        Args:
            zip_data (io.BytesIO): Luồng dữ liệu tệp ZIP nằm trong bộ nhớ RAM.

        Returns:
            pd.DataFrame: DataFrame hoàn chỉnh đã chuẩn hóa toàn bộ dữ liệu.
        """
        cols_order: list[str] = self._get_column_names(
            include_exchange=False, include_source=False
        )
        final_cols_order: list[str] = self._get_column_names(
            include_exchange=True, include_source=True
        )

        csv_dtypes: dict[str, str] = {
            "symbol": "str",
            "open_price": "float32",
            "high_price": "float32",
            "low_price": "float32",
            "close_price": "float32",
            "total_volume": "float32",
        }

        dfs: list[pd.DataFrame] = []
        with zipfile.ZipFile(zip_data) as z:
            csv_files: list[str] = [
                name for name in z.namelist() if name.endswith(".csv")
            ]
            if not csv_files:
                self.logger.warning("📭 Tệp Zip trống hoặc không chứa file '.csv' nào.")
                return pd.DataFrame(columns=final_cols_order)

            for name in csv_files:
                self.logger.info(
                    f"📂 Đang trích xuất & xử lý stream sàn: [yellow]{name}[/yellow]"
                )
                detected_exchange: str = normalize_exchange(name)

                with z.open(name) as f:
                    with io.TextIOWrapper(f, encoding="utf-8") as text_stream:
                        try:
                            # Phân tích định dạng ngày ngay ở C-Engine để tối đa hóa tốc độ tải dữ liệu
                            chunks: pd.io.parsers.TextFileReader = pd.read_csv(
                                text_stream,
                                header=0,
                                names=cols_order,
                                dtype=csv_dtypes,
                                engine="c",
                                on_bad_lines="skip",
                                chunksize=Config.CHUNK_SIZE,
                                parse_dates=[1],
                                date_format="%Y%m%d",
                            )
                            for chunk in chunks:
                                clean_df: pd.DataFrame = self._clean_chunk(
                                    chunk, detected_exchange
                                )
                                if not clean_df.empty:
                                    dfs.append(clean_df)
                                del chunk
                        except pd.errors.EmptyDataError:
                            self.logger.warning(
                                f"⚠️ [CafeF] Tệp CSV '{name}' trống hoặc chỉ chứa tiêu đề. Bỏ qua."
                            )

        if not dfs:
            return pd.DataFrame(columns=final_cols_order)

        self.logger.info("🧱 Đang gộp các phân đoạn dữ liệu lịch sử và tối ưu RAM...")
        result_df: pd.DataFrame = pd.concat(dfs, ignore_index=True)

        del dfs
        gc.collect()

        result_df["exchange"] = result_df["exchange"].astype(
            pd.CategoricalDtype(categories=["HoSE", "HNX", "UPCoM", "Unknown"])
        )
        result_df["symbol"] = result_df["symbol"].astype("category")

        # Sắp xếp và chỉ giữ lại bản ghi giao dịch mới nhất của ngày nếu có trùng lặp
        result_df.sort_values(
            by=["symbol", "trading_date"], inplace=True, ignore_index=True
        )
        result_df.drop_duplicates(
            subset=["symbol", "trading_date"], keep="last", inplace=True
        )

        return result_df[final_cols_order]


class CafeFExtractorETL:
    """Bộ điều phối chính (Orchestrator) quản lý toàn bộ vòng đời chạy của CafeF Pipeline."""

    logger: logging.Logger
    storage: BaseStorage
    processor: DataProcessor

    def __init__(
        self,
        logger_name: str = Config.DEFAULT_LOGGER_NAME,
        storage: BaseStorage | None = None,
    ) -> None:
        """Khởi tạo đối tượng ETL và cấu hình kết nối các phân lớp.

        Args:
            logger_name (str): Tên Logger hệ thống dùng chung.
            storage (BaseStorage | None): Đối tượng Storage chia sẻ kết nối GCP.
        """
        self.logger = setup_logger(logger_name)
        self.storage = storage or get_storage(Config.DEPLOYMENT_ENV, self.logger)
        self.processor = DataProcessor(self.logger, storage=self.storage)

    def run(
        self,
        date_ref: datetime,
        is_raw: bool = True,
        partition: bool = False,
        save_checkpoint: bool = True,
    ) -> pd.DataFrame | None:
        """Thực thi quy trình ETL tải, xử lý làm sạch và nạp dữ liệu CafeF.

        Args:
            date_ref (datetime): Mốc thời gian cần chạy dữ liệu.
            is_raw (bool): True nếu tải giá thô, False nếu tải giá điều chỉnh.
            partition (bool): True để lưu phân mảnh thư mục ngày, False để lưu file tổng hợp tĩnh.
            save_checkpoint (bool): Có cập nhật trạng thái EOD checkpoint lên GCS hay không.

        Returns:
            pd.DataFrame | None: DataFrame kết quả nếu chạy hoàn tất thành công, ngược lại trả về None.
        """
        suffix: str = "raw" if is_raw else "adj"
        self.logger.info(
            f"🚀 Khởi chạy Pipeline CafeF [{suffix.upper()}] ngày: {date_ref.strftime('%Y-%m-%d')}"
        )

        with Downloader(self.logger) as downloader:
            with contextlib.closing(
                downloader.download_zip_stream(date_ref, is_raw=is_raw)
            ) as zip_stream:
                if not zip_stream or zip_stream.getvalue() == b"":
                    self.logger.error(
                        "🛑 Pipeline kết thúc sớm do không tải được tệp tin zip từ máy chủ."
                    )
                    if save_checkpoint:
                        try:
                            date_str: str = date_ref.strftime("%Y-%m-%d")
                            Notifier(self.logger).send_alert(
                                f"Lỗi chạy CafeF [{suffix.upper()}]",
                                f"Không tải được tệp tin ZIP từ máy chủ CafeF cho ngày {date_str}.",
                            )
                        except Exception as notify_err:
                            self.logger.error(
                                f"❌ Không thể gửi thông báo lỗi CafeF: {notify_err}"
                            )
                    return None

                try:
                    df: pd.DataFrame = self.processor.process_zip_content(zip_stream)

                    if partition:
                        # Chỉ lấy dữ liệu đúng ngày cần chạy để nạp phân mảnh
                        df = df[
                            pd.to_datetime(df["trading_date"]).dt.date
                            == date_ref.date()
                        ].copy()

                    if df.empty:
                        self.logger.error(
                            "🛑 Pipeline kết thúc sớm do tập dữ liệu sau khi làm sạch trống rỗng."
                        )
                        if save_checkpoint:
                            try:
                                date_str: str = date_ref.strftime("%Y-%m-%d")
                                Notifier(self.logger).send_alert(
                                    f"Lỗi chạy CafeF [{suffix.upper()}]",
                                    f"Tập dữ liệu sau khi làm sạch trống rỗng cho ngày {date_str}.",
                                )
                            except Exception as notify_err:
                                self.logger.error(
                                    f"❌ Không thể gửi thông báo lỗi CafeF: {notify_err}"
                                )
                        return None

                    self.logger.info(
                        f"✅ Tải và làm sạch dữ liệu thành công! Kích thước: {df.shape}"
                    )

                    gcs_path: str | None = self.storage.save_parquet(
                        df, date_ref, suffix, partition=partition
                    )

                    # Nạp trực tiếp dữ liệu từ file GCS lên BigQuery
                    if gcs_path:
                        target_table: str = (
                            Config.BQ_RAW_TABLE if is_raw else Config.BQ_ADJ_TABLE
                        )
                        if partition:
                            self.storage.sync_partition_to_bigquery(
                                gcs_path, target_table, date_ref.date()
                            )
                        else:
                            self.logger.warning(
                                f"⚠️ Đang nạp toàn bộ lịch sử ở chế độ WRITE_TRUNCATE cho bảng {target_table}..."
                            )
                            self.storage.load_parquet_to_bigquery(
                                gcs_path,
                                target_table,
                                write_disposition="WRITE_TRUNCATE",
                            )

                    if is_raw and save_checkpoint:
                        self.storage.save_checkpoint(df)

                    self.logger.info(
                        f"🏁 Pipeline hoàn thành xuất sắc! Tổng số dòng xử lý: {df.shape[0]:,}"
                    )
                    if save_checkpoint:
                        try:
                            Notifier(self.logger).send_message(
                                f"✅ <b>BÁO CÁO PIPELINE CAFEF</b>\n"
                                f"📅 <b>Ngày chạy:</b> {date_ref.strftime('%Y-%m-%d')}\n"
                                f"📊 <b>Loại dữ liệu:</b> {'Giá thô (Raw)' if is_raw else 'Giá điều chỉnh (Adj)'}\n"
                                f"📈 <b>Dòng xử lý:</b> {df.shape[0]:,} dòng\n"
                                f"✨ <b>Trạng thái:</b> Hoàn thành xuất sắc.\n"
                                f"⏰ <i>Thời gian: {datetime.now(Config.VN_TZ).strftime('%Y-%m-%d %H:%M:%S')}</i>"
                            )
                        except Exception as notify_err:
                            self.logger.error(
                                f"❌ Không thể gửi báo cáo chạy CafeF: {notify_err}"
                            )
                    return df
                except Exception as e:
                    self.logger.error(
                        f"❌ Hệ thống ETL gặp sự cố nghiêm trọng bất ngờ: {e}",
                        exc_info=True,
                    )
                    if save_checkpoint:
                        try:
                            Notifier(self.logger).send_alert(
                                f"Sập Hệ thống CafeF [{suffix.upper()}]",
                                f"{type(e).__name__}: {str(e)}",
                            )
                        except Exception as notify_err:
                            self.logger.error(
                                f"❌ Không thể gửi thông báo lỗi CafeF: {notify_err}"
                            )
                    return None
