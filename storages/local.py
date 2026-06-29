"""Module cung cấp cơ chế lưu trữ dữ liệu chứng khoán cục bộ vào cơ sở dữ liệu PostgreSQL (TimescaleDB)."""

from __future__ import annotations

from datetime import date, datetime
import gc
import logging
import os
import time
from typing import Any

import polars as pl
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    ForeignKey,
    Index,
    Integer,
    JSON,
    MetaData,
    Numeric,
    String,
    Table,
    UniqueConstraint,
    bindparam,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from config import Config
from .base import BaseStorage


class Base(DeclarativeBase):
    """Base class cho tất cả ORM models (SQLAlchemy 2.0 style)."""


class RawPriceModel(Base):
    """Bảng lưu trữ giá thô (raw price) của các cổ phiếu (được chuyển thành Hypertable)."""

    __tablename__ = "raw_price"
    symbol = Column(String(10), primary_key=True)
    trading_date = Column(Date, primary_key=True)
    open_price = Column(Integer, nullable=False)
    high_price = Column(Integer, nullable=False)
    low_price = Column(Integer, nullable=False)
    close_price = Column(Integer, nullable=False)
    total_volume = Column(BigInteger, nullable=False)
    exchange = Column(String(10), nullable=False)
    source = Column(String(50), nullable=False)


# Thiết lập chỉ mục hỗn hợp (Composite Index) hỗ trợ truy vấn lịch sử mã chứng khoán theo thời gian
Index(
    "idx_raw_price_symbol_date", RawPriceModel.symbol, RawPriceModel.trading_date.desc()
)


class AdjustedPriceModel(Base):
    """Bảng lưu trữ giá điều chỉnh (adj price) của các cổ phiếu (được chuyển thành Hypertable)."""

    __tablename__ = "adj_price"
    symbol = Column(String(10), primary_key=True)
    trading_date = Column(Date, primary_key=True)
    open_price = Column(Numeric(18, 2), nullable=False)
    high_price = Column(Numeric(18, 2), nullable=False)
    low_price = Column(Numeric(18, 2), nullable=False)
    close_price = Column(Numeric(18, 2), nullable=False)
    total_volume = Column(BigInteger, nullable=False)
    exchange = Column(String(10), nullable=False)
    source = Column(String(50), nullable=False)


# Thiết lập chỉ mục hỗn hợp (Composite Index) hỗ trợ truy vấn lịch sử mã chứng khoán theo thời gian
Index(
    "idx_adj_price_symbol_date",
    AdjustedPriceModel.symbol,
    AdjustedPriceModel.trading_date.desc(),
)


class PipelineStateModel(Base):
    """Bảng lưu trữ trạng thái chạy và checkpoint EOD (thay thế JSON)."""

    __tablename__ = "pipeline_state"
    key = Column(String(50), primary_key=True)
    value = Column(JSON, nullable=False)


# Tuần tự các cột chuẩn được lưu vào bảng OHLCV trong PostgreSQL.
# Khai báo một lần, dùng chung cho mọi method đọc-ghi Parquet → DB.
_OHLCV_STORAGE_COLS: tuple[str, ...] = (
    "symbol",
    "trading_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "total_volume",
    "exchange",
    "source",
)

# Danh sách trắng các bảng hợp lệ được phép thao tác trong SQL động.
# Ngăn chặn SQL Injection qua f-string khi tên bảng được truyền vào làm tham số.
_ALLOWED_TABLE_NAMES: frozenset[str] = frozenset(
    {
        "raw_price",
        "adj_price",
        "pipeline_state",
    }
)


class LocalStorage(BaseStorage):
    """Bộ lưu trữ dữ liệu cục bộ dùng PostgreSQL (TimescaleDB) và lưu file Parquet cục bộ."""

    engine: Engine
    Session: sessionmaker

    def __init__(self, logger: logging.Logger) -> None:
        """Khởi tạo kết nối cục bộ PostgreSQL, kiểm tra TimescaleDB và di cư (migrate) các bảng.

        Args:
            logger (logging.Logger): Đối tượng Logger dùng để ghi nhận tiến trình.

        Raises:
            ValueError: Nếu DATABASE_URL không được cấu hình.
        """
        super().__init__(logger)
        if not Config.DATABASE_URL:
            self.logger.error(
                "🛑 [LocalStorage] DATABASE_URL không được cấu hình trong file .env!"
            )
            raise ValueError(
                "DATABASE_URL must be configured when DEPLOYMENT_ENV is local"
            )

        # Cơ chế tự động thử lại kết nối phục vụ khởi động Docker Compose đồng bộ
        max_retries: int = 5
        retry_delay: int = 3

        self.logger.info("🔌 [LocalStorage] Đang kết nối đến PostgreSQL/TimescaleDB...")
        for attempt in range(1, max_retries + 1):
            try:
                self.engine = create_engine(
                    Config.DATABASE_URL,
                    pool_size=10,
                    max_overflow=20,
                    pool_pre_ping=True,  # Tự động kiểm tra và phục hồi kết nối chết trước khi dùng
                    pool_recycle=1800,  # Tái chế kết nối sau 30 phút tránh lỗi timeout từ Postgres
                )
                # Chạy thử truy vấn để xác thực kết nối
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                self.logger.info("🐘 [LocalStorage] Kết nối thành công đến PostgreSQL.")
                break
            except Exception as e:
                if attempt == max_retries:
                    self.logger.error(
                        f"🛑 [LocalStorage] Không thể kết nối đến database sau {max_retries} lần thử: {e}"
                    )
                    raise e
                self.logger.warning(
                    f"⚠️ [LocalStorage] Kết nối thất bại lần {attempt}/{max_retries}. "
                    f"Thử lại sau {retry_delay} giây..."
                )
                time.sleep(retry_delay)

        self.Session = sessionmaker(bind=self.engine)
        self._cached_metadata = MetaData()
        self._cached_tables: dict[str, Table] = {}
        self.init_db()

    def init_db(self) -> None:
        """Khởi tạo cấu trúc các bảng và chuyển đổi raw_price/adj_price thành hypertable nếu hỗ trợ.

        Raises:
            Exception: Phát sinh khi khởi tạo cơ sở dữ liệu gặp lỗi.
        """
        try:
            self.logger.info(
                "🛠️ [LocalStorage] Đang thiết lập cấu trúc database (migrations)..."
            )

            # Tạo các bảng cơ bản nếu chưa tồn tại
            Base.metadata.create_all(self.engine)

            with self.engine.begin() as conn:
                # Kích hoạt extension timescaledb nếu có
                try:
                    conn.execute(
                        text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
                    )
                except Exception as ext_err:
                    self.logger.warning(
                        f"⚠️ [LocalStorage] Không thể chạy CREATE EXTENSION timescaledb: {ext_err}"
                    )

                # Kiểm tra xem extension timescaledb có thực sự được cài đặt và kích hoạt không
                res_ext = conn.execute(
                    text("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb';")
                ).fetchone()

                has_timescaledb: bool = res_ext is not None

                if has_timescaledb:
                    # Kiểm tra và chuyển đổi raw_price thành hypertable
                    res_raw = conn.execute(
                        text(
                            "SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'raw_price';"
                        )
                    ).fetchone()
                    if not res_raw:
                        self.logger.info(
                            "✨ [TimescaleDB] Chuyển đổi 'raw_price' sang Hypertable..."
                        )
                        conn.execute(
                            text(
                                "SELECT create_hypertable('raw_price', 'trading_date');"
                            )
                        )

                    # Thiết lập nén cho raw_price nếu chưa cấu hình
                    res_raw_comp = conn.execute(
                        text(
                            "SELECT 1 FROM timescaledb_information.compression_settings WHERE hypertable_name = 'raw_price';"
                        )
                    ).fetchone()
                    if not res_raw_comp:
                        self.logger.info(
                            "✨ [TimescaleDB] Thiết lập chính sách nén (30 ngày) cho 'raw_price'..."
                        )
                        conn.execute(text("""
                                ALTER TABLE raw_price SET (
                                    timescaledb.compress,
                                    timescaledb.compress_segmentby = 'symbol',
                                    timescaledb.compress_orderby = 'trading_date DESC'
                                );
                                """))
                        conn.execute(
                            text(
                                "SELECT add_compression_policy('raw_price', INTERVAL '30 days', if_not_exists => TRUE);"
                            )
                        )

                    # Kiểm tra và chuyển đổi adj_price thành hypertable
                    res_adj = conn.execute(
                        text(
                            "SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'adj_price';"
                        )
                    ).fetchone()
                    if not res_adj:
                        self.logger.info(
                            "✨ [TimescaleDB] Chuyển đổi 'adj_price' sang Hypertable..."
                        )
                        conn.execute(
                            text(
                                "SELECT create_hypertable('adj_price', 'trading_date');"
                            )
                        )

                    # Thiết lập nén cho adj_price nếu chưa cấu hình
                    res_adj_comp = conn.execute(
                        text(
                            "SELECT 1 FROM timescaledb_information.compression_settings WHERE hypertable_name = 'adj_price';"
                        )
                    ).fetchone()
                    if not res_adj_comp:
                        self.logger.info(
                            "✨ [TimescaleDB] Thiết lập chính sách nén (30 ngày) cho 'adj_price'..."
                        )
                        conn.execute(text("""
                                ALTER TABLE adj_price SET (
                                    timescaledb.compress,
                                    timescaledb.compress_segmentby = 'symbol',
                                    timescaledb.compress_orderby = 'trading_date DESC'
                                );
                                """))
                        conn.execute(
                            text(
                                "SELECT add_compression_policy('adj_price', INTERVAL '30 days', if_not_exists => TRUE);"
                            )
                        )
                else:
                    self.logger.warning(
                        "⚠️ [LocalStorage] Extension 'timescaledb' không khả dụng hoặc chưa được kích hoạt. "
                        "Hệ thống sẽ hoạt động trên các bảng PostgreSQL thông thường."
                    )

            self.logger.info("🎉 [LocalStorage] Cấu trúc database đã sẵn sàng.")
        except Exception as e:
            self.logger.error(f"🛑 [LocalStorage] Lỗi khởi tạo database: {e}")
            raise e

    def _validate_table_name(self, table_name: str) -> str:
        """Kiểm tra tên bảng nằm trong danh sách trắng để ngăn chặn SQL Injection.

        Args:
            table_name (str): Tên bảng cần kiểm tra.

        Returns:
            str: Tên bảng đã xác thực (trả về nguyên vẹn nếu hợp lệ).

        Raises:
            ValueError: Nếu tên bảng không nằm trong danh sách cho phép.
        """
        if table_name not in _ALLOWED_TABLE_NAMES:
            raise ValueError(
                f"Tên bảng '{table_name}' không nằm trong danh sách cho phép: "
                f"{sorted(_ALLOWED_TABLE_NAMES)}. Từ chối thực thi SQL."
            )
        return table_name

    def _select_ohlcv_cols(self, df: pl.DataFrame) -> pl.DataFrame:
        """Chọn chỉ các cột OHLCV hợp lệ từ DataFrame, bỏ qua cột ngoài schema.

        Sử dụng hằng số ``_OHLCV_STORAGE_COLS`` làm nguồn sự thật duy nhất cho thứ tự cột.
        Các cột như ``reference_price``, ``average_price`` sẽ bị loại bỏ tự động.

        Args:
            df (pl.DataFrame): DataFrame gốc cần lọc cột.

        Returns:
            pl.DataFrame: DataFrame chỉ gồm các cột nằm trong ``_OHLCV_STORAGE_COLS``
                và có mặt trong DataFrame.
        """
        cols = [c for c in _OHLCV_STORAGE_COLS if c in df.columns]
        return df.select(cols)

    def _get_table_schema(self, table_name: str) -> Table:
        """Lấy schema cấu trúc bảng từ cache hoặc autoload từ cơ sở dữ liệu.

        Args:
            table_name (str): Tên bảng cần lấy schema.

        Returns:
            Table: Đối tượng Table tương ứng của SQLAlchemy.
        """
        self._validate_table_name(table_name)
        if table_name not in self._cached_tables:
            self._cached_tables[table_name] = Table(
                table_name, self._cached_metadata, autoload_with=self.engine
            )
        return self._cached_tables[table_name]

    def _upsert_polars_to_pg(
        self,
        df: pl.DataFrame,
        table_name: str,
        index_elements: list[str],
        chunk_size: int | None = None,
    ) -> None:
        """
        Thực hiện bulk upsert trực tiếp từ Polars sang PostgreSQL, không qua Pandas.
        Giữ nguyên tính chất Idempotent thông qua ON CONFLICT DO UPDATE.
        """
        if df.is_empty():
            return

        if chunk_size is None:
            chunk_size = Config.DB_UPSERT_CHUNK_SIZE

        # Lấy cấu trúc bảng từ cache để tối ưu hóa hiệu năng
        table = self._get_table_schema(table_name)

        # Mở một Transaction duy nhất cho tất cả các chunk để tối ưu hiệu năng
        with self.engine.begin() as conn:
            # Chia nhỏ DataFrame để nạp vào DB, tối ưu RAM
            for i in range(0, df.height, chunk_size):
                chunk = df.slice(i, chunk_size)

                # to_dicts() của Polars rất nhanh và tự động map kiểu Date của Polars sang datetime.date của Python
                data_dicts = chunk.to_dicts()

                insert_stmt = insert(table).values(data_dicts)

                # Lọc ra các cột cần cập nhật (loại trừ khóa chính và cột id tự tăng nếu có)
                update_cols = {
                    c.name: c
                    for c in insert_stmt.excluded
                    if c.name not in index_elements and c.name != "id"
                }

                if not update_cols:
                    upsert_stmt = insert_stmt.on_conflict_do_nothing(
                        index_elements=index_elements
                    )
                else:
                    upsert_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=index_elements, set_=update_cols
                    )

                conn.execute(upsert_stmt)

    def save_parquet(
        self,
        df: pl.DataFrame,
        date_ref: datetime,
        suffix: str = "raw",
        partition: bool = False,
    ) -> str | None:
        """Lưu trữ dữ liệu nén Parquet cục bộ và trả về đường dẫn.

        Args:
            df (pl.DataFrame): DataFrame chứa dữ liệu cần lưu trữ.
            date_ref (datetime): Đối tượng mốc thời gian để làm tiền đề
                đặt tên thư mục/file.
            suffix (str, optional): Hậu tố phân loại dữ liệu (ví dụ: "raw", "adj").
                Mặc định là "raw".
            partition (bool, optional): Có phân vùng theo cấu trúc thư mục
                năm/tháng hay không. Mặc định là False.

        Returns:
            str | None: Đường dẫn tuyệt đối/tương đối của file Parquet đã ghi,
                hoặc None nếu DataFrame rỗng.

        Raises:
            Exception: Lỗi phát sinh trong quá trình ghi file lên đĩa.
        """
        if df is None or df.is_empty():
            return None

        # Tối ưu: Lấy ngày thực tế lớn nhất từ cột trading_date
        # để đặt tên thư mục/file nếu có
        if "trading_date" in df.columns:
            max_date: Any = df["trading_date"].max()
            if max_date is not None:
                if isinstance(max_date, date):
                    date_ref = datetime.combine(max_date, datetime.min.time())
                else:
                    date_ref = max_date

        gcs_path: str
        if partition:
            year_str: str = date_ref.strftime("%Y")
            month_str: str = date_ref.strftime("%m")
            date_str: str = date_ref.strftime("%Y%m%d")
            gcs_path = (
                f"{Config.GCS_PARQUET_PREFIX}/{suffix}/"
                f"year={year_str}/month={month_str}/"
                f"daily_{date_str}.parquet"
            )
        else:
            gcs_path = (
                f"{Config.GCS_PARQUET_PREFIX}/{suffix}/cafef_historical_all.parquet"
            )

        local_path: str = os.path.join("data", gcs_path)
        try:
            self.logger.info(
                f"💾 [Local Disk] Đang ghi dữ liệu nén Parquet cục bộ: {local_path}"
            )
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            df.write_parquet(local_path, compression="snappy")
            self.logger.info(
                f"🎉 [Local Disk] File lưu trữ thành công tại: {local_path}"
            )
            return local_path
        except Exception as e:
            self.logger.error(
                f"❌ [Local Disk] Lỗi trong quá trình ghi file Parquet cục bộ: {e}"
            )
            raise e

    def save_symbol_history(
        self, df: pl.DataFrame, symbol: str, suffix: str = "adj"
    ) -> None:
        """Lưu trữ toàn bộ lịch sử giá của một mã cổ phiếu cụ thể ra file Parquet cục bộ.

        Args:
            df (pl.DataFrame): DataFrame chứa dữ liệu lịch sử giá của mã cổ phiếu.
            symbol (str): Mã cổ phiếu (ví dụ: "FPT", "VIC").
            suffix (str, optional): Hậu tố phân loại thư mục lưu trữ. Mặc định là "adj".

        Raises:
            Exception: Lỗi phát sinh trong quá trình tạo thư mục hoặc ghi file Parquet.
        """
        if df is None or df.is_empty():
            return

        gcs_path: str = (
            f"{Config.GCS_PARQUET_PREFIX}/{suffix}/reloaded/{symbol.upper()}.parquet"
        )
        local_path: str = os.path.join("data", gcs_path)

        try:
            self.logger.info(
                f"💾 [Local Disk] Đang ghi lịch sử cho mã {symbol.upper()} tại: {local_path}"
            )
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            df.write_parquet(local_path, compression="snappy")
            self.logger.info(
                f"🎉 [Local Disk] File lịch sử mã {symbol.upper()} lưu trữ thành công."
            )
        except Exception as e:
            self.logger.error(
                f"❌ [Local Disk] Lỗi khi ghi file lịch sử cho mã {symbol.upper()} cục bộ: {e}"
            )
            raise e

    def sync_partition_to_bigquery(
        self, path: str, table_name: str, date_ref: date
    ) -> None:
        """Đồng bộ dữ liệu phân vùng từ file Parquet cục bộ vào bảng tương ứng trong Postgres.

        Args:
            path (str): Đường dẫn đến file Parquet cục bộ chứa dữ liệu.
            table_name (str): Tên bảng đích trong cơ sở dữ liệu PostgreSQL.
            date_ref (date): Ngày tham chiếu của phân vùng cần đồng bộ.

        Raises:
            Exception: Lỗi phát sinh trong quá trình đọc file hoặc thực thi lệnh SQL.
        """
        self.logger.info(
            f"⚡ [Postgres] Đang đồng bộ phân vùng từ file {path} "
            f"vào bảng {table_name}..."
        )
        try:
            df = pl.read_parquet(path)
            if df.is_empty():
                return

            # Ép kiểu chuẩn ngày tháng trên Polars (không cần dùng apply của Pandas nữa)
            if "trading_date" in df.columns:
                df = df.with_columns(pl.col("trading_date").cast(pl.Date))

            # Loại bỏ các cột không thuộc schema lưu trữ Postgres
            df_write = self._select_ohlcv_cols(df)

            # Gọi hàm _upsert_polars_to_pg
            self._upsert_polars_to_pg(
                df=df_write,
                table_name=table_name,
                index_elements=["symbol", "trading_date"],
            )
            self.logger.info(
                f"🎉 [Postgres] Đồng bộ thành công vào bảng {table_name} "
                f"cho ngày {date_ref}."
            )
        except Exception as e:
            self.logger.error(
                f"❌ [Postgres] Lỗi đồng bộ phân vùng vào bảng {table_name}: {e}"
            )
            raise e

    def sync_adjusted_symbols_to_bigquery(self, symbols: list[str]) -> None:
        """Đọc và đồng bộ lịch sử điều chỉnh của danh sách mã từ Parquet cục bộ vào PostgreSQL.

        Args:
            symbols (list[str]): Danh sách các mã cổ phiếu cần đồng bộ.

        Raises:
            Exception: Lỗi phát sinh khi đọc ghi file Parquet hoặc lưu vào DB.
        """
        if not symbols:
            return

        self.logger.info(
            f"⚡ [Postgres] Bắt đầu đồng bộ gộp lịch sử điều chỉnh cho "
            f"{len(symbols)} mã..."
        )
        try:
            for symbol in symbols:
                gcs_path: str = (
                    f"{Config.GCS_PARQUET_PREFIX}/adj/reloaded/{symbol.upper()}.parquet"
                )
                local_path: str = os.path.join("data", gcs_path)
                if not os.path.exists(local_path):
                    self.logger.warning(
                        f"⚠️ [Postgres] Không thấy file lịch sử {local_path} để sync."
                    )
                    continue

                df = pl.read_parquet(local_path)
                if df.is_empty():
                    continue

                if "trading_date" in df.columns:
                    df = df.with_columns(pl.col("trading_date").cast(pl.Date))

                df_write = self._select_ohlcv_cols(df)

                # Thực thi Bulk Upsert
                self._upsert_polars_to_pg(
                    df=df_write,
                    table_name=Config.BQ_ADJ_TABLE,
                    index_elements=["symbol", "trading_date"],
                )
            self.logger.info(
                f"🎉 [Postgres] Đồng bộ lịch sử điều chỉnh hoàn tất cho "
                f"{len(symbols)} mã."
            )
        except Exception as e:
            self.logger.error(f"❌ [Postgres] Lỗi đồng bộ gộp lịch sử điều chỉnh: {e}")
            raise e

    def sync_daily_adjusted_prices(
        self, dates: list[datetime | date], excluded_symbols: list[str]
    ) -> None:
        """Sao chép giá thô của các ngày sang bảng giá điều chỉnh đối với các mã bình thường.

        Args:
            dates (list[datetime | date]): Danh sách các ngày cần thực hiện đồng bộ.
            excluded_symbols (list[str]): Danh sách các mã cổ phiếu loại trừ không sao chép
                (thường là các mã cần tính toán điều chỉnh riêng).

        Raises:
            Exception: Lỗi phát sinh trong quá trình xóa dữ liệu cũ hoặc chèn dữ liệu mới.
        """
        if not dates:
            return

        date_strings: list[str] = [
            d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in dates
        ]
        self.logger.info(
            f"⚡ [Postgres] Bắt đầu sao chép giá từ raw sang adjusted cho các "
            f"ngày: {date_strings}..."
        )

        chunk_size: int = 100
        for i in range(0, len(date_strings), chunk_size):
            chunk_date_strings = date_strings[i : i + chunk_size]
            chunk_dates = [
                datetime.strptime(ds, "%Y-%m-%d").date() for ds in chunk_date_strings
            ]
            self.logger.info(
                f"⚡ [Postgres] Đang sao chép lô {i // chunk_size + 1} từ raw sang adjusted cho các "
                f"ngày: {chunk_date_strings}..."
            )
            try:
                with self.engine.begin() as conn:
                    # 1. Xóa dữ liệu cũ ngày hôm đó
                    delete_query: str = (
                        "DELETE FROM adj_price WHERE trading_date IN (:dates)"
                    )
                    params: dict[str, Any] = {"dates": chunk_dates}

                    if excluded_symbols:
                        delete_query = (
                            "DELETE FROM adj_price WHERE trading_date IN (:dates) "
                            "AND symbol NOT IN (:excluded_symbols)"
                        )
                        params["excluded_symbols"] = [
                            s.upper() for s in excluded_symbols
                        ]

                    # 2. Sao chép từ raw_price sang adj_price
                    insert_query: str = """
                        INSERT INTO adj_price (
                            symbol, trading_date, open_price, high_price,
                            low_price, close_price, total_volume, exchange, source
                        )
                        SELECT
                            symbol, trading_date, open_price, high_price,
                            low_price, close_price, total_volume, exchange, source
                        FROM raw_price
                        WHERE trading_date IN (:dates)
                    """
                    if excluded_symbols:
                        insert_query = """
                            INSERT INTO adj_price (
                                symbol, trading_date, open_price, high_price,
                                low_price, close_price, total_volume, exchange,
                                source
                            )
                            SELECT
                                symbol, trading_date, open_price, high_price,
                                low_price, close_price, total_volume, exchange,
                                source
                            FROM raw_price
                            WHERE trading_date IN (:dates)
                            AND symbol NOT IN (:excluded_symbols)
                        """

                    # Biên dịch câu lệnh SQL với bindparam(..., expanding=True) để xử lý dạng list an toàn
                    delete_stmt = text(delete_query).bindparams(
                        bindparam("dates", expanding=True)
                    )
                    insert_stmt = text(insert_query).bindparams(
                        bindparam("dates", expanding=True)
                    )

                    if excluded_symbols:
                        delete_stmt = delete_stmt.bindparams(
                            bindparam("excluded_symbols", expanding=True)
                        )
                        insert_stmt = insert_stmt.bindparams(
                            bindparam("excluded_symbols", expanding=True)
                        )

                    conn.execute(delete_stmt, params)
                    conn.execute(insert_stmt, params)
            except Exception as e:
                self.logger.error(
                    f"❌ [Postgres] Lỗi đồng bộ adj_price ở lô {i // chunk_size + 1}: {e}"
                )
                raise e

        self.logger.info(
            f"🎉 [Postgres] Hoàn tất sao chép từ raw sang adjusted "
            f"cho {len(date_strings)} ngày."
        )

    def get_state(self, key: str) -> str | dict | list | int | float | bool | None:
        """Đọc một trạng thái tùy ý từ bảng pipeline_state.

        Giá trị lưu trong cột JSON có thể là bất kỳ kiểu JSON nào (string, dict, list,
        số, bool, None). Caller cần tự ép kiểu nếu cần xử lý nghiệp vụ cụ thể.
        Ví dụ: nếu lưu bằng save_state(key, "2024-01-15") thì trả về str.

        Args:
            key (str): Khóa của trạng thái cần đọc.

        Returns:
            str | dict | list | int | float | bool | None:
                Giá trị JSON đã lưu trước đó, hoặc None nếu không tồn tại.
        """
        try:
            with self.Session() as session:
                row = session.query(PipelineStateModel).filter_by(key=key).first()
                return row.value if row else None
        except Exception as e:
            self.logger.warning(f"⚠️ [Postgres] Không thể đọc trạng thái '{key}': {e}")
            return None

    def save_state(
        self, key: str, value: str | dict | list | int | float | bool
    ) -> None:
        """Lưu một trạng thái tùy ý vào bảng pipeline_state.

        Giá trị phải tương thích JSON (str, dict, list, int, float, bool).
        Không truyền đối tượng Python tùy ý (datetime, Polars DataFrame,...)
        vì sẽ gây lỗi serialization tại tầng SQLAlchemy JSON.

        Args:
            key (str): Khóa của trạng thái cần lưu.
            value (str | dict | list | int | float | bool): Giá trị JSON cần lưu.
        """
        try:
            with self.Session() as session:
                try:
                    row = session.query(PipelineStateModel).filter_by(key=key).first()
                    if not row:
                        row = PipelineStateModel(key=key, value=value)
                        session.add(row)
                    else:
                        row.value = value
                    session.commit()
                except Exception as db_err:
                    session.rollback()
                    raise db_err
        except Exception as e:
            self.logger.error(f"❌ [Postgres] Không thể lưu trạng thái '{key}': {e}")
            raise e

    def read_checkpoint(self) -> dict[str, Any]:
        """Đọc tệp checkpoint EOD từ bảng pipeline_state.

        Returns:
            dict[str, Any]: Từ điển chứa thông tin checkpoint (metadata và snapshots).
                Trả về từ điển trống nếu không tìm thấy dữ liệu.
        """
        try:
            with self.Session() as session:
                metadata_row = (
                    session.query(PipelineStateModel).filter_by(key="metadata").first()
                )
                snapshots_row = (
                    session.query(PipelineStateModel).filter_by(key="snapshots").first()
                )

                metadata = metadata_row.value if metadata_row else {}
                snapshots = snapshots_row.value if snapshots_row else {}

                if not metadata and not snapshots:
                    return {}
                return {"metadata": metadata, "snapshots": snapshots}
        except Exception as e:
            self.logger.warning(
                f"⚠️ [Postgres] Không thể đọc checkpoint từ database: {e}. "
                "Tiến hành khởi tạo mới."
            )
            return {}

    def save_checkpoint(
        self,
        df: pl.DataFrame,
        active_symbols: set[str] | None = None,
        pending_adjusted_reloads: list[str] | None = None,
    ) -> None:
        """Trích xuất và cập nhật checkpoint trạng thái thị trường EOD vào PostgreSQL.

        Args:
            df (pl.DataFrame): DataFrame chứa thông tin giao dịch trong phiên.
            active_symbols (set[str] | None, optional): Tập hợp các mã cổ phiếu đang hoạt động.
                Mặc định là None.
            pending_adjusted_reloads (list[str] | None, optional): Danh sách các mã cổ phiếu
                đang chờ tải lại lịch sử điều chỉnh giá. Mặc định là None.

        Raises:
            Exception: Phát sinh khi không thể ghi đè/chén dữ liệu vào bảng pipeline_state.
        """
        if df is None or df.is_empty():
            return

        self.logger.info(
            "⚡ [Snapshot] Đang trích xuất trạng thái thị trường EOD cho PostgreSQL..."
        )

        # Xây dựng cấu trúc JSON snapshot qua phương thức dùng chung trên BaseStorage
        old_checkpoint: dict[str, Any] = self.read_checkpoint()
        final_json_structure: dict[str, Any] = self._build_eod_snapshot(
            df=df,
            active_symbols=active_symbols,
            pending_adjusted_reloads=pending_adjusted_reloads,
            old_checkpoint=old_checkpoint,
        )
        final_snapshots: dict[str, Any] = final_json_structure["snapshots"]

        # Lưu vào PostgreSQL
        try:
            with self.Session() as session:
                try:
                    # Cập nhật hoặc tạo mới key metadata
                    meta_row = (
                        session.query(PipelineStateModel)
                        .filter_by(key="metadata")
                        .first()
                    )
                    if not meta_row:
                        meta_row = PipelineStateModel(
                            key="metadata", value=final_json_structure["metadata"]
                        )
                        session.add(meta_row)
                    else:
                        meta_row.value = final_json_structure["metadata"]

                    # Cập nhật hoặc tạo mới key snapshots
                    snap_row = (
                        session.query(PipelineStateModel)
                        .filter_by(key="snapshots")
                        .first()
                    )
                    if not snap_row:
                        snap_row = PipelineStateModel(
                            key="snapshots", value=final_json_structure["snapshots"]
                        )
                        session.add(snap_row)
                    else:
                        snap_row.value = final_json_structure["snapshots"]

                    session.commit()
                    self.logger.info(
                        f"💾 [Postgres Checkpoint] Lưu checkpoint thành công cho "
                        f"{len(final_snapshots)} mã."
                    )
                except Exception as db_err:
                    session.rollback()
                    raise db_err
        except Exception as e:
            self.logger.error(
                f"🛑 [Postgres Checkpoint] Ghi tệp snapshot trạng thái vào "
                f"database thất bại: {e}"
            )
        finally:
            gc.collect()

    def read_blacklist(self) -> set[str]:
        """Tải danh sách mã thuộc danh sách đen từ file blacklist.txt cục bộ.

        Returns:
            set[str]: Tập hợp các mã cổ phiếu viết hoa nằm trong danh sách đen.
                Trả về tập hợp rỗng nếu không tìm thấy file.
        """
        try:
            with open(Config.BLACKLIST_PATH_KEY, "r", encoding="utf-8") as file:
                blacklist: set[str] = {
                    line.strip().upper()
                    for line in file
                    if line.strip() and not line.strip().startswith("#")
                }
                self.logger.info(
                    f"📂 [LocalStorage] Đã tải danh sách đen gồm "
                    f"{len(blacklist)} mã từ file cục bộ."
                )
                return blacklist
        except FileNotFoundError:
            self.logger.warning(
                f"⚠️ [LocalStorage] Không tìm thấy file '{Config.BLACKLIST_PATH_KEY}' "
                "cục bộ. Bỏ qua bộ lọc blacklist."
            )
            return set()

    def load_parquet_to_bigquery(
        self,
        gcs_path: str,
        table_name: str,
        write_disposition: str = "WRITE_APPEND",
    ) -> None:
        """Đồng bộ tệp Parquet cục bộ vào cơ sở dữ liệu PostgreSQL.

        Args:
            gcs_path (str): Đường dẫn tệp Parquet.
            table_name (str): Tên bảng đích trong PostgreSQL.
            write_disposition (str): Chế độ ghi ('WRITE_APPEND' hoặc 'WRITE_TRUNCATE').
        """
        self.logger.info(
            f"⚡ [Postgres] Đang nạp tệp Parquet {gcs_path} vào bảng {table_name} "
            f"ở chế độ {write_disposition}..."
        )
        try:
            local_path: str = gcs_path
            if not os.path.exists(local_path):
                local_path = os.path.join("data", gcs_path)

            if not os.path.exists(local_path):
                self.logger.error(
                    f"❌ [Postgres] Không tìm thấy file Parquet tại: {gcs_path} hoặc {local_path}"
                )
                return

            df: pl.DataFrame = pl.read_parquet(local_path)
            if df.is_empty():
                return

            if "trading_date" in df.columns:
                df = df.with_columns(pl.col("trading_date").cast(pl.Date))

            df_write = self._select_ohlcv_cols(df)

            # Xử lý TRUNCATE nếu được yêu cầu
            if write_disposition == "WRITE_TRUNCATE":
                # Kiểm tra tên bảng trước khi nhúng vào SQL để ngăn chặn SQL Injection
                self._validate_table_name(table_name)
                self.logger.info(f"🗑️ [Postgres] Truncate bảng {table_name}...")
                with self.engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))

            # Dùng luôn hàm upsert cho cả 2 trường hợp
            self._upsert_polars_to_pg(
                df=df_write,
                table_name=table_name,
                index_elements=["symbol", "trading_date"],
            )
            self.logger.info(
                f"🎉 [Postgres] Nạp hoàn tất dữ liệu từ file Parquet vào bảng {table_name}."
            )
        except Exception as e:
            self.logger.error(
                f"❌ [Postgres] Lỗi khi nạp file Parquet {gcs_path} vào database: {e}"
            )
            raise e
