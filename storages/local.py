from datetime import date, datetime
import gc
import logging
import os
import time
from typing import Any

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    ForeignKey,
    Integer,
    JSON,
    Numeric,
    String,
    UniqueConstraint,
    create_engine,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from config import Config
from .base import BaseStorage

Base = declarative_base()


class CompanyModel(Base):
    """Bảng lưu trữ thông tin các công ty niêm yết."""

    __tablename__ = "companies"
    symbol = Column(String(10), primary_key=True)
    exchange = Column(String(10), nullable=False)
    company_name = Column(String(255), nullable=False)
    industry = Column(String(255), nullable=False)
    status = Column(String(50), nullable=False)


class CorporateEventModel(Base):
    """Bảng lưu trữ sự kiện doanh nghiệp (chia tách, cổ tức...)."""

    __tablename__ = "corporate_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), ForeignKey("companies.symbol", ondelete="CASCADE"), nullable=False)
    event_type = Column(String(50), nullable=False)
    ex_date = Column(Date, nullable=False)
    record_date = Column(Date, nullable=True)
    ratio = Column(String(50), nullable=True)

    __table_args__ = (
        UniqueConstraint("symbol", "event_type", "ex_date", name="uq_symbol_event_exdate"),
    )


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


class AdjustedPriceModel(Base):
    """Bảng lưu trữ giá điều chỉnh (adjusted price) của các cổ phiếu (được chuyển thành Hypertable)."""

    __tablename__ = "adjusted_price"
    symbol = Column(String(10), primary_key=True)
    trading_date = Column(Date, primary_key=True)
    open_price = Column(Numeric(18, 2), nullable=False)
    high_price = Column(Numeric(18, 2), nullable=False)
    low_price = Column(Numeric(18, 2), nullable=False)
    close_price = Column(Numeric(18, 2), nullable=False)
    total_volume = Column(BigInteger, nullable=False)
    exchange = Column(String(10), nullable=False)
    source = Column(String(50), nullable=False)


class PipelineStateModel(Base):
    """Bảng lưu trữ trạng thái chạy và checkpoint EOD (thay thế JSON)."""

    __tablename__ = "pipeline_state"
    key = Column(String(50), primary_key=True)
    value = Column(JSON, nullable=False)


class LocalStorage(BaseStorage):
    """Bộ lưu trữ dữ liệu cục bộ dùng PostgreSQL (TimescaleDB) và lưu file Parquet cục bộ."""

    engine: Any
    Session: Any

    def __init__(self, logger: logging.Logger) -> None:
        """Khởi tạo kết nối cục bộ PostgreSQL, kiểm tra TimescaleDB và di cư (migrate) các bảng."""
        super().__init__(logger)
        if not Config.DATABASE_URL:
            self.logger.error("🛑 [LocalStorage] DATABASE_URL không được cấu hình trong file .env!")
            raise ValueError("DATABASE_URL must be configured when DEPLOYMENT_ENV is local")

        # Cơ chế tự động thử lại kết nối phục vụ khởi động Docker Compose đồng bộ
        max_retries: int = 5
        retry_delay: int = 3

        self.logger.info("🔌 [LocalStorage] Đang kết nối đến PostgreSQL/TimescaleDB...")
        for attempt in range(1, max_retries + 1):
            try:
                self.engine = create_engine(Config.DATABASE_URL)
                # Chạy thử truy vấn để xác thực kết nối
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                self.logger.info("🐘 [LocalStorage] Kết nối thành công đến PostgreSQL.")
                break
            except Exception as e:
                if attempt == max_retries:
                    self.logger.error(f"🛑 [LocalStorage] Không thể kết nối đến database sau {max_retries} lần thử: {e}")
                    raise e
                self.logger.warning(
                    f"⚠️ [LocalStorage] Kết nối thất bại lần {attempt}/{max_retries}. "
                    f"Thử lại sau {retry_delay} giây..."
                )
                time.sleep(retry_delay)

        self.Session = sessionmaker(bind=self.engine)
        self.init_db()

    def init_db(self) -> None:
        """Khởi tạo cấu trúc các bảng và chuyển đổi raw_price/adjusted_price thành hypertable."""
        try:
            self.logger.info("🛠️ [LocalStorage] Đang thiết lập cấu trúc database (migrations)...")
            
            # Tạo các bảng cơ bản nếu chưa tồn tại
            Base.metadata.create_all(self.engine)

            with self.engine.begin() as conn:
                # Kích hoạt extension timescaledb nếu có
                try:
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
                except Exception as ext_err:
                    self.logger.warning(f"⚠️ [LocalStorage] Không thể chạy CREATE EXTENSION timescaledb: {ext_err}")

                # Kiểm tra và chuyển đổi raw_price thành hypertable
                res_raw = conn.execute(text(
                    "SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'raw_price';"
                )).fetchone()
                if not res_raw:
                    self.logger.info("✨ [TimescaleDB] Chuyển đổi 'raw_price' sang Hypertable...")
                    conn.execute(text("SELECT create_hypertable('raw_price', 'trading_date');"))

                # Kiểm tra và chuyển đổi adjusted_price thành hypertable
                res_adj = conn.execute(text(
                    "SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'adjusted_price';"
                )).fetchone()
                if not res_adj:
                    self.logger.info("✨ [TimescaleDB] Chuyển đổi 'adjusted_price' sang Hypertable...")
                    conn.execute(text("SELECT create_hypertable('adjusted_price', 'trading_date');"))

            self.logger.info("🎉 [LocalStorage] Cấu trúc database đã sẵn sàng.")
        except Exception as e:
            self.logger.error(f"🛑 [LocalStorage] Lỗi khởi tạo database: {e}")
            raise e

    def _create_upsert_method(self, index_elements: list[str]):
        """Tạo hàm helper cho pandas to_sql thực hiện Upsert (INSERT ... ON CONFLICT DO UPDATE)."""
        from sqlalchemy.dialects.postgresql import insert

        def method(table, conn, keys, data_iter):
            data = [dict(zip(keys, row)) for row in data_iter]
            if not data:
                return
            insert_stmt = insert(table.table).values(data)
            update_cols = {
                c.name: c for c in insert_stmt.excluded 
                if c.name not in index_elements and c.name != "id"
            }
            if not update_cols:
                upsert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=index_elements)
            else:
                upsert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=index_elements,
                    set_=update_cols
                )
            conn.execute(upsert_stmt)

        return method

    def save_parquet(
        self,
        df: pd.DataFrame,
        date_ref: datetime,
        suffix: str = "raw",
        partition: bool = False
    ) -> str | None:
        """Lưu trữ dữ liệu nén Parquet cục bộ và trả về đường dẫn."""
        if df is None or df.empty:
            return None

        # Tối ưu: Lấy ngày thực tế lớn nhất từ cột trading_date để đặt tên thư mục/file nếu có
        if "trading_date" in df.columns:
            max_date: Any = pd.to_datetime(df["trading_date"]).max()
            if not pd.isna(max_date):
                date_ref = max_date

        gcs_path: str
        if partition:
            year_str: str = date_ref.strftime("%Y")
            month_str: str = date_ref.strftime("%m")
            date_str: str = date_ref.strftime("%Y%m%d")
            gcs_path = f"{Config.GCS_PARQUET_PREFIX}/{suffix}/year={year_str}/month={month_str}/daily_{date_str}.parquet"
        else:
            gcs_path = f"{Config.GCS_PARQUET_PREFIX}/{suffix}/cafef_historical_all.parquet"

        local_path: str = os.path.join("data", gcs_path)
        try:
            self.logger.info(f"💾 [Local Disk] Đang ghi dữ liệu nén Parquet cục bộ: {local_path}")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            df.to_parquet(local_path, index=False, compression="snappy")
            self.logger.info(f"🎉 [Local Disk] File lưu trữ thành công tại: {local_path}")
            return local_path
        except Exception as e:
            self.logger.error(f"❌ [Local Disk] Lỗi trong quá trình ghi file Parquet cục bộ: {e}")
            raise e

    def save_symbol_history(
        self,
        df: pd.DataFrame,
        symbol: str,
        suffix: str = "adj"
    ) -> None:
        """Lưu trữ toàn bộ lịch sử giá của một mã cổ phiếu cụ thể ra file Parquet cục bộ."""
        if df is None or df.empty:
            return

        gcs_path: str = f"{Config.GCS_PARQUET_PREFIX}/{suffix}/reloaded/{symbol.upper()}.parquet"
        local_path: str = os.path.join("data", gcs_path)

        try:
            self.logger.info(f"💾 [Local Disk] Đang ghi lịch sử cho mã {symbol.upper()} tại: {local_path}")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            df.to_parquet(local_path, index=False, compression="snappy")
            self.logger.info(f"🎉 [Local Disk] File lịch sử mã {symbol.upper()} lưu trữ thành công.")
        except Exception as e:
            self.logger.error(f"❌ [Local Disk] Lỗi khi ghi file lịch sử cho mã {symbol.upper()} cục bộ: {e}")
            raise e

    def sync_partition_to_bigquery(
        self,
        path: str,
        table_name: str,
        date_ref: date
    ) -> None:
        """Đồng bộ dữ liệu phân vùng từ file Parquet cục bộ vào bảng tương ứng trong Postgres."""
        self.logger.info(f"⚡ [Postgres] Đang đồng bộ phân vùng từ file {path} vào bảng {table_name}...")
        try:
            df = pd.read_parquet(path)
            if df.empty:
                return

            if "trading_date" in df.columns:
                df["trading_date"] = pd.to_datetime(df["trading_date"]).dt.date

            # Loại bỏ các cột không thuộc schema lưu trữ Postgres (ví dụ reference_price, average_price)
            valid_cols = ["symbol", "trading_date", "open_price", "high_price", "low_price", "close_price", "total_volume", "exchange", "source"]
            df_write = df[[c for c in valid_cols if c in df.columns]].copy()

            upsert_method = self._create_upsert_method(["symbol", "trading_date"])
            df_write.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="append",
                index=False,
                method=upsert_method,
            )
            self.logger.info(f"🎉 [Postgres] Đồng bộ thành công vào bảng {table_name} cho ngày {date_ref}.")
        except Exception as e:
            self.logger.error(f"❌ [Postgres] Lỗi đồng bộ phân vùng vào bảng {table_name}: {e}")
            raise e

    def sync_adjusted_symbols_to_bigquery(self, symbols: list[str]) -> None:
        """Đọc và đồng bộ lịch sử điều chỉnh của danh sách mã từ Parquet cục bộ vào PostgreSQL."""
        if not symbols:
            return

        self.logger.info(f"⚡ [Postgres] Bắt đầu đồng bộ gộp lịch sử điều chỉnh cho {len(symbols)} mã...")
        try:
            for symbol in symbols:
                gcs_path: str = f"{Config.GCS_PARQUET_PREFIX}/adj/reloaded/{symbol.upper()}.parquet"
                local_path: str = os.path.join("data", gcs_path)
                if not os.path.exists(local_path):
                    self.logger.warning(f"⚠️ [Postgres] Không thấy file lịch sử {local_path} để sync.")
                    continue

                df = pd.read_parquet(local_path)
                if df.empty:
                    continue

                if "trading_date" in df.columns:
                    df["trading_date"] = pd.to_datetime(df["trading_date"]).dt.date

                valid_cols = ["symbol", "trading_date", "open_price", "high_price", "low_price", "close_price", "total_volume", "exchange", "source"]
                df_write = df[[c for c in valid_cols if c in df.columns]].copy()

                upsert_method = self._create_upsert_method(["symbol", "trading_date"])
                df_write.to_sql(
                    name=Config.BQ_ADJ_TABLE,
                    con=self.engine,
                    if_exists="append",
                    index=False,
                    method=upsert_method,
                )
            self.logger.info(f"🎉 [Postgres] Đồng bộ lịch sử điều chỉnh hoàn tất cho {len(symbols)} mã.")
        except Exception as e:
            self.logger.error(f"❌ [Postgres] Lỗi đồng bộ gộp lịch sử điều chỉnh: {e}")
            raise e

    def sync_daily_adjusted_prices(
        self,
        dates: list[datetime | date],
        excluded_symbols: list[str]
    ) -> None:
        """Sao chép giá thô của các ngày sang bảng giá điều chỉnh đối với các mã bình thường."""
        if not dates:
            return

        date_strings: list[str] = [
            d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in dates
        ]
        self.logger.info(f"⚡ [Postgres] Đang sao chép giá từ raw sang adjusted cho các ngày: {date_strings}...")

        try:
            with self.engine.begin() as conn:
                # 1. Xóa dữ liệu cũ ngày hôm đó
                delete_query = "DELETE FROM adjusted_price WHERE trading_date = ANY(:dates)"
                params = {"dates": [datetime.strptime(ds, "%Y-%m-%d").date() for ds in date_strings]}

                if excluded_symbols:
                    delete_query = "DELETE FROM adjusted_price WHERE trading_date = ANY(:dates) AND symbol != ALL(:excluded_symbols)"
                    params["excluded_symbols"] = [s.upper() for s in excluded_symbols]

                conn.execute(text(delete_query), params)

                # 2. Sao chép từ raw_price sang adjusted_price
                insert_query = """
                    INSERT INTO adjusted_price (
                        symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange, source
                    )
                    SELECT
                        symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange, source
                    FROM raw_price
                    WHERE trading_date = ANY(:dates)
                """
                if excluded_symbols:
                    insert_query = """
                        INSERT INTO adjusted_price (
                            symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange, source
                        )
                        SELECT
                            symbol, trading_date, open_price, high_price, low_price, close_price, total_volume, exchange, source
                        FROM raw_price
                        WHERE trading_date = ANY(:dates) AND symbol != ALL(:excluded_symbols)
                    """

                conn.execute(text(insert_query), params)
                self.logger.info(f"🎉 [Postgres] Hoàn tất sao chép từ raw sang adjusted cho {len(date_strings)} ngày.")
        except Exception as e:
            self.logger.error(f"❌ [Postgres] Lỗi đồng bộ adjusted_price số lượng lớn ngày: {e}")
            raise e

    def read_checkpoint(self) -> dict[str, Any]:
        """Đọc tệp checkpoint EOD từ bảng pipeline_state."""
        try:
            with self.Session() as session:
                metadata_row = session.query(PipelineStateModel).filter_by(key="metadata").first()
                snapshots_row = session.query(PipelineStateModel).filter_by(key="snapshots").first()

                metadata = metadata_row.value if metadata_row else {}
                snapshots = snapshots_row.value if snapshots_row else {}

                if not metadata and not snapshots:
                    return {}
                return {
                    "metadata": metadata,
                    "snapshots": snapshots
                }
        except Exception as e:
            self.logger.warning(
                f"⚠️ [Postgres] Không thể đọc checkpoint từ database: {e}. Tiến hành khởi tạo mới."
            )
            return {}

    def save_checkpoint(
        self,
        df: pd.DataFrame,
        active_symbols: set[str] | None = None,
        pending_adjusted_reloads: list[str] | None = None
    ) -> None:
        """Trích xuất và cập nhật checkpoint trạng thái thị trường EOD vào PostgreSQL."""
        if df is None or df.empty:
            return

        self.logger.info("⚡ [Snapshot] Đang trích xuất trạng thái thị trường EOD cho PostgreSQL...")

        df_latest: pd.DataFrame = df.drop_duplicates(subset=["symbol"], keep="last").copy()

        price_cols: list[str] = ["open_price", "high_price", "low_price", "close_price"]
        if "average_price" not in df_latest.columns:
            df_latest["average_price"] = df_latest[price_cols].mean(axis=1)

        for col in price_cols:
            df_latest[col] = df_latest[col].astype(float).round(0).astype(int)
        df_latest["average_price"] = df_latest["average_price"].astype(float).round(1)

        df_latest["trading_date"] = df_latest["trading_date"].dt.strftime("%Y-%m-%d")

        max_date_str: str = str(df_latest["trading_date"].max())

        vn_now: datetime = datetime.now(Config.VN_TZ)
        today_str: str = vn_now.strftime("%Y-%m-%d")
        is_eod: bool
        if max_date_str < today_str:
            is_eod = True
        else:
            is_eod = vn_now.hour > 15 or (vn_now.hour == 15 and vn_now.minute >= 15)

        if isinstance(df_latest["symbol"].dtype, pd.CategoricalDtype):
            df_latest["symbol"] = df_latest["symbol"].astype(str)
        df_latest.set_index("symbol", inplace=True)

        cols_to_extract: list[str] = [
            "exchange", "trading_date", "open_price", "high_price",
            "low_price", "close_price", "average_price", "total_volume"
        ]
        current_data_dict: dict[str, dict[str, Any]] = df_latest[cols_to_extract].to_dict(orient="index")

        old_checkpoint: dict[str, Any] = self.read_checkpoint()
        merged_snapshots: dict[str, dict[str, Any]] = old_checkpoint.get("snapshots", {})
        old_metadata: dict[str, Any] = old_checkpoint.get("metadata") or {}
        old_pending: list[str] = old_metadata.get("pending_adjusted_reloads") or []

        if pending_adjusted_reloads is None:
            pending_adjusted_reloads = old_pending

        if is_eod:
            for sym, new_row in current_data_dict.items():
                if not sym:
                    continue
                old_row: dict[str, Any] | None = merged_snapshots.get(sym)
                if not old_row or new_row["trading_date"] >= old_row["trading_date"]:
                    merged_snapshots[sym] = new_row
        else:
            self.logger.info(
                "ℹ️ [Snapshot] Đang chạy trong phiên (Chưa chốt EOD). Giữ nguyên dữ liệu snapshots cũ."
            )

        for sym, row in merged_snapshots.items():
            for col in price_cols:
                if col in row and isinstance(row[col], (int, float)):
                    row[col] = int(round(float(row[col])))
            if "average_price" in row and isinstance(row["average_price"], (int, float)):
                row["average_price"] = round(float(row["average_price"]), 1)

        final_snapshots: dict[str, dict[str, Any]] = {}
        for sym in sorted(merged_snapshots.keys()):
            if active_symbols and sym not in active_symbols:
                continue
            final_snapshots[sym] = merged_snapshots[sym]

        final_json_structure: dict[str, Any] = {
            "metadata": {
                "last_successful_run": max_date_str,
                "is_eod": is_eod,
                "total_tickers": len(final_snapshots),
                "pending_adjusted_reloads": pending_adjusted_reloads
            },
            "snapshots": final_snapshots
        }

        # Lưu vào PostgreSQL
        try:
            with self.Session() as session:
                # Cập nhật hoặc tạo mới key metadata
                meta_row = session.query(PipelineStateModel).filter_by(key="metadata").first()
                if not meta_row:
                    meta_row = PipelineStateModel(key="metadata", value=final_json_structure["metadata"])
                    session.add(meta_row)
                else:
                    meta_row.value = final_json_structure["metadata"]

                # Cập nhật hoặc tạo mới key snapshots
                snap_row = session.query(PipelineStateModel).filter_by(key="snapshots").first()
                if not snap_row:
                    snap_row = PipelineStateModel(key="snapshots", value=final_json_structure["snapshots"])
                    session.add(snap_row)
                else:
                    snap_row.value = final_json_structure["snapshots"]

                session.commit()
                self.logger.info(
                    f"💾 [Postgres Checkpoint] Lưu checkpoint thành công cho {len(final_snapshots)} mã."
                )
        except Exception as e:
            self.logger.error(f"🛑 [Postgres Checkpoint] Ghi tệp snapshot trạng thái vào database thất bại: {e}")
        finally:
            del current_data_dict, merged_snapshots, final_snapshots
            gc.collect()

    def read_blacklist(self) -> set[str]:
        """Tải danh sách mã thuộc danh sách đen từ file blacklist.txt cục bộ."""
        try:
            with open("blacklist.txt", "r", encoding="utf-8") as file:
                blacklist: set[str] = {
                    line.strip().upper()
                    for line in file
                    if line.strip() and not line.strip().startswith("#")
                }
                self.logger.info(f"📂 [LocalStorage] Đã tải danh sách đen gồm {len(blacklist)} mã từ file cục bộ.")
                return blacklist
        except FileNotFoundError:
            self.logger.warning("⚠️ [LocalStorage] Không tìm thấy file 'blacklist.txt' cục bộ. Bỏ qua bộ lọc blacklist.")
            return set()

    def save_corporate_events(self, events: list[dict[str, Any]]) -> None:
        """Lưu danh sách chi tiết sự kiện doanh nghiệp vào PostgreSQL."""
        if not events:
            return
        self.logger.info(f"💾 [Postgres] Đang lưu {len(events)} sự kiện doanh nghiệp...")
        try:
            df = pd.DataFrame(events)
            
            # Ép kiểu ex_date và record_date về date
            if "ex_date" in df.columns:
                df["ex_date"] = pd.to_datetime(df["ex_date"]).dt.date
            if "record_date" in df.columns:
                df["record_date"] = pd.to_datetime(df["record_date"]).apply(lambda x: x.date() if pd.notna(x) else None)

            upsert_method = self._create_upsert_method(["symbol", "event_type", "ex_date"])
            df.to_sql(
                name="corporate_events",
                con=self.engine,
                if_exists="append",
                index=False,
                method=upsert_method,
            )
            self.logger.info("🎉 [Postgres] Lưu sự kiện doanh nghiệp thành công.")
        except Exception as e:
            self.logger.error(f"❌ [Postgres] Gặp lỗi khi lưu sự kiện doanh nghiệp: {e}")
            raise e

    def save_companies(self, df_companies: pd.DataFrame) -> None:
        """Lưu danh sách công ty vào PostgreSQL."""
        if df_companies is None or df_companies.empty:
            return
        self.logger.info(f"💾 [Postgres] Đang lưu {len(df_companies)} thông tin công ty...")
        try:
            upsert_method = self._create_upsert_method(["symbol"])
            df_companies.to_sql(
                name="companies",
                con=self.engine,
                if_exists="append",
                index=False,
                method=upsert_method,
            )
            self.logger.info("🎉 [Postgres] Lưu thông tin công ty thành công.")
        except Exception as e:
            self.logger.error(f"❌ [Postgres] Gặp lỗi khi lưu thông tin công ty: {e}")
            raise e
