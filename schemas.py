import polars as pl
import pandera.polars as pa


class OHLCVSchema(pa.DataFrameModel):
    """Data Contract cho dữ liệu giá Chứng khoán (OHLCV).

    Quy định chặt chẽ kiểu dữ liệu, giới hạn và tính logic cơ bản.
    Cross-field checks đảm bảo tính hợp lệ logic của OHLCV (high ≥ low, v.v.).
    """

    symbol: str = pa.Field(str_matches=r"^[A-Z0-9_-]{3,12}$", coerce=True)
    trading_date: pl.Date = pa.Field(coerce=True)
    open_price: pl.Float32 = pa.Field(gt=0, coerce=True, nullable=False)
    high_price: pl.Float32 = pa.Field(gt=0, coerce=True, nullable=False)
    low_price: pl.Float32 = pa.Field(gt=0, coerce=True, nullable=False)
    close_price: pl.Float32 = pa.Field(gt=0, coerce=True, nullable=False)
    total_volume: pl.Int64 = pa.Field(ge=0, coerce=True, nullable=False)
    exchange: pl.Categorical = pa.Field(coerce=True)
    source: str = pa.Field(coerce=True)

    @pa.dataframe_check
    @classmethod
    def check_high_gte_low(cls, df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
        """Kiểm tra giá cao nhất (high) phải lớn hơn hoặc bằng giá thấp nhất (low).

        Args:
            df (pl.DataFrame | pl.LazyFrame): DataFrame/LazyFrame cần kiểm tra.

        Returns:
            pl.LazyFrame: Biểu thức logic dưới dạng LazyFrame.
        """
        return df.lazyframe.select(pl.col("high_price") >= pl.col("low_price"))

    @pa.dataframe_check
    @classmethod
    def check_high_gte_open(cls, df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
        """Kiểm tra giá cao nhất (high) phải lớn hơn hoặc bằng giá mở cửa (open).

        Args:
            df (pl.DataFrame | pl.LazyFrame): DataFrame/LazyFrame cần kiểm tra.

        Returns:
            pl.LazyFrame: Biểu thức logic dưới dạng LazyFrame.
        """
        return df.lazyframe.select(pl.col("high_price") >= pl.col("open_price"))

    @pa.dataframe_check
    @classmethod
    def check_high_gte_close(cls, df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
        """Kiểm tra giá cao nhất (high) phải lớn hơn hoặc bằng giá đóng cửa (close).

        Args:
            df (pl.DataFrame | pl.LazyFrame): DataFrame/LazyFrame cần kiểm tra.

        Returns:
            pl.LazyFrame: Biểu thức logic dưới dạng LazyFrame.
        """
        return df.lazyframe.select(pl.col("high_price") >= pl.col("close_price"))
