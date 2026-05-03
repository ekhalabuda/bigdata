"""Bronze layer: загрузка CSV в Delta-таблицу с инкрементальной историей."""
import polars as pl
from deltalake import write_deltalake
import logging

logger = logging.getLogger(__name__)

def create_bronze_table(source_csv: str, target_path: str) -> str:
    logger.info("Loading raw CSV into Bronze Delta table (daily append batches)")

    raw_df = pl.read_csv(source_csv, try_parse_dates=False)

    # Формируем столбец с датой для группировки
    if "FlightDate" in raw_df.columns:
        date_col = pl.col("FlightDate").cast(pl.Utf8)
    elif all(c in raw_df.columns for c in ["Year", "Month", "DayofMonth"]):
        date_col = (
            pl.col("Year").cast(pl.Utf8) + "-" +
            pl.col("Month").cast(pl.Utf8).str.zfill(2) + "-" +
            pl.col("DayofMonth").cast(pl.Utf8).str.zfill(2)
        )
    else:
        raise ValueError("CSV must contain 'FlightDate' or ('Year','Month','DayofMonth')")

    raw_df = raw_df.with_columns(date_col.alias("_date"))
    daily_groups = sorted(raw_df.select("_date").unique().to_series().to_list())

    logger.info(f"Found {len(daily_groups)} daily partitions (first 5: {daily_groups[:5]})")

    first_batch = True
    for day in daily_groups:
        batch = raw_df.filter(pl.col("_date") == day).drop("_date")
        logger.info(f"Writing batch {day} ({len(batch)} rows)")
        write_deltalake(
            target_path,
            batch.to_arrow(),
            mode="overwrite" if first_batch else "append",
            engine="rust"
        )
        first_batch = False

    logger.info(f"Bronze table ready at {target_path}")
    return target_path
