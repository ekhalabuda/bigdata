"""Silver layer: очистка, трансформация и дедупликация с партиционированием."""
import polars as pl
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
import logging

logger = logging.getLogger(__name__)

def _delta_table_exists(path: str) -> bool:
    try:
        DeltaTable(path)
        return True
    except TableNotFoundError:
        return False

def transform_to_silver(bronze_path: str, silver_path: str) -> str:
    logger.info("Starting Silver transformation")

    # Lazy-фрейм из Bronze
    lazy_df = pl.scan_delta(bronze_path)

    # Выбор и переименование колонок
    clean_df = lazy_df.select([
        "Year", "Quarter", "Month", "DayofMonth", "DayOfWeek", "FlightDate",
        "Marketing_Airline_Network", "Origin", "Dest", "CRSDepTime", "DepTime",
        "DepDelay", "DepDelayMinutes", "ArrDelay", "ArrDelayMinutes",
        "Cancelled", "Diverted", "Distance"
    ]).rename({
        "Marketing_Airline_Network": "Airline",
        "DepDelay": "DepartureDelay",
        "ArrDelay": "ArrivalDelay"
    })

    # Фильтрация
    clean_df = clean_df.filter(
        (pl.col("Cancelled") == 0) &
        (pl.col("Diverted") == 0) &
        pl.col("DepartureDelay").is_not_null() &
        pl.col("ArrivalDelay").is_not_null() &
        (pl.col("DepartureDelay").abs() <= 360) &
        (pl.col("ArrivalDelay").abs() <= 360)
    )

    # Производные признаки
    transformed = clean_df.with_columns([
        (pl.col("CRSDepTime") // 100).alias("hour"),
        pl.col("DayOfWeek").alias("day_of_week"),
        pl.col("Month").alias("month"),
        pl.when(pl.col("Month").is_between(3, 5)).then(pl.lit("spring"))
          .when(pl.col("Month").is_between(6, 8)).then(pl.lit("summer"))
          .when(pl.col("Month").is_between(9, 11)).then(pl.lit("fall"))
          .otherwise(pl.lit("winter")).alias("season"),
        (pl.col("Origin") + "-" + pl.col("Dest")).alias("route")
    ])

    # Финальный набор колонок
    silver_lazy = transformed.select([
        "Year", "Month", "DayofMonth", "day_of_week", "FlightDate",
        "Airline", "Origin", "Dest", "route", "hour", "season",
        "DepartureDelay", "ArrivalDelay", "Distance"
    ])

    logger.info("Silver plan:\n%s", silver_lazy.explain(optimized=True))

    silver_df = silver_lazy.collect()
    logger.info(f"Silver rows after cleaning: {len(silver_df)}")

    # MERGE: если таблица существует, дедуплицируем по бизнес-ключу
    if _delta_table_exists(silver_path):
        logger.info("Merging with existing Silver table")
        existing = pl.read_delta(silver_path)
        combined = pl.concat([existing, silver_df])
        key_cols = ["Year", "Month", "DayofMonth", "Airline", "FlightDate", "Origin", "Dest"]
        silver_df = combined.unique(subset=key_cols, keep="last")
        logger.info(f"Merged size: {len(silver_df)}")

    # Запись с партиционированием
    write_deltalake(
        silver_path,
        silver_df.to_arrow(),
        mode="overwrite",
        partition_by=["Year", "Month"],
        engine="rust"
    )

    # Оптимизации
    dt = DeltaTable(silver_path)
    logger.info("Compacting Silver table...")
    dt.optimize.compact()
    logger.info("Applying Z-ORDER on Origin...")
    dt.optimize.z_order(["Origin"])

    logger.info(f"Silver table saved to {silver_path}")
    return silver_path
