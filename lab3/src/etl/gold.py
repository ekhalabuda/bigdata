"""Gold layer: аналитические агрегаты и feature-таблица."""
import polars as pl
from deltalake import write_deltalake, DeltaTable
import logging

logger = logging.getLogger(__name__)

def build_gold_tables(silver_path: str, agg_path: str, feat_path: str) -> tuple:
    logger.info("Creating Gold layers from Silver")

    lazy_silver = pl.scan_delta(silver_path)

    # Аналитическая витрина
    agg_df = (lazy_silver
              .group_by(["Origin", "Airline", "hour", "season"])
              .agg([
                  pl.mean("ArrivalDelay").alias("avg_arrival_delay"),
                  pl.mean("DepartureDelay").alias("avg_departure_delay"),
                  pl.count().alias("flight_count")
              ])
              .collect()
    )
    logger.info(f"Gold aggregations: {len(agg_df)} rows")

    write_deltalake(agg_path, agg_df.to_arrow(), mode="overwrite", engine="rust")

    # Feature-таблица для ML
    feat_df = lazy_silver.select([
        "DepartureDelay", "ArrivalDelay", "hour", "day_of_week",
        pl.col("Month").alias("month"),   # приводим к единообразному имени
        "season", "Distance", "Airline", "Origin", "Dest"
    ]).collect()

    logger.info(f"Gold feature table: {len(feat_df)} rows")
    write_deltalake(feat_path, feat_df.to_arrow(), mode="overwrite", engine="rust")

    # Дополнительные Delta-операции
    dt_agg = DeltaTable(agg_path)
    logger.info("Running VACUUM (dry-run) on aggregations...")
    dt_agg.vacuum(retention_hours=168, dry_run=True)

    dt_feat = DeltaTable(feat_path)
    logger.info("Applying Z-ORDER on Airline for feature table...")
    dt_feat.optimize.z_order(["Airline"])

    return agg_path, feat_path
