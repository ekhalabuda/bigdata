"""Основной entry-point пайплайна."""
import logging
import sys
import os

# Настройка логирования в файл и консоль
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/app.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

from src.etl.bronze import create_bronze_table
from src.etl.silver import transform_to_silver
from src.etl.gold import build_gold_tables
from src.ml.model import run_regression_models

def ensure_dirs(*paths):
    for p in paths:
        os.makedirs(p, exist_ok=True)

def main():
    logger = logging.getLogger("pipeline")
    logger.info("Starting Flight Lakehouse pipeline (variant 2)")

    # Пути внутри контейнера
    csv_input = "/app/data/flight_data_2018_2024.csv"
    bronze_dir = "/app/data/bronze"
    silver_dir = "/app/data/silver"
    gold_agg_dir = "/app/data/gold_agg"
    gold_feat_dir = "/app/data/gold_feat"

    ensure_dirs(bronze_dir, silver_dir, gold_agg_dir, gold_feat_dir)

    try:
        # Bronze
        create_bronze_table(csv_input, bronze_dir)

        # Silver
        transform_to_silver(bronze_dir, silver_dir)

        # Gold
        agg_path, feat_path = build_gold_tables(silver_dir, gold_agg_dir, gold_feat_dir)

        # ML
        run_regression_models(feat_path)

        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.exception("Pipeline failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
