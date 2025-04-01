from pyspark.sql import DataFrame
from pyspark.sql.functions import count, avg, min, max, round
from typing import Optional
from src.utils import setup_logger, load_config_with_overrides

def aggregations(df: DataFrame) -> DataFrame:
    try:
        config, _ = load_config_with_overrides()
        logger = setup_logger(config["logs"]["log_name"], config["logs"]["log_file"])

        return df.groupBy(["name_cleaned", "manufacturer_name_cleaned", "category_cleaned"]).agg(
            count("*").alias("item_count"),
            round(avg("requested_unit_price_cleaned"), 2).alias("avg_price"),
            round(min("requested_unit_price_cleaned"), 2).alias("min_price"),
            round(max("requested_unit_price_cleaned"), 2).alias("max_price")
        )
    except Exception as e:
        logger.exception(f"Aggregation failed: {e}")
        return None