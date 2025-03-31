from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, avg, min, max, countDistinct, when, isnan, round
from src.utils import setup_logger, load_config_with_overrides

def category_aggregations(df: DataFrame) -> DataFrame:
    try:
        config, _ = load_config_with_overrides()
        logger = setup_logger(config["logs"]["log_name"], config["logs"]["log_file"])

        return df.groupBy("category_cleaned").agg(
            count("*").alias("item_count"),
            round(avg("requested_unit_price_cleaned"), 2).alias("avg_price"),
            round(min("requested_unit_price_cleaned"), 2).alias("min_price"),
            round(max("requested_unit_price_cleaned"), 2).alias("max_price")
        )
    except Exception as e:
        logger.exception(f"Category aggregation failed: {e}")
        return None

def column_profiling(df: DataFrame) -> DataFrame:
    try:
        config, _ = load_config_with_overrides()
        logger = setup_logger(config["logs"]["log_name"], config["logs"]["log_file"])

        total_rows = df.count()

        # Build agg expressions
        agg_exprs = []
        for c in df.columns:
            agg_exprs += [
                count(when(col(c).isNull() | isnan(col(c)), c)).alias(f"{c}_nulls"),
                countDistinct(col(c)).alias(f"{c}_unique"),
                min(col(c)).alias(f"{c}_min"),
                max(col(c)).alias(f"{c}_max")
            ]

        # Aggregate
        agg_row = df.agg(*agg_exprs).first().asDict()

        stats = []
        for field in df.schema.fields:
            col_name = field.name
            data_type = str(field.dataType)
            null_count = agg_row.get(f"{col_name}_nulls", 0)
            unique_count = agg_row.get(f"{col_name}_unique", None)
            min_val = agg_row.get(f"{col_name}_min", None)
            max_val = agg_row.get(f"{col_name}_max", None)
            null_percent = round((null_count / total_rows) * 100, 2) if total_rows > 0 else None

            try:
                moda_row = (
                    df.groupBy(col_name)
                    .count()
                    .orderBy(col("count").desc())
                    .filter(col(col_name).isNotNull())
                    .first()
                )
                mode_val = moda_row[col_name] if moda_row else None
            except:
                mode_val = None
                
            stats.append({
                "column": col_name,
                "data_type": data_type,
                "null_count": null_count,
                "null_percent": null_percent,
                "unique_count": unique_count,
                "min": min_val,
                "max": max_val,
                "mode": mode_val
            })

        return df.sparkSession.createDataFrame(stats)

    except Exception as e:
        logger.exception(f"Column profiling failed: {e}")
        return None