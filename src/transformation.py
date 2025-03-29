from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, avg, min, max, countDistinct, when, lit, isnan, round
)

def category_aggregations(df: DataFrame) -> DataFrame:
    try:
        return df.groupBy("category_cleaned").agg(
            count("*").alias("item_count"),
            round(avg("requested_unit_price_cleaned"), 2).alias("avg_price"),
            round(min("requested_unit_price_cleaned"), 2).alias("min_price"),
            round(max("requested_unit_price_cleaned"), 2).alias("max_price")
        )
    except Exception as e:
        print(f"Category aggregation failed: {e}")
        return None

def column_profiling(df: DataFrame) -> DataFrame:
    try:
        # Recolectamos métricas por columna
        stats = []
        for col_name in df.columns:
            col_stats = df.select(
                lit(col_name).alias("column"),
                count(when(col(col_name).isNull() | isnan(col(col_name)), col_name)).alias("null_count"),
                countDistinct(col_name).alias("unique_count"),
                min(col_name).alias("min"),
                max(col_name).alias("max")
            ).collect()[0]
            stats.append(col_stats.asDict())
        
        # Convertimos a un nuevo DataFrame
        return df.sparkSession.createDataFrame(stats)
    except Exception as e:
        print(f"Column profiling failed: {e}")
        return None