from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, avg, min, max, countDistinct, when, expr, lit, desc, isnan
)

def enrich_data(df: DataFrame) -> DataFrame:
    # Crea columna derivada: precio total solicitado
    return df.withColumn(
        "total_requested_price",
        col("requested_quantity") * col("requested_unit_price")
    )

def category_aggregations(df: DataFrame) -> DataFrame:
    return df.groupBy("category").agg(
        count("*").alias("item_count"),
        avg("requested_unit_price").alias("avg_price"),
        min("requested_unit_price").alias("min_price"),
        max("requested_unit_price").alias("max_price")
    )

def column_profiling(df: DataFrame) -> DataFrame:
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