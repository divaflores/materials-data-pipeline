import os
from pyspark.sql import DataFrame

def write_parquet(df: DataFrame, output_path: str, partition_by: str = None, mode: str = "overwrite"):
    if partition_by:
        df.write.mode(mode).partitionBy(partition_by).parquet(output_path)
    else:
        df.write.mode(mode).parquet(output_path)