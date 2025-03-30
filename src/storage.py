import os
from pyspark.sql import DataFrame

def write_data(df: DataFrame, 
               output_path: str, 
               file_format: str, 
               mode: str,
               partition_by: str = None) -> None:
    try:
        writer = df.write.mode(mode)

        if partition_by:
            writer = writer.partitionBy(partition_by)

        file_format = file_format.lower()

        if file_format == "parquet":
            writer.parquet(output_path)
        elif file_format == "orc":
            writer.orc(output_path)
        elif file_format == "json":
            writer.json(output_path)
        elif file_format == "csv":
            writer.option("header", True).csv(output_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        print(f"Data written successfully to {output_path} as {file_format.upper()}.")

    except Exception as e:
        print(f"write_data error: {e}")


def write_parquet(df: DataFrame, 
                  output_path: str, 
                  partition_by: str = None,
                  mode: str = "overwrite"):
    try:
        if partition_by:
            df.write.mode(mode).partitionBy(partition_by).parquet(output_path)
        else:
            df.write.mode(mode).parquet(output_path)
    
    except Exception as e:
        print(f"write_parquet: {e}")
        return None