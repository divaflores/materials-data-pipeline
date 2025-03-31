import os
from pyspark.sql import DataFrame
from src.utils import setup_logger, load_config_with_overrides

logger = setup_logger("pipeline")

def write_data(df: DataFrame, 
               output_path: str, 
               file_format: str, 
               mode: str,
               partition_by: str = None) -> None:
    try:

        config, args = load_config_with_overrides()
        logger = setup_logger(config["logs"]["log_name"], config["logs"]["log_file"])
        
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

        logger.info(f"Data written successfully to {output_path} as {file_format.lower()}.")

    except Exception as e:
        logger.exception(f"Error at writing data: {e}")


def write_parquet(df: DataFrame, 
                  output_path: str, 
                  partition_by: str = None,
                  mode: str = "overwrite"):
    try:
        config, _ = load_config_with_overrides()
        logger = setup_logger(config["logs"]["log_name"], config["logs"]["log_file"])

        if partition_by:
            df.write.mode(mode).partitionBy(partition_by).parquet(output_path)
        else:
            df.write.mode(mode).parquet(output_path)

        logger.info(f"Data written successfully to {output_path}.")

    except Exception as e:
        logger.exception(f"Error at writing data: {e}")
        return None