import argparse
import os
import yaml
import logging
from pyspark.sql import DataFrame

def load_config(path="config/config.yaml"):
    with open(path, "r") as file:
        return yaml.safe_load(file)
  
    
def load_or_checkpoint(df: DataFrame, checkpoint_path: str, eager: bool = True) -> DataFrame:
    if os.path.exists(checkpoint_path):
        print(f"Loading checkpoint from: {checkpoint_path}")
        return df.sparkSession.read.parquet(checkpoint_path)
    else:
        print("Generating checkpoint...")
        df_check = df.checkpoint(eager=eager)
        df_check.write.mode("overwrite").parquet(checkpoint_path)
        return df_check
    

def parse_args():
    parser = argparse.ArgumentParser(description="PySpark Pipeline")

    parser.add_argument("--config", type=str, default="config/config.yaml", help="Path to config file")
    parser.add_argument("--input", type=str, help="Override input path")
    parser.add_argument("--checkpoint", action="store_true", help="Force checkpointing even if disabled in config")

    return parser.parse_args()


def load_config_with_overrides():
    args = parse_args()

    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    # Apply overrides
    if args.input:
        config["paths"]["input_file"] = args.input

    if args.checkpoint:
        config["checkpointing"]["use_checkpoint"] = True

    return config, args


def setup_logger(name: str, log_file: str = None, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")

    # Avoid duplicity
    if not logger.handlers:
        # Console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File
        if log_file:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger