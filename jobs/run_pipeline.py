from src.ingestion import create_spark_session, read_materials_csv
from src.cleaning import clean_and_validate_data
from src.transformation import enrich_data, category_aggregations, column_profiling
from src.storage import write_parquet
from src.utils import load_config
import os

if __name__ == "__main__":
    config = load_config()
    spark = create_spark_session(config["spark"]["app_name"])

    paths = config["paths"]
    input_path = paths["input_csv"]
    quarantine_path = paths["quarantine_data_dir"]
    processed_path = paths["processed_data_dir"]

    # Ingestion
    df_raw = read_materials_csv(spark, input_path)

    # Cleansing and validation
    df_clean = clean_and_validate_data(df_raw, quarantine_path)

    # Transformations
    df_enriched = enrich_data(df_clean)

    # Aggregations
    df_category_stats = category_aggregations(df_enriched)
    df_column_stats = column_profiling(df_enriched)

    # Storage
    write_parquet(df_enriched, os.path.join(processed_path, "enriched_data"), partition_by="category")
    write_parquet(df_category_stats, os.path.join(processed_path, "category_stats"))
    write_parquet(df_column_stats, os.path.join(processed_path, "column_profile"))

    spark.stop()