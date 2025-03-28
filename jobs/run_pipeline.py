import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
from src.ingestion import create_spark_session, read_materials_csv
from src.cleaning import clean_and_validate_data
from src.transformation import category_aggregations
from src.storage import write_parquet
from src.utils import load_config


if __name__ == "__main__":
    config = load_config()
    spark = create_spark_session(config["spark"]["app_name"])

    try:
        paths = config["paths"]
        input_path = paths["input_csv"]
        quarantine_path = paths["quarantine_data_dir"]
        processed_path = paths["processed_data_dir"]

        # Ingestion
        df_raw = read_materials_csv(spark, input_path)

        if df_raw is None:
            print("Data reading failed. Aborting.")
            spark.stop()
            sys.exit(1)
            
        # Cleansing and validation
        df_clean = clean_and_validate_data(df_raw, quarantine_path)

        if df_clean is None:
            print("Data cleaning failed. Aborting.")
            spark.stop()
            sys.exit(1)

        # Aggregations
        df_category_stats = category_aggregations(df_clean)
        #df_column_stats = column_profiling(df_clean)

        if df_category_stats is None:
            print("Data aggregation failed. Aborting.")
            spark.stop()
            sys.exit(1)

        # Storage
        write_parquet(df_category_stats, os.path.join(processed_path, "category_stats"))
        #write_parquet(df_column_stats, os.path.join(processed_path, "column_profile"))

    except KeyboardInterrupt:
        print("\nPipeline interrupted by user. Cleaning up...")
    except Exception as e:
        print(f"Error during pipeline execution: {e}")
    finally:
        try:
            if spark:
                #print("Stopping Spark...")
                spark.stop()
                #print("Spark stopped.")
        except Exception as e:
            print(f"Error stopping Spark: {e}")
        finally:
            import os
            #print("Forced exit")
            os._exit(0)  
