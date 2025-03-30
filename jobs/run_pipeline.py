import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
from src.ingestion import create_spark_session, read_materials
from src.cleaning import clean_and_validate_data
from src.transformation import category_aggregations
from src.storage import write_data
from src.utils import load_or_checkpoint, load_config_with_overrides

if __name__ == "__main__":

    config, args = load_config_with_overrides()

    app_name = config["spark"]["app_name"]
    master = config["spark"].get("master")  # could be None

    spark = create_spark_session(app_name, master)

    try:
        spark.sparkContext.setCheckpointDir("data/checkpoints")

        paths = config["paths"]
        input_path = paths["input_file"]
        raw_path = paths["raw_data_dir"]
        quarantine_path = paths["quarantine_data_dir"]
        processed_path = paths["processed_data_dir"]

        read_opts = config.get("read_options", {})
        header = read_opts.get("header", True)
        infer_schema = read_opts.get("infer_schema", False)
        delimiter = read_opts.get("delimiter", ",")
        multiline = read_opts.get("multiline", False)
        escape = read_opts.get("escape", '"')
        quote = read_opts.get("quote", '"')
        read_file_format = read_opts.get("read_file_format", "csv")

        write_opts = config.get("write_options", {})
        write_file_format = write_opts.get("write_file_format", "parquet")
        mode = write_opts.get("mode", "overwrite")

        # Ingestion
        df_raw = read_materials(spark, input_path, header, quote, escape, multiline, infer_schema, read_file_format)

        if df_raw is None:
            print("Data reading failed. Aborting.")
            spark.stop()
            sys.exit(1)
            
        # Cleansing and validation
        df_clean = clean_and_validate_data(df_raw, quarantine_path, 0.9)

        # Checkpoint: solo se ejecuta si no existe
        if df_clean is not None and config["checkpointing"].get("use_checkpoint", False):
            df_clean = load_or_checkpoint(df_clean, config["checkpointing"]["checkpoint_path"])

        if df_clean is None:
            print("Data cleaning failed. Aborting.")
            spark.stop()
            sys.exit(1)
        else:
            clean_count = df_clean.count()
            write_data(df_clean, raw_path, write_file_format, mode)
            print(f"{clean_count} cleaned rows sent to raw folder.")

        # Aggregations
        df_category_stats = category_aggregations(df_clean)
        #df_column_stats = column_profiling(df_clean)

        if df_category_stats is None:
            print("Data aggregation failed. Aborting.")
            spark.stop()
            sys.exit(1)

        # Storage
        write_data(df_category_stats, os.path.join(processed_path, "category_stats"), write_file_format, mode)
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
