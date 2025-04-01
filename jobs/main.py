import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
from src.ingestion import create_spark_session, read_materials
from src.cleaning import clean_and_validate_data
from src.transformation import aggregations
from src.storage import write_data
from src.utils import load_or_checkpoint, load_config_with_overrides, setup_logger, write_to_postgres
from time import time

if __name__ == "__main__":
    start_pipeline = time()

    try:
        config, args = load_config_with_overrides()
        logger = setup_logger(config["logs"]["log_name"], config["logs"]["log_file"])
        pg_conf = config["postgres"]
        jdbc_url = f"jdbc:postgresql://{pg_conf['host']}:{pg_conf['port']}/{pg_conf['dbname']}"

        logger.info("-------------------------Pipeline started-------------------------")
        
        app_name = config["spark"]["app_name"]
        master = config["spark"].get("master")  # could be None

        spark = create_spark_session(app_name, master)
        logger.info("Spark session created.")

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
            logger.error("Failed to read input data.")
            spark.stop()
            sys.exit(1)
        logger.info(f"Input data loaded: {df_raw.count()} rows.")
            
        # Cleansing and validation
        df_clean = clean_and_validate_data(df_raw, quarantine_path, 0.9)

        # Checkpoint: solo se ejecuta si no existe
        if df_clean is not None and config["checkpointing"].get("use_checkpoint", False):
            df_clean = load_or_checkpoint(df_clean, config["checkpointing"]["checkpoint_path"])
            logger.info("Checkpoint loaded or created.")

        if df_clean is None:
            logger.error("Data cleaning failed.")
            spark.stop()
            sys.exit(1)
        else:
            clean_count = df_clean.count()
            write_data(df_clean, raw_path, write_file_format, mode)     
            # Write to PosgreSQL                  
            # write_to_postgres(
            #     df_clean,
            #     table_name=pg_conf["table"],
            #     jdbc_url=jdbc_url,
            #     properties={
            #         "user": pg_conf["user"],
            #         "password": pg_conf["password"],
            #         "driver": "org.postgresql.Driver"
            #     }
            # )
            #logger.info(f"Data store in PosgreSQL database.")
            logger.info(f"Data cleaned: {df_clean.count()} rows.")            

        # Aggregations
        df_aggregations = aggregations(df_clean)

        if df_aggregations is None:
            logger.error("Aggregation failed.")
            spark.stop()
            sys.exit(1)
        else:
            logger.info("Category stats calculated.")
            write_data(df = df_aggregations, 
                output_path = os.path.join(processed_path, "stats"), 
                file_format = write_file_format, 
                mode = mode,
                partition_by = "category_cleaned")

    except KeyboardInterrupt:
        logger.exception("Pipeline interrupted by user.")
    except Exception as e:
        logger.exception(f"Error during pipeline execution: {e}")
    finally:
        try:
            if spark:
                spark.stop()
                logger.info(f"Spark stopped.")
        except Exception as e:
            logger.exception(f"Error stopping Spark: {e}")
        finally:
            import os
            logger.info(f"----------------Pipeline finished in {round(time() - start_pipeline, 2)} seconds----------------")
            os._exit(0)  
