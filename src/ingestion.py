from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField
from src.utils import load_config

def create_spark_session(app_name):
    config = load_config()
    return SparkSession.builder \
        .appName(app_name) \
        .master(config["spark"]["master"]) \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .config("spark.ui.enabled", "false") \
        .config("spark.eventLog.enabled", "false") \
        .getOrCreate()

def get_schema():
    return StructType([
        StructField("Unnamed: 0", StringType(), True),
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("long_description", StringType(), True),
        StructField("customer_part_id", StringType(), True),
        StructField("manufacturer_name", StringType(), True),
        StructField("manufacturer_part_id", StringType(), True),
        StructField("competitor_name", StringType(), True),
        StructField("competitor_part_name", StringType(), True),
        StructField("competitor_part_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_of_measure", StringType(), True),
        StructField("unit_quantity", StringType(), True),
        StructField("requested_quantity", StringType(), True),
        StructField("requested_unit_price", StringType(), True)
    ])

def read_materials(spark, input_path, file_format, header, quote, escape, multiline):
    schema = get_schema()
    try:
        df = spark.read.format(file_format) \
            .option("header", header) \
            .option("quote", quote) \
            .option("escape", escape) \
            .option("multiline", multiline) \
            .schema(schema) \
            .load(input_path)
        print("File loaded successfully.")
        return df
    except Exception as e:
        print(f"Ingestion process failed: {e}")
        return None