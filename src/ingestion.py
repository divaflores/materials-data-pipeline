import os
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField
import json
from datetime import datetime
from src.utils import load_config_with_overrides, setup_logger

def create_spark_session(app_name: str, master: str = None) -> SparkSession:
    config, args = load_config_with_overrides()
    logger = setup_logger(config["logs"]["log_name"], config["logs"]["log_file"])
    logger.info(f"Creating Spark session: {app_name}")

    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    # Copy to PosgreSQL
    # spark_temp_dir = os.path.join(tempfile.gettempdir(), "spark_temp_dir")
    # os.makedirs(spark_temp_dir, exist_ok=True)

    # builder = builder.config("spark.jars", config["spark"]["jdbc_jar_path"]) \
    #                 .config("spark.local.dir", spark_temp_dir)
                    
    builder = builder.config("spark.sql.shuffle.partitions", 4) \
                    .config("spark.sql.adaptive.enabled", True) \
                    .config("spark.sql.debug.maxToStringFields", 1000) \
                    .config("spark.ui.enabled", "false") \
                    .config("spark.eventLog.enabled", "false")
        
    return builder.getOrCreate()

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

def validate_schema(df, expected_schema: StructType, report_path: str = None) -> bool:

    config, args = load_config_with_overrides()
    logger = setup_logger(config["logs"]["log_name"], config["logs"]["log_file"])

    report = {
        "status": "PASSED",
        "errors": [],
        "missing_columns": [],
        "type_mismatches": [],
        "unexpected_columns": [],
        "actual_schema": df.schema.jsonValue(),
        "expected_schema": expected_schema.jsonValue(),
        "timestamp": datetime.now().isoformat()
    }

    df_fields = {field.name: field.dataType for field in df.schema.fields}
    expected_fields = {field.name: field.dataType for field in expected_schema.fields}

    # Verify missing columns
    for col_name in expected_fields:
        if col_name not in df_fields:
            report["missing_columns"].append(col_name)
            report["errors"].append(f"Missing column: '{col_name}'")

    # Verify wrong types 
    for col_name in expected_fields:
        if col_name in df_fields and df_fields[col_name] != expected_fields[col_name]:
            report["type_mismatches"].append({
                "column": col_name,
                "expected": str(expected_fields[col_name]),
                "actual": str(df_fields[col_name])
            })
            report["errors"].append(f"Type mismatch for column '{col_name}': expected {expected_fields[col_name]}, got {df_fields[col_name]}")

    # Verify unexpected columns
    for col_name in df_fields:
        if col_name not in expected_fields:
            report["unexpected_columns"].append(col_name)
            report["errors"].append(f"Unexpected column: '{col_name}'")

    if report["errors"]:
        report["status"] = "FAILED"
        logger.exception("Schema validation failed.")
        for err in report["errors"]:
            print("   -", err)
    else:
        logger.info("Schema validation passed.")

    # Save JSON file if path is specified
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"schema_validation_report_{timestamp}.json"
    if report_path:
        with open(f"{report_path}/{file_name}", "w") as f:
            json.dump(report, f, indent=4)
        logger.info(f"Validation report saved to: {report_path}")


    return report["status"] == "PASSED"

def read_materials(spark, input_path, header, quote, escape, multiline, infer_schema, file_format):
    config, args = load_config_with_overrides()
    logger = setup_logger(config["logs"]["log_name"], config["logs"]["log_file"])
    expected_schema = get_schema() if not infer_schema else None
    
    try:
        schema_report_path = config["paths"]["schema_report_path"]

        prev_df = spark.read.format(file_format) \
            .option("header", header) \
            .load(input_path)

        is_valid = validate_schema(prev_df, expected_schema, report_path=schema_report_path)

        df = spark.read.format(file_format) \
            .option("header", header) \
            .option("quote", quote) \
            .option("escape", escape) \
            .option("multiline", multiline)
        
        if infer_schema:
            df = df.option("inferSchema", True)            
        else:
            if is_valid:
                df = df.schema(expected_schema)
            else: 
                logger.exception("Schema validation failed. See report for details.")
                raise ValueError("Schema validation failed. See report for details.")
            
        df = df.load(input_path)   
        logger.info(f"Data read successfully from {input_path}. Row count: {df.count()}")     

        return df
    
    except Exception as e:
        logger.exception(f"Ingestion process failed: {e}")
        return None