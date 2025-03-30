from pyspark.sql.functions import col, trim, regexp_replace, when, initcap, count, lit
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import DataFrame
from functools import reduce
import json
from datetime import datetime
from src.utils import load_config_with_overrides
from src.storage import write_data

def clean_and_validate_data(df: DataFrame, quarantine_path: str, null_threshold: float, include_metadata: bool = False) -> DataFrame:
    try:
        config, args = load_config_with_overrides()
        cleaning_log_path = config["paths"]["cleaning_log_path"]
        mode = config["write_options"]["mode"]
        write_file_format = config["write_options"]["write_file_format"]
        total_count = df.count()

        # Clean spaces, normalize text
        text_columns = ["name", "description", "manufacturer_name", "competitor_name", "category"]
        for col_name in text_columns:
            df = df.withColumn(
                f"{col_name}_cleaned",
                trim(regexp_replace(initcap(col(col_name)), r"\s+", " "))
            )

        # Cast id to integer
        df = df.withColumn("id_cleaned", col("id").cast(IntegerType()))

        # Cast to double
        double_columns = ["unit_quantity", "requested_quantity", "requested_unit_price"]
        for c in double_columns:
            df = df.withColumn(f"{c}_cleaned", col(c).cast(DoubleType()))

        # Identify rows with NaN values
        #number_columns = ["id_cleaned", "unit_quantity_cleaned", "requested_quantity_cleaned", "requested_unit_price_cleaned"]
        #invalid_number_condition = reduce(lambda a, b: a | b, [~col(c).rlike(r"^\d+\.0$") for c in number_columns])

        # Identify all columns null
        all_null_condition = reduce(lambda a, b: a & b, [col(c).isNull() for c in df.columns])

        # Validate columns that can't be null
        required_fields = ["id_cleaned", "name_cleaned", "category_cleaned"]
        required_col_condition = reduce(lambda a, b: a | b, [col(c).isNull() for c in required_fields])

        df = df.withColumn(
            "is_valid",
            when(all_null_condition | required_col_condition, "N").otherwise("Y")
        )

        #  Save discarded rows in quarantine
        discarded_rows = df.filter(col("is_valid") == "N").cache()
        discarded_count = discarded_rows.count()

        if discarded_count > 0:
            columns_to_keep = [c for c in df.columns if not c.endswith("_cleaned")]
            write_data(discarded_rows.select(*columns_to_keep), quarantine_path, write_file_format, mode)
            print(f"{discarded_count} invalid rows sent to quarantine.")
        
        discarded_rows.unpersist()

        # Filter valid rows
        valid_columns_to_keep = ["id_cleaned", 
                                 "name_cleaned", 
                                 "description_cleaned",
                                 "long_description",
                                 "customer_part_id",
                                 "manufacturer_name_cleaned",
                                 "manufacturer_part_id",
                                 "competitor_name_cleaned",
                                 "competitor_part_name",
                                 "competitor_part_id",
                                 "category_cleaned",
                                 "unit_of_measure",
                                 "unit_quantity_cleaned",
                                 "requested_quantity_cleaned",
                                 "requested_unit_price_cleaned",
                                 "is_valid"]
        valid_rows = df.select(*valid_columns_to_keep).filter(col("is_valid")=="Y")

        # Drop columns with lost of nulls
        valid_count = valid_rows.count()
        if valid_count == 0:
            print("No valid rows after cleansing")
            return None

        null_counts = valid_rows.select([
            (count(when(col(c).isNull(), c)) / lit(valid_count)).alias(c)
            for c in valid_rows.columns
        ])
        null_ratios = null_counts.first().asDict()
        col_to_keep = [c for c, ratio in null_ratios.items() if ratio is not None and ratio < null_threshold]

        removed_cols = [c for c in valid_rows.columns if c not in col_to_keep]

        if not include_metadata:
            col_to_keep = [c for c in col_to_keep if c != "is_valid"]

        df_clean = valid_rows.select(col_to_keep)

        summary = {
            "total_rows": total_count,
            "valid_rows": valid_count,
            "invalid_rows": discarded_count,
            "null_threshold": null_threshold,
            "columns_removed_due_to_nulls": removed_cols,
            "columns_retained": col_to_keep
        }

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"cleaning_log_{timestamp}.json"
        with open(f"{cleaning_log_path}/{file_name}", "w") as f:
            json.dump(summary, f, indent=4)
        print(f"Cleaning summary saved to: {cleaning_log_path}")

        return df_clean
    
    except Exception as e:
        print(f"Cleaning process failed: {e}")
        return None