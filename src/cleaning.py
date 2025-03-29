from pyspark.sql.functions import col, trim, regexp_replace, when, initcap, count, lit
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import DataFrame
from functools import reduce

def clean_and_validate_data(df: DataFrame, quarantine_path: str, null_threshold: float) -> DataFrame:
    try:
        # Clean spaces, normalize text
        text_columns = ["name", "description", "manufacturer_name", "competitor_name", "category"]
        for col_name in text_columns:
            cleaned_col = col_name + "_cleaned"
            df = df.withColumn(cleaned_col, initcap(col(col_name)))
            df = df.withColumn(cleaned_col, trim(regexp_replace(col(col_name), r"\s+", " ")))

        # Cast id to integer
        df = df.withColumn("id_cleaned", col("id").cast(IntegerType()))

        # Cast to double
        double_columns = ["unit_quantity", "requested_quantity", "requested_unit_price"]
        for c in double_columns:
            cleaned_col = c + "_cleaned"
            df = df.withColumn(cleaned_col, col(c).cast(DoubleType()))

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
        columns_to_keep = [c for c in df.columns if not c.endswith("_cleaned")]

        discarded_rows = df.select(*columns_to_keep) \
                        .filter(col("is_valid")=="N")        
        discarded_count = discarded_rows.count()
        if discarded_count > 0:
            discarded_rows.write.mode("overwrite").parquet(quarantine_path)
            print(f"{discarded_count} rows sent to quarantine.")
        
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
        valid_rows = df.select(*valid_columns_to_keep) \
                        .filter(col("is_valid")=="Y")

        # Drop columns > 90% null
        total_rows = valid_rows.count()

        null_counts = valid_rows.select([
            (count(when(col(c).isNull(), c)) / lit(total_rows)).alias(c)
            for c in valid_rows.columns
        ])
        null_ratios = null_counts.first().asDict()
        col_to_keep = [c for c, ratio in null_ratios.items() if ratio < null_threshold]

        df_clean = valid_rows.select(col_to_keep)

        return df_clean
    
    except Exception as e:
        print(f"Cleaning process failed: {e}")
        return None