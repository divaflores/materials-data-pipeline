from pyspark.sql.functions import col, trim, regexp_replace, when, initcap, count, lit
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import DataFrame
from functools import reduce

def clean_and_validate_data(df: DataFrame, quarantine_path: str) -> DataFrame:
    try:
        # Identify rows with NaN values
        number_columns = ["id", "unit_quantity", "requested_quantity", "requested_unit_price"]
        nan_condition = [~col("id").rlike(r"^[+-]?\d+\.0$") for c in number_columns]
        only_nan_rows = df.filter(reduce(lambda a, b: a & b, nan_condition))

        # Identify all columns null
        null_condition = [col(c).isNull() for c in df.columns]
        only_null_rows = df.filter(reduce(lambda a, b: a & b, null_condition))

        # Clean spaces, normalize text
        text_columns = ["name", "description", "manufacturer_name", "competitor_name", "category"]
        for col_name in text_columns:
            df = df.withColumn(col_name, initcap(col(col_name)))
            df = df.withColumn(col_name, trim(regexp_replace(col(col_name), r"\s+", " ")))

        # Validate columns that can't be null
        invalid_rows = df.filter(
            col("id").isNull() |
            col("name").isNull() |
            col("category").isNull()
        )

        discarded_rows = only_nan_rows.union(only_null_rows)
        discarded_rows = discarded_rows.union(invalid_rows).distinct()
        discarded_rows = discarded_rows.repartition(1) 

        #  Save discarded rows in quarantine
        discarded_count = discarded_rows.count()
        if discarded_count > 0:
            discarded_rows.write.mode("overwrite").parquet(quarantine_path)
            print(f"{discarded_count} rows sent to quarantine.")
        
        # Filter valid rows
        valid_rows = df.subtract(discarded_rows)

        # Cast to integer
        valid_rows = valid_rows.withColumn("id", col("id").cast(IntegerType()))

        # Cast to double
        double_columns = ["unit_quantity", "requested_quantity", "requested_unit_price"]
        for c in double_columns:
            valid_rows = valid_rows.withColumn(c, col(c).cast(DoubleType()))
       
        # Drop columns > 90% null
        total_rows = valid_rows.count()
        threshold = 0.9

        null_counts = valid_rows.select([
            (count(when(col(c).isNull(), c)) / lit(total_rows)).alias(c)
            for c in valid_rows.columns
        ])
        null_ratios = null_counts.first().asDict()
        columns_to_keep = [c for c, ratio in null_ratios.items() if ratio < threshold]
        df_clean = valid_rows.select(columns_to_keep)

        df_clean = df_clean.drop("Unnamed: 0")

        return df_clean
    
    except Exception as e:
        print(f"Cleaning process failed: {e}")
        return None