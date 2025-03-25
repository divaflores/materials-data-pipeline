from pyspark.sql.functions import col, trim, lower, regexp_replace, when, isnan
from pyspark.sql import DataFrame

def clean_and_validate_data(df: DataFrame, quarantine_path: str) -> DataFrame:
    # 1. Limpiar espacios, normalizar texto
    text_columns = ["name", "description", "manufacturer_name", "competitor_name", "category"]
    for col_name in text_columns:
        df = df.withColumn(col_name, trim(lower(col(col_name))))
        df = df.withColumn(col_name, regexp_replace(col(col_name), "[^a-zA-Z0-9 ]", ""))

    # 2. Validar campos obligatorios (por ejemplo: id, name, category no deben ser nulos)
    invalid_rows = df.filter(
        col("id").isNull() |
        col("name").isNull() |
        col("category").isNull() |
        isnan("id")
    )

    # 3. Guardar filas inválidas en cuarentena
    if invalid_rows.count() > 0:
        invalid_rows.write.mode("overwrite").parquet(quarantine_path)
        print(f"{invalid_rows.count()} rows sent to quarantine.")
    
    # 4. Filtrar solo las filas válidas
    valid_rows = df.subtract(invalid_rows)
    return valid_rows