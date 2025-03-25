# Materials Data Engineering Pipeline

This project implements a data engineering pipeline in PySpark to clean, transform, and analyze a CSV file containing material records. It's designed to be scalable, modular, and easily configurable for local or distributed execution (e.g., Azure Databricks).

---

## Objective

Build a robust pipeline that:

- Ingests CSV files (easily extendable to JSON or Parquet)
- Cleans and validates the data
- Applies transformations and creates derived columns
- Performs aggregations and profiling
- Stores processed data in optimized Parquet format
- Is fully configurable via YAML
- Handles invalid rows by sending them to a quarantine folder

```bash
data_pipeline/
├── config/
│   └── pipeline_config.yaml
├── data/
│   ├── materials-2.csv
│   ├── raw/
│   ├── processed/
│   └── quarantine/
├── jobs/
│   └── run_pipeline.py
├── src/
│   ├── ingestion.py
│   ├── cleaning.py
│   ├── transformation.py
│   ├── storage.py
│   └── utils.py
├── requirements.txt
└── README.md
```

## Requirements

- Python 3.8+
- Apache Spark 3.x
- PySpark
- PyYAML

Install dependencies:

pip install -r requirements.txt

## How to Run the Pipeline

python jobs/run_pipeline.py

This will execute all stages of the pipeline:

1. Ingests materials-2.csv

2. Cleans and validates data (invalid rows → data/quarantine/)

3. Applies transformations and creates derived columns

4. Aggregates:

    Category-wise (item count, average/min/max price)

    Column-wise profiling (nulls, uniques, most frequent, etc.)

5. Stores results in data/processed/ as partitioned Parquet files

## Dynamic Configuration

All settings are defined in config/pipeline_config.yaml. You can modify:

- Input/output paths
- Spark job name
- Change local[*] to yarn or a cluster URI
  
## Validation Rules

- Key columns (id, name, category) must not be null.
- Text fields are normalized: lowercase, no special characters, trimmed.
- Invalid rows are written to the quarantine/ folder.

## Pipeline Outputs

processed/enriched_data/    Cleaned data with derived total_price
processed/category_stats/   Aggregated stats by category
processed/column_profile/   Column-wise profiling
quarantine/                 Invalid or rejected rows

## Assumptions & Decisions

- requested_quantity × requested_unit_price is used to compute total_requested_price.
- A row is "invalid" if any of id, name, or category is missing.
- An explicit schema is used for robustness, not inferred.

## Author

Diva Flores
<divaf09@gmail.com>
