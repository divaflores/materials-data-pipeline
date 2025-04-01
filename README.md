# Materials Data Engineering Pipeline

This project implements a data engineering pipeline in PySpark to clean, transform, and analyze a CSV file containing material records. It's designed to be scalable, modular, and easily configurable for local or distributed execution.

---

## Objective

Build a robust pipeline that:

- Ingests CSV files (easily extendable to JSON or Parquet)
- Cleans and validates the data
- Applies transformations
- Performs aggregations
- Stores processed data in optimized Parquet format
- Is configurable via YAML
- Handles invalid rows by sending them to a quarantine folder

```bash
data_pipeline/
├── config/
│   └── config.yaml
├── data/
│   ├── materials-2.csv
│   ├── checkpoints/
│   ├── logs/
│   ├── processed/
│   └── quarantine/
│   ├── raw/
├── jobs/
│   └── main.py
├── src/
│   ├── cleaning.py
│   ├── ingestion.py
│   ├── storage.py
│   ├── transformation.py
│   └── utils.py
├── .gitignore
├── Dockerfile
├── README.md
└── requirements.txt
```

## Requirements

- Python 3.8+
- Apache Spark 3.x
- PySpark
- PyYAML

Install dependencies:

pip install -r requirements.txt

## How to Run the Pipeline

python jobs/main.py

This will execute all stages of the pipeline:

1. Ingests materials-2.csv

2. Cleans and validates data (invalid rows → data/quarantine/)

3. Applies transformations

4. Aggregates:

    Category-wise (item count, average/min/max price)

5. Stores results in data/processed/ as partitioned/non-partitioned Parquet files

## Dynamic Configuration

All settings are defined in config/config.yaml. You can modify:

- Input/output paths
- Spark job name
- Change local[*] to yarn or a cluster URI
  
## Validation Rules

- Key columns (id, name, category) must not be null.
- Text fields are normalized: lowercase, no special characters, trimmed.
- Invalid rows are written to the quarantine/ folder.

## Pipeline Outputs

data/raw/               Clean data
data/processed/stats/   Aggregated stats
data/quarantine/        Invalid or rejected rows

## Assumptions & Decisions

- A row is "invalid" if any of id, name, or category is missing.
- An explicit schema is used for robustness, not inferred.

## Author

Diva Flores
<divaf09@gmail.com>
