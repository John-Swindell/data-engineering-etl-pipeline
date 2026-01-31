# Data Quality Gate

## 1. Objective
This script acts as a firewall between raw data ingestion (Bronze Layer) and analytical datasets (Silver Layer). It ensures that all data entering the modeling pipeline meets strict structural and logical integrity standards.

It is designed with a **Fail-Fast** philosophy: it is better to halt the pipeline and alert an engineer than to silently propagate corrupted data into production models.

## 2. Methodology
The pipeline applies three distinct strategies to handle data quality:

### A. Validation (The "Fail" Strategy)
Checks critical structural requirements. If these are not met, the pipeline fails immediately.
* **Schema Check**: Ensures all required columns (e.g., `canonical_id`, `close`, `date`) are present.

### B. Standardization (The "Fix" Strategy)
Corrects minor data inconsistencies to ensure uniformity.
* **Precision**: Rounds all pricing data to a standard 16 decimal places to prevent floating-point drift during backtesting.

### C. Sanitation (The "Filter" Strategy)
Identifies and removes individual bad records without stopping the entire process, provided the volume of bad data is low.
* **OHLC Integrity**: Removes rows where the `Close` price is mathematically impossible given the `High` and `Low` for that day (e.g., Close > High).

## 3. The Circuit Breaker
To prevent "silent failures" where a filter removes too much data (e.g., a bad API response causes 90% of rows to be dropped), the script implements a **Data Loss Threshold**.

* **Default Threshold**: 5.0%
* **Behavior**: If the sanitation steps remove more than 5% of the total dataset, the script triggers a generic SystemExit(1). This protects downstream models from training on sparse or non-representative data.

## 4. Usage

This script is typically invoked by an orchestrator (e.g., Airflow, shell script) after the ingestion phase.

**Command Line Arguments:**
```bash
python data_quality_gate.py \
  --input_file "data/bronze/historical_data.parquet" \
  --output_file "data/silver/historical_data_clean.parquet"

```

**Optional Override:**
You can adjust the circuit breaker sensitivity for specific runs:

```bash
python data_quality_gate.py ... --threshold 10.0

```