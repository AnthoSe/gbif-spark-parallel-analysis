# GBIF Spark Parallel Analysis

Academic project for the Parallel Computing course.

## Description
This project implements a distributed data processing pipeline using Apache Spark (PySpark) over the GBIF Ecuador dataset.  
The goal is to demonstrate real parallel execution, data cleaning, aggregation metrics, and basic anomaly detection.

## Dataset
- Source: GBIF (Global Biodiversity Information Facility)
- File: `gbif_ecuador.tsv`
- Size: > 1 million records
- Format: TSV

## Technologies
- Apache Spark 4.1.1
- PySpark
- Python 3.11
- Java 17
- Windows (local pseudo-distributed mode)

## Project Structure
- `src/`: Spark pipeline modules
- `data/`: Input dataset
- `output/`: Aggregated results
- `screenshots/`: Spark UI evidences (Jobs, Stages, DAG)

## How to Run
```bash
python src/main.py
# gbif-spark-parallel-analysis
