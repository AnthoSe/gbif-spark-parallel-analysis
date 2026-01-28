# src/io_utils.py
from pyspark.sql import DataFrame, SparkSession
from config import Config
import os

def read_gbif(spark: SparkSession, cfg: Config) -> DataFrame:
    if cfg.input_format.lower() == "tsv":
        df = (
            spark.read
            .option("header", str(cfg.has_header).lower())
            .option("sep", "\t")
            .option("inferSchema", str(cfg.infer_schema).lower())
            .csv(cfg.input_path)
        )
        return df

    raise ValueError("Formato no soportado. Usa TSV en este proyecto.")

def write_df(df: DataFrame, cfg: Config, name: str) -> str:
    out_path = os.path.join(cfg.output_base, name)

    if cfg.output_format.lower() == "csv":
        writer = df
        if cfg.single_file_csv:
            writer = writer.coalesce(1)

        (writer.write
              .mode(cfg.write_mode)
              .option("header", "true")
              .csv(out_path))

    elif cfg.output_format.lower() == "parquet":
        (df.write
           .mode(cfg.write_mode)
           .option("compression", cfg.parquet_compression)
           .parquet(out_path))
    else:
        raise ValueError("Formato de salida no soportado (csv/parquet).")

    return out_path
