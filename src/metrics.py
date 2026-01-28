# src/metrics.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, desc, when, lit, round as sround

def _region_bucket(region_col: str):
    """Normaliza región: NULL => 'SIN_REGION'."""
    return when(col(region_col).isNull(), lit("SIN_REGION")).otherwise(col(region_col))

def total_by_region(df_ok: DataFrame, region_col: str) -> DataFrame:
    return (
        df_ok.groupBy(_region_bucket(region_col).alias("region"))
             .agg(count("*").alias("total_registros"))
             .orderBy(desc("total_registros"))
    )

def top10_entities(df_ok: DataFrame, entity_col: str) -> DataFrame:
    return (
        df_ok.groupBy(col(entity_col).alias("entidad"))
             .agg(count("*").alias("total"))
             .orderBy(desc("total"))
             .limit(10)
    )

def failed_pct_by_region(df_bad: DataFrame, df_all: DataFrame, region_col: str) -> DataFrame:
    """
    En este proyecto, "fallidas" = registros inválidos (df_bad).
    Se calcula % inválidas por región.
    Importante: NULL región se agrupa como 'SIN_REGION' para consistencia.
    """

    total = (
        df_all.groupBy(_region_bucket(region_col).alias("region"))
              .agg(count("*").alias("total"))
    )

    bad = (
        df_bad.groupBy(_region_bucket(region_col).alias("region"))
              .agg(count("*").alias("invalidas"))
    )

    joined = total.join(bad, on="region", how="left").fillna(0, subset=["invalidas"])

    return (
        joined.select(
            col("region"),
            col("total"),
            col("invalidas"),
            sround((col("invalidas") / col("total")) * 100.0, 4).alias("pct_invalidas")
        )
        .orderBy(desc("pct_invalidas"))
    )
