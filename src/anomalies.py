# src/anomalies.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, avg, stddev_samp, desc,
    year, month, regexp_extract, when
)

def monthly_counts(df_ok: DataFrame) -> DataFrame:
    """
    Conteo mensual de registros válidos.
    - Usa event_date (date) cuando está parseada (YYYY-MM-DD).
    - Si event_date es NULL pero event_date_raw viene como YYYY-MM, extrae año/mes de ahí.
    Requiere que df_ok venga de cleaning.py (con event_date_raw y event_date).
    """

    # Extrae YYYY y MM solo si el raw cumple YYYY-MM (exactamente)
    raw_year = regexp_extract(col("event_date_raw"), r"^(\d{4})-(\d{2})$", 1)
    raw_month = regexp_extract(col("event_date_raw"), r"^(\d{4})-(\d{2})$", 2)

    dfm = (
        df_ok
        .withColumn(
            "y",
            when(col("event_date").isNotNull(), year(col("event_date")))
            .when(raw_year != "", raw_year.cast("int"))
        )
        .withColumn(
            "m",
            when(col("event_date").isNotNull(), month(col("event_date")))
            .when(raw_month != "", raw_month.cast("int"))
        )
        .filter(col("y").isNotNull() & col("m").isNotNull())
    )

    return (
        dfm.groupBy("y", "m")
           .agg(count("*").alias("total"))
           .orderBy("y", "m")
    )

def detect_spikes_mean_std(df_month: DataFrame, k: float = 2.0) -> DataFrame:
    """
    Spike si total >= mu + k*sigma (sobre la serie mensual global).
    """
    stats = df_month.agg(
        avg(col("total")).alias("mu"),
        stddev_samp(col("total")).alias("sigma")
    ).collect()[0]

    mu = stats["mu"]
    sigma = stats["sigma"]

    # Si sigma es None (muy pocos datos) o mu es None, devuelve vacío
    if sigma is None or mu is None:
       print(f"[WARN] No se pudo calcular estadísticas: mu={mu}, sigma={sigma}")
       return df_month.limit(0)

    threshold = mu + k * sigma

    return (
        df_month.filter(col("total") >= threshold)
                .orderBy(desc("total"))
    )
