# src/cleaning.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, split, trim, when, lit, length, concat
from config import Config

def pick_existing_col(df: DataFrame, candidates: list[str]) -> str:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    raise ValueError(f"No encontré ninguna columna entre: {candidates}")

def clean_and_validate(df: DataFrame, cfg: Config) -> tuple[DataFrame, DataFrame, dict]:
    """
    Retorna:
    - df_ok: registros válidos
    - df_bad: registros inválidos (se usa como 'fallidas' del proyecto)
    - meta: columnas detectadas
    """

    # Columnas típicas GBIF
    col_region = pick_existing_col(df, ["stateProvince", "province", "provincia"])
    col_entity = pick_existing_col(df, ["scientificName", "species", "taxon"])
    col_date   = pick_existing_col(df, ["eventDate", "date", "fecha"])

    # Normaliza fecha (soporta YYYY, YYYY-MM, YYYY-MM-DD y rangos con "/")
    df2 = (
        df.withColumn("event_date_raw", trim(split(col(col_date), "/").getItem(0)))
          .withColumn(
              "event_date_norm",
              when(length(col("event_date_raw")) == 10, col("event_date_raw"))
              .when(length(col("event_date_raw")) == 7,  concat(col("event_date_raw"), lit("-01")))
              .when(length(col("event_date_raw")) == 4,  concat(col("event_date_raw"), lit("-01-01")))
              .otherwise(lit(None))
          )
          .withColumn("event_date", to_date(col("event_date_norm")))
    )

    # --- Reglas (configurables) ---
    # Región
    has_region = col(col_region).isNotNull()

    # Entidad
    has_entity = col(col_entity).isNotNull() if cfg.require_entity else lit(True)

    # País (en tu TSV tienes countryCode=EC, no "country")
    # Entonces filtramos por countryCode si existe; si no existe, no aplica.
    if cfg.require_country_ec and "countryCode" in df2.columns:
        has_country = (col("countryCode") == lit("EC"))
    else:
        has_country = lit(True)

    # Coordenadas
    if cfg.require_coords and ("decimalLatitude" in df2.columns) and ("decimalLongitude" in df2.columns):
        has_coords = col("decimalLatitude").isNotNull() & col("decimalLongitude").isNotNull()
    else:
        has_coords = lit(True)

    # Fecha obligatoria o no
    has_date = col("event_date").isNotNull() if cfg.require_eventdate else lit(True)

    # Flag de validez
    is_valid_expr = has_region & has_entity & has_country & has_coords & has_date

    df2 = df2.withColumn("is_valid", when(is_valid_expr, lit(True)).otherwise(lit(False)))

    # Razón (simple, útil para justificar en informe)
    df2 = df2.withColumn(
        "invalid_reason",
        when(~has_region, lit("MISSING_REGION"))
        .when(cfg.require_entity & (~has_entity), lit("MISSING_ENTITY"))
        .when(cfg.require_country_ec & (~has_country), lit("NOT_EC"))
        .when(cfg.require_coords & (~has_coords), lit("MISSING_COORDS"))
        .when(cfg.require_eventdate & (~has_date), lit("BAD_DATE"))
        .otherwise(lit(None))
    )

    df_ok  = df2.filter(col("is_valid") == True)
    df_bad = df2.filter(col("is_valid") == False)

    meta = {
        "region_col": col_region,
        "entity_col": col_entity,
        "date_col": col_date,
        "date_norm_col": "event_date"
    }
    return df_ok, df_bad, meta
