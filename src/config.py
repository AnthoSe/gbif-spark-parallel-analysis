# src/config.py
from dataclasses import dataclass

@dataclass
class Config:
    # ===== App / ejecución =====
    app_name: str = "GBIF-Spark-Experiment"
    master: str = "local[*]"                 # local/pseudo-distribuido
    log_level: str = "WARN"                  # INFO si quieres más detalle

    # ===== Dataset =====
    input_path: str = "data/gbif_ecuador.tsv"  # actualiza al nombre real si cambió
    input_format: str = "tsv"                  # tsv => sep '\t'
    has_header: bool = True
    infer_schema: bool = True

    # ===== Paralelismo / rendimiento =====
    shuffle_partitions: int = 200           # spark.sql.shuffle.partitions
    repartition_n: int | None = None        # ej: 200/400; None = no repartition
    persist_level: str | None = None        # None / "MEMORY_ONLY" / "MEMORY_AND_DISK"
    adaptive_enabled: bool = True           # AQE (útil para joins/shuffle)
    broadcast_threshold: int = -1           # deja -1 para default; o ajusta si haces joins
    single_file_csv: bool = False

    # ===== Experimentos de escala =====
    scale_factor: int = 1                   # 1=normal, 2=duplica, 3=triplica (union)
    sample_show_rows: int = 5               # para show() sin reventar consola
    do_counts: bool = True                  # count() fuerza jobs (evidencia Spark UI)

    # ===== “Logs transaccionales” (criterio inválidos/fallidos) =====
    # En GBIF, definimos "BAD" como registros inválidos por calidad mínima.
    # Ajusta si tu cleaning.py usa otro criterio.
    require_country_ec: bool = True
    require_coords: bool = True
    require_entity: bool = True             # species/scientificName not null
    require_eventdate: bool = False         # opcional (si quieres forzar temporal)

    # ===== Salida =====
    output_base: str = "output"
    output_format: str = "csv"              # "csv" o "parquet"
    parquet_compression: str = "snappy"     # si output_format="parquet"
    write_mode: str = "overwrite"           # overwrite / append

    # ===== Anomalías =====
    spike_k: float = 2.0                    # mean + k*std
    month_col_source: str = "event_date"     # columna base para mes/año
