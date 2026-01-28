# src/spark_session.py
from pyspark.sql import SparkSession
from config import Config


def build_spark(cfg: Config) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(cfg.app_name)
        .master(cfg.master)
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(cfg.adaptive_enabled).lower())
    )

    # Construir sesión
    spark = builder.getOrCreate()

    # Nivel de logs (para que no se ensucie la evidencia)
    spark.sparkContext.setLogLevel(cfg.log_level)

    # Mensaje explícito para evidencia
    print(">>> Spark Session creada")
    print(f">>> master = {cfg.master}")
    print(f">>> shuffle_partitions = {cfg.shuffle_partitions}")
    print(f">>> AQE enabled = {cfg.adaptive_enabled}")

    return spark
