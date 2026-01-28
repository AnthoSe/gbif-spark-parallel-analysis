# src/main.py
import time
from config import Config
from spark_session import build_spark
from io_utils import read_gbif, write_df
from cleaning import clean_and_validate
from metrics import total_by_region, top10_entities, failed_pct_by_region
from anomalies import monthly_counts, detect_spikes_mean_std


def _timed(label: str, fn):
    t0 = time.perf_counter()
    out = fn()
    t1 = time.perf_counter()
    print(f"[TIME] {label}: {t1 - t0:.2f}s")
    return out


def main():
    cfg = Config()
    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel(cfg.log_level)

    # 1) Carga distribuida
    df_raw = _timed("read_gbif", lambda: read_gbif(spark, cfg))

    # Muestra rápida (sin reventar consola)
    print(">>> Columnas:", df_raw.columns)
    df_raw.show(cfg.sample_show_rows, truncate=False)

    # 2) Limpieza/validación (ahora pasa cfg)
    df_ok, df_bad, meta = clean_and_validate(df_raw, cfg)

    region_col = meta["region_col"]
    entity_col = meta["entity_col"]

    # Repartition opcional (experimento Avance 3)
    if cfg.repartition_n:
        df_ok = df_ok.repartition(cfg.repartition_n)
        df_bad = df_bad.repartition(cfg.repartition_n)

    # Escalado opcional (stress test)
    if cfg.scale_factor and cfg.scale_factor > 1:
        df_ok_scaled = df_ok
        for _ in range(cfg.scale_factor - 1):
            df_ok_scaled = df_ok_scaled.unionByName(df_ok)
        df_ok = df_ok_scaled

    # Acciones para evidenciar Jobs/Stages
    if cfg.do_counts:
        print("RAW:", _timed("count RAW", lambda: df_raw.count()))
        print("OK :", _timed("count OK",  lambda: df_ok.count()))
        print("BAD:", _timed("count BAD", lambda: df_bad.count()))

    # 3) Métricas principales (coherentes con tu proyecto)
    df_total_region = _timed("metric total_by_region", lambda: total_by_region(df_ok, region_col))
    df_top10 = _timed("metric top10_entities", lambda: top10_entities(df_ok, entity_col))
    df_failed_pct = _timed("metric failed_pct_by_region", lambda: failed_pct_by_region(df_bad, df_raw, region_col))

    # 4) Anomalías básicas (meses con picos)
    df_month = _timed("monthly_counts", lambda: monthly_counts(df_ok))
    df_spikes = _timed("detect_spikes_mean_std", lambda: detect_spikes_mean_std(df_month, k=cfg.spike_k))

    # Prints de evidencia
    df_total_region.show(20, truncate=False)
    df_top10.show(10, truncate=False)
    df_failed_pct.show(20, truncate=False)
    df_spikes.show(50, truncate=False)

    # 5) Persistencia
    _timed("write 01_total_por_region", lambda: write_df(df_total_region, cfg, "01_total_por_region"))
    _timed("write 02_top10_entidades", lambda: write_df(df_top10, cfg, "02_top10_entidades"))
    _timed("write 03_pct_invalidas_por_region", lambda: write_df(df_failed_pct, cfg, "03_pct_invalidas_por_region"))
    _timed("write 04_conteo_mensual", lambda: write_df(df_month, cfg, "04_conteo_mensual"))
    _timed("write 05_spikes_mensuales", lambda: write_df(df_spikes, cfg, "05_spikes_mensuales"))

    spark.stop()


if __name__ == "__main__":
    main()
