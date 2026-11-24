# Databricks notebook source
import json

from pyspark.sql import functions as F

# Registra la ejecución del job y evita destinos externos al proyecto.
dbutils.widgets.text("catalog", "electrocasa_dev")
dbutils.widgets.text("run_id", "")
catalog = dbutils.widgets.get("catalog")
run_id = dbutils.widgets.get("run_id")
if catalog not in {"electrocasa_dev", "electrocasa_prod"} or not run_id.isdigit():
    raise ValueError("Se requiere un catálogo del proyecto y un run_id numérico.")

# Guarda el estado de la cuarentena una vez por corrida, incluso en reintentos.
spark.sql(f"""
    MERGE INTO {catalog}.silver.cuarentena_historial h
    USING (
        SELECT {run_id} run_id, registro_id, fuente, _lote lote, _archivo archivo, regla,
               current_timestamp() procesado_en
        FROM {catalog}.silver.cuarentena
    ) q
    ON h.run_id = q.run_id AND h.registro_id = q.registro_id AND h.fuente = q.fuente AND h.regla = q.regla
    WHEN NOT MATCHED THEN INSERT *
""")

# Muestra conteos por capa, incluyendo sus tablas de control.
counts = []
gold_fingerprints = {}
for layer in ("bronze", "silver", "gold"):
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{layer}").where("NOT isTemporary").collect()
    for row in tables:
        name = row.tableName
        dataset = spark.table(f"{catalog}.{layer}.`{name}`")
        if layer == "gold":
            # Resume el contenido completo de Gold de forma estable y sin exportar filas.
            row_hash = F.sha2(F.to_json(F.struct("*"), {"ignoreNullFields": "false"}), 256)
            summary = dataset.agg(
                F.count("*").alias("filas"),
                F.sha2(F.concat_ws("", F.sort_array(F.collect_list(row_hash))), 256).alias("huella"),
            ).first()
            count = summary.filas
            gold_fingerprints[name] = summary.huella
        else:
            count = dataset.count()
        counts.append((layer, name, count))
display(spark.createDataFrame(sorted(counts), "capa string, tabla string, filas long"))

# Separa filas rechazadas, reglas y advertencias para el informe.
quarantine = spark.table(f"{catalog}.silver.cuarentena")
rejections = quarantine.groupBy("fuente").agg(F.countDistinct("registro_id").alias("filas_rechazadas"),
                                              F.count("*").alias("violaciones")).orderBy("fuente")
display(rejections)
display(spark.table(f"{catalog}.silver.advertencias").groupBy("fuente", "regla").count())
tracking_history = (spark.sql(f"DESCRIBE HISTORY {catalog}.bronze.tracking_envios")
                    .select("version", "timestamp", "operation", "operationMetrics").limit(5))
display(tracking_history)

# Comprueba las siete salidas registradas, incluida la demostración de máscara.
expected = {
    "ventas_netas", "ranking_productos", "dotacion_mensual", "ventas_por_vendedor",
    "resenas_negativas", "despachos_por_courier", "empleados_actuales",
}
actual = {name for layer, name, count in counts if layer == "gold" and count > 0}
if missing := expected - actual:
    raise RuntimeError(f"Tablas Gold ausentes o vacías: {sorted(missing)}")

# Devuelve los conteos agregados a la API de Jobs para la evidencia de cada lote.
dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "conteos": [{"capa": layer, "tabla": name, "filas": count} for layer, name, count in sorted(counts)],
    "rechazos": [row.asDict() for row in rejections.collect()],
    "huellas_gold": gold_fingerprints,
    "historial_tracking": [row.asDict(recursive=True) for row in tracking_history.collect()],
}, ensure_ascii=False, default=str))
