# Databricks notebook source
import ast
import json
from pathlib import Path

from pyspark.sql import Window, functions as F

# Ejecuta las funciones del pipeline sobre un caso aislado de corrección,
# sin vincular la prueba a tablas productivas ni a un nuevo pipeline.
dbutils.widgets.text("codigo_pipeline", "")
source_path = Path(dbutils.widgets.get("codigo_pipeline"))
if source_path.name != "pipeline.py" or not source_path.is_file():
    raise ValueError("Indique la ubicación del pipeline publicado por el bundle.")
source = ast.parse(source_path.read_text(encoding="utf-8"))
names = {"base", "latest", "money"}
functions = [node for node in source.body if isinstance(node, ast.FunctionDef) and node.name in names]
if {node.name for node in functions} != names:
    raise ValueError("El pipeline no contiene las funciones esperadas de reproceso.")

# Crea una venta sintética rechazada y una tabla de correcciones vacía.
original = spark.createDataFrame(
    [("venta-prueba", "-10.00", "lote-prueba.csv", "2026-05-31")],
    "_venta_id string, monto_total string, _archivo string, _lote string",
).withColumnRenamed("_venta_id", "venta_id").withColumn("_ingestado_en", F.lit(None).cast("timestamp"))
corrections = spark.createDataFrame([], "fuente string, registro_id string, revision long, payload_json string")
inputs = {("bronze", "ventas"): original, ("silver", "correcciones"): corrections}
scope = {"F": F, "Window": Window, "AUDIT": ("_archivo", "_lote", "_ingestado_en"), "table": lambda *key: inputs[key]}
exec(compile(ast.Module(body=functions, type_ignores=[]), str(source_path), "exec"), scope)


def evaluate():
    # Usa el mismo cálculo de identidad, corrección y monto que Silver.
    return scope["base"]("ventas").withColumn("monto_normalizado", scope["money"]("monto_total"))


before = evaluate().where("monto_normalizado <= 0").collect()
assert len(before) == 1
record_id = before[0].registro_id
inputs["silver", "correcciones"] = spark.createDataFrame(
    [
        ("ventas", record_id, 1, json.dumps({"venta_id": "venta-prueba", "monto_total": "11.00"})),
        ("ventas", record_id, 2, json.dumps({"venta_id": "venta-prueba", "monto_total": "12.00"})),
    ],
    corrections.schema,
)
# Comprueba que la revisión más reciente gana y no altera el registro original.
after = evaluate()
accepted = after.where("monto_normalizado > 0").collect()
assert len(accepted) == 1 and accepted[0].registro_id == record_id
assert accepted[0].monto_normalizado == 12
assert after.where("monto_normalizado <= 0").count() == 0
assert original.first().monto_total == "-10.00"
assert evaluate().where("monto_normalizado > 0").count() == 1
dbutils.notebook.exit(json.dumps({
    "caso": "venta sintética con monto negativo",
    "rechazos_antes": 1, "rechazos_despues": 0, "filas_aceptadas": 1,
    "revision_aplicada": 2, "bronze_original_conservado": True, "repeticion_idempotente": True,
}, ensure_ascii=False))
