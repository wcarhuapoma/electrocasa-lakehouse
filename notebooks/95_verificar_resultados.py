# Databricks notebook source
import json

# Contrasta las métricas publicadas con comprobaciones independientes.
dbutils.widgets.text("catalog", "electrocasa_dev")
catalog = dbutils.widgets.get("catalog")
if catalog not in {"electrocasa_dev", "electrocasa_prod"}:
    raise ValueError("Seleccione un catálogo de ElectroCasa.")
s = f"{catalog}.silver"
g = f"{catalog}.gold"

checks = {
    "neto_conciliado": f"""
        SELECT abs(
            (SELECT coalesce(sum(ventas_netas), 0) FROM {g}.ventas_netas)
            - (SELECT coalesce(sum(monto_total), 0) FROM {s}.ventas
               WHERE fecha_venta BETWEEN DATE '2026-03-01' AND DATE '2026-10-31')
            + (SELECT coalesce(sum(monto_reembolso), 0) FROM {s}.devoluciones
               WHERE fecha_devolucion BETWEEN DATE '2026-03-01' AND DATE '2026-10-31')
        ) = 0
    """,
    "un_grano_por_producto_y_ranking": f"""
        SELECT count(*) <= 20 AND count(*) = count(DISTINCT struct(criterio, producto_id))
        FROM {g}.ranking_productos
    """,
    "dotacion_para_todas_las_sucursales_y_meses": f"""
        SELECT count(*) = 8 * (SELECT count(*) FROM {s}.sucursales)
        FROM {g}.dotacion_mensual
    """,
    "ventas_fisicas_sin_multiplicar": f"""
        SELECT (SELECT coalesce(sum(cantidad_ventas), 0) FROM {g}.ventas_por_vendedor) = count(*)
        FROM {s}.ventas
        WHERE canal = 'tienda_fisica' AND fecha_venta BETWEEN DATE '2026-03-01' AND DATE '2026-10-31'
    """,
    "resenas_fuera_de_escala_excluidas": f"""
        SELECT count_if(calificacion IS NULL OR calificacion < 1 OR calificacion > 5) = 0
        FROM {s}.resenas
    """,
    "eventos_reprocesados_eliminados": f"""
        SELECT count(*) = count(DISTINCT struct(tracking_id, estado_entrega,
            coalesce(fecha_actualizacion, date(fecha_modificacion))))
        FROM {s}.tracking_envios
    """,
    "despachos_con_porcentajes_validos": f"""
        SELECT count_if(envios_devueltos > envios_finalizados OR porcentaje_devueltos < 0
                        OR porcentaje_devueltos > 100 OR horas_promedio_entrega < 0) = 0
        FROM {g}.despachos_por_courier
    """,
}
result = [(name, bool(spark.sql(query).first()[0])) for name, query in checks.items()]
display(spark.createDataFrame(result, "verificacion string, aprobada boolean"))
if failures := [name for name, passed in result if not passed]:
    raise RuntimeError(f"Verificaciones fallidas: {failures}")

# Devuelve resultados agregados para adjuntar la prueba al repositorio.
dbutils.notebook.exit(json.dumps(dict(result), ensure_ascii=False))
