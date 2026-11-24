# Databricks notebook source
import ast
import json
from bisect import bisect_right
from calendar import monthrange
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import re
import unicodedata

from pyspark.sql import Window, functions as F

# Contrasta las seis salidas con cálculos en Python sobre sus entradas de Silver.
dbutils.widgets.text("catalog", "electrocasa_prod")
dbutils.widgets.text("codigo_pipeline", "")
catalog = dbutils.widgets.get("catalog")
if catalog not in {"electrocasa_dev", "electrocasa_prod"}:
    raise ValueError("Seleccione un catálogo de ElectroCasa.")
spark.conf.set("spark.sql.session.timeZone", "America/Lima")

start, stop = date(2026, 3, 1), date(2026, 11, 1)
checks = {}


def read(layer, table, fields):
    # Lee solo columnas necesarias y solo los datos acotados del examen.
    return spark.table(f"{catalog}.{layer}.{table}").select(*fields.split()).collect()


def period(day):
    return start <= day < stop


def month(day):
    return day.replace(day=1)


def normalized_number(value):
    # Compara decimales y dobles con una tolerancia de diez decimales.
    if isinstance(value, (Decimal, float)):
        return Decimal(str(value)).quantize(Decimal("0.0000000001"))
    return value


def reconcile(name, fields, expected, tolerance=None):
    # Compara filas completas y detecta faltantes, duplicados y medidas distintas.
    def bag(values):
        return Counter(tuple(map(normalized_number, row)) for row in values)

    observed = read("gold", name, fields)
    actual, wanted = bag(observed), bag(expected)
    differences = sum((actual - wanted).values()) + sum((wanted - actual).values())
    if tolerance is not None:
        # La duración de Spark puede redondearse antes del promedio. Compara por courier.
        expected_by_key = {row[0]: row[1:] for row in expected}
        observed_by_key = {row[0]: tuple(row)[1:] for row in observed}
        differences = abs(len(observed) - len(observed_by_key))
        for key in expected_by_key.keys() | observed_by_key.keys():
            if key not in expected_by_key or key not in observed_by_key:
                differences += 1
                continue
            differences += any(
                abs(Decimal(str(left)) - Decimal(str(right))) > tolerance
                if isinstance(left, (float, Decimal)) and isinstance(right, (float, Decimal)) else left != right
                for left, right in zip(observed_by_key[key], expected_by_key[key])
            )
    checks[name] = {
        "aprobada": differences == 0,
        "esperadas": sum(wanted.values()),
        "observadas": sum(actual.values()),
        "diferencias": differences,
    }


sales = [r for r in read("silver", "ventas", "venta_id sucursal_id producto_id vendedor_id cantidad monto_total canal fecha_venta _producto_valido")
         if period(r.fecha_venta)]
returns = [r for r in read("silver", "devoluciones", "devolucion_id sucursal_id producto_id monto_reembolso fecha_devolucion _producto_valido")
           if period(r.fecha_devolucion)]
gross, refund, sale_count = Counter(), Counter(), Counter()
for r in sales:
    key = (r.sucursal_id, month(r.fecha_venta))
    gross[key] += r.monto_total
    sale_count[key] += 1
for r in returns:
    refund[r.sucursal_id, month(r.fecha_devolucion)] += r.monto_reembolso
reconcile("ventas_netas", "sucursal_id mes ventas_brutas reembolsos ventas_netas cantidad_ventas ticket_promedio", [
    (*key, gross[key], refund[key], gross[key] - refund[key], sale_count[key],
     (gross[key] / sale_count[key]).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) if sale_count[key] else None)
    for key in gross.keys() | refund.keys()
])

# Reconstruye ambos rankings por separado y conserva el desempate por producto.
products = dict(read("silver", "catalogo", "producto_id nombre_producto"))
units, transactions, returned_products = Counter(), Counter(), Counter()
for r in sales:
    if r["_producto_valido"]:
        units[r.producto_id] += r.cantidad
        transactions[r.producto_id] += 1
for r in returns:
    if r["_producto_valido"]:
        returned_products[r.producto_id] += 1
rates = {product: Decimal(returned_products[product]) / transactions[product] for product in transactions}
ranking = [
    (metric, index, product, units[product], transactions[product], returned_products[product], rates[product], products[product])
    for metric, values in (("unidades", units), ("tasa_devolucion", rates))
    for index, product in enumerate(sorted(values, key=lambda key: (-values[key], key))[:10], 1)
]
reconcile("ranking_productos", "criterio posicion producto_id unidades ventas devoluciones tasa nombre_producto", ranking)

# Reconstruye la vigencia desde los eventos, sin usar activo ni vigente_hasta de Silver.
employee_events = defaultdict(list)
for r in read("silver", "empleados", "id_empleado fecha_evento fecha_registro registro_id sucursal_id tipo_evento"):
    employee_events[r.id_empleado].append(r)
traces = {}
for key, rows in employee_events.items():
    state = False
    recorded = []
    for row in sorted(rows, key=lambda r: (r.fecha_evento, r.fecha_registro, r.registro_id)):
        if row.tipo_evento in {"alta", "baja"}:
            state = row.tipo_evento == "alta"
        recorded.append((row.sucursal_id, state, row.tipo_evento != "baja"))
    dates = sorted(r.fecha_evento for r in rows)
    traces[key] = (dates, recorded)


def status(person, day):
    # Elige el último evento del día y devuelve también la actividad anterior a la corrección.
    dates, states = traces.get(person, ([], []))
    index = bisect_right(dates, day) - 1
    return states[index] if index >= 0 else (None, False, False)


branches = [r.sucursal_id for r in read("silver", "sucursales", "sucursal_id")]
staff = []
for m in range(3, 11):
    cutoff = date(2026, m, monthrange(2026, m)[1])
    assigned = Counter(branch for person in traces for branch, active, _ in [status(person, cutoff)] if active)
    staff.extend((branch, month(cutoff), assigned[branch]) for branch in branches)
reconcile("dotacion_mensual", "sucursal_id mes empleados_activos", staff)

# Reconcilia cada vendedor, sucursal y mes con el estado en la fecha de venta.
seller_amount, seller_count = Counter(), Counter()
reassigned = 0
for r in sales:
    if r.canal == "tienda_fisica":
        branch, active, old_active = status(r.vendedor_id, r.fecha_venta)
        key = (r.vendedor_id, branch if active and branch else "SIN_ASIGNAR", month(r.fecha_venta))
        seller_amount[key] += r.monto_total
        seller_count[key] += 1
        reassigned += bool(branch and old_active and not active)
reconcile("ventas_por_vendedor", "vendedor_id sucursal_id mes monto cantidad_ventas", [
    (*key, seller_amount[key], seller_count[key]) for key in seller_count
])

# Deriva las categorías de los snapshots Bronze sin considerar los precios.
canonical = {"linea_blanca", "climatizacion", "cocina", "electronica", "entretenimiento"}
snapshots = defaultdict(lambda: defaultdict(set))
for r in read("bronze", "catalogo", "producto_id categoria _lote"):
    category = "".join(c for c in unicodedata.normalize("NFD", (r.categoria or "").strip().lower())
                       if not unicodedata.combining(c))
    category = re.sub(r"[\s-]+", "_", category)
    if r.producto_id and r._lote:
        snapshots[r.producto_id.strip()][r._lote].add(category)
categories = {
    product: sorted(by_date[max(by_date)] & canonical)[0]
    for product, by_date in snapshots.items() if by_date[max(by_date)] & canonical
}
reviews, negative, recovered = Counter(), Counter(), 0
review_ids = set()
for r in read("silver", "resenas", "resena_id producto_id fecha_resena calificacion"):
    if period(r.fecha_resena):
        key = (categories[r.producto_id], month(r.fecha_resena))
        reviews[key] += 1
        negative[key] += r.calificacion in {1, 2}
        recovered += r.producto_id not in products
        review_ids.add(r.resena_id)
reconcile("resenas_negativas", "categoria mes resenas_validas resenas_negativas porcentaje_negativas", [
    (*key, count, negative[key], Decimal(100) * negative[key] / count) for key, count in reviews.items()
])
# Asegura que la aceptación de reseñas coincide por clave con los snapshots.
valid_raw_reviews = set()
for r in read("bronze", "resenas", "resena_id producto_id fecha_resena calificacion"):
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            day = datetime.strptime((r.fecha_resena or "").strip(), fmt).date()
            break
        except ValueError:
            day = None
    if r.resena_id and (r.producto_id or "").strip() in categories and (r.calificacion or "").strip() in {"1", "2", "3", "4", "5"} and day and period(day):
        valid_raw_reviews.add(r.resena_id.strip())
checks["resenas_aceptadas_por_clave"] = {"aprobada": valid_raw_reviews == review_ids}
checks["claves_de_negocio_unicas"] = {
    "aprobada": len(sales) == len({r.venta_id for r in sales})
                and len(returns) == len({r.devolucion_id for r in returns})
                and len(review_ids) == sum(reviews.values()),
}

# Reconcilia por envío el primer pendiente, la primera entrega y el estado final.
shipments = defaultdict(list)
dedup_keys = set()
shipping_rows = spark.table(f"{catalog}.silver.tracking_envios").select(
    "tracking_id", "courier", "estado_entrega", "fecha_actualizacion", "fecha_modificacion", "evento_id",
    F.to_date("fecha_modificacion").alias("fecha_local"),
).collect()
for r in shipping_rows:
    dedup_keys.add((r.tracking_id, r.estado_entrega, r.fecha_actualizacion or r.fecha_local))
    if period(r.fecha_local):
        shipments[r.tracking_id].append(r)
finals, returned_shipments, duration_counts, seconds = Counter(), Counter(), Counter(), Counter()
for rows in shipments.values():
    pending = [r for r in rows if r.estado_entrega == "pendiente"]
    courier = min(pending or rows, key=lambda r: (r.fecha_modificacion, r.evento_id)).courier
    delivered = [r.fecha_modificacion for r in rows if r.estado_entrega == "entregado"]
    returned = any(r.estado_entrega == "devuelto" for r in rows)
    finals[courier] += bool(delivered) or returned
    returned_shipments[courier] += returned
    if pending and delivered:
        duration = (min(delivered) - min(r.fecha_modificacion for r in pending)).total_seconds()
        if duration >= 0:
            seconds[courier] += int(duration)
            duration_counts[courier] += 1
reconcile("despachos_por_courier", "courier envios_finalizados envios_devueltos envios_con_duracion horas_promedio_entrega porcentaje_devueltos", [
    (courier, count, returned_shipments[courier], duration_counts[courier],
     Decimal(seconds[courier]) / (3600 * duration_counts[courier]) if duration_counts[courier] else None,
     Decimal(100) * returned_shipments[courier] / count if count else None)
    for courier, count in finals.items()
], tolerance=Decimal("0.000001"))
checks["tracking_sin_eventos_reprocesados"] = {"aprobada": len(dedup_keys) == len(shipping_rows)}

# Usa las funciones desplegadas sobre eventos sintéticos para probar los límites.
source_path = Path(dbutils.widgets.get("codigo_pipeline"))
if catalog == "electrocasa_prod" and not source_path.is_file():
    raise ValueError("Indique el archivo del pipeline desplegado.")
if source_path.is_file():
    from types import SimpleNamespace

    source = ast.parse(source_path.read_text(encoding="utf-8"))
    history_fn = next(n for n in source.body if isinstance(n, ast.FunctionDef) and n.name == "employee_history")
    history_fn.decorator_list = []
    fixture = spark.createDataFrame([
        ("cambio_salario", "2026-03-01", "2026-03-08 08:00:00", "A", "01"),
        ("transferencia", "2026-03-05", "2026-03-10 08:00:00", "B", "03"),
        ("alta", "2026-03-05", "2026-03-05 08:00:00", "A", "02"),
        ("baja", "2026-03-06", "2026-03-06 08:00:00", "B", "04"),
        ("transferencia", "2026-03-07", "2026-03-07 08:00:00", "C", "05"),
    ], "tipo_evento string, fecha string, registro string, sucursal_id string, registro_id string").selectExpr(
        "*", "'prueba' id_empleado", "date(fecha) fecha_evento", "timestamp(registro) fecha_registro",
        "null nombre", "null email", "null cargo", "null dni", "null salario",
    )
    scope = {"F": F, "Window": Window, "spark": SimpleNamespace(read=SimpleNamespace(table=lambda _: fixture))}
    exec(compile(ast.Module(body=[history_fn], type_ignores=[]), str(source_path), "exec"), scope)
    states = scope["employee_history"]().orderBy("registro_id").select("activo", "vigente_hasta").collect()
    checks["regresion_vigencias"] = {
        "aprobada": [r.activo for r in states] == [False, True, True, False, False]
                    and states[1].vigente_hasta == date(2026, 3, 5),
    }

    # Comprueba que los valores fuera del mapa terminan nulos y fallan la regla del pipeline.
    utils_path = source_path.parents[1] / "utils" / "normalizacion.py"
    utils_scope = {}
    exec(compile(utils_path.read_text(encoding="utf-8"), str(utils_path), "exec"), utils_scope)
    rules = next(ast.literal_eval(n.value) for n in source.body if isinstance(n, ast.Assign)
                 and isinstance(n.targets[0], ast.Name) and n.targets[0].id == "RULES")
    mapped_fixture = spark.createDataFrame([("Olva",), ("OLVA COURIER",), ("Desconocido",)], ["raw"]).select(
        "raw", utils_scope["mapped"]("raw", utils_scope["COURIERS"]).alias("courier")
    )
    mapped_rows = mapped_fixture.withColumn("aceptado", F.expr(rules["tracking_envios"]["courier_conocido"])).collect()
    tracking_fixture = mapped_fixture.selectExpr(
        "raw registro_id", "courier", "'prueba' _lote", "'sintetico' _archivo",
    )
    quarantine_functions = [n for n in source.body
                            if isinstance(n, ast.FunctionDef) and n.name in {"findings", "conditions"}]
    scope["spark"] = SimpleNamespace(read=SimpleNamespace(table=lambda _: tracking_fixture))
    exec(compile(ast.Module(body=quarantine_functions, type_ignores=[]), str(source_path), "exec"), scope)
    rule = {"tracking_envios": {"courier_conocido": rules["tracking_envios"]["courier_conocido"]}}
    quarantined = scope["findings"](rule, "drop").select("registro_id", "regla", "severidad").collect()
    checks["regresion_courier_cuarentena"] = {
        "aprobada": {(row.raw, row.courier, row.aceptado) for row in mapped_rows} == {
            ("Olva", "olva_courier", True), ("OLVA COURIER", "olva_courier", True), ("Desconocido", None, False),
        } and list(map(tuple, quarantined)) == [("Desconocido", "courier_conocido", "drop")],
    }

october = sum(count for _, m, count in staff if m.month == 10)
checks["impacto_revision"] = {
    "aprobada": (october, reassigned, recovered) == (1129, 81, 29),
    "empleados_activos_octubre": october, "ventas_reasignadas": reassigned, "resenas_recuperadas": recovered,
}
display(spark.createDataFrame([(name, check["aprobada"]) for name, check in checks.items()],
                              "verificacion string, aprobada boolean"))
failures = {name: check for name, check in checks.items() if not check["aprobada"]}
if failures:
    raise RuntimeError(f"Verificaciones fallidas: {json.dumps(failures, ensure_ascii=False)}")
dbutils.notebook.exit(json.dumps(checks, ensure_ascii=False))
