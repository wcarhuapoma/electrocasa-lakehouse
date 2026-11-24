from pyspark import pipelines as dp
from pyspark.sql import Window, functions as F

from electrocasa.utils.normalizacion import COURIERS, mapped, slug

# Comparte el catálogo de cada target entre las tres capas.
CAT = spark.conf.get("electrocasa.catalog")
LANDING = f"/Volumes/{CAT}/default/vol_landing"
FIELDS = {
    "ventas": "venta_id sucursal_id producto_id vendedor_id cantidad monto_total metodo_pago fecha_venta",
    "empleados": "id_empleado nombre dni email salario sucursal_id cargo tipo_evento fecha_evento fecha_registro",
    "devoluciones": "devolucion_id venta_id sucursal_id producto_id motivo monto_reembolso fecha_devolucion",
}
FIELDS["ventas"] += " canal"
RESENAS = (
    "resena_id string, producto_id string, cliente_id string, calificacion string, comentario string, "
    "tags array<string>, respuestas array<struct<autor:string,texto:string,fecha:string>>, fecha_resena string"
)
AUDIT = ("_archivo", "_lote", "_ingestado_en")


def table(layer, name):
    # Lee objetos con nombre completo para identificar dependencias entre esquemas.
    return spark.read.table(f"{CAT}.{layer}.{name}")


def audit_file(df):
    # Extrae el nombre y la fecha del lote del archivo de origen.
    return (df.withColumn("_archivo", F.col("_metadata.file_name"))
            .withColumn("_lote", F.coalesce(F.regexp_extract("_archivo", r"(\d{4}-\d{2}-\d{2})", 1),
                                          F.lit("")))
            .withColumn("_lote", F.when(F.col("_lote") != "", F.col("_lote")).otherwise(F.lit("referencia")))
            .withColumn("_ingestado_en", F.current_timestamp()))


def bronze_csv(source):
    # Conserva texto crudo y campos mal formados para su evaluación en Silver.
    schema = ", ".join(f"{name} string" for name in FIELDS[source].split())
    return audit_file(spark.readStream.format("cloudFiles")
                      .option("cloudFiles.format", "csv").option("header", "true")
                      .option("rescuedDataColumn", "_rescued_data").schema(schema)
                      .load(f"{LANDING}/{source}"))


def register_csv(source):
    @dp.table(name=f"bronze.{source}")
    def dataset():
        # Registra una tabla de streaming por fuente CSV incremental.
        return bronze_csv(source)


for source in FIELDS:
    register_csv(source)


@dp.table(name="bronze.resenas")
def bronze_resenas():
    # Lee un objeto por línea y conserva tanto su estructura como el JSON original.
    stream = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "text")
              .load(f"{LANDING}/resenas"))
    return (audit_file(stream).withColumn("_parsed", F.from_json("value", RESENAS))
            .select("_parsed.*", F.get_json_object("value", "$.metadata").alias("metadata"),
                    F.col("value").alias("_raw_json"), *AUDIT))


def base(source):
    # Calcula la identidad con el registro original y aplica la última corrección.
    raw = table("bronze", source)
    if source == "tracking_envios":
        raw = (raw.withColumn("_archivo", F.lit("dbo.TrackingEnvios"))
               .withColumn("_lote", F.lit("azure_sql"))
               .withColumn("_ingestado_en", F.lit(None).cast("timestamp")))
    names = [c for c in raw.columns if not c.startswith("_")]
    payload = F.to_json(F.struct(*names), {"ignoreNullFields": "false"})
    identity = (F.col("evento_id").cast("string") if source == "tracking_envios"
                else F.sha2(F.to_json(F.struct("_archivo", payload.alias("contenido"),
                                               *[c for c in ("_raw_json", "_rescued_data") if c in raw.columns]),
                                      {"ignoreNullFields": "false"}), 256))
    fixes = table("silver", "correcciones").where(F.col("fuente") == source)
    fixes = latest(fixes, ["registro_id"], F.desc("revision"), F.desc("payload_json"))
    combined = raw.withColumn("registro_id", identity).join(
        fixes.select("registro_id", "payload_json"), "registro_id", "left")
    # El reemplazo usa el esquema Bronze de las columnas de negocio.
    schema = raw.select(*names).schema
    combined = combined.withColumn("_fix", F.from_json("payload_json", schema))
    return combined.select(
        *[F.when(F.col("_fix").isNotNull(), F.col(f"_fix.{c}")).otherwise(F.col(c)).alias(c) for c in names],
        *AUDIT, "registro_id",
    )


def latest(df, keys, *order):
    # Selecciona una fila por clave con un orden total explícito.
    return (df.withColumn("_rn", F.row_number().over(Window.partitionBy(*keys).orderBy(*order)))
            .where(F.col("_rn") == 1).drop("_rn"))


def ids(df, *columns):
    # Convierte identificadores vacíos en nulos y unifica sus espacios.
    for name in columns:
        df = df.withColumn(name, F.when(F.trim(name) != "", F.trim(name)))
    return df


def date(column):
    # Lee las fechas ISO y día/mes sin propagar errores de conversión ANSI.
    value = F.trim(column.cast("string"))
    return F.coalesce(
        F.try_to_timestamp(value, F.lit("yyyy-MM-dd")),
        F.try_to_timestamp(value, F.lit("dd/MM/yyyy")),
        F.try_to_timestamp(value, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
        F.try_to_timestamp(value, F.lit("yyyy-MM-dd HH:mm:ss")),
    ).cast("date")


def money(name):
    # Admite 1.234,50 y S/ 1,299.90, además de montos sin separadores.
    value = F.regexp_replace(F.trim(F.col(name)), r"(?i)^(?:S/\.?|PEN)\s*|\s+", "")
    formatted = F.when(value.rlike(r",\d{1,2}$"),
                       F.regexp_replace(F.regexp_replace(value, r"\.", ""), ",", ".")).otherwise(
                           F.regexp_replace(value, ",", ""))
    valid = value.rlike(r"^-?(?:\d+(?:[.,]\d{1,2})?|\d{1,3}(?:\.\d{3})+(?:,\d{1,2})?|\d{1,3}(?:,\d{3})+(?:\.\d{1,2})?)$")
    return F.when(valid, formatted).try_cast("decimal(12,2)")


PAY = {
    "efectivo": "efectivo", "efv": "efectivo", "plin": "plin",
    "tarjeta": "tarjeta", "tarjeta_de_credito": "tarjeta", "tc": "tarjeta", "td": "tarjeta",
    "transferencia": "transferencia", "trf": "transferencia", "yape": "yape",
}
CATEGORIES = {key: key for key in ("linea_blanca", "climatizacion", "cocina", "electronica", "entretenimiento")}
EVENTS = {key: key for key in ("alta", "transferencia", "cambio_salario", "baja")}
EVENTS["cese"] = "baja"
STATUS = {
    "pendiente": "pendiente", "en_transito": "en_transito", "en_camino": "en_transito",
    "entregado": "entregado", "devuelto": "devuelto",
}

# Comparte las condiciones de las expectativas y la cuarentena.
RULES = {
    "ventas": {
        "monto_positivo": "monto_total > 0",
        "venta_ubicable": "venta_id IS NOT NULL AND sucursal_id IS NOT NULL AND fecha_venta IS NOT NULL "
                          "AND cantidad > 0 AND canal IN ('tienda_fisica', 'ecommerce')",
    },
    "catalogo": {
        "precio_valido": "precio_lista > 0",
        "producto_identificable": "producto_id IS NOT NULL AND categoria IS NOT NULL AND activo IS NOT NULL",
    },
    "empleados": {
        "evento_ubicable": "id_empleado IS NOT NULL AND fecha_evento IS NOT NULL AND fecha_registro IS NOT NULL",
        "tipo_evento_valido": "tipo_evento IS NOT NULL AND (tipo_evento = 'baja' OR sucursal_id IS NOT NULL)",
    },
    "resenas": {
        "calificacion_en_escala": "calificacion BETWEEN 1 AND 5",
        "resena_asociada": "resena_id IS NOT NULL AND fecha_resena IS NOT NULL AND _producto_existe",
        "categoria_actual_valida": "NOT _producto_existe OR _categoria_valida",
    },
    "devoluciones": {
        "reembolso_valido": "monto_reembolso BETWEEN 0 AND _monto_venta",
        "devolucion_asociada": "devolucion_id IS NOT NULL AND sucursal_id IS NOT NULL "
                              "AND fecha_devolucion IS NOT NULL AND _monto_venta IS NOT NULL",
    },
    "tracking_envios": {
        "estado_conocido": "estado_entrega IS NOT NULL",
        "courier_conocido": "courier IS NOT NULL",
        "evento_asociado": "evento_id IS NOT NULL AND tracking_id IS NOT NULL "
                           "AND fecha_modificacion IS NOT NULL AND _venta_valida",
    },
    "sucursales": {"sucursal_identificable": "sucursal_id IS NOT NULL AND fecha_apertura IS NOT NULL"},
}
WARNINGS = {
    "ventas": {"metodo_conocido": "metodo_pago IS NOT NULL", "producto_en_catalogo": "_producto_valido"},
    "empleados": {"salario_en_rango": "NOT _salario_invalido", "dni_no_compartido": "NOT _dni_compartido"},
    "devoluciones": {"producto_en_catalogo": "_producto_valido"},
}


def valid_product(df):
    # Marca la cobertura de catálogo sin perder los hechos financieros.
    products = table("silver", "catalogo").select("producto_id").withColumn("_producto_valido", F.lit(True))
    return df.join(products, "producto_id", "left").fillna(False, ["_producto_valido"])


@dp.materialized_view(name="silver.catalogo_categorias")
def product_categories():
    # Resuelve la categoría en la última aparición aunque el precio se rechace.
    products = spark.read.table("_candidatos_catalogo").where(
        "producto_id IS NOT NULL AND fecha_snapshot IS NOT NULL"
    )
    return latest(products, ["producto_id"], F.desc("fecha_snapshot"),
                  F.col("categoria").isNotNull().desc(), F.asc("registro_id")).select(
                      "producto_id", "categoria", "fecha_snapshot"
                  )


def candidates(source):
    # Tipifica los campos y resuelve las dependencias antes de evaluar las reglas.
    df = base(source)
    if source == "catalogo":
        return (ids(df, "producto_id").withColumn("categoria", mapped("categoria", CATEGORIES))
                .withColumn("precio_lista", money("precio_lista"))
                .withColumn("activo", F.col("activo").try_cast("boolean"))
                .withColumn("fecha_snapshot", date(F.col("_lote"))))
    if source == "sucursales":
        return (ids(df, "sucursal_id").withColumn("ciudad", F.initcap(F.trim("ciudad")))
                .withColumn("fecha_apertura", date(F.col("fecha_apertura"))))
    if source == "ventas":
        return valid_product(ids(df, "venta_id", "sucursal_id", "producto_id", "vendedor_id")
                             .withColumn("fecha_venta", date(F.col("fecha_venta")))
                             .withColumn("monto_total", money("monto_total"))
                             .withColumn("cantidad", F.col("cantidad").try_cast("int"))
                             .withColumn("canal", slug("canal")).withColumn("metodo_pago", mapped("metodo_pago", PAY)))
    if source == "empleados":
        df = (ids(df, "id_empleado", "dni", "sucursal_id").withColumn("fecha_evento", date(F.col("fecha_evento")))
              .withColumn("fecha_registro", F.col("fecha_registro").try_cast("timestamp"))
              .withColumn("tipo_evento", mapped("tipo_evento", EVENTS))
              .withColumn("_salario", money("salario"))
              .withColumn("_salario_invalido", F.col("salario").isNotNull()
                          & ~F.coalesce((F.col("_salario") >= 500) & (F.col("_salario") < 20000), F.lit(False)))
              .withColumn("salario", F.when(~F.col("_salario_invalido"), F.col("_salario"))).drop("_salario"))
        shared = (df.where("dni IS NOT NULL AND id_empleado IS NOT NULL").groupBy("dni")
                  .agg(F.countDistinct("id_empleado").alias("_personas")).where("_personas > 1")
                  .select("dni", F.lit(True).alias("_dni_compartido")))
        return df.join(shared, "dni", "left").fillna(False, ["_dni_compartido"])
    if source == "resenas":
        categories = table("silver", "catalogo_categorias").select(
            "producto_id", F.lit(True).alias("_producto_existe"),
            F.col("categoria").isNotNull().alias("_categoria_valida"),
        )
        return (ids(df, "resena_id", "producto_id")
                .withColumn("fecha_resena", date(F.col("fecha_resena")))
                .withColumn("calificacion", F.col("calificacion").try_cast("int"))
                .join(categories, "producto_id", "left")
                .fillna(False, ["_producto_existe", "_categoria_valida"]))
    if source == "devoluciones":
        sales = table("silver", "ventas").select("venta_id", F.col("monto_total").alias("_monto_venta"))
        return (valid_product(ids(df, "devolucion_id", "venta_id", "producto_id", "sucursal_id")
                              .withColumn("monto_reembolso", money("monto_reembolso"))
                              .withColumn("fecha_devolucion", date(F.col("fecha_devolucion"))))
                .join(sales, "venta_id", "left"))
    sales = (table("silver", "ventas").where("canal = 'ecommerce'")
             .select("venta_id", F.lit(True).alias("_venta_valida")))
    return (ids(df, "tracking_id", "venta_id", "courier").withColumn("courier", mapped("courier", COURIERS))
            .withColumn("estado_entrega", mapped("estado_entrega", STATUS))
            .withColumn("fecha_actualizacion", F.col("fecha_actualizacion").cast("date"))
            .withColumn("fecha_modificacion", F.col("fecha_modificacion").cast("timestamp"))
            .join(sales, "venta_id", "left").fillna(False, ["_venta_valida"]))


def conditions(rules):
    # Trata los resultados nulos como incumplimientos en todas las salidas.
    return {name: f"coalesce(({expr}), false)" for name, expr in rules.items()}


KEYS = {
    "ventas": ["venta_id"], "resenas": ["resena_id"], "sucursales": ["sucursal_id"],
    "devoluciones": ["devolucion_id"], "catalogo": ["producto_id"],
    "empleados": ["id_empleado", "fecha_evento", "fecha_registro"],
}


def publish_silver(source):
    # Publica candidatos privados y aplica expectativas a las filas canónicas.
    @dp.materialized_view(name=f"_candidatos_{source}", private=True)
    def staging():
        return candidates(source)

    name = "_empleados_eventos" if source == "empleados" else f"silver.{source}"

    def clean():
        # Deduplica después de ordenar candidatos válidos antes de los rechazados.
        df = spark.read.table(f"_candidatos_{source}")
        accepted = " AND ".join(conditions(RULES[source]).values())
        # Prioriza los registros válidos; las expectativas eliminan los restantes.
        order = [F.expr(accepted).desc(), F.asc("_lote"), F.asc("registro_id")]
        if source == "catalogo":
            return latest(df, KEYS[source], F.expr(accepted).desc(), F.desc("fecha_snapshot"),
                          F.desc("precio_lista"), F.asc("registro_id"))
        if source == "tracking_envios":
            df = df.withColumn("_fecha_cambio", F.coalesce("fecha_actualizacion", F.to_date("fecha_modificacion")))
            return latest(df, ["tracking_id", "estado_entrega", "_fecha_cambio"], F.expr(accepted).desc(),
                          F.asc("fecha_modificacion"), F.asc("evento_id")).drop("_fecha_cambio")
        return latest(df, KEYS[source], *order)

    if source in WARNINGS:
        clean = dp.expect_all(conditions(WARNINGS[source]))(clean)
    clean = dp.expect_all_or_drop(conditions(RULES[source]))(clean)
    dp.materialized_view(name=name, private=source == "empleados")(clean)


for source in RULES:
    publish_silver(source)


def findings(policies, severity):
    # Genera una observación por registro y regla sin exponer datos personales.
    reports = []
    for source, rules in policies.items():
        checks = [F.when(~F.expr(expr), F.lit(name)) for name, expr in conditions(rules).items()]
        reports.append(spark.read.table(f"_candidatos_{source}")
                       .select("registro_id", "_lote", "_archivo", F.explode(F.array(*checks)).alias("regla"))
                       .where("regla IS NOT NULL").withColumn("fuente", F.lit(source))
                       .withColumn("severidad", F.lit(severity)))
    report = reports[0]
    for chunk in reports[1:]:
        report = report.unionByName(chunk)
    return report.dropDuplicates()


@dp.materialized_view(name="silver.cuarentena")
def quarantine():
    # Publica únicamente los rechazos vigentes.
    return findings(RULES, "drop")


@dp.materialized_view(name="silver.advertencias")
def warnings():
    # Publica anomalías que conservan la fila de negocio.
    return findings(WARNINGS, "warn")


EMPLOYEE_MASKS = (
    f"dni STRING MASK {CAT}.silver.mask_dni, "
    f"salario DECIMAL(12,2) MASK {CAT}.silver.mask_salario"
)


@dp.materialized_view(
    name="silver.empleados",
    schema="id_empleado STRING, nombre STRING, email STRING, sucursal_id STRING, cargo STRING, "
           "tipo_evento STRING, fecha_evento DATE, fecha_registro TIMESTAMP, vigente_desde DATE, vigente_hasta DATE, "
           "activo BOOLEAN, registro_id STRING, " + EMPLOYEE_MASKS,
)
def employee_history():
    # Ordena todos los eventos y deja intervalos de fecha semiabiertos.
    events = spark.read.table("_empleados_eventos")
    order = Window.partitionBy("id_empleado").orderBy("fecha_evento", "fecha_registro", "registro_id")
    elapsed = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    # Solo alta y baja modifican el estado; el resto conserva la última transición.
    transition = (F.when(F.col("tipo_evento") == "alta", F.lit(True))
                  .when(F.col("tipo_evento") == "baja", F.lit(False)))
    history = (events.withColumn("vigente_hasta", F.lead("fecha_evento").over(order))
               .withColumn("activo", F.coalesce(F.last(transition, ignorenulls=True).over(elapsed), F.lit(False)))
               .withColumn("vigente_desde", F.col("fecha_evento")))
    return history.select("id_empleado", "nombre", "email", "sucursal_id", "cargo", "tipo_evento",
                          "fecha_evento", "fecha_registro", "vigente_desde", "vigente_hasta",
                          "activo", "registro_id", "dni", "salario")


@dp.materialized_view(name="gold.empleados_actuales", schema="id_empleado STRING, sucursal_id STRING, " + EMPLOYEE_MASKS)
def employees_current():
    # Mantiene las máscaras también en la salida derivada.
    return table("silver", "empleados").where("activo AND vigente_hasta IS NULL").select(
        "id_empleado", "sucursal_id", "dni", "salario")

START = "2026-03-01"
STOP = "2026-11-01"

# Cada consulta tiene el grano de una salida Gold del examen.
GOLD = {
    "ventas_netas": f"""
        WITH ventas AS (
            SELECT sucursal_id, trunc(fecha_venta, 'MM') mes, sum(monto_total) ventas_brutas,
                   count(DISTINCT venta_id) cantidad_ventas
            FROM {CAT}.silver.ventas WHERE fecha_venta >= '{START}' AND fecha_venta < '{STOP}'
            GROUP BY 1, 2
        ), devoluciones AS (
            SELECT sucursal_id, trunc(fecha_devolucion, 'MM') mes, sum(monto_reembolso) reembolsos
            FROM {CAT}.silver.devoluciones WHERE fecha_devolucion >= '{START}' AND fecha_devolucion < '{STOP}'
            GROUP BY 1, 2
        )
        SELECT sucursal_id, mes, coalesce(ventas_brutas, 0) ventas_brutas,
               coalesce(reembolsos, 0) reembolsos,
               coalesce(ventas_brutas, 0) - coalesce(reembolsos, 0) ventas_netas,
               coalesce(cantidad_ventas, 0) cantidad_ventas,
               round(try_divide(ventas_brutas, cantidad_ventas), 2) ticket_promedio
        FROM ventas FULL OUTER JOIN devoluciones USING (sucursal_id, mes)
    """,
    "ranking_productos": f"""
        WITH ventas AS (
            SELECT producto_id, sum(cantidad) unidades, count(DISTINCT venta_id) ventas
            FROM {CAT}.silver.ventas
            WHERE fecha_venta >= '{START}' AND fecha_venta < '{STOP}' AND _producto_valido
            GROUP BY producto_id
        ), dev AS (
            SELECT producto_id, count(DISTINCT devolucion_id) devoluciones
            FROM {CAT}.silver.devoluciones
            WHERE fecha_devolucion >= '{START}' AND fecha_devolucion < '{STOP}' AND _producto_valido
            GROUP BY producto_id
        ), medidas AS (
            SELECT producto_id, unidades, ventas, coalesce(devoluciones, 0) devoluciones,
                   try_divide(coalesce(devoluciones, 0), ventas) tasa
            FROM ventas LEFT JOIN dev USING (producto_id) WHERE ventas > 0
        ), orden AS (
            SELECT 'unidades' criterio, row_number() OVER (ORDER BY unidades DESC, producto_id) posicion, *
            FROM medidas
            UNION ALL
            SELECT 'tasa_devolucion', row_number() OVER (ORDER BY tasa DESC, producto_id), *
            FROM medidas
        )
        SELECT orden.*, c.nombre_producto FROM orden
        JOIN {CAT}.silver.catalogo c USING (producto_id) WHERE posicion <= 10
    """,
    "dotacion_mensual": f"""
        WITH meses AS (
            SELECT explode(sequence(DATE '{START}', add_months(DATE '{STOP}', -1), interval 1 month)) mes
        )
        SELECT s.sucursal_id, m.mes, count(DISTINCT e.id_empleado) empleados_activos
        FROM {CAT}.silver.sucursales s CROSS JOIN meses m
        LEFT JOIN {CAT}.silver.empleados e
          ON s.sucursal_id = e.sucursal_id AND e.activo AND e.vigente_desde <= last_day(m.mes)
         AND (e.vigente_hasta > last_day(m.mes) OR e.vigente_hasta IS NULL)
        GROUP BY 1, 2
    """,
    "ventas_por_vendedor": f"""
        SELECT v.vendedor_id, coalesce(e.sucursal_id, 'SIN_ASIGNAR') sucursal_id,
               trunc(v.fecha_venta, 'MM') mes, sum(v.monto_total) monto,
               count(DISTINCT v.venta_id) cantidad_ventas
        FROM {CAT}.silver.ventas v LEFT JOIN {CAT}.silver.empleados e
          ON v.vendedor_id = e.id_empleado AND e.activo AND e.vigente_desde <= v.fecha_venta
         AND (e.vigente_hasta > v.fecha_venta OR e.vigente_hasta IS NULL)
        WHERE v.canal = 'tienda_fisica' AND v.fecha_venta >= '{START}' AND v.fecha_venta < '{STOP}'
        GROUP BY 1, 2, 3
    """,
    "resenas_negativas": f"""
        SELECT c.categoria, trunc(r.fecha_resena, 'MM') mes, count(*) resenas_validas,
               count_if(r.calificacion IN (1, 2)) resenas_negativas,
               100 * try_divide(count_if(r.calificacion IN (1, 2)), count(*)) porcentaje_negativas
        FROM {CAT}.silver.resenas r JOIN {CAT}.silver.catalogo_categorias c USING (producto_id)
        WHERE r.fecha_resena >= '{START}' AND r.fecha_resena < '{STOP}'
        GROUP BY 1, 2
    """,
    "despachos_por_courier": f"""
        WITH envios AS (
            SELECT tracking_id,
                   min_by(courier, struct(estado_entrega != 'pendiente', fecha_modificacion, evento_id)) courier,
                   min(CASE WHEN estado_entrega = 'pendiente' THEN fecha_modificacion END) pendiente,
                   min(CASE WHEN estado_entrega = 'entregado' THEN fecha_modificacion END) entregado,
                   count_if(estado_entrega = 'devuelto') > 0 devuelto
            FROM {CAT}.silver.tracking_envios
            WHERE fecha_modificacion >= '{START}' AND fecha_modificacion < '{STOP}'
            GROUP BY tracking_id
        )
        SELECT courier, count_if(entregado IS NOT NULL OR devuelto) envios_finalizados,
               count_if(devuelto) envios_devueltos,
               count_if(entregado IS NOT NULL AND pendiente <= entregado) envios_con_duracion,
               avg(CASE WHEN pendiente <= entregado THEN timestampdiff(SECOND, pendiente, entregado) / 3600.0 END)
                   horas_promedio_entrega,
               100 * try_divide(count_if(devuelto), count_if(entregado IS NOT NULL OR devuelto)) porcentaje_devueltos
        FROM envios GROUP BY courier
    """,
}


def publish_gold(name, query):
    @dp.materialized_view(name=f"gold.{name}")
    def dataset():
        # Ejecuta la consulta de negocio dentro del grafo declarativo.
        return spark.sql(query)


for name, query in GOLD.items():
    publish_gold(name, query)
