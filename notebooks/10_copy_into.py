# Databricks notebook source
# Obtiene el catálogo del job y limita los destinos a los targets del proyecto.
dbutils.widgets.text("catalog", "electrocasa_dev")
dbutils.widgets.dropdown("solo_validar", "false", ["false", "true"])
catalog = dbutils.widgets.get("catalog")
if catalog not in {"electrocasa_dev", "electrocasa_prod"}:
    raise ValueError(f"Catálogo inesperado: {catalog}")
validation = "VALIDATE ALL" if dbutils.widgets.get("solo_validar") == "true" else ""

# Carga archivos nuevos con esquemas fijos y conserva los metadatos de origen.
sources = {
    "catalogo": (
        "producto_id, nombre_producto, categoria, marca, precio_lista, activo",
        "JSON",
        "'multiLine'='true', 'primitivesAsString'='true'",
    ),
    "sucursales": (
        "sucursal_id, nombre_sucursal, distrito, ciudad, region, fecha_apertura",
        "CSV",
        "'header'='true'",
    ),
}
for source, (columns, file_format, options) in sources.items():
    result = spark.sql(f"""
        COPY INTO {catalog}.bronze.{source}
        FROM (
            SELECT {columns}, _metadata.file_name AS _archivo,
                   coalesce(nullif(regexp_extract(_metadata.file_name, r'(\\d{{4}}-\\d{{2}}-\\d{{2}})', 1), ''),
                            'referencia') AS _lote,
                   current_timestamp() AS _ingestado_en
            FROM '/Volumes/{catalog}/default/vol_landing/{source}/'
        )
        FILEFORMAT = {file_format}
        {validation}
        FORMAT_OPTIONS ({options})
        COPY_OPTIONS ('force'='false')
    """)
    display(result)
