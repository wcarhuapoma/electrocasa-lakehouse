from pyspark.sql import functions as F

# Las claves representan variantes después de limpiar espacios y casing.
COURIERS = {
    "olva": "olva_courier",
    "olva_courier": "olva_courier",
    "servientrega": "servientrega",
    "shalom": "shalom",
}


def slug(column):
    # Unifica acentos, mayúsculas y separadores.
    clean = F.translate(F.lower(F.trim(column)), "áéíóúüñ", "aeiouun")
    return F.regexp_replace(clean, r"[\s-]+", "_")


def mapped(column, values):
    # Devuelve nulo para variantes fuera del conjunto permitido.
    mapping = F.create_map(*[F.lit(item) for pair in values.items() for item in pair])
    return F.element_at(mapping, slug(column))
