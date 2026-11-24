# Datos del Proyecto Integrador ElectroCasa (Edición 15)

Estos son los archivos que se te entregan junto con el enunciado (`proyecto_integrador.md`). No tienes que construirlos ni conseguirlos. Son datos ficticios, con volumen real (miles de filas) y problemas de calidad puestos a propósito, para que tus reglas de limpieza y tus expectativas de calidad tengan un efecto visible.

La fuente Tracking de envíos no es un archivo: vive en una Azure SQL Database que provee el docente (ver la última sección).

## Archivos

Los archivos llegan por lotes, como llegarían de los sistemas de origen. Procésalos en orden: primero el lote 1, después el 2 y después el 3. Así ves cómo se comporta tu pipeline con cada llegada.

| Carpeta o archivo | Fuente | Archivos | Filas | Formato |
|---|---|---|---|---|
| `ventas/` | Ventas | 3 lotes (al 31-may, 31-ago y 31-oct) | 6.783 / 6.839 / 4.618 | CSV con encabezado |
| `catalogo/` | Catálogo de productos | 2 snapshots completos (1-mar y 1-ago) | 2.305 / 2.400 | JSON (un arreglo por archivo) |
| `empleados/` | Empleados (RR.HH.) | 3 lotes de eventos | 2.257 / 1.267 / 924 | CSV con encabezado |
| `resenas/` | Reseñas | 2 lotes (al 31-jul y 31-oct) | 5.292 / 3.988 | JSON Lines (un objeto por línea) |
| `devoluciones/` | Devoluciones | 3 lotes | 1.387 / 1.704 / 1.126 | CSV con encabezado |
| `sucursales.csv` | Sucursales (referencia) | 1 | 40 | CSV con encabezado |

Sube cada fuente a su propia subcarpeta del volume de landing, en el schema `default` como en los laboratorios: `/Volumes/<catalogo>/default/vol_landing/<fuente>/` (`sucursales.csv` va en `sucursales/`). Auto Loader y COPY INTO leen todos los archivos de una carpeta, así que no mezcles fuentes con columnas distintas en la misma ruta. Sube un lote a la vez, como explica la sección "Cómo procesar los lotes" del enunciado.

## Columnas

- **Ventas:** `venta_id`, `sucursal_id`, `producto_id`, `vendedor_id` (vacío en las ventas de ecommerce), `cantidad`, `monto_total`, `metodo_pago`, `fecha_venta`, `canal` (`tienda_fisica` o `ecommerce`).
- **Catálogo:** `producto_id`, `nombre_producto`, `categoria`, `marca`, `precio_lista`, `activo`.
- **Empleados:** `id_empleado`, `nombre`, `dni`, `email`, `salario`, `sucursal_id`, `cargo`, `tipo_evento`, `fecha_evento` (cuándo ocurrió el evento), `fecha_registro` (cuándo RR.HH. lo cargó en su sistema).
- **Reseñas:** `resena_id`, `producto_id`, `cliente_id`, `calificacion`, `comentario`, `tags` (arreglo), `respuestas` (arreglo de objetos con `autor`, `texto` y `fecha`), `fecha_resena`, `metadata` (objeto cuya estructura cambia según el origen: app, web o kiosko de tienda).
- **Devoluciones:** `devolucion_id`, `venta_id`, `sucursal_id`, `producto_id`, `motivo`, `monto_reembolso`, `fecha_devolucion`.
- **Sucursales:** `sucursal_id`, `nombre_sucursal`, `distrito`, `ciudad`, `region`, `fecha_apertura`.

## Problemas de calidad que vas a encontrar

Ninguna fila viene marcada. Encontrarlos es parte de tu exploración en Bronze, y lo que encuentres va a tu contrato de datos.

- **Ventas:** `metodo_pago` con 17 formas de escribir 5 métodos (`tarjeta`, `TC`, `td`, `EFV`, `Yape`...); `monto_total` vacío, negativo o cero, y a veces con coma decimal y punto de miles (`1.234,50`); `cantidad` en cero; `sucursal_id` vacío; `fecha_venta` en tres formatos (`2026-06-07`, `07/06/2026` y `2026-06-07T14:30:00`); `producto_id` que no existe en el catálogo; ventas de un lote que vuelven a llegar, idénticas, en el lote siguiente.
- **Catálogo:** `precio_lista` a veces como texto con moneda (`"S/ 1,299.90"`), negativo, cero o nulo; `marca` nula; `categoria` con 17 variantes de escritura para 5 categorías; en el snapshot de marzo, algunos productos aparecen dos veces con precios distintos. El snapshot de agosto trae productos nuevos, cambios de precio y productos descontinuados (`activo: false`).
- **Empleados:** un empleado aparece una vez por evento (alta, transferencia, cambio de salario, baja) y `tipo_evento` viene escrito de 13 formas, incluido `cese` como sinónimo de baja. Además:
  - Hay empleados con dos eventos el mismo día. Solo `fecha_registro` dice cuál fue primero.
  - Algunos eventos de junio a agosto los registró RR.HH. recién en octubre y llegan en el lote 3.
  - `dni` vacío en algunas filas, y el mismo `dni` en dos empleados distintos (error de digitación).
  - `fecha_evento` vacía en algunas filas.
  - Salarios con un orden de magnitud fuera de rango.
- **Reseñas:** `calificacion` fuera de la escala de 1 a 5 (0, 6 o nula); `comentario` nulo o vacío; `tags` nulo en vez de arreglo vacío; respuestas sin `autor`; `producto_id` que no existe en el catálogo; `fecha_resena` en dos formatos; reseñas del lote 1 que se repiten en el lote 2.
- **Devoluciones:** `monto_reembolso` negativo o mayor que el monto de la venta de origen; `motivo` vacío; `venta_id` vacío o que no existe en ventas; devoluciones del lote anterior repetidas.
- **Sucursales:** `ciudad` con casing inconsistente en dos filas.

Si lees fechas con `to_date` y ANSI está activo (el default en serverless), un formato que no coincide hace fallar la consulta en vez de devolver nulo. Prueba `try_to_date` o normaliza el formato antes.

Cada archivo del catálogo es un solo arreglo JSON. Para leerlo con COPY INTO o con Auto Loader necesitas la opción `multiLine`. Si además tu tabla Bronze guarda todo como texto, agrega `primitivesAsString`. Sin esa opción, COPY INTO falla con `DELTA_FAILED_TO_MERGE_FIELDS` porque `activo` llega como booleano.

## Tracking de envíos (Azure SQL Database)

El docente provee una Azure SQL Database con la tabla `dbo.TrackingEnvios`. El firewall está abierto y te va a compartir en clase el host, la base de datos y un usuario de solo lectura.

Cada fila es un cambio de estado de un envío del ecommerce, así que un mismo `tracking_id` aparece varias veces:

| Columna | Tipo | Descripción |
|---|---|---|
| `evento_id` | INT, llave primaria | Identificador técnico del evento, único |
| `tracking_id` | VARCHAR | Identificador del envío |
| `venta_id` | VARCHAR | Venta de ecommerce que originó el envío |
| `courier` | VARCHAR | Courier, con casing inconsistente |
| `estado_entrega` | VARCHAR | Estado, con valores y casing inconsistentes (`En camino`, `EN_CAMINO`, `en_transito`...) |
| `sucursal_origen` | VARCHAR | Sucursal que despachó |
| `fecha_actualizacion` | DATE, puede ser nula | Fecha de negocio del cambio de estado |
| `fecha_modificacion` | DATETIME2, nunca nula | Momento en que el registro entró a la base. Es la columna cursor para la ingesta incremental |

Problemas de calidad: además de los de casing, hay mensajes reprocesados (el mismo cambio de estado registrado dos veces, con distinto `evento_id`), `fecha_actualizacion` nula y algún `venta_id` que no existe en ventas.

Cómo ingerirla:

1. Crea una Connection de Unity Catalog de tipo SQL Server con los datos que te comparte el docente. El asistente también te pide crear un catálogo foráneo. Ponle un nombre distinto del catálogo de tu proyecto, por ejemplo `electrocasa_sqlserver`. Ese catálogo te sirve para explorar la tabla antes de ingerirla, pero no reemplaza al conector.
2. Crea una ingesta con el conector query-based de SQL Server de Lakeflow Connect: tabla `dbo.TrackingEnvios`, cursor `fecha_modificacion`, llave primaria `evento_id`. Elige el modo de historia (SCD Type 1 o append only) y justifícalo en tu README.
3. Corre la ingesta. La tabla trae 15.139 eventos, de marzo a octubre, y ya está completa desde el inicio del proyecto.
4. Vuelve a correr la ingesta sin cambios en la tabla. Esta segunda corrida no tiene que traer filas nuevas, porque el conector solo lee lo que tiene un `fecha_modificacion` mayor al último valor que ya leyó. Muestra las dos corridas en tu README. La forma más simple es `DESCRIBE HISTORY` sobre la tabla destino, donde la primera corrida deja un `MERGE` con 15.139 filas y la segunda no agrega ninguna versión.

No uses `tracking_id` como llave primaria del conector: se repite por diseño, y con SCD Type 1 o 2 el conector usa la llave para hacer el merge, así que una llave repetida hace que se pierdan eventos sin ningún aviso.
