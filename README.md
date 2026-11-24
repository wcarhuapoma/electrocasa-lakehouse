# ElectroCasa Lakehouse

Proyecto integrador para una cadena ficticia de 40 sucursales. Define un pipeline administrado en Databricks Free Edition para analizar ventas, catálogo, devoluciones, empleados, reseñas y despachos de marzo a octubre de 2026.

## Arquitectura y decisiones

**Bronze → Silver → Gold.** Auto Loader carga los archivos recurrentes de ventas, empleados, devoluciones y reseñas. El pipeline administra su estado. El notebook `10_copy_into` carga los snapshots de catálogo y el maestro de sucursales con `COPY INTO`. Lakeflow Connect ingiere Tracking por `fecha_modificacion` con SCD Tipo 1 por `evento_id`, conservando los cambios de estado del envío como eventos distintos. Las credenciales viven en la conexión de Unity Catalog.

Silver normaliza fechas, decimales y categorías. Para las reingestas elige una fila válida por clave y desempata por lote e identificador estable. En el catálogo comercial prioriza el snapshot válido más reciente y el mayor precio para resolver duplicados dentro del mismo snapshot. Conserva el estado de los productos descontinuados. Las ventas y devoluciones con producto desconocido siguen en los totales financieros y se excluyen de los rankings. Reseñas usa `silver.catalogo_categorias`, que conserva la categoría del último snapshot por producto con independencia de la validación del precio. Una reseña con producto ausente o categoría inválida en ese snapshot va a cuarentena con su regla correspondiente. Las referencias se vuelven a evaluar con cada lote.

Empleados reconstruye los intervalos por `fecha_evento`, `fecha_registro` e identificador del registro. El alta activa al empleado y la baja lo desactiva. Transferencias y cambios de salario conservan el estado de actividad anterior. Los estados intermedios del mismo día tienen un intervalo de fecha vacío y el último estado del día se usa en los cruces por fecha. Los salarios fuera de [500, 20000) se convierten en nulo y generan advertencia. Los DNI compartidos generan advertencia y cada `id_empleado` mantiene su historial. Las máscaras conservan el valor para la identidad autorizada de Ingeniería y muestran `***` o nulo a otras identidades.

Tracking usa tres couriers canónicos. `Olva` y `Olva Courier` se agrupan en `olva_courier`. Las variantes de escritura de Servientrega y Shalom se estandarizan a `servientrega` y `shalom`. Un courier desconocido se registra con la regla `courier_conocido` en cuarentena. Los eventos sin una venta de ecommerce válida se vuelven a evaluar en los lotes posteriores.

El job ejecuta **COPY INTO → Tracking → Medallion → Validar**. Se usa serverless para las cuatro tareas por el volumen pequeño y la frecuencia mensual. Se habilita una sola corrida a la vez, con un reintento y alerta de fallo por correo. El job configura un disparador el primer día de cada mes a las 08:00 de Lima. Durante la validación de `prod` el disparador permanece pausado y los lotes se lanzan manualmente.

## Preparar y ejecutar

1. Autenticar un perfil de CLI en el workspace de Free Edition con `databricks auth login --host <URL> --profile electrocasa-free`. Verificar el destino con `databricks current-user me -p electrocasa-free`.
2. Crear desde la interfaz los grupos `electrocasa_ingenieria`, `electrocasa_analistas` y `electrocasa_auditoria`. Agregar a Ingeniería la cuenta que desplegará el bundle. Importar y ejecutar `notebooks/00_setup.ipynb` con el catálogo del target elegido. El notebook contiene las instrucciones de permisos para los tres grupos. Si Free Edition rechaza el principal, guardar el error y repetir con el modo de usuario del notebook.
3. Crear la conexión SQL Server en Unity Catalog. Usar el nombre `electrocasa_sqlserver` o pasar el nombre elegido al script. Crear el catálogo foráneo `electrocasa_sqlserver_ext` para comprobar el origen. La consulta debe devolver 15.139 filas, la misma cantidad de claves distintas y cero cursores nulos.

   ```sql
   SELECT count(*) AS filas, count(DISTINCT evento_id) AS claves,
          count_if(fecha_modificacion IS NULL) AS cursores_nulos
   FROM electrocasa_sqlserver_ext.dbo.TrackingEnvios;
   ```

4. Colocar los archivos entregados por el docente en las rutas de `datos/README.md`. Desde PowerShell 7, ejecutar el siguiente comando en un landing nuevo. El script comprueba los archivos, valida ambos targets, despliega el target elegido y espera cada lote antes de subir el siguiente. Guarda los resultados de los tres lotes y la repetición en `evidencias/<target>/`. El target predeterminado es `dev`.

   ```powershell
   ./scripts/ejecutar_dev.ps1 -Perfil electrocasa-free -BaseTracking '<base del docente>' -CorreoAlertas '<correo real>' -Target prod
   ```

5. Ejecutar `95_verificar_resultados` en serverless con el catálogo del target validado y el parámetro `codigo_pipeline` apuntando al archivo `src/electrocasa/transformations/pipeline.py` sincronizado por el bundle. Guardar su resultado junto con los conteos y huellas que devuelve `90_validar`.

## Consultas Gold

```sql
SELECT * FROM electrocasa_prod.gold.ventas_netas ORDER BY mes, sucursal_id;
SELECT * FROM electrocasa_prod.gold.ranking_productos ORDER BY criterio, posicion;
SELECT * FROM electrocasa_prod.gold.dotacion_mensual ORDER BY mes, sucursal_id;
SELECT * FROM electrocasa_prod.gold.ventas_por_vendedor ORDER BY mes, sucursal_id, vendedor_id;
SELECT * FROM electrocasa_prod.gold.resenas_negativas ORDER BY mes, categoria;
SELECT * FROM electrocasa_prod.gold.despachos_por_courier ORDER BY courier;
```

## Cuarentena y reproceso

`silver.cuarentena` muestra una fila por registro y regla incumplida. `silver.cuarentena_historial` guarda además el momento de procesamiento y el identificador de la corrida. `silver.advertencias` enumera anomalías que conservan la fila de negocio. El notebook de validación cuenta registros rechazados distintos por fuente y también cuenta las violaciones.

Para el reto, elegir en un entorno de prueba un rechazo cuyo valor correcto esté respaldado por la fuente. Localizar el original con el identificador y el archivo de cuarentena, reconstruir el objeto JSON completo con los nombres y tipos de Bronze, y registrar una revisión en `silver.correcciones`. La clave operativa es `(fuente, registro_id, revision)`. Asignar una revisión mayor que las anteriores y registrar el motivo. Ejecutar el job y comprobar que el identificador deja de figurar en la cuarentena activa, figura una sola vez en Silver y conserva las filas anteriores del historial. Volver a ejecutar y demostrar que Silver se mantiene estable. Para un registro con información sensible, conservar la corrección y su evidencia detallada dentro del workspace.

El notebook `96_probar_reproceso` ejecutó las funciones del pipeline en Free Edition con una venta sintética. Aplicó la segunda revisión, pasó de un rechazo a una fila aceptada, conservó Bronze y obtuvo el mismo resultado al repetir la prueba. [Evidencia del reproceso](evidencias/reproceso.json)

## Validación de la versión corregida en prod

El bundle pasó la validación estricta para `dev` y `prod`. En `prod` se desplegaron el conector de Tracking, el pipeline Medallion y el [job de orquestación](https://dbc-02d8efd4-96e8.cloud.databricks.com/jobs/294303803545328?o=7474644067615801).

| Corrida de prod | Bronze de archivos | Silver de negocio | Gold | Registros rechazados |
|---|---:|---:|---:|---:|
| [Lote 1](evidencias/prod/lote-01.json) | 18.064 | 25.596 | 3.591 | 9.824 |
| [Lote 2](evidencias/prod/lote-02.json) | 34.262 | 44.412 | 5.852 | 4.880 |
| [Lote 3](evidencias/prod/lote-03.json) | 40.930 | 54.259 | 7.334 | 1.532 |
| [Repetición sin archivos nuevos](evidencias/prod/repeticion.json) | 40.930 | 54.259 | 7.334 | 1.532 |

Los conteos Silver de negocio incluyen la dimensión de categorías y excluyen las tablas de correcciones, advertencias y cuarentena. El historial de Tracking muestra un `MERGE` inicial de **15.139 inserciones** en la versión Delta 2. La segunda corrida mantuvo la misma versión y el mismo historial. Cada evidencia contiene el enlace de la corrida y los conteos por tabla.

- **Contratos.** Los tipos desplegados y las expectativas coinciden con las siete declaraciones YAML. [Verificación de contratos](evidencias/prod/contratos.json)
- **Gold e idempotencia.** Las seis salidas reconciliaron por fila. El cierre de octubre dio **1.129 empleados activos**, **81 ventas físicas** pasaron a `SIN_ASIGNAR` y se recuperaron **29 reseñas válidas**. Las regresiones de vigencias y cuarentena aprobaron. Entre el lote 3 y la repetición coincidieron los conteos de negocio, los rechazos y las huellas de contenido de las siete vistas Gold. [Validación Gold](evidencias/prod/verificaciones-gold.json)
- **Gobierno.** El `GRANT` a Ingeniería devolvió `PRINCIPAL_DOES_NOT_EXIST` en Free Edition. El [notebook de setup](notebooks/00_setup.ipynb) contiene los permisos de los tres grupos y el modo alternativo aplicado con el propietario y `account users`. Se comprobó `SELECT` temporal sobre Silver y su posterior revocación. [Comprobación inicial](evidencias/prod/permisos-preflight.json) · [Permisos observados](evidencias/prod/gobierno.json)
- **Máscaras.** La cuenta propietaria consultó 1.129 empleados actuales y el contraste de DNI y salario entre Silver y Gold dio cero diferencias. La prueba con otra identidad requiere una cuenta lectora disponible. [Conteos de máscaras](evidencias/prod/mascaras.json)
- **Linaje.** La captura de Catalog Explorer muestra que `gold.ventas_netas` se nutre de `silver.ventas` y `silver.devoluciones`. La API de Unity Catalog confirmó también los orígenes de las siete vistas Gold, incluido el enlace entre Reseñas y la nueva dimensión. [Captura de Catalog Explorer](evidencias/prod/catalog-explorer-linaje.png) · [Linaje por tabla](evidencias/prod/linaje.json)
- **Reproceso.** La prueba aislada volvió a pasar en Databricks sobre el código desplegado de `prod`. [Resultado](evidencias/prod/reproceso.json)
- **Monitoreo.** Las cuatro actualizaciones de Tracking y las cuatro del pipeline Medallion figuran en el historial. Los jobs de `dev` y `prod` se verificaron con el disparador pausado. [Estado y configuración](evidencias/prod/job.json)

## Evidencia histórica de dev

- **Validación y despliegue.** `databricks bundle validate --strict` pasó para `dev` y `prod` en Free Edition con la base `electrocasadb`. El despliegue de `dev` creó el job, el pipeline Medallion y el conector de Tracking.
- **Configuración de dev.** El [run 955070585184459](https://dbc-02d8efd4-96e8.cloud.databricks.com/?o=7474644067615801#job/219946340814219/run/955070585184459) terminó con estado `SUCCESS` y creó el catálogo y los esquemas. Se usó `solo_objetos=true` porque aún no existían los grupos del examen.
- **Preparación del lote 1.** Los seis archivos se verificaron en el volumen de dev. El run `1009293586996167` terminó con estado `SUCCESS` y ejecutó `COPY INTO VALIDATE ALL` sobre Catálogo y Sucursales.
- **Verificación de Tracking.** La consulta federada `01f1e83e-5eac-1c08-ba6a-85256232452a` devolvió 15.139 filas, 15.139 claves distintas y cero cursores nulos desde la conexión de solo lectura.
- **Alternativa de permisos.** La lista de grupos de Free Edition mostraba solo los grupos predeterminados. Se usó `modo_usuario=true` en el run exitoso `205067055147937` para aplicar las máscaras con la identidad del dueño y dar lectura Gold al principal disponible.
- **Verificación local.** Se comprobaron los conteos de los 14 archivos, el esquema del bundle, la sintaxis de los notebooks y los siete contratos. Las seis consultas Gold se probaron sobre datos sintéticos en DuckDB con adaptaciones de dialecto.
- **Verificación Gold en Databricks.** El run `160270565668811` aprobó las siete comprobaciones, incluida la conciliación del neto y la deduplicación de eventos de envío. [Resultados](evidencias/verificaciones-gold.json)
- **Prueba de permisos.** Se capturó `SHOW GRANTS ON SCHEMA electrocasa_dev.silver` antes, durante y después de otorgar temporalmente `SELECT` al principal `account users`. Se revocó el privilegio y se comprobó su ausencia. En esta demostración no se otorgó `USE SCHEMA`. La cuenta dueña pudo consultar el objeto enmascarado de Gold y ver sus valores autorizados. [Evidencia de gobierno](evidencias/gobierno.json)

| Corrida | Bronze de archivos esperado, acumulado | Tracking esperado | Conteos observados de Silver, Gold y rechazos |
|---|---:|---:|---|
| [Lote 1](evidencias/lote-01.json) | 18.064 | 15.139 insertados, versión Delta 2 | 23.215 filas de negocio Silver, 3.602 Gold y 9.925 registros rechazados |
| [Lote 2](evidencias/lote-02.json) | 34.262 | 15.139 acumulados, cero nuevos, versión Delta 2 | 41.983 filas de negocio Silver, 5.866 Gold y 4.909 registros rechazados |
| [Lote 3](evidencias/lote-03.json) | 40.930 | 15.139 acumulados, versión Delta 2 | 51.830 filas de negocio Silver, 7.347 Gold y 1.561 registros rechazados |
| [Repetición sin archivos nuevos](evidencias/repeticion.json) | 40.930 | 15.139 acumulados, versión Delta 2 | Silver, Gold y rechazos idénticos al lote 3. Solo aumentó el historial de cuarentena por la nueva corrida. |

Los cuatro runs finalizaron en `SUCCESS`. [Lote 1](https://dbc-02d8efd4-96e8.cloud.databricks.com/jobs/70172595208905/runs/628637681475647?o=7474644067615801) · [Lote 2](https://dbc-02d8efd4-96e8.cloud.databricks.com/jobs/70172595208905/runs/1029534711173493?o=7474644067615801) · [Lote 3](https://dbc-02d8efd4-96e8.cloud.databricks.com/jobs/70172595208905/runs/978773737751305?o=7474644067615801) · [Repetición](https://dbc-02d8efd4-96e8.cloud.databricks.com/jobs/70172595208905/runs/160108478881999?o=7474644067615801)
