# ElectroCasa Lakehouse

Proyecto integrador para una cadena ficticia de 40 sucursales. Define un pipeline administrado en Databricks Free Edition para analizar ventas, catálogo, devoluciones, empleados, reseñas y despachos de marzo a octubre de 2026.

## Arquitectura y decisiones

**Bronze → Silver → Gold.** Auto Loader carga los archivos recurrentes de ventas, empleados, devoluciones y reseñas. El pipeline administra su estado. El notebook `10_copy_into` carga los snapshots de catálogo y el maestro de sucursales con `COPY INTO`. Lakeflow Connect ingiere Tracking por `fecha_modificacion` con SCD Tipo 1 por `evento_id`, conservando los cambios de estado del envío como eventos distintos. Las credenciales viven en la conexión de Unity Catalog.

Silver normaliza fechas, decimales y categorías. Para las reingestas elige una fila válida por clave y desempata por lote e identificador estable. En Catálogo prioriza el snapshot válido más reciente y el mayor precio para resolver duplicados dentro del mismo snapshot. Conserva el estado de los productos descontinuados. Las ventas y devoluciones con producto desconocido siguen en los totales financieros y se excluyen de los rankings. La falta de una venta válida en devoluciones o Tracking y la falta de producto en reseñas se registran en cuarentena. Las referencias se vuelven a evaluar con cada lote.

Empleados reconstruye los intervalos por `fecha_evento` y `fecha_registro` sobre todos los eventos vigentes de origen. Los estados intermedios del mismo día tienen un intervalo de fecha vacío y el último estado del día se usa en los cruces por fecha. Los salarios fuera de [500, 20000) se convierten en nulo y generan advertencia. Los DNI compartidos generan advertencia y cada `id_empleado` mantiene su historial. Las máscaras conservan el valor para Ingeniería y muestran `***` o nulo a otras identidades. El propietario del pipeline debe pertenecer a Ingeniería.

El job ejecuta **COPY INTO → Tracking → Medallion → Validar**. Se usa serverless para las cuatro tareas por el volumen pequeño y la frecuencia mensual. Se habilita una sola corrida a la vez, con un reintento y alerta de fallo por correo. El disparador del primer día de cada mes a las 08:00 de Lima permite que los archivos del cierre estén disponibles. Durante la prueba se lanzan manualmente los lotes y se pausa el disparador al finalizar.

## Preparar y ejecutar

1. Autenticar un perfil de CLI en el workspace de Free Edition con `databricks auth login --host <URL> --profile electrocasa-free`. Verificar el destino con `databricks current-user me -p electrocasa-free`.
2. Crear desde la interfaz los grupos `electrocasa_ingenieria`, `electrocasa_analistas` y `electrocasa_auditoria`. Agregar a Ingeniería la cuenta que desplegará el bundle. Importar y ejecutar `notebooks/00_setup.ipynb` en `electrocasa_dev`. Si Free Edition rechaza el principal de grupo, guardar el error y repetir con el modo de usuario que ofrece el notebook.
3. Crear la conexión SQL Server en Unity Catalog. Usar el nombre `electrocasa_sqlserver` o pasar el nombre elegido al script. Crear el catálogo foráneo `electrocasa_sqlserver_ext` para comprobar el origen. La consulta debe devolver 15.139 filas, la misma cantidad de claves distintas y cero cursores nulos.

   ```sql
   SELECT count(*) AS filas, count(DISTINCT evento_id) AS claves,
          count_if(fecha_modificacion IS NULL) AS cursores_nulos
   FROM electrocasa_sqlserver_ext.dbo.TrackingEnvios;
   ```

4. Colocar los archivos entregados por el docente en las rutas de `datos/README.md`. Desde PowerShell 7, ejecutar el siguiente comando en un landing nuevo. El script valida ambos targets, despliega dev, sube los archivos por lote, espera las tres ejecuciones, repite el último procesamiento sin subir archivos y vuelve a desplegar con el disparador pausado. Registrar los enlaces a las corridas que imprime el CLI.

   ```powershell
   ./scripts/ejecutar_dev.ps1 -Perfil electrocasa-free -BaseTracking '<base del docente>' -CorreoAlertas '<correo real>'
   ```

5. Ejecutar `95_verificar_resultados` en serverless, y guardar el resultado junto con el output de `90_validar` de las cuatro corridas. `prod` usa el mismo bundle y requiere ejecutar primero el setup en `electrocasa_prod`.

## Consultas Gold

```sql
SELECT * FROM electrocasa_dev.gold.ventas_netas ORDER BY mes, sucursal_id;
SELECT * FROM electrocasa_dev.gold.ranking_productos ORDER BY criterio, posicion;
SELECT * FROM electrocasa_dev.gold.dotacion_mensual ORDER BY mes, sucursal_id;
SELECT * FROM electrocasa_dev.gold.ventas_por_vendedor ORDER BY mes, sucursal_id, vendedor_id;
SELECT * FROM electrocasa_dev.gold.resenas_negativas ORDER BY mes, categoria;
SELECT * FROM electrocasa_dev.gold.despachos_por_courier ORDER BY courier;
```

## Cuarentena y reproceso

`silver.cuarentena` muestra una fila por registro y regla incumplida. `silver.cuarentena_historial` guarda además el momento de procesamiento y el identificador de la corrida. `silver.advertencias` enumera anomalías que conservan la fila de negocio. El notebook de validación cuenta registros rechazados distintos por fuente y también cuenta las violaciones.

Para el reto, elegir en un entorno de prueba un rechazo cuyo valor correcto esté respaldado por la fuente. Localizar el original con el identificador y el archivo de cuarentena, reconstruir el objeto JSON completo con los nombres y tipos de Bronze, y registrar una revisión en `silver.correcciones`. La clave operativa es `(fuente, registro_id, revision)`. Asignar una revisión mayor que las anteriores y registrar el motivo. Ejecutar el job y comprobar que el identificador deja de figurar en la cuarentena activa, figura una sola vez en Silver y conserva las filas anteriores del historial. Volver a ejecutar y demostrar que Silver se mantiene estable. Para un registro con información sensible, conservar la corrección y su evidencia detallada dentro del workspace.

El notebook `96_probar_reproceso` ejecutó las funciones del pipeline en Free Edition con una venta sintética. Aplicó la segunda revisión, pasó de un rechazo a una fila aceptada, conservó Bronze y obtuvo el mismo resultado al repetir la prueba. [Evidencia del reproceso](evidencias/reproceso.json)

## Evidencia de gobierno y entrega

- **Validación y despliegue.** `databricks bundle validate --strict` pasó para `dev` y `prod` en Free Edition con la base `electrocasadb`. El despliegue de `dev` creó el job, el pipeline Medallion y el conector de Tracking.
- **Configuración de dev.** El [run 955070585184459](https://dbc-02d8efd4-96e8.cloud.databricks.com/?o=7474644067615801#job/219946340814219/run/955070585184459) terminó con estado `SUCCESS` y creó el catálogo y los esquemas. Se usó `solo_objetos=true` porque aún no existían los grupos del examen.
- **Preparación del lote 1.** Los seis archivos se verificaron en el volumen de dev. El run `1009293586996167` terminó con estado `SUCCESS` y ejecutó `COPY INTO VALIDATE ALL` sobre Catálogo y Sucursales.
- **Verificación de Tracking.** La consulta federada `01f1e83e-5eac-1c08-ba6a-85256232452a` devolvió 15.139 filas, 15.139 claves distintas y cero cursores nulos desde la conexión de solo lectura.
- **Alternativa de permisos.** La lista de grupos de Free Edition mostraba solo los grupos predeterminados. Se usó `modo_usuario=true` en el run exitoso `205067055147937` para aplicar las máscaras con la identidad del dueño y dar lectura Gold al principal disponible.
- **Verificación local.** Se comprobaron los conteos de los 14 archivos, el esquema del bundle, la sintaxis de los notebooks y los siete contratos. Las seis consultas Gold se probaron sobre datos sintéticos en DuckDB con adaptaciones de dialecto.
- **Verificación Gold en Databricks.** El run `160270565668811` aprobó las siete comprobaciones, incluida la conciliación del neto y la deduplicación de eventos de envío. [Resultados](evidencias/verificaciones-gold.json)
- **Prueba de permisos.** Se capturó `SHOW GRANTS ON SCHEMA electrocasa_dev.silver` antes, durante y después de otorgar temporalmente `SELECT` al principal `account users`. Se revocó el privilegio y se comprobó su ausencia. En esta demostración no se otorgó `USE SCHEMA`. La cuenta dueña pudo consultar el objeto enmascarado de Gold y ver sus valores autorizados. [Evidencia de gobierno](evidencias/gobierno.json)
- Probar las máscaras sobre `gold.empleados_actuales` con Ingeniería y con una identidad de solo lectura. Guardar únicamente los conteos de filas visibles, enmascaradas y con salario nulo. Obtener la evidencia de auditoría y linaje desde Catalog Explorer si Free Edition deniega los grants a `system.access`. Conservar el error y el event log o historial del job.
- Guardar las ejecuciones del bundle y del job, los resultados de Gold y la evidencia de reproceso. Comparar en cada lote los conteos con el output del notebook. Para Tracking, capturar el historial Delta y los IDs de actualización que muestran la carga inicial y la siguiente corrida sin cambios.

| Corrida | Bronze de archivos esperado, acumulado | Tracking esperado | Conteos observados de Silver, Gold y rechazos |
|---|---:|---:|---|
| [Lote 1](evidencias/lote-01.json) | 18.064 | 15.139 insertados, versión Delta 2 | 23.215 filas de negocio Silver, 3.602 Gold y 9.925 registros rechazados |
| [Lote 2](evidencias/lote-02.json) | 34.262 | 15.139 acumulados, cero nuevos, versión Delta 2 | 41.983 filas de negocio Silver, 5.866 Gold y 4.909 registros rechazados |
| [Lote 3](evidencias/lote-03.json) | 40.930 | 15.139 acumulados, versión Delta 2 | 51.830 filas de negocio Silver, 7.347 Gold y 1.561 registros rechazados |
| [Repetición sin archivos nuevos](evidencias/repeticion.json) | 40.930 | 15.139 acumulados, versión Delta 2 | Silver, Gold y rechazos idénticos al lote 3. Solo aumentó el historial de cuarentena por la nueva corrida. |

Los cuatro runs finalizaron en `SUCCESS`. [Lote 1](https://dbc-02d8efd4-96e8.cloud.databricks.com/jobs/70172595208905/runs/628637681475647?o=7474644067615801) · [Lote 2](https://dbc-02d8efd4-96e8.cloud.databricks.com/jobs/70172595208905/runs/1029534711173493?o=7474644067615801) · [Lote 3](https://dbc-02d8efd4-96e8.cloud.databricks.com/jobs/70172595208905/runs/978773737751305?o=7474644067615801) · [Repetición](https://dbc-02d8efd4-96e8.cloud.databricks.com/jobs/70172595208905/runs/160108478881999?o=7474644067615801)
