# Despliegue y verificación del parcial de reservas

Ejecutar desde esta carpeta en el Mac. El código HTTP y el frontend están en `Corte 2/midterm 2`. Los dos módulos de publicación y control de refrescos acompañan a `main.py`. La cuenta, región y recursos del parcial están declarados en `ops/settings.py`. La configuración histórica de Corte 2 usa el catálogo Glue `midterm2` para ocupación.

## Identificadores locales

La cuenta AWS, el endpoint de RDS y el ID de la instancia EC2 no se guardan en el repositorio. Antes de usar `ops/`:

```sh
cp ops/local.env.example ops/local.env   # completar con los valores del laboratorio
```

`release.py` entrega la cuenta y el host a `on_ec2.py` como argumentos. En `parcial3-harness-estudiantes/config.yaml` el repositorio guarda `base_url: http://YOUR-EC2-IP:8000`; `ops/preflight.py --sync` escribe la IP vigente en la copia local.

## Preparar los recursos

```sh
P3_PY=../parcial3-harness-estudiantes/.venv/bin/python
P3_PACKAGES=$(mktemp -d)
aws sts get-caller-identity --region us-east-1 --output json
"$P3_PY" ops/build.py "$P3_PACKAGES"
ops/crear_stream.sh
"$P3_PY" ops/lake.py
"$P3_PY" ops/functions.py live "$P3_PACKAGES/live.zip"
"$P3_PY" ops/functions.py historical "$P3_PACKAGES/historical.zip"
"```

El script del stream comprueba un shard provisionado, 24 horas de retención y cero consumidores de enhanced fan-out. Firehose agrupa hasta 1 MiB o 60 segundos y entrega JSON concatenado con gzip en `raw/events/`. La Lambda histórica lee también JSON por líneas. El workgroup `airline-star` impone 1.000.000.000 bytes por consulta. Las llamadas CLI usan región y formato explícitos porque el perfil local inspeccionado declaraba valores inválidos.

## Activar el backend y el consumidor

```sh
"$P3_PY" ops/release.py stage
"$P3_PY" ops/release.py migrate
"$P3_PY" ops/release.py identity
"$P3_PY" ops/release.py enable-live
"$P3_PY" ops/release.py switch
"$P3_PY" ops/preflight.py --sync
```

`stage` descubre la IP actual por el ID y la etiqueta de la instancia, conserva el código original en `~/midterm2`, crea un respaldo privado en `~/airline-parcial3/backups` y prepara un directorio de versión. La migración comprueba el servidor y la base anterior antes de crear la base de analítica y su tabla. La contraseña viaja de EC2 a la configuración cifrada de la Lambda mediante el SDK. Los ZIP y el repositorio contienen únicamente código y dependencias. La función en la VPC reutiliza los grupos de RDS y dos subredes. Si caduca la sesión del Learner Lab, renovar el acceso del usuario `ubuntu` por el mecanismo habitual y verificar `identity`.

El acceso transaccional sigue usando `postgres.public.flights` y `postgres.public.reservations`. La consulta en vivo y su consumidor comparten `midterm2.public.live_counts`. El arranque verifica `/health`; para volver a la aplicación anterior:

```sh
"$P3_PY" ops/release.py rollback
```

La reversión conserva reservas, eventos raw y Parquet. El respaldo contiene el localizador del arranque anterior en `start.sh`. La migración es aditiva. Para retirar la extensión se puede regresar a la aplicación anterior sin eliminar la base ni ninguna fila.

## Seguir una búsqueda

```sh
"$P3_PY" ops/smoke.py
```

La prueba llama a `GET /flights/search` con un `X-Session-Id` nuevo. Verifica la respuesta y examina el stream durante 15 segundos para comprobar un solo registro, la clave de partición y los campos de la búsqueda. Guarda su `event_id` en `evidencia/busqueda.json`. La página cliente usa la misma ruta. Cuando la página omite la cabecera, el servidor genera el identificador de sesión.

Para la ventana, consultar `GET /analytics/live?window=5`. PostgreSQL compara `event_ts` con su reloj y deduplica reintentos por la clave primaria. Un evento del replay de 2025 puede aparecer en raw y en Athena sin entrar en la ventana de hoy.

Después de que Firehose entregue el evento, llamar a `POST /admin/refresh-star` y sondear `GET /admin/refresh-status` hasta `SUCCEEDED`. El `job_id` permite seguir la ejecución en CloudWatch. En Athena, seleccionar `airline-star`, base `airline_star`, y sustituir el marcador por el identificador de la evidencia:

```sql
SELECT f.event_id, d.full_date, r.route, f.results_count
FROM fact_search f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_route r ON f.route_key = r.route_key
WHERE f.event_id = '<event_id>';
```

Las ocho consultas de negocio se encuentran en `sql/preguntas.sql`. Consultan también los eventos que genera la interfaz.

## Ejecutar el harness

```sh
"$P3_PY" ops/preflight.py --sync
cd ../parcial3-harness-estudiantes
HARNESS_REPORT=../implementacion/evidencia/bloque-stream .venv/bin/python -m pytest -q tests/test_1_stream.py
HARNESS_REPORT=../implementacion/evidencia/bloque-estrella .venv/bin/python -m pytest -q tests/test_2_star.py
HARNESS_REPORT=../implementacion/evidencia/bloque-costo .venv/bin/python -m pytest -q tests/test_3_cost.py
.venv/bin/python ../implementacion/ops/preflight.py
.venv/bin/python -m pytest -q
```

Ejecutar los bloques y la corrida integral en serie. Cada sesión con replay escribe en el stream y llama a la API. Antes de utilizar datos con otra seed o una asignación privada del profesor, acordar un destino de evaluación limpio si reutiliza los mismos identificadores con contenido diferente. La deduplicación exige que un `event_id` identifique un evento inmutable. La consulta de enero de R10 cuenta todos los eventos de ese mes.

El harness escribe `report.md` y `report.json` en su carpeta. La primera evidencia de R12 está archivada en `evidencia/anterior/`. El archivo de costos original sigue en el harness y la configuración selecciona `cost_projection_completa.yaml`.

## Fin de una sesión de pruebas

Mientras el stream exista acumula horas de shard. Tras la autorización de cierre, `ops/eliminar_stream.sh` elimina el stream del proyecto. Conservar la zona raw, las versiones Parquet, las tablas Glue y la evidencia para la defensa. Para otra sesión hay que comprobar que Firehose y el mapeo de Lambda siguen el stream recién creado, y volver a comprobar la IP de EC2.
