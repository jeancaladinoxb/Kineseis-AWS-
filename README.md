# Sistema de reservas de aerolínea: eventos, vista en vivo y esquema estrella

Tercer parcial de Big Data e Ingeniería de Datos. El backend de reservas del segundo parcial publica cada búsqueda, reserva y cancelación en **Amazon Kinesis**. A partir del mismo stream salen dos caminos:

- **Camino rápido (segundos).** Una Lambda consume el stream y registra cada evento en PostgreSQL. `GET /analytics/live?window=5` cuenta por `event_ts` los eventos de los últimos minutos.
- **Camino analítico (minutos).** Firehose entrega los eventos a S3 en JSON. Bajo demanda, una Lambda los limpia (duplicados, eventos tardíos y fuera de orden), construye un **esquema estrella** en Parquet y lo registra en Glue para consultarlo con **Athena**.

**Resultado del harness: 70 / 70** (R1–R12), con la semilla 417. Ver [`parcial3-harness-estudiantes/report.md`](parcial3-harness-estudiantes/report.md).

## Arquitectura

![Arquitectura AWS](aws%20reservations%20system.jpg)

```
API (EC2, FastAPI) ──put_record──► Kinesis live-reservations (1 shard provisionado, 24 h)
                                     │                         │
                                     ▼                         ▼
                      Lambda airline-live-counts      Firehose (gzip, 60 s)
                                     │                         │
                                     ▼                         ▼
                      RDS PostgreSQL midterm2.live_counts   S3 raw/events/
                                     ▲                         │  POST /admin/refresh-star
                      GET /analytics/live                      ▼
                                                    Lambda airline-star-refresh
                                                     (pyarrow → Parquet + Glue)
                                                               │
                                                               ▼
                                                    Athena (workgroup con tope de 1 GB)
```

## Esquema estrella

![Diagrama entidad-relación](diagrama%20estrella.jpg)

| Tabla | Grano |
|---|---|
| `fact_search` | Una fila por evento de búsqueda (`event_id` único). Particionada por `month` (YYYY-MM). |
| `fact_booking_event` | Una fila por creación o cancelación. Una reserva creada y luego cancelada genera dos filas. |
| `dim_date` | Un día de 2023-01-01 a 2027-12-31, con nombre del día, trimestre, festivo y puente. |
| `dim_route` | SCD tipo 2: una versión por categoría de ruta, con `valid_from` / `valid_to` / `is_current`. |
| `dim_fare_class` | ECONOMY y BUSINESS. |

## Estructura del repositorio

| Ruta | Contenido |
|---|---|
| `backend/` | API FastAPI: búsqueda, reservas, cancelación, vista en vivo, inicio del refresco y consulta de su estado. Publicación a Kinesis en `streaming.py`. |
| `implementacion/lambdas/` | `live.py` (consumidor en vivo), `historical.py` + `model.py` (refresco del esquema estrella). |
| `implementacion/ops/` | Creación y borrado del stream, aprovisionamiento de S3/Glue/Athena/Firehose, despliegue reversible en EC2 y prueba de humo. |
| `implementacion/sql/` | Migración de `live_counts` y consultas de negocio Q1–Q8. |
| `implementacion/evidencia/` | Corridas del harness y anexo de uso de IA. |
| `parcial3-harness-estudiantes/` | Harness del curso, `config.yaml`, proyección de costos y reporte final. |

## Reproducir

Requisitos: AWS CLI con credenciales del Learner Lab y Python 3.12 o superior.

```sh
cp implementacion/ops/local.env.example implementacion/ops/local.env   # cuenta, endpoint RDS, ID de la EC2
cd implementacion
ops/crear_stream.sh                       # 1 shard provisionado, 24 h, sin enhanced fan-out
```

Los pasos completos de aprovisionamiento, despliegue y reversión están en [`implementacion/README.md`](implementacion/README.md). Para ejecutar las pruebas:

```sh
cd parcial3-harness-estudiantes
pip install -r requirements.txt
# poner la IP actual de la EC2 en config.yaml (base_url)
pytest
```

Un shard provisionado se cobra por hora: al terminar, ejecutar `implementacion/ops/eliminar_stream.sh`.

## Costos

La proyección mensual está en [`cost_projection_completa.yaml`](parcial3-harness-estudiantes/cost_projection_completa.yaml): 30,98 USD/mes incluyendo EC2 y RDS, frente a un tope de 50 USD. El stream solo existe durante las pruebas.
