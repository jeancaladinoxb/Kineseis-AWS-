# Anexo de uso de inteligencia artificial

**25 de noviembre de 2026 · Bogotá.** El texto completo de la solicitud inicial está en `solicitud-original.txt`. El usuario pidió inspeccionar primero Corte 2, respetar sus diagramas, desplegar y probar en su Learner Lab. Limitó la autorización a los componentes del parcial y pidió resolver contradicciones antes de intervenir.

| Punto planteado por el asistente | Respuesta del responsable del proyecto | Aplicación |
|---|---|---|
| Nombres del dibujo y valores de ejemplo del harness | «Crear los dos del diagrama y apuntar allí el harness». | Stream `live-reservations` y bucket `midterm-kinesis-jarl-100093`. |
| El archivo que había pasado R12 omite instancias y contiene una clave Glue mal escrita | «Actualizar una copia con costos completos y volver a verificar R12». | Copia completa y archivo anterior conservado. |
| Workgroup compartido sin límite de escaneo | «Crear el workgroup airline-star solo para Corte 3». | Tope de 1.000.000.000 bytes. |
| Falta de perfil IAM en EC2 | «Conservar el acceso actual y comprobarlo por separado». | Se probó STS desde el entorno de la aplicación. |
| STS en EC2 devolvió InvalidClientTokenId | «Renovaré las credenciales en EC2 y te avisaré». | La renovación corresponde al responsable de la cuenta. |
| La API de Corte 2 usa postgres y el catálogo no contenía una base PostgreSQL midterm2 | «Crear midterm2 solo para live_counts y conservar las reservas en postgres». | Dos conexiones al mismo servidor RDS. |
| Las tres rutas de los vuelos existentes carecen de fila en la referencia 417 | «Conservar cada ruta y marcar región y categoría como UNKNOWN hasta recibir referencias». | Claves estables por ruta y fecha efectiva. |

## Segunda sesión · 25–26 de noviembre de 2026

El usuario pidió a un segundo asistente (Claude Code) revisar lo implementado frente a la solicitud original y consultarle antes de cada cambio. La revisión inicial fue de solo lectura: archivos locales, recursos AWS y estado de la EC2.

| Punto planteado por el asistente | Respuesta del responsable del proyecto | Aplicación |
|---|---|---|
| La API estaba detenida tras un reinicio de la EC2 y `~/.aws/credentials` estaba vencido. Opciones: A) pegar credenciales nuevas del laboratorio, B) cambiar el perfil de instancia a LabRole | «Hice la opción A». | Se verificaron STS, Lambda, Kinesis y S3 desde la EC2. |
| La IP pública cambió al reiniciar el laboratorio. Faltaban `enable-live`, `switch` y actualizar `config.yaml` | «Los tres». | Trigger Kinesis → `airline-live-counts` activo, API nueva en el puerto 8000 y `base_url` actualizado. Prueba de humo: un `search_performed` visible en el stream en 1,6 s y contado en `/analytics/live`. |
| Primer refresco del esquema estrella o refresco junto con el harness | «Refresca primero». | `SUCCEEDED` en 5,5 s: `dim_date` 1.826 filas, `dim_route` 15 versiones (SCD 2), `fact_search` 17.001 filas, `fact_booking_event` 2.912 filas; tablas y particiones registradas en Glue. |
| Ejecutar el harness | «Sí». | Tres corridas completas desde el Mac (`corrida-mac-1..3`), 70/70 en cada una. |
| Semilla del equipo | «Sí, semilla 417». | Se mantiene la 417. |
| Ejecutar el harness desde la EC2 | «Sube a la EC2 el harness y ejecuta desde allí; aún no elimines el stream». | Copia en `~/parcial3-harness` usando el Python de `~/midterm2/venv` (el disco tenía unos 1 GB libres). 70/70: p95 0,306 s, vista en vivo en 1,0 s, S3 en 46 s, refresco en 10 s. |
| Eliminar el stream | «Elimina el Kinesis». | No se pudo: la sesión del laboratorio había terminado (deny de `voc-cancel-cred`). Pendiente hasta renovar las credenciales. |
| `report.md` de la carpeta del harness seguía siendo el de 2/70 | «Cópialo». | Se copió el 70/70; el anterior está en `anterior/`. |
| Contenido del repositorio | Backend en `backend/`; harness sin `.venv`, sin enunciado ni zip; autor `jeancaladinoxb` con email noreply; el documento de las secciones 5 y 6 se entrega aparte. | `.gitignore` excluye entornos, llaves `.pem`, `.env` y credenciales. |
| La cuenta AWS, el host RDS, el ID de la EC2 y la IP aparecían en el código | «No subas eso»; variables locales y marcador para la IP. | `ops/local.env` (ignorado por git) con `local.env.example`; `config.yaml` y `busqueda.json` con `YOUR-EC2-IP`. |

El asistente inspeccionó el código y la configuración, escribió los componentes de software y preparó los comandos de despliegue y las comprobaciones. Para distinguir una comprobación del diseño se conservan las respuestas del harness y las mediciones de la API. Los registros excluyen valores de conexión, contraseñas y tokens.
