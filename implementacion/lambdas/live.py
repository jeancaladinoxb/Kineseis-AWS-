"""Consumidor Kinesis estándar. Un commit confirma el lote completo al servicio."""
import base64
import json
import os

import psycopg2
from psycopg2.extras import execute_values

connection = None


def handler(event, context):
    global connection
    rows = []
    for record in event["Records"]:
        data = json.loads(base64.b64decode(record["kinesis"]["data"]))
        if data["schema_version"] != 1:
            raise ValueError("Unsupported event schema")
        rows.append((data["event_id"], data["event_type"], data["event_ts"]))
    if not rows:
        return
    try:
        if connection is None or connection.closed:
            connection = psycopg2.connect(os.environ["LIVE_DSN"], connect_timeout=4,
                                          application_name="airline-live-consumer", sslmode="require")
        with connection, connection.cursor() as cur:
            execute_values(cur, """
                INSERT INTO public.live_counts(event_id,event_type,event_ts) VALUES %s
                ON CONFLICT (event_id) DO NOTHING
            """, rows, page_size=500)
    except Exception:
        if connection is not None:
            connection.close()
        connection = None
        raise
