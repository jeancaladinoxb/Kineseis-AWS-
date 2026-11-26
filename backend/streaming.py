"""Publicación del corte 3. El cliente de Kinesis se comparte entre hilos."""
import json
import logging
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3
from botocore.config import Config


log = logging.getLogger(__name__)


class Publisher:
    def __init__(self):
        self.stream = os.getenv("KINESIS_STREAM", "live-reservations")
        self.client = boto3.client(
            "kinesis", region_name=os.getenv("AWS_REGION", "us-east-1"),
            config=Config(connect_timeout=2, read_timeout=4, max_pool_connections=24,
                          retries={"mode": "standard", "total_max_attempts": 4}),
        )
        self.pool = ThreadPoolExecutor(max_workers=12, thread_name_prefix="search-event")

    @staticmethod
    def event(kind, **fields):
        return dict(event_id=str(uuid.uuid4()), event_type=kind,
                    event_ts=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    schema_version=1, **fields)

    def send(self, event, key):
        self.client.put_record(StreamName=self.stream, PartitionKey=key,
                               Data=json.dumps(event, separators=(",", ":")).encode())

    def search(self, session_id, **fields):
        event = self.event("search_performed", session_id=session_id, **fields)
        def publish():
            try:
                self.send(event, session_id)
            except Exception:
                log.error("Kinesis search publication failed: event_id=%s", event["event_id"])
        self.pool.submit(publish)

    def close(self):
        self.pool.shutdown(wait=True)
        self.client.close()
