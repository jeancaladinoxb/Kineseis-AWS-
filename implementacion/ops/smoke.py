"""Primera prueba de la API desplegada, con un session_id único y evidencia reproducible."""
import argparse
import datetime as dt
import json
import sys
import time
import uuid

import httpx

from preflight import check
from settings import HARNESS, ROOT
sys.path.insert(0, str(HARNESS))
from harness.aws import RealAWS
from harness.util import decode_events


def run(route, day):
    cfg = check()
    aws = RealAWS(cfg["aws"]["region"])
    base, stream = cfg["backend"]["base_url"], cfg["aws"]["kinesis_stream"]
    sid = "smoke-" + uuid.uuid4().hex
    origin, dest = route.split("-")
    since = dt.datetime.now(dt.timezone.utc)
    t = time.monotonic()
    r = httpx.get(base + "/flights/search", headers={"X-Session-Id": sid},
                  params={"origin": origin, "destination": dest, "date": day}, timeout=10)
    r.raise_for_status()
    result_count = len(r.json())
    first = None
    while time.monotonic() - t < 15:
        events = decode_events(aws.kinesis_read_since(stream, since - dt.timedelta(seconds=2), max_seconds=4))
        mine = [(pk, e) for pk, e in events if e.get("session_id") == sid]
        if mine and first is None:
            first = round(time.monotonic() - t, 3)
        time.sleep(0.6)
    events = decode_events(aws.kinesis_read_since(stream, since - dt.timedelta(seconds=2), max_seconds=8))
    mine = [(pk, e) for pk, e in events if e.get("session_id") == sid]
    if len(mine) != 1 or first is None:
        raise RuntimeError(f"{len(mine)} eventos en la ventana de verificación; primera lectura {first}")
    pk, event = mine[0]
    if not (pk == sid and event["event_type"] == "search_performed" and event["schema_version"] == 1 and
            event["origin"] == origin and event["destination"] == dest and event["travel_date"] == day and
            event["results_count"] == result_count and "passenger_name" not in event):
        raise RuntimeError("El evento difiere de la búsqueda HTTP")
    evidence = {"request_utc": since.isoformat(), "http": r.status_code, "base_url": base,
                "stream": stream, "matches": len(mine), "visible_seconds": first,
                "partition_key": pk, "search_results": result_count, "event": event}
    (ROOT / "evidencia/busqueda.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--route", default="BAQ-MDE")
    p.add_argument("--date", default="2023-02-19")
    args = p.parse_args()
    run(args.route, args.date)
