"""Comprueba los destinos del harness antes de permitir la inyección de eventos."""
import argparse
from pathlib import Path

import yaml

from settings import ACCOUNT, BUCKET, INSTANCE_ID, DATABASE, HARNESS, REGION, STREAM, WORKGROUP, session


def check(sync=False):
    aws = session()
    ec2 = aws.client("ec2")
    i = ec2.describe_instances(InstanceIds=[INSTANCE_ID])["Reservations"][0]["Instances"][0]
    if i["State"]["Name"] != "running" or not any(t == {"Key": "Name", "Value": "airline-api-server"} for t in i["Tags"]):
        raise RuntimeError("La EC2 del parcial no está disponible")
    base = "http://" + i["PublicIpAddress"] + ":8000"
    aws.client("s3").head_bucket(Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT)
    info = aws.client("kinesis").describe_stream_summary(StreamName=STREAM)["StreamDescriptionSummary"]
    if info["StreamStatus"] != "ACTIVE":
        raise RuntimeError("El stream no está activo")
    wg = aws.client("athena").get_work_group(WorkGroup=WORKGROUP)["WorkGroup"]["Configuration"]
    if not 0 < wg.get("BytesScannedCutoffPerQuery", 0) <= 1000000000:
        raise RuntimeError("El workgroup no cumple R10")
    aws.client("glue").get_database(Name=DATABASE)
    path = HARNESS / "config.yaml"
    cfg = yaml.safe_load(path.read_text())
    expected = {"region": REGION, "kinesis_stream": STREAM, "raw_events_uri": f"s3://{BUCKET}/raw/events/",
                "athena_workgroup": WORKGROUP, "athena_database": DATABASE, "athena_output": f"s3://{BUCKET}/athena-results/"}
    cost = "cost_projection_completa.yaml"
    if sync:
        cfg["backend"]["base_url"] = base
        cfg["aws"] = expected
        cfg["cost_projection_file"] = cost
        tmp = path.with_suffix(".yaml.tmp")
        tmp.write_text(yaml.safe_dump(cfg, sort_keys=False))
        tmp.replace(path)
    if cfg["backend"]["base_url"] != base or cfg["aws"] != expected or cfg["cost_projection_file"] != cost:
        raise RuntimeError("Los destinos cambiaron. Revisar y ejecutar preflight.py --sync")
    if not (HARNESS / cost).is_file():
        raise RuntimeError("Falta la copia completa de costos")
    print("EC2", base, "Kinesis", STREAM, "S3", BUCKET, "Glue", DATABASE, "Athena", WORKGROUP, "seed", cfg["seed"])
    return cfg


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--sync", action="store_true")
    check(p.parse_args().sync)
