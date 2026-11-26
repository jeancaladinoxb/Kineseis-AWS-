"""Microbatch completo para el volumen del laboratorio."""
import csv
import gzip
import hashlib
import io
import json
import os
import time

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

import star_jobs
from model import SCHEMAS, build


ARROW = {"int": pa.int32(), "bigint": pa.int64(), "string": pa.string(),
         "date": pa.date32(), "boolean": pa.bool_()}
OWNER = "airline-parcial3"


def decode(body):
    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    text = body.decode("utf-8")
    decoder = json.JSONDecoder()
    pos = 0
    while pos < len(text):
        if text[pos].isspace():
            pos += 1
            continue
        event, pos = decoder.raw_decode(text, pos)
        yield event


def raw_events(s3, bucket):
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="raw/events/"):
        for obj in page.get("Contents", []):
            if obj["Size"]:
                yield from decode(s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read())


def reference(s3, bucket, name):
    body = s3.get_object(Bucket=bucket, Key="ref/" + name)["Body"].read().decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(body)))


def parquet(name, rows):
    schema = pa.schema([(k, ARROW[t]) for k, t in SCHEMAS[name]])
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), sink, compression="snappy")
    return sink.getvalue().to_pybytes()


def store(s3, bucket, name, rows, month=None):
    body = parquet(name, rows)
    path = "star/objects/" + name + ("/month=" + month if month else "")
    prefix = f"{path}/{hashlib.sha256(body).hexdigest()}/"
    try:
        s3.put_object(Bucket=bucket, Key=prefix + "data.parquet", Body=body, IfNoneMatch="*")
    except ClientError as e:
        if e.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
            raise
    return f"s3://{bucket}/{prefix}"


def descriptor(name, location):
    return dict(Columns=[{"Name": k, "Type": t} for k, t in SCHEMAS[name]], Location=location,
                InputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                OutputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                SerdeInfo={"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"})


def register_table(glue, db, name, location):
    table = dict(Name=name, TableType="EXTERNAL_TABLE", Parameters={"classification": "parquet",
                 "EXTERNAL": "TRUE", "managed_by": OWNER}, StorageDescriptor=descriptor(name, location),
                 PartitionKeys=[{"Name": "month", "Type": "string"}] if name == "fact_search" else [])
    try:
        current = glue.get_table(DatabaseName=db, Name=name)["Table"]
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=db, TableInput=table)
        return
    if current.get("Parameters", {}).get("managed_by") != OWNER:
        raise RuntimeError("La tabla pertenece a otra instalación: " + name)
    if (current["StorageDescriptor"]["Location"] != location or
            current["StorageDescriptor"]["Columns"] != table["StorageDescriptor"]["Columns"] or
            current.get("PartitionKeys", []) != table["PartitionKeys"]):
        glue.update_table(DatabaseName=db, TableInput=table)


def register_month(glue, db, month, location):
    base = dict(DatabaseName=db, TableName="fact_search")
    partition = dict(Values=[month], StorageDescriptor=descriptor("fact_search", location))
    try:
        old = glue.get_partition(**base, PartitionValues=[month])["Partition"]
    except glue.exceptions.EntityNotFoundException:
        glue.create_partition(**base, PartitionInput=partition)
        return
    if old["StorageDescriptor"]["Location"] != location:
        glue.update_partition(**base, PartitionValueList=[month], PartitionInput=partition)


def handler(event, context):
    bucket, db, job = os.environ["STAR_BUCKET"], os.environ["STAR_DATABASE"], event["job_id"]
    s3, glue = boto3.client("s3"), boto3.client("glue")
    state, tag = star_jobs.read(s3, bucket)
    if state.get("job_id") != job or not star_jobs.running(state) or state.get("invocation"):
        return
    state["invocation"] = context.aws_request_id
    if not star_jobs.write(s3, bucket, state, tag):
        return
    try:
        tables = build(raw_events(s3, bucket), reference(s3, bucket, "routes_ref.csv"),
                       reference(s3, bucket, "route_category_history.csv"),
                       reference(s3, bucket, "holidays_ref.csv"))
        monthly = tables.pop("fact_search")
        locations = {name: store(s3, bucket, name, rows) for name, rows in tables.items()}
        partitions = {m: store(s3, bucket, "fact_search", rows, m) for m, rows in monthly.items()}
        # Todos los objetos ya existen. Publicar dimensiones antes que hechos
        # conserva las claves antiguas válidas durante un cambio de versión.
        if time.time() + 15 >= state["deadline"]:
            raise TimeoutError("Insufficient time to publish catalog")
        for name, location in locations.items():
            register_table(glue, db, name, location)
        register_table(glue, db, "fact_search", f"s3://{bucket}/star/partitions/fact_search/")
        for month, location in sorted(partitions.items()):
            register_month(glue, db, month, location)
        counts = {name: len(rows) for name, rows in tables.items()}
        counts["fact_search"] = sum(map(len, monthly.values()))
        star_jobs.finish(s3, bucket, job, "SUCCEEDED", counts=counts)
        print(json.dumps({"job_id": job, "counts": counts}))
    except Exception as e:
        star_jobs.finish(s3, bucket, job, "FAILED", error=type(e).__name__)
        raise
