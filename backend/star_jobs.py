"""Un objeto S3 por instalación arbitra las invocaciones concurrentes de refresco."""
import json
import os
import time
import uuid

from botocore.exceptions import ClientError


KEY = "control/refresh.json"
# Una invocación expirada ya no puede publicar ni modificar el estado de otra.
JOB_SECONDS = 900


def read(s3, bucket):
    try:
        obj = s3.get_object(Bucket=bucket, Key=KEY)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
            return {}, None
        raise
    return json.loads(obj["Body"].read()), obj["ETag"]


def write(s3, bucket, state, etag):
    condition = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
    try:
        s3.put_object(Bucket=bucket, Key=KEY, Body=json.dumps(state).encode(),
                      ContentType="application/json", **condition)
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] in (409, 412):
            return False
        raise
    return True


def running(state):
    return state.get("state") == "RUNNING" and time.time() < state["deadline"]


def finish(s3, bucket, job_id, result, **details):
    state, tag = read(s3, bucket)
    if state.get("job_id") == job_id and state["state"] == "RUNNING":
        return write(s3, bucket, dict(state, state=result, **details), tag)
    return False


def start(s3, invoke, bucket):
    for _ in range(8):
        old, tag = read(s3, bucket)
        if running(old):
            return old["job_id"]
        job_id = str(uuid.uuid4())
        state = dict(job_id=job_id, state="RUNNING", deadline=time.time() + JOB_SECONDS)
        if write(s3, bucket, state, tag):
            break
    else:
        raise RuntimeError("Concurrent refresh requests")
    try:
        invoke.invoke(FunctionName=os.environ["STAR_FUNCTION"], InvocationType="Event",
                      Payload=json.dumps({"job_id": job_id}).encode())
    except ClientError:
        finish(s3, bucket, job_id, "FAILED")
        raise
    return job_id


def status(s3, bucket):
    state, _ = read(s3, bucket)
    if state.get("state") == "RUNNING" and not running(state):
        return {"state": "FAILED", "job_id": state["job_id"]}
    return {k: state[k] for k in ("state", "job_id") if k in state} or {"state": "FAILED"}
