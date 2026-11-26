"""Aprovisiona la zona del parcial. Un nombre ocupado por otra configuración detiene el despliegue."""
import hashlib

from botocore.exceptions import ClientError

from settings import ACCOUNT, BUCKET, DATABASE, DELIVERY, HARNESS, OWNER, ROLE, WORKGROUP, session
from stream import ensure


def lake(aws):
    s3 = aws.client("s3")
    try:
        s3.head_bucket(Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT)
    except ClientError as e:
        if e.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
        s3.create_bucket(Bucket=BUCKET)
    for path in sorted((HARNESS / "ref").glob("*.csv")):
        data = path.read_bytes()
        key = "ref/" + path.name
        try:
            old = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        except s3.exceptions.NoSuchKey:
            s3.put_object(Bucket=BUCKET, Key=key, Body=data, IfNoneMatch="*")
            old = data
        if hashlib.sha256(old).digest() != hashlib.sha256(data).digest():
            raise RuntimeError("Referencia existente diferente: " + key)
    glue = aws.client("glue")
    try:
        glue.get_database(Name=DATABASE)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": DATABASE, "Description": OWNER})
    athena = aws.client("athena")
    workgroups, page = set(), {}
    while True:
        page = athena.list_work_groups(**({"NextToken": page["NextToken"]} if page.get("NextToken") else {}))
        workgroups.update(w["Name"] for w in page["WorkGroups"])
        if not page.get("NextToken"):
            break
    if WORKGROUP not in workgroups:
        athena.create_work_group(Name=WORKGROUP, Description=OWNER, Configuration={
            "EnforceWorkGroupConfiguration": True, "BytesScannedCutoffPerQuery": 1000000000,
            "ResultConfiguration": {"OutputLocation": f"s3://{BUCKET}/athena-results/"}})
    config = athena.get_work_group(WorkGroup=WORKGROUP)["WorkGroup"]["Configuration"]
    if not (0 < config.get("BytesScannedCutoffPerQuery", 0) <= 1000000000):
        raise RuntimeError("El workgroup existente difiere de R10")
    if config.get("ResultConfiguration", {}).get("OutputLocation", "").rstrip("/") != f"s3://{BUCKET}/athena-results":
        raise RuntimeError("Destino inesperado del workgroup")


def delivery(aws, stream_arn):
    firehose = aws.client("firehose")
    try:
        info = firehose.describe_delivery_stream(DeliveryStreamName=DELIVERY)["DeliveryStreamDescription"]
    except firehose.exceptions.ResourceNotFoundException:
        firehose.create_delivery_stream(
            DeliveryStreamName=DELIVERY, DeliveryStreamType="KinesisStreamAsSource",
            KinesisStreamSourceConfiguration={"KinesisStreamARN": stream_arn, "RoleARN": ROLE},
            ExtendedS3DestinationConfiguration={
                "RoleARN": ROLE, "BucketARN": "arn:aws:s3:::" + BUCKET,
                "Prefix": "raw/events/!{timestamp:yyyy/MM/dd/HH}/", "CompressionFormat": "GZIP",
                "ErrorOutputPrefix": "raw/errors/!{firehose:error-output-type}/",
                "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 60}},
            Tags=[{"Key": "Project", "Value": OWNER}])
        return "CREATING"
    dest = info["Destinations"][0]["ExtendedS3DestinationDescription"]
    if (info["Source"]["KinesisStreamSourceDescription"]["KinesisStreamARN"] != stream_arn or
            dest["BucketARN"] != "arn:aws:s3:::" + BUCKET or not dest["Prefix"].startswith("raw/events/") or
            dest["BufferingHints"]["IntervalInSeconds"] > 300):
        raise RuntimeError("Firehose existente no coincide con el diagrama")
    return info["DeliveryStreamStatus"]


if __name__ == "__main__":
    aws = session()
    lake(aws)
    print("Bucket, referencias, Glue y workgroup verificados", flush=True)
    print("Firehose:", delivery(aws, ensure(aws.client("kinesis"))))
