"""Instala el código. EC2 configura la conexión privada del consumidor antes de activar el trigger."""
import argparse
import base64
import hashlib
from pathlib import Path

from settings import BUCKET, DATABASE, LIVE, OWNER, ROLE, STAR, session


def install(aws, kind, artifact):
    fn = aws.client("lambda")
    name = LIVE if kind == "live" else STAR
    data = artifact.read_bytes()
    digest = hashlib.sha256(data).digest()
    key = "deployment/" + digest.hex() + "/" + artifact.name
    s3 = aws.client("s3")
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
    except Exception as exc:
        if getattr(exc, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode") != 404:
            raise
        s3.upload_file(str(artifact), BUCKET, key)
    code = dict(S3Bucket=BUCKET, S3Key=key)
    try:
        old = fn.get_function(FunctionName=name)
    except fn.exceptions.ResourceNotFoundException:
        kwargs = {}
        if kind == "live":
            db = aws.client("rds").describe_db_instances(DBInstanceIdentifier="airline-db")["DBInstances"][0]
            subnets = [s["SubnetIdentifier"] for s in db["DBSubnetGroup"]["Subnets"] if s["SubnetStatus"] == "Active"]
            kwargs["VpcConfig"] = {"SubnetIds": sorted(subnets)[:2],
                                   "SecurityGroupIds": [g["VpcSecurityGroupId"] for g in db["VpcSecurityGroups"]]}
        else:
            kwargs["Environment"] = {"Variables": {"STAR_BUCKET": BUCKET, "STAR_DATABASE": DATABASE}}
        fn.create_function(FunctionName=name, Role=ROLE, Runtime="python3.12", Architectures=["x86_64"],
                           Code=code, Handler=kind + ".handler", MemorySize=256 if kind == "live" else 1024,
                           Timeout=45 if kind == "live" else 840, Tags={"Project": OWNER}, **kwargs)
        fn.get_waiter("function_active_v2").wait(FunctionName=name)
    else:
        if old.get("Tags", {}).get("Project") != OWNER:
            raise RuntimeError("Función existente sin etiqueta del proyecto: " + name)
        if old["Configuration"]["CodeSha256"] != base64.b64encode(digest).decode():
            fn.update_function_code(FunctionName=name, **code)
            fn.get_waiter("function_updated_v2").wait(FunctionName=name)
    print(name, fn.get_function_configuration(FunctionName=name)["State"])


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("kind", choices=("live", "historical"))
    p.add_argument("artifact", type=Path)
    args = p.parse_args()
    install(session(), args.kind, args.artifact)
