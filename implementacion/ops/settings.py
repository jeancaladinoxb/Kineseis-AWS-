import os
from pathlib import Path

import boto3


def _load_local_env(path=Path(__file__).with_name("local.env")):
    """Carga ops/local.env (fuera del repo) sin sobrescribir variables ya definidas."""
    if path.is_file():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())


def _required(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Falta {name}: copie ops/local.env.example a ops/local.env y complételo")
    return value


_load_local_env()

REGION = "us-east-1"
ACCOUNT = _required("P3_AWS_ACCOUNT")
RDS_HOST = _required("P3_RDS_HOST")
INSTANCE_ID = _required("P3_INSTANCE_ID")
BUCKET = "midterm-kinesis-jarl-100093"
STREAM = "live-reservations"
DELIVERY = "airline-reservations-to-s3"
DATABASE = "airline_star"
WORKGROUP = "airline-star"
LIVE = "airline-live-counts"
STAR = "airline-star-refresh"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/LabRole"
OWNER = "airline-parcial3"
ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT.parent / "parcial3-harness-estudiantes"
API = ROOT.parents[2] / "Corte 2" / "midterm 2"


def session():
    aws = boto3.Session(region_name=REGION)
    if aws.client("sts").get_caller_identity()["Account"] != ACCOUNT:
        raise RuntimeError("La cuenta AWS difiere de la verificada para este proyecto")
    return aws
