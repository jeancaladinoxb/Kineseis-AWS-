"""Se ejecuta en EC2. Reutiliza la conexión de Corte 2 sin sacar el secreto de AWS."""
import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen

import boto3
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


OLD = Path.home() / "midterm2"
RELEASE = Path(__file__).resolve().parent
CONTROL = Path.home() / "airline-parcial3"
PIDFILE = CONTROL / "server.pid"
# release.py los entrega como argumentos desde ops/local.env; no se guardan en el repositorio.
HOST = None
ACCOUNT = None
REGION = "us-east-1"


def urls():
    # La .env del proyecto fue inspeccionada: usa una asignación literal DATABASE_URL.
    line = re.search(r"^\s*(?:export\s+)?DATABASE_URL\s*=\s*(.*?)\s*$",
                     (OLD / ".env").read_text(), re.MULTILINE)
    if not line:
        raise RuntimeError("DATABASE_URL ausente")
    base = make_url(line[1].strip().strip(chr(39) + chr(34)))
    if base.host != HOST:
        raise RuntimeError("El host PostgreSQL de la aplicación ha cambiado")
    return base, base.set(database="midterm2")


def migrate():
    base, live = urls()
    with create_engine(base, isolation_level="AUTOCOMMIT").connect() as c:
        if c.execute(text("SELECT current_database()")).scalar() != "postgres":
            raise RuntimeError("La conexión de las reservas ha cambiado")
        if not c.execute(text("SELECT EXISTS(SELECT FROM pg_database WHERE datname='midterm2')")).scalar():
            c.execute(text("CREATE DATABASE midterm2"))
    with psycopg2.connect(live.set(drivername="postgresql").render_as_string(hide_password=False),
                          sslmode="require") as db, db.cursor() as c:
        c.execute((RELEASE / "001_live_counts.sql").read_text())
        c.execute("SELECT current_database(), to_regclass('public.live_counts')::text")
        print("Migración verificada", c.fetchone())


def identity():
    aws = boto3.Session(region_name=REGION)
    if aws.client("sts").get_caller_identity()["Account"] != ACCOUNT:
        raise RuntimeError("EC2 no está autenticada en la cuenta del proyecto")
    return aws


def enable_live():
    aws = identity()
    fn, kin = aws.client("lambda"), aws.client("kinesis")
    name = "airline-live-counts"
    config = fn.get_function_configuration(FunctionName=name)
    env = config.get("Environment", {}).get("Variables", {})
    dsn = urls()[1].set(drivername="postgresql").render_as_string(hide_password=False)
    if env.get("LIVE_DSN") != dsn:
        fn.update_function_configuration(FunctionName=name, RevisionId=config["RevisionId"],
                                         Environment={"Variables": {**env, "LIVE_DSN": dsn}})
        fn.get_waiter("function_updated_v2").wait(FunctionName=name)
    arn = kin.describe_stream_summary(StreamName="live-reservations")["StreamDescriptionSummary"]["StreamARN"]
    mappings = fn.list_event_source_mappings(EventSourceArn=arn, FunctionName=name)["EventSourceMappings"]
    if not mappings:
        mapping = fn.create_event_source_mapping(FunctionName=name, EventSourceArn=arn, StartingPosition="TRIM_HORIZON",
                                                BatchSize=500, MaximumBatchingWindowInSeconds=1, Enabled=True)
    else:
        mapping, = mappings
    for _ in range(90):
        info = fn.get_event_source_mapping(UUID=mapping["UUID"])
        if info["State"] == "Enabled":
            print("Consumidor estándar habilitado")
            return
        if info["State"] not in ("Creating", "Enabling", "Updating"):
            raise RuntimeError("Estado del consumidor: " + info["State"])
        time.sleep(2)
    raise RuntimeError("No se habilitó el consumidor en tres minutos")


def stop_ours():
    if not PIDFILE.exists():
        return
    pid = int(PIDFILE.read_text())
    proc = Path("/proc") / str(pid)
    if proc.exists():
        cwd = (proc / "cwd").resolve()
        if not (CONTROL / "releases") in cwd.parents and cwd != OLD:
            raise RuntimeError("El PID registrado pertenece a otro proceso")
        if b"uvicorn" not in (proc / "cmdline").read_bytes():
            raise RuntimeError("El PID registrado no es Uvicorn")
        os.kill(pid, signal.SIGTERM)
        for _ in range(100):
            if not proc.exists() or (proc / "stat").read_text().split()[2] == "Z":
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("El proceso anterior no terminó")
    PIDFILE.unlink(missing_ok=True)


def launch(directory):
    base, live = urls()
    env = dict(os.environ, DATABASE_URL=base.render_as_string(hide_password=False),
               AWS_REGION=REGION, AWS_DEFAULT_REGION=REGION)
    if directory != OLD:
        env.update(LIVE_DATABASE_URL=live.render_as_string(hide_password=False),
                   KINESIS_STREAM="live-reservations", STAR_BUCKET="midterm-kinesis-jarl-100093",
                   STAR_FUNCTION="airline-star-refresh")
    with (CONTROL / "uvicorn.log").open("ab") as log:
        child = subprocess.Popen([str(OLD / "venv/bin/python"), "-m", "uvicorn", "main:app",
                                  "--host", "0.0.0.0", "--port", "8000"], cwd=directory, env=env,
                                 stdout=log, stderr=log, stdin=subprocess.DEVNULL, start_new_session=True)
    PIDFILE.write_text(str(child.pid))
    for _ in range(50):
        if child.poll() is not None:
            raise RuntimeError("Uvicorn terminó durante el arranque")
        try:
            with urlopen("http://127.0.0.1:8000/health", timeout=1) as r:
                if r.status == 200 and child.poll() is None:
                    print("Uvicorn activo", directory)
                    return
        except OSError:
            pass
        time.sleep(0.3)
    raise RuntimeError("Falló el control de salud")


def switch(rollback=False):
    if not rollback:
        identity()
    previous = OLD
    if PIDFILE.exists():
        proc = Path("/proc") / PIDFILE.read_text() / "cwd"
        if proc.exists():
            previous = proc.resolve()
    elif subprocess.run(["ss", "-Hltn", "sport", "=", ":8000"], capture_output=True).stdout.strip():
        raise RuntimeError("Otro proceso ocupa el puerto 8000; se requiere identificarlo")
    stop_ours()
    try:
        launch(OLD if rollback else RELEASE)
    except Exception:
        stop_ours()
        if not rollback:
            launch(previous)
        raise


if __name__ == "__main__":
    os.umask(0o077)
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=["migrate", "enable-live", "switch", "rollback", "identity"])
    p.add_argument("--account", required=True)
    p.add_argument("--db-host", required=True)
    parsed = p.parse_args()
    action, ACCOUNT, HOST = parsed.action, parsed.account, parsed.db_host
    try:
        if action == "migrate":
            migrate()
        elif action == "enable-live":
            enable_live()
        elif action == "identity":
            identity()
            print("Identidad EC2/cuenta verificada")
        else:
            switch(action == "rollback")
    except Exception as exc:
        # Los errores de conectividad pueden traer el DSN. Solo exponer tipo y código AWS.
        reason = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        message = str(exc) if isinstance(exc, RuntimeError) else ""
        print("Operación fallida", type(exc).__name__, reason, message, file=sys.stderr)
        sys.exit(1)
