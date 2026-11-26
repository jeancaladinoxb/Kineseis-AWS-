import argparse
import hashlib
import io
import os
import subprocess
import tarfile
from pathlib import Path

from settings import ACCOUNT, API, INSTANCE_ID, RDS_HOST, ROOT, session


def release():
    files = {"main.py": API / "main.py", "streaming.py": API / "streaming.py",
             "star_jobs.py": API / "star_jobs.py", "frontend/index.html": API / "frontend/index.html",
             "frontend/manager.html": API / "frontend/manager.html",
             "on_ec2.py": ROOT / "ops/on_ec2.py", "001_live_counts.sql": ROOT / "sql/001_live_counts.sql"}
    digest = hashlib.sha256()
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, path in files.items():
            digest.update(name.encode() + b"\0" + path.read_bytes())
            tar.add(path, arcname=name, recursive=False)
    return digest.hexdigest()[:16], buf.getvalue()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=("stage", "migrate", "enable-live", "switch", "rollback", "identity"))
    args = p.parse_args()
    ec2 = session().client("ec2")
    instances = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["airline-api-server"]},
        {"Name": "instance-state-name", "Values": ["running"]}])["Reservations"]
    instance, = [i for r in instances for i in r["Instances"]]
    if instance["InstanceId"] != INSTANCE_ID or instance["KeyName"] != "airline":
        raise RuntimeError("Instancia o clave diferente a la verificada")
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "ConnectTimeout=10",
           "-o", "StrictHostKeyChecking=accept-new", "-i", str(API / "airline.pem")]
    if os.getenv("P3_KNOWN_HOSTS"):
        ssh += ["-o", "UserKnownHostsFile=" + os.environ["P3_KNOWN_HOSTS"]]
    ssh.append("ubuntu@" + instance["PublicIpAddress"])
    revision, data = release()
    directory = "airline-parcial3/releases/" + revision
    if args.action == "stage":
        command = ("umask 077 && mkdir -p airline-parcial3/backups " + directory +
                   " && test -e airline-parcial3/backups/corte2-original.tgz || "
                   "(umask 077 && tar -czf airline-parcial3/backups/corte2-original.tgz -C midterm2 main.py frontend start.sh)")
        subprocess.run(ssh + [command], check=True)
        subprocess.run(ssh + ["tar -xzf - -C " + directory], input=data, check=True)
    else:
        subprocess.run(ssh + ["midterm2/venv/bin/python " + directory + "/on_ec2.py " + args.action +
                              " --account " + ACCOUNT + " --db-host " + RDS_HOST], check=True)
    print(args.action, revision)
