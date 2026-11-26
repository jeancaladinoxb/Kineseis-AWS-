"""Ejecuta pytest sin alterar fixtures; guarda comando, tiempos, salida y reportes."""
import argparse
import ast
import datetime as dt
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

from preflight import check
from settings import HARNESS, ROOT


TARGETS = {
    "costo": ["tests/test_3_cost.py"],
    "landing": ["tests/test_1_stream.py::test_events_reach_landing_zone"],
    "workgroup": ["tests/test_2_star.py::test_workgroup_has_scan_limit"],
    "stream": ["tests/test_1_stream.py"],
    "estrella": ["tests/test_2_star.py"], "integral": [],
}


def run(kind):
    cfg = check()
    directory = ROOT / "evidencia" / kind
    directory.mkdir(exist_ok=True)
    command = [str(HARNESS / ".venv/bin/python"), "-m", "pytest", "-q", "--tb=short", *TARGETS[kind]]
    source = HARNESS / "report" if kind == "integral" else directory / "report"
    env = dict(os.environ, HARNESS_REPORT=str(source), AWS_DEFAULT_REGION=cfg["aws"]["region"],
               AWS_REGION=cfg["aws"]["region"], PYTHONUNBUFFERED="1")
    start = dt.datetime.now(dt.timezone.utc)
    with (directory / "pytest.log").open("w") as f:
        result = subprocess.run(command, cwd=HARNESS, env=env, stdout=f, stderr=subprocess.STDOUT)
    end = dt.datetime.now(dt.timezone.utc)
    paths = [source.with_suffix(ext) for ext in (".md", ".json")]
    if any(not p.exists() or p.stat().st_mtime < start.timestamp()-1 for p in paths):
        raise RuntimeError("El harness no generó ambos reportes en esta sesión")
    report = json.loads(paths[1].read_text())
    tested = {t["name"] for r in report["requirements"] for t in r["tests"]}
    if kind == "integral":
        expected = {n.name for p in (HARNESS/"tests").glob("test_*.py") for n in ast.parse(p.read_text()).body
                    if isinstance(n, ast.FunctionDef) and n.name.startswith("test_")}
        if tested != expected:
            raise RuntimeError("Los reportes no corresponden a la colección íntegra")
        for p in paths:
            shutil.copyfile(p, directory / p.name)
    metadata = dict(command=command, cwd=str(HARNESS), start_utc=start.isoformat(), end_utc=end.isoformat(),
                    exit_code=result.returncode, tests_recorded=len(tested), score=report["points"])
    (directory/"ejecucion.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(kind, metadata, flush=True)
    return result.returncode


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("block", choices=TARGETS)
    sys.exit(run(p.parse_args().block))
