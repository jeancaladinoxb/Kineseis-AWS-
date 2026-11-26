"""Empaqueta binarios de Linux x86_64 para Python 3.12, incluso al ejecutarse desde macOS."""
import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from settings import API, ROOT


def package(output, name, libraries, sources):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        subprocess.run([sys.executable, "-m", "pip", "install", "--disable-pip-version-check", "--no-compile",
                        "--only-binary=:all:", "--platform", "manylinux2014_x86_64",
                        "--platform", "manylinux_2_28_x86_64", "--implementation", "cp",
                        "--python-version", "3.12", "--abi", "cp312", "--target", directory, *libraries], check=True)
        for src in sources:
            shutil.copyfile(src, root / src.name)
        files = sorted(p for p in root.rglob("*") if p.is_file() and
                       not ({"tests", "__pycache__", "include"} & set(p.relative_to(root).parts)))
        if sum(p.stat().st_size for p in files) > 250 * 1024**2:
            raise RuntimeError("Lambda ZIP exceeds 250 MiB uncompressed")
        with ZipFile(output / (name + ".zip"), "w", ZIP_DEFLATED) as out:
            for p in files:
                out.write(p, p.relative_to(root))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(exist_ok=True, parents=True)
    src = ROOT / "lambdas"
    package(args.output, "live", ["psycopg2-binary==2.9.10"], [src / "live.py"])
    package(args.output, "historical", ["pyarrow==19.0.1", "numpy==1.26.4"],
            [src / "historical.py", src / "model.py", API / "star_jobs.py"])
