import hashlib
import json
import os
import shutil
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WHEEL_DIR = ROOT / "wheels"
JAR_DIR = ROOT / "spark" / "jars"
MAVEN = "https://repo.maven.apache.org/maven2/"
DRIVERS = {
    "org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar":
        "a2644cbfba1baa145ff7e8c8ef582a6eed7a7ec4ca792f7f054122bdec756268",
    "com/clickhouse/clickhouse-jdbc/0.7.2/clickhouse-jdbc-0.7.2-shaded-all.jar":
        "13e2162f7a347e5a767577b67864ec07960bd148e4ef352f5ad0b53fa0797dd5",
}


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def matches(path, expected):
    return path.is_file() and digest(path) == expected


def download(path, url, expected):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".part", delete=False) as tmp:
        partial = Path(tmp.name)
    try:
        print("Fetching", path.name, flush=True)
        with urllib.request.urlopen(url, timeout=60) as response, partial.open("wb") as stream:
            shutil.copyfileobj(response, stream)
        if digest(partial) != expected:
            raise ValueError("Checksum mismatch for " + path.name)
        partial.chmod(0o644)
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)


def wheels():
    for line in (WHEEL_DIR / "SHA256SUMS").read_text().splitlines():
        checksum, name = line.split()
        yield WHEEL_DIR / name, checksum


def ensure_wheel(path, checksum):
    if matches(path, checksum):
        return path
    package, version, _ = path.name.split("-", 2)
    with urllib.request.urlopen(f"https://pypi.org/pypi/{package}/{version}/json", timeout=30) as response:
        files = json.load(response)["urls"]
    for item in files:
        if item["filename"] == path.name and item["digests"]["sha256"] == checksum:
            download(path, item["url"], checksum)
            return path
    raise ValueError("Pinned wheel unavailable: " + path.name)


def yaml_wheel():
    for path, checksum in wheels():
        if path.name.lower().startswith("pyyaml-"):
            return ensure_wheel(path, checksum)
    raise ValueError("PyYAML missing from wheel manifest")


def prepare():
    for path, checksum in wheels():
        ensure_wheel(path, checksum)
    for artifact, checksum in DRIVERS.items():
        path = JAR_DIR / artifact.rsplit("/", 1)[1]
        if not matches(path, checksum):
            download(path, MAVEN + artifact, checksum)


if __name__ == "__main__":
    prepare()
