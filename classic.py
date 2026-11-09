import argparse
import base64
import fcntl
import getpass
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import time
import urllib.request
import urllib.parse
import uuid
import build_inputs
import port_priority
from pathlib import Path

ROOT = Path(__file__).resolve().parent
STATE = ROOT / ".runtime"
ENV = ROOT / ".env"
PROJECT = "northwind_classic"
COMMIT = "2f85f3724ee45776a5183ed34d064488a6e1dc53"
DIGEST = "3cc62b3fca6d244a47dbde698b809331e4f85988a0685b2b370717d431e94871"
BASE = [
    "northwind_sqlserver",
    "northwind_postgres",
    "northwind_clickhouse",
    "northwind_kafka",
    "airflow-db",
]
APPS = [
    "spark-master",
    "spark-worker",
    "airflow-webserver",
    "airflow-scheduler",
    "ops-console",
    "spark-ui",
    "sms-monitor",
]
STREAM = [
    "customer_consumer",
    "product_consumer",
    "factorders_consumer",
    "customer_cdc_reader",
    "product_cdc_reader",
    "orders_cdc_reader",
    "orderdetails_cdc_reader",
]
TOPICS = [
    "dimcustomer_changes",
    "dimproducts_changes",
    "orders_changes",
    "orderdetails_changes",
]


def read_env(path):
    data = {}
    for line in path.read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        k, v = line.split("=", 1)
        data[k.strip()] = v.strip().strip("'").strip('"')
    return data


def save_env(values):
    tmp = ENV.with_suffix(".tmp")
    fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w") as out:
        for key, v in values.items():
            if "\n" in v or "\r" in v or "'" in v:
                raise ValueError("Unsupported character in setting " + key)
            out.write(f"{key}='{v}'\n")
    tmp.chmod(0o600)
    tmp.replace(ENV)


def configure():
    values = read_env(ROOT / ".env.example")
    if ENV.is_file():
        values.update(read_env(ENV))
    if values.pop("SMS_PROVIDER", "sunway").strip().lower() != "sunway":
        values["SUNWAY_ENABLED"] = "false"
    for key, port in {
        "AIRFLOW_PORT": 8080,
        "OPS_PORT": 8090,
        "KAFKA_PORT": 9092,
        "SPARK_PORT": 8081,
    }.items():
        values[key] = str(port)
    values["AIRFLOW_ADMIN_PASSWORD"] = "admin"
    keys = [
        "SQLSERVER_PASSWORD",
        "POSTGRES_PASSWORD",
        "CLICKHOUSE_PASSWORD",
        "AIRFLOW_DB_PASSWORD",
        "AIRFLOW_ADMIN_PASSWORD",
        "FERNET_KEY",
        "WEB_SECRET",
        "KAFKA_CLUSTER_ID",
    ]
    missing = [k for k in keys if not values.get(k)]
    if missing:
        result = subprocess.run(
            [
                "docker",
                "volume",
                "ls",
                "-q",
                "--filter",
                "label=com.docker.compose.project=" + PROJECT,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        if result.stdout.strip():
            raise RuntimeError(
                "Restore the classic .env for the existing classic volumes"
            )
        for k in missing:
            if k == "FERNET_KEY":
                values[k] = base64.urlsafe_b64encode(os.urandom(32)).decode()
            elif k == "KAFKA_CLUSTER_ID":
                values[k] = (
                    base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")
                )
            else:
                values[k] = "Aa1!" + secrets.token_urlsafe(24)
    values["AIRFLOW_DB_PASSWORD_URI"] = urllib.parse.quote(
        values["AIRFLOW_DB_PASSWORD"], safe=""
    )
    values["DOCKER_GID"] = str(Path("/var/run/docker.sock").stat().st_gid)
    save_env(values)
    return values


def compose(*args, capture=False, body=None, timeout=1800):
    values = read_env(ENV)
    cmd = [
        "docker",
        "compose",
        "-p",
        PROJECT,
        "--env-file",
        str(ENV),
        "-f",
        str(ROOT / "docker-compose.yml"),
        *args,
    ]
    p = subprocess.run(
        cmd,
        cwd=ROOT,
        env={**os.environ, **values},
        text=True,
        input=body,
        capture_output=capture,
        timeout=timeout,
    )
    if p.returncode:
        if capture:
            print(p.stderr[-1200:], file=sys.stderr)
        raise RuntimeError("Docker Compose failed in " + str(args[0]))
    return (p.stdout or "").strip()


def sql(query, db="master"):
    return compose(
        "exec",
        "-T",
        "northwind_sqlserver",
        "sh",
        "-c",
        'SQLCMDPASSWORD="$MSSQL_SA_PASSWORD" exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -C -b -h -1 -W -d "$1" -Q "$2"',
        "sh",
        db,
        "SET NOCOUNT ON; " + query,
        capture=True,
    )


def script(text, db):
    compose(
        "exec",
        "-T",
        "northwind_sqlserver",
        "sh",
        "-c",
        """set -eu
file=$(mktemp /tmp/northwind-XXXXXXXX.sql)
trap 'rm -f "$file"' EXIT
cat > "$file"
SQLCMDPASSWORD="$MSSQL_SA_PASSWORD" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -C -b -d "$1" -f 65001 -i "$file"
""",
        "sh",
        db,
        body=text,
    )


def source_bytes():
    cache = ROOT / ".cache" / "instnwnd.sql"
    if cache.exists():
        raw = cache.read_bytes()
    else:
        url = (
            "https://raw.githubusercontent.com/microsoft/sql-server-samples/"
            + COMMIT
            + "/samples/databases/northwind-pubs/instnwnd.sql"
        )
        with urllib.request.urlopen(url, timeout=90) as response:
            raw = response.read(1_100_000)
    if hashlib.sha256(raw).hexdigest() != DIGEST:
        raise ValueError("Northwind sample checksum mismatch")
    if not cache.exists():
        cache.parent.mkdir(exist_ok=True)
        cache.write_bytes(raw)
    return raw.decode("utf-8-sig")


def prepare_source():
    if sql("SELECT COUNT(*) FROM sys.databases WHERE name='Northwind'") == "0":
        raw = source_bytes()
        stage = "Northwind_Classic_Seed"
        if (
            sql(
                "SELECT COUNT(*) FROM sys.databases WHERE name='Northwind_Classic_Seed'"
            )
            == "0"
        ):
            sql("EXEC(N'CREATE DATABASE Northwind_Classic_Seed')")
        if sql("SELECT COUNT(*) FROM sys.tables WHERE is_ms_shipped=0", stage) == "0":
            script(
                "SET XACT_ABORT ON\nBEGIN TRANSACTION\nGO\n"
                + raw
                + "\nGO\nEXEC sys.sp_addextendedproperty @name=N'classic_seed',@value=N'"
                + DIGEST
                + "'\nCOMMIT TRANSACTION\nGO\n",
                stage,
            )
        if (
            sql(
                "SELECT COUNT(*) FROM sys.extended_properties WHERE class=0 AND name='classic_seed' AND CONVERT(nvarchar(64),value)='"
                + DIGEST
                + "'",
                stage,
            )
            != "1"
        ):
            raise RuntimeError("Unrecognized seed database; import stopped")
        if sql("SELECT COUNT(*) FROM dbo.[Order Details]", stage) != "2155":
            raise RuntimeError("Sample import did not produce all 2155 order lines")
        sql("ALTER DATABASE Northwind_Classic_Seed MODIFY NAME=Northwind")
    if (
        sql(
            "SELECT COUNT(*) FROM sys.tables WHERE schema_id=SCHEMA_ID('dbo') AND name IN ('Customers','Products','Categories','Orders','Order Details','Shippers','Suppliers','Employees')",
            "Northwind",
        )
        != "8"
    ):
        raise RuntimeError("The existing Northwind source is incomplete")
    if sql("SELECT COUNT(*) FROM sys.databases WHERE name='NorthwindDW'") == "0":
        sql("EXEC(N'CREATE DATABASE NorthwindDW')")
    script((ROOT / "sql/warehouse.sql").read_text(), "NorthwindDW")


def pg(query, meta=False):
    service, user, db = (
        ("airflow-db", "airflow", "airflow")
        if meta
        else ("northwind_postgres", "postgres", "Staging_Northwind")
    )
    return compose(
        "exec",
        "-T",
        service,
        "psql",
        "-X",
        "-U",
        user,
        "-d",
        db,
        "-At",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        query,
        capture=True,
    )


def toolbox(name):
    return compose(
        "run", "--rm", "--no-deps", "toolbox", "python", "-u", "/app/" + name
    )


def wait_for(check, purpose, seconds):
    end = time.monotonic() + seconds
    while not check():
        if time.monotonic() >= end:
            raise RuntimeError("Timed out waiting for " + purpose)
        time.sleep(3)


def wait_dag(name):
    wait_for(
        lambda: pg(
            "SELECT count(*) FROM dag WHERE dag_id='"
            + name
            + "' AND is_active AND NOT is_paused",
            True,
        )
        == "1",
        "active DAG " + name,
        180,
    )


def trigger(name, rid, conf):
    compose(
        "exec",
        "-T",
        "airflow-scheduler",
        "airflow",
        "dags",
        "trigger",
        name,
        "--run-id",
        rid,
        "--conf",
        json.dumps(conf),
    )


def batch():
    name = "northwind_etl"
    wait_dag(name)
    path = STATE / "batch.json"
    rid = json.loads(path.read_text())["run_id"] if path.is_file() else ""
    if rid and not re.fullmatch("classic_[a-f0-9]{32}", rid):
        raise RuntimeError("Invalid batch journal")

    def state():
        return pg(
            "SELECT state FROM dag_run WHERE dag_id='northwind_etl' AND run_id='"
            + rid
            + "'",
            True,
        )

    if not rid or state() == "failed":
        rid = "classic_" + uuid.uuid4().hex
        path.write_text(json.dumps({"run_id": rid}))
    if not state():
        if (
            pg(
                "SELECT count(*) FROM dag_run WHERE dag_id='northwind_etl' AND state IN ('running','queued')",
                True,
            )
            != "0"
        ):
            raise RuntimeError("An ETL run is already active")
        trigger(name, rid, {})

    def finished():
        value = state()
        if value == "failed":
            raise RuntimeError(
                "ETL failed. Inspect the failed task in Airflow; correct it and run up again"
            )
        return value == "success"

    wait_for(finished, "initial ETL " + rid, 1800)


def up(build=True):
    opts = configure()
    compose("config", "--quiet")
    if build:
        build_inputs.prepare()
        compose("build", "toolbox", "airflow-webserver", "spark-master", "ops-console")
    port_priority.prioritize()
    compose("up", "-d", "--wait", "--wait-timeout", "480", *BASE)
    schema = (ROOT / "sql/ops.sql").read_text()
    pg(schema)
    pg("DELETE FROM ops.installation WHERE step='planned_stop'")
    first = (
        pg("SELECT count(*) FROM ops.installation WHERE step='batch_complete'") == "0"
    )
    if first:
        if compose(
            "--profile",
            "stream",
            "ps",
            "-q",
            "--status",
            "running",
            *STREAM,
            capture=True,
        ):
            raise RuntimeError(
                "Streaming is active during initial load; stop the classic stack and run up"
            )
        prepare_source()
        for topic in TOPICS:
            compose(
                "exec",
                "-T",
                "northwind_kafka",
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "localhost:19092",
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
            )
        toolbox("prepare.py")
    toolbox("local_accounts.py")
    compose("run", "--rm", "--no-deps", "airflow-init")
    compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "420", *APPS)
    if first:
        batch()
        toolbox("check.py")
        pg(
            "INSERT INTO ops.installation(step) VALUES ('batch_complete') ON CONFLICT DO NOTHING"
        )
    pg(
        "INSERT INTO ops.installation(step) VALUES ('planned_stop') ON CONFLICT DO NOTHING"
    )
    try:
        compose("--profile", "stream", "up", "-d", "--no-build", *STREAM)
    finally:
        pg("DELETE FROM ops.installation WHERE step='planned_stop'")
    compose(
        "exec",
        "-T",
        "airflow-scheduler",
        "airflow",
        "jobs",
        "check",
        "--job-type",
        "SchedulerJob",
        "--local",
    )
    if not first:
        toolbox("check.py")
    print(
        "Ops: http://127.0.0.1:"
        + opts["OPS_PORT"]
        + "  Airflow: http://127.0.0.1:"
        + opts["AIRFLOW_PORT"]
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action",
        choices=["up", "stop", "status", "check", "stop-job", "sms", "ports"],
    )
    parser.add_argument("option", nargs="?")
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args()
    if args.action == "ports":
        port_priority.prioritize(preview=True)
        return
    if args.action == "stop-job":
        target = args.option or "northwind_etl"
        if target != "northwind_etl":
            parser.error("Unknown DAG")
        compose(
            "exec",
            "-T",
            "airflow-scheduler",
            "python",
            "/opt/airflow/scripts/stop_job.py",
            target,
        )
        return
    STATE.mkdir(exist_ok=True)
    with (STATE / "command.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.action == "up":
            up(build=not args.no_build)
        elif args.action == "stop":
            try:
                pg(
                    "INSERT INTO ops.installation(step) VALUES ('planned_stop') ON CONFLICT DO NOTHING"
                )
            except (RuntimeError, subprocess.SubprocessError):
                pass
            compose("--profile", "stream", "stop", "-t", "60")
        elif args.action == "status":
            compose("--profile", "stream", "ps", "-a")
        elif args.action == "check":
            toolbox("check.py")
        elif args.action == "sms":
            options = configure()
            if args.option == "off":
                options["SUNWAY_ENABLED"] = "false"
            else:
                username = getpass.getpass("Sunway username: ").strip()
                password = getpass.getpass("Sunway web-service password: ")
                sender = input("Sunway sender number (Enter keeps the saved value): ").strip() or options.get("SUNWAY_SENDER", "").strip()
                recipient = getpass.getpass("Alert recipient (09xxxxxxxxx): ").strip()
                if not username or not password or not re.fullmatch("[0-9]{4,18}", sender) or not re.fullmatch("09[0-9]{9}", recipient):
                    raise ValueError("Invalid Sunway settings")
                options.update(
                    SUNWAY_ENABLED="true",
                    SUNWAY_USERNAME=username,
                    SUNWAY_PASSWORD=password,
                    SUNWAY_SENDER=sender,
                    SUNWAY_RECIPIENT=recipient,
                )
            save_env(options)
            print("Settings saved. Run python3 classic.py up after active jobs finish.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as exc:
        print("STOP:", exc, file=sys.stderr)
        sys.exit(1)
