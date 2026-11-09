import sys
import subprocess

from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from common.alert_manager import task_alert
from common.stop import require_active


def checkpoint():
    context = get_current_context()
    require_active("northwind_etl", context["run_id"])


def cname(name):
    return "northwind_classic_" + name.replace("-", "_")


CONSUMERS = ["customer_consumer", "product_consumer", "factorders_consumer"]


def check_consumers_stopped(docker_client):
    for name in CONSUMERS:
        from docker.errors import NotFound

        try:
            container = docker_client.containers.get(cname(name))
        except NotFound:
            continue
        if container.status not in ("exited", "created"):
            raise ValueError(
                f"Stop {name} before the initial load/backfill. "
                "Do not update the Source during this batch run."
            )


@task
def check_batch_ready():
    checkpoint()
    import docker

    docker_client = docker.from_env()
    try:
        check_consumers_stopped(docker_client)
        for name in ["spark-master", "spark-worker"]:
            if docker_client.containers.get(cname(name)).status != "running":
                raise ValueError(f"{name} is not running")
    finally:
        docker_client.close()


@task
def validate_clickhouse_dimensions():
    checkpoint()
    sys.path.insert(0, "/opt/airflow/scripts")
    from common.connections import get_clickhouse_connection

    client = get_clickhouse_connection()
    try:
        for table in [
            "dimgeography",
            "dimsupplier",
            "dimproducts",
            "dimcustomer",
            "dimemployee",
            "dimshipper",
            "dimdate",
        ]:
            count = client.command(f"SELECT count() FROM {table}")
            print(f"{table}: {count}")
            if count == 0:
                raise ValueError(f"{table} is empty; dimensions must be loaded first")

        if client.command("SELECT count() FROM dimcustomer WHERE iscurrent = 1") == 0:
            raise ValueError("No current customer dimension rows")
        if client.command("SELECT count() FROM dimproducts WHERE enddate IS NULL") == 0:
            raise ValueError("No current product dimension rows")
    finally:
        client.close()


@task
def load_staging_factorders():
    checkpoint()
    process = subprocess.run(
        [sys.executable, "-u", "/opt/airflow/scripts/load_factorders.py"],
        capture_output=True,
        text=True,
    )
    print(process.stdout, flush=True)
    checkpoint()
    if process.returncode:
        raise RuntimeError(
            (process.stderr.strip().splitlines() or ["Staging loader failed"])[-1]
        )


@task
def validate_staging_factorders():
    checkpoint()
    sys.path.insert(0, "/opt/airflow/scripts")
    from common.connections import get_staging_connection

    conn = get_staging_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT count(*) FROM public.staging_factorders")
        count = cursor.fetchone()[0]
        print(f"staging_factorders: {count}")
        if count == 0:
            raise ValueError("staging_factorders is empty")
    finally:
        cursor.close()
        conn.close()


@task
def run_spark_factorders():
    checkpoint()
    import docker

    docker_client = docker.from_env(timeout=3600)
    try:
        check_consumers_stopped(docker_client)
        command = [
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            "--conf",
            "spark.driver.host=spark-master",
            "--conf",
            "spark.driver.bindAddress=0.0.0.0",
            "--jars",
            "/opt/spark/jars/postgresql-42.7.3.jar,"
            "/opt/spark/jars/clickhouse-jdbc-0.7.2-shaded-all.jar",
            "/opt/spark-apps/spark_factorders_job.py",
            "--publish",
        ]
        execution = docker_client.api.exec_create(
            cname("spark-master"),
            command,
            environment={
                "PYTHONUNBUFFERED": "1",
                "CLASSIC_RUN_ID": get_current_context()["run_id"],
            },
        )
        execution_id = execution["Id"]
        print(f"Spark execution: {execution_id}", flush=True)

        cause = ""
        for output in docker_client.api.exec_start(execution_id, stream=True):
            line = output.decode("utf-8", errors="replace")
            print(line, end="", flush=True)
            for part in line.splitlines():
                if "exception" in part.lower() or "error" in part.lower():
                    cause = part.strip()[:350]

        result = docker_client.api.exec_inspect(execution_id)
        checkpoint()
        if result["ExitCode"] != 0:
            raise RuntimeError(f"Spark exited {result['ExitCode']}: {cause}")
    finally:
        docker_client.close()


@task
def validate_factorders():
    checkpoint()
    sys.path.insert(0, "/opt/airflow/scripts")
    from common.connections import get_staging_connection, get_clickhouse_connection

    conn = get_staging_connection()
    cursor = conn.cursor()
    client = get_clickhouse_connection()
    try:
        cursor.execute("SELECT count(*) FROM public.staging_factorders")
        staging_count = cursor.fetchone()[0]
        rows, unique_rows = client.query("""
            SELECT count(), uniqExact(tuple(orderid, productkey))
            FROM factorders
        """).result_rows[0]
        print(f"staging={staging_count}, fact={rows}, unique={unique_rows}")
        if not (staging_count > 0 and staging_count == rows == unique_rows):
            raise ValueError("FactOrders count or uniqueness validation failed")
    finally:
        cursor.close()
        conn.close()
        client.close()


@dag(
    dag_id="northwind_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": task_alert},
    tags=["northwind", "spark", "initial-load"],
    description="Manual FactOrders initial load/backfill using existing ClickHouse dimensions",
)
def northwind_etl():
    ready = check_batch_ready()
    dimensions = validate_clickhouse_dimensions()
    staging = load_staging_factorders()
    staging_check = validate_staging_factorders()
    spark_job = run_spark_factorders()
    fact_check = validate_factorders()

    ready >> dimensions >> staging >> staging_check >> spark_job >> fact_check


northwind_etl()
