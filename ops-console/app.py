import os
from local_login import protect
import jdatetime

import docker
import psycopg2

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from psycopg2.extras import RealDictCursor

from datetime import datetime
from zoneinfo import ZoneInfo

app = FastAPI(title="Northwind Operations Console")
protect(app)

templates = Jinja2Templates(directory="/app/templates")


TEHRAN_TZ = ZoneInfo("Asia/Tehran")


def format_datetime(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=ZoneInfo("UTC"))

        return jdatetime.datetime.fromgregorian(
            datetime=value.astimezone(TEHRAN_TZ)
        ).strftime("%Y-%m-%d %H:%M:%S")

    return value


def format_row_dates(row):
    if not row:
        return row

    data = dict(row)

    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = format_datetime(value)

    return data


SERVICES = [
    ("SQL Server", "northwind_sqlserver"),
    ("PostgreSQL", "northwind_postgres"),
    ("Kafka", "northwind_kafka"),
    ("ClickHouse", "northwind_clickhouse"),
    ("Airflow Webserver", "airflow_webserver"),
    ("Airflow Scheduler", "airflow_scheduler"),
    ("Spark Master", "spark-master"),
    ("Spark Worker", "spark-worker"),
    ("Orders CDC", "orders_cdc_reader"),
    ("Order Details CDC", "orderdetails_cdc_reader"),
    ("Fact Consumer", "factorders_consumer"),
    ("Customer CDC", "customer_cdc_reader"),
    ("Customer Consumer", "customer_consumer"),
    ("Product CDC", "product_cdc_reader"),
    ("Product Consumer", "product_consumer"),
]


def get_pg_connection():
    return psycopg2.connect(
        connect_timeout=6,
        host=os.getenv("POSTGRES_HOST", "northwind_postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "Staging_Northwind"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.environ["POSTGRES_PASSWORD"],
    )


def classic_name(name):
    return "northwind_classic_" + name.replace("-", "_")


def get_service_statuses():
    results = []

    try:
        client = docker.from_env()

        try:
            for title, container_name in SERVICES:
                try:
                    container = client.containers.get(classic_name(container_name))

                    container.reload()

                    results.append(
                        {
                            "name": title,
                            "container": container_name,
                            "status": container.status,
                            "running": (container.status == "running"),
                        }
                    )

                except docker.errors.NotFound:
                    results.append(
                        {
                            "name": title,
                            "container": container_name,
                            "status": "not_found",
                            "running": False,
                        }
                    )

        finally:
            client.close()

    except Exception as exc:
        results.append(
            {
                "name": "Docker",
                "container": "docker",
                "status": f"error: {exc}",
                "running": False,
            }
        )

    return results


def get_airflow_connection():
    return psycopg2.connect(
        connect_timeout=6,
        host=os.getenv("AIRFLOW_DB_HOST", "airflow_metadata_db"),
        port=os.getenv("AIRFLOW_DB_PORT", "5432"),
        dbname=os.getenv("AIRFLOW_DB_NAME", "airflow"),
        user=os.getenv("AIRFLOW_DB_USER", "airflow"),
        password=os.environ["AIRFLOW_DB_PASSWORD"],
    )


def get_airflow_data():
    result = {
        "summary": {"total": 0, "success": 0, "failed": 0, "running": 0},
        "latest_run": None,
        "tasks": [],
        "recent_runs": [],
    }

    conn = get_airflow_connection()

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            select
                count(*) as total,
                count(*) filter (
                    where state = 'success'
                ) as success,
                count(*) filter (
                    where state = 'failed'
                ) as failed,
                count(*) filter (
                    where state = 'running'
                ) as running
            from dag_run
            where dag_id = 'northwind_etl'
            """)

        result["summary"] = dict(cursor.fetchone())

        cursor.execute("""
            select
                dag_id,
                run_id,
                state,
                run_type,
                start_date,
                end_date
            from dag_run
            where dag_id = 'northwind_etl'
            order by
                start_date desc nulls last
            limit 1
            """)

        latest = cursor.fetchone()

        if latest:
            result["latest_run"] = format_row_dates(latest)

            cursor.execute(
                """
                select
                    task_id,
                    state,
                    start_date,
                    end_date,
                    duration,
                    try_number
                from task_instance
                where dag_id = 'northwind_etl'
                and run_id = %s
                order by task_id
                """,
                (latest["run_id"],),
            )

            result["tasks"] = [format_row_dates(row) for row in cursor.fetchall()]

        cursor.execute("""
            select
                run_id,
                state,
                run_type,
                start_date,
                end_date
            from dag_run
            where dag_id = 'northwind_etl'
            order by
                start_date desc nulls last
            limit 10
            """)

        result["recent_runs"] = [format_row_dates(row) for row in cursor.fetchall()]

        cursor.close()

    finally:
        conn.close()

    return result


LOG_SOURCES = {
    "airflow": "airflow_scheduler",
    "spark": "spark-master",
    "orders": "orders_cdc_reader",
    "orderdetails": "orderdetails_cdc_reader",
    "consumer": "factorders_consumer",
    "kafka": "northwind_kafka",
}


def get_container_logs(source, tail=80):
    if source not in LOG_SOURCES:
        raise ValueError(f"unknown log source: {source}")

    container_name = LOG_SOURCES[source]

    client = docker.from_env()

    try:
        container = client.containers.get(classic_name(container_name))

        output = container.logs(tail=tail, timestamps=True)

        return output.decode("utf-8", errors="replace")

    finally:
        client.close()


@app.get("/api/logs/{source}")
def container_logs(source: str, tail: int = 80):
    tail = max(10, min(tail, 300))

    try:
        logs = get_container_logs(source, tail)

        return {"source": source, "container": LOG_SOURCES[source], "logs": logs}

    except Exception as exc:
        return {
            "source": source,
            "container": LOG_SOURCES.get(source, "unknown"),
            "logs": "",
            "error": str(exc),
        }


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "airflow_port": int(os.getenv("AIRFLOW_PORT", "8080")),
            "spark_port": int(os.getenv("SPARK_PORT", "8081")),
            "grafana_url": os.getenv("GRAFANA_URL", ""),
        },
    )


@app.get("/api/dashboard")
def dashboard():
    conn = get_pg_connection()

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            select *
            from ops.pipeline_metrics
            """)

        metrics = dict(cursor.fetchone() or {})

        cursor.execute("""
            select
                event_id,
                trace_id,
                source_table,
                action,
                orderid,
                productid,
                event_status,
                kafka_topic,
                kafka_partition,
                kafka_offset,
                received_at,
                processed_at,
                retry_count,
                error_message,
                current_stage,
                current_stage_status
            from ops.event_trace
            order by event_id desc
            limit 1
            """)

        row = cursor.fetchone()

        latest_event = format_row_dates(row) if row else None

        pipeline_stages = []

        if latest_event:
            cursor.execute(
                """
                select distinct on (stage)
                    stage,
                    status,
                    message,
                    created_at
                from ops.pipeline_events
                where trace_id = %s
                order by
                    stage,
                    id desc
                """,
                (latest_event["trace_id"],),
            )

            pipeline_stages = [format_row_dates(item) for item in cursor.fetchall()]

        cursor.execute("""
            select
                event_id,
                trace_id,
                source_table,
                action,
                orderid,
                productid,
                event_status,
                current_stage,
                current_stage_status,
                received_at,
                processed_at,
                retry_count
            from ops.event_trace
            order by event_id desc
            limit 20
            """)

        recent_events = [format_row_dates(item) for item in cursor.fetchall()]

        cursor.execute("""
            select
                count(*) as total,
                count(*) filter (
                    where severity = 'error'
                ) as errors,
                count(*) filter (
                    where sms_status = 'sent'
                ) as sms_sent,
                count(*) filter (
                    where sms_status = 'failed'
                ) as sms_failed
            from ops.alerts
            """)

        alert_summary = dict(cursor.fetchone() or {})

        cursor.execute("""
            select
                alert_id,
                trace_id,
                pipeline_name,
                stage,
                severity,
                message,
                sms_status,
                provider,
                send_attempts,
                created_at,
                sms_sent_at
            from ops.alerts
            order by alert_id desc
            limit 20
            """)

        alerts = [format_row_dates(item) for item in cursor.fetchall()]

        cursor.close()

    finally:
        conn.close()

    try:
        airflow = get_airflow_data()
    except Exception as exc:
        airflow = {
            "error": str(exc),
            "summary": {"total": 0, "success": 0, "failed": 0, "running": 0},
            "latest_run": None,
            "tasks": [],
            "recent_runs": [],
        }

    return {
        "metrics": metrics,
        "services": get_service_statuses(),
        "latest_event": latest_event,
        "pipeline_stages": pipeline_stages,
        "recent_events": recent_events,
        "alert_summary": alert_summary,
        "alerts": alerts,
        "airflow": airflow,
    }


@app.get("/health")
def health():
    return {"status": "ok"}
