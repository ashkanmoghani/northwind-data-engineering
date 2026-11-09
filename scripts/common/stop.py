import os
import psycopg2


class JobStopped(RuntimeError):
    pass


def control_db():
    return psycopg2.connect(
        host=os.getenv("postgres_host", "northwind_postgres"),
        port=os.getenv("postgres_port", "5432"),
        dbname="Staging_Northwind",
        user="postgres",
        password=os.environ["postgres_password"],
        connect_timeout=6,
    )


def require_active(dag_id, run_id):
    conn = control_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM ops.job_stops WHERE dag_id=%s AND run_id=%s",
                (dag_id, run_id),
            )
            if cur.fetchone():
                raise JobStopped("توقف به درخواست کاربر")
    finally:
        conn.close()
