import os
import subprocess
import psycopg2

subprocess.run(["airflow", "db", "migrate"], check=True)
with psycopg2.connect(
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].replace(
        "postgresql+psycopg2:", "postgresql:"
    )
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.ab_user')")
        ready = cur.fetchone()[0] is not None
        if ready:
            cur.execute("SELECT 1 FROM ab_user WHERE username='admin'")
            ready = cur.fetchone() is not None
if not ready:
    cmd = [
        "airflow",
        "users",
        "create",
        "--username",
        "admin",
        "--firstname",
        "Northwind",
        "--lastname",
        "Admin",
        "--email",
        "admin@localhost.invalid",
        "--role",
        "Admin",
        "--password",
        "admin",
    ]
    if subprocess.run(cmd).returncode:
        raise SystemExit("Could not initialize the Airflow admin account")
else:
    subprocess.run(
        [
            "airflow",
            "users",
            "reset-password",
            "--username",
            "admin",
            "--password",
            "admin",
        ],
        check=True,
    )
