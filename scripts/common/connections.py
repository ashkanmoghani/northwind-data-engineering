import time
import pyodbc
import psycopg2
import clickhouse_connect

from common.config import (
    sql_server_host,
    sql_server_user,
    sql_server_password,
    sql_server_source_db,
    sql_server_dw_db,
    postgres_host,
    postgres_port,
    postgres_db,
    postgres_user,
    postgres_password,
    clickhouse_host,
    clickhouse_port,
    clickhouse_database,
    clickhouse_user,
    clickhouse_password,
)


def retry_connection(func, name, retries=30, delay=5):
    for attempt in range(1, retries + 1):
        try:
            return func()
        except Exception as e:
            print(f"{name} connection failed ({attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(delay)

    raise RuntimeError(f"{name} unavailable after {retries} attempts")


def get_source_connection():
    return retry_connection(
        lambda: pyodbc.connect(
            "driver={ODBC Driver 18 for SQL Server};"
            f"server={sql_server_host};"
            f"database={sql_server_source_db};"
            f"uid={sql_server_user};"
            f"pwd={sql_server_password};"
            "trustservercertificate=yes;",
            autocommit=True,
            timeout=15,
        ),
        "SQL Server",
    )


def get_dw_connection():
    return pyodbc.connect(
        "driver={ODBC Driver 18 for SQL Server};"
        f"server={sql_server_host};"
        f"database={sql_server_dw_db};"
        f"uid={sql_server_user};"
        f"pwd={sql_server_password};"
        "trustservercertificate=yes;"
    )


def get_staging_connection():
    return retry_connection(
        lambda: psycopg2.connect(
            host=postgres_host,
            port=postgres_port,
            database=postgres_db,
            user=postgres_user,
            password=postgres_password,
        ),
        "Postgres",
    )


def get_clickhouse_connection():
    return retry_connection(
        lambda: clickhouse_connect.get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            username=clickhouse_user,
            password=clickhouse_password,
            settings={"mutations_sync": 1},
            database=clickhouse_database,
        ),
        "ClickHouse",
    )
