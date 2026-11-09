import subprocess
import sys
import time
from pathlib import Path
from common.cdc import SOURCES
from common.connections import get_staging_connection, get_source_connection

ROOT = Path(__file__).resolve().parent
pg = get_staging_connection()
sql = get_source_connection()
with pg, pg.cursor() as cur:
    for name in ("ops", "staging"):
        cur.execute((Path("/classic-sql") / (name + ".sql")).read_text())
with sql.cursor() as cur:
    cur.execute("SELECT is_cdc_enabled FROM sys.databases WHERE database_id=DB_ID()")
    if not cur.fetchone()[0]:
        cur.execute("EXEC sys.sp_cdc_enable_db")
    for table, _, _ in SOURCES.values():
        capture = "dbo_" + table
        cur.execute(
            "SELECT count(*) FROM cdc.change_tables WHERE capture_instance=? AND source_object_id=OBJECT_ID(?)",
            capture,
            "dbo.[" + table + "]",
        )
        if not cur.fetchone()[0]:
            cur.execute(
                "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo',@source_name=?,@role_name=NULL,@capture_instance=?,@supports_net_changes=0",
                table,
                capture,
            )
    end = time.monotonic() + 240
    while True:
        cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
        high = cur.fetchone()[0]
        lows = []
        for table, _, _ in SOURCES.values():
            cur.execute("SELECT sys.fn_cdc_get_min_lsn(?)", "dbo_" + table)
            lows.append(cur.fetchone()[0])
        if (
            high
            and any(bytes(high))
            and all(v and any(bytes(v)) and bytes(v) <= bytes(high) for v in lows)
        ):
            break
        if time.monotonic() >= end:
            raise RuntimeError("CDC capture is not ready. Check SQL Server Agent.")
        time.sleep(3)
with pg, pg.cursor() as cur:
    for name in SOURCES:
        cur.execute(
            "INSERT INTO cdc_offsets(pipeline_name,last_lsn) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (name, "0x" + bytes(high).hex()),
        )
sql.close()


def done(step):
    with pg, pg.cursor() as cur:
        cur.execute("SELECT 1 FROM ops.installation WHERE step=%s", (step,))
        return bool(cur.fetchone())


def mark(step):
    with pg, pg.cursor() as cur:
        cur.execute(
            "INSERT INTO ops.installation(step) VALUES (%s) ON CONFLICT DO NOTHING",
            (step,),
        )


def run(names):
    for name in names.split():
        print(name, flush=True)
        subprocess.run([sys.executable, "-u", str(ROOT / (name + ".py"))], check=True)


if not done("dimensions_dw"):
    run(
        "load_geography load_suppliers load_products load_customers load_employees load_shippers load_dim_geography load_dim_supplier load_dim_products load_dim_customer load_dim_employee load_dim_shipper load_dim_date"
    )
    mark("dimensions_dw")
if not done("dimensions_clickhouse"):
    run(
        "create_clickhouse_dimensions create_clickhouse_factorders load_clickhouse_dimensions"
    )
    mark("dimensions_clickhouse")
pg.close()
print("Dimensions and CDC checkpoints ready")
