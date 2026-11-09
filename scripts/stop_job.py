import sys
from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session
from common.stop import control_db

dag_id = sys.argv[1]
if dag_id != "northwind_etl":
    raise SystemExit("Unknown DAG")
with create_session() as session:
    rows = session.query(DagRun.run_id).filter_by(dag_id=dag_id, state="running").all()
if len(rows) != 1:
    raise SystemExit("Expected one running DAG")
run_id = rows[0][0]
db = control_db()
with db, db.cursor() as cur:
    cur.execute(
        "INSERT INTO ops.job_stops(dag_id,run_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
        (dag_id, run_id),
    )
db.close()
print("Stop requested for", run_id)
