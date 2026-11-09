import hashlib
import logging
from datetime import datetime, timezone
from common.sms import clean_error, display_time, send
from common.sms import enabled as sms_enabled
from common.stop import JobStopped, control_db


def create_alert(
    cursor,
    conn,
    pipeline_name,
    stage,
    message,
    trace_id=None,
    severity="error",
    sms_text=None,
    dedup=None,
):
    reason = clean_error(message)
    identity = dedup or f"{pipeline_name}:{stage}:{trace_id or reason}"
    key = hashlib.sha256(identity.encode()).hexdigest()
    cursor.execute(
        """INSERT INTO ops.alerts(trace_id,pipeline_name,stage,severity,message,sms_status,provider,dedup_key)
        VALUES (%s,%s,%s,%s,%s,'pending',%s,%s) ON CONFLICT(dedup_key) DO NOTHING RETURNING alert_id""",
        (
            trace_id,
            pipeline_name,
            stage,
            severity,
            reason,
            "sunway",
            key,
        ),
    )
    row = cursor.fetchone()
    conn.commit()
    if row is None:
        return
    body = (
        sms_text
        or f"Northwind | خطا\nجاب: {pipeline_name}\nمرحله: {stage}\nزمان تهران: {display_time(datetime.now(timezone.utc))}\nعلت: {reason}"
    )
    enabled = sms_enabled()
    try:
        msg_id = send(body)
        status = "sent" if msg_id is not None else "disabled"
    except Exception as exc:
        msg_id = None
        status = "failed"
        logging.getLogger("airflow.task").error(
            "SMS delivery request failed: %s", clean_error(exc)
        )
    cursor.execute(
        """UPDATE ops.alerts SET sms_status=%s,provider_id=%s,send_attempts=%s,
        sms_sent_at=CASE WHEN %s THEN now() END WHERE alert_id=%s""",
        (status, msg_id, int(enabled), msg_id is not None, row[0]),
    )
    conn.commit()


def task_alert(context):
    from airflow.exceptions import AirflowTaskTerminated

    ti = context["ti"]
    error = context.get("exception")
    stopped = isinstance(error, (JobStopped, AirflowTaskTerminated))
    now = ti.end_date or datetime.now(timezone.utc)
    reason = clean_error(error or "Worker reported task failure")
    state = "توقف" if stopped else "خطا"
    duration = (
        f"{max(0, round((now-ti.start_date).total_seconds()))} ثانیه"
        if ti.start_date
        else "نامشخص"
    )
    text = f"Northwind | {state}\nجاب: {ti.dag_id}\nتسک: {ti.task_id}\nعلت: {reason}\nشروع تهران: {display_time(ti.start_date)}\nپایان تهران: {display_time(now)}\nمدت: {duration}\nاجرا: {ti.run_id}"
    key = f"{ti.dag_id}:{ti.run_id}:{ti.task_id}:{ti.map_index}:{ti.try_number}"
    try:
        conn = control_db()
    except Exception:
        logging.getLogger("airflow.task").warning(
            "Alert database unavailable; sending directly"
        )
        send(text)
        return
    try:
        with conn.cursor() as cur:
            create_alert(
                cur,
                conn,
                ti.dag_id,
                ti.task_id,
                reason,
                ti.run_id,
                "stopped" if stopped else "error",
                text,
                key,
            )
    finally:
        conn.close()
