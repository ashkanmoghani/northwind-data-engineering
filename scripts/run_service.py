import logging
import time
import hashlib
import runpy
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from common.alert_manager import create_alert
from common.sms import clean_error, display_time, send
from common.stop import control_db


class ServiceStopped(BaseException):
    pass


def stop(signum, frame):
    raise ServiceStopped(signal.Signals(signum).name)


def report(name, start, exc):
    now = datetime.now(timezone.utc)
    stopped = isinstance(exc, ServiceStopped)
    state = "توقف" if stopped else "خطا"
    reason = clean_error(exc)
    text = f"Northwind | {state}\nجاب: {name}\nعلت: {reason}\nشروع تهران: {display_time(start)}\nپایان تهران: {display_time(now)}\nمدت: {round((now-start).total_seconds())} ثانیه"
    try:
        db = control_db()
    except Exception:
        stamp = Path("/tmp/classic-last-unavailable-alert")
        signature = hashlib.sha256((name + state + reason).encode()).hexdigest()
        if (
            stamp.exists()
            and stamp.read_text() == signature
            and time.time() - stamp.stat().st_mtime < 300
        ):
            return
        stamp.write_text(signature)
        send(text)
        return
    try:
        with db.cursor() as cursor:
            if stopped:
                cursor.execute(
                    "SELECT 1 FROM ops.installation WHERE step='planned_stop'"
                )
                if cursor.fetchone():
                    return
            trace = getattr(exc, "classic_trace", None)
            key = f"{name}:{state}:{trace or reason}:{int(now.timestamp())//300}"
            create_alert(
                cursor,
                db,
                name,
                "service",
                reason,
                trace,
                "stopped" if stopped else "error",
                text,
                key,
            )
    finally:
        db.close()


def main():
    name = sys.argv[1]
    if name not in {
        "customer_cdc_reader",
        "orders_cdc_reader",
        "orderdetails_cdc_reader",
        "product_cdc_reader",
        "product_consumer",
        "customer_consumer",
        "factorders_consumer",
    }:
        raise SystemExit("Unknown service")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    start = datetime.now(timezone.utc)
    try:
        runpy.run_path(str(Path(__file__).with_name(name + ".py")), run_name="__main__")
    except ServiceStopped as exc:
        report(name, start, exc)
    except Exception as exc:
        try:
            report(name, start, exc)
        except Exception as alert_error:
            logging.error(
                "Could not register service alert: %s", clean_error(alert_error)
            )
        raise


if __name__ == "__main__":
    main()
