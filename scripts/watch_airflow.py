import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import docker
from common.sms import clean_error, display_time, send

LOG = logging.getLogger("northwind.monitor")
STATE = Path("/opt/airflow/logs/sunway-monitor.json")
TARGETS = ("airflow_scheduler", "airflow_webserver")


def snapshot(container):
    state = container.attrs["State"]
    return {
        "status": state.get("Status", "unknown"),
        "health": state.get("Health", {}).get("Status", "none"),
        "started": state.get("StartedAt", ""),
        "exit": state.get("ExitCode", 0),
    }


def transition(old, new):
    if new["status"] != "running":
        return "توقف" if old is None or old["status"] == "running" else None
    if new["health"] == "starting":
        return None
    if new["health"] == "unhealthy":
        return (
            "اختلال"
            if old is None
            or old["health"] != "unhealthy"
            or old["started"] != new["started"]
            else None
        )
    if old is None or old["status"] != "running":
        return "شروع"
    if old["started"] != new["started"]:
        return "راه‌اندازی مجدد"
    if old["health"] == "unhealthy":
        return "بازیابی"
    return None


def persist(values):
    temp = STATE.with_suffix(".tmp")
    temp.write_text(json.dumps(values), encoding="utf-8")
    temp.replace(STATE)


def main():
    logging.basicConfig(level=logging.INFO)
    previous = json.loads(STATE.read_text()) if STATE.exists() else {}
    pending = {}
    client = docker.from_env(timeout=8)
    try:
        while True:
            for name in TARGETS:
                try:
                    try:
                        current = snapshot(
                            client.containers.get("northwind_classic_" + name)
                        )
                    except docker.errors.NotFound:
                        current = {
                            "status": "missing",
                            "health": "none",
                            "started": "",
                            "exit": 0,
                        }
                    old = previous.get(name)
                    if current["health"] == "starting":
                        pending.pop(name, None)
                        continue
                    if pending.get(name) != current:
                        pending[name] = current
                        continue
                    event = transition(old, current)
                    if old == current:
                        continue
                    previous[name] = current
                    persist(previous)
                    if event:
                        now = datetime.now(timezone.utc)
                        message = f"Northwind | {event}\nسرویس: {name}\nوضعیت: {current['status']}\nسلامت: {current['health']}\nکد خروج: {current['exit']}\nزمان تهران: {display_time(now)}"
                        event_id = hashlib.sha256(
                            (name + str(now) + str(current)).encode()
                        ).hexdigest()[:12]
                        LOG.info("Airflow event %s: %s", event_id, message)
                        try:
                            message_id = send(message)
                            LOG.info(
                                "Airflow event %s provider_id=%s", event_id, message_id
                            )
                        except Exception as exc:
                            LOG.error(
                                "Airflow event %s SMS failed: %s",
                                event_id,
                                clean_error(exc),
                            )
                except Exception as exc:
                    LOG.error(
                        "Monitor inspection failed for %s: %s", name, clean_error(exc)
                    )
            time.sleep(5)
    finally:
        client.close()


if __name__ == "__main__":
    main()
