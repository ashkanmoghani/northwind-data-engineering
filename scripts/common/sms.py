import logging
import os
import re
import requests
import secrets
from datetime import timezone
from zoneinfo import ZoneInfo
import jdatetime

LOG = logging.getLogger("airflow.task")


def display_time(value):
    if value is None:
        return "نامشخص"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    local = value.astimezone(ZoneInfo("Asia/Tehran"))
    return jdatetime.datetime.fromgregorian(datetime=local).strftime(
        "%Y/%m/%d %H:%M:%S"
    )


def clean_error(value):
    text = str(value)
    for k, v in os.environ.items():
        if (
            any(
                s in k.upper()
                for s in (
                    "PASSWORD",
                    "SECRET",
                    "TOKEN",
                    "API_KEY",
                    "FERNET",
                    "RECIPIENT",
                    "USERNAME",
                )
            )
            and len(v) > 3
        ):
            text = text.replace(v, "***")
    text = re.sub(r"://[^/\s]+@", "://***@", text)
    text = re.sub(
        r"(?i)\b(password|pwd|token|api[-_ ]?key)\s*[:=]\s*[^;\s,]+", r"\1=***", text
    )
    return " ".join(text.split())[:240]


def enabled():
    return os.getenv("SUNWAY_ENABLED", "false").strip().lower() == "true"


def send(text):
    if not enabled():
        LOG.info("SMS preview\n%s", text)
        return None
    username = os.getenv("SUNWAY_USERNAME", "").strip()
    password = os.getenv("SUNWAY_PASSWORD", "")
    recipient = os.getenv("SUNWAY_RECIPIENT", "").strip()
    sender = os.getenv("SUNWAY_SENDER", "").strip()
    if (
        not username
        or not password
        or not re.fullmatch(r"09[0-9]{9}", recipient)
        or not re.fullmatch(r"[0-9]{4,18}", sender)
    ):
        raise ValueError("Invalid Sunway credentials, sender or recipient")
    tracking = str(1_000_000_000_000 + secrets.randbelow(9_000_000_000_000))
    LOG.info("Sunway request tracking=%s", tracking)
    try:
        response = requests.get(
            "https://sms.sunwaysms.com/smsws/HttpService.ashx",
            params={
                "service": "SendArray",
                "UserName": username,
                "Password": password,
                "To": recipient,
                "From": sender,
                "Message": text,
                "Flash": "false",
                "chkMessageId": tracking,
            },
            timeout=(4, 25),
            allow_redirects=False,
        )
    except requests.RequestException:
        raise RuntimeError(
            "Sunway response unavailable; inspect panel before retrying; tracking="
            + tracking
        ) from None
    if response.status_code != 200:
        raise RuntimeError(
            f"Sunway HTTP {response.status_code}; inspect panel before retrying; tracking={tracking}"
        )
    result = response.content.decode("utf-8-sig", errors="replace").strip()
    if not re.fullmatch(r"[0-9]{1,19}", result):
        raise RuntimeError(
            "Unrecognized Sunway response; inspect panel before retrying; tracking="
            + tracking
        )
    message_id = int(result)
    if message_id <= 1000:
        raise RuntimeError(
            f"Sunway rejected request: code={message_id}; tracking={tracking}"
        )
    return message_id
