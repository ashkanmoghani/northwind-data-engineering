import os
import secrets
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from common import sms
from watch_airflow import transition


class SmsTests(unittest.TestCase):
    def setUp(self):
        environment = {
            "SUNWAY_ENABLED": "true",
            "SUNWAY_USERNAME": secrets.token_hex(8),
            "SUNWAY_PASSWORD": secrets.token_urlsafe(18),
            "SUNWAY_SENDER": "5000",
            "SUNWAY_RECIPIENT": "09" + "0" * 9,
        }
        settings = patch.dict(os.environ, environment, clear=True)
        settings.start()
        self.addCleanup(settings.stop)

    def test_disabled_does_not_send(self):
        with patch.dict(os.environ, {"SUNWAY_ENABLED": "false"}), patch.object(sms.requests, "get") as get:
            self.assertIsNone(sms.send("pipeline alert"))
        get.assert_not_called()

    def test_missing_sender_rejected(self):
        with patch.dict(os.environ, {"SUNWAY_SENDER": ""}), patch.object(sms.requests, "get") as get:
            with self.assertRaisesRegex(ValueError, "sender"):
                sms.send("pipeline alert")
        get.assert_not_called()

    def test_accepted_response_returns_id(self):
        response = Mock(status_code=200, content=b"1234567")
        with patch.object(sms.requests, "get", return_value=response) as get:
            self.assertEqual(sms.send("pipeline alert"), 1234567)
        get.assert_called_once()
        self.assertFalse(get.call_args.kwargs["allow_redirects"])
        self.assertTrue(get.call_args.kwargs["params"]["chkMessageId"].isdigit())

    def test_provider_rejection_is_not_success(self):
        response = Mock(status_code=200, content=b"69")
        with patch.object(sms.requests, "get", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "code=69"):
                sms.send("pipeline alert")

    def test_timeout_is_not_retried(self):
        password = os.environ["SUNWAY_PASSWORD"]
        with patch.object(sms.requests, "get", side_effect=sms.requests.Timeout(password)) as get:
            with self.assertRaisesRegex(RuntimeError, "inspect panel") as result:
                sms.send("pipeline alert")
        self.assertNotIn(password, str(result.exception))
        get.assert_called_once()

    def test_enabled_without_credentials_does_not_contact_sunway(self):
        with patch.dict(os.environ, {"SUNWAY_ENABLED": "true"}, clear=True), patch.object(sms.requests, "get") as get:
            with self.assertRaisesRegex(ValueError, "credentials"):
                sms.send("pipeline alert")
        get.assert_not_called()

    def test_error_redaction(self):
        private = [os.environ[k] for k in ("SUNWAY_USERNAME", "SUNWAY_PASSWORD", "SUNWAY_RECIPIENT")]
        shown = sms.clean_error(" ".join(private))
        for value in private:
            self.assertNotIn(value, shown)


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.healthy = {"status": "running", "health": "healthy", "started": "one", "exit": 0}

    def test_healthy_state_is_quiet(self):
        self.assertIsNone(transition(self.healthy, self.healthy.copy()))

    def test_stopping_and_starting_are_separate(self):
        stopped = {**self.healthy, "status": "exited"}
        self.assertEqual(transition(self.healthy, stopped), "توقف")
        self.assertEqual(transition(stopped, {**self.healthy, "started": "two"}), "شروع")

    def test_wait_for_startup(self):
        self.assertIsNone(transition(None, {**self.healthy, "health": "starting"}))


if __name__ == "__main__":
    unittest.main()
