import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("classic", root / "classic.py")
classic = importlib.util.module_from_spec(spec)
spec.loader.exec_module(classic)


class StartupTests(unittest.TestCase):
    def test_modified_download_is_never_imported(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = Path(folder) / ".cache"
            cache.mkdir()
            (cache / "instnwnd.sql").write_bytes(b"unverified source")
            with patch.object(classic, "ROOT", Path(folder)), self.assertRaisesRegex(
                ValueError, "checksum"
            ):
                classic.source_bytes()

    def test_existing_source_skips_sample(self):
        with patch.object(
            classic, "sql", side_effect=["1", "8", "1"]
        ) as sql, patch.object(classic, "source_bytes") as download, patch.object(
            classic, "script"
        ) as script:
            classic.prepare_source()
        download.assert_not_called()
        self.assertEqual(sql.call_count, 3)
        self.assertEqual(script.call_args.args[1], "NorthwindDW")

    def test_incomplete_existing_source_stops_without_overwrite(self):
        with patch.object(classic, "sql", side_effect=["1", "3"]), patch.object(
            classic, "script"
        ) as script, self.assertRaisesRegex(RuntimeError, "incomplete"):
            classic.prepare_source()
        script.assert_not_called()

    def test_half_import_is_not_published(self):
        with patch.object(
            classic, "source_bytes", return_value="SELECT 1"
        ), patch.object(
            classic, "sql", side_effect=["0", "1", "5", "0"]
        ) as sql, patch.object(
            classic, "script"
        ) as script, self.assertRaisesRegex(
            RuntimeError, "Unrecognized"
        ):
            classic.prepare_source()
        script.assert_not_called()
        self.assertFalse(any("MODIFY NAME" in c.args[0] for c in sql.call_args_list))

    def test_resume_existing_dag_run(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(
            classic, "STATE", Path(folder)
        ), patch.object(classic, "wait_dag"), patch.object(
            classic, "pg", side_effect=["running", "running", "success"]
        ), patch.object(
            classic, "trigger"
        ) as trigger:
            (Path(folder) / "batch.json").write_text(
                '{"run_id":"classic_00000000000000000000000000000000"}'
            )
            classic.batch()
        trigger.assert_not_called()


if __name__ == "__main__":
    unittest.main()
