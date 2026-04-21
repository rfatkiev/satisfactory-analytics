import unittest
from pathlib import Path

from satisfactory_parser.parse import parse_save, write_timeline_csv, write_timeline_csv_bundle


class TestRealSave(unittest.TestCase):
    def test_real_save_smoke(self):
        save_path = Path(__file__).parent / "Третий день.sav"
        if not save_path.exists():
            self.skipTest("Real save file not found")

        result = parse_save(save_path)
        self.assertIn("schema_version", result.metadata)
        self.assertIn("parser_version", result.metadata)
        self.assertIn("save_datetime", result.metadata)
        self.assertGreater(result.metadata.get("objects_count", 0), 0)
        self.assertGreater(len(result.machine_counts), 0)
        self.assertGreater(len(result.production), 0)
        self.assertIn("summary", result.power)

    def test_timeline_csv_smoke(self):
        save_paths = sorted(Path(__file__).parent.glob("*.sav"))
        if not save_paths:
            self.skipTest("Real save files not found")

        output_path = Path(__file__).parent / "_timeline_smoke.csv"
        try:
            results = [parse_save(path) for path in save_paths[:2]]
            write_timeline_csv(results, output_path)
            self.assertTrue(output_path.exists())
            header = output_path.read_text(encoding="utf-8-sig").splitlines()[0]
            self.assertIn("save_datetime", header)
            self.assertIn("save_day", header)
            self.assertIn("metric__power_production_mw", header)
        finally:
            if output_path.exists():
                output_path.unlink()

    def test_timeline_bundle_smoke(self):
        save_paths = sorted(Path(__file__).parent.glob("*.sav"))
        if not save_paths:
            self.skipTest("Real save files not found")

        output_dir = Path(__file__).parent / "_timeline_bundle_smoke"
        try:
            results = [parse_save(path) for path in save_paths[:2]]
            files = write_timeline_csv_bundle(results, output_dir)
            self.assertGreater(len(files), 0)
            metadata_path = output_dir / "saves_metadata.csv"
            self.assertTrue(metadata_path.exists())
            header = metadata_path.read_text(encoding="utf-8-sig").splitlines()[0]
            self.assertIn("save_datetime", header)
            self.assertIn("save_day", header)
            self.assertIn("play_time_hours", header)
            self.assertIn("save_date_raw", header)
        finally:
            if output_dir.exists():
                for path in output_dir.glob("*"):
                    path.unlink()
                output_dir.rmdir()


if __name__ == "__main__":
    unittest.main()
