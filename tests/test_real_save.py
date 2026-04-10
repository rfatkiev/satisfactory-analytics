import unittest
from pathlib import Path

from satisfactory_parser.parse import parse_save


class TestRealSave(unittest.TestCase):
    def test_real_save_smoke(self):
        save_path = Path(__file__).parent / "Третий день.sav"
        if not save_path.exists():
            self.skipTest("Real save file not found")

        result = parse_save(save_path)
        self.assertIn("schema_version", result.metadata)
        self.assertIn("parser_version", result.metadata)
        self.assertGreater(result.metadata.get("objects_count", 0), 0)
        self.assertGreater(len(result.machine_counts), 0)
        self.assertGreater(len(result.production), 0)
        self.assertIn("summary", result.power)


if __name__ == "__main__":
    unittest.main()
