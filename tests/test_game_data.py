import tempfile
import unittest
from pathlib import Path

from satisfactory_parser.game_data import catalog_counts, load_game_data, recipe_rates
from satisfactory_parser.parse import parse_save
from satisfactory_parser.postgres import write_postgres_bundle


class TestGameData(unittest.TestCase):
    def test_catalog_loaded(self):
        counts = catalog_counts()
        self.assertGreater(counts["items"], 100)
        self.assertGreater(counts["recipes"], 500)
        self.assertGreater(counts["buildings"], 300)

    def test_recipe_rates(self):
        rates = recipe_rates("Recipe_IronPlate_C", "Desc_ConstructorMk1_C", 1.0)
        self.assertIsNotNone(rates)
        self.assertEqual(rates["recipe_name"], "Iron Plate")
        self.assertGreater(len(rates["products"]), 0)


class TestPostgresExport(unittest.TestCase):
    def test_export_bundle(self):
        save_path = Path(__file__).parent / "Третий день.sav"
        if not save_path.exists():
            self.skipTest("Real save file not found")

        result = parse_save(save_path)
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = write_postgres_bundle(result, Path(tmpdir))
            self.assertGreaterEqual(len(paths), 5)
            self.assertTrue((Path(tmpdir) / "save_snapshots.csv").exists())
            self.assertTrue((Path(tmpdir) / "machines.csv").exists())


if __name__ == "__main__":
    unittest.main()
