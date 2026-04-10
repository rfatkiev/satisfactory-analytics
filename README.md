# WORK IN PROGRESS

# Satisfactory Save Parser

Python tools for parsing Satisfactory save files and exporting factory analytics.

## Features

- Full save parsing for modern Satisfactory saves
- JSON export with parsed entities, properties, production, and power data
- CSV export for quick analysis
- Normalized PostgreSQL export:
  - `save_snapshots`
  - `machines`
  - `machine_power`
  - `machine_recipes`
  - `resource_extraction`
- Static game catalog for items, recipes, buildings, generators, and extractors

## Project Structure

- `satisfactory_parser/` - parser, game data helpers, PostgreSQL export
- `tests/` - tests and sample save files
- `.vendor/` - bundled third-party parsers and game data sources
- `schema.sql` - PostgreSQL schema

## Requirements

- Python 3.13+

## Clone

Clone with submodules:

```powershell
git clone --recurse-submodules https://github.com/<your-username>/<your-repo>.git
```

If you already cloned the repository:

```powershell
git submodule update --init --recursive
```

## Usage

Parse a save to JSON or CSV bundle:

```powershell
python -m satisfactory_parser parse --input "path\\to\\save.sav" --output "csv_out"
```

Export normalized PostgreSQL tables:

```powershell
python -m satisfactory_parser export-db --input "path\\to\\save.sav" --output-dir "pg_export"
```

Load exported tables into PostgreSQL:

```powershell
python -m satisfactory_parser load-postgres --input-dir "pg_export" --dsn "postgresql://user:password@host:5432/dbname"
```

## Notes

- The project depends on Git submodules in `.vendor/`.
- PostgreSQL loading requires `psycopg` or `psycopg2`.
- Real `.sav` files are not included in the repository.
- Generated outputs such as `csv_out/`, `pg_export/`, and `out.json` are ignored by Git.
