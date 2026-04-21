from __future__ import annotations

import argparse
from pathlib import Path

from .parse import (
    parse_save,
    parse_saves_in_directory,
    write_csv,
    write_csv_bundle,
    write_json,
    write_timeline_csv,
    write_timeline_csv_bundle,
)
from .postgres import load_postgres_bundle, write_postgres_bundle


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="satisfactory_parser")
    sub = parser.add_subparsers(dest="command", required=True)

    parse_cmd = sub.add_parser("parse", help="Parse a Satisfactory .sav file")
    parse_cmd.add_argument("--input", required=True, help="Path to .sav file")
    parse_cmd.add_argument("--output", required=True, help="Path to output JSON/CSV or output directory for CSV bundle")

    scan_cmd = sub.add_parser("scan-folder", help="Parse all .sav files in a directory and export one timeline CSV")
    scan_cmd.add_argument("--input-dir", required=True, help="Directory with .sav files")
    scan_cmd.add_argument("--output", required=True, help="Path to aggregated CSV file")

    export_db_cmd = sub.add_parser("export-db", help="Export normalized PostgreSQL CSV tables from a .sav file")
    export_db_cmd.add_argument("--input", required=True, help="Path to .sav file")
    export_db_cmd.add_argument("--output-dir", required=True, help="Directory for normalized CSV tables")

    load_db_cmd = sub.add_parser("load-postgres", help="Load normalized CSV tables into PostgreSQL")
    load_db_cmd.add_argument("--input-dir", required=True, help="Directory produced by export-db")
    load_db_cmd.add_argument("--dsn", required=True, help="PostgreSQL DSN")
    load_db_cmd.add_argument("--schema", required=False, help="Optional path to schema.sql")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "parse":
        input_path = Path(args.input)
        output_path = Path(args.output)
        result = parse_save(input_path)

        if output_path.is_dir() or output_path.suffix == "":
            write_csv_bundle(result, output_path)
            return 0

        if output_path.suffix.lower() == ".csv":
            write_csv_bundle(result, output_path)
        elif output_path.suffix.lower() == ".json":
            write_json(result, output_path)
        else:
            write_csv(result, output_path)
        return 0

    if args.command == "scan-folder":
        input_dir = Path(args.input_dir)
        output_path = Path(args.output)
        results = parse_saves_in_directory(input_dir)
        if output_path.suffix.lower() == ".csv":
            write_timeline_csv(results, output_path)
        else:
            write_timeline_csv_bundle(results, output_path)
        return 0

    if args.command == "export-db":
        input_path = Path(args.input)
        output_dir = Path(args.output_dir)
        result = parse_save(input_path)
        write_postgres_bundle(result, output_dir)
        return 0

    if args.command == "load-postgres":
        input_dir = Path(args.input_dir)
        schema_path = Path(args.schema) if args.schema else None
        load_postgres_bundle(input_dir, args.dsn, schema_path)
        return 0

    parser.print_help()
    return 1
