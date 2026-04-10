from __future__ import annotations

import csv
import hashlib
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .game_data import (
    build_class_to_desc_class,
    display_name,
    recipe_rates,
)


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema.sql"
TABLE_FILES = {
    "save_snapshots": "save_snapshots.csv",
    "machines": "machines.csv",
    "machine_power": "machine_power.csv",
    "machine_recipes": "machine_recipes.csv",
    "resource_extraction": "resource_extraction.csv",
}


def snapshot_id_from_metadata(metadata: Dict[str, Any]) -> str:
    raw = "|".join(
        [
            str(metadata.get("save_name", "")),
            str(metadata.get("save_date", "")),
            str(metadata.get("play_duration_seconds", "")),
            str(metadata.get("source_path", "")),
        ]
    )
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"snapshot_{metadata.get('save_date', 'unknown')}_{digest}"


def write_postgres_bundle(result: Any, output_dir: Path) -> List[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_id = snapshot_id_from_metadata(result.metadata)
    production_by_instance = {row["instance_name"]: row for row in result.production}

    files: List[Path] = []
    files.append(_write_rows(output_dir / TABLE_FILES["save_snapshots"], _snapshot_rows(result, snapshot_id)))
    files.append(_write_rows(output_dir / TABLE_FILES["machines"], _machine_rows(result, snapshot_id, production_by_instance)))
    files.append(_write_rows(output_dir / TABLE_FILES["machine_power"], _machine_power_rows(result, snapshot_id)))
    files.append(_write_rows(output_dir / TABLE_FILES["machine_recipes"], _machine_recipe_rows(result, snapshot_id)))
    files.append(_write_rows(output_dir / TABLE_FILES["resource_extraction"], _resource_extraction_rows(result, snapshot_id)))

    if SCHEMA_PATH.exists():
        shutil.copyfile(SCHEMA_PATH, output_dir / "schema.sql")

    return files


def load_postgres_bundle(input_dir: Path, dsn: str, schema_path: Path | None = None) -> None:
    connect, copy_impl = _get_postgres_driver()
    schema_file = schema_path or (input_dir / "schema.sql" if (input_dir / "schema.sql").exists() else SCHEMA_PATH)
    if not schema_file.exists():
        raise FileNotFoundError(f"schema.sql not found: {schema_file}")

    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_file.read_text(encoding="utf-8"))
            conn.commit()
            for table_name, file_name in TABLE_FILES.items():
                csv_path = input_dir / file_name
                if not csv_path.exists():
                    continue
                copy_impl(cur, table_name, csv_path)
            conn.commit()


def _snapshot_rows(result: Any, snapshot_id: str) -> List[Dict[str, Any]]:
    metadata = result.metadata
    metrics = result.metrics
    return [
        {
            "snapshot_id": snapshot_id,
            "save_name": metadata.get("save_name"),
            "session_name": metadata.get("session_name"),
            "source_path": metadata.get("source_path"),
            "save_header_type": metadata.get("save_header_type"),
            "save_version": metadata.get("save_version"),
            "build_version": metadata.get("build_version"),
            "play_duration_seconds": metadata.get("play_duration_seconds"),
            "save_date_raw": metadata.get("save_date"),
            "parser_version": metadata.get("parser_version"),
            "schema_version": metadata.get("schema_version"),
            "levels_count": metadata.get("levels_count"),
            "partitions_count": metadata.get("partitions_count"),
            "objects_count": metadata.get("objects_count"),
            "actor_count": metadata.get("actor_count"),
            "component_count": metadata.get("component_count"),
            "biomass_total": metrics.get("biomass_total"),
            "biomass_working": metrics.get("biomass_working"),
            "power_production_mw": metrics.get("power_production_mw"),
            "power_consumption_mw": metrics.get("power_consumption_mw"),
            "power_max_consumption_mw": metrics.get("power_max_consumption_mw"),
        }
    ]


def _machine_rows(result: Any, snapshot_id: str, production_by_instance: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for entity in result.entities:
        if entity.get("header_kind") != "ActorHeader":
            continue
        class_name = entity.get("class_name", "")
        if not str(class_name).startswith("Build_"):
            continue

        prod = production_by_instance.get(entity["instance_name"], {})
        position = entity.get("position") or [None, None, None]
        rows.append(
            {
                "snapshot_id": snapshot_id,
                "instance_name": entity.get("instance_name"),
                "level_name": entity.get("level_name"),
                "machine_type": class_name.replace("Build_", "", 1),
                "class_name": class_name,
                "machine_name": prod.get("machine_name") or display_name(build_class_to_desc_class(class_name)) or display_name(class_name),
                "building_class": prod.get("building_class") or build_class_to_desc_class(class_name),
                "category": prod.get("category") or "other",
                "type_path": entity.get("type_path"),
                "built_with_recipe_class": prod.get("built_with_recipe"),
                "built_with_recipe_name": prod.get("built_with_recipe_name"),
                "current_recipe_class": prod.get("current_recipe"),
                "current_recipe_name": prod.get("current_recipe_name"),
                "is_producing": prod.get("is_producing"),
                "current_potential": prod.get("current_potential"),
                "manufacturing_progress": prod.get("manufacturing_progress"),
                "position_x": position[0] if len(position) > 0 else None,
                "position_y": position[1] if len(position) > 1 else None,
                "position_z": position[2] if len(position) > 2 else None,
            }
        )
    return rows


def _machine_power_rows(result: Any, snapshot_id: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for prod in result.production:
        rows.append(
            {
                "snapshot_id": snapshot_id,
                "instance_name": prod.get("instance_name"),
                "power_production_mw": prod.get("power_production_mw"),
                "power_consumption_mw": prod.get("power_consumption_mw"),
                "max_power_consumption_mw": prod.get("max_power_consumption_mw"),
                "fuel_item_class": prod.get("fuel_item_class"),
                "fuel_item_name": prod.get("fuel_item_name"),
                "fuel_consumption_per_min": prod.get("fuel_consumption_per_min"),
                "supplemental_item_class": prod.get("supplemental_item_class"),
                "supplemental_item_name": prod.get("supplemental_item_name"),
                "supplemental_consumption_per_min": prod.get("supplemental_consumption_per_min"),
            }
        )
    return rows


def _machine_recipe_rows(result: Any, snapshot_id: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for prod in result.production:
        recipe = recipe_rates(prod.get("current_recipe"), prod.get("building_class"), float(prod.get("current_potential") or 1.0))
        if recipe is not None:
            for direction, items in (("input", recipe["ingredients"]), ("output", recipe["products"])):
                for item in items:
                    rows.append(
                        {
                            "snapshot_id": snapshot_id,
                            "instance_name": prod.get("instance_name"),
                            "recipe_class": recipe.get("recipe_class"),
                            "recipe_name": recipe.get("recipe_name"),
                            "direction": direction,
                            "item_class": item.get("item_class"),
                            "item_name": item.get("item_name"),
                            "amount_per_cycle": item.get("amount_per_cycle"),
                            "cycle_time_seconds": recipe.get("cycle_time_seconds"),
                            "amount_per_min": item.get("amount_per_min"),
                            "is_alternate": recipe.get("is_alternate"),
                        }
                    )
            continue

        if prod.get("category") == "mining" and prod.get("resource_class"):
            rows.append(
                {
                    "snapshot_id": snapshot_id,
                    "instance_name": prod.get("instance_name"),
                    "recipe_class": None,
                    "recipe_name": "Extraction",
                    "direction": "output",
                    "item_class": prod.get("resource_class"),
                    "item_name": prod.get("resource_type"),
                    "amount_per_cycle": None,
                    "cycle_time_seconds": None,
                    "amount_per_min": prod.get("resource_rate_per_min"),
                    "is_alternate": False,
                }
            )

        if prod.get("category") == "power_generation" and prod.get("fuel_item_class"):
            rows.append(
                {
                    "snapshot_id": snapshot_id,
                    "instance_name": prod.get("instance_name"),
                    "recipe_class": None,
                    "recipe_name": "Power Generation",
                    "direction": "input",
                    "item_class": prod.get("fuel_item_class"),
                    "item_name": prod.get("fuel_item_name"),
                    "amount_per_cycle": None,
                    "cycle_time_seconds": None,
                    "amount_per_min": prod.get("fuel_consumption_per_min"),
                    "is_alternate": False,
                }
            )
            if prod.get("supplemental_item_class"):
                rows.append(
                    {
                        "snapshot_id": snapshot_id,
                        "instance_name": prod.get("instance_name"),
                        "recipe_class": None,
                        "recipe_name": "Power Generation",
                        "direction": "input",
                        "item_class": prod.get("supplemental_item_class"),
                        "item_name": prod.get("supplemental_item_name"),
                        "amount_per_cycle": None,
                        "cycle_time_seconds": None,
                        "amount_per_min": prod.get("supplemental_consumption_per_min"),
                        "is_alternate": False,
                    }
                )
    return rows


def _resource_extraction_rows(result: Any, snapshot_id: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for prod in result.production:
        if prod.get("category") != "mining" or not prod.get("resource_class"):
            continue
        rows.append(
            {
                "snapshot_id": snapshot_id,
                "instance_name": prod.get("instance_name"),
                "extractor_class": prod.get("class_name"),
                "extractor_name": prod.get("machine_name"),
                "resource_class": prod.get("resource_class"),
                "resource_name": prod.get("resource_type"),
                "purity": prod.get("resource_purity"),
                "amount_per_min": prod.get("resource_rate_per_min"),
            }
        )
    return rows


def _write_rows(path: Path, rows: List[Dict[str, Any]]) -> Path:
    headers = list(rows[0].keys()) if rows else []
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        if headers:
            writer.writeheader()
            for row in rows:
                writer.writerow({key: _csv_value(value) for key, value in row.items()})
    return path


def _csv_value(value: Any) -> Any:
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _get_postgres_driver():
    try:
        import psycopg  # type: ignore

        def connect(dsn: str):
            return psycopg.connect(dsn)

        def copy_impl(cur: Any, table_name: str, csv_path: Path) -> None:
            with cur.copy(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)") as copy:
                with csv_path.open("r", encoding="utf-8") as handle:
                    while True:
                        chunk = handle.read(1024 * 1024)
                        if not chunk:
                            break
                        copy.write(chunk)

        return connect, copy_impl
    except ImportError:
        pass

    try:
        import psycopg2  # type: ignore

        def connect(dsn: str):
            return psycopg2.connect(dsn)

        def copy_impl(cur: Any, table_name: str, csv_path: Path) -> None:
            with csv_path.open("r", encoding="utf-8") as handle:
                cur.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", handle)

        return connect, copy_impl
    except ImportError as exc:
        raise ImportError("Install `psycopg` or `psycopg2` to load data into PostgreSQL.") from exc
