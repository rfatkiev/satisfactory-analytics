from __future__ import annotations

import csv
import json
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from . import __version__
from .game_data import (
    build_class_to_desc_class,
    catalog_counts,
    display_name,
    extractor_rate_per_min,
    generator_consumption,
    normalize_class_name,
    recipe_class_name,
)


SCHEMA_VERSION = "2.0"
VENDOR_DIR = Path(__file__).resolve().parent.parent / ".vendor" / "sat_sav_parse"
RESOURCE_LABELS = {
    "Desc_OreIron_C": "Железо",
    "Desc_OreCopper_C": "Медь",
    "Desc_Stone_C": "Известняк",
    "Desc_RawQuartz_C": "Кварц",
}
MINER_BASE_RATES = {
    "Build_MinerMk1": {"IMPURE": 30.0, "NORMAL": 60.0, "PURE": 120.0},
    "Build_MinerMk2": {"IMPURE": 60.0, "NORMAL": 120.0, "PURE": 240.0},
    "Build_MinerMk3": {"IMPURE": 120.0, "NORMAL": 240.0, "PURE": 480.0},
}
PRODUCT_RATE_MAP = {
    "Recipe_IronRod_C": ("Железные прутья", 15.0),
    "Recipe_CopperSheet_C": ("Медные листы", 10.0),
    "Recipe_Concrete_C": ("Бетон", 15.0),
    "Recipe_GenericBiomass_C": ("Биомасса", 120.0),
    "Recipe_IronPlate_C": ("Железные пластины", 20.0),
    "Recipe_Wire_C": ("Железная проволока", 30.0),
    "Recipe_Biofuel_C": ("Твёрдое биотопливо", 60.0),
    "Recipe_Screw_C": ("Винты", 40.0),
    "Recipe_Cable_C": ("Кабель", 15.0),
}
PRODUCTION_CATEGORIES = (
    "Build_Miner",
    "Build_Constructor",
    "Build_Smelter",
    "Build_Assembler",
    "Build_Foundry",
    "Build_Manufacturer",
    "Build_Refinery",
    "Build_Blender",
    "Build_Packager",
    "Build_Generator",
    "Build_FrackingExtractor",
    "Build_OilPump",
    "Build_WaterPump",
)


@dataclass
class ParseWarning:
    message: str


@dataclass
class ParseResult:
    metadata: Dict[str, Any]
    entities: List[Dict[str, Any]]
    machine_counts: List[Dict[str, Any]]
    production: List[Dict[str, Any]]
    power: Dict[str, Any]
    properties_summary: List[Dict[str, Any]]
    metrics: Dict[str, Any]
    warnings: List[ParseWarning]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metadata": self.metadata,
            "entities": self.entities,
            "machine_counts": self.machine_counts,
            "production": self.production,
            "power": self.power,
            "properties_summary": self.properties_summary,
            "metrics": self.metrics,
            "warnings": [w.message for w in self.warnings],
        }


def parse_save(path: Path) -> ParseResult:
    sav_parse, resource_purity = _load_vendor_modules()
    parsed_save = sav_parse.readFullSaveFile(str(path))

    flattened = _flatten_save(parsed_save)
    machine_counts = _summarize_machine_counts(flattened["header_rows"])
    production = _build_production_rows(
        flattened["header_rows"],
        flattened["objects_by_name"],
        sav_parse,
        resource_purity,
    )
    power = _build_power_summary(flattened["objects_by_name"], production, sav_parse)
    properties_summary = _summarize_properties(flattened["objects_by_name"].values())
    metrics = _build_metrics(parsed_save, machine_counts, production, power, sav_parse)
    metadata = _build_metadata(path, parsed_save, flattened)

    return ParseResult(
        metadata=metadata,
        entities=flattened["entities"],
        machine_counts=machine_counts,
        production=production,
        power=power,
        properties_summary=properties_summary,
        metrics=metrics,
        warnings=[],
    )
def _load_vendor_modules():
    if not VENDOR_DIR.exists():
        raise FileNotFoundError(
            f"Vendor parser not found: {VENDOR_DIR}. Expected GreyHak sat_sav_parse."
        )

    vendor_path = str(VENDOR_DIR)
    if vendor_path not in sys.path:
        sys.path.insert(0, vendor_path)

    import sav_parse  # type: ignore
    from sav_data import resourcePurity  # type: ignore

    return sav_parse, resourcePurity


def _flatten_save(parsed_save: Any) -> Dict[str, Any]:
    entities: List[Dict[str, Any]] = []
    header_rows: List[Dict[str, Any]] = []
    objects_by_name: Dict[str, Any] = {}

    for level in parsed_save.levels:
        for obj in level.objects:
            objects_by_name[obj.instanceName] = obj

    for level in parsed_save.levels:
        for header, obj in zip(level.actorAndComponentObjectHeaders, level.objects):
            header_row = {
                "level_name": level.levelName,
                "header_kind": type(header).__name__,
                "instance_name": header.instanceName,
                "type_path": getattr(header, "typePath", ""),
                "class_name": _type_path_to_class_name(getattr(header, "typePath", "")),
                "root_object": getattr(header, "rootObject", ""),
                "position": list(getattr(header, "position", [])) or None,
                "rotation": list(getattr(header, "rotation", [])) or None,
                "scale": list(getattr(header, "scale", [])) or None,
                "was_placed_in_level": getattr(header, "wasPlacedInLevel", None),
                "flags": getattr(header, "flags", None),
            }
            header_rows.append(header_row)
            entities.append(
                {
                    **header_row,
                    "property_count": len(getattr(obj, "properties", [])),
                    "properties": _serialize_properties(
                        getattr(obj, "properties", []),
                        getattr(obj, "propertyTypes", []),
                    ),
                    "actor_reference_associations": _serialize_value(
                        getattr(obj, "actorReferenceAssociations", [])
                    ),
                    "actor_specific_info": _serialize_value(getattr(obj, "actorSpecificInfo", None)),
                }
            )

    return {
        "entities": entities,
        "header_rows": header_rows,
        "objects_by_name": objects_by_name,
        "actor_count": sum(1 for row in header_rows if row["header_kind"] == "ActorHeader"),
        "component_count": sum(1 for row in header_rows if row["header_kind"] == "ComponentHeader"),
    }


def _build_metadata(path: Path, parsed_save: Any, flattened: Dict[str, Any]) -> Dict[str, Any]:
    info = parsed_save.saveFileInfo
    return {
        "schema_version": SCHEMA_VERSION,
        "parser_version": __version__,
        "timestamp": int(time.time()),
        "save_name": getattr(info, "saveName", path.stem),
        "session_name": getattr(info, "sessionName", ""),
        "map_name": getattr(info, "mapName", ""),
        "map_options": getattr(info, "mapOptions", ""),
        "play_duration_seconds": getattr(info, "playDurationInSeconds", None),
        "save_date": getattr(info, "saveDateTimeInTicks", None),
        "save_header_type": getattr(info, "saveHeaderType", None),
        "save_version": getattr(info, "saveVersion", None),
        "build_version": getattr(info, "buildVersion", None),
        "is_modded": bool(getattr(info, "isModdedSave", 0)),
        "save_identifier": getattr(info, "saveIdentifier", None),
        "levels_count": len(parsed_save.levels),
        "partitions_count": len(getattr(parsed_save, "partitions", [])),
        "top_level_name": getattr(parsed_save, "aLevelName", ""),
        "objects_count": len(flattened["objects_by_name"]),
        "entities_count": len(flattened["entities"]),
        "actor_count": flattened["actor_count"],
        "component_count": flattened["component_count"],
        "source_path": str(path),
        "source_parser": "GreyHak sat_sav_parse",
        "source_parser_license": "GPL-3.0-only",
        "source_parser_path": str(VENDOR_DIR / "sav_parse.py"),
        "static_catalog": catalog_counts(),
    }


def _summarize_machine_counts(header_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counts: Counter[str] = Counter()
    for row in header_rows:
        if row["header_kind"] != "ActorHeader":
            continue
        class_name = row.get("class_name", "")
        if not class_name.startswith("Build_"):
            continue
        counts[class_name] += 1

    summary = []
    for class_name, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        summary.append(
            {
                "machine_type": class_name.replace("Build_", "", 1),
                "class_name": class_name,
                "count": count,
            }
        )
    return summary


def _build_production_rows(
    header_rows: Iterable[Dict[str, Any]],
    objects_by_name: Dict[str, Any],
    sav_parse: Any,
    resource_purity: Any,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for row in header_rows:
        if row["header_kind"] != "ActorHeader":
            continue

        class_name = row.get("class_name", "")
        if not any(class_name.startswith(prefix) for prefix in PRODUCTION_CATEGORIES):
            continue

        obj = objects_by_name.get(row["instance_name"])
        if obj is None:
            continue

        building_desc_class = build_class_to_desc_class(class_name)
        current_recipe = recipe_class_name(_object_ref_name(sav_parse.getPropertyValue(obj.properties, "mCurrentRecipe")))
        built_with_recipe = recipe_class_name(_object_ref_name(sav_parse.getPropertyValue(obj.properties, "mBuiltWithRecipe")))
        resource_ref = sav_parse.getPropertyValue(obj.properties, "mExtractableResource")
        resource_class = None
        resource_name = None
        resource_purity_name = None
        current_potential = _numeric_property(obj, sav_parse, "mCurrentPotential", 1.0)
        resource_rate = 0.0

        if resource_ref is not None:
            resource_key = getattr(resource_ref, "pathName", "")
            purity_info = resource_purity.RESOURCE_PURITY.get(resource_key)
            if purity_info is not None:
                (resource_desc, purity, _position, _core) = purity_info
                resource_class = normalize_class_name(resource_desc)
                resource_name = display_name(resource_desc) or RESOURCE_LABELS.get(resource_desc, resource_desc)
                resource_purity_name = getattr(purity, "name", str(purity))
            elif class_name == "Build_WaterPump":
                resource_class = "Desc_Water_C"
                resource_name = "Water"
                resource_purity_name = "NORMAL"

            resource_rate = extractor_rate_per_min(
                class_name,
                resource_class,
                resource_purity_name,
                current_potential,
            )

        power_info_ref = sav_parse.getPropertyValue(obj.properties, "mPowerInfo")
        power_obj = objects_by_name.get(getattr(power_info_ref, "pathName", ""))
        produced_power = _first_numeric_property(
            power_obj,
            sav_parse,
            ("mDynamicProductionCapacity", "mBaseProduction"),
        )
        consumed_power = _first_numeric_property(
            power_obj,
            sav_parse,
            ("mTargetConsumption", "mActualConsumption"),
        )
        max_consumed_power = _first_numeric_property(
            power_obj,
            sav_parse,
            ("mMaximumTargetConsumption", "mMaxTargetConsumption", "mMaximumConsumption"),
        )
        fuel_class = normalize_class_name(_object_ref_name(sav_parse.getPropertyValue(obj.properties, "mCurrentFuelClass")))
        generator_stats = generator_consumption(class_name, produced_power or 0.0, fuel_class)

        rows.append(
            {
                "instance_name": row["instance_name"],
                "level_name": row["level_name"],
                "machine_type": class_name.replace("Build_", "", 1),
                "class_name": class_name,
                "machine_name": display_name(building_desc_class) or display_name(class_name),
                "building_class": building_desc_class,
                "type_path": row.get("type_path", ""),
                "category": _machine_category(class_name),
                "current_recipe": current_recipe,
                "current_recipe_name": display_name(current_recipe),
                "built_with_recipe": built_with_recipe,
                "built_with_recipe_name": display_name(built_with_recipe),
                "is_producing": _bool_or_none(sav_parse.getPropertyValue(obj.properties, "mIsProducing")),
                "is_productivity_monitor_enabled": _bool_or_none(
                    sav_parse.getPropertyValue(obj.properties, "mIsProductivityMonitorEnabled")
                ),
                "current_potential": current_potential,
                "manufacturing_progress": _numeric_property(
                    obj, sav_parse, "mCurrentManufacturingProgress", None
                ),
                "resource_class": resource_class,
                "resource_type": resource_name,
                "resource_purity": resource_purity_name,
                "resource_rate_per_min": round(resource_rate, 3),
                "power_production_mw": round(produced_power or 0.0, 3),
                "power_consumption_mw": round(consumed_power or 0.0, 3),
                "max_power_consumption_mw": round(max_consumed_power or 0.0, 3),
                "fuel_item_class": generator_stats["fuel_item_class"],
                "fuel_item_name": generator_stats["fuel_item_name"],
                "fuel_consumption_per_min": generator_stats["fuel_rate_per_min"],
                "supplemental_item_class": generator_stats["supplemental_item_class"],
                "supplemental_item_name": generator_stats["supplemental_item_name"],
                "supplemental_consumption_per_min": generator_stats["supplemental_rate_per_min"],
                "position": row.get("position"),
            }
        )

    return sorted(rows, key=lambda item: (item["machine_type"], item["instance_name"]))


def _build_power_summary(objects_by_name: Dict[str, Any], production: List[Dict[str, Any]], sav_parse: Any) -> Dict[str, Any]:
    power_objects = []
    total_production = 0.0
    total_consumption = 0.0
    total_max_consumption = 0.0

    for instance_name, obj in objects_by_name.items():
        if not instance_name.endswith(".powerInfo"):
            continue

        production_value = _first_numeric_property(
            obj,
            sav_parse,
            ("mDynamicProductionCapacity", "mBaseProduction"),
        ) or 0.0
        consumption_value = _first_numeric_property(
            obj,
            sav_parse,
            ("mTargetConsumption", "mActualConsumption"),
        ) or 0.0
        max_consumption_value = _first_numeric_property(
            obj,
            sav_parse,
            ("mMaximumTargetConsumption", "mMaxTargetConsumption", "mMaximumConsumption"),
        ) or 0.0

        total_production += production_value
        total_consumption += consumption_value
        total_max_consumption += max_consumption_value
        power_objects.append(
            {
                "instance_name": instance_name,
                "production_mw": round(production_value, 3),
                "consumption_mw": round(consumption_value, 3),
                "max_consumption_mw": round(max_consumption_value, 3),
            }
        )

    generators = [row for row in production if row["category"] == "power_generation"]
    consumers = [row for row in production if row["category"] in {"manufacturing", "refining", "mining"}]

    return {
        "summary": {
            "total_production_mw": round(total_production, 3),
            "total_consumption_mw": round(total_consumption, 3),
            "total_max_consumption_mw": round(total_max_consumption, 3),
            "power_object_count": len(power_objects),
        },
        "generators": generators,
        "consumers": consumers,
        "networks": [],
        "power_objects": power_objects,
    }


def _summarize_properties(objects: Iterable[Any]) -> List[Dict[str, Any]]:
    counts: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for obj in objects:
        property_type_map = {name: prop_type for name, prop_type, *_ in getattr(obj, "propertyTypes", [])}
        for property_name, property_value in getattr(obj, "properties", []):
            property_type = property_type_map.get(property_name, _infer_property_type(property_value))
            key = (property_name, property_type)
            entry = counts.get(key)
            if entry is None:
                entry = {
                    "property_name": property_name,
                    "property_type": property_type,
                    "count": 0,
                    "true_count": 0,
                }
                counts[key] = entry
            entry["count"] += 1
            if isinstance(property_value, bool) and property_value:
                entry["true_count"] += 1
            elif property_value == 1 and property_type == "BoolProperty":
                entry["true_count"] += 1

    return sorted(counts.values(), key=lambda item: (-item["count"], item["property_name"]))


def _build_metrics(
    parsed_save: Any,
    machine_counts: List[Dict[str, Any]],
    production: List[Dict[str, Any]],
    power: Dict[str, Any],
    sav_parse: Any,
) -> Dict[str, Any]:
    machine_count_map = {item["machine_type"]: item["count"] for item in machine_counts}

    biomass_rows = [
        row
        for row in production
        if row["machine_type"] in {"GeneratorBiomass_Automated", "GeneratorIntegratedBiomass"}
    ]
    mining_rows = [row for row in production if row["category"] == "mining"]

    mining_totals: Dict[str, float] = {label: 0.0 for label in RESOURCE_LABELS.values()}
    mining_buildings: Dict[str, int] = {label: 0 for label in RESOURCE_LABELS.values()}
    for row in mining_rows:
        label = row.get("resource_type")
        if label in mining_totals:
            mining_totals[label] += float(row.get("resource_rate_per_min") or 0.0)
            mining_buildings[label] += 1

    schematic_obj = _find_object(parsed_save, "Persistent_Level:PersistentLevel.schematicManager")
    unlock_obj = _find_object(parsed_save, "Persistent_Level:PersistentLevel.unlockSubsystem")
    purchased_schematics = sav_parse.getPropertyValue(
        getattr(schematic_obj, "properties", []), "mPurchasedSchematics"
    ) or []

    hub_schematics = 0
    mam_schematics = 0
    awesome_schematics = 0
    for schematic in purchased_schematics:
        path_name = getattr(schematic, "pathName", "")
        if (
            "/Progression/" in path_name
            or "/Tutorial/" in path_name
            or "StartingRecipes" in path_name
            or "/Milestone/" in path_name
            or "/HUB/" in path_name
            or "/Tiers/" in path_name
        ):
            hub_schematics += 1
        elif "/Research/" in path_name or "/MAM/" in path_name:
            mam_schematics += 1
        elif "/ResourceSink/" in path_name or "/AwesomeShop/" in path_name:
            awesome_schematics += 1

    production_recipe_counts = Counter(
        row["current_recipe"] for row in production if row.get("current_recipe")
    )
    biomass_production_mw = sum(float(row.get("power_production_mw") or 0.0) for row in biomass_rows)
    biomass_active_production_mw = sum(
        float(row.get("power_production_mw") or 0.0)
        for row in biomass_rows
        if row.get("is_producing") is True
    )

    return {
        "biomass_total": len(biomass_rows),
        "biomass_working": sum(1 for row in biomass_rows if row.get("is_producing") is True),
        "biomass_production_mw": round(biomass_production_mw, 3),
        "biomass_active_production_mw": round(biomass_active_production_mw, 3),
        "power_production_mw": power["summary"]["total_production_mw"],
        "power_consumption_mw": power["summary"]["total_consumption_mw"],
        "power_max_consumption_mw": power["summary"]["total_max_consumption_mw"],
        "machine_totals": machine_count_map,
        "mining_buildings": mining_buildings,
        "mining_rate_per_min": {key: round(value, 3) for key, value in mining_totals.items()},
        "purchased_schematics_total": len(purchased_schematics),
        "purchased_hub_schematics": hub_schematics,
        "purchased_mam_schematics": mam_schematics,
        "purchased_awesome_schematics": awesome_schematics,
        "map_unlocked": _bool_or_none(
            sav_parse.getPropertyValue(getattr(unlock_obj, "properties", []), "mIsMapUnlocked")
        ),
        "building_efficiency_unlocked": _bool_or_none(
            sav_parse.getPropertyValue(
                getattr(unlock_obj, "properties", []), "mIsBuildingEfficiencyUnlocked"
            )
        ),
        "building_overclock_unlocked": _bool_or_none(
            sav_parse.getPropertyValue(
                getattr(unlock_obj, "properties", []), "mIsBuildingOverclockUnlocked"
            )
        ),
        "blueprints_unlocked": _bool_or_none(
            sav_parse.getPropertyValue(getattr(unlock_obj, "properties", []), "mIsBlueprintsUnlocked")
        ),
        "production_recipe_counts": dict(sorted(production_recipe_counts.items())),
    }


def _find_object(parsed_save: Any, instance_name: str) -> Any | None:
    needle = instance_name.casefold()
    for level in parsed_save.levels:
        for obj in level.objects:
            haystack = obj.instanceName.casefold()
            if haystack == needle or haystack.endswith(needle.rsplit(".", 1)[-1]):
                return obj
    return None


def _serialize_properties(properties: Iterable[Any], property_types: Iterable[Any]) -> Dict[str, Any]:
    property_type_map = {name: prop_type for name, prop_type, *_ in property_types}
    output: Dict[str, Any] = {}
    for property_name, property_value in properties:
        output[property_name] = {
            "type": property_type_map.get(property_name, _infer_property_type(property_value)),
            "value": _serialize_value(property_value),
        }
    return output


def _serialize_value(value: Any, depth: int = 0) -> Any:
    if depth >= 6:
        return "<max-depth>"
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "levelName") and hasattr(value, "pathName"):
        return {
            "level_name": getattr(value, "levelName", ""),
            "path_name": getattr(value, "pathName", ""),
        }
    if isinstance(value, dict):
        items = list(value.items())[:50]
        return {str(key): _serialize_value(item, depth + 1) for key, item in items}
    if isinstance(value, (list, tuple)):
        if all(isinstance(item, (list, tuple)) and len(item) == 2 and isinstance(item[0], str) for item in value):
            return {item[0]: _serialize_value(item[1], depth + 1) for item in list(value)[:50]}
        return [_serialize_value(item, depth + 1) for item in list(value)[:50]]
    return repr(value)


def _type_path_to_class_name(type_path: str) -> str:
    if not type_path:
        return ""
    class_name = type_path.rsplit("/", 1)[-1]
    class_name = class_name.split(".", 1)[0]
    if class_name.endswith("_C"):
        class_name = class_name[:-2]
    return class_name


def _machine_category(class_name: str) -> str:
    lower = class_name.lower()
    if "miner" in lower or "extractor" in lower or "waterpump" in lower or "oilpump" in lower:
        return "mining"
    if "smelter" in lower or "foundry" in lower or "refinery" in lower or "blender" in lower:
        return "refining"
    if "constructor" in lower or "assembler" in lower or "manufacturer" in lower or "packager" in lower:
        return "manufacturing"
    if "generator" in lower:
        return "power_generation"
    return "other"


def _object_ref_name(value: Any) -> str | None:
    path_name = getattr(value, "pathName", "")
    if not path_name:
        return None
    return normalize_class_name(path_name)


def _miner_rate(class_name: str, purity_name: str | None, current_potential: float) -> float:
    if purity_name is None:
        return 0.0
    rate_table = MINER_BASE_RATES.get(class_name)
    if rate_table is None:
        return 0.0
    return rate_table.get(purity_name, 0.0) * current_potential


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _numeric_property(obj: Any, sav_parse: Any, property_name: str, default: float | None) -> float | None:
    if obj is None:
        return default
    value = sav_parse.getPropertyValue(obj.properties, property_name)
    if isinstance(value, (int, float)):
        return float(value)
    return default


def _first_numeric_property(obj: Any, sav_parse: Any, property_names: Iterable[str]) -> float | None:
    if obj is None:
        return None
    for property_name in property_names:
        value = sav_parse.getPropertyValue(obj.properties, property_name)
        if isinstance(value, (int, float)):
            return float(value)
    return None


def _infer_property_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BoolProperty"
    if isinstance(value, int):
        return "IntProperty"
    if isinstance(value, float):
        return "FloatProperty"
    if isinstance(value, str):
        return "StrProperty"
    if hasattr(value, "levelName") and hasattr(value, "pathName"):
        return "ObjectProperty"
    if isinstance(value, list):
        return "ArrayProperty"
    if isinstance(value, tuple):
        return "StructProperty"
    return type(value).__name__


def write_json(result: ParseResult, output_path: Path) -> None:
    output_path.write_text(json.dumps(result.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(result: ParseResult, output_path: Path) -> None:
    time_headers, time_values = _time_columns(result.metadata)
    with output_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            time_headers
            + [
                "instance_name",
                "machine_type",
                "category",
                "current_recipe",
                "is_producing",
                "resource_type",
                "resource_purity",
                "resource_rate_per_min",
                "power_production_mw",
                "power_consumption_mw",
            ]
        )
        for row in result.production:
            writer.writerow(
                time_values
                + [
                    row.get("instance_name", ""),
                    row.get("machine_type", ""),
                    row.get("category", ""),
                    row.get("current_recipe", ""),
                    row.get("is_producing", ""),
                    row.get("resource_type", ""),
                    row.get("resource_purity", ""),
                    row.get("resource_rate_per_min", 0),
                    row.get("power_production_mw", 0),
                    row.get("power_consumption_mw", 0),
                ]
            )


def write_csv_bundle(result: ParseResult, output_path: Path) -> List[Path]:
    output_dir, prefix = _resolve_bundle_output(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    machine_counts = {item["machine_type"]: item["count"] for item in result.machine_counts}
    time_headers, time_values = _time_columns(result.metadata)

    def get_count(name: str) -> int:
        return int(machine_counts.get(name, 0))

    def sum_prefix(prefixes: Iterable[str]) -> int:
        total = 0
        for key, value in machine_counts.items():
            if any(key.startswith(prefix) for prefix in prefixes):
                total += int(value)
        return total

    mining_rate = result.metrics.get("mining_rate_per_min", {})
    production_totals = _summarize_output_rates(result.production)

    files: List[Path] = []
    files.append(
        _write_single_row_csv(
            output_dir / f"{prefix}_mining.csv",
            time_headers + ["Железо", "Медь", "Известняк", "Кварц"],
            time_values
            + [
                _fmt_number(mining_rate.get("Железо", 0)),
                _fmt_number(mining_rate.get("Медь", 0)),
                _fmt_number(mining_rate.get("Известняк", 0)),
                _fmt_number(mining_rate.get("Кварц", 0)),
            ],
        )
    )
    files.append(
        _write_single_row_csv(
            output_dir / f"{prefix}_production_stage1.csv",
            time_headers + ["Железные прутья", "Медные листы", "Бетон", "Биомасса"],
            time_values
            + [
                _fmt_number(production_totals.get("Железные прутья", 0)),
                _fmt_number(production_totals.get("Медные листы", 0)),
                _fmt_number(production_totals.get("Бетон", 0)),
                _fmt_number(production_totals.get("Биомасса", 0)),
            ],
        )
    )
    files.append(
        _write_single_row_csv(
            output_dir / f"{prefix}_production_stage2.csv",
            time_headers + ["Железные пластины", "Железная проволока", "Твёрдое биотопливо"],
            time_values
            + [
                _fmt_number(production_totals.get("Железные пластины", 0)),
                _fmt_number(production_totals.get("Железная проволока", 0)),
                _fmt_number(production_totals.get("Твёрдое биотопливо", 0)),
            ],
        )
    )
    files.append(
        _write_single_row_csv(
            output_dir / f"{prefix}_production_stage3.csv",
            time_headers + ["Винты", "Кабель"],
            time_values
            + [
                _fmt_number(production_totals.get("Винты", 0)),
                _fmt_number(production_totals.get("Кабель", 0)),
            ],
        )
    )
    files.append(
        _write_single_row_csv(
            output_dir / f"{prefix}_biomass.csv",
            time_headers
            + [
                "Сжигатель (только работающие шт.)",
                "Расход (шт)",
                "Производство (МВт)",
                "Пропускная способность (МВт)",
                "Потребление (МВт)",
                "Макс. потребление (МВт)",
            ],
            time_values
            + [
                str(result.metrics.get("biomass_working", 0)),
                "0",
                _fmt_number(result.metrics.get("biomass_active_production_mw", 0)),
                _fmt_number(result.metrics.get("biomass_production_mw", 0)),
                "0",
                "0",
            ],
        )
    )
    files.append(
        _write_single_row_csv(
            output_dir / f"{prefix}_power.csv",
            time_headers + ["Общая произв-во (МВт)", "Общие потреб-е (МВт)", "Макс. потреб-е (МВт)"],
            time_values
            + [
                _fmt_number(result.metrics.get("power_production_mw", 0)),
                _fmt_number(result.metrics.get("power_consumption_mw", 0)),
                _fmt_number(result.metrics.get("power_max_consumption_mw", 0)),
            ],
        )
    )
    files.append(
        _write_single_row_csv(
            output_dir / f"{prefix}_network.csv",
            time_headers
            + ["Линии электропередач (шт.)", "Электростолб 1 (шт.)", "Электростолб 2 (шт.)"],
            time_values
            + [
                str(get_count("PowerLine")),
                str(get_count("PowerPoleMk1")),
                str(get_count("PowerPoleMk2")),
            ],
        )
    )
    files.append(
        _write_single_row_csv(
            output_dir / f"{prefix}_buildings.csv",
            time_headers
            + [
                "Буровые установки",
                "Плавильня",
                "Конструктор",
                "Сборщик",
                "Личное хранилище",
                "Конвейер",
                "Лента",
            ],
            time_values
            + [
                str(sum_prefix(["MinerMk"])),
                str(get_count("SmelterMk1")),
                str(get_count("ConstructorMk1")),
                str(get_count("AssemblerMk1")),
                str(get_count("StoragePlayer")),
                str(sum_prefix(["ConveyorLift"])),
                str(sum_prefix(["ConveyorBelt"])),
            ],
        )
    )
    files.append(
        _write_single_row_csv(
            output_dir / f"{prefix}_tech.csv",
            time_headers
            + [
                "Открыто технологий",
                "Всего технологий открыто",
                "Уровень достигнут",
                "Улучшений открыто уже",
                "Всего открыто",
                "Убито существ 1",
                "Убито существ 2",
            ],
            time_values
            + [
                str(result.metrics.get("purchased_hub_schematics", 0)),
                str(result.metrics.get("purchased_schematics_total", 0)),
                str(result.metrics.get("purchased_hub_schematics", 0)),
                str(result.metrics.get("purchased_mam_schematics", 0)),
                str(
                    result.metrics.get("purchased_hub_schematics", 0)
                    + result.metrics.get("purchased_mam_schematics", 0)
                    + result.metrics.get("purchased_awesome_schematics", 0)
                ),
                "0",
                "0",
            ],
        )
    )
    files.append(
        _write_properties_csv(
            output_dir / f"{prefix}_properties.csv",
            time_headers,
            time_values,
            result.properties_summary,
        )
    )
    files.append(
        _write_machine_counts_csv(
            output_dir / f"{prefix}_machine_counts.csv",
            time_headers,
            time_values,
            result.machine_counts,
        )
    )
    files.append(
        _write_production_csv(
            output_dir / f"{prefix}_production.csv",
            time_headers,
            time_values,
            result.production,
        )
    )

    return files


def _summarize_output_rates(production: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    totals = {
        "Железные прутья": 0.0,
        "Медные листы": 0.0,
        "Бетон": 0.0,
        "Биомасса": 0.0,
        "Железные пластины": 0.0,
        "Железная проволока": 0.0,
        "Твёрдое биотопливо": 0.0,
        "Винты": 0.0,
        "Кабель": 0.0,
    }

    for row in production:
        recipe_name = row.get("current_recipe")
        if recipe_name in PRODUCT_RATE_MAP:
            label, base_rate = PRODUCT_RATE_MAP[recipe_name]
            totals[label] += base_rate * float(row.get("current_potential") or 1.0)

    return {key: round(value, 3) for key, value in totals.items()}


def _resolve_bundle_output(output_path: Path) -> Tuple[Path, str]:
    if output_path.suffix.lower() == ".csv":
        return output_path.parent, output_path.stem
    return output_path, "satisfactory"


def _write_single_row_csv(path: Path, headers: List[str], values: List[str]) -> Path:
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerow(values)
    return path


def _write_properties_csv(
    path: Path,
    time_headers: List[str],
    time_values: List[str],
    items: List[Dict[str, Any]],
) -> Path:
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(time_headers + ["property_name", "property_type", "count", "true_count"])
        for item in items:
            writer.writerow(
                time_values
                + [
                    item.get("property_name", ""),
                    item.get("property_type", ""),
                    str(item.get("count", 0)),
                    str(item.get("true_count", 0)),
                ]
            )
    return path


def _write_machine_counts_csv(
    path: Path,
    time_headers: List[str],
    time_values: List[str],
    items: List[Dict[str, Any]],
) -> Path:
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(time_headers + ["machine_type", "class_name", "count"])
        for item in items:
            writer.writerow(
                time_values
                + [
                    item.get("machine_type", ""),
                    item.get("class_name", ""),
                    str(item.get("count", 0)),
                ]
            )
    return path


def _write_production_csv(
    path: Path,
    time_headers: List[str],
    time_values: List[str],
    items: List[Dict[str, Any]],
) -> Path:
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            time_headers
            + [
                "instance_name",
                "machine_type",
                "category",
                "current_recipe",
                "is_producing",
                "current_potential",
                "resource_type",
                "resource_purity",
                "resource_rate_per_min",
                "power_production_mw",
                "power_consumption_mw",
                "max_power_consumption_mw",
            ]
        )
        for item in items:
            writer.writerow(
                time_values
                + [
                    item.get("instance_name", ""),
                    item.get("machine_type", ""),
                    item.get("category", ""),
                    item.get("current_recipe", ""),
                    item.get("is_producing", ""),
                    _fmt_number(item.get("current_potential", 0)),
                    item.get("resource_type", ""),
                    item.get("resource_purity", ""),
                    _fmt_number(item.get("resource_rate_per_min", 0)),
                    _fmt_number(item.get("power_production_mw", 0)),
                    _fmt_number(item.get("power_consumption_mw", 0)),
                    _fmt_number(item.get("max_power_consumption_mw", 0)),
                ]
            )
    return path


def _fmt_number(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return str(value)


def _time_columns(metadata: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    play_seconds = metadata.get("play_duration_seconds")
    if isinstance(play_seconds, int):
        play_hours = round(play_seconds / 3600, 2)
        play_hours_str = f"{play_hours:.2f}"
    else:
        play_hours_str = ""

    save_date_raw = metadata.get("save_date")
    save_date_str = str(save_date_raw) if save_date_raw is not None else ""

    return ["play_time_hours", "save_date_raw"], [play_hours_str, save_date_str]
