from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable


BASE_DIR = Path(__file__).resolve().parent.parent
TOOLS_DATA_PATH = BASE_DIR / ".vendor" / "SatisfactoryTools" / "data" / "data.json"
VENDOR_PARSE_DIR = BASE_DIR / ".vendor" / "sat_sav_parse"
RESOURCE_MULTIPLIERS = {"IMPURE": 0.5, "NORMAL": 1.0, "PURE": 2.0}
BUILD_TO_DESC_OVERRIDES = {
    "Build_GeneratorBiomass_Automated": "Desc_GeneratorBiomass_C",
    "Build_GeneratorIntegratedBiomass": "Desc_GeneratorBiomass_C",
    "Build_GeneratorBiomass": "Desc_GeneratorBiomass_C",
    "Build_WaterPump": "Desc_WaterPump_C",
}


@dataclass(frozen=True)
class GameData:
    items: Dict[str, Dict[str, Any]]
    recipes: Dict[str, Dict[str, Any]]
    schematics: Dict[str, Dict[str, Any]]
    generators: Dict[str, Dict[str, Any]]
    resources: Dict[str, Dict[str, Any]]
    miners: Dict[str, Dict[str, Any]]
    buildings: Dict[str, Dict[str, Any]]
    readable_names: Dict[str, str]


@lru_cache(maxsize=1)
def load_game_data() -> GameData:
    if not TOOLS_DATA_PATH.exists():
        raise FileNotFoundError(f"SatisfactoryTools data file not found: {TOOLS_DATA_PATH}")

    vendor_path = str(VENDOR_PARSE_DIR)
    if vendor_path not in sys.path:
        sys.path.insert(0, vendor_path)

    from sav_data import readableNames  # type: ignore

    data = json.loads(TOOLS_DATA_PATH.read_text(encoding="utf-8"))
    return GameData(
        items=data["items"],
        recipes=data["recipes"],
        schematics=data["schematics"],
        generators=data["generators"],
        resources=data["resources"],
        miners=data["miners"],
        buildings=data["buildings"],
        readable_names=dict(readableNames.READABLE_PATH_NAME_CORRECTIONS),
    )


def normalize_class_name(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = str(raw)
    if not value:
        return None
    if "/" in value:
        value = value.rsplit("/", 1)[-1]
    if "." in value:
        value = value.rsplit(".", 1)[-1]
    return value


def recipe_class_name(raw: str | None) -> str | None:
    value = normalize_class_name(raw)
    if value is None:
        return None
    if value in load_game_data().recipes:
        return value
    if not value.endswith("_C") and f"{value}_C" in load_game_data().recipes:
        return f"{value}_C"
    return value


def item_class_name(raw: str | None) -> str | None:
    value = normalize_class_name(raw)
    if value is None:
        return None
    if value in load_game_data().items:
        return value
    if not value.endswith("_C") and f"{value}_C" in load_game_data().items:
        return f"{value}_C"
    return value


def build_class_to_desc_class(build_class: str | None) -> str | None:
    value = normalize_class_name(build_class)
    if value is None:
        return None
    if value in BUILD_TO_DESC_OVERRIDES:
        return BUILD_TO_DESC_OVERRIDES[value]
    if value.startswith("Build_"):
        return f"Desc_{value.replace('Build_', '', 1)}_C"
    if value.startswith("Desc_"):
        return value if value.endswith("_C") else f"{value}_C"
    return value


def get_recipe(recipe_class: str | None) -> Dict[str, Any] | None:
    key = recipe_class_name(recipe_class)
    if key is None:
        return None
    return load_game_data().recipes.get(key)


def get_item(item_class: str | None) -> Dict[str, Any] | None:
    key = item_class_name(item_class)
    if key is None:
        return None
    return load_game_data().items.get(key)


def get_building(desc_class: str | None) -> Dict[str, Any] | None:
    key = normalize_class_name(desc_class)
    if key is None:
        return None
    data = load_game_data()
    return data.buildings.get(key) or data.generators.get(key) or data.miners.get(key)


def get_generator(desc_class: str | None) -> Dict[str, Any] | None:
    key = normalize_class_name(desc_class)
    if key is None:
        return None
    return load_game_data().generators.get(key)


def get_miner(desc_class: str | None) -> Dict[str, Any] | None:
    key = normalize_class_name(desc_class)
    if key is None:
        return None
    return load_game_data().miners.get(key)


def display_name(class_name: str | None) -> str | None:
    key = normalize_class_name(class_name)
    if key is None:
        return None
    data = load_game_data()
    if key in data.items:
        return data.items[key]["name"]
    if key in data.recipes:
        return data.recipes[key]["name"]
    building = get_building(key)
    if building is not None:
        return building.get("name")
    if key in data.readable_names:
        return data.readable_names[key]
    if key.endswith("_C") and key in data.readable_names:
        return data.readable_names[key]
    return key.replace("_C", "").replace("_", " ")


def recipe_rates(recipe_class: str | None, building_desc_class: str | None, potential: float = 1.0) -> Dict[str, Any] | None:
    recipe = get_recipe(recipe_class)
    if recipe is None:
        return None
    building = get_building(building_desc_class)
    manufacturing_speed = 1.0
    if building is not None:
        manufacturing_speed = float(building.get("metadata", {}).get("manufacturingSpeed") or 1.0)

    overclock = max(float(potential or 1.0), 0.0)
    if overclock == 0:
        overclock = 1.0

    recipe_time = float(recipe["time"])
    production_time = (1.0 / overclock) * recipe_time * (1.0 / manufacturing_speed)
    cycle_rate = 60.0 / production_time if production_time else 0.0

    return {
        "recipe_class": recipe["className"],
        "recipe_name": recipe["name"],
        "cycle_time_seconds": round(production_time, 6),
        "ingredients": [
            {
                "item_class": item_class_name(ingredient["item"]),
                "item_name": display_name(ingredient["item"]),
                "amount_per_cycle": float(ingredient["amount"]),
                "amount_per_min": round(float(ingredient["amount"]) * cycle_rate, 6),
            }
            for ingredient in recipe["ingredients"]
        ],
        "products": [
            {
                "item_class": item_class_name(product["item"]),
                "item_name": display_name(product["item"]),
                "amount_per_cycle": float(product["amount"]),
                "amount_per_min": round(float(product["amount"]) * cycle_rate, 6),
            }
            for product in recipe["products"]
        ],
        "is_alternate": bool(recipe.get("alternate")),
    }


def extractor_rate_per_min(
    build_class: str | None,
    resource_class: str | None,
    purity: str | None,
    potential: float = 1.0,
) -> float:
    desc_class = build_class_to_desc_class(build_class)
    if desc_class == "Desc_WaterPump_C":
        return round(120.0 * float(potential or 1.0), 6)

    miner = get_miner(desc_class)
    if miner is None:
        return 0.0

    base_rate = (float(miner["itemsPerCycle"]) / float(miner["extractCycleTime"])) * 60.0
    if miner.get("allowLiquids"):
        base_rate /= 1000.0
    multiplier = RESOURCE_MULTIPLIERS.get((purity or "NORMAL").upper(), 1.0)
    return round(base_rate * multiplier * float(potential or 1.0), 6)


def generator_consumption(
    build_class: str | None,
    actual_power_mw: float,
    fuel_item_class: str | None,
) -> Dict[str, Any]:
    desc_class = build_class_to_desc_class(build_class)
    generator = get_generator(desc_class)
    fuel_item = get_item(fuel_item_class)

    fuel_rate = None
    if generator is not None and fuel_item is not None:
        energy_value = float(fuel_item.get("energyValue") or 0.0)
        if energy_value > 0:
            fuel_rate = round((float(actual_power_mw) / energy_value) * 60.0, 6)

    water_rate = None
    if generator is not None and float(generator.get("waterToPowerRatio") or 0.0) > 0:
        water_rate = round((60.0 * (float(actual_power_mw) * float(generator["waterToPowerRatio"]))) / 1000.0, 6)

    return {
        "fuel_item_class": item_class_name(fuel_item_class),
        "fuel_item_name": display_name(fuel_item_class),
        "fuel_rate_per_min": fuel_rate,
        "supplemental_item_class": "Desc_Water_C" if water_rate is not None else None,
        "supplemental_item_name": "Water" if water_rate is not None else None,
        "supplemental_rate_per_min": water_rate,
    }


def catalog_counts() -> Dict[str, int]:
    data = load_game_data()
    return {
        "items": len(data.items),
        "recipes": len(data.recipes),
        "buildings": len(data.buildings),
        "generators": len(data.generators),
        "miners": len(data.miners),
        "resources": len(data.resources),
        "schematics": len(data.schematics),
    }


def known_item_classes() -> Iterable[str]:
    return load_game_data().items.keys()
