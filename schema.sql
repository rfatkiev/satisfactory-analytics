CREATE TABLE IF NOT EXISTS save_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    save_name TEXT NOT NULL,
    session_name TEXT,
    source_path TEXT,
    save_header_type INTEGER,
    save_version INTEGER,
    build_version INTEGER,
    play_duration_seconds INTEGER,
    save_date_raw BIGINT,
    parser_version TEXT,
    schema_version TEXT,
    levels_count INTEGER,
    partitions_count INTEGER,
    objects_count INTEGER,
    actor_count INTEGER,
    component_count INTEGER,
    biomass_total INTEGER,
    biomass_working INTEGER,
    power_production_mw DOUBLE PRECISION,
    power_consumption_mw DOUBLE PRECISION,
    power_max_consumption_mw DOUBLE PRECISION,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS machines (
    snapshot_id TEXT NOT NULL REFERENCES save_snapshots(snapshot_id) ON DELETE CASCADE,
    instance_name TEXT NOT NULL,
    level_name TEXT,
    machine_type TEXT,
    class_name TEXT,
    machine_name TEXT,
    building_class TEXT,
    category TEXT,
    type_path TEXT,
    built_with_recipe_class TEXT,
    built_with_recipe_name TEXT,
    current_recipe_class TEXT,
    current_recipe_name TEXT,
    is_producing BOOLEAN,
    current_potential DOUBLE PRECISION,
    manufacturing_progress DOUBLE PRECISION,
    position_x DOUBLE PRECISION,
    position_y DOUBLE PRECISION,
    position_z DOUBLE PRECISION,
    PRIMARY KEY (snapshot_id, instance_name)
);

CREATE TABLE IF NOT EXISTS machine_power (
    snapshot_id TEXT NOT NULL,
    instance_name TEXT NOT NULL,
    power_production_mw DOUBLE PRECISION,
    power_consumption_mw DOUBLE PRECISION,
    max_power_consumption_mw DOUBLE PRECISION,
    fuel_item_class TEXT,
    fuel_item_name TEXT,
    fuel_consumption_per_min DOUBLE PRECISION,
    supplemental_item_class TEXT,
    supplemental_item_name TEXT,
    supplemental_consumption_per_min DOUBLE PRECISION,
    PRIMARY KEY (snapshot_id, instance_name),
    FOREIGN KEY (snapshot_id, instance_name) REFERENCES machines(snapshot_id, instance_name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS machine_recipes (
    snapshot_id TEXT NOT NULL,
    instance_name TEXT NOT NULL,
    recipe_class TEXT,
    recipe_name TEXT,
    direction TEXT NOT NULL CHECK (direction IN ('input', 'output')),
    item_class TEXT,
    item_name TEXT,
    amount_per_cycle DOUBLE PRECISION,
    cycle_time_seconds DOUBLE PRECISION,
    amount_per_min DOUBLE PRECISION,
    is_alternate BOOLEAN,
    FOREIGN KEY (snapshot_id, instance_name) REFERENCES machines(snapshot_id, instance_name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS resource_extraction (
    snapshot_id TEXT NOT NULL,
    instance_name TEXT NOT NULL,
    extractor_class TEXT,
    extractor_name TEXT,
    resource_class TEXT,
    resource_name TEXT,
    purity TEXT,
    amount_per_min DOUBLE PRECISION,
    FOREIGN KEY (snapshot_id, instance_name) REFERENCES machines(snapshot_id, instance_name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_machines_snapshot_category ON machines (snapshot_id, category);
CREATE INDEX IF NOT EXISTS idx_machine_recipes_snapshot_item ON machine_recipes (snapshot_id, item_class);
CREATE INDEX IF NOT EXISTS idx_resource_extraction_snapshot_resource ON resource_extraction (snapshot_id, resource_class);
