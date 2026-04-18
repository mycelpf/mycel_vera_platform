#!/usr/bin/env python3
"""
Vera Provisioning Loader — Direct SQL seeding from layered JSON files.

Reads provisioning JSON from the 4-layer hierarchy (universal → country → LOB → client)
and inserts records directly into PostgreSQL. Idempotent via ON CONFLICT (code) DO UPDATE.

Usage from any module's bootstrap.py:
    from provision_loader import load_provisioning
    load_provisioning(db_url, provisioning_root, layers=["universal", "country/india", "lob/motor", "client/hegi"])
"""
import asyncio
import json
import os
from pathlib import Path
from typing import Optional
from uuid import uuid4
from datetime import datetime

import asyncpg


# ── TypeKey INSERT ──────────────────────────────────────────────────────

TYPEKEY_UPSERT = """
INSERT INTO mycel_vera_products.type_keys
    (id, code, name, categories, priority, can_retired, sort_order, version, created_at, updated_at)
VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, 1, $8, $8)
ON CONFLICT (code) WHERE code IS NOT NULL DO UPDATE SET
    name = EXCLUDED.name,
    categories = EXCLUDED.categories,
    priority = EXCLUDED.priority,
    can_retired = EXCLUDED.can_retired,
    sort_order = EXCLUDED.sort_order,
    updated_at = EXCLUDED.updated_at
"""


async def _check_unique_code_index(conn: asyncpg.Connection) -> bool:
    """Check if the unique index on type_keys.code exists; create if not."""
    row = await conn.fetchrow(
        "SELECT 1 FROM pg_indexes WHERE schemaname = 'mycel_vera_products' "
        "AND tablename = 'type_keys' AND indexdef LIKE '%unique%code%'"
    )
    if not row:
        # Create partial unique index for idempotent upsert
        await conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS type_keys_code_uq "
            "ON mycel_vera_products.type_keys (code) WHERE code IS NOT NULL"
        )
        return True
    return False


async def _seed_typekeys(conn: asyncpg.Connection, records: list[dict], source: str) -> int:
    """Insert TypeKey records in a single batch. Returns count of records submitted."""
    now = datetime.utcnow()
    rows = []
    for rec in records:
        code = rec.get("code")
        if not code:
            continue
        categories = json.dumps(rec.get("categories", []))
        rows.append((
            uuid4(), code, rec.get("name", code), categories,
            rec.get("priority", 0), rec.get("canRetired", False),
            rec.get("sortOrder", rec.get("priority", 0)),
            now,
        ))
    if rows:
        await conn.executemany(TYPEKEY_UPSERT, rows)
    return len(rows)


# ── Activity Template INSERT ────────────────────────────────────────────

ACTIVITY_TEMPLATE_UPSERT = """
INSERT INTO mycel_vera_products.activity_templates
    (id, code, name, default_priority, sla_days, is_mandatory, version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $7)
ON CONFLICT DO NOTHING
"""


async def _seed_activity_templates(conn: asyncpg.Connection, records: list[dict]) -> int:
    """Insert ActivityTemplate records if table has code column."""
    # Check if unique index exists
    await conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS activity_templates_code_uq "
        "ON mycel_vera_products.activity_templates (code) WHERE code IS NOT NULL"
    )
    now = datetime.utcnow()
    rows = []
    for rec in records:
        code = rec.get("code")
        if not code:
            continue
        rows.append((
            uuid4(), code, rec.get("name", code), rec.get("description"),
            rec.get("defaultPriority", "routine"),
            rec.get("isMandatory", False),
            now,
        ))
    if rows:
        await conn.executemany(
            """INSERT INTO mycel_vera_products.activity_templates
               (id, code, subject, description, priority, mandatory, version, created_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $7)
               ON CONFLICT (code) WHERE code IS NOT NULL DO NOTHING""",
            rows,
        )
    return len(rows)


# ── Claims config INSERT handlers ───────────────────────────────────────

async def _seed_business_rules(conn: asyncpg.Connection, records: list[dict]) -> int:
    now = datetime.utcnow()
    rows = [(uuid4(), r.get("ruleCode"), r.get("name"), r.get("description"),
             r.get("value"), r.get("unit"), r.get("lob"), now)
            for r in records if r.get("ruleCode")]
    if rows:
        await conn.executemany(
            """INSERT INTO mycel_vera_claims.claims_business_rules
               (id, rule_code, name, description, value, unit, lob, created_at, updated_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8)
               ON CONFLICT DO NOTHING""", rows)
    return len(rows)


async def _seed_assignment_rules(conn: asyncpg.Connection, records: list[dict]) -> int:
    now = datetime.utcnow()
    rows = [(uuid4(), r.get("ruleCode"), r.get("name"), r.get("description"),
             r.get("queueType"), r.get("lob"), r.get("priority"),
             r.get("triggerEvent"), r.get("strategy"),
             json.dumps(r.get("assignableRoles", [])),
             r.get("maxCaseload"), r.get("active", True), now)
            for r in records if r.get("ruleCode")]
    if rows:
        await conn.executemany(
            """INSERT INTO mycel_vera_claims.claims_assignment_rules
               (id, rule_code, name, description, queue_type, lob, priority,
                trigger_event, strategy, assignable_roles, max_caseload, active, created_at, updated_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$13)
               ON CONFLICT DO NOTHING""", rows)
    return len(rows)


async def _seed_reserve_rules(conn: asyncpg.Connection, records: list[dict]) -> int:
    now = datetime.utcnow()
    rows = [(uuid4(), r.get("exposureType"), r.get("name"), r.get("description"),
             r.get("formula"), r.get("percentage"), r.get("basis"), r.get("currency"), now)
            for r in records if r.get("exposureType")]
    if rows:
        await conn.executemany(
            """INSERT INTO mycel_vera_claims.claims_reserve_rules
               (id, exposure_type, name, description, formula, percentage, basis, currency, created_at, updated_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)
               ON CONFLICT DO NOTHING""", rows)
    return len(rows)


async def _seed_surveyor_panel(conn: asyncpg.Connection, records: list[dict]) -> int:
    now = datetime.utcnow()
    rows = [(uuid4(), r.get("licenseNumber"), r.get("name"), r.get("zone"),
             r.get("specialization"), r.get("contactPhone"), r.get("contactEmail"),
             json.dumps(r.get("feeSchedule", {})), r.get("turnaroundHours"), now)
            for r in records if r.get("licenseNumber")]
    if rows:
        await conn.executemany(
            """INSERT INTO mycel_vera_claims.claims_surveyor_panel
               (id, license_number, name, zone, specialization, contact_phone, contact_email,
                fee_schedule, turnaround_hours, created_at, updated_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$10)
               ON CONFLICT DO NOTHING""", rows)
    return len(rows)


async def _seed_garage_network(conn: asyncpg.Connection, records: list[dict]) -> int:
    now = datetime.utcnow()
    rows = [(uuid4(), r.get("code"), r.get("name"), r.get("networkStatus"),
             r.get("city"), r.get("state"), json.dumps(r.get("authorizedBrands", [])),
             r.get("labourRatePerHourINR"), r.get("partsDiscountPercent"),
             r.get("contactPhone"), r.get("contactEmail"),
             r.get("bankAccountForCashless"), now)
            for r in records if r.get("code")]
    if rows:
        await conn.executemany(
            """INSERT INTO mycel_vera_claims.claims_garage_network
               (id, code, name, network_status, city, state, authorized_brands,
                labour_rate_per_hour_inr, parts_discount_percent,
                contact_phone, contact_email, bank_account_for_cashless, created_at, updated_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,$12,$13,$13)
               ON CONFLICT DO NOTHING""", rows)
    return len(rows)


async def _seed_legal_panel(conn: asyncpg.Connection, records: list[dict]) -> int:
    now = datetime.utcnow()
    rows = [(uuid4(), r.get("name"), r.get("specialization"), r.get("city"), r.get("state"),
             r.get("barCouncilId"), r.get("contactPhone"), r.get("contactEmail"),
             json.dumps(r.get("feeSchedule", {})), now)
            for r in records if r.get("name")]
    if rows:
        await conn.executemany(
            """INSERT INTO mycel_vera_claims.claims_legal_panel
               (id, name, specialization, city, state, bar_council_id,
                contact_phone, contact_email, fee_schedule, created_at, updated_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$10)
               ON CONFLICT DO NOTHING""", rows)
    return len(rows)


async def _seed_medical_panel(conn: asyncpg.Connection, records: list[dict]) -> int:
    from datetime import date
    now = datetime.utcnow()
    rows = []
    for r in records:
        if not r.get("name"):
            continue
        emp_date = r.get("empanelmentDate")
        if emp_date:
            try:
                emp_date = date.fromisoformat(emp_date)
            except (ValueError, TypeError):
                emp_date = None
        rows.append((uuid4(), r.get("name"), r.get("type"), r.get("city"), r.get("state"),
                     json.dumps(r.get("specialties", [])), r.get("contactPhone"), emp_date, now))
    if rows:
        await conn.executemany(
            """INSERT INTO mycel_vera_claims.claims_medical_panel
               (id, name, facility_type, city, state, specialties,
                contact_phone, empanelment_date, created_at, updated_at)
               VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$9)
               ON CONFLICT DO NOTHING""", rows)
    return len(rows)


# ── Depreciation Schedule INSERT ────────────────────────────────────────

async def _seed_depreciation_schedule(conn: asyncpg.Connection, records: list[dict]) -> int:
    now = datetime.utcnow()
    rows = [(uuid4(), r.get("partCategory"), r.get("ageRange"),
             r.get("depreciationPercent"), r.get("description"), now)
            for r in records if r.get("partCategory")]
    if rows:
        await conn.executemany(
            """INSERT INTO mycel_vera_products.depreciation_schedules
               (id, part_category, age_range, depreciation_percent, description, created_at, updated_at)
               VALUES ($1,$2,$3,$4,$5,$6,$6)
               ON CONFLICT DO NOTHING""", rows)
    return len(rows)


# ── Main Loader ─────────────────────────────────────────────────────────

# Map API endpoint patterns to handler functions
ENDPOINT_HANDLERS = {
    "type-keys": "typekeys",
    "activity/templates": "activity_templates",
    "document-templates": "typekeys",       # Same shape as TypeKeys
    "depreciation-schedule": "depreciation_schedule",
    "/vera_claims/config/business-rules": "business_rules",
    "/vera_claims/config/assignment-rules": "assignment_rules",
    "/vera_claims/config/reserve-rules": "reserve_rules",
    "/vera_claims/config/surveyor-panel": "surveyor_panel",
    "/vera_claims/config/garage-network": "garage_network",
    "/vera_claims/config/legal-panel": "legal_panel",
    "/vera_claims/config/medical-panel": "medical_panel",
    "/iam/permissions": "skip",             # Handled by RBAC bootstrap
    "/iam/roles": "skip",                   # Handled by RBAC bootstrap
    "/iam/users": "skip",                   # Runtime concern, not seed data
    "/iam/service-accounts": "skip",        # Runtime concern, not seed data
}


def _classify_endpoint(endpoint: str) -> str:
    """Classify an API endpoint into a handler type."""
    for pattern, handler in ENDPOINT_HANDLERS.items():
        if pattern in endpoint:
            return handler
    # Default: most code-table endpoints use TypeKey shape
    return "typekeys"


async def _load_file(conn: asyncpg.Connection, filepath: Path, stats: dict, claims_conn: asyncpg.Connection = None) -> None:
    """Load a single provisioning JSON file."""
    with open(filepath) as f:
        data = json.load(f)

    meta = data.get("_metadata", {})
    records = data.get("records", [])
    if not records:
        return

    endpoint = meta.get("apiEndpoint", "")
    handler = _classify_endpoint(endpoint)

    if handler == "skip":
        stats["skipped"] += len(records)
        return

    # Claims-specific handlers use claims_conn if available, else skip
    claims_handlers = {
        "business_rules", "assignment_rules", "reserve_rules",
        "surveyor_panel", "garage_network", "legal_panel", "medical_panel",
    }

    if handler == "typekeys":
        count = await _seed_typekeys(conn, records, str(filepath))
        stats["typekeys"] += count
    elif handler == "activity_templates":
        count = await _seed_activity_templates(conn, records)
        stats["other"] += count
    elif handler == "depreciation_schedule":
        count = await _seed_depreciation_schedule(conn, records)
        stats["other"] += count
    elif handler in claims_handlers:
        c = claims_conn or conn
        if handler == "business_rules":
            count = await _seed_business_rules(c, records)
        elif handler == "assignment_rules":
            count = await _seed_assignment_rules(c, records)
        elif handler == "reserve_rules":
            count = await _seed_reserve_rules(c, records)
        elif handler == "surveyor_panel":
            count = await _seed_surveyor_panel(c, records)
        elif handler == "garage_network":
            count = await _seed_garage_network(c, records)
        elif handler == "legal_panel":
            count = await _seed_legal_panel(c, records)
        elif handler == "medical_panel":
            count = await _seed_medical_panel(c, records)
        stats["other"] += count
    else:
        stats["skipped"] += len(records)


async def _load_layer(conn: asyncpg.Connection, layer_path: Path, stats: dict, emit,
                      claims_conn: asyncpg.Connection = None) -> None:
    """Load all JSON files from a provisioning layer directory."""
    if not layer_path.is_dir():
        return

    json_files = sorted(layer_path.rglob("*.json"))
    for fp in json_files:
        try:
            await _load_file(conn, fp, stats, claims_conn=claims_conn)
        except Exception as e:
            emit(f"    ! Error loading {fp.name}: {e}")


async def load_provisioning(
    db_url: str,
    provisioning_root: str,
    layers: Optional[list[str]] = None,
    claims_db_url: Optional[str] = None,
    emit=print,
) -> dict:
    """
    Load provisioning data from layered JSON files into PostgreSQL.

    Args:
        db_url: PostgreSQL connection URL for products schema (asyncpg format)
        provisioning_root: Path to mycel_vera_provisioning/ directory
        layers: List of layer paths to load, e.g. ["universal", "country/india", "lob/motor", "client/hegi"]
                Defaults to ["universal"] if not specified.
        claims_db_url: Optional separate URL for claims schema (for config tables).
                       Falls back to db_url if not provided.
        emit: Callback for log output

    Returns:
        Stats dict with counts of loaded records
    """
    if layers is None:
        layers = ["universal"]

    root = Path(provisioning_root)
    if not root.is_dir():
        emit(f"  ! Provisioning root not found: {provisioning_root}")
        return {"typekeys": 0, "other": 0, "skipped": 0}

    def _dsn(url: str) -> str:
        return url.replace("postgresql+asyncpg://", "postgresql://")

    conn = await asyncpg.connect(_dsn(db_url))
    claims_conn = await asyncpg.connect(_dsn(claims_db_url)) if claims_db_url else None

    stats = {"typekeys": 0, "other": 0, "skipped": 0}

    try:
        created = await _check_unique_code_index(conn)
        if created:
            emit("  Created unique index on type_keys.code")

        for layer in layers:
            layer_path = root / layer
            emit(f"  Loading layer: {layer} ({layer_path.name}/)")
            await _load_layer(conn, layer_path, stats, emit, claims_conn=claims_conn)

        emit(f"  Provisioning complete: {stats['typekeys']} TypeKeys, {stats['other']} other, {stats['skipped']} skipped")

    finally:
        await conn.close()
        if claims_conn:
            await claims_conn.close()

    return stats


# ── CLI entry point ─────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Vera Provisioning Loader")
    parser.add_argument("--db-url", required=True, help="PostgreSQL connection URL")
    parser.add_argument("--root", required=True, help="Path to mycel_vera_provisioning/")
    parser.add_argument("--layers", nargs="+", default=["universal"], help="Layers to load")
    args = parser.parse_args()

    print(f"\n=== Vera Provisioning Loader ===")
    result = asyncio.run(load_provisioning(args.db_url, args.root, args.layers))
    print(f"\nDone: {result}")
