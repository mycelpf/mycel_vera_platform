#!/usr/bin/env python3
"""
RBAC Bootstrap for mycel_vera_platform

Seeds permissions and roles into IAM schema using raw SQL.
No dependency on IAM code — connects directly to IAM database.

Runs during: mycel-platform fabric db seed --module mycel_vera_platform
"""
import asyncio
import os

import asyncpg


PERMISSIONS = [
    ("read", "analytics", "Read analytics records"),
    ("write", "analytics", "Create/update/delete analytics records"),
    ("read", "integration", "Read integration records"),
    ("write", "integration", "Create/update/delete integration records"),
    ("read", "shared", "Read shared records"),
    ("write", "shared", "Create/update/delete shared records"),
    ("read", "workflow", "Read workflow records"),
    ("write", "workflow", "Create/update/delete workflow records"),
]

ROLES = {
    "mycel_vera_platform_reader": {
        "description": "Read-only access to mycel_vera_platform data",
        "filter": lambda a, r: a == "read",
    },
    "mycel_vera_platform_writer": {
        "description": "Full CRUD access to mycel_vera_platform data",
        "filter": lambda a, r: True,
    },
}


async def seed_rbac():
    iam_url = os.environ.get("IAM_DATABASE_URL")
    if not iam_url:
        print("  ! IAM_DATABASE_URL not set — skipping RBAC seed")
        return

    # asyncpg needs postgresql:// not postgresql+asyncpg://
    dsn = iam_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)

    try:
        # Find PLATFORM tenant
        tenant = await conn.fetchrow(
            "SELECT id FROM mycel_iam.tenants WHERE type = $1", "PLATFORM"
        )
        if not tenant:
            print("  ! PLATFORM tenant not found — run IAM seed first")
            return

        tenant_id = tenant["id"]
        print(f"  Tenant: mycel_iam ({tenant_id})")

        # Step 1: Seed permissions
        print("  Seeding permissions...")
        perm_ids = {}

        for action, resource, description in PERMISSIONS:
            row = await conn.fetchrow(
                "SELECT id FROM mycel_iam.permissions "
                "WHERE tenant_id = $1 AND action = $2 AND resource = $3",
                tenant_id, action, resource,
            )
            if row:
                perm_ids[(action, resource)] = row["id"]
            else:
                row = await conn.fetchrow(
                    "INSERT INTO mycel_iam.permissions (id, tenant_id, action, resource, description, created_at) "
                    "VALUES (gen_random_uuid(), $1, $2, $3, $4, now()) RETURNING id",
                    tenant_id, action, resource, description,
                )
                perm_ids[(action, resource)] = row["id"]
                print(f"    + {action}:{resource}")

        # Step 2: Seed roles
        print("  Seeding roles...")
        for role_name, role_def in ROLES.items():
            row = await conn.fetchrow(
                "SELECT id FROM mycel_iam.roles "
                "WHERE tenant_id = $1 AND name = $2",
                tenant_id, role_name,
            )
            if row:
                role_id = row["id"]
                print(f"    - Role exists: {role_name}")
            else:
                row = await conn.fetchrow(
                    "INSERT INTO mycel_iam.roles (id, tenant_id, name, description, is_system, created_at, updated_at) "
                    "VALUES (gen_random_uuid(), $1, $2, $3, true, now(), now()) RETURNING id",
                    tenant_id, role_name, role_def["description"],
                )
                role_id = row["id"]
                print(f"    + Created role: {role_name}")

            # Link permissions
            linked = 0
            for (action, resource), perm_id in perm_ids.items():
                if not role_def["filter"](action, resource):
                    continue
                exists = await conn.fetchrow(
                    "SELECT 1 FROM mycel_iam.role_permissions "
                    "WHERE role_id = $1 AND permission_id = $2",
                    role_id, perm_id,
                )
                if not exists:
                    await conn.execute(
                        "INSERT INTO mycel_iam.role_permissions (role_id, permission_id, assigned_at) "
                        "VALUES ($1, $2, now())",
                        role_id, perm_id,
                    )
                    linked += 1
            if linked:
                print(f"    + Linked {linked} permissions to {role_name}")

    finally:
        await conn.close()

    print(f"  RBAC seed complete for mycel_vera_platform")


if __name__ == "__main__":
    print(f"\n=== RBAC Bootstrap: mycel_vera_platform ===")
    asyncio.run(seed_rbac())
