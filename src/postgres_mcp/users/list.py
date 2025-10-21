"""List users operation implementation."""

from typing import Optional, Dict, Any, List
import psycopg
from datetime import datetime
from ..permissions import Permission
from ..operations import OperationResult, MonitoringOperation


class ListUsersOperation(MonitoringOperation):
    """Operation to list database users and roles."""

    def get_required_permissions(self) -> List[Permission]:
        """Get required permissions for listing users."""
        return [Permission.VIEW_USERS]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        """Validate list users operation parameters.

        Args:
            filter_type: Filter by type (users, roles, all)
            include_permissions: Include permission details
            include_memberships: Include role memberships
            include_settings: Include user configuration settings
            pattern: Filter by name pattern (SQL LIKE pattern)
            show_system: Include system users/roles

        Returns:
            Tuple of (is_valid, error_message)
        """
        filter_type = kwargs.get("filter_type", "all")
        if filter_type not in ["users", "roles", "all"]:
            return False, "filter_type must be 'users', 'roles', or 'all'"

        return True, None

    async def execute(self, **kwargs) -> OperationResult:
        """Execute the list users operation.

        Args:
            filter_type: Filter by type (users, roles, all)
            include_permissions: Include permission details
            include_memberships: Include role memberships
            include_settings: Include user configuration settings
            include_owned_objects: Include count of owned objects
            pattern: Filter by name pattern
            show_system: Include system users/roles

        Returns:
            OperationResult with list of users/roles
        """
        filter_type = kwargs.get("filter_type", "all")
        include_permissions = kwargs.get("include_permissions", False)
        include_memberships = kwargs.get("include_memberships", False)
        include_settings = kwargs.get("include_settings", False)
        include_owned_objects = kwargs.get("include_owned_objects", False)
        pattern = kwargs.get("pattern")
        show_system = kwargs.get("show_system", False)

        try:
            # Build base query
            base_query = """
                SELECT
                    r.rolname as name,
                    r.rolsuper as is_superuser,
                    r.rolinherit as inherits,
                    r.rolcreaterole as can_create_role,
                    r.rolcreatedb as can_create_db,
                    r.rolcanlogin as can_login,
                    r.rolreplication as is_replication,
                    r.rolbypassrls as bypass_rls,
                    r.rolconnlimit as connection_limit,
                    r.rolvaliduntil as valid_until,
                    r.oid as role_id,
                    CASE
                        WHEN r.rolcanlogin THEN 'user'
                        ELSE 'role'
                    END as type,
                    pg_catalog.shobj_description(r.oid, 'pg_authid') as description,
                    ARRAY(
                        SELECT b.rolname
                        FROM pg_catalog.pg_auth_members m
                        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
                        WHERE m.member = r.oid
                    ) as member_of,
                    (
                        SELECT COUNT(*)
                        FROM pg_stat_activity
                        WHERE usename = r.rolname
                    ) as active_connections
                FROM pg_roles r
                WHERE 1=1
            """

            conditions = []
            params = []

            # Apply type filter
            if filter_type == "users":
                conditions.append("r.rolcanlogin = true")
            elif filter_type == "roles":
                conditions.append("r.rolcanlogin = false")

            # Apply pattern filter
            if pattern:
                conditions.append("r.rolname LIKE %s")
                params.append(pattern)

            # Filter system users/roles
            if not show_system:
                conditions.append("r.rolname NOT LIKE 'pg_%'")
                conditions.append("r.rolname != 'postgres'")

            # Add conditions to query
            if conditions:
                base_query += " AND " + " AND ".join(conditions)

            base_query += " ORDER BY r.rolname"

            users_list = []

            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(base_query, params)
                    rows = await cur.fetchall()

                    for row in rows:
                        user_info = {
                            "name": row[0],
                            "type": row[11],
                            "is_superuser": row[1],
                            "inherits": row[2],
                            "can_create_role": row[3],
                            "can_create_db": row[4],
                            "can_login": row[5],
                            "is_replication": row[6],
                            "bypass_rls": row[7],
                            "connection_limit": row[8] if row[8] != -1 else "unlimited",
                            "valid_until": row[9].isoformat() if row[9] else None,
                            "description": row[12],
                            "active_connections": row[14]
                        }

                        # Add role memberships if requested
                        if include_memberships:
                            user_info["member_of"] = row[13] if row[13] else []

                            # Also get members of this role (if it's a role)
                            if row[11] == "role":
                                members_query = """
                                    SELECT u.rolname
                                    FROM pg_auth_members m
                                    JOIN pg_roles u ON m.member = u.oid
                                    WHERE m.roleid = %s
                                """
                                await cur.execute(members_query, (row[10],))
                                members = await cur.fetchall()
                                user_info["members"] = [m[0] for m in members] if members else []

                        # Add permissions if requested
                        if include_permissions:
                            perms = await self.get_user_permissions(row[0])
                            # Filter out empty permission categories
                            user_info["permissions"] = {
                                k: v for k, v in perms.items() if v
                            }

                        # Add owned objects count if requested
                        if include_owned_objects:
                            owned = await self.get_owned_objects(row[0])
                            user_info["owned_objects_count"] = {
                                k: len(v) for k, v in owned.items() if v
                            }

                        # Add settings if requested
                        if include_settings:
                            settings_query = """
                                SELECT unnest(rolconfig) as setting
                                FROM pg_roles
                                WHERE rolname = %s
                                AND rolconfig IS NOT NULL
                            """
                            await cur.execute(settings_query, (row[0],))
                            settings = await cur.fetchall()
                            if settings:
                                user_info["settings"] = [s[0] for s in settings]

                        users_list.append(user_info)

            # Get summary statistics
            total_users = len([u for u in users_list if u["type"] == "user"])
            total_roles = len([u for u in users_list if u["type"] == "role"])
            total_superusers = len([u for u in users_list if u["is_superuser"]])
            total_active = sum(u["active_connections"] for u in users_list)

            return OperationResult(
                success=True,
                operation_type=self.get_operation_type(),
                operation_name="ListUsers",
                affected_objects=[],
                message=f"Found {len(users_list)} users/roles ({total_users} users, {total_roles} roles)",
                details={
                    "users": users_list,
                    "summary": {
                        "total": len(users_list),
                        "users": total_users,
                        "roles": total_roles,
                        "superusers": total_superusers,
                        "active_connections": total_active
                    },
                    "filters": {
                        "type": filter_type,
                        "pattern": pattern,
                        "show_system": show_system
                    }
                }
            )

        except Exception as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="ListUsers",
                error=f"Failed to list users: {str(e)}",
                details={"filter_type": filter_type}
            )