"""Base class for user management operations."""

from typing import Optional, Dict, Any, List
import psycopg
from ..operations import DDLOperation
from ..permissions import Permission


class UserOperation(DDLOperation):
    """Base class for user and role operations."""

    async def check_user_exists(self, username: str) -> bool:
        """Check if user exists in the database.

        Args:
            username: The username to check

        Returns:
            True if user exists, False otherwise
        """
        query = "SELECT EXISTS(SELECT 1 FROM pg_user WHERE usename = %s)"

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (username,))
                result = await cur.fetchone()
                return result[0] if result else False

    async def check_role_exists(self, rolename: str) -> bool:
        """Check if role exists in the database.

        Args:
            rolename: The role name to check

        Returns:
            True if role exists, False otherwise
        """
        query = "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = %s)"

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (rolename,))
                result = await cur.fetchone()
                return result[0] if result else False

    async def get_user_info(self, username: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a user.

        Args:
            username: The username to get info for

        Returns:
            Dictionary with user information or None if user doesn't exist
        """
        query = """
            SELECT
                u.usename,
                u.usesysid,
                u.usecreatedb,
                u.usesuper,
                u.userepl,
                u.usebypassrls,
                u.valuntil,
                u.useconfig,
                CASE
                    WHEN u.usesuper THEN -1
                    ELSE COALESCE(r.rolconnlimit, -1)
                END as connection_limit,
                r.rolcanlogin,
                r.rolcreaterole,
                r.rolcreatedb,
                r.rolinherit,
                r.rolreplication
            FROM pg_user u
            JOIN pg_roles r ON u.usename = r.rolname
            WHERE u.usename = %s
        """

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (username,))
                result = await cur.fetchone()

                if not result:
                    return None

                return {
                    "username": result[0],
                    "user_id": result[1],
                    "can_create_db": result[2],
                    "is_superuser": result[3],
                    "can_replicate": result[4],
                    "bypass_rls": result[5],
                    "valid_until": result[6],
                    "config": result[7],
                    "connection_limit": result[8],
                    "can_login": result[9],
                    "can_create_role": result[10],
                    "inherits": result[12],
                    "is_replication_role": result[13]
                }

    async def get_user_permissions(self, username: str) -> Dict[str, List[str]]:
        """Get all permissions for a user.

        Args:
            username: The username to get permissions for

        Returns:
            Dictionary mapping object types to lists of permissions
        """
        permissions = {
            "database": [],
            "schema": [],
            "table": [],
            "sequence": [],
            "function": [],
            "type": [],
            "role_membership": []
        }

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                # Get database permissions
                await cur.execute("""
                    SELECT datname, datacl
                    FROM pg_database
                    WHERE datacl::text LIKE %s
                """, (f'%{username}=%',))

                for row in await cur.fetchall():
                    permissions["database"].append(f"{row[0]}: {row[1]}")

                # Get schema permissions
                await cur.execute("""
                    SELECT nspname, nspacl
                    FROM pg_namespace
                    WHERE nspacl::text LIKE %s
                """, (f'%{username}=%',))

                for row in await cur.fetchall():
                    permissions["schema"].append(f"{row[0]}: {row[1]}")

                # Get table permissions
                await cur.execute("""
                    SELECT schemaname, tablename,
                           privilege_type
                    FROM information_schema.table_privileges
                    WHERE grantee = %s
                """, (username,))

                for row in await cur.fetchall():
                    permissions["table"].append(f"{row[0]}.{row[1]}: {row[2]}")

                # Get role memberships
                await cur.execute("""
                    SELECT r.rolname
                    FROM pg_roles r
                    JOIN pg_auth_members m ON r.oid = m.roleid
                    JOIN pg_roles u ON m.member = u.oid
                    WHERE u.rolname = %s
                """, (username,))

                for row in await cur.fetchall():
                    permissions["role_membership"].append(row[0])

                return permissions

    async def get_owned_objects(self, username: str) -> Dict[str, List[str]]:
        """Get all database objects owned by a user.

        Args:
            username: The username to check ownership for

        Returns:
            Dictionary mapping object types to lists of object names
        """
        owned_objects = {
            "databases": [],
            "schemas": [],
            "tables": [],
            "sequences": [],
            "indexes": [],
            "functions": [],
            "types": []
        }

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                # Get owned databases
                await cur.execute("""
                    SELECT datname
                    FROM pg_database d
                    JOIN pg_roles r ON d.datdba = r.oid
                    WHERE r.rolname = %s
                """, (username,))

                owned_objects["databases"] = [row[0] for row in await cur.fetchall()]

                # Get owned schemas
                await cur.execute("""
                    SELECT nspname
                    FROM pg_namespace n
                    JOIN pg_roles r ON n.nspowner = r.oid
                    WHERE r.rolname = %s
                    AND nspname NOT LIKE 'pg_%'
                    AND nspname != 'information_schema'
                """, (username,))

                owned_objects["schemas"] = [row[0] for row in await cur.fetchall()]

                # Get owned tables
                await cur.execute("""
                    SELECT schemaname || '.' || tablename
                    FROM pg_tables
                    WHERE tableowner = %s
                """, (username,))

                owned_objects["tables"] = [row[0] for row in await cur.fetchall()]

                # Get owned sequences
                await cur.execute("""
                    SELECT schemaname || '.' || sequencename
                    FROM pg_sequences
                    WHERE sequenceowner = %s
                """, (username,))

                owned_objects["sequences"] = [row[0] for row in await cur.fetchall()]

                # Get owned indexes
                await cur.execute("""
                    SELECT schemaname || '.' || indexname
                    FROM pg_indexes
                    WHERE indexdef IS NOT NULL
                    AND schemaname || '.' || tablename IN (
                        SELECT schemaname || '.' || tablename
                        FROM pg_tables
                        WHERE tableowner = %s
                    )
                """, (username,))

                owned_objects["indexes"] = [row[0] for row in await cur.fetchall()]

                return owned_objects

    def validate_password_strength(self, password: str) -> tuple[bool, Optional[str]]:
        """Validate password strength.

        Args:
            password: The password to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if len(password) < 8:
            return False, "Password must be at least 8 characters long"

        has_upper = any(c.isupper() for c in password)
        has_lower = any(c.islower() for c in password)
        has_digit = any(c.isdigit() for c in password)
        has_special = any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password)

        if not (has_upper and has_lower and has_digit):
            return False, "Password must contain uppercase, lowercase, and numeric characters"

        # Warning for missing special characters (not required but recommended)
        if not has_special:
            # Log warning but don't fail
            pass

        return True, None