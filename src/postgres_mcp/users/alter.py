"""Alter user operation implementation."""

from typing import Optional, Dict, Any, List
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import UserOperation


class AlterUserOperation(UserOperation):
    """Operation to alter an existing database user or role."""

    def get_required_permissions(self) -> List[Permission]:
        """Get required permissions for altering users."""
        return [Permission.ALTER_USER]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        """Validate alter user operation parameters.

        Args:
            username: The username to alter
            rename_to: New name for the user
            password: New password
            connection_limit: New connection limit
            valid_until: New expiration timestamp
            options: Boolean options to set

        Returns:
            Tuple of (is_valid, error_message)
        """
        username = kwargs.get("username")

        if not username:
            return False, "Username is required"

        # Validate identifier format
        is_valid, error = self.name_validator.validate_identifier(
            username,
            allow_quoted=False
        )
        if not is_valid:
            return False, f"Invalid username: {error}"

        # Check if user exists
        if not await self.check_user_exists(username):
            return False, f"User '{username}' does not exist"

        # Validate rename if provided
        if rename_to := kwargs.get("rename_to"):
            is_valid, error = self.name_validator.validate_identifier(
                rename_to,
                allow_quoted=False
            )
            if not is_valid:
                return False, f"Invalid new username: {error}"

            # Check if target name already exists
            if await self.check_user_exists(rename_to):
                return False, f"User '{rename_to}' already exists"

        # Validate password if provided
        if password := kwargs.get("password"):
            if not kwargs.get("skip_password_validation", False):
                is_strong, error = self.validate_password_strength(password)
                if not is_strong:
                    return False, error

        # Validate connection limit
        if connection_limit := kwargs.get("connection_limit"):
            if not isinstance(connection_limit, int) or connection_limit < -1:
                return False, "connection_limit must be -1 (unlimited) or a positive integer"

        # Check if at least one change is specified
        has_change = any([
            kwargs.get("rename_to"),
            kwargs.get("password"),
            kwargs.get("connection_limit") is not None,
            kwargs.get("valid_until"),
            kwargs.get("createdb") is not None,
            kwargs.get("createrole") is not None,
            kwargs.get("login") is not None,
            kwargs.get("replication") is not None,
            kwargs.get("bypassrls") is not None,
            kwargs.get("inherit") is not None,
            kwargs.get("superuser") is not None,
            kwargs.get("reset_password", False),
            kwargs.get("reset_config", False)
        ])

        if not has_change:
            return False, "No changes specified"

        return True, None

    async def execute(self, **kwargs) -> OperationResult:
        """Execute the alter user operation.

        Args:
            username: The username to alter
            rename_to: New name for the user
            password: New password
            connection_limit: New connection limit
            valid_until: New expiration timestamp
            createdb: Can create databases
            createrole: Can create roles
            login: Can login
            replication: Can initiate replication
            bypassrls: Bypass row-level security
            inherit: Inherit privileges from roles
            superuser: Superuser privilege
            reset_password: Remove password
            reset_config: Reset configuration to default

        Returns:
            OperationResult with success status and details
        """
        username = kwargs.get("username")
        changes_made = []

        try:
            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Handle rename separately (must be done first)
                    if rename_to := kwargs.get("rename_to"):
                        rename_sql = f"""
                            ALTER USER {self._quote_identifier(username)}
                            RENAME TO {self._quote_identifier(rename_to)}
                        """
                        await cur.execute(rename_sql)
                        changes_made.append(f"Renamed to '{rename_to}'")
                        username = rename_to  # Update username for subsequent operations

                    # Build ALTER USER statement for other changes
                    sql_parts = [f"ALTER USER {self._quote_identifier(username)}"]
                    params = []
                    alter_changes = []

                    # Password change
                    if password := kwargs.get("password"):
                        alter_changes.append("WITH PASSWORD %s")
                        params.append(password)
                        changes_made.append("Password updated")
                    elif kwargs.get("reset_password"):
                        alter_changes.append("WITH PASSWORD NULL")
                        changes_made.append("Password removed")

                    # Boolean options
                    bool_options = {
                        "CREATEDB": kwargs.get("createdb"),
                        "CREATEROLE": kwargs.get("createrole"),
                        "LOGIN": kwargs.get("login"),
                        "REPLICATION": kwargs.get("replication"),
                        "BYPASSRLS": kwargs.get("bypassrls"),
                        "INHERIT": kwargs.get("inherit"),
                        "SUPERUSER": kwargs.get("superuser")
                    }

                    for option, value in bool_options.items():
                        if value is True:
                            alter_changes.append(option)
                            changes_made.append(f"{option} enabled")
                        elif value is False:
                            alter_changes.append(f"NO{option}")
                            changes_made.append(f"{option} disabled")

                    # Connection limit
                    if (connection_limit := kwargs.get("connection_limit")) is not None:
                        alter_changes.append(f"CONNECTION LIMIT {connection_limit}")
                        changes_made.append(f"Connection limit set to {connection_limit}")

                    # Valid until
                    if valid_until := kwargs.get("valid_until"):
                        alter_changes.append("VALID UNTIL %s")
                        params.append(valid_until)
                        changes_made.append(f"Expiration set to {valid_until}")

                    # Reset configuration
                    if kwargs.get("reset_config"):
                        reset_sql = f"ALTER USER {self._quote_identifier(username)} RESET ALL"
                        await cur.execute(reset_sql)
                        changes_made.append("Configuration reset to defaults")

                    # Execute ALTER USER if there are changes
                    if alter_changes:
                        sql = sql_parts[0] + " " + " ".join(alter_changes)
                        await cur.execute(sql, params)

                    return OperationResult(
                        success=True,
                        operation_type=self.get_operation_type(),
                        operation_name="AlterUser",
                        affected_objects=[f"user:{username}"],
                        message=f"User '{username}' altered successfully",
                        details={
                            "username": username,
                            "changes": changes_made
                        }
                    )

        except psycopg.errors.UndefinedObject as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="AlterUser",
                error=f"User does not exist: {str(e)}",
                details={"username": username}
            )

        except psycopg.errors.DuplicateObject as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="AlterUser",
                error=f"Target name already exists: {str(e)}",
                details={"username": username, "rename_to": kwargs.get("rename_to")}
            )

        except psycopg.errors.InsufficientPrivilege as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="AlterUser",
                error=f"Insufficient privileges: {str(e)}",
                details={"username": username}
            )

        except Exception as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="AlterUser",
                error=f"Failed to alter user: {str(e)}",
                details={"username": username}
            )