"""Create user operation implementation."""

from typing import Optional, Dict, Any, List
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import UserOperation


class CreateUserOperation(UserOperation):
    """Operation to create a new database user or role."""

    def get_required_permissions(self) -> List[Permission]:
        """Get required permissions for creating users."""
        return [Permission.CREATE_USER]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        """Validate create user operation parameters.

        Args:
            username: The username to create
            password: Optional password for the user
            role_name: Optional role name (for CREATE ROLE)
            options: Additional user options

        Returns:
            Tuple of (is_valid, error_message)
        """
        username = kwargs.get("username")
        role_name = kwargs.get("role_name")

        # Check if we have either username or role_name
        if not username and not role_name:
            return False, "Either username or role_name is required"

        identifier = username or role_name

        # Validate identifier format
        is_valid, error = self.name_validator.validate_identifier(
            identifier,
            allow_quoted=False
        )
        if not is_valid:
            return False, f"Invalid identifier: {error}"

        # Check if user/role already exists
        if username and await self.check_user_exists(username):
            return False, f"User '{username}' already exists"

        if role_name and await self.check_role_exists(role_name):
            return False, f"Role '{role_name}' already exists"

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

        return True, None

    def build_create_user_sql(self, **kwargs) -> tuple[str, List[Any]]:
        """Build CREATE USER/ROLE SQL statement.

        Args:
            username: Username for CREATE USER
            role_name: Role name for CREATE ROLE
            password: Optional password
            connection_limit: Connection limit (-1 for unlimited)
            valid_until: Expiration timestamp
            createdb: Can create databases
            createrole: Can create roles
            login: Can login (default True for users, False for roles)
            replication: Can initiate replication
            bypassrls: Bypass row-level security
            inherit: Inherit privileges from roles
            superuser: Superuser privilege
            in_role: Roles to be a member of
            role: Roles that are members of this role
            admin: Roles with admin option

        Returns:
            Tuple of (sql_statement, parameters)
        """
        username = kwargs.get("username")
        role_name = kwargs.get("role_name")
        identifier = username or role_name

        # Use CREATE USER for users, CREATE ROLE for roles
        if username:
            sql_parts = [f"CREATE USER {self._quote_identifier(identifier)}"]
        else:
            sql_parts = [f"CREATE ROLE {self._quote_identifier(identifier)}"]

        params = []

        # Add password if provided
        if password := kwargs.get("password"):
            sql_parts.append("WITH PASSWORD %s")
            params.append(password)

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

        # For CREATE USER, default LOGIN to True unless explicitly False
        if username and "login" not in kwargs:
            bool_options["LOGIN"] = True

        for option, value in bool_options.items():
            if value is True:
                sql_parts.append(option)
            elif value is False:
                sql_parts.append(f"NO{option}")

        # Connection limit
        if (connection_limit := kwargs.get("connection_limit")) is not None:
            sql_parts.append(f"CONNECTION LIMIT {connection_limit}")

        # Valid until
        if valid_until := kwargs.get("valid_until"):
            sql_parts.append("VALID UNTIL %s")
            params.append(valid_until)

        # Role memberships
        if in_role := kwargs.get("in_role"):
            if isinstance(in_role, str):
                in_role = [in_role]
            sql_parts.append(f"IN ROLE {','.join(self._quote_identifier(r) for r in in_role)}")

        if role := kwargs.get("role"):
            if isinstance(role, str):
                role = [role]
            sql_parts.append(f"ROLE {','.join(self._quote_identifier(r) for r in role)}")

        if admin := kwargs.get("admin"):
            if isinstance(admin, str):
                admin = [admin]
            sql_parts.append(f"ADMIN {','.join(self._quote_identifier(r) for r in admin)}")

        return " ".join(sql_parts), params

    async def execute(self, **kwargs) -> OperationResult:
        """Execute the create user operation.

        Args:
            Same as build_create_user_sql

        Returns:
            OperationResult with success status and details
        """
        username = kwargs.get("username")
        role_name = kwargs.get("role_name")
        identifier = username or role_name
        object_type = "user" if username else "role"

        try:
            # Build SQL statement
            sql, params = self.build_create_user_sql(**kwargs)

            # Execute the statement
            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)

                    # Get the number of affected rows
                    affected = cur.rowcount

                    # Optionally grant initial permissions
                    if initial_grants := kwargs.get("initial_grants"):
                        for grant in initial_grants:
                            grant_sql = f"""
                                GRANT {grant['privilege']}
                                ON {grant['object_type']} {grant['object_name']}
                                TO {self._quote_identifier(identifier)}
                            """
                            await cur.execute(grant_sql)

                    return OperationResult(
                        success=True,
                        operation_type=self.get_operation_type(),
                        operation_name="CreateUser",
                        affected_objects=[f"{object_type}:{identifier}"],
                        message=f"{object_type.capitalize()} '{identifier}' created successfully",
                        affected_rows=affected,
                        details={
                            "identifier": identifier,
                            "type": object_type,
                            "options": {
                                k: v for k, v in kwargs.items()
                                if k not in ["password", "username", "role_name"]
                            }
                        }
                    )

        except psycopg.errors.DuplicateObject as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="CreateUser",
                error=f"{object_type.capitalize()} already exists: {str(e)}",
                details={"identifier": identifier, "type": object_type}
            )

        except psycopg.errors.InvalidPassword as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="CreateUser",
                error=f"Invalid password: {str(e)}",
                details={"identifier": identifier, "type": object_type}
            )

        except psycopg.errors.InsufficientPrivilege as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="CreateUser",
                error=f"Insufficient privileges: {str(e)}",
                details={"identifier": identifier, "type": object_type}
            )

        except Exception as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="CreateUser",
                error=f"Failed to create {object_type}: {str(e)}",
                details={"identifier": identifier, "type": object_type}
            )