"""Permission management operations."""

from typing import Optional, Dict, Any, List, Union
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import UserOperation


class ManagePermissionsOperation(UserOperation):
    """Operation to manage database permissions (GRANT/REVOKE)."""

    # Valid privileges by object type
    PERMISSION_MATRIX = {
        "TABLE": ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "ALL"],
        "SEQUENCE": ["USAGE", "SELECT", "UPDATE", "ALL"],
        "DATABASE": ["CREATE", "CONNECT", "TEMPORARY", "TEMP", "ALL"],
        "SCHEMA": ["CREATE", "USAGE", "ALL"],
        "FUNCTION": ["EXECUTE", "ALL"],
        "PROCEDURE": ["EXECUTE", "ALL"],
        "LANGUAGE": ["USAGE", "ALL"],
        "TYPE": ["USAGE", "ALL"],
        "COLUMN": ["SELECT", "INSERT", "UPDATE", "REFERENCES", "ALL"]
    }

    def get_required_permissions(self) -> List[Permission]:
        """Get required permissions for managing permissions."""
        return [Permission.MANAGE_PERMISSIONS]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        """Validate permission management operation parameters.

        Args:
            operation: GRANT or REVOKE
            privileges: List of privileges to grant/revoke
            object_type: Type of object (TABLE, SCHEMA, DATABASE, etc.)
            object_name: Name of the object
            schema_name: Schema name for schema-qualified objects
            column_names: List of column names (for column-level permissions)
            username: User to grant/revoke privileges to/from
            role_name: Role to grant/revoke privileges to/from
            with_grant_option: Allow grantee to grant to others
            cascade: CASCADE for REVOKE
            all_tables: Apply to all tables in schema
            all_sequences: Apply to all sequences in schema
            all_functions: Apply to all functions in schema

        Returns:
            Tuple of (is_valid, error_message)
        """
        operation = kwargs.get("operation", "").upper()
        if operation not in ["GRANT", "REVOKE"]:
            return False, "Operation must be GRANT or REVOKE"

        # Check that we have either username or role_name
        username = kwargs.get("username")
        role_name = kwargs.get("role_name")

        if not username and not role_name:
            return False, "Either username or role_name is required"

        grantee = username or role_name

        # Validate grantee exists
        if username and not await self.check_user_exists(username):
            return False, f"User '{username}' does not exist"

        if role_name and not await self.check_role_exists(role_name):
            return False, f"Role '{role_name}' does not exist"

        # Validate object type
        object_type = kwargs.get("object_type", "").upper()

        # Handle special "ALL" cases
        all_tables = kwargs.get("all_tables", False)
        all_sequences = kwargs.get("all_sequences", False)
        all_functions = kwargs.get("all_functions", False)

        if all_tables or all_sequences or all_functions:
            if not kwargs.get("schema_name"):
                return False, "Schema name is required when using all_tables, all_sequences, or all_functions"
        else:
            if object_type not in self.PERMISSION_MATRIX:
                return False, f"Invalid object type: {object_type}. Valid types: {', '.join(self.PERMISSION_MATRIX.keys())}"

            # Validate object name
            if not kwargs.get("object_name"):
                return False, "Object name is required"

        # Validate privileges
        privileges = kwargs.get("privileges", [])
        if not privileges:
            return False, "At least one privilege must be specified"

        if isinstance(privileges, str):
            privileges = [privileges]

        # Normalize privileges to uppercase
        privileges = [p.upper() for p in privileges]

        # Validate privileges for object type
        if not (all_tables or all_sequences or all_functions):
            valid_privs = self.PERMISSION_MATRIX.get(object_type, [])
            for priv in privileges:
                if priv not in valid_privs:
                    return False, f"Invalid privilege '{priv}' for {object_type}. Valid privileges: {', '.join(valid_privs)}"

        # Store normalized privileges
        kwargs["privileges"] = privileges

        # Validate column names for column-level permissions
        if object_type == "COLUMN":
            if not kwargs.get("column_names"):
                return False, "Column names are required for COLUMN permissions"

        # Validate CASCADE option (only for REVOKE)
        if kwargs.get("cascade") and operation != "REVOKE":
            return False, "CASCADE option is only valid for REVOKE"

        # Validate WITH GRANT OPTION (only for GRANT)
        if kwargs.get("with_grant_option") and operation != "GRANT":
            return False, "WITH GRANT OPTION is only valid for GRANT"

        return True, None

    def build_permission_sql(self, **kwargs) -> tuple[str, List[Any]]:
        """Build GRANT/REVOKE SQL statement.

        Args:
            Same as validate method

        Returns:
            Tuple of (sql_statement, parameters)
        """
        operation = kwargs.get("operation", "").upper()
        privileges = kwargs.get("privileges", [])
        object_type = kwargs.get("object_type", "").upper()
        object_name = kwargs.get("object_name")
        schema_name = kwargs.get("schema_name")
        column_names = kwargs.get("column_names", [])
        username = kwargs.get("username")
        role_name = kwargs.get("role_name")
        grantee = username or role_name
        with_grant_option = kwargs.get("with_grant_option", False)
        cascade = kwargs.get("cascade", False)
        all_tables = kwargs.get("all_tables", False)
        all_sequences = kwargs.get("all_sequences", False)
        all_functions = kwargs.get("all_functions", False)

        # Build privilege list
        if "ALL" in privileges:
            priv_list = "ALL PRIVILEGES"
        else:
            priv_list = ", ".join(privileges)

        # Build object specification
        if all_tables:
            object_spec = f"ALL TABLES IN SCHEMA {self._quote_identifier(schema_name)}"
        elif all_sequences:
            object_spec = f"ALL SEQUENCES IN SCHEMA {self._quote_identifier(schema_name)}"
        elif all_functions:
            object_spec = f"ALL FUNCTIONS IN SCHEMA {self._quote_identifier(schema_name)}"
        elif object_type == "COLUMN":
            # Column-level permissions
            cols = ", ".join(self._quote_identifier(c) for c in column_names)
            if schema_name:
                table_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(object_name)}"
            else:
                table_name = self._quote_identifier(object_name)
            object_spec = f"{cols} ON TABLE {table_name}"
        else:
            # Regular object permissions
            if schema_name and object_type in ["TABLE", "SEQUENCE", "FUNCTION", "PROCEDURE"]:
                full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(object_name)}"
            else:
                full_name = self._quote_identifier(object_name)

            # Add object type keyword
            if object_type == "DATABASE":
                object_spec = f"DATABASE {full_name}"
            elif object_type == "SCHEMA":
                object_spec = f"SCHEMA {full_name}"
            elif object_type == "TABLE":
                object_spec = f"TABLE {full_name}"
            elif object_type == "SEQUENCE":
                object_spec = f"SEQUENCE {full_name}"
            elif object_type == "FUNCTION":
                object_spec = f"FUNCTION {full_name}"
            elif object_type == "PROCEDURE":
                object_spec = f"PROCEDURE {full_name}"
            elif object_type == "LANGUAGE":
                object_spec = f"LANGUAGE {full_name}"
            elif object_type == "TYPE":
                object_spec = f"TYPE {full_name}"
            else:
                object_spec = full_name

        # Build TO/FROM clause
        target = self._quote_identifier(grantee)

        # Build complete statement
        if operation == "GRANT":
            sql = f"GRANT {priv_list} ON {object_spec} TO {target}"
            if with_grant_option:
                sql += " WITH GRANT OPTION"
        else:  # REVOKE
            sql = f"REVOKE {priv_list} ON {object_spec} FROM {target}"
            if cascade:
                sql += " CASCADE"

        return sql, []

    async def execute(self, **kwargs) -> OperationResult:
        """Execute the permission management operation.

        Args:
            Same as validate method

        Returns:
            OperationResult with success status and details
        """
        operation = kwargs.get("operation", "").upper()
        username = kwargs.get("username")
        role_name = kwargs.get("role_name")
        grantee = username or role_name
        object_name = kwargs.get("object_name", "")
        object_type = kwargs.get("object_type", "").upper()

        # Determine affected object description
        if kwargs.get("all_tables"):
            affected_desc = f"all tables in schema {kwargs.get('schema_name')}"
        elif kwargs.get("all_sequences"):
            affected_desc = f"all sequences in schema {kwargs.get('schema_name')}"
        elif kwargs.get("all_functions"):
            affected_desc = f"all functions in schema {kwargs.get('schema_name')}"
        else:
            schema_prefix = f"{kwargs.get('schema_name')}." if kwargs.get("schema_name") else ""
            affected_desc = f"{object_type.lower()} {schema_prefix}{object_name}"

        try:
            # Build SQL statement
            sql, params = self.build_permission_sql(**kwargs)

            # Execute the statement
            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)

                    # Build success message
                    action = "granted to" if operation == "GRANT" else "revoked from"
                    privs = kwargs.get("privileges", [])
                    priv_desc = "ALL PRIVILEGES" if "ALL" in privs else ", ".join(privs)

                    return OperationResult(
                        success=True,
                        operation_type=self.get_operation_type(),
                        operation_name="ManagePermissions",
                        affected_objects=[f"{grantee}:{affected_desc}"],
                        message=f"{priv_desc} on {affected_desc} {action} '{grantee}'",
                        details={
                            "operation": operation,
                            "grantee": grantee,
                            "privileges": privs,
                            "object_type": object_type,
                            "object": affected_desc,
                            "with_grant_option": kwargs.get("with_grant_option", False),
                            "cascade": kwargs.get("cascade", False)
                        }
                    )

        except psycopg.errors.UndefinedObject as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="ManagePermissions",
                error=f"Object does not exist: {str(e)}",
                details={
                    "operation": operation,
                    "grantee": grantee,
                    "object": affected_desc
                }
            )

        except psycopg.errors.InsufficientPrivilege as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="ManagePermissions",
                error=f"Insufficient privileges to {operation.lower()} permissions: {str(e)}",
                details={
                    "operation": operation,
                    "grantee": grantee,
                    "object": affected_desc
                }
            )

        except psycopg.errors.InvalidGrantOperation as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="ManagePermissions",
                error=f"Invalid grant operation: {str(e)}",
                details={
                    "operation": operation,
                    "grantee": grantee,
                    "object": affected_desc
                }
            )

        except Exception as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="ManagePermissions",
                error=f"Failed to {operation.lower()} permissions: {str(e)}",
                details={
                    "operation": operation,
                    "grantee": grantee,
                    "object": affected_desc
                }
            )