"""Schema management operation implementation."""

from typing import Optional, List
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import TableOperation


class ManageSchemaOperation(TableOperation):
    """Operation to manage database schemas."""

    def get_required_permissions(self) -> List[Permission]:
        """Get required permissions for schema management."""
        operation = self.current_operation
        if operation == "CREATE":
            return [Permission.CREATE_SCHEMA]
        elif operation == "DROP":
            return [Permission.DROP_SCHEMA]
        else:
            return [Permission.ALTER_SCHEMA]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_operation = None

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        """Validate schema management operation parameters."""
        operation = kwargs.get("operation", "").upper()
        schema_name = kwargs.get("schema_name")

        if operation not in ["CREATE", "ALTER", "DROP"]:
            return False, "Operation must be CREATE, ALTER, or DROP"

        self.current_operation = operation

        if not schema_name:
            return False, "Schema name is required"

        # Validate schema name
        is_valid, error = self.name_validator.validate_identifier(schema_name, allow_quoted=False)
        if not is_valid:
            return False, f"Invalid schema name: {error}"

        if operation == "CREATE":
            if not kwargs.get("if_not_exists", False):
                if await self.check_schema_exists(schema_name):
                    return False, f"Schema '{schema_name}' already exists"

        elif operation in ["ALTER", "DROP"]:
            if not kwargs.get("if_exists", False):
                if not await self.check_schema_exists(schema_name):
                    return False, f"Schema '{schema_name}' does not exist"

        # Validate rename_to for ALTER
        if operation == "ALTER":
            if rename_to := kwargs.get("rename_to"):
                is_valid, error = self.name_validator.validate_identifier(rename_to, allow_quoted=False)
                if not is_valid:
                    return False, f"Invalid new schema name: {error}"

                if await self.check_schema_exists(rename_to):
                    return False, f"Schema '{rename_to}' already exists"

        return True, None

    async def execute(self, **kwargs) -> OperationResult:
        """Execute the schema management operation."""
        operation = kwargs.get("operation", "").upper()
        schema_name = kwargs.get("schema_name")

        try:
            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    if operation == "CREATE":
                        return await self._create_schema(cur, **kwargs)
                    elif operation == "ALTER":
                        return await self._alter_schema(cur, **kwargs)
                    elif operation == "DROP":
                        return await self._drop_schema(cur, **kwargs)

        except psycopg.errors.DuplicateSchema as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="ManageSchema",
                error=f"Schema already exists: {str(e)}",
                details={"schema": schema_name, "operation": operation}
            )

        except psycopg.errors.InvalidSchemaName as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="ManageSchema",
                error=f"Invalid schema name: {str(e)}",
                details={"schema": schema_name, "operation": operation}
            )

        except psycopg.errors.DependentObjectsStillExist as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="ManageSchema",
                error=f"Schema has dependent objects. Use CASCADE option: {str(e)}",
                details={"schema": schema_name, "operation": operation}
            )

        except Exception as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="ManageSchema",
                error=f"Failed to {operation.lower()} schema: {str(e)}",
                details={"schema": schema_name, "operation": operation}
            )

    async def _create_schema(self, cur, **kwargs) -> OperationResult:
        """Create a new schema."""
        schema_name = kwargs.get("schema_name")
        if_not_exists = kwargs.get("if_not_exists", False)
        authorization = kwargs.get("authorization")

        # Check existence for if_not_exists
        if if_not_exists and await self.check_schema_exists(schema_name):
            return OperationResult(
                success=True,
                operation_type=self.get_operation_type(),
                operation_name="CreateSchema",
                affected_objects=[],
                message=f"Schema '{schema_name}' already exists, skipping",
                details={"schema": schema_name, "skipped": True}
            )

        # Build CREATE SCHEMA statement
        sql_parts = ["CREATE SCHEMA"]

        if if_not_exists:
            sql_parts.append("IF NOT EXISTS")

        sql_parts.append(self._quote_identifier(schema_name))

        if authorization:
            sql_parts.append(f"AUTHORIZATION {self._quote_identifier(authorization)}")

        sql = " ".join(sql_parts)
        await cur.execute(sql)

        # Set comment if provided
        if comment := kwargs.get("comment"):
            comment_sql = f"COMMENT ON SCHEMA {self._quote_identifier(schema_name)} IS %s"
            await cur.execute(comment_sql, (comment,))

        return OperationResult(
            success=True,
            operation_type=self.get_operation_type(),
            operation_name="CreateSchema",
            affected_objects=[f"schema:{schema_name}"],
            message=f"Schema '{schema_name}' created successfully",
            details={"schema": schema_name, "authorization": authorization}
        )

    async def _alter_schema(self, cur, **kwargs) -> OperationResult:
        """Alter an existing schema."""
        schema_name = kwargs.get("schema_name")
        alterations_made = []

        # Rename schema
        if rename_to := kwargs.get("rename_to"):
            sql = f"ALTER SCHEMA {self._quote_identifier(schema_name)} RENAME TO {self._quote_identifier(rename_to)}"
            await cur.execute(sql)
            alterations_made.append(f"Renamed to '{rename_to}'")
            schema_name = rename_to

        # Change owner
        if owner_to := kwargs.get("owner_to"):
            sql = f"ALTER SCHEMA {self._quote_identifier(schema_name)} OWNER TO {self._quote_identifier(owner_to)}"
            await cur.execute(sql)
            alterations_made.append(f"Changed owner to '{owner_to}'")

        return OperationResult(
            success=True,
            operation_type=self.get_operation_type(),
            operation_name="AlterSchema",
            affected_objects=[f"schema:{schema_name}"],
            message=f"Schema '{schema_name}' altered successfully",
            details={"schema": schema_name, "alterations": alterations_made}
        )

    async def _drop_schema(self, cur, **kwargs) -> OperationResult:
        """Drop an existing schema."""
        schema_name = kwargs.get("schema_name")
        cascade = kwargs.get("cascade", False)
        if_exists = kwargs.get("if_exists", False)

        # Check existence for if_exists
        if if_exists and not await self.check_schema_exists(schema_name):
            return OperationResult(
                success=True,
                operation_type=self.get_operation_type(),
                operation_name="DropSchema",
                affected_objects=[],
                message=f"Schema '{schema_name}' does not exist, skipping",
                details={"schema": schema_name, "skipped": True}
            )

        # Build DROP SCHEMA statement
        sql_parts = ["DROP SCHEMA"]

        if if_exists:
            sql_parts.append("IF EXISTS")

        sql_parts.append(self._quote_identifier(schema_name))

        if cascade:
            sql_parts.append("CASCADE")
        else:
            sql_parts.append("RESTRICT")

        sql = " ".join(sql_parts)
        await cur.execute(sql)

        return OperationResult(
            success=True,
            operation_type=self.get_operation_type(),
            operation_name="DropSchema",
            affected_objects=[f"schema:{schema_name}"],
            message=f"Schema '{schema_name}' dropped successfully",
            details={"schema": schema_name, "cascade": cascade}
        )