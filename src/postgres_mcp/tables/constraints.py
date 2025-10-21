"""Constraint management operation implementation."""

from typing import Optional, List, Dict, Any
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import TableOperation


class ManageConstraintsOperation(TableOperation):
    """Operation to manage table constraints."""

    def get_required_permissions(self) -> List[Permission]:
        """Get required permissions for constraint management."""
        return [Permission.ALTER_TABLE]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        """Validate constraint management operation parameters."""
        operation = kwargs.get("operation", "").upper()
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")

        if operation not in ["ADD", "DROP", "VALIDATE", "DISABLE", "ENABLE"]:
            return False, "Operation must be ADD, DROP, VALIDATE, DISABLE, or ENABLE"

        if not table_name:
            return False, "Table name is required"

        # Check if table exists
        if not await self.check_table_exists(table_name, schema_name):
            return False, f"Table '{schema_name}.{table_name}' does not exist"

        if operation == "ADD":
            constraint = kwargs.get("constraint", {})
            if not constraint:
                return False, "Constraint definition is required for ADD operation"

            constraint_type = constraint.get("type", "").upper()
            if constraint_type not in ["PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK", "EXCLUDE"]:
                return False, f"Invalid constraint type: {constraint_type}"

        elif operation in ["DROP", "VALIDATE", "DISABLE", "ENABLE"]:
            constraint_name = kwargs.get("constraint_name")
            if not constraint_name:
                return False, "Constraint name is required for this operation"

        return True, None

    async def execute(self, **kwargs) -> OperationResult:
        """Execute the constraint management operation."""
        operation = kwargs.get("operation", "").upper()
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")

        try:
            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    full_table_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"

                    if operation == "ADD":
                        return await self._add_constraint(cur, full_table_name, **kwargs)
                    elif operation == "DROP":
                        return await self._drop_constraint(cur, full_table_name, **kwargs)
                    elif operation == "VALIDATE":
                        return await self._validate_constraint(cur, full_table_name, **kwargs)
                    elif operation == "DISABLE":
                        return await self._disable_constraint(cur, full_table_name, **kwargs)
                    elif operation == "ENABLE":
                        return await self._enable_constraint(cur, full_table_name, **kwargs)

        except psycopg.errors.DuplicateObject as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="ManageConstraints",
                error=f"Constraint already exists: {str(e)}",
                details={"table": f"{schema_name}.{table_name}", "operation": operation}
            )

        except psycopg.errors.UndefinedObject as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="ManageConstraints",
                error=f"Constraint or referenced object does not exist: {str(e)}",
                details={"table": f"{schema_name}.{table_name}", "operation": operation}
            )

        except Exception as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="ManageConstraints",
                error=f"Failed to {operation.lower()} constraint: {str(e)}",
                details={"table": f"{schema_name}.{table_name}", "operation": operation}
            )

    async def _add_constraint(self, cur, full_table_name: str, **kwargs) -> OperationResult:
        """Add a constraint to the table."""
        constraint = kwargs.get("constraint", {})
        constraint_def = self.format_constraint_definition(constraint)

        sql = f"ALTER TABLE {full_table_name} ADD {constraint_def}"
        await cur.execute(sql)

        constraint_name = constraint.get("name", "unnamed")
        return OperationResult(
            success=True,
            operation_type=self.get_operation_type(),
            operation_name="AddConstraint",
            affected_objects=[f"constraint:{constraint_name}"],
            message=f"Constraint '{constraint_name}' added successfully",
            details={"constraint": constraint_name, "type": constraint.get("type")}
        )

    async def _drop_constraint(self, cur, full_table_name: str, **kwargs) -> OperationResult:
        """Drop a constraint from the table."""
        constraint_name = kwargs.get("constraint_name")
        cascade = kwargs.get("cascade", False)
        if_exists = kwargs.get("if_exists", False)

        sql_parts = [f"ALTER TABLE {full_table_name} DROP CONSTRAINT"]

        if if_exists:
            sql_parts.append("IF EXISTS")

        sql_parts.append(self._quote_identifier(constraint_name))

        if cascade:
            sql_parts.append("CASCADE")

        sql = " ".join(sql_parts)
        await cur.execute(sql)

        return OperationResult(
            success=True,
            operation_type=self.get_operation_type(),
            operation_name="DropConstraint",
            affected_objects=[f"constraint:{constraint_name}"],
            message=f"Constraint '{constraint_name}' dropped successfully",
            details={"constraint": constraint_name, "cascade": cascade}
        )

    async def _validate_constraint(self, cur, full_table_name: str, **kwargs) -> OperationResult:
        """Validate a constraint."""
        constraint_name = kwargs.get("constraint_name")

        sql = f"ALTER TABLE {full_table_name} VALIDATE CONSTRAINT {self._quote_identifier(constraint_name)}"
        await cur.execute(sql)

        return OperationResult(
            success=True,
            operation_type=self.get_operation_type(),
            operation_name="ValidateConstraint",
            affected_objects=[f"constraint:{constraint_name}"],
            message=f"Constraint '{constraint_name}' validated successfully",
            details={"constraint": constraint_name}
        )

    async def _disable_constraint(self, cur, full_table_name: str, **kwargs) -> OperationResult:
        """Disable a constraint (PostgreSQL-specific for foreign keys and check constraints)."""
        constraint_name = kwargs.get("constraint_name")

        # Note: PostgreSQL doesn't have direct DISABLE CONSTRAINT syntax like some other databases
        # This would need to be implemented by dropping and recreating, or using triggers
        # For now, we'll return an informational message

        return OperationResult(
            success=False,
            operation_type=self.get_operation_type(),
            operation_name="DisableConstraint",
            error="PostgreSQL does not support disabling constraints directly. Consider dropping the constraint instead.",
            details={"constraint": constraint_name}
        )

    async def _enable_constraint(self, cur, full_table_name: str, **kwargs) -> OperationResult:
        """Enable a constraint (PostgreSQL-specific)."""
        constraint_name = kwargs.get("constraint_name")

        return OperationResult(
            success=False,
            operation_type=self.get_operation_type(),
            operation_name="EnableConstraint",
            error="PostgreSQL does not support enabling/disabling constraints directly. Constraints are always active when present.",
            details={"constraint": constraint_name}
        )