"""Drop table operation implementation."""

from typing import Optional, List
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import TableOperation


class DropTableOperation(TableOperation):
    """Operation to drop a database table."""

    def get_required_permissions(self) -> List[Permission]:
        """Get required permissions for dropping tables."""
        return [Permission.DROP_TABLE]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        """Validate drop table operation parameters."""
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")

        if not table_name:
            return False, "Table name is required"

        # Validate table name
        is_valid, error = self.name_validator.validate_identifier(table_name, allow_quoted=False)
        if not is_valid:
            return False, f"Invalid table name: {error}"

        # Check if table exists (unless if_exists is True)
        if not kwargs.get("if_exists", False):
            if not await self.check_table_exists(table_name, schema_name):
                return False, f"Table '{schema_name}.{table_name}' does not exist"

        return True, None

    async def execute(self, **kwargs) -> OperationResult:
        """Execute the drop table operation."""
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")
        cascade = kwargs.get("cascade", False)
        if_exists = kwargs.get("if_exists", False)

        try:
            # Check if table exists for if_exists handling
            table_exists = await self.check_table_exists(table_name, schema_name)

            if not table_exists and if_exists:
                return OperationResult(
                    success=True,
                    operation_type=self.get_operation_type(),
                    operation_name="DropTable",
                    affected_objects=[],
                    message=f"Table '{schema_name}.{table_name}' does not exist, skipping",
                    details={"table": f"{schema_name}.{table_name}", "skipped": True}
                )

            # Get table info before dropping
            table_info = await self.get_table_info(table_name, schema_name) if table_exists else None

            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Build DROP TABLE statement
                    sql_parts = ["DROP TABLE"]

                    if if_exists:
                        sql_parts.append("IF EXISTS")

                    full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"
                    sql_parts.append(full_name)

                    if cascade:
                        sql_parts.append("CASCADE")
                    else:
                        sql_parts.append("RESTRICT")

                    sql = " ".join(sql_parts)
                    await cur.execute(sql)

                    return OperationResult(
                        success=True,
                        operation_type=self.get_operation_type(),
                        operation_name="DropTable",
                        affected_objects=[f"table:{schema_name}.{table_name}"],
                        message=f"Table '{schema_name}.{table_name}' dropped successfully",
                        details={
                            "table": f"{schema_name}.{table_name}",
                            "cascade": cascade,
                            "dropped_info": table_info
                        }
                    )

        except psycopg.errors.DependentObjectsStillExist as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="DropTable",
                error=f"Table has dependent objects. Use CASCADE option: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )

        except psycopg.errors.InsufficientPrivilege as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="DropTable",
                error=f"Insufficient privileges: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )

        except Exception as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="DropTable",
                error=f"Failed to drop table: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )