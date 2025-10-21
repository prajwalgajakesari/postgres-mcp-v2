"""Reindex operation implementation."""

from typing import Optional, List
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import IndexOperation


class ReindexOperation(IndexOperation):
    """Operation to rebuild database indexes."""

    def get_required_permissions(self) -> List[Permission]:
        return [Permission.REINDEX]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        target_type = kwargs.get("target_type", "").upper()
        if target_type not in ["INDEX", "TABLE", "SCHEMA", "DATABASE", "SYSTEM"]:
            return False, "target_type must be INDEX, TABLE, SCHEMA, DATABASE, or SYSTEM"

        target_name = kwargs.get("target_name")
        if target_type in ["INDEX", "TABLE", "SCHEMA"] and not target_name:
            return False, f"target_name is required for {target_type} reindex"

        return True, None

    async def execute(self, **kwargs) -> OperationResult:
        target_type = kwargs.get("target_type", "").upper()
        target_name = kwargs.get("target_name")
        schema_name = kwargs.get("schema_name", "public")
        concurrent = kwargs.get("concurrent", False)
        verbose = kwargs.get("verbose", False)

        try:
            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    sql_parts = ["REINDEX"]

                    if concurrent:
                        sql_parts.append("(CONCURRENTLY)")
                    elif verbose:
                        sql_parts.append("(VERBOSE)")

                    sql_parts.append(target_type)

                    if target_type in ["INDEX", "TABLE"]:
                        sql_parts.append(f"{self._quote_identifier(schema_name)}.{self._quote_identifier(target_name)}")
                    elif target_type == "SCHEMA":
                        sql_parts.append(self._quote_identifier(target_name))
                    elif target_type == "DATABASE":
                        sql_parts.append(self._quote_identifier(target_name or "current_database()"))

                    sql = " ".join(sql_parts)
                    await cur.execute(sql)

                    return OperationResult(
                        success=True, operation_type=self.get_operation_type(),
                        operation_name="Reindex",
                        affected_objects=[f"{target_type.lower()}:{target_name or 'system'}"],
                        message=f"{target_type} '{target_name or 'system'}' reindexed successfully",
                        details={"target_type": target_type, "target_name": target_name, "concurrent": concurrent}
                    )

        except Exception as e:
            return OperationResult(
                success=False, operation_type=self.get_operation_type(),
                operation_name="Reindex", error=f"Failed to reindex: {str(e)}",
                details={"target_type": target_type, "target_name": target_name}
            )