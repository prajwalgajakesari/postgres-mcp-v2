"""Drop index operation implementation."""

from typing import Optional, List
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import IndexOperation


class DropIndexOperation(IndexOperation):
    """Operation to drop database indexes."""

    def get_required_permissions(self) -> List[Permission]:
        return [Permission.DROP_INDEX]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        index_name = kwargs.get("index_name")
        if not index_name:
            return False, "Index name is required"

        if not kwargs.get("if_exists", False):
            if not await self.check_index_exists(index_name, kwargs.get("schema_name", "public")):
                return False, f"Index '{index_name}' does not exist"
        return True, None

    async def execute(self, **kwargs) -> OperationResult:
        index_name = kwargs.get("index_name")
        schema_name = kwargs.get("schema_name", "public")
        concurrent = kwargs.get("concurrent", False)
        if_exists = kwargs.get("if_exists", False)
        cascade = kwargs.get("cascade", False)

        try:
            if if_exists and not await self.check_index_exists(index_name, schema_name):
                return OperationResult(
                    success=True, operation_type=self.get_operation_type(),
                    operation_name="DropIndex", affected_objects=[],
                    message=f"Index '{index_name}' does not exist, skipping",
                    details={"index": index_name, "skipped": True}
                )

            index_info = await self.get_index_info(index_name, schema_name)

            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    sql_parts = ["DROP INDEX"]
                    if concurrent:
                        sql_parts.append("CONCURRENTLY")
                    if if_exists:
                        sql_parts.append("IF EXISTS")

                    sql_parts.append(f"{self._quote_identifier(schema_name)}.{self._quote_identifier(index_name)}")

                    if cascade:
                        sql_parts.append("CASCADE")

                    sql = " ".join(sql_parts)
                    await cur.execute(sql)

                    return OperationResult(
                        success=True, operation_type=self.get_operation_type(),
                        operation_name="DropIndex",
                        affected_objects=[f"index:{index_name}"],
                        message=f"Index '{index_name}' dropped successfully",
                        details={"index": index_name, "concurrent": concurrent, "dropped_info": index_info}
                    )

        except Exception as e:
            return OperationResult(
                success=False, operation_type=self.get_operation_type(),
                operation_name="DropIndex", error=f"Failed to drop index: {str(e)}",
                details={"index": index_name}
            )