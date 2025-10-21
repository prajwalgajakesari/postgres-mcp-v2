"""Create index operation implementation."""

from typing import Optional, List, Any
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import IndexOperation


class CreateIndexOperation(IndexOperation):
    """Operation to create database indexes."""

    def get_required_permissions(self) -> List[Permission]:
        return [Permission.CREATE_INDEX]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        index_name = kwargs.get("index_name")
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")
        columns = kwargs.get("columns", [])
        index_type = kwargs.get("index_type", "btree")

        if not index_name:
            return False, "Index name is required"
        if not table_name:
            return False, "Table name is required"
        if not columns:
            return False, "At least one column is required"

        # Validate index type
        is_valid, error = self.validate_index_type(index_type)
        if not is_valid:
            return False, error

        # Check if index exists
        if not kwargs.get("if_not_exists", False):
            if await self.check_index_exists(index_name, schema_name):
                return False, f"Index '{index_name}' already exists"

        return True, None

    async def execute(self, **kwargs) -> OperationResult:
        index_name = kwargs.get("index_name")
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")
        columns = kwargs.get("columns", [])
        index_type = kwargs.get("index_type", "btree")
        unique = kwargs.get("unique", False)
        concurrent = kwargs.get("concurrent", False)
        if_not_exists = kwargs.get("if_not_exists", False)
        where_clause = kwargs.get("where")
        include_columns = kwargs.get("include_columns", [])

        try:
            if if_not_exists and await self.check_index_exists(index_name, schema_name):
                return OperationResult(
                    success=True, operation_type=self.get_operation_type(),
                    operation_name="CreateIndex", affected_objects=[],
                    message=f"Index '{index_name}' already exists, skipping",
                    details={"index": index_name, "skipped": True}
                )

            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    sql_parts = ["CREATE"]
                    if unique:
                        sql_parts.append("UNIQUE")
                    sql_parts.append("INDEX")
                    if concurrent:
                        sql_parts.append("CONCURRENTLY")
                    if if_not_exists:
                        sql_parts.append("IF NOT EXISTS")
                    
                    sql_parts.append(self._quote_identifier(index_name))
                    sql_parts.append("ON")
                    sql_parts.append(f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}")

                    # USING clause comes before columns
                    if index_type.upper() != "BTREE":
                        sql_parts.append(f"USING {index_type.upper()}")

                    # Format columns
                    col_list = ", ".join(
                        self._quote_identifier(c) if isinstance(c, str) else c
                        for c in columns
                    )
                    sql_parts.append(f"({col_list})")
                    
                    # INCLUDE clause for covering indexes
                    if include_columns:
                        include_list = ", ".join(self._quote_identifier(c) for c in include_columns)
                        sql_parts.append(f"INCLUDE ({include_list})")
                    
                    # WHERE clause for partial indexes
                    if where_clause:
                        # Basic validation to prevent obvious SQL injection
                        if any(dangerous in where_clause.upper() for dangerous in [';', 'DROP', 'DELETE', 'INSERT', 'UPDATE']):
                            raise ValueError("Invalid characters in WHERE clause")
                        sql_parts.append(f"WHERE {where_clause}")
                    
                    sql = " ".join(sql_parts)
                    await cur.execute(sql)
                    
                    # Add comment if provided
                    if comment := kwargs.get("comment"):
                        comment_sql = f"COMMENT ON INDEX {self._quote_identifier(index_name)} IS %s"
                        await cur.execute(comment_sql, (comment,))
                    
                    index_info = await self.get_index_info(index_name, schema_name)
                    
                    return OperationResult(
                        success=True, operation_type=self.get_operation_type(),
                        operation_name="CreateIndex",
                        affected_objects=[f"index:{index_name}"],
                        message=f"Index '{index_name}' created successfully",
                        details={"index": index_name, "table": f"{schema_name}.{table_name}", "method": method, "index_info": index_info}
                    )

        except Exception as e:
            return OperationResult(
                success=False, operation_type=self.get_operation_type(),
                operation_name="CreateIndex", error=f"Failed to create index: {str(e)}",
                details={"index": index_name, "table": f"{schema_name}.{table_name}"}
            )