"""Base class for index and trigger operations."""

from typing import Optional, Dict, Any, List
from ..operations import DDLOperation
from ..permissions import Permission


class IndexOperation(DDLOperation):
    """Base class for index and trigger operations."""

    # PostgreSQL index types
    INDEX_TYPES = {
        "btree", "hash", "gist", "spgist", "gin", "brin"
    }

    async def check_index_exists(self, index_name: str, schema_name: str = "public") -> bool:
        """Check if index exists."""
        query = """
            SELECT EXISTS(
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = %s
                AND indexname = %s
            )
        """
        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (schema_name, index_name))
                result = await cur.fetchone()
                return result[0] if result else False

    async def get_index_info(self, index_name: str, schema_name: str = "public") -> Optional[Dict[str, Any]]:
        """Get detailed information about an index."""
        query = """
            SELECT
                i.indexname,
                i.tablename,
                i.indexdef,
                idx.indisunique,
                idx.indisprimary,
                idx.indisvalid,
                idx.indoption,
                am.amname as index_method,
                pg_size_pretty(pg_relation_size(idx.indexrelid)) as index_size,
                obj_description(idx.indexrelid) as comment
            FROM pg_indexes i
            JOIN pg_class c ON c.relname = i.tablename
            JOIN pg_namespace n ON n.nspname = i.schemaname
            JOIN pg_index idx ON idx.indrelid = c.oid
            JOIN pg_class ic ON ic.oid = idx.indexrelid AND ic.relname = i.indexname
            JOIN pg_am am ON am.oid = ic.relam
            WHERE i.schemaname = %s AND i.indexname = %s
        """

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (schema_name, index_name))
                result = await cur.fetchone()

                if not result:
                    return None

                return {
                    "name": result[0],
                    "table": result[1],
                    "definition": result[2],
                    "is_unique": result[3],
                    "is_primary": result[4],
                    "is_valid": result[5],
                    "method": result[7],
                    "size": result[8],
                    "comment": result[9]
                }

    async def check_trigger_exists(self, trigger_name: str, table_name: str, schema_name: str = "public") -> bool:
        """Check if trigger exists."""
        query = """
            SELECT EXISTS(
                SELECT 1
                FROM information_schema.triggers
                WHERE trigger_schema = %s
                AND event_object_table = %s
                AND trigger_name = %s
            )
        """
        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (schema_name, table_name, trigger_name))
                result = await cur.fetchone()
                return result[0] if result else False

    def validate_index_type(self, index_type: str) -> tuple[bool, Optional[str]]:
        """Validate index type."""
        if index_type.lower() not in self.INDEX_TYPES:
            return False, f"Invalid index type. Valid types: {', '.join(self.INDEX_TYPES)}"
        return True, None