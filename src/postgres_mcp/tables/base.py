"""Base class for table management operations."""

from typing import Optional, Dict, Any, List, Tuple
import psycopg
from ..operations import DDLOperation
from ..permissions import Permission


class TableOperation(DDLOperation):
    """Base class for table and schema operations."""

    # PostgreSQL data types
    POSTGRES_TYPES = {
        # Numeric types
        "smallint", "integer", "bigint", "decimal", "numeric", "real",
        "double precision", "smallserial", "serial", "bigserial",

        # Monetary
        "money",

        # Character types
        "character varying", "varchar", "character", "char", "text",

        # Binary data
        "bytea",

        # Date/time types
        "timestamp", "timestamp with time zone", "timestamp without time zone",
        "date", "time", "time with time zone", "time without time zone",
        "interval",

        # Boolean
        "boolean",

        # Enumerated
        "enum",

        # Geometric
        "point", "line", "lseg", "box", "path", "polygon", "circle",

        # Network addresses
        "cidr", "inet", "macaddr", "macaddr8",

        # Bit strings
        "bit", "bit varying",

        # Text search
        "tsvector", "tsquery",

        # UUID
        "uuid",

        # XML
        "xml",

        # JSON
        "json", "jsonb",

        # Arrays (handled separately)
        # Composite types (handled separately)
        # Range types
        "int4range", "int8range", "numrange", "tsrange", "tstzrange", "daterange",

        # Other
        "pg_lsn", "pg_snapshot", "txid_snapshot"
    }

    async def check_table_exists(self, table_name: str, schema_name: str = "public") -> bool:
        """Check if table exists in the database.

        Args:
            table_name: The table name to check
            schema_name: The schema name (default: public)

        Returns:
            True if table exists, False otherwise
        """
        query = """
            SELECT EXISTS(
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = %s
            )
        """

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (schema_name, table_name))
                result = await cur.fetchone()
                return result[0] if result else False

    async def check_schema_exists(self, schema_name: str) -> bool:
        """Check if schema exists in the database.

        Args:
            schema_name: The schema name to check

        Returns:
            True if schema exists, False otherwise
        """
        query = """
            SELECT EXISTS(
                SELECT 1
                FROM information_schema.schemata
                WHERE schema_name = %s
            )
        """

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (schema_name,))
                result = await cur.fetchone()
                return result[0] if result else False

    async def check_column_exists(self, table_name: str, column_name: str, schema_name: str = "public") -> bool:
        """Check if column exists in a table.

        Args:
            table_name: The table name
            column_name: The column name to check
            schema_name: The schema name (default: public)

        Returns:
            True if column exists, False otherwise
        """
        query = """
            SELECT EXISTS(
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = %s
                AND table_name = %s
                AND column_name = %s
            )
        """

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (schema_name, table_name, column_name))
                result = await cur.fetchone()
                return result[0] if result else False

    async def get_table_info(self, table_name: str, schema_name: str = "public") -> Optional[Dict[str, Any]]:
        """Get detailed information about a table.

        Args:
            table_name: The table name
            schema_name: The schema name (default: public)

        Returns:
            Dictionary with table information or None if table doesn't exist
        """
        # Get basic table info
        table_query = """
            SELECT
                t.table_type,
                obj_description(c.oid) as comment,
                pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
                pg_size_pretty(pg_relation_size(c.oid)) as table_size,
                pg_size_pretty(pg_indexes_size(c.oid)) as indexes_size,
                n.nspowner::regrole as owner
            FROM information_schema.tables t
            JOIN pg_class c ON c.relname = t.table_name
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
            WHERE t.table_schema = %s AND t.table_name = %s
        """

        # Get column info
        column_query = """
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default,
                is_identity,
                identity_generation,
                is_generated,
                generation_expression
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """

        # Get constraint info
        constraint_query = """
            SELECT
                tc.constraint_name,
                tc.constraint_type,
                tc.is_deferrable,
                tc.initially_deferred,
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                rc.update_rule,
                rc.delete_rule,
                cc.check_clause
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            LEFT JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema != tc.table_schema
            LEFT JOIN information_schema.referential_constraints rc
                ON rc.constraint_name = tc.constraint_name
            LEFT JOIN information_schema.check_constraints cc
                ON cc.constraint_name = tc.constraint_name
            WHERE tc.table_schema = %s AND tc.table_name = %s
            ORDER BY tc.constraint_type, tc.constraint_name
        """

        # Get index info
        index_query = """
            SELECT
                i.indexname,
                i.indexdef,
                idx.indisunique,
                idx.indisprimary,
                pg_size_pretty(pg_relation_size(idx.indexrelid)) as index_size
            FROM pg_indexes i
            JOIN pg_class c ON c.relname = i.tablename
            JOIN pg_namespace n ON n.nspname = i.schemaname
            JOIN pg_index idx ON idx.indrelid = c.oid
            JOIN pg_class ic ON ic.oid = idx.indexrelid AND ic.relname = i.indexname
            WHERE i.schemaname = %s AND i.tablename = %s
        """

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                # Get basic table info
                await cur.execute(table_query, (schema_name, table_name))
                table_result = await cur.fetchone()

                if not table_result:
                    return None

                # Get columns
                await cur.execute(column_query, (schema_name, table_name))
                columns = []
                for row in await cur.fetchall():
                    columns.append({
                        "name": row[0],
                        "type": row[1],
                        "max_length": row[2],
                        "numeric_precision": row[3],
                        "numeric_scale": row[4],
                        "nullable": row[5] == "YES",
                        "default": row[6],
                        "is_identity": row[7] == "YES",
                        "identity_generation": row[8],
                        "is_generated": row[9] == "ALWAYS",
                        "generation_expression": row[10]
                    })

                # Get constraints
                await cur.execute(constraint_query, (schema_name, table_name))
                constraints = {}
                for row in await cur.fetchall():
                    constraint_name = row[0]
                    if constraint_name not in constraints:
                        constraints[constraint_name] = {
                            "type": row[1],
                            "is_deferrable": row[2] == "YES",
                            "initially_deferred": row[3] == "YES",
                            "columns": [],
                            "foreign_table_schema": row[5],
                            "foreign_table_name": row[6],
                            "foreign_columns": [],
                            "update_rule": row[8],
                            "delete_rule": row[9],
                            "check_clause": row[10]
                        }
                    if row[4]:  # column_name
                        constraints[constraint_name]["columns"].append(row[4])
                    if row[7]:  # foreign_column_name
                        constraints[constraint_name]["foreign_columns"].append(row[7])

                # Get indexes
                await cur.execute(index_query, (schema_name, table_name))
                indexes = []
                for row in await cur.fetchall():
                    indexes.append({
                        "name": row[0],
                        "definition": row[1],
                        "is_unique": row[2],
                        "is_primary": row[3],
                        "size": row[4]
                    })

                return {
                    "schema": schema_name,
                    "name": table_name,
                    "type": table_result[0],
                    "comment": table_result[1],
                    "total_size": table_result[2],
                    "table_size": table_result[3],
                    "indexes_size": table_result[4],
                    "owner": table_result[5],
                    "columns": columns,
                    "constraints": constraints,
                    "indexes": indexes
                }

    def validate_data_type(self, data_type: str) -> tuple[bool, Optional[str]]:
        """Validate a PostgreSQL data type.

        Args:
            data_type: The data type to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Remove array notation if present
        base_type = data_type.replace("[]", "").strip().lower()

        # Check for parameterized types
        if "(" in base_type:
            base_type = base_type.split("(")[0].strip()

        # Check if it's a known PostgreSQL type
        if base_type in self.POSTGRES_TYPES:
            return True, None

        # Check for common variations
        if base_type in ["int", "int2", "int4", "int8"]:
            return True, None

        if base_type in ["float4", "float8"]:
            return True, None

        if base_type == "timestamptz":
            return True, None

        # Could be a custom type - we'll let PostgreSQL validate it
        return True, None

    def format_column_definition(self, column: Dict[str, Any]) -> str:
        """Format a column definition for CREATE/ALTER TABLE.

        Args:
            column: Dictionary with column properties

        Returns:
            SQL column definition string
        """
        parts = [self._quote_identifier(column["name"])]

        # Data type
        data_type = column["type"]
        if column.get("array"):
            data_type += "[]"
        parts.append(data_type)

        # Constraints
        if column.get("not_null"):
            parts.append("NOT NULL")

        if column.get("primary_key"):
            parts.append("PRIMARY KEY")

        if column.get("unique"):
            parts.append("UNIQUE")

        if "default" in column:
            default = column["default"]
            # Handle special defaults
            if isinstance(default, str):
                if default.upper() in ["CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"]:
                    parts.append(f"DEFAULT {default}")
                elif default.startswith("nextval(") or default.startswith("uuid_generate"):
                    parts.append(f"DEFAULT {default}")
                else:
                    parts.append(f"DEFAULT '{default}'")
            elif default is None:
                parts.append("DEFAULT NULL")
            elif isinstance(default, bool):
                parts.append(f"DEFAULT {str(default).upper()}")
            else:
                parts.append(f"DEFAULT {default}")

        if column.get("check"):
            parts.append(f"CHECK ({column['check']})")

        if column.get("references"):
            ref = column["references"]
            ref_sql = f"REFERENCES {self._quote_identifier(ref['table'])}"
            if ref.get("column"):
                ref_sql += f"({self._quote_identifier(ref['column'])})"
            if ref.get("on_delete"):
                ref_sql += f" ON DELETE {ref['on_delete']}"
            if ref.get("on_update"):
                ref_sql += f" ON UPDATE {ref['on_update']}"
            parts.append(ref_sql)

        if column.get("generated"):
            gen = column["generated"]
            if gen.get("always"):
                if gen.get("identity"):
                    parts.append("GENERATED ALWAYS AS IDENTITY")
                elif gen.get("expression"):
                    parts.append(f"GENERATED ALWAYS AS ({gen['expression']}) STORED")
            elif gen.get("by_default") and gen.get("identity"):
                parts.append("GENERATED BY DEFAULT AS IDENTITY")

        return " ".join(parts)