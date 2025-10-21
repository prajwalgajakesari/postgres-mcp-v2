"""Base class for function and procedure operations."""

from typing import Optional, Dict, Any, List
from ..operations import DDLOperation


class FunctionOperation(DDLOperation):
    """Base class for function and procedure operations."""

    # PostgreSQL function languages
    FUNCTION_LANGUAGES = {
        "sql", "plpgsql", "c", "internal", "plpython3u", "plperl", "pltcl"
    }

    async def check_function_exists(self, function_name: str, schema_name: str = "public") -> bool:
        """Check if function exists."""
        query = """
            SELECT EXISTS(
                SELECT 1
                FROM information_schema.routines
                WHERE routine_schema = %s
                AND routine_name = %s
                AND routine_type = 'FUNCTION'
            )
        """
        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (schema_name, function_name))
                result = await cur.fetchone()
                return result[0] if result else False

    async def check_procedure_exists(self, procedure_name: str, schema_name: str = "public") -> bool:
        """Check if procedure exists."""
        query = """
            SELECT EXISTS(
                SELECT 1
                FROM information_schema.routines
                WHERE routine_schema = %s
                AND routine_name = %s
                AND routine_type = 'PROCEDURE'
            )
        """
        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (schema_name, procedure_name))
                result = await cur.fetchone()
                return result[0] if result else False

    async def get_function_info(self, function_name: str, schema_name: str = "public") -> Optional[Dict[str, Any]]:
        """Get detailed information about a function."""
        query = """
            SELECT
                r.routine_name,
                r.routine_type,
                r.data_type as return_type,
                r.routine_body,
                r.routine_definition,
                r.external_language,
                r.is_deterministic,
                r.sql_data_access,
                r.security_type,
                r.routine_comment,
                p.provolatile,
                p.proisstrict,
                p.prosecdef,
                p.proleakproof,
                p.procost,
                p.prorows
            FROM information_schema.routines r
            JOIN pg_proc p ON p.proname = r.routine_name
            JOIN pg_namespace n ON n.oid = p.pronamespace AND n.nspname = r.routine_schema
            WHERE r.routine_schema = %s AND r.routine_name = %s
        """

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (schema_name, function_name))
                result = await cur.fetchone()

                if not result:
                    return None

                return {
                    "name": result[0],
                    "type": result[1],
                    "return_type": result[2],
                    "body": result[3],
                    "definition": result[4],
                    "language": result[5],
                    "is_deterministic": result[6] == "YES",
                    "sql_data_access": result[7],
                    "security_type": result[8],
                    "comment": result[9],
                    "volatility": result[10],
                    "is_strict": result[11],
                    "security_definer": result[12],
                    "leakproof": result[13],
                    "cost": result[14],
                    "rows": result[15]
                }

    def validate_language(self, language: str) -> tuple[bool, Optional[str]]:
        """Validate function language."""
        if language.lower() not in self.FUNCTION_LANGUAGES:
            return False, f"Invalid language. Valid languages: {', '.join(self.FUNCTION_LANGUAGES)}"
        return True, None

    def format_parameter_list(self, parameters: List[Dict[str, Any]]) -> str:
        """Format function parameters."""
        if not parameters:
            return "()"

        param_parts = []
        for param in parameters:
            parts = []

            # Parameter mode (IN, OUT, INOUT)
            if mode := param.get("mode"):
                parts.append(mode.upper())

            # Parameter name
            if name := param.get("name"):
                parts.append(self._quote_identifier(name))

            # Parameter type
            if param_type := param.get("type"):
                parts.append(param_type)

            # Default value
            if default := param.get("default"):
                if isinstance(default, str):
                    parts.append(f"DEFAULT '{default}'")
                else:
                    parts.append(f"DEFAULT {default}")

            param_parts.append(" ".join(parts))

        return f"({', '.join(param_parts)})"