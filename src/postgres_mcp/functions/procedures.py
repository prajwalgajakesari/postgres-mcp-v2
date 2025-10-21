"""Procedure management operation implementation."""

from typing import Optional, List, Dict, Any
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import FunctionOperation


class ManageProceduresOperation(FunctionOperation):
    """Operation to manage database stored procedures."""

    def get_required_permissions(self) -> List[Permission]:
        operation = getattr(self, 'current_operation', 'CREATE')
        if operation == 'DROP':
            return [Permission.DROP_FUNCTION]
        return [Permission.CREATE_FUNCTION]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        operation = kwargs.get("operation", "").upper()
        self.current_operation = operation

        if operation not in ["CREATE", "DROP", "ALTER"]:
            return False, "Operation must be CREATE, DROP, or ALTER"

        procedure_name = kwargs.get("procedure_name")
        if not procedure_name:
            return False, "Procedure name is required"

        if operation == "CREATE":
            language = kwargs.get("language", "plpgsql")
            is_valid, error = self.validate_language(language)
            if not is_valid:
                return False, error

            body = kwargs.get("body")
            if not body:
                return False, "Procedure body is required"

        return True, None

    async def execute(self, **kwargs) -> OperationResult:
        operation = kwargs.get("operation", "").upper()
        procedure_name = kwargs.get("procedure_name")
        schema_name = kwargs.get("schema_name", "public")

        try:
            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    if operation == "CREATE":
                        return await self._create_procedure(cur, **kwargs)
                    elif operation == "DROP":
                        return await self._drop_procedure(cur, **kwargs)
                    elif operation == "ALTER":
                        return await self._alter_procedure(cur, **kwargs)

        except Exception as e:
            return OperationResult(
                success=False, operation_type=self.get_operation_type(),
                operation_name="ManageProcedures", error=f"Failed to {operation.lower()} procedure: {str(e)}",
                details={"procedure": procedure_name, "schema": schema_name, "operation": operation}
            )

    async def _create_procedure(self, cur, **kwargs) -> OperationResult:
        procedure_name = kwargs.get("procedure_name")
        schema_name = kwargs.get("schema_name", "public")
        parameters = kwargs.get("parameters", [])
        language = kwargs.get("language", "plpgsql")
        body = kwargs.get("body")
        or_replace = kwargs.get("or_replace", False)
        security_definer = kwargs.get("security_definer", False)

        sql_parts = ["CREATE"]
        if or_replace:
            sql_parts.append("OR REPLACE")
        sql_parts.append("PROCEDURE")

        full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(procedure_name)}"
        param_list = self.format_parameter_list(parameters)
        sql_parts.append(f"{full_name}{param_list}")

        sql_parts.append(f"LANGUAGE {language}")

        if security_definer:
            sql_parts.append("SECURITY DEFINER")

        sql_parts.append(f"AS $$\n{body}\n$$")

        sql = " ".join(sql_parts)
        await cur.execute(sql)

        if comment := kwargs.get("comment"):
            comment_sql = f"COMMENT ON PROCEDURE {full_name}{param_list} IS %s"
            await cur.execute(comment_sql, (comment,))

        procedure_info = await self.get_procedure_info(procedure_name, schema_name)

        return OperationResult(
            success=True, operation_type=self.get_operation_type(),
            operation_name="CreateProcedure",
            affected_objects=[f"procedure:{schema_name}.{procedure_name}"],
            message=f"Procedure '{schema_name}.{procedure_name}' created successfully",
            details={"procedure": f"{schema_name}.{procedure_name}", "language": language, "procedure_info": procedure_info}
        )

    async def _drop_procedure(self, cur, **kwargs) -> OperationResult:
        procedure_name = kwargs.get("procedure_name")
        schema_name = kwargs.get("schema_name", "public")
        parameters = kwargs.get("parameters", [])
        if_exists = kwargs.get("if_exists", False)
        cascade = kwargs.get("cascade", False)

        if if_exists and not await self.check_procedure_exists(procedure_name, schema_name):
            return OperationResult(
                success=True, operation_type=self.get_operation_type(),
                operation_name="DropProcedure", affected_objects=[],
                message=f"Procedure '{procedure_name}' does not exist, skipping",
                details={"procedure": procedure_name, "skipped": True}
            )

        procedure_info = await self.get_procedure_info(procedure_name, schema_name)

        sql_parts = ["DROP PROCEDURE"]
        if if_exists:
            sql_parts.append("IF EXISTS")

        full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(procedure_name)}"
        param_list = self.format_parameter_list(parameters) if parameters else ""
        sql_parts.append(f"{full_name}{param_list}")

        if cascade:
            sql_parts.append("CASCADE")

        sql = " ".join(sql_parts)
        await cur.execute(sql)

        return OperationResult(
            success=True, operation_type=self.get_operation_type(),
            operation_name="DropProcedure",
            affected_objects=[f"procedure:{schema_name}.{procedure_name}"],
            message=f"Procedure '{schema_name}.{procedure_name}' dropped successfully",
            details={"procedure": f"{schema_name}.{procedure_name}", "cascade": cascade, "dropped_info": procedure_info}
        )

    async def _alter_procedure(self, cur, **kwargs) -> OperationResult:
        procedure_name = kwargs.get("procedure_name")
        schema_name = kwargs.get("schema_name", "public")
        parameters = kwargs.get("parameters", [])
        rename_to = kwargs.get("rename_to")
        owner_to = kwargs.get("owner_to")
        set_schema = kwargs.get("set_schema")
        security_definer = kwargs.get("security_definer")

        full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(procedure_name)}"
        param_list = self.format_parameter_list(parameters) if parameters else ""
        alterations_made = []

        if rename_to:
            sql = f"ALTER PROCEDURE {full_name}{param_list} RENAME TO {self._quote_identifier(rename_to)}"
            await cur.execute(sql)
            alterations_made.append(f"Renamed to '{rename_to}'")
            procedure_name = rename_to
            full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(procedure_name)}"

        if owner_to:
            sql = f"ALTER PROCEDURE {full_name}{param_list} OWNER TO {self._quote_identifier(owner_to)}"
            await cur.execute(sql)
            alterations_made.append(f"Changed owner to '{owner_to}'")

        if set_schema:
            sql = f"ALTER PROCEDURE {full_name}{param_list} SET SCHEMA {self._quote_identifier(set_schema)}"
            await cur.execute(sql)
            alterations_made.append(f"Moved to schema '{set_schema}'")
            schema_name = set_schema

        if security_definer is not None:
            action = "SECURITY DEFINER" if security_definer else "SECURITY INVOKER"
            sql = f"ALTER PROCEDURE {full_name}{param_list} {action}"
            await cur.execute(sql)
            alterations_made.append(f"Set to {action}")

        procedure_info = await self.get_procedure_info(procedure_name, schema_name)

        return OperationResult(
            success=True, operation_type=self.get_operation_type(),
            operation_name="AlterProcedure",
            affected_objects=[f"procedure:{schema_name}.{procedure_name}"],
            message=f"Procedure '{schema_name}.{procedure_name}' altered successfully",
            details={"procedure": f"{schema_name}.{procedure_name}", "alterations": alterations_made, "procedure_info": procedure_info}
        )

    async def get_procedure_info(self, procedure_name: str, schema_name: str = "public") -> Optional[Dict[str, Any]]:
        """Get detailed information about a procedure."""
        query = """
            SELECT
                r.routine_name,
                r.routine_type,
                r.routine_body,
                r.routine_definition,
                r.external_language,
                r.security_type,
                r.routine_comment,
                p.prosecdef
            FROM information_schema.routines r
            JOIN pg_proc p ON p.proname = r.routine_name
            JOIN pg_namespace n ON n.oid = p.pronamespace AND n.nspname = r.routine_schema
            WHERE r.routine_schema = %s AND r.routine_name = %s AND r.routine_type = 'PROCEDURE'
        """

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (schema_name, procedure_name))
                result = await cur.fetchone()

                if not result:
                    return None

                return {
                    "name": result[0],
                    "type": result[1],
                    "body": result[2],
                    "definition": result[3],
                    "language": result[4],
                    "security_type": result[5],
                    "comment": result[6],
                    "security_definer": result[7]
                }