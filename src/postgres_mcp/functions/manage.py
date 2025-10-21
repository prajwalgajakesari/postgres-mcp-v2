"""Function management operation implementation."""

from typing import Optional, List, Dict, Any
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import FunctionOperation


class ManageFunctionsOperation(FunctionOperation):
    """Operation to manage database functions."""

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

        function_name = kwargs.get("function_name")
        if not function_name:
            return False, "Function name is required"

        if operation == "CREATE":
            language = kwargs.get("language", "plpgsql")
            is_valid, error = self.validate_language(language)
            if not is_valid:
                return False, error

            body = kwargs.get("body")
            if not body:
                return False, "Function body is required"

        return True, None

    async def execute(self, **kwargs) -> OperationResult:
        operation = kwargs.get("operation", "").upper()
        function_name = kwargs.get("function_name")
        schema_name = kwargs.get("schema_name", "public")

        try:
            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    if operation == "CREATE":
                        return await self._create_function(cur, **kwargs)
                    elif operation == "DROP":
                        return await self._drop_function(cur, **kwargs)
                    elif operation == "ALTER":
                        return await self._alter_function(cur, **kwargs)

        except Exception as e:
            return OperationResult(
                success=False, operation_type=self.get_operation_type(),
                operation_name="ManageFunctions", error=f"Failed to {operation.lower()} function: {str(e)}",
                details={"function": function_name, "schema": schema_name, "operation": operation}
            )

    async def _create_function(self, cur, **kwargs) -> OperationResult:
        function_name = kwargs.get("function_name")
        schema_name = kwargs.get("schema_name", "public")
        parameters = kwargs.get("parameters", [])
        return_type = kwargs.get("return_type", "void")
        language = kwargs.get("language", "plpgsql")
        body = kwargs.get("body")
        or_replace = kwargs.get("or_replace", False)
        volatility = kwargs.get("volatility", "VOLATILE")
        strict = kwargs.get("strict", False)
        security_definer = kwargs.get("security_definer", False)
        cost = kwargs.get("cost")
        rows = kwargs.get("rows")

        sql_parts = ["CREATE"]
        if or_replace:
            sql_parts.append("OR REPLACE")
        sql_parts.append("FUNCTION")

        full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(function_name)}"
        param_list = self.format_parameter_list(parameters)
        sql_parts.append(f"{full_name}{param_list}")

        sql_parts.append(f"RETURNS {return_type}")
        sql_parts.append(f"LANGUAGE {language}")
        sql_parts.append(volatility.upper())

        if strict:
            sql_parts.append("STRICT")
        if security_definer:
            sql_parts.append("SECURITY DEFINER")
        if cost:
            sql_parts.append(f"COST {cost}")
        if rows:
            sql_parts.append(f"ROWS {rows}")

        sql_parts.append(f"AS $$\n{body}\n$$")

        sql = " ".join(sql_parts)
        await cur.execute(sql)

        if comment := kwargs.get("comment"):
            comment_sql = f"COMMENT ON FUNCTION {full_name}{param_list} IS %s"
            await cur.execute(comment_sql, (comment,))

        function_info = await self.get_function_info(function_name, schema_name)

        return OperationResult(
            success=True, operation_type=self.get_operation_type(),
            operation_name="CreateFunction",
            affected_objects=[f"function:{schema_name}.{function_name}"],
            message=f"Function '{schema_name}.{function_name}' created successfully",
            details={"function": f"{schema_name}.{function_name}", "language": language, "function_info": function_info}
        )

    async def _drop_function(self, cur, **kwargs) -> OperationResult:
        function_name = kwargs.get("function_name")
        schema_name = kwargs.get("schema_name", "public")
        parameters = kwargs.get("parameters", [])
        if_exists = kwargs.get("if_exists", False)
        cascade = kwargs.get("cascade", False)

        if if_exists and not await self.check_function_exists(function_name, schema_name):
            return OperationResult(
                success=True, operation_type=self.get_operation_type(),
                operation_name="DropFunction", affected_objects=[],
                message=f"Function '{function_name}' does not exist, skipping",
                details={"function": function_name, "skipped": True}
            )

        function_info = await self.get_function_info(function_name, schema_name)

        sql_parts = ["DROP FUNCTION"]
        if if_exists:
            sql_parts.append("IF EXISTS")

        full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(function_name)}"
        param_list = self.format_parameter_list(parameters) if parameters else ""
        sql_parts.append(f"{full_name}{param_list}")

        if cascade:
            sql_parts.append("CASCADE")

        sql = " ".join(sql_parts)
        await cur.execute(sql)

        return OperationResult(
            success=True, operation_type=self.get_operation_type(),
            operation_name="DropFunction",
            affected_objects=[f"function:{schema_name}.{function_name}"],
            message=f"Function '{schema_name}.{function_name}' dropped successfully",
            details={"function": f"{schema_name}.{function_name}", "cascade": cascade, "dropped_info": function_info}
        )

    async def _alter_function(self, cur, **kwargs) -> OperationResult:
        function_name = kwargs.get("function_name")
        schema_name = kwargs.get("schema_name", "public")
        parameters = kwargs.get("parameters", [])
        rename_to = kwargs.get("rename_to")
        owner_to = kwargs.get("owner_to")
        set_schema = kwargs.get("set_schema")
        volatility = kwargs.get("volatility")
        strict = kwargs.get("strict")
        security_definer = kwargs.get("security_definer")

        full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(function_name)}"
        param_list = self.format_parameter_list(parameters) if parameters else ""
        alterations_made = []

        if rename_to:
            sql = f"ALTER FUNCTION {full_name}{param_list} RENAME TO {self._quote_identifier(rename_to)}"
            await cur.execute(sql)
            alterations_made.append(f"Renamed to '{rename_to}'")
            function_name = rename_to
            full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(function_name)}"

        if owner_to:
            sql = f"ALTER FUNCTION {full_name}{param_list} OWNER TO {self._quote_identifier(owner_to)}"
            await cur.execute(sql)
            alterations_made.append(f"Changed owner to '{owner_to}'")

        if set_schema:
            sql = f"ALTER FUNCTION {full_name}{param_list} SET SCHEMA {self._quote_identifier(set_schema)}"
            await cur.execute(sql)
            alterations_made.append(f"Moved to schema '{set_schema}'")
            schema_name = set_schema

        if volatility:
            sql = f"ALTER FUNCTION {full_name}{param_list} {volatility.upper()}"
            await cur.execute(sql)
            alterations_made.append(f"Set volatility to {volatility.upper()}")

        if strict is not None:
            action = "STRICT" if strict else "CALLED ON NULL INPUT"
            sql = f"ALTER FUNCTION {full_name}{param_list} {action}"
            await cur.execute(sql)
            alterations_made.append(f"Set to {action}")

        if security_definer is not None:
            action = "SECURITY DEFINER" if security_definer else "SECURITY INVOKER"
            sql = f"ALTER FUNCTION {full_name}{param_list} {action}"
            await cur.execute(sql)
            alterations_made.append(f"Set to {action}")

        function_info = await self.get_function_info(function_name, schema_name)

        return OperationResult(
            success=True, operation_type=self.get_operation_type(),
            operation_name="AlterFunction",
            affected_objects=[f"function:{schema_name}.{function_name}"],
            message=f"Function '{schema_name}.{function_name}' altered successfully",
            details={"function": f"{schema_name}.{function_name}", "alterations": alterations_made, "function_info": function_info}
        )