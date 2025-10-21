"""Trigger management operation implementation."""

from typing import Optional, List, Dict, Any
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import IndexOperation


class ManageTriggersOperation(IndexOperation):
    """Operation to manage database triggers."""

    def get_required_permissions(self) -> List[Permission]:
        operation = getattr(self, 'current_operation', 'CREATE')
        if operation == 'DROP':
            return [Permission.DROP_TRIGGER]
        return [Permission.CREATE_TRIGGER]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        operation = kwargs.get("operation", "").upper()
        self.current_operation = operation

        if operation not in ["CREATE", "DROP", "ALTER"]:
            return False, "Operation must be CREATE, DROP, or ALTER"

        trigger_name = kwargs.get("trigger_name")
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")

        if not trigger_name:
            return False, "Trigger name is required"
        if not table_name:
            return False, "Table name is required"

        if operation == "CREATE":
            function_name = kwargs.get("function_name")
            if not function_name:
                return False, "Function name is required for CREATE operation"

            timing = kwargs.get("timing", "").upper()
            if timing not in ["BEFORE", "AFTER", "INSTEAD OF"]:
                return False, "Timing must be BEFORE, AFTER, or INSTEAD OF"

            events = kwargs.get("events", [])
            if not events:
                return False, "At least one event is required"

            valid_events = {"INSERT", "UPDATE", "DELETE", "TRUNCATE"}
            for event in events:
                if event.upper() not in valid_events:
                    return False, f"Invalid event: {event}. Valid events: {', '.join(valid_events)}"

        elif operation == "DROP":
            if not kwargs.get("if_exists", False):
                if not await self.check_trigger_exists(trigger_name, table_name, schema_name):
                    return False, f"Trigger '{trigger_name}' does not exist"

        return True, None

    async def execute(self, **kwargs) -> OperationResult:
        operation = kwargs.get("operation", "").upper()
        trigger_name = kwargs.get("trigger_name")
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")

        try:
            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    if operation == "CREATE":
                        return await self._create_trigger(cur, **kwargs)
                    elif operation == "DROP":
                        return await self._drop_trigger(cur, **kwargs)
                    elif operation == "ALTER":
                        return await self._alter_trigger(cur, **kwargs)

        except Exception as e:
            return OperationResult(
                success=False, operation_type=self.get_operation_type(),
                operation_name="ManageTriggers", error=f"Failed to {operation.lower()} trigger: {str(e)}",
                details={"trigger": trigger_name, "table": f"{schema_name}.{table_name}", "operation": operation}
            )

    async def _create_trigger(self, cur, **kwargs) -> OperationResult:
        trigger_name = kwargs.get("trigger_name")
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")
        function_name = kwargs.get("function_name")
        timing = kwargs.get("timing", "").upper()
        events = [e.upper() for e in kwargs.get("events", [])]
        for_each = kwargs.get("for_each", "ROW").upper()
        when_condition = kwargs.get("when_condition")

        sql_parts = ["CREATE TRIGGER", self._quote_identifier(trigger_name)]
        sql_parts.append(timing)
        sql_parts.append(" OR ".join(events))
        sql_parts.append("ON")
        sql_parts.append(f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}")
        sql_parts.append(f"FOR EACH {for_each}")

        if when_condition:
            sql_parts.append(f"WHEN ({when_condition})")

        sql_parts.append("EXECUTE FUNCTION")
        sql_parts.append(f"{self._quote_identifier(function_name)}()")

        sql = " ".join(sql_parts)
        await cur.execute(sql)

        return OperationResult(
            success=True, operation_type=self.get_operation_type(),
            operation_name="CreateTrigger",
            affected_objects=[f"trigger:{trigger_name}"],
            message=f"Trigger '{trigger_name}' created successfully",
            details={"trigger": trigger_name, "table": f"{schema_name}.{table_name}", "function": function_name}
        )

    async def _drop_trigger(self, cur, **kwargs) -> OperationResult:
        trigger_name = kwargs.get("trigger_name")
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")
        if_exists = kwargs.get("if_exists", False)
        cascade = kwargs.get("cascade", False)

        if if_exists and not await self.check_trigger_exists(trigger_name, table_name, schema_name):
            return OperationResult(
                success=True, operation_type=self.get_operation_type(),
                operation_name="DropTrigger", affected_objects=[],
                message=f"Trigger '{trigger_name}' does not exist, skipping",
                details={"trigger": trigger_name, "skipped": True}
            )

        sql_parts = ["DROP TRIGGER"]
        if if_exists:
            sql_parts.append("IF EXISTS")

        sql_parts.append(self._quote_identifier(trigger_name))
        sql_parts.append("ON")
        sql_parts.append(f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}")

        if cascade:
            sql_parts.append("CASCADE")

        sql = " ".join(sql_parts)
        await cur.execute(sql)

        return OperationResult(
            success=True, operation_type=self.get_operation_type(),
            operation_name="DropTrigger",
            affected_objects=[f"trigger:{trigger_name}"],
            message=f"Trigger '{trigger_name}' dropped successfully",
            details={"trigger": trigger_name, "table": f"{schema_name}.{table_name}", "cascade": cascade}
        )

    async def _alter_trigger(self, cur, **kwargs) -> OperationResult:
        trigger_name = kwargs.get("trigger_name")
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")
        rename_to = kwargs.get("rename_to")

        if rename_to:
            sql = f"""
                ALTER TRIGGER {self._quote_identifier(trigger_name)}
                ON {self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}
                RENAME TO {self._quote_identifier(rename_to)}
            """
            await cur.execute(sql)

            return OperationResult(
                success=True, operation_type=self.get_operation_type(),
                operation_name="AlterTrigger",
                affected_objects=[f"trigger:{rename_to}"],
                message=f"Trigger '{trigger_name}' renamed to '{rename_to}'",
                details={"old_name": trigger_name, "new_name": rename_to, "table": f"{schema_name}.{table_name}"}
            )

        return OperationResult(
            success=False, operation_type=self.get_operation_type(),
            operation_name="AlterTrigger", error="No alterations specified",
            details={"trigger": trigger_name}
        )