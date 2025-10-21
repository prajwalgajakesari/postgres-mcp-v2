"""MCP tool definitions for table management operations."""

from typing import Optional, List, Dict, Any
import mcp.types as types
from mcp.server.fastmcp import FastMCP
from pydantic import Field

from .tables import (
    CreateTableOperation,
    AlterTableOperation,
    DropTableOperation,
    ManageSchemaOperation,
    ManageConstraintsOperation
)
from .permissions import RBACManager, AuditLogger
from .sql import DbConnPool

ResponseType = List[types.TextContent | types.ImageContent | types.EmbeddedResource]


def format_text_response(text: str) -> ResponseType:
    """Format a text response."""
    return [types.TextContent(type="text", text=str(text))]


def format_error_response(error: str) -> ResponseType:
    """Format an error response."""
    return format_text_response(f"Error: {error}")


def format_operation_result(result: Any) -> ResponseType:
    """Format an operation result for MCP response."""
    if hasattr(result, "success"):
        if result.success:
            response = result.message or "Operation completed successfully"
            if hasattr(result, "details") and result.details:
                response += f"\n\nDetails:\n{_format_details(result.details)}"
        else:
            response = f"Operation failed: {result.error}"
            if hasattr(result, "details") and result.details:
                response += f"\n\nDetails:\n{_format_details(result.details)}"
        return format_text_response(response)
    else:
        return format_text_response(str(result))


def _format_details(details: Dict[str, Any], indent: int = 0) -> str:
    """Format details dictionary for display."""
    lines = []
    prefix = "  " * indent
    for key, value in details.items():
        if isinstance(value, dict):
            lines.append(f"{prefix}{key}:")
            lines.append(_format_details(value, indent + 1))
        elif isinstance(value, list):
            lines.append(f"{prefix}{key}:")
            for item in value:
                if isinstance(item, dict):
                    lines.append(_format_details(item, indent + 1))
                else:
                    lines.append(f"{prefix}  - {item}")
        else:
            lines.append(f"{prefix}{key}: {value}")
    return "\n".join(lines)


def register_table_tools(
    mcp: FastMCP,
    db_connection: DbConnPool,
    rbac_manager: Optional[RBACManager] = None,
    audit_logger: Optional[AuditLogger] = None
):
    """Register table management tools with MCP server."""

    # Default to permissive if no RBAC manager provided
    if rbac_manager is None:
        rbac_manager = RBACManager()
        rbac_manager.add_custom_role("default", permissions=["*"])

    # Default to no-op audit logger if not provided
    if audit_logger is None:
        audit_logger = AuditLogger(enable_console=False, enable_file=False)

    @mcp.tool(
        description="Create a new database table",
        params={
            "table_name": Field(..., description="Name of the table to create"),
            "schema_name": Field("public", description="Schema name (default: public)"),
            "columns": Field(..., description="List of column definitions"),
            "constraints": Field(None, description="Optional table constraints"),
            "if_not_exists": Field(False, description="Don't error if table exists"),
            "inherits": Field(None, description="Parent table(s) to inherit from"),
            "temporary": Field(False, description="Create as temporary table"),
            "comment": Field(None, description="Table comment"),
            "indexes": Field(None, description="Optional indexes to create")
        }
    )
    async def create_table(
        table_name: str,
        columns: List[Dict[str, Any]],
        schema_name: str = "public",
        constraints: Optional[List[Dict[str, Any]]] = None,
        if_not_exists: bool = False,
        inherits: Optional[List[str]] = None,
        temporary: bool = False,
        comment: Optional[str] = None,
        indexes: Optional[List[Dict[str, Any]]] = None
    ) -> ResponseType:
        """Create a new database table."""
        try:
            operation = CreateTableOperation(
                connection_pool=db_connection,
                rbac_manager=rbac_manager,
                audit_logger=audit_logger
            )

            kwargs = {
                k: v for k, v in locals().items()
                if k not in ["operation"] and v is not None
            }

            result = await operation.run(
                user="system",
                role="default",
                **kwargs
            )

            return format_operation_result(result)

        except Exception as e:
            return format_error_response(str(e))

    @mcp.tool(
        description="Alter an existing database table",
        params={
            "table_name": Field(..., description="Name of the table to alter"),
            "schema_name": Field("public", description="Schema name (default: public)"),
            "add_columns": Field(None, description="Columns to add"),
            "drop_columns": Field(None, description="Columns to drop"),
            "alter_columns": Field(None, description="Columns to alter"),
            "rename_columns": Field(None, description="Columns to rename (dict of old_name: new_name)"),
            "add_constraints": Field(None, description="Constraints to add"),
            "drop_constraints": Field(None, description="Constraints to drop"),
            "rename_to": Field(None, description="New table name"),
            "set_schema": Field(None, description="New schema name"),
            "cascade": Field(False, description="Use CASCADE for operations"),
            "if_exists": Field(False, description="Don't error if object doesn't exist")
        }
    )
    async def alter_table(
        table_name: str,
        schema_name: str = "public",
        add_columns: Optional[List[Dict[str, Any]]] = None,
        drop_columns: Optional[List[str]] = None,
        alter_columns: Optional[List[Dict[str, Any]]] = None,
        rename_columns: Optional[Dict[str, str]] = None,
        add_constraints: Optional[List[Dict[str, Any]]] = None,
        drop_constraints: Optional[List[str]] = None,
        rename_to: Optional[str] = None,
        set_schema: Optional[str] = None,
        cascade: bool = False,
        if_exists: bool = False
    ) -> ResponseType:
        """Alter an existing database table."""
        try:
            operation = AlterTableOperation(
                connection_pool=db_connection,
                rbac_manager=rbac_manager,
                audit_logger=audit_logger
            )

            kwargs = {
                k: v for k, v in locals().items()
                if k not in ["operation"] and v is not None
            }

            result = await operation.run(
                user="system",
                role="default",
                **kwargs
            )

            return format_operation_result(result)

        except Exception as e:
            return format_error_response(str(e))

    @mcp.tool(
        description="Drop a database table",
        params={
            "table_name": Field(..., description="Name of the table to drop"),
            "schema_name": Field("public", description="Schema name (default: public)"),
            "cascade": Field(False, description="Drop dependent objects (CASCADE)"),
            "if_exists": Field(False, description="Don't error if table doesn't exist")
        }
    )
    async def drop_table(
        table_name: str,
        schema_name: str = "public",
        cascade: bool = False,
        if_exists: bool = False
    ) -> ResponseType:
        """Drop a database table."""
        try:
            operation = DropTableOperation(
                connection_pool=db_connection,
                rbac_manager=rbac_manager,
                audit_logger=audit_logger
            )

            result = await operation.run(
                user="system",
                role="default",
                table_name=table_name,
                schema_name=schema_name,
                cascade=cascade,
                if_exists=if_exists
            )

            return format_operation_result(result)

        except Exception as e:
            return format_error_response(str(e))

    @mcp.tool(
        description="Manage database schemas (CREATE, ALTER, DROP)",
        params={
            "operation": Field(..., description="Operation: CREATE, ALTER, or DROP"),
            "schema_name": Field(..., description="Schema name"),
            "if_not_exists": Field(False, description="For CREATE: don't error if exists"),
            "if_exists": Field(False, description="For DROP: don't error if doesn't exist"),
            "cascade": Field(False, description="For DROP: drop dependent objects"),
            "authorization": Field(None, description="For CREATE: schema owner"),
            "rename_to": Field(None, description="For ALTER: new schema name"),
            "owner_to": Field(None, description="For ALTER: new owner"),
            "comment": Field(None, description="Schema comment")
        }
    )
    async def manage_schema(
        operation: str,
        schema_name: str,
        if_not_exists: bool = False,
        if_exists: bool = False,
        cascade: bool = False,
        authorization: Optional[str] = None,
        rename_to: Optional[str] = None,
        owner_to: Optional[str] = None,
        comment: Optional[str] = None
    ) -> ResponseType:
        """Manage database schemas."""
        try:
            op = ManageSchemaOperation(
                connection_pool=db_connection,
                rbac_manager=rbac_manager,
                audit_logger=audit_logger
            )

            kwargs = {
                k: v for k, v in locals().items()
                if k not in ["op"] and v is not None
            }

            result = await op.run(
                user="system",
                role="default",
                **kwargs
            )

            return format_operation_result(result)

        except Exception as e:
            return format_error_response(str(e))

    @mcp.tool(
        description="Manage table constraints (ADD, DROP, VALIDATE)",
        params={
            "operation": Field(..., description="Operation: ADD, DROP, VALIDATE"),
            "table_name": Field(..., description="Table name"),
            "schema_name": Field("public", description="Schema name (default: public)"),
            "constraint": Field(None, description="For ADD: constraint definition"),
            "constraint_name": Field(None, description="For DROP/VALIDATE: constraint name"),
            "cascade": Field(False, description="For DROP: use CASCADE"),
            "if_exists": Field(False, description="For DROP: don't error if doesn't exist")
        }
    )
    async def manage_constraints(
        operation: str,
        table_name: str,
        schema_name: str = "public",
        constraint: Optional[Dict[str, Any]] = None,
        constraint_name: Optional[str] = None,
        cascade: bool = False,
        if_exists: bool = False
    ) -> ResponseType:
        """Manage table constraints."""
        try:
            op = ManageConstraintsOperation(
                connection_pool=db_connection,
                rbac_manager=rbac_manager,
                audit_logger=audit_logger
            )

            kwargs = {
                k: v for k, v in locals().items()
                if k not in ["op"] and v is not None
            }

            result = await op.run(
                user="system",
                role="default",
                **kwargs
            )

            return format_operation_result(result)

        except Exception as e:
            return format_error_response(str(e))