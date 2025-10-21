"""MCP tool definitions for user management operations."""

from typing import Optional, List, Dict, Any
import mcp.types as types
from mcp.server.fastmcp import FastMCP
from pydantic import Field

from .users import (
    CreateUserOperation,
    AlterUserOperation,
    DropUserOperation,
    ManagePermissionsOperation,
    ListUsersOperation
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


def register_user_tools(
    mcp: FastMCP,
    db_connection: DbConnPool,
    rbac_manager: Optional[RBACManager] = None,
    audit_logger: Optional[AuditLogger] = None
):
    """Register user management tools with MCP server.

    Args:
        mcp: FastMCP instance
        db_connection: Database connection pool
        rbac_manager: Optional RBAC manager for permission checking
        audit_logger: Optional audit logger
    """

    # Default to permissive if no RBAC manager provided
    if rbac_manager is None:
        rbac_manager = RBACManager()
        rbac_manager.add_custom_role("default", permissions=["*"])

    # Default to no-op audit logger if not provided
    if audit_logger is None:
        audit_logger = AuditLogger(enable_console=False, enable_file=False)

    @mcp.tool(
        description="Create a new database user or role",
        params={
            "username": Field(None, description="Username for CREATE USER"),
            "role_name": Field(None, description="Role name for CREATE ROLE"),
            "password": Field(None, description="Password for the user"),
            "createdb": Field(None, description="Can create databases"),
            "createrole": Field(None, description="Can create roles"),
            "login": Field(None, description="Can login (default True for users)"),
            "superuser": Field(None, description="Superuser privilege"),
            "connection_limit": Field(None, description="Connection limit (-1 for unlimited)"),
            "valid_until": Field(None, description="Expiration timestamp"),
            "in_role": Field(None, description="Roles to be a member of"),
            "skip_password_validation": Field(False, description="Skip password strength validation")
        }
    )
    async def create_user(
        username: Optional[str] = None,
        role_name: Optional[str] = None,
        password: Optional[str] = None,
        createdb: Optional[bool] = None,
        createrole: Optional[bool] = None,
        login: Optional[bool] = None,
        superuser: Optional[bool] = None,
        connection_limit: Optional[int] = None,
        valid_until: Optional[str] = None,
        in_role: Optional[List[str]] = None,
        skip_password_validation: bool = False
    ) -> ResponseType:
        """Create a new database user or role."""
        try:
            operation = CreateUserOperation(
                connection_pool=db_connection,
                rbac_manager=rbac_manager,
                audit_logger=audit_logger
            )

            # Collect all provided arguments
            kwargs = {
                k: v for k, v in locals().items()
                if k not in ["operation"] and v is not None
            }

            # Use a default user and role for now
            # In production, these would come from authentication
            result = await operation.run(
                user="system",
                role="default",
                **kwargs
            )

            return format_operation_result(result)

        except Exception as e:
            return format_error_response(str(e))

    @mcp.tool(
        description="Alter an existing database user",
        params={
            "username": Field(..., description="Username to alter"),
            "rename_to": Field(None, description="New name for the user"),
            "password": Field(None, description="New password"),
            "createdb": Field(None, description="Can create databases"),
            "createrole": Field(None, description="Can create roles"),
            "login": Field(None, description="Can login"),
            "superuser": Field(None, description="Superuser privilege"),
            "connection_limit": Field(None, description="Connection limit"),
            "valid_until": Field(None, description="Expiration timestamp"),
            "reset_password": Field(False, description="Remove password"),
            "reset_config": Field(False, description="Reset configuration to defaults")
        }
    )
    async def alter_user(
        username: str,
        rename_to: Optional[str] = None,
        password: Optional[str] = None,
        createdb: Optional[bool] = None,
        createrole: Optional[bool] = None,
        login: Optional[bool] = None,
        superuser: Optional[bool] = None,
        connection_limit: Optional[int] = None,
        valid_until: Optional[str] = None,
        reset_password: bool = False,
        reset_config: bool = False
    ) -> ResponseType:
        """Alter an existing database user."""
        try:
            operation = AlterUserOperation(
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
        description="Drop a database user or role",
        params={
            "username": Field(..., description="Username to drop"),
            "cascade": Field(False, description="Drop owned objects (CASCADE)"),
            "reassign_to": Field(None, description="User to reassign ownership to"),
            "if_exists": Field(False, description="Don't error if user doesn't exist"),
            "force": Field(False, description="Terminate active connections before dropping")
        }
    )
    async def drop_user(
        username: str,
        cascade: bool = False,
        reassign_to: Optional[str] = None,
        if_exists: bool = False,
        force: bool = False
    ) -> ResponseType:
        """Drop a database user or role."""
        try:
            operation = DropUserOperation(
                connection_pool=db_connection,
                rbac_manager=rbac_manager,
                audit_logger=audit_logger
            )

            result = await operation.run(
                user="system",
                role="default",
                username=username,
                cascade=cascade,
                reassign_to=reassign_to,
                if_exists=if_exists,
                force=force
            )

            return format_operation_result(result)

        except Exception as e:
            return format_error_response(str(e))

    @mcp.tool(
        description="Grant or revoke database permissions",
        params={
            "operation": Field(..., description="GRANT or REVOKE"),
            "privileges": Field(..., description="List of privileges (e.g., SELECT, INSERT, UPDATE)"),
            "object_type": Field(..., description="Type of object (TABLE, SCHEMA, DATABASE, etc.)"),
            "object_name": Field(..., description="Name of the object"),
            "schema_name": Field(None, description="Schema name for schema-qualified objects"),
            "username": Field(None, description="User to grant/revoke privileges to/from"),
            "role_name": Field(None, description="Role to grant/revoke privileges to/from"),
            "with_grant_option": Field(False, description="Allow grantee to grant to others"),
            "cascade": Field(False, description="CASCADE for REVOKE"),
            "all_tables": Field(False, description="Apply to all tables in schema"),
            "all_sequences": Field(False, description="Apply to all sequences in schema"),
            "all_functions": Field(False, description="Apply to all functions in schema")
        }
    )
    async def manage_permissions(
        operation: str,
        privileges: List[str],
        object_type: str,
        object_name: str,
        schema_name: Optional[str] = None,
        username: Optional[str] = None,
        role_name: Optional[str] = None,
        with_grant_option: bool = False,
        cascade: bool = False,
        all_tables: bool = False,
        all_sequences: bool = False,
        all_functions: bool = False
    ) -> ResponseType:
        """Grant or revoke database permissions."""
        try:
            op = ManagePermissionsOperation(
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
        description="List database users and roles",
        params={
            "filter_type": Field("all", description="Filter by type: users, roles, or all"),
            "include_permissions": Field(False, description="Include permission details"),
            "include_memberships": Field(False, description="Include role memberships"),
            "include_settings": Field(False, description="Include user configuration settings"),
            "include_owned_objects": Field(False, description="Include count of owned objects"),
            "pattern": Field(None, description="Filter by name pattern (SQL LIKE pattern)"),
            "show_system": Field(False, description="Include system users/roles")
        }
    )
    async def list_users(
        filter_type: str = "all",
        include_permissions: bool = False,
        include_memberships: bool = False,
        include_settings: bool = False,
        include_owned_objects: bool = False,
        pattern: Optional[str] = None,
        show_system: bool = False
    ) -> ResponseType:
        """List database users and roles."""
        try:
            operation = ListUsersOperation(
                connection_pool=db_connection,
                rbac_manager=rbac_manager,
                audit_logger=audit_logger
            )

            result = await operation.run(
                user="system",
                role="default",
                filter_type=filter_type,
                include_permissions=include_permissions,
                include_memberships=include_memberships,
                include_settings=include_settings,
                include_owned_objects=include_owned_objects,
                pattern=pattern,
                show_system=show_system
            )

            if result.success:
                # Format the user list nicely
                details = result.details
                users = details.get("users", [])
                summary = details.get("summary", {})

                response_lines = [
                    f"Found {summary.get('total', 0)} users/roles:",
                    f"  Users: {summary.get('users', 0)}",
                    f"  Roles: {summary.get('roles', 0)}",
                    f"  Superusers: {summary.get('superusers', 0)}",
                    f"  Active connections: {summary.get('active_connections', 0)}",
                    "",
                ]

                for user in users:
                    response_lines.append(f"- {user['name']} ({user['type']})")
                    if user.get("is_superuser"):
                        response_lines.append("    Superuser: Yes")
                    if user.get("active_connections", 0) > 0:
                        response_lines.append(f"    Active connections: {user['active_connections']}")
                    if user.get("member_of"):
                        response_lines.append(f"    Member of: {', '.join(user['member_of'])}")
                    if user.get("permissions"):
                        response_lines.append("    Permissions:")
                        for perm_type, perms in user["permissions"].items():
                            if perms:
                                response_lines.append(f"      {perm_type}: {len(perms)} entries")
                    response_lines.append("")

                return format_text_response("\n".join(response_lines))
            else:
                return format_operation_result(result)

        except Exception as e:
            return format_error_response(str(e))