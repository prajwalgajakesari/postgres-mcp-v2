"""Function and procedure management tools for PostgreSQL MCP Server."""

import logging
from typing import Dict, Any, List
from mcp.types import Tool, TextContent

from .sql import DbConnPool
from .functions import ManageFunctionsOperation, ManageProceduresOperation, DebugFunctionOperation

logger = logging.getLogger(__name__)


def register_function_tools(mcp, db_connection: DbConnPool) -> None:
    """Register function and procedure management tools with the MCP server."""

    # Import common utilities
    from .common_utils import SimpleAuditLogger, create_default_rbac_manager
    rbac_manager = create_default_rbac_manager()

    @mcp.tool()
    async def manage_functions(
        operation: str,
        function_name: str,
        schema_name: str = "public",
        parameters: List[Dict[str, Any]] = None,
        return_type: str = "void",
        language: str = "plpgsql",
        body: str = "",
        or_replace: bool = False,
        volatility: str = "VOLATILE",
        strict: bool = False,
        security_definer: bool = False,
        cost: int = None,
        rows: int = None,
        comment: str = "",
        if_exists: bool = False,
        cascade: bool = False,
        rename_to: str = "",
        owner_to: str = "",
        set_schema: str = ""
    ) -> List[TextContent]:
        """
        Manage database functions (CREATE, DROP, ALTER).

        Args:
            operation: Operation to perform (CREATE, DROP, ALTER)
            function_name: Name of the function
            schema_name: Schema name (default: public)
            parameters: Function parameters as list of dicts with keys: name, type, mode, default
            return_type: Return type for the function
            language: Function language (sql, plpgsql, c, etc.)
            body: Function body/implementation
            or_replace: Use CREATE OR REPLACE
            volatility: Function volatility (VOLATILE, STABLE, IMMUTABLE)
            strict: Function returns NULL on NULL input
            security_definer: Run with definer's privileges
            cost: Execution cost estimate
            rows: Estimated rows returned (for set-returning functions)
            comment: Function comment
            if_exists: Don't error if function doesn't exist (DROP)
            cascade: Drop dependent objects (DROP)
            rename_to: New name (ALTER)
            owner_to: New owner (ALTER)
            set_schema: Move to new schema (ALTER)
        """
        try:
            operation_obj = ManageFunctionsOperation(db_connection, rbac_manager, SimpleAuditLogger())
            result = await operation_obj.execute(
                operation=operation,
                function_name=function_name,
                schema_name=schema_name,
                parameters=parameters or [],
                return_type=return_type,
                language=language,
                body=body,
                or_replace=or_replace,
                volatility=volatility,
                strict=strict,
                security_definer=security_definer,
                cost=cost,
                rows=rows,
                comment=comment,
                if_exists=if_exists,
                cascade=cascade,
                rename_to=rename_to,
                owner_to=owner_to,
                set_schema=set_schema
            )

            if result.success:
                logger.info(f"Function operation completed: {result.message}")
                return [TextContent(type="text", text=f"✅ {result.message}\n\nDetails: {result.details}")]
            else:
                logger.error(f"Failed to manage function: {result.error}")
                return [TextContent(type="text", text=f"❌ Error: {result.error}\n\nDetails: {result.details}")]

        except Exception as e:
            logger.error(f"Error managing function: {str(e)}")
            return [TextContent(type="text", text=f"❌ Unexpected error: {str(e)}")]

    @mcp.tool()
    async def manage_procedures(
        operation: str,
        procedure_name: str,
        schema_name: str = "public",
        parameters: List[Dict[str, Any]] = None,
        language: str = "plpgsql",
        body: str = "",
        or_replace: bool = False,
        security_definer: bool = False,
        comment: str = "",
        if_exists: bool = False,
        cascade: bool = False,
        rename_to: str = "",
        owner_to: str = "",
        set_schema: str = ""
    ) -> List[TextContent]:
        """
        Manage database stored procedures (CREATE, DROP, ALTER).

        Args:
            operation: Operation to perform (CREATE, DROP, ALTER)
            procedure_name: Name of the procedure
            schema_name: Schema name (default: public)
            parameters: Procedure parameters as list of dicts with keys: name, type, mode, default
            language: Procedure language (sql, plpgsql, c, etc.)
            body: Procedure body/implementation
            or_replace: Use CREATE OR REPLACE
            security_definer: Run with definer's privileges
            comment: Procedure comment
            if_exists: Don't error if procedure doesn't exist (DROP)
            cascade: Drop dependent objects (DROP)
            rename_to: New name (ALTER)
            owner_to: New owner (ALTER)
            set_schema: Move to new schema (ALTER)
        """
        try:
            operation_obj = ManageProceduresOperation(db_connection, rbac_manager, SimpleAuditLogger())
            result = await operation_obj.execute(
                operation=operation,
                procedure_name=procedure_name,
                schema_name=schema_name,
                parameters=parameters or [],
                language=language,
                body=body,
                or_replace=or_replace,
                security_definer=security_definer,
                comment=comment,
                if_exists=if_exists,
                cascade=cascade,
                rename_to=rename_to,
                owner_to=owner_to,
                set_schema=set_schema
            )

            if result.success:
                logger.info(f"Procedure operation completed: {result.message}")
                return [TextContent(type="text", text=f"✅ {result.message}\n\nDetails: {result.details}")]
            else:
                logger.error(f"Failed to manage procedure: {result.error}")
                return [TextContent(type="text", text=f"❌ Error: {result.error}\n\nDetails: {result.details}")]

        except Exception as e:
            logger.error(f"Error managing procedure: {str(e)}")
            return [TextContent(type="text", text=f"❌ Unexpected error: {str(e)}")]

    @mcp.tool()
    async def debug_function(
        action: str,
        function_name: str,
        schema_name: str = "public",
        parameters: List[Dict[str, Any]] = None,
        iterations: int = 100
    ) -> List[TextContent]:
        """
        Debug and analyze database functions.

        Args:
            action: Debug action (ANALYZE, EXPLAIN, PROFILE, DEPENDENCIES)
            function_name: Name of the function to debug
            schema_name: Schema name (default: public)
            parameters: Function parameters for testing (list of dicts with value key)
            iterations: Number of iterations for profiling
        """
        try:
            operation = DebugFunctionOperation(db_connection, rbac_manager, SimpleAuditLogger())
            result = await operation.execute(
                action=action,
                function_name=function_name,
                schema_name=schema_name,
                parameters=parameters or [],
                iterations=iterations
            )

            if result.success:
                logger.info(f"Function debug completed: {result.message}")

                # Format the output based on action type
                if action.upper() == "ANALYZE":
                    details = result.details
                    output = f"✅ {result.message}\n\n"
                    output += f"**Function Information:**\n"
                    if func_info := details.get("function_info"):
                        output += f"- Language: {func_info.get('language', 'N/A')}\n"
                        output += f"- Volatility: {func_info.get('volatility', 'N/A')}\n"
                        output += f"- Security: {func_info.get('security_type', 'N/A')}\n"

                    if complexity := details.get("complexity"):
                        output += f"\n**Complexity Analysis:**\n"
                        output += f"- Lines of code: {complexity.get('code_lines', 0)}\n"
                        output += f"- Complexity score: {complexity.get('complexity_score', 0)}\n"
                        output += f"- Estimated complexity: {complexity.get('estimated_complexity', 'unknown')}\n"

                    if issues := details.get("issues"):
                        output += f"\n**Issues Found:**\n"
                        for issue in issues:
                            output += f"- {issue}\n"

                    return [TextContent(type="text", text=output)]

                elif action.upper() == "PROFILE":
                    details = result.details
                    performance = details.get("performance", {})
                    output = f"✅ {result.message}\n\n"
                    output += f"**Performance Profile:**\n"
                    output += f"- Iterations: {performance.get('iterations', 0)}\n"
                    output += f"- Average time: {performance.get('average_time_ms', 0)}ms\n"
                    output += f"- Min time: {performance.get('min_time_ms', 0)}ms\n"
                    output += f"- Max time: {performance.get('max_time_ms', 0)}ms\n"
                    output += f"- Total time: {performance.get('total_time_ms', 0)}ms\n"

                    return [TextContent(type="text", text=output)]

                elif action.upper() == "DEPENDENCIES":
                    details = result.details
                    output = f"✅ {result.message}\n\n"

                    if dependencies := details.get("dependencies"):
                        output += f"**Dependencies ({len(dependencies)}):**\n"
                        for dep in dependencies:
                            output += f"- {dep['object_type']}: {dep['schema']}.{dep['object_name']} ({dep['dependency_type']})\n"

                    if dependents := details.get("dependents"):
                        output += f"\n**Dependent Functions ({len(dependents)}):**\n"
                        for dep in dependents:
                            output += f"- {dep['schema']}.{dep['function_name']} ({dep['dependency_type']})\n"

                    return [TextContent(type="text", text=output)]

                else:
                    return [TextContent(type="text", text=f"✅ {result.message}\n\nDetails: {result.details}")]

            else:
                logger.error(f"Failed to debug function: {result.error}")
                return [TextContent(type="text", text=f"❌ Error: {result.error}\n\nDetails: {result.details}")]

        except Exception as e:
            logger.error(f"Error debugging function: {str(e)}")
            return [TextContent(type="text", text=f"❌ Unexpected error: {str(e)}")]

    # Register the tools with proper metadata
    mcp.tool(
        name="manage_functions",
        description="Comprehensive function management including CREATE, DROP, and ALTER operations with full PostgreSQL feature support"
    )(manage_functions)

    mcp.tool(
        name="manage_procedures",
        description="Complete stored procedure management including CREATE, DROP, and ALTER operations"
    )(manage_procedures)

    mcp.tool(
        name="debug_function",
        description="Debug and analyze functions with complexity analysis, performance profiling, and dependency tracking"
    )(debug_function)

    logger.info("Function and procedure management tools registered successfully")