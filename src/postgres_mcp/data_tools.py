"""MCP tools for data import/export operations."""

import logging
from typing import Any, Dict, List, Optional

from mcp.types import Tool, TextContent
import mcp.types as types

from .data import CopyDataOperation, ExportQueryOperation, ImportDataOperation, DataFormat
from .permissions.rbac import RBACManager
from .common_utils import create_default_rbac_manager, SimpleAuditLogger

logger = logging.getLogger(__name__)


def create_data_tools(connection_pool) -> List[Tool]:
    """Create data management MCP tools.

    Args:
        connection_pool: Database connection pool

    Returns:
        List of MCP tools for data operations
    """
    return [
        Tool(
            name="copy_data",
            description="Copy data using PostgreSQL COPY command - between files and tables or between tables",
            inputSchema={
                "type": "object",
                "properties": {
                    "operation_type": {
                        "type": "string",
                        "enum": ["to_file", "from_file", "between_tables"],
                        "description": "Type of copy operation to perform"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Name of the source/target table"
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Path to the file (for to_file and from_file operations)"
                    },
                    "source_table": {
                        "type": "string",
                        "description": "Source table name (for between_tables operation)"
                    },
                    "target_table": {
                        "type": "string",
                        "description": "Target table name (for between_tables operation)"
                    },
                    "role_name": {
                        "type": "string",
                        "default": "default",
                        "description": "Role name for permission checking"
                    },
                    "schema_name": {
                        "type": "string",
                        "default": "public",
                        "description": "Schema name"
                    },
                    "source_schema": {
                        "type": "string",
                        "default": "public",
                        "description": "Source schema name (for between_tables operation)"
                    },
                    "target_schema": {
                        "type": "string",
                        "default": "public",
                        "description": "Target schema name (for between_tables operation)"
                    },
                    "format_type": {
                        "type": "string",
                        "enum": ["csv", "json", "jsonl", "tsv", "binary", "text"],
                        "default": "csv",
                        "description": "Data format"
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of specific columns to copy"
                    },
                    "where_clause": {
                        "type": "string",
                        "description": "Optional WHERE clause for filtering data"
                    },
                    "column_mapping": {
                        "type": "object",
                        "description": "Optional mapping from source to target column names"
                    },
                    "format_options": {
                        "type": "object",
                        "description": "Optional format-specific options"
                    },
                    "on_conflict": {
                        "type": "string",
                        "enum": ["error", "ignore", "update"],
                        "default": "error",
                        "description": "How to handle conflicts during import"
                    }
                },
                "required": ["operation_type"]
            }
        ),

        Tool(
            name="export_data",
            description="Export data from PostgreSQL to various formats",
            inputSchema={
                "type": "object",
                "properties": {
                    "operation_type": {
                        "type": "string",
                        "enum": ["query", "table", "schema_structure"],
                        "description": "Type of export operation"
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Output file path"
                    },
                    "query": {
                        "type": "string",
                        "description": "SQL query to export (for query operation)"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Table name to export (for table operation)"
                    },
                    "role_name": {
                        "type": "string",
                        "default": "default",
                        "description": "Role name for permission checking"
                    },
                    "schema_name": {
                        "type": "string",
                        "default": "public",
                        "description": "Schema name"
                    },
                    "format_type": {
                        "type": "string",
                        "enum": ["csv", "json", "jsonl", "tsv"],
                        "default": "csv",
                        "description": "Export format"
                    },
                    "parameters": {
                        "type": "array",
                        "description": "Optional query parameters"
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of specific columns to export"
                    },
                    "where_clause": {
                        "type": "string",
                        "description": "Optional WHERE clause for table export"
                    },
                    "order_by": {
                        "type": "string",
                        "description": "Optional ORDER BY clause for table export"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Optional row limit for table export"
                    },
                    "batch_size": {
                        "type": "integer",
                        "default": 1000,
                        "description": "Number of rows to process at once"
                    },
                    "include_metadata": {
                        "type": "boolean",
                        "default": true,
                        "description": "Whether to include metadata in output"
                    },
                    "include_data": {
                        "type": "boolean",
                        "default": false,
                        "description": "Whether to include sample data (schema_structure operation)"
                    },
                    "include_constraints": {
                        "type": "boolean",
                        "default": true,
                        "description": "Whether to include constraints (schema_structure operation)"
                    },
                    "include_indexes": {
                        "type": "boolean",
                        "default": true,
                        "description": "Whether to include indexes (schema_structure operation)"
                    }
                },
                "required": ["operation_type", "file_path"]
            }
        ),

        Tool(
            name="import_data",
            description="Import data into PostgreSQL from various sources and formats",
            inputSchema={
                "type": "object",
                "properties": {
                    "operation_type": {
                        "type": "string",
                        "enum": ["from_file", "from_json_schema", "with_validation"],
                        "description": "Type of import operation"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Target table name"
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Input file path"
                    },
                    "json_file_path": {
                        "type": "string",
                        "description": "JSON file path (for from_json_schema operation)"
                    },
                    "role_name": {
                        "type": "string",
                        "default": "default",
                        "description": "Role name for permission checking"
                    },
                    "schema_name": {
                        "type": "string",
                        "default": "public",
                        "description": "Target schema name"
                    },
                    "format_type": {
                        "type": "string",
                        "enum": ["csv", "json", "jsonl", "tsv"],
                        "default": "csv",
                        "description": "Input file format"
                    },
                    "column_mapping": {
                        "type": "object",
                        "description": "Optional mapping from file columns to table columns"
                    },
                    "create_table": {
                        "type": "boolean",
                        "default": false,
                        "description": "Whether to auto-create the table"
                    },
                    "truncate_first": {
                        "type": "boolean",
                        "default": false,
                        "description": "Whether to truncate table before import"
                    },
                    "batch_size": {
                        "type": "integer",
                        "default": 1000,
                        "description": "Number of rows to insert at once"
                    },
                    "on_error": {
                        "type": "string",
                        "enum": ["abort", "skip", "log"],
                        "default": "abort",
                        "description": "How to handle errors during import"
                    },
                    "data_path": {
                        "type": "string",
                        "default": "",
                        "description": "JSONPath to extract data (for from_json_schema operation)"
                    },
                    "flatten_nested": {
                        "type": "boolean",
                        "default": true,
                        "description": "Whether to flatten nested objects (for from_json_schema operation)"
                    },
                    "max_depth": {
                        "type": "integer",
                        "default": 3,
                        "description": "Maximum nesting depth to flatten"
                    },
                    "validation_rules": {
                        "type": "object",
                        "description": "Column validation rules (for with_validation operation)"
                    },
                    "reject_file": {
                        "type": "string",
                        "description": "Optional file path for rejected rows"
                    }
                },
                "required": ["operation_type", "table_name"]
            }
        )
    ]


async def handle_copy_data(arguments: Dict[str, Any], connection_pool) -> List[types.TextContent]:
    """Handle copy_data tool calls."""
    try:
        rbac_manager = create_default_rbac_manager()
        audit_logger = SimpleAuditLogger()

        copy_op = CopyDataOperation(connection_pool, rbac_manager, audit_logger)

        operation_type = arguments["operation_type"]

        # Convert format string to enum
        format_str = arguments.get("format_type", "csv")
        format_type = DataFormat(format_str)
        arguments["format_type"] = format_type

        result = await copy_op.execute(operation_type, **arguments)

        return [
            TextContent(
                type="text",
                text=f"Copy operation {'completed successfully' if result.success else 'failed'}: {result.message}\n\n"
                     f"Rows affected: {result.rows_affected}\n"
                     f"Details: {result.to_dict()}"
            )
        ]

    except Exception as e:
        logger.error(f"Error in copy_data tool: {e}")
        return [
            TextContent(
                type="text",
                text=f"Error executing copy operation: {str(e)}"
            )
        ]


async def handle_export_data(arguments: Dict[str, Any], connection_pool) -> List[types.TextContent]:
    """Handle export_data tool calls."""
    try:
        rbac_manager = create_default_rbac_manager()
        audit_logger = SimpleAuditLogger()

        export_op = ExportQueryOperation(connection_pool, rbac_manager, audit_logger)

        operation_type = arguments["operation_type"]

        # Convert format string to enum
        format_str = arguments.get("format_type", "csv")
        format_type = DataFormat(format_str)
        arguments["format_type"] = format_type

        result = await export_op.execute(operation_type, **arguments)

        return [
            TextContent(
                type="text",
                text=f"Export operation {'completed successfully' if result.success else 'failed'}: {result.message}\n\n"
                     f"Rows exported: {result.rows_affected}\n"
                     f"Output file: {result.file_path}\n"
                     f"Format: {result.format.value if result.format else 'N/A'}\n"
                     f"Details: {result.to_dict()}"
            )
        ]

    except Exception as e:
        logger.error(f"Error in export_data tool: {e}")
        return [
            TextContent(
                type="text",
                text=f"Error executing export operation: {str(e)}"
            )
        ]


async def handle_import_data(arguments: Dict[str, Any], connection_pool) -> List[types.TextContent]:
    """Handle import_data tool calls."""
    try:
        rbac_manager = create_default_rbac_manager()
        audit_logger = SimpleAuditLogger()

        import_op = ImportDataOperation(connection_pool, rbac_manager, audit_logger)

        operation_type = arguments["operation_type"]

        # Convert format string to enum if present
        if "format_type" in arguments:
            format_str = arguments["format_type"]
            format_type = DataFormat(format_str)
            arguments["format_type"] = format_type

        result = await import_op.execute(operation_type, **arguments)

        return [
            TextContent(
                type="text",
                text=f"Import operation {'completed successfully' if result.success else 'failed'}: {result.message}\n\n"
                     f"Rows imported: {result.rows_affected}\n"
                     f"Source file: {result.file_path or arguments.get('file_path', 'N/A')}\n"
                     f"Details: {result.to_dict()}"
            )
        ]

    except Exception as e:
        logger.error(f"Error in import_data tool: {e}")
        return [
            TextContent(
                type="text",
                text=f"Error executing import operation: {str(e)}"
            )
        ]