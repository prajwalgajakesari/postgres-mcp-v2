"""Data management operations for PostgreSQL MCP Server."""

from .base import DataOperation, FormatHandler, DataOperationResult, DataFormat
from .copy import CopyDataOperation
from .export import ExportQueryOperation

# Import from module with Python keyword name requires special syntax
from importlib import import_module
_import_module = import_module('.import', package='postgres_mcp.data')
ImportDataOperation = _import_module.ImportDataOperation

__all__ = [
    "DataOperation",
    "FormatHandler",
    "DataOperationResult",
    "DataFormat",
    "CopyDataOperation",
    "ExportQueryOperation",
    "ImportDataOperation",
]