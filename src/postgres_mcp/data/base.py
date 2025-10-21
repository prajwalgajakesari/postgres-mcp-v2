"""Base classes for data import/export operations."""

import asyncio
import csv
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union, AsyncGenerator, Tuple
from pathlib import Path
from io import StringIO
from dataclasses import dataclass
from enum import Enum

from ..permissions.rbac import RBACManager, Permission
from ..common_utils import SimpleAuditLogger

logger = logging.getLogger(__name__)


class DataFormat(str, Enum):
    """Supported data formats for import/export."""

    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
    TSV = "tsv"
    BINARY = "binary"
    TEXT = "text"


@dataclass
class DataOperationResult:
    """Result of a data operation."""

    success: bool
    message: str
    rows_affected: int = 0
    file_path: Optional[str] = None
    format: Optional[DataFormat] = None
    metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "rows_affected": self.rows_affected,
            "file_path": self.file_path,
            "format": self.format.value if self.format else None,
            "metadata": self.metadata or {}
        }


class FormatHandler(ABC):
    """Abstract base class for format-specific handlers."""

    def __init__(self, format_type: DataFormat):
        self.format_type = format_type

    @abstractmethod
    async def parse_data(self, content: str) -> List[Dict[str, Any]]:
        """Parse data from string content."""
        pass

    @abstractmethod
    async def format_data(self, data: List[Dict[str, Any]]) -> str:
        """Format data to string content."""
        pass

    @abstractmethod
    def get_copy_options(self) -> Dict[str, str]:
        """Get PostgreSQL COPY command options for this format."""
        pass


class CSVHandler(FormatHandler):
    """Handler for CSV format."""

    def __init__(self, delimiter: str = ",", quote_char: str = '"', has_header: bool = True):
        super().__init__(DataFormat.CSV)
        self.delimiter = delimiter
        self.quote_char = quote_char
        self.has_header = has_header

    async def parse_data(self, content: str) -> List[Dict[str, Any]]:
        """Parse CSV content into list of dictionaries."""
        reader = csv.DictReader(
            StringIO(content),
            delimiter=self.delimiter,
            quotechar=self.quote_char
        )
        return [dict(row) for row in reader]

    async def format_data(self, data: List[Dict[str, Any]]) -> str:
        """Format data as CSV."""
        if not data:
            return ""

        output = StringIO()
        fieldnames = data[0].keys()
        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames,
            delimiter=self.delimiter,
            quotechar=self.quote_char,
            quoting=csv.QUOTE_MINIMAL
        )

        if self.has_header:
            writer.writeheader()

        for row in data:
            writer.writerow(row)

        return output.getvalue()

    def get_copy_options(self) -> Dict[str, str]:
        """Get COPY options for CSV format."""
        options = {
            "FORMAT": "CSV",
            "DELIMITER": f"'{self.delimiter}'",
            "QUOTE": f"'{self.quote_char}'"
        }
        if self.has_header:
            options["HEADER"] = "true"
        return options


class JSONHandler(FormatHandler):
    """Handler for JSON format."""

    def __init__(self):
        super().__init__(DataFormat.JSON)

    async def parse_data(self, content: str) -> List[Dict[str, Any]]:
        """Parse JSON content."""
        data = json.loads(content)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        else:
            raise ValueError("JSON must contain object or array of objects")

    async def format_data(self, data: List[Dict[str, Any]]) -> str:
        """Format data as JSON."""
        return json.dumps(data, indent=2, default=str)

    def get_copy_options(self) -> Dict[str, str]:
        """Get COPY options for JSON format."""
        return {"FORMAT": "TEXT"}


class JSONLHandler(FormatHandler):
    """Handler for JSON Lines format."""

    def __init__(self):
        super().__init__(DataFormat.JSONL)

    async def parse_data(self, content: str) -> List[Dict[str, Any]]:
        """Parse JSONL content."""
        lines = content.strip().split('\n')
        return [json.loads(line) for line in lines if line.strip()]

    async def format_data(self, data: List[Dict[str, Any]]) -> str:
        """Format data as JSONL."""
        return '\n'.join(json.dumps(row, default=str) for row in data)

    def get_copy_options(self) -> Dict[str, str]:
        """Get COPY options for JSONL format."""
        return {"FORMAT": "TEXT"}


class TSVHandler(CSVHandler):
    """Handler for TSV (Tab-Separated Values) format."""

    def __init__(self, has_header: bool = True):
        super().__init__(delimiter="\t", quote_char='"', has_header=has_header)
        self.format_type = DataFormat.TSV


class DataOperation(ABC):
    """Base class for all data operations."""

    def __init__(self, connection_pool, rbac_manager: RBACManager, audit_logger: SimpleAuditLogger):
        self.connection_pool = connection_pool
        self.rbac_manager = rbac_manager
        self.audit_logger = audit_logger
        self.format_handlers = self._initialize_format_handlers()

    def _initialize_format_handlers(self) -> Dict[DataFormat, FormatHandler]:
        """Initialize format handlers."""
        return {
            DataFormat.CSV: CSVHandler(),
            DataFormat.JSON: JSONHandler(),
            DataFormat.JSONL: JSONLHandler(),
            DataFormat.TSV: TSVHandler(),
        }

    def get_format_handler(self, format_type: DataFormat) -> FormatHandler:
        """Get handler for specified format."""
        if format_type not in self.format_handlers:
            raise ValueError(f"Unsupported format: {format_type}")
        return self.format_handlers[format_type]

    async def validate_table_exists(self, connection, table_name: str, schema_name: str = "public") -> bool:
        """Validate that table exists."""
        query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )
        """
        result = await connection.fetchval(query, schema_name, table_name)
        return bool(result)

    async def get_table_columns(self, connection, table_name: str, schema_name: str = "public") -> List[Tuple[str, str]]:
        """Get table column names and types."""
        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        """
        rows = await connection.fetch(query, schema_name, table_name)
        return [(row['column_name'], row['data_type']) for row in rows]

    async def validate_file_access(self, file_path: str, operation: str = "read") -> None:
        """Validate file access permissions."""
        path = Path(file_path)

        if operation == "read":
            if not path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            if not path.is_file():
                raise ValueError(f"Path is not a file: {file_path}")
            if not path.stat().st_size > 0:
                logger.warning(f"File is empty: {file_path}")

        elif operation == "write":
            if path.exists() and not path.is_file():
                raise ValueError(f"Path exists but is not a file: {file_path}")

            parent_dir = path.parent
            if not parent_dir.exists():
                try:
                    parent_dir.mkdir(parents=True, exist_ok=True)
                except PermissionError:
                    raise PermissionError(f"Cannot create directory: {parent_dir}")

    def _validate_permissions(self, role_name: str, required_permissions: List[Permission]) -> None:
        """Validate user has required permissions."""
        for permission in required_permissions:
            if not self.rbac_manager.check_permission(role_name, permission):
                raise PermissionError(f"Missing permission: {permission.value}")

    def _quote_identifier(self, identifier: str) -> str:
        """Quote SQL identifier to prevent injection."""
        return f'"{identifier.replace('"', '""')}"'

    def _validate_identifier(self, identifier: str, max_length: int = 63) -> None:
        """Validate SQL identifier."""
        if not identifier or not identifier.replace('_', '').replace('-', '').isalnum():
            raise ValueError(f"Invalid identifier: {identifier}")
        if len(identifier) > max_length:
            raise ValueError(f"Identifier too long (max {max_length}): {identifier}")

    @abstractmethod
    async def execute(self, **kwargs) -> DataOperationResult:
        """Execute the data operation."""
        pass