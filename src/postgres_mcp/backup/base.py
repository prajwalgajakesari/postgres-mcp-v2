"""Base classes for backup and restore operations."""

import asyncio
import logging
import subprocess
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
from datetime import datetime
import json
import shutil

from ..permissions.rbac import RBACManager, Permission
from ..common_utils import SimpleAuditLogger

logger = logging.getLogger(__name__)


class BackupFormat(str, Enum):
    """Supported backup formats."""

    CUSTOM = "custom"        # pg_dump -Fc (compressed binary)
    TAR = "tar"             # pg_dump -Ft (tar archive)
    PLAIN = "plain"         # pg_dump -Fp (plain SQL)
    DIRECTORY = "directory" # pg_dump -Fd (directory format)


class BackupScope(str, Enum):
    """Backup scope options."""

    DATABASE = "database"   # Single database
    SCHEMA = "schema"       # Single schema
    TABLE = "table"         # Single table
    GLOBALS = "globals"     # Global objects (roles, tablespaces)


@dataclass
class BackupMetadata:
    """Metadata for backup operations."""

    backup_id: str
    timestamp: datetime
    database_name: str
    backup_format: BackupFormat
    backup_scope: BackupScope
    file_path: str
    file_size: Optional[int] = None
    compression: Optional[str] = None
    pg_version: Optional[str] = None
    description: Optional[str] = None
    includes: Optional[List[str]] = None
    excludes: Optional[List[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary."""
        return {
            "backup_id": self.backup_id,
            "timestamp": self.timestamp.isoformat(),
            "database_name": self.database_name,
            "backup_format": self.backup_format.value,
            "backup_scope": self.backup_scope.value,
            "file_path": self.file_path,
            "file_size": self.file_size,
            "compression": self.compression,
            "pg_version": self.pg_version,
            "description": self.description,
            "includes": self.includes,
            "excludes": self.excludes
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BackupMetadata':
        """Create metadata from dictionary."""
        return cls(
            backup_id=data["backup_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            database_name=data["database_name"],
            backup_format=BackupFormat(data["backup_format"]),
            backup_scope=BackupScope(data["backup_scope"]),
            file_path=data["file_path"],
            file_size=data.get("file_size"),
            compression=data.get("compression"),
            pg_version=data.get("pg_version"),
            description=data.get("description"),
            includes=data.get("includes"),
            excludes=data.get("excludes")
        )


@dataclass
class BackupOperationResult:
    """Result of a backup operation."""

    success: bool
    message: str
    backup_metadata: Optional[BackupMetadata] = None
    duration_seconds: Optional[float] = None
    warnings: Optional[List[str]] = None
    errors: Optional[List[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "backup_metadata": self.backup_metadata.to_dict() if self.backup_metadata else None,
            "duration_seconds": self.duration_seconds,
            "warnings": self.warnings or [],
            "errors": self.errors or []
        }


class BackupOperation(ABC):
    """Base class for backup operations."""

    def __init__(self, connection_pool, rbac_manager: RBACManager, audit_logger: SimpleAuditLogger):
        self.connection_pool = connection_pool
        self.rbac_manager = rbac_manager
        self.audit_logger = audit_logger

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

    async def _check_pg_tools(self, tool_name: str) -> bool:
        """Check if PostgreSQL command-line tool is available."""
        try:
            result = await asyncio.create_subprocess_exec(
                tool_name, "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await result.communicate()
            return result.returncode == 0
        except FileNotFoundError:
            return False

    async def _get_database_version(self, connection) -> str:
        """Get PostgreSQL version."""
        try:
            version_result = await connection.fetchval("SELECT version()")
            return version_result.split()[1]
        except Exception:
            return "unknown"

    async def _get_database_name(self, connection) -> str:
        """Get current database name."""
        try:
            return await connection.fetchval("SELECT current_database()")
        except Exception:
            return "unknown"

    def _build_pg_dump_command(self,
                             database_url: str,
                             output_path: str,
                             backup_format: BackupFormat,
                             options: Optional[Dict[str, Any]] = None) -> List[str]:
        """Build pg_dump command with options."""
        cmd = ["pg_dump"]

        # Format options
        format_map = {
            BackupFormat.CUSTOM: "-Fc",
            BackupFormat.TAR: "-Ft",
            BackupFormat.PLAIN: "-Fp",
            BackupFormat.DIRECTORY: "-Fd"
        }
        cmd.append(format_map[backup_format])

        # Output
        if backup_format == BackupFormat.DIRECTORY:
            cmd.extend(["-f", output_path])
        else:
            cmd.extend(["-f", output_path])

        # Additional options
        if options:
            if options.get("verbose"):
                cmd.append("-v")

            if options.get("compress_level") and backup_format in [BackupFormat.CUSTOM, BackupFormat.TAR]:
                cmd.extend(["-Z", str(options["compress_level"])])

            if options.get("schema_only"):
                cmd.append("-s")

            if options.get("data_only"):
                cmd.append("-a")

            if options.get("no_owner"):
                cmd.append("-O")

            if options.get("no_privileges"):
                cmd.append("-x")

            if options.get("clean"):
                cmd.append("-c")

            if options.get("create"):
                cmd.append("-C")

            if options.get("if_exists"):
                cmd.append("--if-exists")

            # Include/exclude options
            for schema in options.get("include_schemas", []):
                cmd.extend(["-n", schema])

            for schema in options.get("exclude_schemas", []):
                cmd.extend(["-N", schema])

            for table in options.get("include_tables", []):
                cmd.extend(["-t", table])

            for table in options.get("exclude_tables", []):
                cmd.extend(["-T", table])

        # Database URL
        cmd.append(database_url)

        return cmd

    def _build_pg_restore_command(self,
                                backup_path: str,
                                database_url: str,
                                options: Optional[Dict[str, Any]] = None) -> List[str]:
        """Build pg_restore command with options."""
        cmd = ["pg_restore"]

        # Additional options
        if options:
            if options.get("verbose"):
                cmd.append("-v")

            if options.get("clean"):
                cmd.append("-c")

            if options.get("create"):
                cmd.append("-C")

            if options.get("if_exists"):
                cmd.append("--if-exists")

            if options.get("no_owner"):
                cmd.append("-O")

            if options.get("no_privileges"):
                cmd.append("-x")

            if options.get("single_transaction"):
                cmd.append("-1")

            if options.get("exit_on_error"):
                cmd.append("-e")

            if options.get("jobs") and options["jobs"] > 1:
                cmd.extend(["-j", str(options["jobs"])])

            # Schema/table selection
            for schema in options.get("include_schemas", []):
                cmd.extend(["-n", schema])

            for table in options.get("include_tables", []):
                cmd.extend(["-t", table])

        # Database connection
        cmd.extend(["-d", database_url])

        # Backup file
        cmd.append(backup_path)

        return cmd

    async def _execute_command(self, cmd: List[str], timeout: int = 3600) -> tuple[bool, str, str]:
        """Execute command with timeout."""
        try:
            logger.info(f"Executing command: {' '.join(cmd[:3])}...")  # Don't log full command with credentials

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
                return_code = process.returncode

                stdout_str = stdout.decode('utf-8') if stdout else ""
                stderr_str = stderr.decode('utf-8') if stderr else ""

                return return_code == 0, stdout_str, stderr_str

            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                return False, "", f"Command timed out after {timeout} seconds"

        except Exception as e:
            logger.error(f"Error executing command: {e}")
            return False, "", str(e)

    def _validate_file_path(self, file_path: str, operation: str = "write") -> None:
        """Validate file path for backup operations."""
        path = Path(file_path)

        if operation == "write":
            # Check parent directory exists or can be created
            parent_dir = path.parent
            if not parent_dir.exists():
                try:
                    parent_dir.mkdir(parents=True, exist_ok=True)
                except PermissionError:
                    raise PermissionError(f"Cannot create directory: {parent_dir}")

            # Check if file already exists
            if path.exists() and not path.is_file():
                raise ValueError(f"Path exists but is not a file: {file_path}")

        elif operation == "read":
            if not path.exists():
                raise FileNotFoundError(f"Backup file not found: {file_path}")
            if not path.is_file():
                raise ValueError(f"Path is not a file: {file_path}")

    def _generate_backup_id(self) -> str:
        """Generate unique backup ID."""
        import uuid
        return f"backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

    @abstractmethod
    async def execute(self, **kwargs) -> BackupOperationResult:
        """Execute the backup operation."""
        pass