"""Base classes for database management operations."""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

from ..permissions.rbac import RBACManager, Permission
from ..common_utils import SimpleAuditLogger

logger = logging.getLogger(__name__)


class DatabaseEncoding(str, Enum):
    """Supported database encodings."""

    UTF8 = "UTF8"
    LATIN1 = "LATIN1"
    WIN1252 = "WIN1252"
    SQL_ASCII = "SQL_ASCII"


class DatabaseTemplate(str, Enum):
    """Standard PostgreSQL template databases."""

    TEMPLATE0 = "template0"
    TEMPLATE1 = "template1"


@dataclass
class DatabaseInfo:
    """Information about a database."""

    name: str
    owner: str
    encoding: str
    collate: str
    ctype: str
    size_bytes: Optional[int] = None
    connection_limit: Optional[int] = None
    tablespace: Optional[str] = None
    template: Optional[str] = None
    created_at: Optional[datetime] = None
    last_stats_reset: Optional[datetime] = None
    num_backends: Optional[int] = None
    xact_commit: Optional[int] = None
    xact_rollback: Optional[int] = None
    blks_read: Optional[int] = None
    blks_hit: Optional[int] = None
    tup_returned: Optional[int] = None
    tup_fetched: Optional[int] = None
    tup_inserted: Optional[int] = None
    tup_updated: Optional[int] = None
    tup_deleted: Optional[int] = None
    conflicts: Optional[int] = None
    temp_files: Optional[int] = None
    temp_bytes: Optional[int] = None
    deadlocks: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert database info to dictionary."""
        return {
            "name": self.name,
            "owner": self.owner,
            "encoding": self.encoding,
            "collate": self.collate,
            "ctype": self.ctype,
            "size_bytes": self.size_bytes,
            "connection_limit": self.connection_limit,
            "tablespace": self.tablespace,
            "template": self.template,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "last_stats_reset": self.last_stats_reset.isoformat() if self.last_stats_reset else None,
            "num_backends": self.num_backends,
            "statistics": {
                "transactions": {
                    "committed": self.xact_commit,
                    "rolled_back": self.xact_rollback
                },
                "blocks": {
                    "read": self.blks_read,
                    "hit": self.blks_hit
                },
                "tuples": {
                    "returned": self.tup_returned,
                    "fetched": self.tup_fetched,
                    "inserted": self.tup_inserted,
                    "updated": self.tup_updated,
                    "deleted": self.tup_deleted
                },
                "conflicts": self.conflicts,
                "temp_files": self.temp_files,
                "temp_bytes": self.temp_bytes,
                "deadlocks": self.deadlocks
            }
        }


@dataclass
class DatabaseOperationResult:
    """Result of a database operation."""

    success: bool
    message: str
    database_info: Optional[DatabaseInfo] = None
    affected_databases: Optional[List[str]] = None
    duration_seconds: Optional[float] = None
    warnings: Optional[List[str]] = None
    errors: Optional[List[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "database_info": self.database_info.to_dict() if self.database_info else None,
            "affected_databases": self.affected_databases or [],
            "duration_seconds": self.duration_seconds,
            "warnings": self.warnings or [],
            "errors": self.errors or []
        }


class DatabaseOperation(ABC):
    """Base class for database operations."""

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

    async def _database_exists(self, connection, database_name: str) -> bool:
        """Check if database exists."""
        query = "SELECT 1 FROM pg_database WHERE datname = $1"
        result = await connection.fetchval(query, database_name)
        return bool(result)

    async def _get_database_info(self, connection, database_name: str) -> Optional[DatabaseInfo]:
        """Get detailed information about a database."""
        try:
            # Basic database information
            basic_query = """
                SELECT
                    d.datname as name,
                    pg_catalog.pg_get_userbyid(d.datdba) as owner,
                    pg_catalog.pg_encoding_to_char(d.encoding) as encoding,
                    d.datcollate as collate,
                    d.datctype as ctype,
                    d.datconnlimit as connection_limit,
                    t.spcname as tablespace,
                    CASE
                        WHEN d.datistemplate THEN 'template'
                        ELSE NULL
                    END as template
                FROM pg_catalog.pg_database d
                LEFT JOIN pg_catalog.pg_tablespace t ON d.dattablespace = t.oid
                WHERE d.datname = $1
            """

            basic_info = await connection.fetchrow(basic_query, database_name)
            if not basic_info:
                return None

            # Database size
            size_query = "SELECT pg_database_size($1)"
            size_bytes = await connection.fetchval(size_query, database_name)

            # Database statistics (if available)
            stats_query = """
                SELECT
                    numbackends,
                    xact_commit,
                    xact_rollback,
                    blks_read,
                    blks_hit,
                    tup_returned,
                    tup_fetched,
                    tup_inserted,
                    tup_updated,
                    tup_deleted,
                    conflicts,
                    temp_files,
                    temp_bytes,
                    deadlocks,
                    stats_reset
                FROM pg_stat_database
                WHERE datname = $1
            """

            stats = await connection.fetchrow(stats_query, database_name)

            return DatabaseInfo(
                name=basic_info['name'],
                owner=basic_info['owner'],
                encoding=basic_info['encoding'],
                collate=basic_info['collate'],
                ctype=basic_info['ctype'],
                size_bytes=size_bytes,
                connection_limit=basic_info['connection_limit'],
                tablespace=basic_info['tablespace'],
                template=basic_info['template'],
                num_backends=stats['numbackends'] if stats else None,
                xact_commit=stats['xact_commit'] if stats else None,
                xact_rollback=stats['xact_rollback'] if stats else None,
                blks_read=stats['blks_read'] if stats else None,
                blks_hit=stats['blks_hit'] if stats else None,
                tup_returned=stats['tup_returned'] if stats else None,
                tup_fetched=stats['tup_fetched'] if stats else None,
                tup_inserted=stats['tup_inserted'] if stats else None,
                tup_updated=stats['tup_updated'] if stats else None,
                tup_deleted=stats['tup_deleted'] if stats else None,
                conflicts=stats['conflicts'] if stats else None,
                temp_files=stats['temp_files'] if stats else None,
                temp_bytes=stats['temp_bytes'] if stats else None,
                deadlocks=stats['deadlocks'] if stats else None,
                last_stats_reset=stats['stats_reset'] if stats else None
            )

        except Exception as e:
            logger.warning(f"Failed to get database info: {e}")
            return None

    async def _terminate_database_connections(self, connection, database_name: str, exclude_current: bool = True) -> int:
        """Terminate all connections to a database."""
        try:
            # Get current backend PID to exclude it
            current_pid = await connection.fetchval("SELECT pg_backend_pid()")

            if exclude_current:
                query = """
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = $1 AND pid != $2
                """
                results = await connection.fetch(query, database_name, current_pid)
            else:
                query = """
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = $1
                """
                results = await connection.fetch(query, database_name)

            terminated_count = sum(1 for result in results if result[0])
            return terminated_count

        except Exception as e:
            logger.warning(f"Failed to terminate connections: {e}")
            return 0

    async def _get_database_size(self, connection, database_name: str) -> int:
        """Get database size in bytes."""
        try:
            query = "SELECT pg_database_size($1)"
            return await connection.fetchval(query, database_name)
        except Exception:
            return 0

    async def _validate_encoding(self, encoding: str) -> None:
        """Validate database encoding."""
        valid_encodings = [e.value for e in DatabaseEncoding]
        if encoding.upper() not in valid_encodings:
            logger.warning(f"Encoding {encoding} may not be supported")

    async def _validate_collation(self, connection, collation: str) -> bool:
        """Validate collation exists."""
        try:
            query = "SELECT 1 FROM pg_collation WHERE collname = $1"
            result = await connection.fetchval(query, collation)
            return bool(result)
        except Exception:
            return False

    async def _get_template_info(self, connection, template_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a template database."""
        try:
            query = """
                SELECT
                    datname,
                    pg_catalog.pg_encoding_to_char(encoding) as encoding,
                    datcollate,
                    datctype,
                    datistemplate
                FROM pg_database
                WHERE datname = $1
            """
            result = await connection.fetchrow(query, template_name)
            return dict(result) if result else None
        except Exception:
            return None

    async def _check_database_activity(self, connection, database_name: str) -> Dict[str, Any]:
        """Check database activity and connections."""
        try:
            query = """
                SELECT
                    COUNT(*) as total_connections,
                    COUNT(CASE WHEN state = 'active' THEN 1 END) as active_connections,
                    COUNT(CASE WHEN state = 'idle' THEN 1 END) as idle_connections,
                    COUNT(CASE WHEN state = 'idle in transaction' THEN 1 END) as idle_in_transaction,
                    MAX(backend_start) as oldest_connection
                FROM pg_stat_activity
                WHERE datname = $1
            """
            result = await connection.fetchrow(query, database_name)
            return dict(result) if result else {}
        except Exception:
            return {}

    @abstractmethod
    async def execute(self, **kwargs) -> DatabaseOperationResult:
        """Execute the database operation."""
        pass