"""Base classes for database operations with permission checking and auditing."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
import asyncio
from enum import Enum
import logging
import re
from datetime import datetime, timezone

from ..permissions import Permission, RBACManager, AuditLogger, OperationStatus
from .validators import NameValidator, SQLValidator, ParameterValidator, OperationValidator

logger = logging.getLogger(__name__)


class OperationType(str, Enum):
    """Types of database operations."""
    DDL = "ddl"  # Data Definition Language
    DML = "dml"  # Data Manipulation Language
    DCL = "dcl"  # Data Control Language
    TCL = "tcl"  # Transaction Control Language
    MAINTENANCE = "maintenance"
    MONITORING = "monitoring"
    BACKUP = "backup"
    REPLICATION = "replication"


@dataclass
class OperationResult:
    """Result of a database operation."""

    success: bool
    operation_type: OperationType
    operation_name: str
    affected_objects: List[str] = field(default_factory=list)
    execution_time_ms: float = 0.0
    message: Optional[str] = None
    error: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    affected_rows: Optional[int] = None
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "operation_type": self.operation_type.value,
            "operation_name": self.operation_name,
            "affected_objects": self.affected_objects,
            "execution_time_ms": self.execution_time_ms,
            "message": self.message,
            "error": self.error,
            "details": self.details,
            "affected_rows": self.affected_rows,
            "warnings": self.warnings
        }


class BaseOperation(ABC):
    """Abstract base class for all database operations."""

    def __init__(self,
                 connection_pool,
                 rbac_manager: RBACManager,
                 audit_logger: AuditLogger):
        """Initialize base operation.

        Args:
            connection_pool: Database connection pool
            rbac_manager: RBAC manager for permission checking
            audit_logger: Audit logger for operation logging
        """
        self.connection_pool = connection_pool
        self.rbac_manager = rbac_manager
        self.audit_logger = audit_logger

        # Initialize validators
        self.name_validator = NameValidator()
        self.sql_validator = SQLValidator()
        self.parameter_validator = ParameterValidator()
        self.operation_validator = OperationValidator()

    def _quote_identifier(self, identifier: str) -> str:
        """Quote a PostgreSQL identifier if necessary.

        Args:
            identifier: The identifier to quote

        Returns:
            Quoted identifier
        """
        # Already quoted
        if identifier.startswith('"') and identifier.endswith('"'):
            return identifier

        # Check if quoting is needed
        needs_quoting = (
            identifier.lower() in NameValidator.RESERVED_KEYWORDS or
            not re.match(r'^[a-z_][a-z0-9_$]*$', identifier.lower()) or
            identifier != identifier.lower()  # Contains uppercase
        )

        if needs_quoting:
            # Escape any quotes in the identifier
            escaped = identifier.replace('"', '""')
            return f'"{escaped}"'

        return identifier

    @abstractmethod
    def get_operation_type(self) -> OperationType:
        """Get the type of this operation.

        Returns:
            Operation type
        """
        pass

    @abstractmethod
    def get_required_permissions(self) -> List[Permission]:
        """Get the required permissions for this operation.

        Returns:
            List of required permissions (ANY of these permissions allows the operation)
        """
        pass

    @abstractmethod
    async def validate(self, **kwargs) -> Tuple[bool, Optional[str]]:
        """Validate the operation parameters.

        Args:
            **kwargs: Operation parameters

        Returns:
            Tuple of (is_valid, error_message)
        """
        pass

    @abstractmethod
    async def execute(self, **kwargs) -> OperationResult:
        """Execute the operation.

        Args:
            **kwargs: Operation parameters

        Returns:
            Operation result
        """
        pass

    async def pre_execute(self, **kwargs) -> Optional[str]:
        """Hook called before execution.

        Args:
            **kwargs: Operation parameters

        Returns:
            Error message if pre-execution fails, None otherwise
        """
        return None

    async def post_execute(self, result: OperationResult, **kwargs):
        """Hook called after execution.

        Args:
            result: Operation result
            **kwargs: Operation parameters
        """
        pass

    def check_permissions(self, role: str) -> Tuple[bool, Optional[Permission]]:
        """Check if role has any of the required permissions.

        Args:
            role: Role name

        Returns:
            Tuple of (has_permission, granted_permission)
        """
        required_permissions = self.get_required_permissions()

        for permission in required_permissions:
            if self.rbac_manager.check_permission(role, permission):
                return True, permission

        return False, None

    async def run(self,
                 user: str,
                 role: str,
                 session_id: Optional[str] = None,
                 client_ip: Optional[str] = None,
                 **kwargs) -> OperationResult:
        """Run the operation with permission checking and auditing.

        Args:
            user: Username performing the operation
            role: Role of the user
            session_id: Optional session identifier
            client_ip: Optional client IP address
            **kwargs: Operation parameters

        Returns:
            Operation result
        """
        operation_name = self.__class__.__name__
        start_time = asyncio.get_event_loop().time()

        # Extract database context if provided
        database_name = kwargs.get('database_name')
        schema_name = kwargs.get('schema_name', 'public')
        object_name = kwargs.get('object_name') or kwargs.get('table_name')
        object_type = kwargs.get('object_type', 'unknown')

        try:
            # Check permissions
            has_permission, granted_permission = self.check_permissions(role)

            if not has_permission:
                required = self.get_required_permissions()
                await self.audit_logger.log_operation(
                    user=user,
                    role=role,
                    operation=operation_name,
                    status=OperationStatus.DENIED,
                    object_type=object_type,
                    object_name=object_name,
                    schema_name=schema_name,
                    database_name=database_name,
                    details=kwargs,
                    error_message=f"Required permissions: {[p.value for p in required]}",
                    session_id=session_id,
                    client_ip=client_ip
                )
                return OperationResult(
                    success=False,
                    operation_type=self.get_operation_type(),
                    operation_name=operation_name,
                    affected_objects=[],
                    execution_time_ms=0,
                    error=f"Permission denied. Required: {[p.value for p in required]}"
                )

            # Validate parameters
            is_valid, error_message = await self.validate(**kwargs)
            if not is_valid:
                await self.audit_logger.log_operation(
                    user=user,
                    role=role,
                    operation=operation_name,
                    status=OperationStatus.FAILURE,
                    object_type=object_type,
                    object_name=object_name,
                    schema_name=schema_name,
                    database_name=database_name,
                    details=kwargs,
                    error_message=f"Validation failed: {error_message}",
                    session_id=session_id,
                    client_ip=client_ip
                )
                return OperationResult(
                    success=False,
                    operation_type=self.get_operation_type(),
                    operation_name=operation_name,
                    affected_objects=[],
                    execution_time_ms=0,
                    error=f"Validation failed: {error_message}"
                )

            # Pre-execution hook
            pre_error = await self.pre_execute(**kwargs)
            if pre_error:
                await self.audit_logger.log_operation(
                    user=user,
                    role=role,
                    operation=operation_name,
                    status=OperationStatus.FAILURE,
                    object_type=object_type,
                    object_name=object_name,
                    schema_name=schema_name,
                    database_name=database_name,
                    details=kwargs,
                    error_message=f"Pre-execution failed: {pre_error}",
                    session_id=session_id,
                    client_ip=client_ip
                )
                return OperationResult(
                    success=False,
                    operation_type=self.get_operation_type(),
                    operation_name=operation_name,
                    affected_objects=[],
                    execution_time_ms=0,
                    error=f"Pre-execution failed: {pre_error}"
                )

            # Execute operation
            logger.info(f"Executing {operation_name} for user {user}/{role}")
            result = await self.execute(**kwargs)

            # Add execution time
            end_time = asyncio.get_event_loop().time()
            result.execution_time_ms = (end_time - start_time) * 1000

            # Post-execution hook
            await self.post_execute(result, **kwargs)

            # Log result
            await self.audit_logger.log_operation(
                user=user,
                role=role,
                operation=operation_name,
                status=OperationStatus.SUCCESS if result.success else OperationStatus.FAILURE,
                object_type=object_type,
                object_name=object_name,
                schema_name=schema_name,
                database_name=database_name,
                details=kwargs,
                error_message=result.error,
                execution_time_ms=result.execution_time_ms,
                affected_rows=result.affected_rows,
                session_id=session_id,
                client_ip=client_ip
            )

            return result

        except Exception as e:
            # Log error
            end_time = asyncio.get_event_loop().time()
            execution_time = (end_time - start_time) * 1000

            await self.audit_logger.log_operation(
                user=user,
                role=role,
                operation=operation_name,
                status=OperationStatus.ERROR,
                object_type=object_type,
                object_name=object_name,
                schema_name=schema_name,
                database_name=database_name,
                details=kwargs,
                error_message=str(e),
                execution_time_ms=execution_time,
                session_id=session_id,
                client_ip=client_ip
            )

            logger.error(f"Operation {operation_name} failed with exception: {e}")

            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name=operation_name,
                affected_objects=[],
                execution_time_ms=execution_time,
                error=str(e)
            )


class DDLOperation(BaseOperation):
    """Base class for DDL (Data Definition Language) operations."""

    def get_operation_type(self) -> OperationType:
        """Get operation type."""
        return OperationType.DDL

    async def check_object_exists(self,
                                 object_type: str,
                                 object_name: str,
                                 schema: str = "public") -> bool:
        """Check if a database object exists.

        Args:
            object_type: Type of object (table, view, index, etc.)
            object_name: Name of the object
            schema: Schema name

        Returns:
            True if object exists, False otherwise
        """
        query_map = {
            "table": """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                    AND table_type = 'BASE TABLE'
                )
            """,
            "view": """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.views
                    WHERE table_schema = %s AND table_name = %s
                )
            """,
            "index": """
                SELECT EXISTS (
                    SELECT 1 FROM pg_indexes
                    WHERE schemaname = %s AND indexname = %s
                )
            """,
            "sequence": """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.sequences
                    WHERE sequence_schema = %s AND sequence_name = %s
                )
            """,
            "function": """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.routines
                    WHERE routine_schema = %s AND routine_name = %s
                )
            """
        }

        query = query_map.get(object_type.lower())
        if not query:
            raise ValueError(f"Unknown object type: {object_type}")

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (schema, object_name))
                result = await cur.fetchone()
                return result[0] if result else False

    async def get_dependent_objects(self,
                                   object_type: str,
                                   object_name: str,
                                   schema: str = "public") -> List[Dict[str, Any]]:
        """Get objects that depend on the specified object.

        Args:
            object_type: Type of object
            object_name: Name of the object
            schema: Schema name

        Returns:
            List of dependent objects
        """
        # This is a simplified version - real implementation would be more comprehensive
        query = """
            SELECT
                dep.classid::regclass AS dependent_type,
                dep.objid::regclass AS dependent_object
            FROM pg_depend dep
            JOIN pg_class cls ON cls.oid = dep.refobjid
            JOIN pg_namespace ns ON ns.oid = cls.relnamespace
            WHERE cls.relname = %s AND ns.nspname = %s
        """

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (object_name, schema))
                rows = await cur.fetchall()
                return [
                    {"type": row[0], "name": row[1]}
                    for row in rows
                ]


class DMLOperation(BaseOperation):
    """Base class for DML (Data Manipulation Language) operations."""

    def get_operation_type(self) -> OperationType:
        """Get operation type."""
        return OperationType.DML

    async def estimate_affected_rows(self,
                                    table: str,
                                    condition: Optional[str] = None,
                                    schema: str = "public") -> int:
        """Estimate number of rows that would be affected.

        Args:
            table: Table name
            condition: WHERE clause condition
            schema: Schema name

        Returns:
            Estimated row count
        """
        query = f"SELECT COUNT(*) FROM {schema}.{table}"
        if condition:
            query += f" WHERE {condition}"

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                result = await cur.fetchone()
                return result[0] if result else 0


class MaintenanceOperation(BaseOperation):
    """Base class for maintenance operations."""

    def get_operation_type(self) -> OperationType:
        """Get operation type."""
        return OperationType.MAINTENANCE

    async def estimate_operation_time(self, **kwargs) -> float:
        """Estimate how long the operation will take.

        Args:
            **kwargs: Operation parameters

        Returns:
            Estimated time in seconds
        """
        # Default implementation - subclasses should override
        return 0.0

    async def check_can_run_now(self) -> Tuple[bool, Optional[str]]:
        """Check if the operation can run now.

        Returns:
            Tuple of (can_run, reason_if_not)
        """
        # Check for active connections, running queries, etc.
        query = """
            SELECT COUNT(*)
            FROM pg_stat_activity
            WHERE state = 'active'
            AND pid != pg_backend_pid()
        """

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                result = await cur.fetchone()
                active_connections = result[0] if result else 0

                if active_connections > 10:  # Arbitrary threshold
                    return False, f"Too many active connections: {active_connections}"

                return True, None

    async def wait_for_locks(self, timeout: int = 30) -> bool:
        """Wait for locks to be released.

        Args:
            timeout: Maximum wait time in seconds

        Returns:
            True if locks released, False if timeout
        """
        start_time = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start_time < timeout:
            can_run, _ = await self.check_can_run_now()
            if can_run:
                return True
            await asyncio.sleep(1)

        return False


class MonitoringOperation(BaseOperation):
    """Base class for monitoring operations."""

    def get_operation_type(self) -> OperationType:
        """Get operation type."""
        return OperationType.MONITORING

    def get_required_permissions(self) -> List[Permission]:
        """Most monitoring operations require VIEW_STATS permission."""
        return [Permission.VIEW_STATS]

    async def get_database_size(self, database: Optional[str] = None) -> Dict[str, Any]:
        """Get database size information.

        Args:
            database: Database name (current if None)

        Returns:
            Size information
        """
        query = """
            SELECT
                pg_database_size(current_database()) as size_bytes,
                pg_size_pretty(pg_database_size(current_database())) as size_pretty
        """

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                result = await cur.fetchone()
                return {
                    "size_bytes": result[0],
                    "size_pretty": result[1]
                } if result else {}


class BackupOperation(BaseOperation):
    """Base class for backup operations."""

    def get_operation_type(self) -> OperationType:
        """Get operation type."""
        return OperationType.BACKUP

    def get_required_permissions(self) -> List[Permission]:
        """Backup operations require BACKUP permission."""
        return [Permission.BACKUP]

    async def validate_backup_path(self, path: str) -> Tuple[bool, Optional[str]]:
        """Validate backup path.

        Args:
            path: Backup file path

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Basic validation - real implementation would be more comprehensive
        if not path:
            return False, "Backup path is required"

        if ".." in path:
            return False, "Path traversal not allowed"

        return True, None