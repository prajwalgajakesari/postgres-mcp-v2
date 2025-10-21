"""Audit logging system for database operations."""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict, field
from pathlib import Path
import aiofiles
from enum import Enum


class OperationStatus(str, Enum):
    """Operation status for audit entries."""
    SUCCESS = "success"
    FAILURE = "failure"
    DENIED = "denied"
    ERROR = "error"


@dataclass
class AuditEntry:
    """Audit log entry."""

    timestamp: datetime
    user: str
    role: str
    operation: str
    status: OperationStatus
    object_type: Optional[str] = None
    object_name: Optional[str] = None
    schema_name: Optional[str] = None
    database_name: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    execution_time_ms: Optional[float] = None
    affected_rows: Optional[int] = None
    client_ip: Optional[str] = None
    session_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        result['timestamp'] = self.timestamp.isoformat()
        result['status'] = self.status.value
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AuditEntry':
        """Create from dictionary."""
        data = data.copy()
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        data['status'] = OperationStatus(data['status'])
        return cls(**data)


class AuditLogger:
    """Handles audit logging for all database operations."""

    def __init__(self,
                 log_file: Optional[str] = None,
                 max_memory_entries: int = 10000,
                 enable_console: bool = True):
        """Initialize audit logger.

        Args:
            log_file: Path to audit log file
            max_memory_entries: Maximum entries to keep in memory
            enable_console: Whether to also log to console
        """
        self.log_file = Path(log_file) if log_file else Path("postgres_mcp_audit.jsonl")
        self.max_memory_entries = max_memory_entries
        self.entries: List[AuditEntry] = []
        self._lock = asyncio.Lock()

        # Setup standard logger for console output
        self.logger = logging.getLogger("audit")
        if enable_console:
            self._setup_console_logger()

        # Ensure log directory exists
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

    def _setup_console_logger(self):
        """Setup console logger for audit events."""
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - AUDIT - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    async def log_operation(self,
                           user: str,
                           role: str,
                           operation: str,
                           status: OperationStatus,
                           object_type: Optional[str] = None,
                           object_name: Optional[str] = None,
                           schema_name: Optional[str] = None,
                           database_name: Optional[str] = None,
                           details: Optional[Dict[str, Any]] = None,
                           error_message: Optional[str] = None,
                           execution_time_ms: Optional[float] = None,
                           affected_rows: Optional[int] = None,
                           client_ip: Optional[str] = None,
                           session_id: Optional[str] = None):
        """Log a database operation.

        Args:
            user: Username performing operation
            role: Role of the user
            operation: Operation name
            status: Operation status
            object_type: Type of database object
            object_name: Name of database object
            schema_name: Schema name
            database_name: Database name
            details: Additional operation details
            error_message: Error message if failed
            execution_time_ms: Execution time in milliseconds
            affected_rows: Number of affected rows
            client_ip: Client IP address
            session_id: Session identifier
        """
        entry = AuditEntry(
            timestamp=datetime.now(timezone.utc),
            user=user,
            role=role,
            operation=operation,
            status=status,
            object_type=object_type,
            object_name=object_name,
            schema_name=schema_name,
            database_name=database_name,
            details=details,
            error_message=error_message,
            execution_time_ms=execution_time_ms,
            affected_rows=affected_rows,
            client_ip=client_ip,
            session_id=session_id
        )

        async with self._lock:
            # Add to memory
            self.entries.append(entry)

            # Trim memory entries if needed
            if len(self.entries) > self.max_memory_entries:
                self.entries = self.entries[-self.max_memory_entries:]

            # Write to file
            await self._write_to_file(entry)

        # Log to console
        self._log_to_console(entry)

    async def _write_to_file(self, entry: AuditEntry):
        """Write entry to audit log file.

        Args:
            entry: Audit entry to write
        """
        try:
            async with aiofiles.open(self.log_file, mode='a') as f:
                await f.write(json.dumps(entry.to_dict()) + '\n')
        except Exception as e:
            self.logger.error(f"Failed to write audit log to file: {e}")

    def _log_to_console(self, entry: AuditEntry):
        """Log entry to console.

        Args:
            entry: Audit entry to log
        """
        message = f"[{entry.user}/{entry.role}] {entry.operation}"

        if entry.object_name:
            message += f" on {entry.object_type or 'object'} '{entry.object_name}'"

        if entry.schema_name:
            message += f" in schema '{entry.schema_name}'"

        message += f" - {entry.status.value.upper()}"

        if entry.execution_time_ms:
            message += f" ({entry.execution_time_ms:.2f}ms)"

        if entry.affected_rows is not None:
            message += f" [{entry.affected_rows} rows]"

        # Choose log level based on status
        if entry.status == OperationStatus.SUCCESS:
            self.logger.info(message)
        elif entry.status == OperationStatus.DENIED:
            self.logger.warning(f"{message} - {entry.error_message or 'Permission denied'}")
        else:
            self.logger.error(f"{message} - {entry.error_message or 'Operation failed'}")

    async def get_audit_trail(self,
                             user: Optional[str] = None,
                             role: Optional[str] = None,
                             operation: Optional[str] = None,
                             status: Optional[OperationStatus] = None,
                             object_name: Optional[str] = None,
                             start_time: Optional[datetime] = None,
                             end_time: Optional[datetime] = None,
                             limit: Optional[int] = None) -> List[AuditEntry]:
        """Retrieve audit trail with optional filters.

        Args:
            user: Filter by user
            role: Filter by role
            operation: Filter by operation
            status: Filter by status
            object_name: Filter by object name
            start_time: Filter by start time
            end_time: Filter by end time
            limit: Maximum entries to return

        Returns:
            List of matching audit entries
        """
        async with self._lock:
            results = list(self.entries)  # Copy to avoid modification during iteration

        # Apply filters
        if user:
            results = [e for e in results if e.user == user]

        if role:
            results = [e for e in results if e.role == role]

        if operation:
            results = [e for e in results if e.operation == operation]

        if status:
            results = [e for e in results if e.status == status]

        if object_name:
            results = [e for e in results if e.object_name == object_name]

        if start_time:
            results = [e for e in results if e.timestamp >= start_time]

        if end_time:
            results = [e for e in results if e.timestamp <= end_time]

        # Sort by timestamp (newest first)
        results.sort(key=lambda e: e.timestamp, reverse=True)

        # Apply limit
        if limit:
            results = results[:limit]

        return results

    async def get_statistics(self,
                            start_time: Optional[datetime] = None,
                            end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Get audit statistics.

        Args:
            start_time: Start time for statistics
            end_time: End time for statistics

        Returns:
            Dictionary with statistics
        """
        entries = await self.get_audit_trail(
            start_time=start_time,
            end_time=end_time
        )

        if not entries:
            return {
                "total_operations": 0,
                "by_status": {},
                "by_operation": {},
                "by_user": {},
                "by_role": {},
                "average_execution_time_ms": 0
            }

        # Calculate statistics
        stats = {
            "total_operations": len(entries),
            "by_status": {},
            "by_operation": {},
            "by_user": {},
            "by_role": {},
            "by_object_type": {},
            "failed_operations": [],
            "denied_operations": [],
            "slowest_operations": []
        }

        # Count by status
        for entry in entries:
            status = entry.status.value
            stats["by_status"][status] = stats["by_status"].get(status, 0) + 1

            # Count by operation
            stats["by_operation"][entry.operation] = \
                stats["by_operation"].get(entry.operation, 0) + 1

            # Count by user
            stats["by_user"][entry.user] = stats["by_user"].get(entry.user, 0) + 1

            # Count by role
            stats["by_role"][entry.role] = stats["by_role"].get(entry.role, 0) + 1

            # Count by object type
            if entry.object_type:
                stats["by_object_type"][entry.object_type] = \
                    stats["by_object_type"].get(entry.object_type, 0) + 1

            # Track failed operations
            if entry.status == OperationStatus.FAILURE:
                stats["failed_operations"].append({
                    "timestamp": entry.timestamp.isoformat(),
                    "user": entry.user,
                    "operation": entry.operation,
                    "error": entry.error_message
                })

            # Track denied operations
            if entry.status == OperationStatus.DENIED:
                stats["denied_operations"].append({
                    "timestamp": entry.timestamp.isoformat(),
                    "user": entry.user,
                    "operation": entry.operation,
                    "object": entry.object_name
                })

        # Calculate average execution time
        exec_times = [e.execution_time_ms for e in entries if e.execution_time_ms]
        if exec_times:
            stats["average_execution_time_ms"] = sum(exec_times) / len(exec_times)

            # Find slowest operations
            sorted_by_time = sorted(
                [e for e in entries if e.execution_time_ms],
                key=lambda e: e.execution_time_ms,
                reverse=True
            )[:10]

            stats["slowest_operations"] = [
                {
                    "timestamp": e.timestamp.isoformat(),
                    "operation": e.operation,
                    "object": e.object_name,
                    "execution_time_ms": e.execution_time_ms
                }
                for e in sorted_by_time
            ]
        else:
            stats["average_execution_time_ms"] = 0

        return stats

    async def clear_old_entries(self, days_to_keep: int = 30):
        """Clear audit entries older than specified days.

        Args:
            days_to_keep: Number of days to keep
        """
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)

        async with self._lock:
            self.entries = [
                e for e in self.entries
                if e.timestamp >= cutoff_date
            ]

        self.logger.info(f"Cleared audit entries older than {days_to_keep} days")

    async def export_to_file(self,
                            export_file: str,
                            start_time: Optional[datetime] = None,
                            end_time: Optional[datetime] = None):
        """Export audit trail to file.

        Args:
            export_file: Path to export file
            start_time: Optional start time filter
            end_time: Optional end time filter
        """
        entries = await self.get_audit_trail(
            start_time=start_time,
            end_time=end_time
        )

        export_data = {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "entry_count": len(entries),
            "entries": [e.to_dict() for e in entries]
        }

        async with aiofiles.open(export_file, 'w') as f:
            await f.write(json.dumps(export_data, indent=2))

        self.logger.info(f"Exported {len(entries)} audit entries to {export_file}")