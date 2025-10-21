"""Tests for audit logging system."""

import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from postgres_mcp.permissions.audit import AuditLogger, AuditEntry, OperationStatus
import pytest_asyncio


@pytest.mark.asyncio
class TestAuditLogger:
    """Test audit logging functionality."""

    @pytest_asyncio.fixture
    async def audit_logger(self, tmp_path):
        """Create audit logger instance."""
        log_file = tmp_path / "test_audit.jsonl"
        return AuditLogger(log_file=str(log_file), enable_console=False)

    async def test_log_operation_success(self, audit_logger):
        """Test logging a successful operation."""
        await audit_logger.log_operation(
            user="testuser",
            role="developer",
            operation="CREATE_TABLE",
            status=OperationStatus.SUCCESS,
            object_type="table",
            object_name="test_table",
            schema_name="public",
            database_name="testdb",
            execution_time_ms=125.5,
            affected_rows=0
        )

        assert len(audit_logger.entries) == 1
        entry = audit_logger.entries[0]

        assert entry.user == "testuser"
        assert entry.role == "developer"
        assert entry.operation == "CREATE_TABLE"
        assert entry.status == OperationStatus.SUCCESS
        assert entry.object_name == "test_table"
        assert entry.execution_time_ms == 125.5

    async def test_log_operation_failure(self, audit_logger):
        """Test logging a failed operation."""
        await audit_logger.log_operation(
            user="testuser",
            role="analyst",
            operation="DROP_TABLE",
            status=OperationStatus.FAILURE,
            object_type="table",
            object_name="important_table",
            error_message="Permission denied"
        )

        assert len(audit_logger.entries) == 1
        entry = audit_logger.entries[0]

        assert entry.status == OperationStatus.FAILURE
        assert entry.error_message == "Permission denied"

    async def test_log_operation_denied(self, audit_logger):
        """Test logging a denied operation."""
        await audit_logger.log_operation(
            user="testuser",
            role="readonly",
            operation="DELETE",
            status=OperationStatus.DENIED,
            object_type="table",
            object_name="users",
            error_message="Insufficient privileges"
        )

        entry = audit_logger.entries[0]
        assert entry.status == OperationStatus.DENIED
        assert "Insufficient privileges" in entry.error_message

    async def test_audit_trail_filtering_by_user(self, audit_logger):
        """Test filtering audit trail by user."""
        # Log multiple operations for different users
        await audit_logger.log_operation("user1", "developer", "CREATE_TABLE", OperationStatus.SUCCESS)
        await audit_logger.log_operation("user2", "analyst", "SELECT", OperationStatus.SUCCESS)
        await audit_logger.log_operation("user1", "developer", "DROP_TABLE", OperationStatus.FAILURE)

        # Filter by user
        user1_trail = await audit_logger.get_audit_trail(user="user1")
        assert len(user1_trail) == 2
        assert all(e.user == "user1" for e in user1_trail)

    async def test_audit_trail_filtering_by_operation(self, audit_logger):
        """Test filtering audit trail by operation."""
        await audit_logger.log_operation("user1", "developer", "CREATE_TABLE", OperationStatus.SUCCESS)
        await audit_logger.log_operation("user1", "developer", "CREATE_INDEX", OperationStatus.SUCCESS)
        await audit_logger.log_operation("user2", "developer", "CREATE_TABLE", OperationStatus.SUCCESS)

        # Filter by operation
        create_table_trail = await audit_logger.get_audit_trail(operation="CREATE_TABLE")
        assert len(create_table_trail) == 2
        assert all(e.operation == "CREATE_TABLE" for e in create_table_trail)

    async def test_audit_trail_filtering_by_status(self, audit_logger):
        """Test filtering audit trail by status."""
        await audit_logger.log_operation("user1", "dev", "OP1", OperationStatus.SUCCESS)
        await audit_logger.log_operation("user2", "dev", "OP2", OperationStatus.FAILURE)
        await audit_logger.log_operation("user3", "dev", "OP3", OperationStatus.DENIED)
        await audit_logger.log_operation("user4", "dev", "OP4", OperationStatus.SUCCESS)

        # Filter by status
        success_trail = await audit_logger.get_audit_trail(status=OperationStatus.SUCCESS)
        assert len(success_trail) == 2
        assert all(e.status == OperationStatus.SUCCESS for e in success_trail)

    async def test_audit_trail_filtering_by_time(self, audit_logger):
        """Test filtering audit trail by time range."""
        now = datetime.now(timezone.utc)

        # Log operations
        await audit_logger.log_operation("user1", "dev", "OP1", OperationStatus.SUCCESS)
        await asyncio.sleep(0.1)  # Small delay

        mid_time = datetime.now(timezone.utc)

        await audit_logger.log_operation("user2", "dev", "OP2", OperationStatus.SUCCESS)

        # Get operations after mid_time
        recent_trail = await audit_logger.get_audit_trail(start_time=mid_time)
        assert len(recent_trail) == 1
        assert recent_trail[0].operation == "OP2"

        # Get operations before mid_time
        older_trail = await audit_logger.get_audit_trail(end_time=mid_time)
        assert len(older_trail) == 1
        assert older_trail[0].operation == "OP1"

    async def test_audit_trail_limit(self, audit_logger):
        """Test limiting audit trail results."""
        # Log multiple operations
        for i in range(10):
            await audit_logger.log_operation(f"user{i}", "dev", f"OP{i}", OperationStatus.SUCCESS)

        # Get limited results
        limited_trail = await audit_logger.get_audit_trail(limit=5)
        assert len(limited_trail) == 5

    async def test_get_statistics(self, audit_logger):
        """Test getting audit statistics."""
        # Log various operations
        await audit_logger.log_operation("user1", "dev", "CREATE_TABLE", OperationStatus.SUCCESS, execution_time_ms=100)
        await audit_logger.log_operation("user1", "dev", "DROP_TABLE", OperationStatus.FAILURE, error_message="Not found")
        await audit_logger.log_operation("user2", "readonly", "DELETE", OperationStatus.DENIED)
        await audit_logger.log_operation("user2", "dev", "CREATE_INDEX", OperationStatus.SUCCESS, execution_time_ms=200)

        stats = await audit_logger.get_statistics()

        assert stats["total_operations"] == 4
        assert stats["by_status"]["success"] == 2
        assert stats["by_status"]["failure"] == 1
        assert stats["by_status"]["denied"] == 1
        assert stats["by_user"]["user1"] == 2
        assert stats["by_user"]["user2"] == 2
        assert stats["average_execution_time_ms"] == 150  # (100 + 200) / 2

    async def test_clear_old_entries(self, audit_logger):
        """Test clearing old audit entries."""
        # Create old entry
        old_entry = AuditEntry(
            timestamp=datetime.now(timezone.utc) - timedelta(days=35),
            user="olduser",
            role="dev",
            operation="OLD_OP",
            status=OperationStatus.SUCCESS
        )
        audit_logger.entries.append(old_entry)

        # Create recent entry
        await audit_logger.log_operation("recentuser", "dev", "RECENT_OP", OperationStatus.SUCCESS)

        assert len(audit_logger.entries) == 2

        # Clear old entries
        await audit_logger.clear_old_entries(days_to_keep=30)

        assert len(audit_logger.entries) == 1
        assert audit_logger.entries[0].operation == "RECENT_OP"

    async def test_export_to_file(self, audit_logger, tmp_path):
        """Test exporting audit trail to file."""
        # Log operations
        await audit_logger.log_operation("user1", "dev", "OP1", OperationStatus.SUCCESS)
        await audit_logger.log_operation("user2", "dev", "OP2", OperationStatus.FAILURE)

        export_file = tmp_path / "export.json"
        await audit_logger.export_to_file(str(export_file))

        assert export_file.exists()

        # Read and verify export
        import json
        with open(export_file) as f:
            exported = json.load(f)

        assert exported["entry_count"] == 2
        assert len(exported["entries"]) == 2

    async def test_audit_entry_serialization(self, audit_logger):
        """Test audit entry serialization."""
        entry = AuditEntry(
            timestamp=datetime.now(timezone.utc),
            user="testuser",
            role="developer",
            operation="TEST_OP",
            status=OperationStatus.SUCCESS,
            details={"key": "value"},
            affected_rows=10
        )

        # Convert to dict
        entry_dict = entry.to_dict()

        assert entry_dict["user"] == "testuser"
        assert entry_dict["affected_rows"] == 10
        assert entry_dict["details"]["key"] == "value"

        # Create from dict
        restored_entry = AuditEntry.from_dict(entry_dict)

        assert restored_entry.user == entry.user
        assert restored_entry.operation == entry.operation
        assert restored_entry.affected_rows == entry.affected_rows

    async def test_concurrent_logging(self, audit_logger):
        """Test concurrent audit logging."""
        async def log_operation(i):
            await audit_logger.log_operation(
                user=f"user{i}",
                role="dev",
                operation=f"OP{i}",
                status=OperationStatus.SUCCESS
            )

        # Log operations concurrently
        tasks = [log_operation(i) for i in range(10)]
        await asyncio.gather(*tasks)

        assert len(audit_logger.entries) == 10

    async def test_max_memory_entries(self, tmp_path):
        """Test that memory entries are limited."""
        audit_logger = AuditLogger(
            log_file=str(tmp_path / "test.jsonl"),
            max_memory_entries=5,
            enable_console=False
        )

        # Log more than max entries
        for i in range(10):
            await audit_logger.log_operation(f"user{i}", "dev", f"OP{i}", OperationStatus.SUCCESS)

        # Should only keep the last 5
        assert len(audit_logger.entries) == 5
        assert audit_logger.entries[0].user == "user5"
        assert audit_logger.entries[-1].user == "user9"