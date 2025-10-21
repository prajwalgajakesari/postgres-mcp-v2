"""Integration tests for Phase 3 operations."""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timedelta

from postgres_mcp.data import CopyDataOperation, ExportQueryOperation, ImportDataOperation, DataFormat
from postgres_mcp.backup import CreateBackupOperation, RestoreBackupOperation, ManageBackupsOperation, BackupFormat
from postgres_mcp.database import CreateDatabaseOperation, DropDatabaseOperation, CloneDatabaseOperation
from postgres_mcp.partitions import CreatePartitionOperation, ManagePartitionsOperation, PartitionStrategy
from postgres_mcp.common_utils import create_default_rbac_manager, SimpleAuditLogger


class TestPhase3Integration:
    """Integration tests for Phase 3 operations."""

    @pytest.fixture
    def mock_pool(self):
        """Create a mock connection pool."""
        pool = MagicMock()

        # Create a mock connection
        mock_connection = MagicMock()
        mock_connection.fetchval = AsyncMock(return_value=1)
        mock_connection.fetchrow = AsyncMock(return_value={'exists': True})
        mock_connection.fetch = AsyncMock(return_value=[])
        mock_connection.execute = AsyncMock()
        mock_connection.copy_to_table = AsyncMock()
        mock_connection.copy_from_table = AsyncMock()
        mock_connection.copy_from_query = AsyncMock()

        # Create async context manager for acquire
        async_context = AsyncMock()
        async_context.__aenter__ = AsyncMock(return_value=mock_connection)
        async_context.__aexit__ = AsyncMock()

        pool.acquire = MagicMock(return_value=async_context)

        return pool

    @pytest.fixture
    def rbac_manager(self):
        """Create RBAC manager."""
        return create_default_rbac_manager()

    @pytest.fixture
    def audit_logger(self):
        """Create audit logger."""
        return SimpleAuditLogger()

    @pytest.mark.asyncio
    async def test_data_export_import_workflow(self, mock_pool, rbac_manager, audit_logger):
        """Test complete data export and import workflow."""
        # Export data
        export_op = ExportQueryOperation(mock_pool, rbac_manager, audit_logger)

        # Mock the export operation
        with patch('pathlib.Path.exists', return_value=False), \
             patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True) as mock_open:

            mock_file = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file

            result = await export_op.export_table(
                table_name="users",
                file_path="/tmp/users.csv",
                format_type=DataFormat.CSV
            )

            assert result.success == True
            assert "export" in result.message.lower()

    @pytest.mark.asyncio
    async def test_backup_restore_workflow(self, mock_pool, rbac_manager, audit_logger):
        """Test complete backup and restore workflow."""
        # Create backup
        backup_op = CreateBackupOperation(mock_pool, rbac_manager, audit_logger)

        # Mock pg_dump check and execution
        with patch.object(backup_op, '_check_pg_tools', return_value=True), \
             patch.object(backup_op, '_execute_command', return_value=(True, "", "")), \
             patch('pathlib.Path.exists', return_value=False), \
             patch('pathlib.Path.mkdir'), \
             patch('pathlib.Path.stat') as mock_stat:

            mock_stat.return_value.st_size = 1024

            backup_result = await backup_op.create_database_backup(
                database_name="testdb",
                output_path="/tmp/backup.dump",
                backup_format=BackupFormat.CUSTOM
            )

            assert backup_result.success == True
            assert backup_result.backup_metadata is not None

        # Restore backup
        restore_op = RestoreBackupOperation(mock_pool, rbac_manager, audit_logger)

        with patch.object(restore_op, '_check_pg_tools', return_value=True), \
             patch.object(restore_op, '_execute_command', return_value=(True, "", "")), \
             patch.object(restore_op, '_detect_backup_format', return_value=BackupFormat.CUSTOM), \
             patch('pathlib.Path.exists', return_value=True):

            restore_result = await restore_op.restore_backup(
                backup_path="/tmp/backup.dump",
                target_database="testdb_restored"
            )

            assert restore_result.success == True

    @pytest.mark.asyncio
    async def test_database_clone_workflow(self, mock_pool, rbac_manager, audit_logger):
        """Test database cloning workflow."""
        # Create database
        create_op = CreateDatabaseOperation(mock_pool, rbac_manager, audit_logger)

        # Mock database doesn't exist
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=None)
        mock_connection.execute = AsyncMock()

        async_context = AsyncMock()
        async_context.__aenter__ = AsyncMock(return_value=mock_connection)
        async_context.__aexit__ = AsyncMock()

        mock_pool.acquire = MagicMock(return_value=async_context)

        create_result = await create_op.create_database(
            database_name="source_db"
        )

        assert create_result.success == True

        # Clone database
        clone_op = CloneDatabaseOperation(mock_pool, rbac_manager, audit_logger)

        # Mock source exists, target doesn't
        mock_connection.fetchval = AsyncMock(side_effect=[True, None])

        clone_result = await clone_op.clone_database(
            source_database="source_db",
            target_database="cloned_db"
        )

        assert clone_result.success == True

    @pytest.mark.asyncio
    async def test_partition_management_workflow(self, mock_pool, rbac_manager, audit_logger):
        """Test partition management workflow."""
        # Create partitioned table
        create_op = CreatePartitionOperation(mock_pool, rbac_manager, audit_logger)

        # Mock table doesn't exist
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=None)
        mock_connection.execute = AsyncMock()
        mock_connection.fetch = AsyncMock(return_value=[])

        async_context = AsyncMock()
        async_context.__aenter__ = AsyncMock(return_value=mock_connection)
        async_context.__aexit__ = AsyncMock()

        mock_pool.acquire = MagicMock(return_value=async_context)

        create_result = await create_op.create_partitioned_table(
            table_name="events",
            partition_key="created_at",
            partition_type=PartitionStrategy.RANGE
        )

        assert create_result.success == True

        # Create partitions
        partition_result = await create_op.create_partition(
            parent_table="events",
            partition_name="events_2024_01",
            from_value="2024-01-01",
            to_value="2024-02-01"
        )

        assert partition_result.success == True

        # Manage partitions
        manage_op = ManagePartitionsOperation(mock_pool, rbac_manager, audit_logger)

        # Mock partition listing
        mock_connection.fetch = AsyncMock(return_value=[
            {
                'partition_name': 'events_2024_01',
                'parent_table': 'events',
                'partition_expr': 'created_at',
                'partition_bounds': 'FOR VALUES FROM (''2024-01-01'') TO (''2024-02-01'')'
            }
        ])

        list_result = await manage_op.list_partitions(table_name="events")

        assert list_result.success == True

    @pytest.mark.asyncio
    async def test_cross_phase_integration(self, mock_pool, rbac_manager, audit_logger):
        """Test integration across different Phase 3 components."""
        # Create database
        create_db_op = CreateDatabaseOperation(mock_pool, rbac_manager, audit_logger)

        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=None)
        mock_connection.execute = AsyncMock()

        async_context = AsyncMock()
        async_context.__aenter__ = AsyncMock(return_value=mock_connection)
        async_context.__aexit__ = AsyncMock()

        mock_pool.acquire = MagicMock(return_value=async_context)

        db_result = await create_db_op.create_database(database_name="analytics_db")
        assert db_result.success == True

        # Create partitioned table in new database
        create_part_op = CreatePartitionOperation(mock_pool, rbac_manager, audit_logger)

        table_result = await create_part_op.create_partitioned_table(
            table_name="metrics",
            partition_key="timestamp",
            partition_type=PartitionStrategy.RANGE
        )
        assert table_result.success == True

        # Export table structure
        export_op = ExportQueryOperation(mock_pool, rbac_manager, audit_logger)

        with patch('pathlib.Path.exists', return_value=False), \
             patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True) as mock_open:

            mock_file = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file

            # Mock schema structure query
            mock_connection.fetch = AsyncMock(return_value=[
                {
                    'column_name': 'id',
                    'data_type': 'bigint',
                    'is_nullable': 'NO',
                    'column_default': None
                },
                {
                    'column_name': 'timestamp',
                    'data_type': 'timestamp',
                    'is_nullable': 'NO',
                    'column_default': None
                }
            ])

            export_result = await export_op.export_schema_structure(
                schema_name="public",
                file_path="/tmp/schema.json",
                format_type=DataFormat.JSON
            )

            assert export_result.success == True

        # Backup the database
        backup_op = CreateBackupOperation(mock_pool, rbac_manager, audit_logger)

        with patch.object(backup_op, '_check_pg_tools', return_value=True), \
             patch.object(backup_op, '_execute_command', return_value=(True, "", "")), \
             patch('pathlib.Path.exists', return_value=False), \
             patch('pathlib.Path.mkdir'), \
             patch('pathlib.Path.stat') as mock_stat:

            mock_stat.return_value.st_size = 2048

            backup_result = await backup_op.create_database_backup(
                database_name="analytics_db",
                output_path="/tmp/analytics.dump",
                backup_format=BackupFormat.CUSTOM
            )

            assert backup_result.success == True
            assert backup_result.backup_metadata.database_name == "analytics_db"

    @pytest.mark.asyncio
    async def test_error_handling_integration(self, mock_pool, rbac_manager, audit_logger):
        """Test error handling across Phase 3 components."""
        # Test data operation error handling
        copy_op = CopyDataOperation(mock_pool, rbac_manager, audit_logger)

        # Mock connection error
        mock_pool.acquire = MagicMock(side_effect=Exception("Connection failed"))

        with pytest.raises(Exception) as exc_info:
            await copy_op.copy_to_file(
                table_name="test_table",
                file_path="/tmp/test.csv"
            )

        assert "Connection failed" in str(exc_info.value)

        # Test backup operation error handling
        backup_op = CreateBackupOperation(mock_pool, rbac_manager, audit_logger)

        # Reset mock pool
        mock_connection = AsyncMock()
        async_context = AsyncMock()
        async_context.__aenter__ = AsyncMock(return_value=mock_connection)
        async_context.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=async_context)

        with patch.object(backup_op, '_check_pg_tools', return_value=False):
            result = await backup_op.create_database_backup(
                database_name="testdb",
                output_path="/tmp/backup.dump"
            )

            assert result.success == False
            assert "pg_dump not found" in result.message

        # Test partition operation error handling
        partition_op = CreatePartitionOperation(mock_pool, rbac_manager, audit_logger)

        # Mock table already exists
        mock_connection.fetchval = AsyncMock(return_value=True)

        result = await partition_op.create_partitioned_table(
            table_name="existing_table",
            partition_key="id",
            partition_type=PartitionStrategy.RANGE
        )

        assert result.success == False
        assert "already exists" in result.message.lower()

    @pytest.mark.asyncio
    async def test_permission_integration(self, mock_pool, audit_logger):
        """Test RBAC permission integration across Phase 3."""
        # Create custom RBAC manager
        rbac_manager = create_default_rbac_manager()

        # Create read-only role
        rbac_manager.create_role("readonly")
        rbac_manager.assign_permission("readonly", "database:list")

        # Test operations with limited permissions
        mock_connection = AsyncMock()
        async_context = AsyncMock()
        async_context.__aenter__ = AsyncMock(return_value=mock_connection)
        async_context.__aexit__ = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=async_context)

        # Try to create database with readonly role - should fail
        create_op = CreateDatabaseOperation(mock_pool, rbac_manager, audit_logger)

        with pytest.raises(PermissionError) as exc_info:
            await create_op.create_database(
                database_name="testdb",
                role_name="readonly"
            )

        assert "Missing permission" in str(exc_info.value)

        # Try to backup with readonly role - should fail
        backup_op = CreateBackupOperation(mock_pool, rbac_manager, audit_logger)

        with pytest.raises(PermissionError) as exc_info:
            await backup_op.create_database_backup(
                database_name="testdb",
                output_path="/tmp/backup.dump",
                role_name="readonly"
            )

        assert "Missing permission" in str(exc_info.value)