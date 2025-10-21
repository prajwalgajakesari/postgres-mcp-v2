"""Tests for Phase 3 Week 10: Backup & Restore Operations."""

import pytest
import asyncio
import tempfile
import os
import json
from pathlib import Path
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from datetime import datetime

from postgres_mcp.backup import (
    CreateBackupOperation, RestoreBackupOperation, ManageBackupsOperation,
    BackupFormat, BackupScope, BackupMetadata, BackupOperationResult
)
from postgres_mcp.permissions.rbac import RBACManager, Permission


@pytest.fixture
def mock_connection_pool():
    """Create mock connection pool."""
    pool = MagicMock()
    pool._config = {
        'host': 'localhost',
        'port': 5432,
        'user': 'test_user',
        'password': 'test_pass',
        'database': 'test_db'
    }
    return pool


@pytest.fixture
def mock_rbac_manager():
    """Create mock RBAC manager."""
    rbac = Mock(spec=RBACManager)
    rbac.check_permission = Mock(return_value=True)
    return rbac


@pytest.fixture
def mock_audit_logger():
    """Create mock audit logger."""
    logger = Mock()
    logger.audit_log = AsyncMock()
    return logger


@pytest.fixture
def sample_backup_metadata():
    """Create sample backup metadata."""
    return BackupMetadata(
        backup_id="backup_20240101_120000_abc123",
        timestamp=datetime.utcnow(),
        database_name="test_db",
        backup_format=BackupFormat.CUSTOM,
        backup_scope=BackupScope.DATABASE,
        file_path="/tmp/test_backup.dump",
        file_size=1024000,
        compression="level-6",
        pg_version="14.5",
        description="Test backup"
    )


class TestCreateBackupOperation:
    """Test backup creation operations."""

    @pytest.mark.asyncio
    @patch('postgres_mcp.backup.create.CreateBackupOperation._check_pg_tools')
    @patch('postgres_mcp.backup.create.CreateBackupOperation._execute_command')
    async def test_create_database_backup(self, mock_execute, mock_check_tools,
                                         mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test creating a database backup."""
        # Setup mocks
        mock_check_tools.return_value = True
        mock_execute.return_value = (True, "Backup completed", "")

        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(side_effect=["14.5", "test_db"])

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        backup_op = CreateBackupOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        with tempfile.NamedTemporaryFile(delete=False, suffix='.dump') as tmp:
            temp_path = tmp.name

        try:
            result = await backup_op.create_database_backup(
                database_name="test_db",
                output_path=temp_path,
                role_name="test_role",
                backup_format=BackupFormat.CUSTOM,
                compression_level=6
            )

            assert result.success is True
            assert "successfully" in result.message.lower()
            assert result.backup_metadata is not None
            assert result.backup_metadata.database_name == "test_db"
            assert result.backup_metadata.backup_format == BackupFormat.CUSTOM

            # Verify pg_dump command was built correctly
            mock_execute.assert_called_once()
            cmd = mock_execute.call_args[0][0]
            assert "pg_dump" in cmd
            assert "-Fc" in cmd  # Custom format

        finally:
            # Cleanup
            try:
                os.unlink(temp_path)
                os.unlink(f"{temp_path}.metadata.json")
            except:
                pass

    @pytest.mark.asyncio
    @patch('postgres_mcp.backup.create.CreateBackupOperation._check_pg_tools')
    @patch('postgres_mcp.backup.create.CreateBackupOperation._execute_command')
    async def test_create_schema_backup(self, mock_execute, mock_check_tools,
                                       mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test creating a schema-specific backup."""
        mock_check_tools.return_value = True
        mock_execute.return_value = (True, "Backup completed", "")

        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(side_effect=["14.5", "test_db"])

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        backup_op = CreateBackupOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        with tempfile.NamedTemporaryFile(delete=False, suffix='.dump') as tmp:
            temp_path = tmp.name

        try:
            result = await backup_op.create_schema_backup(
                database_name="test_db",
                schema_name="public",
                output_path=temp_path,
                role_name="test_role"
            )

            assert result.success is True
            assert result.backup_metadata is not None

            # Verify schema filter was applied
            mock_execute.assert_called_once()
            cmd = mock_execute.call_args[0][0]
            assert "-n" in cmd  # Schema filter flag

        finally:
            # Cleanup
            try:
                os.unlink(temp_path)
                os.unlink(f"{temp_path}.metadata.json")
            except:
                pass

    @pytest.mark.asyncio
    @patch('postgres_mcp.backup.create.CreateBackupOperation._check_pg_tools')
    async def test_backup_without_pg_dump(self, mock_check_tools,
                                         mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test backup failure when pg_dump is not available."""
        mock_check_tools.return_value = False

        backup_op = CreateBackupOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await backup_op.create_database_backup(
            database_name="test_db",
            output_path="/tmp/test.dump",
            role_name="test_role"
        )

        assert result.success is False
        assert "pg_dump command not found" in result.message

    @pytest.mark.asyncio
    async def test_backup_with_insufficient_permissions(self, mock_connection_pool, mock_audit_logger):
        """Test backup with insufficient permissions."""
        mock_rbac = Mock(spec=RBACManager)
        mock_rbac.check_permission = Mock(return_value=False)

        backup_op = CreateBackupOperation(mock_connection_pool, mock_rbac, mock_audit_logger)

        with pytest.raises(PermissionError):
            await backup_op.create_database_backup(
                database_name="test_db",
                output_path="/tmp/test.dump",
                role_name="test_role"
            )


class TestRestoreBackupOperation:
    """Test backup restoration operations."""

    @pytest.mark.asyncio
    @patch('postgres_mcp.backup.restore.RestoreBackupOperation._check_pg_tools')
    @patch('postgres_mcp.backup.restore.RestoreBackupOperation._execute_command')
    @patch('postgres_mcp.backup.restore.RestoreBackupOperation._detect_backup_format')
    async def test_restore_backup(self, mock_detect, mock_execute, mock_check_tools,
                                 mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test restoring a backup."""
        # Setup mocks
        mock_check_tools.return_value = True
        mock_detect.return_value = BackupFormat.CUSTOM
        mock_execute.return_value = (True, "Restore completed", "")

        # Create test backup file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.dump') as tmp:
            tmp.write(b"PGDMP")  # Mock custom format header
            temp_path = tmp.name

        try:
            mock_connection = AsyncMock()
            mock_connection.fetchval = AsyncMock(return_value=None)  # Database doesn't exist yet
            mock_connection.execute = AsyncMock()

            mock_connection_pool.acquire = AsyncMock()
            mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
            mock_connection_pool.acquire().__aexit__ = AsyncMock()

            restore_op = RestoreBackupOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

            result = await restore_op.restore_backup(
                backup_path=temp_path,
                target_database="restored_db",
                role_name="test_role",
                clean_before_restore=True
            )

            assert result.success is True
            assert "successfully" in result.message.lower()

            # Verify pg_restore command was built
            mock_execute.assert_called_once()
            cmd = mock_execute.call_args[0][0]
            assert "pg_restore" in cmd
            assert "-c" in cmd  # Clean flag

        finally:
            # Cleanup
            try:
                os.unlink(temp_path)
            except:
                pass

    @pytest.mark.asyncio
    @patch('postgres_mcp.backup.restore.RestoreBackupOperation._check_pg_tools')
    @patch('postgres_mcp.backup.restore.RestoreBackupOperation._execute_command')
    async def test_restore_plain_backup(self, mock_execute, mock_check_tools,
                                       mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test restoring a plain SQL backup."""
        mock_check_tools.return_value = True
        mock_execute.return_value = (True, "Restore completed", "")

        # Create test SQL file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql') as tmp:
            tmp.write("CREATE TABLE test (id INT);")
            temp_path = tmp.name

        try:
            mock_connection = AsyncMock()

            mock_connection_pool.acquire = AsyncMock()
            mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
            mock_connection_pool.acquire().__aexit__ = AsyncMock()

            restore_op = RestoreBackupOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

            result = await restore_op.restore_backup(
                backup_path=temp_path,
                target_database="restored_db",
                role_name="test_role"
            )

            assert result.success is True

            # Verify psql was used for plain backup
            mock_execute.assert_called_once()
            cmd = mock_execute.call_args[0][0]
            assert "psql" in cmd

        finally:
            # Cleanup
            try:
                os.unlink(temp_path)
            except:
                pass

    @pytest.mark.asyncio
    async def test_restore_nonexistent_backup(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test restoring a nonexistent backup file."""
        restore_op = RestoreBackupOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await restore_op.restore_backup(
            backup_path="/nonexistent/backup.dump",
            target_database="test_db",
            role_name="test_role"
        )

        assert result.success is False
        assert "not found" in result.message.lower()


class TestManageBackupsOperation:
    """Test backup management operations."""

    @pytest.mark.asyncio
    async def test_list_backups(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger,
                               sample_backup_metadata):
        """Test listing available backups."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test backup files
            backup_path = Path(tmpdir) / "test_backup.dump"
            backup_path.write_bytes(b"test backup content")

            metadata_path = Path(tmpdir) / "test_backup.dump.metadata.json"
            metadata_path.write_text(json.dumps(sample_backup_metadata.to_dict()))

            manage_op = ManageBackupsOperation(
                mock_connection_pool, mock_rbac_manager, mock_audit_logger,
                backup_storage_path=tmpdir
            )

            result = await manage_op.list_backups(role_name="test_role")

            assert result.success is True
            assert "Found" in result.message

    @pytest.mark.asyncio
    async def test_validate_backup(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test backup validation."""
        with tempfile.NamedTemporaryFile(delete=False, suffix='.dump') as tmp:
            tmp.write(b"PGDMP")  # Mock custom format header
            temp_path = tmp.name

        try:
            manage_op = ManageBackupsOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

            result = await manage_op.validate_backup(
                backup_path=temp_path,
                role_name="test_role"
            )

            assert result.success is True
            assert "validation" in result.message.lower()

        finally:
            # Cleanup
            try:
                os.unlink(temp_path)
            except:
                pass

    @pytest.mark.asyncio
    async def test_cleanup_old_backups(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test cleaning up old backups."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create old and new backup files
            old_backup = Path(tmpdir) / "old_backup.dump"
            old_backup.write_bytes(b"old backup")

            # Set old modification time
            old_time = datetime.utcnow().timestamp() - (400 * 24 * 3600)  # 400 days old
            os.utime(old_backup, (old_time, old_time))

            new_backup = Path(tmpdir) / "new_backup.dump"
            new_backup.write_bytes(b"new backup")

            manage_op = ManageBackupsOperation(
                mock_connection_pool, mock_rbac_manager, mock_audit_logger,
                backup_storage_path=tmpdir
            )

            # First test dry run
            result = await manage_op.cleanup_old_backups(
                retention_days=365,
                role_name="test_role",
                dry_run=True
            )

            assert result.success is True
            assert "Would delete" in result.message
            assert old_backup.exists()  # Should still exist (dry run)

            # Now test actual deletion
            result = await manage_op.cleanup_old_backups(
                retention_days=365,
                role_name="test_role",
                dry_run=False
            )

            assert result.success is True
            assert "Deleted" in result.message
            assert not old_backup.exists()  # Should be deleted
            assert new_backup.exists()  # Should still exist

    @pytest.mark.asyncio
    async def test_get_backup_info(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger,
                                  sample_backup_metadata):
        """Test getting detailed backup information."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test backup with metadata
            backup_path = Path(tmpdir) / "test_backup.dump"
            backup_path.write_bytes(b"PGDMP")  # Mock custom format

            metadata_path = Path(tmpdir) / "test_backup.dump.metadata.json"
            metadata_path.write_text(json.dumps(sample_backup_metadata.to_dict()))

            manage_op = ManageBackupsOperation(
                mock_connection_pool, mock_rbac_manager, mock_audit_logger,
                backup_storage_path=tmpdir
            )

            result = await manage_op.get_backup_info(
                backup_path=str(backup_path),
                role_name="test_role"
            )

            assert result.success is True
            assert "information" in result.message.lower()


class TestBackupMetadata:
    """Test backup metadata operations."""

    def test_metadata_serialization(self, sample_backup_metadata):
        """Test metadata serialization and deserialization."""
        # Convert to dict
        metadata_dict = sample_backup_metadata.to_dict()

        assert metadata_dict["backup_id"] == sample_backup_metadata.backup_id
        assert metadata_dict["database_name"] == sample_backup_metadata.database_name
        assert metadata_dict["backup_format"] == sample_backup_metadata.backup_format.value

        # Convert back from dict
        restored = BackupMetadata.from_dict(metadata_dict)

        assert restored.backup_id == sample_backup_metadata.backup_id
        assert restored.database_name == sample_backup_metadata.database_name
        assert restored.backup_format == sample_backup_metadata.backup_format

    def test_backup_format_detection(self):
        """Test backup format detection from file."""
        from postgres_mcp.backup.restore import RestoreBackupOperation

        restore_op = RestoreBackupOperation(None, None, None)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Test directory format
            dir_path = Path(tmpdir) / "backup_dir"
            dir_path.mkdir()
            assert asyncio.run(restore_op._detect_backup_format(str(dir_path), None)) == BackupFormat.DIRECTORY

            # Test plain SQL format
            sql_path = Path(tmpdir) / "backup.sql"
            sql_path.write_text("CREATE TABLE test;")
            assert asyncio.run(restore_op._detect_backup_format(str(sql_path), None)) == BackupFormat.PLAIN

            # Test custom format
            custom_path = Path(tmpdir) / "backup.dump"
            custom_path.write_bytes(b"PGDMP")
            assert asyncio.run(restore_op._detect_backup_format(str(custom_path), None)) == BackupFormat.CUSTOM