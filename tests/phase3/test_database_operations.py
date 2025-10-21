"""Tests for Phase 3 Week 11: Database Management Operations."""

import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from datetime import datetime

from postgres_mcp.database import (
    CreateDatabaseOperation, DropDatabaseOperation, CloneDatabaseOperation,
    ManageDatabaseOperation, DatabaseInfo, DatabaseEncoding
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
def sample_database_info():
    """Create sample database information."""
    return DatabaseInfo(
        name="test_db",
        owner="postgres",
        encoding="UTF8",
        collate="en_US.UTF-8",
        ctype="en_US.UTF-8",
        size_bytes=10485760,
        connection_limit=-1,
        tablespace="pg_default",
        num_backends=5,
        xact_commit=1000,
        xact_rollback=10
    )


class TestCreateDatabaseOperation:
    """Test database creation operations."""

    @pytest.mark.asyncio
    async def test_create_database(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test creating a new database."""
        # Setup mock connection
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=False)  # Database doesn't exist
        mock_connection.execute = AsyncMock()
        mock_connection.fetchrow = AsyncMock(return_value={
            'name': 'new_db',
            'owner': 'test_user',
            'encoding': 'UTF8',
            'collate': 'en_US.UTF-8',
            'ctype': 'en_US.UTF-8',
            'connection_limit': -1,
            'tablespace': None,
            'template': None
        })

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        create_op = CreateDatabaseOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await create_op.create_database(
            database_name="new_db",
            role_name="test_role",
            owner="test_user",
            encoding="UTF8"
        )

        assert result.success is True
        assert "successfully" in result.message.lower()
        assert result.database_info is not None

        # Verify CREATE DATABASE was executed
        mock_connection.execute.assert_called_once()
        create_call = mock_connection.execute.call_args[0][0]
        assert "CREATE DATABASE" in create_call
        assert '"new_db"' in create_call  # Quoted identifier

    @pytest.mark.asyncio
    async def test_create_database_already_exists(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test creating a database that already exists."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=True)  # Database exists

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        create_op = CreateDatabaseOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await create_op.create_database(
            database_name="existing_db",
            role_name="test_role"
        )

        assert result.success is False
        assert "already exists" in result.message

    @pytest.mark.asyncio
    async def test_create_database_from_template(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test creating a database from a template."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(side_effect=[
            False,  # Target database doesn't exist
            True,   # Template database exists
            None    # Owner check
        ])
        mock_connection.fetchrow = AsyncMock(side_effect=[
            # Template info
            {
                'encoding': 'UTF8',
                'datcollate': 'en_US.UTF-8',
                'datctype': 'en_US.UTF-8',
                'datistemplate': True
            },
            # Created database info
            {
                'name': 'new_from_template',
                'owner': 'postgres',
                'encoding': 'UTF8',
                'collate': 'en_US.UTF-8',
                'ctype': 'en_US.UTF-8',
                'connection_limit': -1,
                'tablespace': None,
                'template': 'template_db'
            }
        ])
        mock_connection.execute = AsyncMock()

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        create_op = CreateDatabaseOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await create_op.create_database_from_template(
            database_name="new_from_template",
            template_database="template_db",
            role_name="test_role"
        )

        assert result.success is True
        assert "successfully" in result.message.lower()

    @pytest.mark.asyncio
    async def test_create_database_with_extensions(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test creating a database with extensions."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(side_effect=[
            False,  # Database doesn't exist
            True,   # Extension 1 available
            True    # Extension 2 available
        ])
        mock_connection.execute = AsyncMock()
        mock_connection.fetchrow = AsyncMock(return_value={
            'name': 'db_with_ext',
            'owner': 'postgres',
            'encoding': 'UTF8',
            'collate': 'en_US.UTF-8',
            'ctype': 'en_US.UTF-8',
            'connection_limit': -1,
            'tablespace': None,
            'template': None
        })

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        create_op = CreateDatabaseOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await create_op.create_database_with_extensions(
            database_name="db_with_ext",
            extensions=["uuid-ossp", "pgcrypto"],
            role_name="test_role"
        )

        assert result.success is True
        assert "successfully" in result.message.lower()
        assert "extensions" in result.message.lower()


class TestDropDatabaseOperation:
    """Test database drop operations."""

    @pytest.mark.asyncio
    async def test_drop_database(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger,
                                sample_database_info):
        """Test dropping a database."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=True)  # Database exists
        mock_connection.fetchrow = AsyncMock(side_effect=[
            # Database info
            {
                'name': 'drop_db',
                'owner': 'postgres',
                'encoding': 'UTF8',
                'collate': 'en_US.UTF-8',
                'ctype': 'en_US.UTF-8',
                'connection_limit': -1,
                'tablespace': None,
                'template': None
            },
            # Activity check
            {
                'total_connections': 0,
                'active_connections': 0,
                'idle_connections': 0,
                'idle_in_transaction': 0,
                'oldest_connection': None
            }
        ])
        mock_connection.execute = AsyncMock()

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        drop_op = DropDatabaseOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await drop_op.drop_database(
            database_name="drop_db",
            role_name="test_role"
        )

        assert result.success is True
        assert "successfully" in result.message.lower()

        # Verify DROP DATABASE was executed
        drop_calls = [call for call in mock_connection.execute.call_args_list
                     if 'DROP DATABASE' in str(call)]
        assert len(drop_calls) > 0

    @pytest.mark.asyncio
    async def test_drop_database_with_active_connections(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test dropping a database with active connections."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=True)  # Database exists
        mock_connection.fetchrow = AsyncMock(side_effect=[
            # Database info
            {'name': 'busy_db', 'owner': 'postgres', 'encoding': 'UTF8',
             'collate': 'en_US.UTF-8', 'ctype': 'en_US.UTF-8',
             'connection_limit': -1, 'tablespace': None, 'template': None},
            # Activity check - has active connections
            {'total_connections': 3, 'active_connections': 2,
             'idle_connections': 1, 'idle_in_transaction': 0,
             'oldest_connection': datetime.utcnow()}
        ])
        mock_connection.fetch = AsyncMock(return_value=[
            {'pid': 123}, {'pid': 124}
        ])
        mock_connection.execute = AsyncMock()

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        drop_op = DropDatabaseOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        # Test without force - should fail
        result = await drop_op.drop_database(
            database_name="busy_db",
            role_name="test_role",
            force=False
        )

        assert result.success is False
        assert "active connections" in result.message.lower()

        # Test with force - should succeed
        result = await drop_op.drop_database(
            database_name="busy_db",
            role_name="test_role",
            force=True
        )

        assert result.success is True
        assert "terminated" in result.message.lower()

    @pytest.mark.asyncio
    async def test_drop_system_database(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test attempting to drop a system database."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=True)  # Database exists

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        drop_op = DropDatabaseOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await drop_op.drop_database(
            database_name="postgres",
            role_name="test_role"
        )

        assert result.success is False
        assert "system database" in result.message.lower()


class TestCloneDatabaseOperation:
    """Test database cloning operations."""

    @pytest.mark.asyncio
    @patch('postgres_mcp.database.clone.CloneDatabaseOperation._clone_using_template')
    async def test_clone_database_template_method(self, mock_clone_template,
                                                 mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test cloning a database using template method."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(side_effect=[
            True,   # Source database exists
            False   # Target database doesn't exist
        ])
        mock_connection.fetchrow = AsyncMock(side_effect=[
            # Source database info
            {'name': 'source_db', 'owner': 'postgres', 'encoding': 'UTF8',
             'collate': 'en_US.UTF-8', 'ctype': 'en_US.UTF-8',
             'connection_limit': -1, 'tablespace': None, 'template': None},
            # Activity check
            {'total_connections': 0, 'active_connections': 0,
             'idle_connections': 0, 'idle_in_transaction': 0,
             'oldest_connection': None}
        ])

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        # Mock successful template clone
        from postgres_mcp.database import DatabaseOperationResult, DatabaseInfo
        mock_clone_template.return_value = DatabaseOperationResult(
            success=True,
            message="Clone successful",
            database_info=DatabaseInfo(
                name="cloned_db",
                owner="postgres",
                encoding="UTF8",
                collate="en_US.UTF-8",
                ctype="en_US.UTF-8"
            )
        )

        clone_op = CloneDatabaseOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await clone_op.clone_database(
            source_database="source_db",
            target_database="cloned_db",
            role_name="test_role"
        )

        assert result.success is True
        mock_clone_template.assert_called_once()

    @pytest.mark.asyncio
    async def test_clone_database_structure_only(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test cloning database structure without data."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(side_effect=[
            True,   # Source database exists
            False   # Target database doesn't exist
        ])
        mock_connection.fetchrow = AsyncMock(return_value={
            'name': 'source_db', 'owner': 'postgres', 'encoding': 'UTF8',
            'collate': 'en_US.UTF-8', 'ctype': 'en_US.UTF-8',
            'connection_limit': -1, 'tablespace': None, 'template': None
        })

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        with patch('postgres_mcp.database.clone.CloneDatabaseOperation._clone_using_dump_restore') as mock_dump:
            from postgres_mcp.database import DatabaseOperationResult
            mock_dump.return_value = DatabaseOperationResult(
                success=True,
                message="Structure cloned"
            )

            clone_op = CloneDatabaseOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

            result = await clone_op.clone_database_structure(
                source_database="source_db",
                target_database="structure_only_db",
                role_name="test_role"
            )

            assert result.success is True
            # Verify include_data was False
            call_kwargs = mock_dump.call_args[1]
            assert call_kwargs['include_data'] is False


class TestManageDatabaseOperation:
    """Test database management operations."""

    @pytest.mark.asyncio
    async def test_list_databases(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test listing databases."""
        mock_connection = AsyncMock()
        mock_connection.fetch = AsyncMock(return_value=[
            {'name': 'db1', 'owner': 'user1', 'encoding': 'UTF8',
             'collate': 'en_US.UTF-8', 'ctype': 'en_US.UTF-8',
             'connection_limit': -1, 'is_template': False,
             'allow_connections': True, 'tablespace': 'pg_default'},
            {'name': 'db2', 'owner': 'user2', 'encoding': 'UTF8',
             'collate': 'en_US.UTF-8', 'ctype': 'en_US.UTF-8',
             'connection_limit': 10, 'is_template': False,
             'allow_connections': True, 'tablespace': 'pg_default'}
        ])
        mock_connection.fetchval = AsyncMock(return_value=1048576)  # Database size
        mock_connection.fetchrow = AsyncMock(return_value={
            'numbackends': 2, 'xact_commit': 100, 'xact_rollback': 5,
            'blks_read': 1000, 'blks_hit': 9000, 'tup_returned': 10000,
            'tup_fetched': 5000, 'tup_inserted': 100, 'tup_updated': 50,
            'tup_deleted': 10, 'conflicts': 0, 'temp_files': 2,
            'temp_bytes': 1024, 'deadlocks': 0, 'stats_reset': datetime.utcnow()
        })

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        manage_op = ManageDatabaseOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await manage_op.list_databases(
            role_name="test_role",
            include_statistics=True
        )

        assert result.success is True
        assert "Found 2 databases" in result.message
        assert result.affected_databases == ['db1', 'db2']

    @pytest.mark.asyncio
    async def test_vacuum_database(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test vacuum database operation."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=True)  # Database exists
        mock_connection.execute = AsyncMock()

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        manage_op = ManageDatabaseOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await manage_op.vacuum_database(
            database_name="test_db",
            role_name="test_role",
            full=True,
            analyze=True
        )

        assert result.success is True
        assert "completed" in result.message.lower()

        # Verify VACUUM command was executed
        vacuum_calls = [call for call in mock_connection.execute.call_args_list
                       if 'VACUUM' in str(call)]
        assert len(vacuum_calls) > 0

    @pytest.mark.asyncio
    async def test_get_database_activity(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test getting database activity."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=True)  # Database exists
        mock_connection.fetch = AsyncMock(return_value=[
            {'pid': 123, 'usename': 'user1', 'application_name': 'psql',
             'client_addr': '127.0.0.1', 'client_port': 5000,
             'backend_start': datetime.utcnow(), 'xact_start': None,
             'query_start': datetime.utcnow(), 'state_change': datetime.utcnow(),
             'state': 'active', 'backend_xmin': None,
             'query': 'SELECT * FROM test', 'wait_event_type': None,
             'wait_event': None},
            {'pid': 124, 'usename': 'user2', 'application_name': 'app',
             'client_addr': '192.168.1.100', 'client_port': 5001,
             'backend_start': datetime.utcnow(), 'xact_start': None,
             'query_start': None, 'state_change': datetime.utcnow(),
             'state': 'idle', 'backend_xmin': None,
             'query': None, 'wait_event_type': None,
             'wait_event': None}
        ])

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        manage_op = ManageDatabaseOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await manage_op.get_database_activity(
            database_name="test_db",
            role_name="test_role"
        )

        assert result.success is True
        assert "2 connections" in result.message