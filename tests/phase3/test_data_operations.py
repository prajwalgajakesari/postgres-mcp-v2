"""Tests for Phase 3 Week 9: Data Import/Export Operations."""

import pytest
import asyncio
import tempfile
import os
import csv
import json
from pathlib import Path
from unittest.mock import Mock, AsyncMock, MagicMock, patch

from postgres_mcp.data import (
    CopyDataOperation, ExportQueryOperation, ImportDataOperation,
    DataFormat, DataOperationResult
)
from postgres_mcp.permissions.rbac import RBACManager, Permission


@pytest.fixture
def mock_connection_pool():
    """Create mock connection pool."""
    pool = MagicMock()
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
def temp_file():
    """Create temporary file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        f.write("id,name,value\n")
        f.write("1,test1,100\n")
        f.write("2,test2,200\n")
        temp_path = f.name

    yield temp_path

    # Cleanup
    try:
        os.unlink(temp_path)
    except:
        pass


class TestCopyDataOperation:
    """Test COPY data operations."""

    @pytest.mark.asyncio
    async def test_copy_to_file(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test copying data from table to file."""
        # Setup mock connection
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=True)  # Table exists
        mock_connection.transaction = MagicMock()
        mock_connection.cursor = AsyncMock()

        mock_cursor = AsyncMock()
        mock_copy = AsyncMock()
        mock_copy.__aenter__ = AsyncMock(return_value=mock_copy)
        mock_copy.__aexit__ = AsyncMock()
        mock_copy.__aiter__ = AsyncMock(return_value=iter([b"test,data\n"]))

        mock_cursor.copy = MagicMock(return_value=mock_copy)
        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock()
        mock_connection.cursor.return_value = mock_cursor

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        copy_op = CopyDataOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
            temp_path = tmp.name

        try:
            result = await copy_op.copy_to_file(
                table_name="test_table",
                file_path=temp_path,
                role_name="test_role",
                format_type=DataFormat.CSV
            )

            assert result.success is True
            assert "Successfully copied" in result.message
            assert result.file_path == temp_path
            assert result.format == DataFormat.CSV

            # Verify file was created
            assert os.path.exists(temp_path)

        finally:
            # Cleanup
            try:
                os.unlink(temp_path)
            except:
                pass

    @pytest.mark.asyncio
    async def test_copy_from_file(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger, temp_file):
        """Test copying data from file to table."""
        # Setup mock connection
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=True)  # Table exists
        mock_connection.execute = AsyncMock()
        mock_connection.transaction = MagicMock()
        mock_connection.cursor = AsyncMock()

        mock_cursor = AsyncMock()
        mock_copy = AsyncMock()
        mock_copy.write = AsyncMock()
        mock_copy.__aenter__ = AsyncMock(return_value=mock_copy)
        mock_copy.__aexit__ = AsyncMock()

        mock_cursor.copy = MagicMock(return_value=mock_copy)
        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock()
        mock_connection.cursor.return_value = mock_cursor

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        copy_op = CopyDataOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await copy_op.copy_from_file(
            table_name="test_table",
            file_path=temp_file,
            role_name="test_role",
            format_type=DataFormat.CSV
        )

        assert result.success is True
        assert "Successfully copied" in result.message
        assert result.file_path == temp_file

    @pytest.mark.asyncio
    async def test_copy_between_tables(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test copying data between tables."""
        # Setup mock connection
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=True)  # Tables exist
        mock_connection.execute = AsyncMock(return_value="INSERT 0 100")
        mock_connection.transaction = MagicMock()

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        copy_op = CopyDataOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await copy_op.copy_between_tables(
            source_table="source_table",
            target_table="target_table",
            role_name="test_role"
        )

        assert result.success is True
        assert "Successfully copied" in result.message
        assert result.rows_affected == 100

    @pytest.mark.asyncio
    async def test_copy_with_invalid_permissions(self, mock_connection_pool, mock_audit_logger):
        """Test copy operation with insufficient permissions."""
        mock_rbac = Mock(spec=RBACManager)
        mock_rbac.check_permission = Mock(return_value=False)

        copy_op = CopyDataOperation(mock_connection_pool, mock_rbac, mock_audit_logger)

        with pytest.raises(PermissionError):
            await copy_op.copy_to_file(
                table_name="test_table",
                file_path="/tmp/test.csv",
                role_name="test_role"
            )


class TestExportQueryOperation:
    """Test export query operations."""

    @pytest.mark.asyncio
    async def test_export_query_to_csv(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test exporting query results to CSV."""
        # Setup mock connection
        mock_connection = AsyncMock()
        mock_cursor = AsyncMock()

        # Mock cursor with results
        mock_cursor.description = [('id',), ('name',), ('value',)]
        mock_cursor.__aiter__ = lambda self: self
        mock_cursor.__anext__ = AsyncMock(side_effect=[
            (1, 'test1', 100),
            (2, 'test2', 200),
            StopAsyncIteration
        ])

        mock_connection.execute = AsyncMock(return_value=mock_cursor)
        mock_connection.transaction = MagicMock()

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        export_op = ExportQueryOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
            temp_path = tmp.name

        try:
            result = await export_op.export_query(
                query="SELECT * FROM test_table",
                file_path=temp_path,
                role_name="test_role",
                format_type=DataFormat.CSV
            )

            assert result.success is True
            assert result.rows_affected == 2
            assert result.file_path == temp_path
            assert result.format == DataFormat.CSV

            # Verify CSV content
            with open(temp_path, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 2

        finally:
            # Cleanup
            try:
                os.unlink(temp_path)
            except:
                pass

    @pytest.mark.asyncio
    async def test_export_table(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test exporting table data."""
        # Setup mock connection
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=True)  # Table exists
        mock_cursor = AsyncMock()

        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.__aiter__ = lambda self: self
        mock_cursor.__anext__ = AsyncMock(side_effect=[
            (1, 'test1'),
            StopAsyncIteration
        ])

        mock_connection.execute = AsyncMock(return_value=mock_cursor)
        mock_connection.transaction = MagicMock()

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        export_op = ExportQueryOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as tmp:
            temp_path = tmp.name

        try:
            result = await export_op.export_table(
                table_name="test_table",
                file_path=temp_path,
                role_name="test_role",
                format_type=DataFormat.JSON
            )

            assert result.success is True
            assert result.rows_affected == 1
            assert result.format == DataFormat.JSON

        finally:
            # Cleanup
            try:
                os.unlink(temp_path)
            except:
                pass

    @pytest.mark.asyncio
    async def test_export_schema_structure(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test exporting schema structure."""
        # Setup mock connection
        mock_connection = AsyncMock()

        # Mock table list
        mock_connection.fetch = AsyncMock(side_effect=[
            # Tables query
            [{'table_name': 'test_table', 'table_type': 'BASE TABLE'}],
            # Columns query
            [{'column_name': 'id', 'data_type': 'integer', 'is_nullable': 'NO',
              'column_default': None, 'character_maximum_length': None,
              'numeric_precision': 32, 'numeric_scale': 0}],
            # Constraints query
            [{'constraint_name': 'test_table_pkey', 'constraint_type': 'PRIMARY KEY'}],
            # Indexes query
            [{'indexname': 'test_table_pkey', 'indexdef': 'CREATE UNIQUE INDEX...'}],
            # Functions query
            []
        ])

        mock_connection.fetchval = AsyncMock(return_value=1000)  # Table size

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        export_op = ExportQueryOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as tmp:
            temp_path = tmp.name

        try:
            result = await export_op.export_schema_structure(
                file_path=temp_path,
                role_name="test_role"
            )

            assert result.success is True
            assert result.rows_affected == 1  # 1 table
            assert result.file_path == temp_path

            # Verify JSON structure
            with open(temp_path, 'r') as f:
                schema_data = json.load(f)
                assert 'tables' in schema_data
                assert len(schema_data['tables']) == 1
                assert schema_data['tables'][0]['name'] == 'test_table'

        finally:
            # Cleanup
            try:
                os.unlink(temp_path)
            except:
                pass


class TestImportDataOperation:
    """Test import data operations."""

    @pytest.mark.asyncio
    async def test_import_from_csv_file(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger, temp_file):
        """Test importing data from CSV file."""
        # Setup mock connection
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=True)  # Table exists
        mock_connection.execute = AsyncMock(return_value="INSERT 0 2")
        mock_connection.transaction = MagicMock()

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        import_op = ImportDataOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await import_op.import_from_file(
            table_name="test_table",
            file_path=temp_file,
            role_name="test_role",
            format_type=DataFormat.CSV
        )

        assert result.success is True
        assert result.rows_affected == 2
        assert result.file_path == temp_file

    @pytest.mark.asyncio
    async def test_import_with_validation(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test importing data with validation rules."""
        # Create test CSV file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name,age\n")
            f.write("1,Alice,25\n")
            f.write("2,Bob,150\n")  # Invalid age
            f.write("3,Charlie,30\n")
            temp_path = f.name

        try:
            # Setup mock connection
            mock_connection = AsyncMock()
            mock_connection.fetchval = AsyncMock(return_value=True)
            mock_connection.execute = AsyncMock(return_value="INSERT 0 2")
            mock_connection.transaction = MagicMock()

            mock_connection_pool.acquire = AsyncMock()
            mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
            mock_connection_pool.acquire().__aexit__ = AsyncMock()

            import_op = ImportDataOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

            validation_rules = {
                'age': {
                    'type': 'int',
                    'min': 0,
                    'max': 120
                }
            }

            with tempfile.NamedTemporaryFile(delete=False, suffix='_reject.csv') as reject:
                reject_path = reject.name

            try:
                result = await import_op.import_with_validation(
                    table_name="test_table",
                    file_path=temp_path,
                    validation_rules=validation_rules,
                    role_name="test_role",
                    format_type=DataFormat.CSV,
                    reject_file=reject_path
                )

                # Should have 2 valid rows (1 and 3)
                assert result.rows_affected == 2
                assert result.metadata['valid_rows'] == 2
                assert result.metadata['invalid_rows'] == 1

            finally:
                # Cleanup reject file
                try:
                    os.unlink(reject_path)
                except:
                    pass

        finally:
            # Cleanup test file
            try:
                os.unlink(temp_path)
            except:
                pass

    @pytest.mark.asyncio
    async def test_import_with_auto_create_table(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test importing with automatic table creation."""
        # Create test JSON file
        test_data = [
            {"id": 1, "name": "Test1", "active": True},
            {"id": 2, "name": "Test2", "active": False}
        ]

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            json.dump(test_data, f)
            temp_path = f.name

        try:
            # Setup mock connection
            mock_connection = AsyncMock()
            mock_connection.fetchval = AsyncMock(return_value=False)  # Table doesn't exist
            mock_connection.execute = AsyncMock()  # For CREATE TABLE and INSERT
            mock_connection.transaction = MagicMock()

            mock_connection_pool.acquire = AsyncMock()
            mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
            mock_connection_pool.acquire().__aexit__ = AsyncMock()

            import_op = ImportDataOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

            result = await import_op.import_from_file(
                table_name="new_table",
                file_path=temp_path,
                role_name="test_role",
                format_type=DataFormat.JSON,
                create_table=True
            )

            assert result.success is True
            assert result.rows_affected == 2

            # Verify CREATE TABLE was called
            create_calls = [call for call in mock_connection.execute.call_args_list
                          if 'CREATE TABLE' in str(call)]
            assert len(create_calls) > 0

        finally:
            # Cleanup
            try:
                os.unlink(temp_path)
            except:
                pass


class TestDataFormats:
    """Test different data format handlers."""

    def test_csv_format(self):
        """Test CSV format parsing and formatting."""
        from postgres_mcp.data.base import CSVHandler

        handler = CSVHandler()
        assert handler.format_type == DataFormat.CSV

        # Test parsing
        csv_content = "name,age\nAlice,30\nBob,25"
        loop = asyncio.get_event_loop()
        parsed = loop.run_until_complete(handler.parse_data(csv_content))

        assert len(parsed) == 2
        assert parsed[0] == {'name': 'Alice', 'age': '30'}
        assert parsed[1] == {'name': 'Bob', 'age': '25'}

        # Test formatting
        formatted = loop.run_until_complete(handler.format_data(parsed))
        assert 'name,age' in formatted
        assert 'Alice,30' in formatted

    def test_json_format(self):
        """Test JSON format parsing and formatting."""
        from postgres_mcp.data.base import JSONHandler

        handler = JSONHandler()
        assert handler.format_type == DataFormat.JSON

        # Test parsing
        json_content = '[{"name": "Alice", "age": 30}]'
        loop = asyncio.get_event_loop()
        parsed = loop.run_until_complete(handler.parse_data(json_content))

        assert len(parsed) == 1
        assert parsed[0] == {'name': 'Alice', 'age': 30}

        # Test formatting
        formatted = loop.run_until_complete(handler.format_data(parsed))
        parsed_formatted = json.loads(formatted)
        assert parsed_formatted == parsed

    def test_jsonl_format(self):
        """Test JSONL (JSON Lines) format."""
        from postgres_mcp.data.base import JSONLHandler

        handler = JSONLHandler()
        assert handler.format_type == DataFormat.JSONL

        # Test parsing
        jsonl_content = '{"name": "Alice", "age": 30}\n{"name": "Bob", "age": 25}'
        loop = asyncio.get_event_loop()
        parsed = loop.run_until_complete(handler.parse_data(jsonl_content))

        assert len(parsed) == 2
        assert parsed[0] == {'name': 'Alice', 'age': 30}
        assert parsed[1] == {'name': 'Bob', 'age': 25}

        # Test formatting
        formatted = loop.run_until_complete(handler.format_data(parsed))
        lines = formatted.strip().split('\n')
        assert len(lines) == 2
        assert json.loads(lines[0]) == parsed[0]