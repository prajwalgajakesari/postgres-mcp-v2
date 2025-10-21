"""Tests for CreateIndexOperation."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from postgres_mcp.indexes import CreateIndexOperation
from postgres_mcp.permissions import Permission
from postgres_mcp.operations import OperationType


@pytest.fixture
def pool_mock():
    """Mock connection pool."""
    pool = MagicMock()
    conn = AsyncMock()
    cur = AsyncMock()

    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)
    conn.cursor.return_value.__aenter__ = AsyncMock(return_value=cur)
    conn.cursor.return_value.__aexit__ = AsyncMock(return_value=None)

    return pool


@pytest.fixture
def audit_logger():
    """Mock audit logger."""
    return MagicMock()


@pytest.fixture
def create_index_op(pool_mock, audit_logger):
    """Create CreateIndexOperation instance."""
    from postgres_mcp.permissions import RBACManager

    rbac_manager = RBACManager()
    rbac_manager.create_custom_role("default", permissions=["*"])

    return CreateIndexOperation(pool_mock, rbac_manager, audit_logger)


class TestCreateIndexOperation:
    """Test CreateIndexOperation class."""

    def test_get_operation_type(self, create_index_op):
        """Test operation type."""
        assert create_index_op.get_operation_type() == OperationType.DDL

    def test_get_required_permissions(self, create_index_op):
        """Test required permissions."""
        assert create_index_op.get_required_permissions() == [Permission.CREATE_INDEX]

    @pytest.mark.asyncio
    async def test_validate_success(self, create_index_op):
        """Test successful validation."""
        is_valid, error = await create_index_op.validate(
            index_name="test_idx",
            table_name="test_table",
            columns=["col1", "col2"],
            index_type="btree"
        )
        assert is_valid is True
        assert error is None

    @pytest.mark.asyncio
    async def test_validate_missing_index_name(self, create_index_op):
        """Test validation with missing index name."""
        is_valid, error = await create_index_op.validate(
            table_name="test_table",
            columns=["col1"]
        )
        assert is_valid is False
        assert "Index name is required" in error

    @pytest.mark.asyncio
    async def test_validate_missing_table_name(self, create_index_op):
        """Test validation with missing table name."""
        is_valid, error = await create_index_op.validate(
            index_name="test_idx",
            columns=["col1"]
        )
        assert is_valid is False
        assert "Table name is required" in error

    @pytest.mark.asyncio
    async def test_validate_empty_columns(self, create_index_op):
        """Test validation with empty columns."""
        is_valid, error = await create_index_op.validate(
            index_name="test_idx",
            table_name="test_table",
            columns=[]
        )
        assert is_valid is False
        assert "At least one column is required" in error

    @pytest.mark.asyncio
    async def test_validate_invalid_index_type(self, create_index_op):
        """Test validation with invalid index type."""
        is_valid, error = await create_index_op.validate(
            index_name="test_idx",
            table_name="test_table",
            columns=["col1"],
            index_type="invalid"
        )
        assert is_valid is False
        assert "Invalid index type" in error

    @pytest.mark.asyncio
    async def test_execute_basic_index(self, create_index_op, pool_mock):
        """Test basic index creation."""
        # Mock cursor execute
        pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value.execute = AsyncMock()

        result = await create_index_op.execute(
            index_name="test_idx",
            table_name="test_table",
            columns=["col1"],
            schema_name="public"
        )

        assert result.success is True
        assert "created successfully" in result.message
        assert result.operation_name == "CreateIndex"

    @pytest.mark.asyncio
    async def test_execute_unique_index(self, create_index_op, pool_mock):
        """Test unique index creation."""
        # Mock cursor execute
        pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value.execute = AsyncMock()

        result = await create_index_op.execute(
            index_name="test_unique_idx",
            table_name="test_table",
            columns=["email"],
            unique=True,
            schema_name="public"
        )

        assert result.success is True
        assert "created successfully" in result.message

    @pytest.mark.asyncio
    async def test_execute_concurrent_index(self, create_index_op, pool_mock):
        """Test concurrent index creation."""
        # Mock cursor execute
        pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value.execute = AsyncMock()

        result = await create_index_op.execute(
            index_name="test_concurrent_idx",
            table_name="test_table",
            columns=["col1"],
            concurrent=True,
            schema_name="public"
        )

        assert result.success is True
        assert "created successfully" in result.message

    @pytest.mark.asyncio
    async def test_execute_gin_index(self, create_index_op, pool_mock):
        """Test GIN index creation."""
        # Mock cursor execute
        pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value.execute = AsyncMock()

        result = await create_index_op.execute(
            index_name="test_gin_idx",
            table_name="test_table",
            columns=["data"],
            index_type="gin",
            schema_name="public"
        )

        assert result.success is True
        assert "created successfully" in result.message

    @pytest.mark.asyncio
    async def test_execute_partial_index(self, create_index_op, pool_mock):
        """Test partial index creation."""
        # Mock cursor execute
        pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value.execute = AsyncMock()

        result = await create_index_op.execute(
            index_name="test_partial_idx",
            table_name="test_table",
            columns=["status"],
            where_clause="status = 'active'",
            schema_name="public"
        )

        assert result.success is True
        assert "created successfully" in result.message

    @pytest.mark.asyncio
    async def test_execute_with_error(self, create_index_op, pool_mock):
        """Test execution with database error."""
        # Mock cursor execute to raise exception
        pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value.execute = AsyncMock(
            side_effect=Exception("Index already exists")
        )

        result = await create_index_op.execute(
            index_name="test_idx",
            table_name="test_table",
            columns=["col1"]
        )

        assert result.success is False
        assert "Failed to create index" in result.error
        assert "Index already exists" in result.error