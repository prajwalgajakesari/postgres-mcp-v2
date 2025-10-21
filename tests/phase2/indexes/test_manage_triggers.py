"""Tests for ManageTriggersOperation."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from postgres_mcp.indexes import ManageTriggersOperation
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
def triggers_op(pool_mock, audit_logger):
    """Create ManageTriggersOperation instance."""
    return ManageTriggersOperation(pool_mock, audit_logger)


class TestManageTriggersOperation:
    """Test ManageTriggersOperation class."""

    def test_get_operation_type(self, triggers_op):
        """Test operation type."""
        assert triggers_op.get_operation_type() == OperationType.DDL

    def test_get_required_permissions_create(self, triggers_op):
        """Test required permissions for CREATE."""
        triggers_op.current_operation = 'CREATE'
        assert triggers_op.get_required_permissions() == [Permission.CREATE_TRIGGER]

    def test_get_required_permissions_drop(self, triggers_op):
        """Test required permissions for DROP."""
        triggers_op.current_operation = 'DROP'
        assert triggers_op.get_required_permissions() == [Permission.DROP_TRIGGER]

    @pytest.mark.asyncio
    async def test_validate_create_success(self, triggers_op):
        """Test successful validation for CREATE."""
        is_valid, error = await triggers_op.validate(
            operation="CREATE",
            trigger_name="test_trigger",
            table_name="test_table",
            function_name="test_function",
            timing="BEFORE",
            events=["INSERT", "UPDATE"]
        )
        assert is_valid is True
        assert error is None

    @pytest.mark.asyncio
    async def test_validate_invalid_operation(self, triggers_op):
        """Test validation with invalid operation."""
        is_valid, error = await triggers_op.validate(
            operation="INVALID",
            trigger_name="test_trigger",
            table_name="test_table"
        )
        assert is_valid is False
        assert "Operation must be CREATE, DROP, or ALTER" in error

    @pytest.mark.asyncio
    async def test_validate_missing_trigger_name(self, triggers_op):
        """Test validation with missing trigger name."""
        is_valid, error = await triggers_op.validate(
            operation="CREATE",
            table_name="test_table"
        )
        assert is_valid is False
        assert "Trigger name is required" in error

    @pytest.mark.asyncio
    async def test_validate_missing_table_name(self, triggers_op):
        """Test validation with missing table name."""
        is_valid, error = await triggers_op.validate(
            operation="CREATE",
            trigger_name="test_trigger"
        )
        assert is_valid is False
        assert "Table name is required" in error

    @pytest.mark.asyncio
    async def test_validate_create_missing_function(self, triggers_op):
        """Test CREATE validation with missing function name."""
        is_valid, error = await triggers_op.validate(
            operation="CREATE",
            trigger_name="test_trigger",
            table_name="test_table",
            timing="BEFORE",
            events=["INSERT"]
        )
        assert is_valid is False
        assert "Function name is required for CREATE operation" in error

    @pytest.mark.asyncio
    async def test_validate_create_invalid_timing(self, triggers_op):
        """Test CREATE validation with invalid timing."""
        is_valid, error = await triggers_op.validate(
            operation="CREATE",
            trigger_name="test_trigger",
            table_name="test_table",
            function_name="test_function",
            timing="INVALID",
            events=["INSERT"]
        )
        assert is_valid is False
        assert "Timing must be BEFORE, AFTER, or INSTEAD OF" in error

    @pytest.mark.asyncio
    async def test_validate_create_empty_events(self, triggers_op):
        """Test CREATE validation with empty events."""
        is_valid, error = await triggers_op.validate(
            operation="CREATE",
            trigger_name="test_trigger",
            table_name="test_table",
            function_name="test_function",
            timing="BEFORE",
            events=[]
        )
        assert is_valid is False
        assert "At least one event is required" in error

    @pytest.mark.asyncio
    async def test_validate_create_invalid_event(self, triggers_op):
        """Test CREATE validation with invalid event."""
        is_valid, error = await triggers_op.validate(
            operation="CREATE",
            trigger_name="test_trigger",
            table_name="test_table",
            function_name="test_function",
            timing="BEFORE",
            events=["INSERT", "INVALID"]
        )
        assert is_valid is False
        assert "Invalid event: INVALID" in error

    @pytest.mark.asyncio
    async def test_execute_create_trigger(self, triggers_op, pool_mock):
        """Test CREATE trigger execution."""
        # Mock cursor execute
        pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value.execute = AsyncMock()

        result = await triggers_op.execute(
            operation="CREATE",
            trigger_name="test_trigger",
            table_name="test_table",
            function_name="test_function",
            timing="BEFORE",
            events=["INSERT", "UPDATE"],
            schema_name="public"
        )

        assert result.success is True
        assert "created successfully" in result.message
        assert result.operation_name == "CreateTrigger"

    @pytest.mark.asyncio
    async def test_execute_drop_trigger(self, triggers_op, pool_mock):
        """Test DROP trigger execution."""
        # Mock cursor execute and check_trigger_exists
        pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value.execute = AsyncMock()

        result = await triggers_op.execute(
            operation="DROP",
            trigger_name="test_trigger",
            table_name="test_table",
            schema_name="public"
        )

        assert result.success is True
        assert "dropped successfully" in result.message
        assert result.operation_name == "DropTrigger"

    @pytest.mark.asyncio
    async def test_execute_alter_trigger(self, triggers_op, pool_mock):
        """Test ALTER trigger execution."""
        # Mock cursor execute
        pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value.execute = AsyncMock()

        result = await triggers_op.execute(
            operation="ALTER",
            trigger_name="test_trigger",
            table_name="test_table",
            rename_to="new_trigger_name",
            schema_name="public"
        )

        assert result.success is True
        assert "renamed to" in result.message
        assert result.operation_name == "AlterTrigger"

    @pytest.mark.asyncio
    async def test_execute_with_error(self, triggers_op, pool_mock):
        """Test execution with database error."""
        # Mock cursor execute to raise exception
        pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value.execute = AsyncMock(
            side_effect=Exception("Trigger already exists")
        )

        result = await triggers_op.execute(
            operation="CREATE",
            trigger_name="test_trigger",
            table_name="test_table",
            function_name="test_function",
            timing="BEFORE",
            events=["INSERT"]
        )

        assert result.success is False
        assert "Failed to create trigger" in result.error
        assert "Trigger already exists" in result.error