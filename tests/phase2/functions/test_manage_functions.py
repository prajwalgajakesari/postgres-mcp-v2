"""Tests for ManageFunctionsOperation."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from postgres_mcp.functions import ManageFunctionsOperation
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
def functions_op(pool_mock, audit_logger):
    """Create ManageFunctionsOperation instance."""
    return ManageFunctionsOperation(pool_mock, audit_logger)


class TestManageFunctionsOperation:
    """Test ManageFunctionsOperation class."""

    def test_get_operation_type(self, functions_op):
        """Test operation type."""
        assert functions_op.get_operation_type() == OperationType.DDL

    def test_get_required_permissions_create(self, functions_op):
        """Test required permissions for CREATE."""
        functions_op.current_operation = 'CREATE'
        assert functions_op.get_required_permissions() == [Permission.CREATE_FUNCTION]

    def test_get_required_permissions_drop(self, functions_op):
        """Test required permissions for DROP."""
        functions_op.current_operation = 'DROP'
        assert functions_op.get_required_permissions() == [Permission.DROP_FUNCTION]

    @pytest.mark.asyncio
    async def test_validate_create_success(self, functions_op):
        """Test successful validation for CREATE."""
        is_valid, error = await functions_op.validate(
            operation="CREATE",
            function_name="test_function",
            body="BEGIN RETURN 1; END;",
            language="plpgsql"
        )
        assert is_valid is True
        assert error is None

    @pytest.mark.asyncio
    async def test_validate_invalid_operation(self, functions_op):
        """Test validation with invalid operation."""
        is_valid, error = await functions_op.validate(
            operation="INVALID",
            function_name="test_function"
        )
        assert is_valid is False
        assert "Operation must be CREATE, DROP, or ALTER" in error

    @pytest.mark.asyncio
    async def test_validate_missing_function_name(self, functions_op):
        """Test validation with missing function name."""
        is_valid, error = await functions_op.validate(
            operation="CREATE",
            body="BEGIN RETURN 1; END;"
        )
        assert is_valid is False
        assert "Function name is required" in error

    @pytest.mark.asyncio
    async def test_validate_create_invalid_language(self, functions_op):
        """Test CREATE validation with invalid language."""
        is_valid, error = await functions_op.validate(
            operation="CREATE",
            function_name="test_function",
            body="BEGIN RETURN 1; END;",
            language="invalid"
        )
        assert is_valid is False
        assert "Invalid language" in error

    @pytest.mark.asyncio
    async def test_validate_create_missing_body(self, functions_op):
        """Test CREATE validation with missing body."""
        is_valid, error = await functions_op.validate(
            operation="CREATE",
            function_name="test_function",
            language="plpgsql"
        )
        assert is_valid is False
        assert "Function body is required" in error

    @pytest.mark.asyncio
    async def test_execute_create_function(self, functions_op, pool_mock):
        """Test CREATE function execution."""
        # Mock cursor execute and get_function_info
        cursor_mock = pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value
        cursor_mock.execute = AsyncMock()

        # Mock get_function_info method
        functions_op.get_function_info = AsyncMock(return_value={
            "name": "test_function",
            "language": "plpgsql",
            "return_type": "integer"
        })

        result = await functions_op.execute(
            operation="CREATE",
            function_name="test_function",
            body="BEGIN RETURN 1; END;",
            return_type="integer",
            language="plpgsql",
            schema_name="public"
        )

        assert result.success is True
        assert "created successfully" in result.message
        assert result.operation_name == "CreateFunction"

    @pytest.mark.asyncio
    async def test_execute_create_function_with_parameters(self, functions_op, pool_mock):
        """Test CREATE function with parameters execution."""
        # Mock cursor execute and get_function_info
        cursor_mock = pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value
        cursor_mock.execute = AsyncMock()

        functions_op.get_function_info = AsyncMock(return_value={
            "name": "test_function",
            "language": "plpgsql",
            "return_type": "integer"
        })

        parameters = [
            {"name": "param1", "type": "integer"},
            {"name": "param2", "type": "text", "default": "default_value"}
        ]

        result = await functions_op.execute(
            operation="CREATE",
            function_name="test_function",
            body="BEGIN RETURN param1; END;",
            parameters=parameters,
            return_type="integer",
            language="plpgsql",
            schema_name="public"
        )

        assert result.success is True
        assert "created successfully" in result.message

    @pytest.mark.asyncio
    async def test_execute_drop_function(self, functions_op, pool_mock):
        """Test DROP function execution."""
        # Mock cursor execute and function info methods
        cursor_mock = pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value
        cursor_mock.execute = AsyncMock()

        functions_op.check_function_exists = AsyncMock(return_value=True)
        functions_op.get_function_info = AsyncMock(return_value={
            "name": "test_function",
            "language": "plpgsql"
        })

        result = await functions_op.execute(
            operation="DROP",
            function_name="test_function",
            schema_name="public"
        )

        assert result.success is True
        assert "dropped successfully" in result.message
        assert result.operation_name == "DropFunction"

    @pytest.mark.asyncio
    async def test_execute_drop_function_if_exists(self, functions_op, pool_mock):
        """Test DROP function with IF EXISTS when function doesn't exist."""
        # Mock check_function_exists to return False
        functions_op.check_function_exists = AsyncMock(return_value=False)

        result = await functions_op.execute(
            operation="DROP",
            function_name="nonexistent_function",
            if_exists=True,
            schema_name="public"
        )

        assert result.success is True
        assert "does not exist, skipping" in result.message
        assert result.operation_name == "DropFunction"

    @pytest.mark.asyncio
    async def test_execute_alter_function(self, functions_op, pool_mock):
        """Test ALTER function execution."""
        # Mock cursor execute and get_function_info
        cursor_mock = pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value
        cursor_mock.execute = AsyncMock()

        functions_op.get_function_info = AsyncMock(return_value={
            "name": "new_function_name",
            "language": "plpgsql"
        })

        result = await functions_op.execute(
            operation="ALTER",
            function_name="test_function",
            rename_to="new_function_name",
            schema_name="public"
        )

        assert result.success is True
        assert "altered successfully" in result.message
        assert result.operation_name == "AlterFunction"

    @pytest.mark.asyncio
    async def test_execute_with_error(self, functions_op, pool_mock):
        """Test execution with database error."""
        # Mock cursor execute to raise exception
        cursor_mock = pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value
        cursor_mock.execute = AsyncMock(side_effect=Exception("Function already exists"))

        result = await functions_op.execute(
            operation="CREATE",
            function_name="test_function",
            body="BEGIN RETURN 1; END;",
            language="plpgsql"
        )

        assert result.success is False
        assert "Failed to create function" in result.error
        assert "Function already exists" in result.error

    def test_format_parameter_list_empty(self, functions_op):
        """Test parameter list formatting with empty parameters."""
        result = functions_op.format_parameter_list([])
        assert result == "()"

    def test_format_parameter_list_basic(self, functions_op):
        """Test parameter list formatting with basic parameters."""
        parameters = [
            {"name": "param1", "type": "integer"},
            {"name": "param2", "type": "text"}
        ]
        result = functions_op.format_parameter_list(parameters)
        assert "param1 integer" in result
        assert "param2 text" in result

    def test_format_parameter_list_with_mode_and_default(self, functions_op):
        """Test parameter list formatting with mode and default values."""
        parameters = [
            {"name": "param1", "type": "integer", "mode": "IN"},
            {"name": "param2", "type": "text", "default": "test"}
        ]
        result = functions_op.format_parameter_list(parameters)
        assert "IN param1 integer" in result
        assert "param2 text DEFAULT 'test'" in result

    def test_validate_language_valid(self, functions_op):
        """Test language validation with valid language."""
        is_valid, error = functions_op.validate_language("plpgsql")
        assert is_valid is True
        assert error is None

    def test_validate_language_invalid(self, functions_op):
        """Test language validation with invalid language."""
        is_valid, error = functions_op.validate_language("invalid")
        assert is_valid is False
        assert "Invalid language" in error