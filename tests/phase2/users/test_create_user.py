"""Tests for create user operation."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from postgres_mcp.users.create import CreateUserOperation
from postgres_mcp.permissions import Permission


class MockAsyncContextManager:
    """Helper class to properly mock async context managers."""

    def __init__(self, return_value):
        self.return_value = return_value

    async def __aenter__(self):
        return self.return_value

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


@pytest.mark.asyncio
class TestCreateUserOperation:
    """Test create user operation functionality."""

    @pytest.fixture
    def operation(self):
        """Create operation instance with mocked dependencies."""
        op = CreateUserOperation(
            connection_pool=MagicMock(),
            rbac_manager=MagicMock(),
            audit_logger=AsyncMock()
        )
        # Mock the check methods
        op.check_user_exists = AsyncMock(return_value=False)
        op.check_role_exists = AsyncMock(return_value=False)
        return op

    async def test_get_required_permissions(self, operation):
        """Test that CREATE_USER permission is required."""
        perms = operation.get_required_permissions()
        assert Permission.CREATE_USER in perms

    async def test_validate_success_with_username(self, operation):
        """Test successful validation with username."""
        is_valid, error = await operation.validate(
            username="testuser",
            password="StrongP@ss123"
        )

        assert is_valid is True
        assert error is None
        operation.check_user_exists.assert_called_once_with("testuser")

    async def test_validate_success_with_role(self, operation):
        """Test successful validation with role name."""
        is_valid, error = await operation.validate(
            role_name="testrole"
        )

        assert is_valid is True
        assert error is None
        operation.check_role_exists.assert_called_once_with("testrole")

    async def test_validate_missing_identifier(self, operation):
        """Test validation fails without username or role_name."""
        is_valid, error = await operation.validate()

        assert is_valid is False
        assert "Either username or role_name is required" in error

    async def test_validate_user_exists(self, operation):
        """Test validation fails if user already exists."""
        operation.check_user_exists = AsyncMock(return_value=True)

        is_valid, error = await operation.validate(
            username="existinguser"
        )

        assert is_valid is False
        assert "User 'existinguser' already exists" in error

    async def test_validate_weak_password(self, operation):
        """Test validation fails with weak password."""
        is_valid, error = await operation.validate(
            username="testuser",
            password="weak"
        )

        assert is_valid is False
        assert "Password must be at least 8 characters" in error

    async def test_validate_skip_password_validation(self, operation):
        """Test skipping password validation."""
        is_valid, error = await operation.validate(
            username="testuser",
            password="weak",
            skip_password_validation=True
        )

        assert is_valid is True
        assert error is None

    async def test_validate_invalid_connection_limit(self, operation):
        """Test validation fails with invalid connection limit."""
        is_valid, error = await operation.validate(
            username="testuser",
            connection_limit=-5
        )

        assert is_valid is False
        assert "connection_limit must be -1" in error

    async def test_build_create_user_sql_basic(self, operation):
        """Test building basic CREATE USER SQL."""
        sql, params = operation.build_create_user_sql(
            username="testuser"
        )

        assert "CREATE USER" in sql
        assert "testuser" in sql
        assert "LOGIN" in sql  # Default for CREATE USER
        assert len(params) == 0

    async def test_build_create_user_sql_with_password(self, operation):
        """Test building CREATE USER SQL with password."""
        sql, params = operation.build_create_user_sql(
            username="testuser",
            password="secret123"
        )

        assert "CREATE USER" in sql
        assert "WITH PASSWORD %s" in sql
        assert params == ["secret123"]

    async def test_build_create_role_sql(self, operation):
        """Test building CREATE ROLE SQL."""
        sql, params = operation.build_create_user_sql(
            role_name="testrole"
        )

        assert "CREATE ROLE" in sql
        assert "testrole" in sql
        # CREATE ROLE doesn't default to LOGIN
        assert "LOGIN" not in sql

    async def test_build_sql_with_options(self, operation):
        """Test building SQL with various options."""
        sql, params = operation.build_create_user_sql(
            username="testuser",
            createdb=True,
            createrole=False,
            connection_limit=10,
            valid_until="2024-12-31"
        )

        assert "CREATEDB" in sql
        assert "NOCREATEROLE" in sql
        assert "CONNECTION LIMIT 10" in sql
        assert "VALID UNTIL %s" in sql
        assert "2024-12-31" in params

    async def test_build_sql_with_role_memberships(self, operation):
        """Test building SQL with role memberships."""
        sql, params = operation.build_create_user_sql(
            username="testuser",
            in_role=["role1", "role2"],
            role="role3",
            admin="role4"
        )

        assert "IN ROLE" in sql
        assert "ROLE" in sql
        assert "ADMIN" in sql

    async def test_execute_success(self, operation):
        """Test successful user creation."""
        # Create mock cursor
        mock_cur = AsyncMock()
        mock_cur.rowcount = 1
        mock_cur.execute = AsyncMock()

        # Create mock connection with proper cursor context manager
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = MockAsyncContextManager(mock_cur)

        # Setup connection pool to return connection context manager
        operation.connection_pool.connection.return_value = MockAsyncContextManager(mock_conn)

        result = await operation.execute(
            username="testuser",
            password="StrongP@ss123"
        )

        assert result.success is True
        assert result.operation_name == "CreateUser"
        assert "user:testuser" in result.affected_objects
        assert "created successfully" in result.message
        mock_cur.execute.assert_called_once()

    async def test_execute_with_initial_grants(self, operation):
        """Test user creation with initial permission grants."""
        # Create mock cursor
        mock_cur = AsyncMock()
        mock_cur.rowcount = 1
        mock_cur.execute = AsyncMock()

        # Create mock connection with proper cursor context manager
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = MockAsyncContextManager(mock_cur)

        # Setup connection pool
        operation.connection_pool.connection.return_value = MockAsyncContextManager(mock_conn)

        result = await operation.execute(
            username="testuser",
            initial_grants=[
                {
                    "privilege": "SELECT",
                    "object_type": "TABLE",
                    "object_name": "test_table"
                }
            ]
        )

        assert result.success is True
        # Should have executed CREATE USER and GRANT
        assert mock_cur.execute.call_count == 2

    async def test_execute_duplicate_user_error(self, operation):
        """Test handling duplicate user error."""
        import psycopg

        # Create mock cursor that raises DuplicateObject
        mock_cur = AsyncMock()
        mock_cur.execute.side_effect = psycopg.errors.DuplicateObject("User exists")

        # Create mock connection
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = MockAsyncContextManager(mock_cur)

        # Setup connection pool
        operation.connection_pool.connection.return_value = MockAsyncContextManager(mock_conn)

        result = await operation.execute(
            username="testuser"
        )

        assert result.success is False
        assert "already exists" in result.error

    async def test_execute_insufficient_privilege_error(self, operation):
        """Test handling insufficient privilege error."""
        import psycopg

        # Create mock cursor that raises InsufficientPrivilege
        mock_cur = AsyncMock()
        mock_cur.execute.side_effect = psycopg.errors.InsufficientPrivilege("Permission denied")

        # Create mock connection
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = MockAsyncContextManager(mock_cur)

        # Setup connection pool
        operation.connection_pool.connection.return_value = MockAsyncContextManager(mock_conn)

        result = await operation.execute(
            username="testuser"
        )

        assert result.success is False
        assert "Insufficient privileges" in result.error

    async def test_password_strength_validation(self, operation):
        """Test password strength validation logic."""
        # Too short
        is_valid, error = operation.validate_password_strength("short")
        assert is_valid is False
        assert "at least 8 characters" in error

        # Missing uppercase
        is_valid, error = operation.validate_password_strength("lowercase123")
        assert is_valid is False
        assert "uppercase" in error

        # Missing lowercase
        is_valid, error = operation.validate_password_strength("UPPERCASE123")
        assert is_valid is False
        assert "lowercase" in error

        # Missing digit
        is_valid, error = operation.validate_password_strength("UpperLower")
        assert is_valid is False
        assert "numeric" in error

        # Strong password
        is_valid, error = operation.validate_password_strength("StrongP@ss123")
        assert is_valid is True
        assert error is None