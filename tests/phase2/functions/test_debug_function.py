"""Tests for DebugFunctionOperation."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from postgres_mcp.functions import DebugFunctionOperation
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
def debug_op(pool_mock, audit_logger):
    """Create DebugFunctionOperation instance."""
    return DebugFunctionOperation(pool_mock, audit_logger)


class TestDebugFunctionOperation:
    """Test DebugFunctionOperation class."""

    def test_get_operation_type(self, debug_op):
        """Test operation type."""
        assert debug_op.get_operation_type() == OperationType.DDL

    def test_get_required_permissions(self, debug_op):
        """Test required permissions."""
        assert debug_op.get_required_permissions() == [Permission.SELECT]

    @pytest.mark.asyncio
    async def test_validate_success(self, debug_op):
        """Test successful validation."""
        is_valid, error = await debug_op.validate(
            function_name="test_function",
            action="ANALYZE"
        )
        assert is_valid is True
        assert error is None

    @pytest.mark.asyncio
    async def test_validate_missing_function_name(self, debug_op):
        """Test validation with missing function name."""
        is_valid, error = await debug_op.validate(
            action="ANALYZE"
        )
        assert is_valid is False
        assert "Function name is required" in error

    @pytest.mark.asyncio
    async def test_validate_invalid_action(self, debug_op):
        """Test validation with invalid action."""
        is_valid, error = await debug_op.validate(
            function_name="test_function",
            action="INVALID"
        )
        assert is_valid is False
        assert "Action must be ANALYZE, EXPLAIN, PROFILE, or DEPENDENCIES" in error

    @pytest.mark.asyncio
    async def test_execute_analyze_function(self, debug_op, pool_mock):
        """Test ANALYZE function execution."""
        # Mock cursor execute and get_function_info
        cursor_mock = pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value
        cursor_mock.execute = AsyncMock()
        cursor_mock.fetchone = AsyncMock(return_value=["CREATE FUNCTION test_function() RETURNS integer AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql;"])

        debug_op.get_function_info = AsyncMock(return_value={
            "name": "test_function",
            "language": "plpgsql",
            "return_type": "integer",
            "volatility": "v",
            "is_strict": False,
            "security_definer": False
        })

        result = await debug_op.execute(
            action="ANALYZE",
            function_name="test_function",
            schema_name="public"
        )

        assert result.success is True
        assert "analyzed successfully" in result.message
        assert result.operation_name == "AnalyzeFunction"
        assert "complexity" in result.details
        assert "issues" in result.details

    @pytest.mark.asyncio
    async def test_execute_analyze_function_not_found(self, debug_op, pool_mock):
        """Test ANALYZE function when function doesn't exist."""
        debug_op.get_function_info = AsyncMock(return_value=None)

        result = await debug_op.execute(
            action="ANALYZE",
            function_name="nonexistent_function",
            schema_name="public"
        )

        assert result.success is False
        assert "not found" in result.error

    @pytest.mark.asyncio
    async def test_execute_explain_function(self, debug_op, pool_mock):
        """Test EXPLAIN function execution."""
        cursor_mock = pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value
        cursor_mock.execute = AsyncMock()
        cursor_mock.fetchall = AsyncMock(return_value=[
            ["Index Scan using idx_test on table"],
            ["  Index Cond: (id = 1)"],
            ["  Rows: 1  Width: 4  Cost: 0.00..8.27"]
        ])

        result = await debug_op.execute(
            action="EXPLAIN",
            function_name="test_function",
            parameters=[{"value": "1"}],
            schema_name="public"
        )

        assert result.success is True
        assert "execution plan generated" in result.message
        assert result.operation_name == "ExplainFunction"
        assert "execution_plan" in result.details

    @pytest.mark.asyncio
    async def test_execute_profile_function(self, debug_op, pool_mock):
        """Test PROFILE function execution."""
        cursor_mock = pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value
        cursor_mock.execute = AsyncMock()
        cursor_mock.fetchall = AsyncMock(return_value=[[1]])

        result = await debug_op.execute(
            action="PROFILE",
            function_name="test_function",
            iterations=10,
            schema_name="public"
        )

        assert result.success is True
        assert "profiled successfully" in result.message
        assert result.operation_name == "ProfileFunction"
        assert "performance" in result.details

    @pytest.mark.asyncio
    async def test_execute_dependencies_function(self, debug_op, pool_mock):
        """Test DEPENDENCIES function execution."""
        cursor_mock = pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value
        cursor_mock.execute = AsyncMock()
        cursor_mock.fetchall = AsyncMock(side_effect=[
            [  # Dependencies
                ["pg_class", "test_table", "public", "n"],
                ["pg_proc", "helper_function", "public", "n"]
            ],
            [  # Dependents
                ["calling_function", "public", "n"]
            ]
        ])

        result = await debug_op.execute(
            action="DEPENDENCIES",
            function_name="test_function",
            schema_name="public"
        )

        assert result.success is True
        assert "Dependencies for function" in result.message
        assert result.operation_name == "GetFunctionDependencies"
        assert "dependencies" in result.details
        assert "dependents" in result.details

    @pytest.mark.asyncio
    async def test_execute_with_error(self, debug_op, pool_mock):
        """Test execution with database error."""
        cursor_mock = pool_mock.connection.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value
        cursor_mock.execute = AsyncMock(side_effect=Exception("Database connection error"))

        result = await debug_op.execute(
            action="ANALYZE",
            function_name="test_function"
        )

        assert result.success is False
        assert "Failed to analyze function" in result.error
        assert "Database connection error" in result.error

    def test_analyze_complexity_basic(self, debug_op):
        """Test complexity analysis with basic function."""
        source = """
        CREATE FUNCTION test_function() RETURNS integer AS $$
        BEGIN
            RETURN 1;
        END;
        $$ LANGUAGE plpgsql;
        """
        function_info = {"language": "plpgsql", "volatility": "v"}

        result = debug_op._analyze_complexity(source, function_info)

        assert "complexity_score" in result
        assert "estimated_complexity" in result
        assert result["language"] == "plpgsql"

    def test_analyze_complexity_complex(self, debug_op):
        """Test complexity analysis with complex function."""
        source = """
        CREATE FUNCTION complex_function() RETURNS integer AS $$
        BEGIN
            IF condition THEN
                FOR i IN 1..10 LOOP
                    CASE status
                        WHEN 'active' THEN
                            WHILE counter > 0 LOOP
                                -- Complex logic
                            END LOOP;
                    END CASE;
                END LOOP;
            END IF;
            RETURN 1;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN 0;
        END;
        $$ LANGUAGE plpgsql;
        """
        function_info = {"language": "plpgsql", "volatility": "v"}

        result = debug_op._analyze_complexity(source, function_info)

        assert result["complexity_score"] > 5
        assert result["estimated_complexity"] in ["medium", "high"]

    def test_check_common_issues(self, debug_op):
        """Test common issues checking."""
        source = """
        CREATE FUNCTION test_function() RETURNS integer AS $$
        BEGIN
            SELECT * FROM table;
            RETURN 1;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN 0;
        END;
        $$ LANGUAGE plpgsql;
        """
        function_info = {
            "volatility": "v",
            "is_strict": False,
            "security_definer": True
        }

        issues = debug_op._check_common_issues(source, function_info)

        assert len(issues) > 0
        # Should detect generic exception handling and security definer issues
        assert any("Generic exception handling" in issue for issue in issues)
        assert any("SECURITY DEFINER" in issue for issue in issues)

    def test_check_common_issues_no_issues(self, debug_op):
        """Test common issues checking with clean function."""
        source = """
        CREATE FUNCTION test_function(param integer) RETURNS integer AS $$
        BEGIN
            RETURN param * 2;
        END;
        $$ LANGUAGE plpgsql IMMUTABLE STRICT;
        """
        function_info = {
            "volatility": "i",
            "is_strict": True,
            "security_definer": False
        }

        issues = debug_op._check_common_issues(source, function_info)

        assert "No common issues detected" in issues