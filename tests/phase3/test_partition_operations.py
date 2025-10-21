"""Tests for Phase 3 Week 12: Partition Management Operations."""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

from postgres_mcp.partitions import (
    CreatePartitionOperation, ManagePartitionsOperation,
    PartitionStrategy, PartitionInfo, PartitionOperationResult,
    RangePartitioningStrategy, ListPartitioningStrategy, HashPartitioningStrategy,
    TimeBasedPartitioningStrategy, GeographicPartitioningStrategy,
    PartitionMaintenanceStrategy
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
def sample_partition_info():
    """Create sample partition information."""
    return PartitionInfo(
        partition_name="test_table_p_202401",
        parent_table="test_table",
        strategy=PartitionStrategy.RANGE,
        partition_key="created_at",
        bounds={"start": "2024-01-01", "end": "2024-02-01"},
        is_default=False,
        size_bytes=1048576,
        row_count=1000
    )


class TestCreatePartitionOperation:
    """Test partition creation operations."""

    @pytest.mark.asyncio
    async def test_create_partitioned_table(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test creating a partitioned table."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=False)  # Table doesn't exist
        mock_connection.execute = AsyncMock()

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        create_op = CreatePartitionOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        columns = [
            {"name": "id", "type": "BIGSERIAL"},
            {"name": "created_at", "type": "TIMESTAMP", "not_null": True},
            {"name": "data", "type": "JSONB"}
        ]

        constraints = [
            {"type": "PRIMARY KEY", "columns": ["id", "created_at"]}
        ]

        result = await create_op.create_partitioned_table(
            table_name="partitioned_table",
            columns=columns,
            partition_key="created_at",
            strategy=PartitionStrategy.RANGE,
            role_name="test_role",
            constraints=constraints
        )

        assert result.success is True
        assert "successfully" in result.message.lower()

        # Verify CREATE TABLE with PARTITION BY was executed
        mock_connection.execute.assert_called_once()
        create_call = mock_connection.execute.call_args[0][0]
        assert "CREATE TABLE" in create_call
        assert "PARTITION BY RANGE" in create_call
        assert '"created_at"' in create_call

    @pytest.mark.asyncio
    async def test_create_partition(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test creating a partition."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(side_effect=[
            True,   # Parent table exists
            True,   # Parent is partitioned
            False,  # Partition doesn't exist
            'timestamp'  # Column type
        ])
        mock_connection.fetchrow = AsyncMock(side_effect=[
            # Parent partition info
            {'partstrat': 'r', 'partition_key': 'PARTITION BY RANGE (created_at)'},
            # Created partition info
            {'partition_name': 'test_table_p_202401', 'parent_table': 'test_table',
             'strategy': 'r', 'partition_key': 'created_at',
             'partition_bounds': 'FOR VALUES FROM (\'2024-01-01\') TO (\'2024-02-01\')',
             'oid': 12345, 'tablespace': None}
        ])
        mock_connection.execute = AsyncMock()

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        create_op = CreatePartitionOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await create_op.create_partition(
            parent_table="test_table",
            partition_name="test_table_p_202401",
            bounds={"start": "2024-01-01", "end": "2024-02-01"},
            role_name="test_role"
        )

        assert result.success is True
        assert "successfully" in result.message.lower()
        assert result.partition_info is not None

        # Verify CREATE TABLE PARTITION OF was executed
        mock_connection.execute.assert_called_once()
        create_call = mock_connection.execute.call_args[0][0]
        assert "CREATE TABLE" in create_call
        assert "PARTITION OF" in create_call
        assert "FOR VALUES FROM" in create_call

    @pytest.mark.asyncio
    async def test_create_default_partition(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test creating a default partition."""
        mock_connection = AsyncMock()
        mock_connection.execute = AsyncMock()
        mock_connection.fetchrow = AsyncMock(return_value={
            'partition_name': 'test_table_default',
            'parent_table': 'test_table',
            'strategy': 'r',
            'partition_key': 'created_at',
            'partition_bounds': 'DEFAULT',
            'oid': 12346,
            'tablespace': None
        })
        mock_connection.fetchval = AsyncMock(return_value=1000000)  # Size

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        create_op = CreatePartitionOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await create_op.create_default_partition(
            parent_table="test_table",
            role_name="test_role"
        )

        assert result.success is True
        assert "default partition" in result.message.lower()
        assert result.partition_info.is_default is True

    @pytest.mark.asyncio
    async def test_create_multiple_partitions(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test creating multiple partitions at once."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(side_effect=[
            True, True, False, 'timestamp',  # First partition checks
            True, True, False, 'timestamp',  # Second partition checks
            True, True, False, 'timestamp'   # Third partition checks
        ])
        mock_connection.fetchrow = AsyncMock(return_value={
            'partstrat': 'r',
            'partition_key': 'PARTITION BY RANGE (created_at)'
        })
        mock_connection.execute = AsyncMock()

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        create_op = CreatePartitionOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        partitions = [
            {"name": "test_table_p_202401", "bounds": {"start": "2024-01-01", "end": "2024-02-01"}},
            {"name": "test_table_p_202402", "bounds": {"start": "2024-02-01", "end": "2024-03-01"}},
            {"name": "test_table_p_202403", "bounds": {"start": "2024-03-01", "end": "2024-04-01"}}
        ]

        result = await create_op.create_multiple_partitions(
            parent_table="test_table",
            partitions=partitions,
            role_name="test_role"
        )

        assert result.success is True
        assert "Created 3 partitions" in result.message
        assert len(result.affected_partitions) == 3


class TestManagePartitionsOperation:
    """Test partition management operations."""

    @pytest.mark.asyncio
    async def test_list_partitions(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test listing partitions."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(side_effect=[
            True,   # Table exists
            True,   # Table is partitioned
            1048576,  # Partition 1 size
            500,      # Partition 1 row count
            2,        # Partition 1 indexes
            1,        # Partition 1 constraints
            2097152,  # Partition 2 size
            1500,     # Partition 2 row count
            3,        # Partition 2 indexes
            2         # Partition 2 constraints
        ])
        mock_connection.fetch = AsyncMock(return_value=[
            {'partition_name': 'test_table_p_202401', 'parent_table': 'test_table',
             'strategy': 'r', 'partition_key': 'PARTITION BY RANGE (created_at)',
             'partition_bounds': 'FOR VALUES FROM (\'2024-01-01\') TO (\'2024-02-01\')',
             'partition_oid': 12345, 'tablespace': None, 'is_default': False},
            {'partition_name': 'test_table_p_202402', 'parent_table': 'test_table',
             'strategy': 'r', 'partition_key': 'PARTITION BY RANGE (created_at)',
             'partition_bounds': 'FOR VALUES FROM (\'2024-02-01\') TO (\'2024-03-01\')',
             'partition_oid': 12346, 'tablespace': None, 'is_default': False}
        ])

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        manage_op = ManagePartitionsOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await manage_op.list_partitions(
            parent_table="test_table",
            role_name="test_role",
            include_statistics=True
        )

        assert result.success is True
        assert "Found 2 partitions" in result.message
        assert len(result.affected_partitions) == 2

    @pytest.mark.asyncio
    async def test_analyze_partition_distribution(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test analyzing partition data distribution."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=True)  # Table is partitioned
        mock_connection.fetchrow = AsyncMock(return_value={
            'strategy': 'r',
            'partition_key': 'PARTITION BY RANGE (created_at)'
        })

        # Mock list_partitions to provide partition data
        with patch.object(ManagePartitionsOperation, 'list_partitions') as mock_list:
            mock_list.return_value = PartitionOperationResult(
                success=True,
                message="Partitions listed",
                affected_partitions=["p1", "p2"]
            )

            mock_connection_pool.acquire = AsyncMock()
            mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
            mock_connection_pool.acquire().__aexit__ = AsyncMock()

            manage_op = ManagePartitionsOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

            result = await manage_op.analyze_partition_distribution(
                parent_table="test_table",
                role_name="test_role",
                suggest_optimizations=True
            )

            assert result.success is True
            assert "analysis completed" in result.message.lower()

    @pytest.mark.asyncio
    async def test_suggest_partitioning_strategy(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test suggesting partitioning strategy for a table."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(return_value=True)  # Table exists
        mock_connection.fetchrow = AsyncMock(side_effect=[
            # Column info
            {'data_type': 'timestamp', 'is_nullable': 'NO'},
            # Data statistics
            {'row_count': 1000000, 'distinct_values': 365,
             'min_value': '2023-01-01', 'max_value': '2023-12-31'}
        ])
        mock_connection.fetch = AsyncMock(return_value=[])  # Value distribution

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        manage_op = ManagePartitionsOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await manage_op.suggest_partitioning_strategy(
            table_name="test_table",
            column_name="created_at",
            role_name="test_role"
        )

        assert result.success is True
        assert "analysis completed" in result.message.lower()

    @pytest.mark.asyncio
    async def test_drop_partition(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger,
                                 sample_partition_info):
        """Test dropping a partition."""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(side_effect=[
            True,   # Partition exists
            1048576  # Partition size
        ])
        mock_connection.fetchrow = AsyncMock(return_value={
            'partition_name': 'test_table_p_202401',
            'parent_table': 'test_table',
            'strategy': 'r',
            'partition_key': 'created_at',
            'partition_bounds': 'FOR VALUES FROM (\'2024-01-01\') TO (\'2024-02-01\')',
            'oid': 12345,
            'tablespace': None
        })
        mock_connection.execute = AsyncMock()

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        manage_op = ManagePartitionsOperation(mock_connection_pool, mock_rbac_manager, mock_audit_logger)

        result = await manage_op.drop_partition(
            partition_name="test_table_p_202401",
            role_name="test_role"
        )

        assert result.success is True
        assert "dropped successfully" in result.message.lower()

        # Verify DROP TABLE was executed
        mock_connection.execute.assert_called_once()
        drop_call = mock_connection.execute.call_args[0][0]
        assert "DROP TABLE" in drop_call


class TestPartitioningStrategies:
    """Test different partitioning strategies."""

    def test_range_partitioning_strategy(self):
        """Test range partitioning strategy."""
        strategy = RangePartitioningStrategy()

        # Test partition name generation
        name = strategy.generate_partition_name(
            "test_table",
            {"start": "2024-01-01", "end": "2024-02-01"}
        )
        assert "test_table_p_" in name
        assert "2024" in name

        # Test bounds validation
        strategy.validate_bounds(
            {"start": "2024-01-01", "end": "2024-02-01"},
            "timestamp"
        )

        # Test invalid bounds
        with pytest.raises(ValueError):
            strategy.validate_bounds(
                {"start": "2024-02-01", "end": "2024-01-01"},  # Start > end
                "timestamp"
            )

        # Test SQL formatting
        sql = strategy.format_bounds_for_sql(
            {"start": "2024-01-01", "end": "2024-02-01"}
        )
        assert "FROM ('2024-01-01') TO ('2024-02-01')" == sql

    def test_list_partitioning_strategy(self):
        """Test list partitioning strategy."""
        strategy = ListPartitioningStrategy()

        # Test partition name generation
        name = strategy.generate_partition_name(
            "test_table",
            {"values": ["US", "CA", "MX"]}
        )
        assert "test_table_p_" in name

        # Test bounds validation
        strategy.validate_bounds(
            {"values": ["US", "CA", "MX"]},
            "text"
        )

        # Test invalid bounds
        with pytest.raises(ValueError):
            strategy.validate_bounds(
                {"values": []},  # Empty list
                "text"
            )

        # Test SQL formatting
        sql = strategy.format_bounds_for_sql(
            {"values": ["US", "CA", "MX"]}
        )
        assert "IN ('US', 'CA', 'MX')" == sql

    def test_hash_partitioning_strategy(self):
        """Test hash partitioning strategy."""
        strategy = HashPartitioningStrategy()

        # Test partition name generation
        name = strategy.generate_partition_name(
            "test_table",
            {"modulus": 4, "remainder": 0}
        )
        assert "test_table_p_h0of4" == name

        # Test bounds validation
        strategy.validate_bounds(
            {"modulus": 4, "remainder": 2},
            "integer"
        )

        # Test invalid bounds
        with pytest.raises(ValueError):
            strategy.validate_bounds(
                {"modulus": 4, "remainder": 5},  # Remainder >= modulus
                "integer"
            )

        # Test SQL formatting
        sql = strategy.format_bounds_for_sql(
            {"modulus": 4, "remainder": 2}
        )
        assert "WITH (MODULUS 4, REMAINDER 2)" == sql

    def test_time_based_partitioning_strategy(self):
        """Test time-based partitioning strategy."""
        strategy = TimeBasedPartitioningStrategy(interval="month")

        # Test partition name generation
        name = strategy.generate_partition_name(
            "test_table",
            {"start": "2024-01-01", "end": "2024-02-01"}
        )
        assert "test_table_p_202401" == name

        # Test different intervals
        strategy_day = TimeBasedPartitioningStrategy(interval="day")
        name = strategy_day.generate_partition_name(
            "test_table",
            {"start": "2024-01-15", "end": "2024-01-16"}
        )
        assert "20240115" in name

        strategy_year = TimeBasedPartitioningStrategy(interval="year")
        name = strategy_year.generate_partition_name(
            "test_table",
            {"start": "2024-01-01", "end": "2025-01-01"}
        )
        assert "2024" in name

        # Test partition suggestions
        data_analysis = {
            'column_stats': {
                'min': '2023-01-01',
                'max': '2023-12-31',
                'type': 'timestamp'
            },
            'row_count': 100000
        }

        suggestions = strategy.suggest_partitions(data_analysis)
        assert len(suggestions) == 12  # 12 months

    def test_geographic_partitioning_strategy(self):
        """Test geographic partitioning strategy."""
        regions = {
            "north": ["NY", "MA", "CT", "VT", "NH", "ME"],
            "south": ["FL", "GA", "AL", "MS", "LA", "TX"],
            "west": ["CA", "OR", "WA", "NV", "AZ"],
            "midwest": ["IL", "IN", "OH", "MI", "WI"]
        }

        strategy = GeographicPartitioningStrategy(regions)

        # Test partition name generation
        name = strategy.generate_partition_name(
            "test_table",
            {"values": ["NY", "MA", "CT"]}
        )
        assert "north" in name

        # Test partition suggestions
        data_analysis = {
            'value_counts': {
                'NY': 1000,
                'CA': 800,
                'FL': 600,
                'IL': 400,
                'XX': 100  # Unknown state
            }
        }

        suggestions = strategy.suggest_partitions(data_analysis)
        assert len(suggestions) >= 4  # At least one per region with data


class TestPartitionMaintenanceStrategy:
    """Test automated partition maintenance."""

    @pytest.mark.asyncio
    async def test_auto_create_time_partitions(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test automatic creation of future time partitions."""
        with patch('postgres_mcp.partitions.create.CreatePartitionOperation.create_partition') as mock_create:
            mock_create.return_value = PartitionOperationResult(
                success=True,
                message="Partition created"
            )

            maintenance = PartitionMaintenanceStrategy(
                mock_connection_pool, mock_rbac_manager, mock_audit_logger
            )

            result = await maintenance.auto_create_time_partitions(
                parent_table="test_table",
                advance_periods=3,
                interval="month",
                role_name="test_role"
            )

            assert result["success"] is True
            assert len(result["created_partitions"]) == 3
            assert mock_create.call_count == 3

    @pytest.mark.asyncio
    async def test_cleanup_old_partitions(self, mock_connection_pool, mock_rbac_manager, mock_audit_logger):
        """Test cleanup of old partitions."""
        mock_connection = AsyncMock()

        # Mock partition list with old partitions
        old_date = datetime.utcnow() - timedelta(days=400)
        recent_date = datetime.utcnow() - timedelta(days=30)

        mock_connection.fetch = AsyncMock(return_value=[
            {'partition_name': 'test_table_p_202201',
             'partition_bounds': f"FOR VALUES FROM ('{old_date.strftime('%Y-%m-%d')}') TO ('{(old_date + timedelta(days=30)).strftime('%Y-%m-%d')}')"},
            {'partition_name': 'test_table_p_202401',
             'partition_bounds': f"FOR VALUES FROM ('{recent_date.strftime('%Y-%m-%d')}') TO ('{(recent_date + timedelta(days=30)).strftime('%Y-%m-%d')}')"}
        ])

        mock_connection_pool.acquire = AsyncMock()
        mock_connection_pool.acquire().__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_pool.acquire().__aexit__ = AsyncMock()

        with patch('postgres_mcp.partitions.manage.ManagePartitionsOperation.list_partitions') as mock_list:
            mock_list.return_value = PartitionOperationResult(
                success=True,
                message="Partitions listed"
            )

            with patch('postgres_mcp.partitions.manage.ManagePartitionsOperation.drop_partition') as mock_drop:
                mock_drop.return_value = PartitionOperationResult(
                    success=True,
                    message="Partition dropped"
                )

                maintenance = PartitionMaintenanceStrategy(
                    mock_connection_pool, mock_rbac_manager, mock_audit_logger
                )

                # Test dry run first
                result = await maintenance.cleanup_old_partitions(
                    parent_table="test_table",
                    retention_days=365,
                    role_name="test_role",
                    dry_run=True
                )

                assert result["success"] is True
                assert result["dry_run"] is True
                assert "Would drop" == result["action"]
                mock_drop.assert_not_called()

                # Test actual cleanup
                result = await maintenance.cleanup_old_partitions(
                    parent_table="test_table",
                    retention_days=365,
                    role_name="test_role",
                    dry_run=False
                )

                assert result["success"] is True
                assert result["dry_run"] is False
                assert "Dropped" == result["action"]


class TestPartitionInfo:
    """Test partition info operations."""

    def test_partition_info_serialization(self, sample_partition_info):
        """Test partition info serialization."""
        # Convert to dict
        info_dict = sample_partition_info.to_dict()

        assert info_dict["partition_name"] == sample_partition_info.partition_name
        assert info_dict["parent_table"] == sample_partition_info.parent_table
        assert info_dict["strategy"] == sample_partition_info.strategy.value
        assert info_dict["size_mb"] == round(sample_partition_info.size_bytes / (1024 * 1024), 2)

    def test_partition_bounds_parsing(self):
        """Test parsing of PostgreSQL partition bounds."""
        from postgres_mcp.partitions.base import PartitionOperation

        op = PartitionOperation(None, None, None)

        # Test range bounds
        range_bounds = op._parse_partition_bounds(
            "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
            PartitionStrategy.RANGE
        )
        assert range_bounds["start"] == "2024-01-01"
        assert range_bounds["end"] == "2024-02-01"

        # Test list bounds
        list_bounds = op._parse_partition_bounds(
            "FOR VALUES IN ('US', 'CA', 'MX')",
            PartitionStrategy.LIST
        )
        assert list_bounds["values"] == ["US", "CA", "MX"]

        # Test hash bounds
        hash_bounds = op._parse_partition_bounds(
            "FOR VALUES WITH (modulus 4, remainder 2)",
            PartitionStrategy.HASH
        )
        assert hash_bounds["modulus"] == 4
        assert hash_bounds["remainder"] == 2