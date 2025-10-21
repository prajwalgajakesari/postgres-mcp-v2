"""Base classes for partition management operations."""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union, Tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, date

from ..permissions.rbac import RBACManager, Permission
from ..common_utils import SimpleAuditLogger

logger = logging.getLogger(__name__)


class PartitionStrategy(str, Enum):
    """Partition strategies supported by PostgreSQL."""

    RANGE = "range"
    LIST = "list"
    HASH = "hash"


class PartitionBound(str, Enum):
    """Partition bound types."""

    MINVALUE = "MINVALUE"
    MAXVALUE = "MAXVALUE"


@dataclass
class PartitionInfo:
    """Information about a partition."""

    partition_name: str
    parent_table: str
    strategy: PartitionStrategy
    partition_key: str
    bounds: Dict[str, Any]
    is_default: bool = False
    size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    created_at: Optional[datetime] = None
    tablespace: Optional[str] = None
    has_indexes: bool = False
    has_constraints: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert partition info to dictionary."""
        return {
            "partition_name": self.partition_name,
            "parent_table": self.parent_table,
            "strategy": self.strategy.value,
            "partition_key": self.partition_key,
            "bounds": self.bounds,
            "is_default": self.is_default,
            "size_bytes": self.size_bytes,
            "size_mb": round(self.size_bytes / (1024 * 1024), 2) if self.size_bytes else None,
            "row_count": self.row_count,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "tablespace": self.tablespace,
            "has_indexes": self.has_indexes,
            "has_constraints": self.has_constraints
        }


@dataclass
class PartitionOperationResult:
    """Result of a partition operation."""

    success: bool
    message: str
    partition_info: Optional[PartitionInfo] = None
    affected_partitions: Optional[List[str]] = None
    duration_seconds: Optional[float] = None
    warnings: Optional[List[str]] = None
    errors: Optional[List[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "partition_info": self.partition_info.to_dict() if self.partition_info else None,
            "affected_partitions": self.affected_partitions or [],
            "duration_seconds": self.duration_seconds,
            "warnings": self.warnings or [],
            "errors": self.errors or []
        }


class PartitioningStrategy(ABC):
    """Base class for partitioning strategies."""

    def __init__(self, strategy: PartitionStrategy):
        self.strategy = strategy

    @abstractmethod
    def generate_partition_name(self, parent_table: str, bounds: Dict[str, Any]) -> str:
        """Generate partition name based on bounds."""
        pass

    @abstractmethod
    def validate_bounds(self, bounds: Dict[str, Any], column_type: str) -> None:
        """Validate partition bounds for this strategy."""
        pass

    @abstractmethod
    def format_bounds_for_sql(self, bounds: Dict[str, Any]) -> str:
        """Format bounds for SQL CREATE TABLE statement."""
        pass

    @abstractmethod
    def suggest_partitions(self, data_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Suggest partition bounds based on data analysis."""
        pass


class RangePartitioningStrategy(PartitioningStrategy):
    """Range partitioning strategy implementation."""

    def __init__(self):
        super().__init__(PartitionStrategy.RANGE)

    def generate_partition_name(self, parent_table: str, bounds: Dict[str, Any]) -> str:
        """Generate partition name for range partition."""
        start = bounds.get('start', 'min')
        end = bounds.get('end', 'max')

        # Clean values for name generation
        start_str = str(start).replace('-', 'neg').replace(' ', '_').replace(':', '')[:10]
        end_str = str(end).replace('-', 'neg').replace(' ', '_').replace(':', '')[:10]

        return f"{parent_table}_p_{start_str}_to_{end_str}"

    def validate_bounds(self, bounds: Dict[str, Any], column_type: str) -> None:
        """Validate range partition bounds."""
        if 'start' not in bounds and 'end' not in bounds:
            raise ValueError("Range partition must specify 'start' or 'end' bounds")

        start = bounds.get('start')
        end = bounds.get('end')

        if start is not None and end is not None:
            if start >= end:
                raise ValueError("Range partition start must be less than end")

        # Validate types based on column type
        for bound_key, bound_value in bounds.items():
            if bound_value is None or bound_value in [PartitionBound.MINVALUE, PartitionBound.MAXVALUE]:
                continue

            if column_type.lower() in ['integer', 'bigint', 'smallint']:
                if not isinstance(bound_value, int):
                    try:
                        int(bound_value)
                    except (ValueError, TypeError):
                        raise ValueError(f"Invalid integer value for {bound_key}: {bound_value}")

            elif column_type.lower() in ['date', 'timestamp']:
                if not isinstance(bound_value, (date, datetime, str)):
                    raise ValueError(f"Invalid date/timestamp value for {bound_key}: {bound_value}")

    def format_bounds_for_sql(self, bounds: Dict[str, Any]) -> str:
        """Format range bounds for SQL."""
        start = bounds.get('start')
        end = bounds.get('end')

        if start == PartitionBound.MINVALUE:
            start_str = "MINVALUE"
        elif start is None:
            start_str = "MINVALUE"
        else:
            start_str = f"'{start}'" if isinstance(start, str) else str(start)

        if end == PartitionBound.MAXVALUE:
            end_str = "MAXVALUE"
        elif end is None:
            end_str = "MAXVALUE"
        else:
            end_str = f"'{end}'" if isinstance(end, str) else str(end)

        return f"FROM ({start_str}) TO ({end_str})"

    def suggest_partitions(self, data_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Suggest range partitions based on data distribution."""
        suggestions = []

        column_stats = data_analysis.get('column_stats', {})
        row_count = data_analysis.get('row_count', 0)

        if row_count < 1000:
            return []  # Not worth partitioning small tables

        min_val = column_stats.get('min')
        max_val = column_stats.get('max')
        column_type = column_stats.get('type', 'unknown')

        if min_val is None or max_val is None:
            return []

        # Suggest partitions based on data type
        if column_type.lower() in ['date', 'timestamp']:
            suggestions = self._suggest_date_partitions(min_val, max_val)
        elif column_type.lower() in ['integer', 'bigint']:
            suggestions = self._suggest_numeric_partitions(min_val, max_val, row_count)

        return suggestions

    def _suggest_date_partitions(self, min_date, max_date) -> List[Dict[str, Any]]:
        """Suggest date-based range partitions."""
        from dateutil.relativedelta import relativedelta

        suggestions = []

        try:
            if isinstance(min_date, str):
                min_date = datetime.fromisoformat(min_date.replace('Z', '+00:00'))
            if isinstance(max_date, str):
                max_date = datetime.fromisoformat(max_date.replace('Z', '+00:00'))

            # Monthly partitions
            current = min_date.replace(day=1)
            while current <= max_date:
                next_month = current + relativedelta(months=1)
                suggestions.append({
                    'start': current.strftime('%Y-%m-%d'),
                    'end': next_month.strftime('%Y-%m-%d')
                })
                current = next_month

        except Exception as e:
            logger.warning(f"Failed to generate date partition suggestions: {e}")

        return suggestions

    def _suggest_numeric_partitions(self, min_val: int, max_val: int, row_count: int) -> List[Dict[str, Any]]:
        """Suggest numeric range partitions."""
        suggestions = []

        try:
            # Aim for partitions with roughly 100K-1M rows each
            target_partitions = max(2, min(20, row_count // 100000))
            range_size = (max_val - min_val) // target_partitions

            if range_size <= 0:
                return []

            current = min_val
            while current < max_val:
                next_val = min(current + range_size, max_val)
                suggestions.append({
                    'start': current,
                    'end': next_val
                })
                current = next_val

        except Exception as e:
            logger.warning(f"Failed to generate numeric partition suggestions: {e}")

        return suggestions


class ListPartitioningStrategy(PartitioningStrategy):
    """List partitioning strategy implementation."""

    def __init__(self):
        super().__init__(PartitionStrategy.LIST)

    def generate_partition_name(self, parent_table: str, bounds: Dict[str, Any]) -> str:
        """Generate partition name for list partition."""
        values = bounds.get('values', [])
        if not values:
            return f"{parent_table}_p_empty"

        # Use first few values for naming
        value_strs = [str(v)[:10] for v in values[:3]]
        value_part = "_".join(value_strs).replace(' ', '_').replace('-', 'neg')
        return f"{parent_table}_p_{value_part}"

    def validate_bounds(self, bounds: Dict[str, Any], column_type: str) -> None:
        """Validate list partition bounds."""
        if 'values' not in bounds or not bounds['values']:
            raise ValueError("List partition must specify 'values'")

        values = bounds['values']
        if not isinstance(values, list):
            raise ValueError("List partition 'values' must be a list")

        if len(values) == 0:
            raise ValueError("List partition must have at least one value")

    def format_bounds_for_sql(self, bounds: Dict[str, Any]) -> str:
        """Format list bounds for SQL."""
        values = bounds.get('values', [])
        formatted_values = []

        for value in values:
            if isinstance(value, str):
                formatted_values.append(f"'{value}'")
            else:
                formatted_values.append(str(value))

        return f"IN ({', '.join(formatted_values)})"

    def suggest_partitions(self, data_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Suggest list partitions based on value distribution."""
        suggestions = []

        value_counts = data_analysis.get('value_counts', {})
        if not value_counts:
            return []

        # Group high-frequency values into partitions
        sorted_values = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)

        # Create partitions for top values
        for i in range(0, min(len(sorted_values), 20), 5):  # Group in sets of 5
            batch = sorted_values[i:i+5]
            values = [item[0] for item in batch]
            suggestions.append({'values': values})

        return suggestions


class HashPartitioningStrategy(PartitioningStrategy):
    """Hash partitioning strategy implementation."""

    def __init__(self):
        super().__init__(PartitionStrategy.HASH)

    def generate_partition_name(self, parent_table: str, bounds: Dict[str, Any]) -> str:
        """Generate partition name for hash partition."""
        modulus = bounds.get('modulus', 1)
        remainder = bounds.get('remainder', 0)
        return f"{parent_table}_p_h{remainder}of{modulus}"

    def validate_bounds(self, bounds: Dict[str, Any], column_type: str) -> None:
        """Validate hash partition bounds."""
        if 'modulus' not in bounds or 'remainder' not in bounds:
            raise ValueError("Hash partition must specify 'modulus' and 'remainder'")

        modulus = bounds['modulus']
        remainder = bounds['remainder']

        if not isinstance(modulus, int) or modulus <= 0:
            raise ValueError("Hash partition 'modulus' must be a positive integer")

        if not isinstance(remainder, int) or remainder < 0 or remainder >= modulus:
            raise ValueError(f"Hash partition 'remainder' must be between 0 and {modulus-1}")

    def format_bounds_for_sql(self, bounds: Dict[str, Any]) -> str:
        """Format hash bounds for SQL."""
        modulus = bounds['modulus']
        remainder = bounds['remainder']
        return f"WITH (MODULUS {modulus}, REMAINDER {remainder})"

    def suggest_partitions(self, data_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Suggest hash partitions based on data size."""
        suggestions = []

        row_count = data_analysis.get('row_count', 0)
        if row_count < 10000:
            return []  # Not worth hash partitioning small tables

        # Suggest power-of-2 partition counts
        target_partitions = 4
        if row_count > 1000000:
            target_partitions = 8
        if row_count > 10000000:
            target_partitions = 16

        for i in range(target_partitions):
            suggestions.append({
                'modulus': target_partitions,
                'remainder': i
            })

        return suggestions


class PartitionOperation(ABC):
    """Base class for partition operations."""

    def __init__(self, connection_pool, rbac_manager: RBACManager, audit_logger: SimpleAuditLogger):
        self.connection_pool = connection_pool
        self.rbac_manager = rbac_manager
        self.audit_logger = audit_logger
        self.strategies = {
            PartitionStrategy.RANGE: RangePartitioningStrategy(),
            PartitionStrategy.LIST: ListPartitioningStrategy(),
            PartitionStrategy.HASH: HashPartitioningStrategy()
        }

    def _validate_permissions(self, role_name: str, required_permissions: List[Permission]) -> None:
        """Validate user has required permissions."""
        for permission in required_permissions:
            if not self.rbac_manager.check_permission(role_name, permission):
                raise PermissionError(f"Missing permission: {permission.value}")

    def _quote_identifier(self, identifier: str) -> str:
        """Quote SQL identifier to prevent injection."""
        return f'"{identifier.replace('"', '""')}"'

    def _validate_identifier(self, identifier: str, max_length: int = 63) -> None:
        """Validate SQL identifier."""
        if not identifier or not identifier.replace('_', '').replace('-', '').isalnum():
            raise ValueError(f"Invalid identifier: {identifier}")
        if len(identifier) > max_length:
            raise ValueError(f"Identifier too long (max {max_length}): {identifier}")

    def get_strategy(self, strategy: PartitionStrategy) -> PartitioningStrategy:
        """Get partitioning strategy implementation."""
        return self.strategies[strategy]

    async def _table_exists(self, connection, table_name: str, schema_name: str = "public") -> bool:
        """Check if table exists."""
        query = """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        """
        result = await connection.fetchval(query, schema_name, table_name)
        return bool(result)

    async def _is_partitioned_table(self, connection, table_name: str, schema_name: str = "public") -> bool:
        """Check if table is partitioned."""
        query = """
            SELECT partrelid IS NOT NULL
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_partitioned_table p ON c.oid = p.partrelid
            WHERE n.nspname = $1 AND c.relname = $2
        """
        result = await connection.fetchval(query, schema_name, table_name)
        return bool(result)

    async def _get_partition_info(self, connection, table_name: str, schema_name: str = "public") -> Optional[PartitionInfo]:
        """Get information about a partition."""
        try:
            # Get basic partition information
            query = """
                SELECT
                    c.relname as partition_name,
                    parent.relname as parent_table,
                    pt.partstrat as strategy,
                    pg_get_partkeydef(pt.partrelid) as partition_key,
                    pg_get_expr(c.relpartbound, c.oid) as partition_bounds,
                    c.oid,
                    ts.spcname as tablespace
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                JOIN pg_inherits i ON c.oid = i.inhrelid
                JOIN pg_class parent ON i.inhparent = parent.oid
                JOIN pg_partitioned_table pt ON parent.oid = pt.partrelid
                LEFT JOIN pg_tablespace ts ON c.reltablespace = ts.oid
                WHERE n.nspname = $1 AND c.relname = $2
            """

            row = await connection.fetchrow(query, schema_name, table_name)
            if not row:
                return None

            # Map strategy code to enum
            strategy_map = {'r': PartitionStrategy.RANGE, 'l': PartitionStrategy.LIST, 'h': PartitionStrategy.HASH}
            strategy = strategy_map.get(row['strategy'], PartitionStrategy.RANGE)

            # Get size and row count
            size_query = "SELECT pg_relation_size($1)"
            size_bytes = await connection.fetchval(size_query, row['oid'])

            count_query = f"SELECT COUNT(*) FROM {self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"
            try:
                row_count = await connection.fetchval(count_query)
            except Exception:
                row_count = None

            # Parse bounds
            bounds = self._parse_partition_bounds(row['partition_bounds'], strategy)

            return PartitionInfo(
                partition_name=row['partition_name'],
                parent_table=row['parent_table'],
                strategy=strategy,
                partition_key=row['partition_key'],
                bounds=bounds,
                size_bytes=size_bytes,
                row_count=row_count,
                tablespace=row['tablespace']
            )

        except Exception as e:
            logger.warning(f"Failed to get partition info: {e}")
            return None

    def _parse_partition_bounds(self, bounds_expr: str, strategy: PartitionStrategy) -> Dict[str, Any]:
        """Parse PostgreSQL partition bounds expression."""
        if not bounds_expr:
            return {}

        try:
            if strategy == PartitionStrategy.RANGE:
                # Parse "FOR VALUES FROM (...) TO (...)"
                if "FOR VALUES FROM" in bounds_expr:
                    parts = bounds_expr.split("FOR VALUES FROM")[1].strip()
                    if " TO " in parts:
                        start_part, end_part = parts.split(" TO ", 1)
                        start = start_part.strip("() '\"")
                        end = end_part.strip("() '\"")
                        return {'start': start, 'end': end}

            elif strategy == PartitionStrategy.LIST:
                # Parse "FOR VALUES IN (...)"
                if "FOR VALUES IN" in bounds_expr:
                    values_part = bounds_expr.split("FOR VALUES IN")[1].strip("() ")
                    values = [v.strip(" '\"") for v in values_part.split(",")]
                    return {'values': values}

            elif strategy == PartitionStrategy.HASH:
                # Parse "FOR VALUES WITH (modulus X, remainder Y)"
                if "modulus" in bounds_expr and "remainder" in bounds_expr:
                    import re
                    modulus_match = re.search(r'modulus\s+(\d+)', bounds_expr)
                    remainder_match = re.search(r'remainder\s+(\d+)', bounds_expr)
                    if modulus_match and remainder_match:
                        return {
                            'modulus': int(modulus_match.group(1)),
                            'remainder': int(remainder_match.group(1))
                        }

        except Exception as e:
            logger.warning(f"Failed to parse partition bounds: {e}")

        return {'raw': bounds_expr}

    @abstractmethod
    async def execute(self, **kwargs) -> PartitionOperationResult:
        """Execute the partition operation."""
        pass