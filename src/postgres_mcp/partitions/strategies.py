"""Advanced partitioning strategies and utilities."""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

from .base import PartitioningStrategy, PartitionStrategy
from ..permissions.rbac import RBACManager, Permission

logger = logging.getLogger(__name__)


class TimeBasedPartitioningStrategy(PartitioningStrategy):
    """Specialized strategy for time-based range partitioning."""

    def __init__(self, interval: str = "month"):
        super().__init__(PartitionStrategy.RANGE)
        self.interval = interval  # day, week, month, quarter, year

    def generate_partition_name(self, parent_table: str, bounds: Dict[str, Any]) -> str:
        """Generate time-based partition names."""
        start = bounds.get('start')
        if isinstance(start, str):
            try:
                start_date = datetime.fromisoformat(start.replace('Z', '+00:00'))
            except:
                start_date = datetime.strptime(start, '%Y-%m-%d')
        elif isinstance(start, datetime):
            start_date = start
        else:
            return f"{parent_table}_p_unknown"

        if self.interval == "day":
            return f"{parent_table}_p_{start_date.strftime('%Y%m%d')}"
        elif self.interval == "week":
            year, week, _ = start_date.isocalendar()
            return f"{parent_table}_p_{year}w{week:02d}"
        elif self.interval == "month":
            return f"{parent_table}_p_{start_date.strftime('%Y%m')}"
        elif self.interval == "quarter":
            quarter = (start_date.month - 1) // 3 + 1
            return f"{parent_table}_p_{start_date.year}q{quarter}"
        elif self.interval == "year":
            return f"{parent_table}_p_{start_date.year}"
        else:
            return f"{parent_table}_p_{start_date.strftime('%Y%m%d')}"

    def validate_bounds(self, bounds: Dict[str, Any], column_type: str) -> None:
        """Validate time-based bounds."""
        super().validate_bounds(bounds, column_type)

        # Additional validation for time-based partitioning
        start = bounds.get('start')
        end = bounds.get('end')

        if start and end:
            try:
                if isinstance(start, str):
                    start_date = datetime.fromisoformat(start.replace('Z', '+00:00'))
                else:
                    start_date = start

                if isinstance(end, str):
                    end_date = datetime.fromisoformat(end.replace('Z', '+00:00'))
                else:
                    end_date = end

                # Validate interval consistency
                delta = end_date - start_date

                if self.interval == "day" and delta.days != 1:
                    logger.warning(f"Time partition interval mismatch: expected 1 day, got {delta.days} days")
                elif self.interval == "month" and not (28 <= delta.days <= 31):
                    logger.warning(f"Time partition interval mismatch: expected ~30 days, got {delta.days} days")

            except Exception as e:
                raise ValueError(f"Invalid time values for time-based partitioning: {e}")

    def format_bounds_for_sql(self, bounds: Dict[str, Any]) -> str:
        """Format time bounds with proper casting."""
        start = bounds.get('start')
        end = bounds.get('end')

        if start == "MINVALUE" or start is None:
            start_str = "MINVALUE"
        else:
            start_str = f"'{start}'::timestamp" if isinstance(start, str) else f"'{start}'"

        if end == "MAXVALUE" or end is None:
            end_str = "MAXVALUE"
        else:
            end_str = f"'{end}'::timestamp" if isinstance(end, str) else f"'{end}'"

        return f"FROM ({start_str}) TO ({end_str})"

    def suggest_partitions(self, data_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Suggest time-based partitions with proper intervals."""
        suggestions = []

        column_stats = data_analysis.get('column_stats', {})
        min_val = column_stats.get('min')
        max_val = column_stats.get('max')

        if not min_val or not max_val:
            return []

        try:
            if isinstance(min_val, str):
                min_date = datetime.fromisoformat(min_val.replace('Z', '+00:00'))
            else:
                min_date = min_val

            if isinstance(max_val, str):
                max_date = datetime.fromisoformat(max_val.replace('Z', '+00:00'))
            else:
                max_date = max_val

            current = self._normalize_date_to_interval(min_date)

            while current < max_date:
                next_date = self._add_interval(current)
                suggestions.append({
                    'start': current.strftime('%Y-%m-%d %H:%M:%S'),
                    'end': next_date.strftime('%Y-%m-%d %H:%M:%S')
                })
                current = next_date

        except Exception as e:
            logger.error(f"Failed to generate time-based partition suggestions: {e}")

        return suggestions

    def _normalize_date_to_interval(self, dt: datetime) -> datetime:
        """Normalize date to the start of the interval."""
        if self.interval == "day":
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        elif self.interval == "week":
            # Start of week (Monday)
            days_since_monday = dt.weekday()
            monday = dt - timedelta(days=days_since_monday)
            return monday.replace(hour=0, minute=0, second=0, microsecond=0)
        elif self.interval == "month":
            return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        elif self.interval == "quarter":
            quarter_start_month = ((dt.month - 1) // 3) * 3 + 1
            return dt.replace(month=quarter_start_month, day=1, hour=0, minute=0, second=0, microsecond=0)
        elif self.interval == "year":
            return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            return dt

    def _add_interval(self, dt: datetime) -> datetime:
        """Add one interval to the datetime."""
        if self.interval == "day":
            return dt + timedelta(days=1)
        elif self.interval == "week":
            return dt + timedelta(weeks=1)
        elif self.interval == "month":
            return dt + relativedelta(months=1)
        elif self.interval == "quarter":
            return dt + relativedelta(months=3)
        elif self.interval == "year":
            return dt + relativedelta(years=1)
        else:
            return dt + timedelta(days=1)


class GeographicPartitioningStrategy(PartitioningStrategy):
    """Specialized strategy for geographic/regional list partitioning."""

    def __init__(self, regions: Dict[str, List[str]]):
        super().__init__(PartitionStrategy.LIST)
        self.regions = regions  # {"north": ["NY", "MA", "CT"], "south": ["FL", "GA", "AL"]}

    def generate_partition_name(self, parent_table: str, bounds: Dict[str, Any]) -> str:
        """Generate region-based partition names."""
        values = bounds.get('values', [])

        # Try to find matching region
        for region_name, region_values in self.regions.items():
            if any(val in region_values for val in values):
                return f"{parent_table}_p_{region_name}"

        # Fallback to generic naming
        if values:
            value_str = "_".join(str(v)[:5] for v in values[:3])
            return f"{parent_table}_p_region_{value_str}"

        return f"{parent_table}_p_region_unknown"

    def suggest_partitions(self, data_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Suggest region-based partitions."""
        suggestions = []

        value_counts = data_analysis.get('value_counts', {})
        if not value_counts:
            return []

        # Create partition for each defined region
        for region_name, region_values in self.regions.items():
            # Check if any of the region values exist in the data
            region_data_values = [val for val in region_values if val in value_counts]
            if region_data_values:
                suggestions.append({'values': region_data_values})

        # Create partition for values not in any defined region
        all_region_values = set()
        for region_values in self.regions.values():
            all_region_values.update(region_values)

        unassigned_values = [val for val in value_counts.keys() if val not in all_region_values]
        if unassigned_values:
            suggestions.append({'values': unassigned_values[:10]})  # Limit to first 10

        return suggestions


class HierarchicalPartitioningStrategy:
    """Strategy for creating hierarchical (sub-partitioned) tables."""

    def __init__(self,
                 primary_strategy: PartitioningStrategy,
                 secondary_strategy: PartitioningStrategy,
                 primary_column: str,
                 secondary_column: str):
        self.primary_strategy = primary_strategy
        self.secondary_strategy = secondary_strategy
        self.primary_column = primary_column
        self.secondary_column = secondary_column

    def suggest_hierarchical_partitions(self, data_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Suggest hierarchical partitioning scheme."""
        primary_analysis = data_analysis.get('primary_column', {})
        secondary_analysis = data_analysis.get('secondary_column', {})

        primary_partitions = self.primary_strategy.suggest_partitions(primary_analysis)
        secondary_partitions = self.secondary_strategy.suggest_partitions(secondary_analysis)

        return {
            "primary_partitions": primary_partitions,
            "secondary_partitions": secondary_partitions,
            "estimated_total_partitions": len(primary_partitions) * len(secondary_partitions),
            "primary_column": self.primary_column,
            "secondary_column": self.secondary_column,
            "primary_strategy": self.primary_strategy.strategy.value,
            "secondary_strategy": self.secondary_strategy.strategy.value
        }


class PartitionMaintenanceStrategy:
    """Automated partition maintenance operations."""

    def __init__(self, connection_pool, rbac_manager: RBACManager, audit_logger):
        self.connection_pool = connection_pool
        self.rbac_manager = rbac_manager
        self.audit_logger = audit_logger

    async def auto_create_time_partitions(self,
                                        parent_table: str,
                                        schema_name: str = "public",
                                        role_name: str = "default",
                                        advance_periods: int = 3,
                                        interval: str = "month") -> Dict[str, Any]:
        """Automatically create future time-based partitions.

        Args:
            parent_table: Name of the partitioned table
            schema_name: Schema name
            role_name: Role for permission checking
            advance_periods: Number of future periods to create
            interval: Time interval (day, week, month, quarter, year)

        Returns:
            Dictionary with creation results
        """
        try:
            from .create import CreatePartitionOperation

            create_op = CreatePartitionOperation(self.connection_pool, self.rbac_manager, self.audit_logger)
            time_strategy = TimeBasedPartitioningStrategy(interval)

            # Determine next partition start date
            current_time = datetime.utcnow()
            next_period_start = time_strategy._normalize_date_to_interval(current_time)
            next_period_start = time_strategy._add_interval(next_period_start)

            created_partitions = []
            errors = []

            for i in range(advance_periods):
                period_start = next_period_start
                period_end = time_strategy._add_interval(period_start)

                bounds = {
                    'start': period_start.strftime('%Y-%m-%d %H:%M:%S'),
                    'end': period_end.strftime('%Y-%m-%d %H:%M:%S')
                }

                partition_name = time_strategy.generate_partition_name(parent_table, bounds)

                try:
                    result = await create_op.create_partition(
                        parent_table=parent_table,
                        partition_name=partition_name,
                        bounds=bounds,
                        role_name=role_name,
                        schema_name=schema_name
                    )

                    if result.success:
                        created_partitions.append(partition_name)
                    else:
                        errors.extend(result.errors or [f"Failed to create partition {partition_name}"])

                except Exception as e:
                    errors.append(f"Error creating partition {partition_name}: {e}")

                next_period_start = period_end

            await self.audit_logger.audit_log(
                operation="auto_create_time_partitions",
                role=role_name,
                details={
                    "parent_table": f"{schema_name}.{parent_table}",
                    "created_partitions": created_partitions,
                    "advance_periods": advance_periods,
                    "interval": interval
                }
            )

            return {
                "success": len(errors) == 0,
                "created_partitions": created_partitions,
                "errors": errors,
                "advance_periods": advance_periods
            }

        except Exception as e:
            logger.error(f"Error in auto-create time partitions: {e}")
            return {
                "success": False,
                "created_partitions": [],
                "errors": [str(e)],
                "advance_periods": 0
            }

    async def cleanup_old_partitions(self,
                                   parent_table: str,
                                   schema_name: str = "public",
                                   role_name: str = "default",
                                   retention_days: int = 365,
                                   dry_run: bool = False) -> Dict[str, Any]:
        """Clean up old time-based partitions.

        Args:
            parent_table: Name of the partitioned table
            schema_name: Schema name
            role_name: Role for permission checking
            retention_days: Number of days to retain
            dry_run: If True, only show what would be deleted

        Returns:
            Dictionary with cleanup results
        """
        try:
            from .manage import ManagePartitionsOperation

            manage_op = ManagePartitionsOperation(self.connection_pool, self.rbac_manager, self.audit_logger)

            # Get list of partitions
            list_result = await manage_op.list_partitions(
                parent_table=parent_table,
                role_name=role_name,
                schema_name=schema_name,
                include_statistics=True
            )

            if not list_result.success:
                return {
                    "success": False,
                    "dropped_partitions": [],
                    "errors": ["Failed to list partitions"]
                }

            cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
            partitions_to_drop = []
            dropped_partitions = []
            errors = []

            # This would need to analyze partition bounds to determine age
            # For now, we'll use a simplified approach based on partition names

            async with self.connection_pool.acquire() as connection:
                # Get partitions with their bounds
                partitions_query = """
                    SELECT
                        c.relname as partition_name,
                        pg_get_expr(c.relpartbound, c.oid) as partition_bounds
                    FROM pg_class c
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    JOIN pg_inherits i ON c.oid = i.inhrelid
                    JOIN pg_class parent ON i.inhparent = parent.oid
                    WHERE n.nspname = $1 AND parent.relname = $2
                """

                partition_rows = await connection.fetch(partitions_query, schema_name, parent_table)

                for row in partition_rows:
                    partition_name = row['partition_name']
                    bounds_expr = row['partition_bounds']

                    # Try to extract date from partition bounds or name
                    partition_date = self._extract_partition_date(partition_name, bounds_expr)

                    if partition_date and partition_date < cutoff_date:
                        partitions_to_drop.append(partition_name)

                # Drop old partitions (unless dry run)
                if not dry_run:
                    for partition_name in partitions_to_drop:
                        try:
                            result = await manage_op.drop_partition(
                                partition_name=partition_name,
                                role_name=role_name,
                                schema_name=schema_name
                            )

                            if result.success:
                                dropped_partitions.append(partition_name)
                            else:
                                errors.extend(result.errors or [f"Failed to drop partition {partition_name}"])

                        except Exception as e:
                            errors.append(f"Error dropping partition {partition_name}: {e}")

            await self.audit_logger.audit_log(
                operation="cleanup_old_partitions",
                role=role_name,
                details={
                    "parent_table": f"{schema_name}.{parent_table}",
                    "retention_days": retention_days,
                    "partitions_identified": len(partitions_to_drop),
                    "partitions_dropped": len(dropped_partitions),
                    "dry_run": dry_run
                }
            )

            action = "Would drop" if dry_run else "Dropped"
            return {
                "success": len(errors) == 0,
                "action": action,
                "partitions_identified": partitions_to_drop,
                "partitions_dropped": dropped_partitions,
                "errors": errors,
                "dry_run": dry_run
            }

        except Exception as e:
            logger.error(f"Error in cleanup old partitions: {e}")
            return {
                "success": False,
                "partitions_dropped": [],
                "errors": [str(e)]
            }

    def _extract_partition_date(self, partition_name: str, bounds_expr: str) -> Optional[datetime]:
        """Extract date from partition name or bounds expression."""
        try:
            # Try to extract from partition name patterns
            import re

            # Pattern: table_p_YYYYMMDD
            date_match = re.search(r'_p_(\d{8})', partition_name)
            if date_match:
                date_str = date_match.group(1)
                return datetime.strptime(date_str, '%Y%m%d')

            # Pattern: table_p_YYYYMM
            month_match = re.search(r'_p_(\d{6})', partition_name)
            if month_match:
                date_str = month_match.group(1)
                return datetime.strptime(date_str, '%Y%m')

            # Try to extract from bounds expression
            if "FROM" in bounds_expr and "TO" in bounds_expr:
                # Extract date from bounds like "FOR VALUES FROM ('2023-01-01') TO ('2023-02-01')"
                date_match = re.search(r"FROM \('([^']+)'\)", bounds_expr)
                if date_match:
                    date_str = date_match.group(1)
                    return datetime.fromisoformat(date_str.replace('Z', '+00:00'))

        except Exception as e:
            logger.debug(f"Failed to extract partition date: {e}")

        return None