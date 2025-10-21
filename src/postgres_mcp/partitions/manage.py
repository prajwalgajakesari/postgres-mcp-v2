"""Partition management operations for PostgreSQL."""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Tuple

from .base import (
    PartitionOperation, PartitionOperationResult, PartitionInfo,
    PartitionStrategy, PartitioningStrategy
)
from ..permissions.rbac import Permission

logger = logging.getLogger(__name__)


class ManagePartitionsOperation(PartitionOperation):
    """Handles partition management operations like listing, analyzing, and maintenance."""

    async def list_partitions(self,
                            parent_table: str,
                            role_name: str = "default",
                            schema_name: str = "public",
                            include_statistics: bool = True,
                            order_by: str = "name") -> PartitionOperationResult:
        """List all partitions for a partitioned table.

        Args:
            parent_table: Name of the parent partitioned table
            role_name: Role for permission checking
            schema_name: Schema containing the table
            include_statistics: Whether to include partition statistics
            order_by: Order results by (name, size, rows)

        Returns:
            PartitionOperationResult with partition listing
        """
        try:
            self._validate_permissions(role_name, [Permission.VIEW_STATS])
            self._validate_identifier(parent_table)
            self._validate_identifier(schema_name)

            async with self.connection_pool.acquire() as connection:
                full_table_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(parent_table)}"

                # Check if table exists and is partitioned
                if not await self._table_exists(connection, parent_table, schema_name):
                    return PartitionOperationResult(
                        success=False,
                        message=f"Table '{full_table_name}' does not exist",
                        errors=[f"Table '{full_table_name}' not found"]
                    )

                if not await self._is_partitioned_table(connection, parent_table, schema_name):
                    return PartitionOperationResult(
                        success=False,
                        message=f"Table '{full_table_name}' is not a partitioned table",
                        errors=[f"Table '{full_table_name}' is not partitioned"]
                    )

                # Get partition information
                partitions_query = """
                    SELECT
                        c.relname as partition_name,
                        parent.relname as parent_table,
                        pt.partstrat as strategy,
                        pg_get_partkeydef(pt.partrelid) as partition_key,
                        pg_get_expr(c.relpartbound, c.oid) as partition_bounds,
                        c.oid as partition_oid,
                        ts.spcname as tablespace,
                        CASE
                            WHEN pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT' THEN true
                            ELSE false
                        END as is_default
                    FROM pg_class c
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    JOIN pg_inherits i ON c.oid = i.inhrelid
                    JOIN pg_class parent ON i.inhparent = parent.oid
                    JOIN pg_partitioned_table pt ON parent.oid = pt.partrelid
                    LEFT JOIN pg_tablespace ts ON c.reltablespace = ts.oid
                    WHERE n.nspname = $1 AND parent.relname = $2
                """

                if order_by == "name":
                    partitions_query += " ORDER BY c.relname"
                elif order_by == "size":
                    partitions_query += " ORDER BY pg_relation_size(c.oid) DESC"
                else:
                    partitions_query += " ORDER BY c.relname"

                partition_rows = await connection.fetch(partitions_query, schema_name, parent_table)

                partitions = []
                strategy_map = {'r': PartitionStrategy.RANGE, 'l': PartitionStrategy.LIST, 'h': PartitionStrategy.HASH}

                for row in partition_rows:
                    strategy = strategy_map.get(row['strategy'], PartitionStrategy.RANGE)
                    bounds = self._parse_partition_bounds(row['partition_bounds'], strategy)

                    partition_info = {
                        "partition_name": row['partition_name'],
                        "parent_table": row['parent_table'],
                        "strategy": strategy.value,
                        "partition_key": row['partition_key'],
                        "bounds": bounds,
                        "is_default": row['is_default'],
                        "tablespace": row['tablespace']
                    }

                    # Add statistics if requested
                    if include_statistics:
                        try:
                            # Get partition size
                            size_bytes = await connection.fetchval("SELECT pg_relation_size($1)", row['partition_oid'])
                            partition_info["size_bytes"] = size_bytes
                            partition_info["size_mb"] = round(size_bytes / (1024 * 1024), 2) if size_bytes else 0

                            # Get row count
                            partition_full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(row['partition_name'])}"
                            count_query = f"SELECT COUNT(*) FROM {partition_full_name}"
                            row_count = await connection.fetchval(count_query)
                            partition_info["row_count"] = row_count

                            # Check for indexes and constraints
                            indexes_query = """
                                SELECT COUNT(*) FROM pg_indexes
                                WHERE schemaname = $1 AND tablename = $2
                            """
                            index_count = await connection.fetchval(indexes_query, schema_name, row['partition_name'])
                            partition_info["has_indexes"] = index_count > 0
                            partition_info["index_count"] = index_count

                            constraints_query = """
                                SELECT COUNT(*) FROM information_schema.table_constraints
                                WHERE table_schema = $1 AND table_name = $2
                            """
                            constraint_count = await connection.fetchval(constraints_query, schema_name, row['partition_name'])
                            partition_info["has_constraints"] = constraint_count > 0
                            partition_info["constraint_count"] = constraint_count

                        except Exception as e:
                            logger.warning(f"Failed to get statistics for partition {row['partition_name']}: {e}")

                    partitions.append(partition_info)

                # Sort by order_by if it's based on statistics
                if order_by == "size" and include_statistics:
                    partitions.sort(key=lambda p: p.get("size_bytes", 0), reverse=True)
                elif order_by == "rows" and include_statistics:
                    partitions.sort(key=lambda p: p.get("row_count", 0), reverse=True)

                await self.audit_logger.audit_log(
                    operation="list_partitions",
                    role=role_name,
                    details={
                        "parent_table": full_table_name,
                        "partitions_found": len(partitions),
                        "include_statistics": include_statistics
                    }
                )

                return PartitionOperationResult(
                    success=True,
                    message=f"Found {len(partitions)} partitions for table '{full_table_name}'",
                    affected_partitions=[p["partition_name"] for p in partitions]
                )

        except Exception as e:
            logger.error(f"Error listing partitions: {e}")
            return PartitionOperationResult(
                success=False,
                message=f"Failed to list partitions: {e}",
                errors=[str(e)]
            )

    async def analyze_partition_distribution(self,
                                           parent_table: str,
                                           role_name: str = "default",
                                           schema_name: str = "public",
                                           suggest_optimizations: bool = True) -> PartitionOperationResult:
        """Analyze partition data distribution and performance.

        Args:
            parent_table: Name of the parent partitioned table
            role_name: Role for permission checking
            schema_name: Schema containing the table
            suggest_optimizations: Whether to suggest optimizations

        Returns:
            PartitionOperationResult with analysis details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.VIEW_STATS, Permission.ANALYZE])
            self._validate_identifier(parent_table)
            self._validate_identifier(schema_name)

            async with self.connection_pool.acquire() as connection:
                full_table_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(parent_table)}"

                # Check if table exists and is partitioned
                if not await self._is_partitioned_table(connection, parent_table, schema_name):
                    return PartitionOperationResult(
                        success=False,
                        message=f"Table '{full_table_name}' is not a partitioned table",
                        errors=[f"Table '{full_table_name}' is not partitioned"]
                    )

                # Get partitioning strategy and key
                strategy_query = """
                    SELECT
                        pt.partstrat as strategy,
                        pg_get_partkeydef(pt.partrelid) as partition_key
                    FROM pg_class c
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    JOIN pg_partitioned_table pt ON c.oid = pt.partrelid
                    WHERE n.nspname = $1 AND c.relname = $2
                """
                strategy_info = await connection.fetchrow(strategy_query, schema_name, parent_table)

                if not strategy_info:
                    return PartitionOperationResult(
                        success=False,
                        message=f"Could not retrieve partitioning information for '{full_table_name}'",
                        errors=["Failed to get partitioning info"]
                    )

                strategy_map = {'r': PartitionStrategy.RANGE, 'l': PartitionStrategy.LIST, 'h': PartitionStrategy.HASH}
                strategy = strategy_map.get(strategy_info['strategy'], PartitionStrategy.RANGE)

                # Analyze each partition
                analysis = {
                    "table_name": full_table_name,
                    "strategy": strategy.value,
                    "partition_key": strategy_info['partition_key'],
                    "analysis_time": datetime.utcnow().isoformat(),
                    "partitions": [],
                    "summary": {},
                    "recommendations": []
                }

                # Get partition list
                list_result = await self.list_partitions(
                    parent_table, role_name, schema_name, include_statistics=True
                )

                if not list_result.success:
                    return list_result

                partitions_info = []  # This would be populated from list_result in real implementation

                # Analyze distribution
                total_size = 0
                total_rows = 0
                empty_partitions = 0
                oversized_partitions = []

                for partition in partitions_info:
                    size_bytes = partition.get("size_bytes", 0)
                    row_count = partition.get("row_count", 0)

                    total_size += size_bytes
                    total_rows += row_count

                    if row_count == 0:
                        empty_partitions += 1

                    # Consider partition oversized if it's > 30% of total data
                    if size_bytes > 0 and total_size > 0:
                        size_ratio = size_bytes / total_size
                        if size_ratio > 0.3:
                            oversized_partitions.append(partition["partition_name"])

                    analysis["partitions"].append({
                        "name": partition["partition_name"],
                        "size_bytes": size_bytes,
                        "row_count": row_count,
                        "bounds": partition.get("bounds", {}),
                        "is_default": partition.get("is_default", False)
                    })

                # Summary statistics
                analysis["summary"] = {
                    "total_partitions": len(partitions_info),
                    "total_size_bytes": total_size,
                    "total_size_mb": round(total_size / (1024 * 1024), 2),
                    "total_rows": total_rows,
                    "empty_partitions": empty_partitions,
                    "average_partition_size_mb": round((total_size / (1024 * 1024)) / max(len(partitions_info), 1), 2),
                    "average_rows_per_partition": round(total_rows / max(len(partitions_info), 1))
                }

                # Generate recommendations
                if suggest_optimizations:
                    recommendations = []

                    if empty_partitions > 0:
                        recommendations.append(f"Consider dropping {empty_partitions} empty partitions to reduce overhead")

                    if oversized_partitions:
                        recommendations.append(f"Partitions {oversized_partitions} are oversized and may need splitting")

                    if len(partitions_info) > 100:
                        recommendations.append("Large number of partitions may impact query planning performance")

                    if strategy == PartitionStrategy.RANGE and len(partitions_info) < 3:
                        recommendations.append("Range partitioning with few partitions may not provide significant benefits")

                    analysis["recommendations"] = recommendations

                duration = time.time() - start_time

                await self.audit_logger.audit_log(
                    operation="analyze_partition_distribution",
                    role=role_name,
                    details={
                        "table_name": full_table_name,
                        "total_partitions": len(partitions_info),
                        "total_size_mb": analysis["summary"]["total_size_mb"],
                        "duration_seconds": duration
                    }
                )

                return PartitionOperationResult(
                    success=True,
                    message=f"Partition analysis completed for '{full_table_name}'",
                    duration_seconds=duration
                )

        except Exception as e:
            logger.error(f"Error analyzing partition distribution: {e}")
            return PartitionOperationResult(
                success=False,
                message=f"Failed to analyze partition distribution: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def suggest_partitioning_strategy(self,
                                          table_name: str,
                                          column_name: str,
                                          role_name: str = "default",
                                          schema_name: str = "public") -> PartitionOperationResult:
        """Analyze a table and suggest optimal partitioning strategy.

        Args:
            table_name: Name of the table to analyze
            column_name: Column to analyze for partitioning
            role_name: Role for permission checking
            schema_name: Schema containing the table

        Returns:
            PartitionOperationResult with strategy suggestions
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.VIEW_STATS])
            self._validate_identifier(table_name)
            self._validate_identifier(column_name)
            self._validate_identifier(schema_name)

            async with self.connection_pool.acquire() as connection:
                full_table_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"

                # Check if table exists
                if not await self._table_exists(connection, table_name, schema_name):
                    return PartitionOperationResult(
                        success=False,
                        message=f"Table '{full_table_name}' does not exist",
                        errors=[f"Table '{full_table_name}' not found"]
                    )

                # Get column information
                column_info_query = """
                    SELECT data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
                """
                column_info = await connection.fetchrow(column_info_query, schema_name, table_name, column_name)

                if not column_info:
                    return PartitionOperationResult(
                        success=False,
                        message=f"Column '{column_name}' not found in table '{full_table_name}'",
                        errors=[f"Column '{column_name}' not found"]
                    )

                # Analyze table and column data
                data_analysis = await self._analyze_column_for_partitioning(
                    connection, full_table_name, column_name, column_info['data_type']
                )

                # Generate suggestions for each strategy
                suggestions = {
                    "table_name": full_table_name,
                    "column_name": column_name,
                    "column_type": column_info['data_type'],
                    "analysis": data_analysis,
                    "strategies": {}
                }

                # Range partitioning suggestions
                range_strategy = self.get_strategy(PartitionStrategy.RANGE)
                range_suggestions = range_strategy.suggest_partitions(data_analysis)
                if range_suggestions:
                    suggestions["strategies"]["range"] = {
                        "recommended": True,
                        "reason": "Good for time-series data or numeric ranges with clear patterns",
                        "sample_partitions": range_suggestions[:5],  # Show first 5 suggestions
                        "total_suggested_partitions": len(range_suggestions)
                    }

                # List partitioning suggestions
                list_strategy = self.get_strategy(PartitionStrategy.LIST)
                list_suggestions = list_strategy.suggest_partitions(data_analysis)
                if list_suggestions:
                    suggestions["strategies"]["list"] = {
                        "recommended": True,
                        "reason": "Good for categorical data with distinct values",
                        "sample_partitions": list_suggestions[:3],
                        "total_suggested_partitions": len(list_suggestions)
                    }

                # Hash partitioning suggestions
                hash_strategy = self.get_strategy(PartitionStrategy.HASH)
                hash_suggestions = hash_strategy.suggest_partitions(data_analysis)
                if hash_suggestions:
                    suggestions["strategies"]["hash"] = {
                        "recommended": True,
                        "reason": "Good for even distribution when other strategies don't fit",
                        "sample_partitions": hash_suggestions[:4],
                        "total_suggested_partitions": len(hash_suggestions)
                    }

                # Determine best strategy
                best_strategy = self._determine_best_strategy(data_analysis, column_info['data_type'])
                suggestions["recommended_strategy"] = best_strategy

                duration = time.time() - start_time

                await self.audit_logger.audit_log(
                    operation="suggest_partitioning_strategy",
                    role=role_name,
                    details={
                        "table_name": full_table_name,
                        "column_name": column_name,
                        "recommended_strategy": best_strategy,
                        "duration_seconds": duration
                    }
                )

                return PartitionOperationResult(
                    success=True,
                    message=f"Partitioning strategy analysis completed for '{full_table_name}.{column_name}'",
                    duration_seconds=duration
                )

        except Exception as e:
            logger.error(f"Error suggesting partitioning strategy: {e}")
            return PartitionOperationResult(
                success=False,
                message=f"Failed to suggest partitioning strategy: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def drop_partition(self,
                           partition_name: str,
                           role_name: str = "default",
                           schema_name: str = "public",
                           cascade: bool = False) -> PartitionOperationResult:
        """Drop a partition.

        Args:
            partition_name: Name of the partition to drop
            role_name: Role for permission checking
            schema_name: Schema containing the partition
            cascade: Whether to cascade the drop

        Returns:
            PartitionOperationResult with drop details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.DROP_TABLE, Permission.MANAGE_PARTITIONS])
            self._validate_identifier(partition_name)
            self._validate_identifier(schema_name)

            async with self.connection_pool.acquire() as connection:
                full_partition_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(partition_name)}"

                # Check if partition exists
                if not await self._table_exists(connection, partition_name, schema_name):
                    return PartitionOperationResult(
                        success=False,
                        message=f"Partition '{full_partition_name}' does not exist",
                        duration_seconds=time.time() - start_time,
                        errors=[f"Partition '{full_partition_name}' not found"]
                    )

                # Get partition info before dropping
                partition_info = await self._get_partition_info(connection, partition_name, schema_name)

                # Build DROP TABLE statement
                drop_query = f"DROP TABLE {full_partition_name}"
                if cascade:
                    drop_query += " CASCADE"

                logger.info(f"Dropping partition: {full_partition_name}")

                # Execute drop
                await connection.execute(drop_query)

                duration = time.time() - start_time

                await self.audit_logger.audit_log(
                    operation="drop_partition",
                    role=role_name,
                    details={
                        "partition_name": full_partition_name,
                        "parent_table": partition_info.parent_table if partition_info else None,
                        "cascade": cascade,
                        "duration_seconds": duration
                    }
                )

                return PartitionOperationResult(
                    success=True,
                    message=f"Partition '{full_partition_name}' dropped successfully",
                    partition_info=partition_info,
                    duration_seconds=duration
                )

        except Exception as e:
            logger.error(f"Error dropping partition: {e}")
            await self.audit_logger.audit_log(
                operation="drop_partition",
                role=role_name,
                details={
                    "partition_name": f"{schema_name}.{partition_name}",
                    "error": str(e)
                }
            )
            return PartitionOperationResult(
                success=False,
                message=f"Failed to drop partition: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def _analyze_column_for_partitioning(self,
                                             connection,
                                             table_name: str,
                                             column_name: str,
                                             column_type: str) -> Dict[str, Any]:
        """Analyze column data to determine partitioning suitability."""
        try:
            # Get basic statistics
            stats_query = f"""
                SELECT
                    COUNT(*) as row_count,
                    COUNT(DISTINCT {self._quote_identifier(column_name)}) as distinct_values,
                    MIN({self._quote_identifier(column_name)}) as min_value,
                    MAX({self._quote_identifier(column_name)}) as max_value
                FROM {table_name}
            """

            stats = await connection.fetchrow(stats_query)

            # Get value distribution for categorical analysis
            value_counts = {}
            if stats['distinct_values'] <= 100:  # Only for manageable number of distinct values
                try:
                    value_dist_query = f"""
                        SELECT {self._quote_identifier(column_name)}, COUNT(*) as count
                        FROM {table_name}
                        GROUP BY {self._quote_identifier(column_name)}
                        ORDER BY count DESC
                        LIMIT 20
                    """
                    value_rows = await connection.fetch(value_dist_query)
                    value_counts = {str(row[0]): row[1] for row in value_rows}
                except Exception:
                    pass

            return {
                "row_count": stats['row_count'],
                "distinct_values": stats['distinct_values'],
                "column_stats": {
                    "min": stats['min_value'],
                    "max": stats['max_value'],
                    "type": column_type
                },
                "value_counts": value_counts,
                "cardinality_ratio": stats['distinct_values'] / max(stats['row_count'], 1)
            }

        except Exception as e:
            logger.warning(f"Failed to analyze column for partitioning: {e}")
            return {"row_count": 0, "distinct_values": 0}

    def _determine_best_strategy(self, data_analysis: Dict[str, Any], column_type: str) -> str:
        """Determine the best partitioning strategy based on data analysis."""
        row_count = data_analysis.get("row_count", 0)
        distinct_values = data_analysis.get("distinct_values", 0)
        cardinality_ratio = data_analysis.get("cardinality_ratio", 1)

        # Not worth partitioning small tables
        if row_count < 10000:
            return "none"

        # For date/timestamp columns, range is usually best
        if column_type.lower() in ['date', 'timestamp', 'timestamptz']:
            return "range"

        # For low cardinality categorical data, list partitioning
        if distinct_values <= 20 and cardinality_ratio < 0.1:
            return "list"

        # For high cardinality with no clear pattern, hash partitioning
        if cardinality_ratio > 0.5:
            return "hash"

        # For numeric data with clear ranges, range partitioning
        if column_type.lower() in ['integer', 'bigint', 'numeric']:
            return "range"

        # Default to hash for even distribution
        return "hash"

    async def execute(self, operation_type: str, **kwargs) -> PartitionOperationResult:
        """Execute the specified partition management operation."""
        if operation_type == "list":
            return await self.list_partitions(**kwargs)
        elif operation_type == "analyze_distribution":
            return await self.analyze_partition_distribution(**kwargs)
        elif operation_type == "suggest_strategy":
            return await self.suggest_partitioning_strategy(**kwargs)
        elif operation_type == "drop":
            return await self.drop_partition(**kwargs)
        else:
            raise ValueError(f"Unknown partition management operation: {operation_type}")