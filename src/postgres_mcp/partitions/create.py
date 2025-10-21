"""Create partition operations for PostgreSQL."""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional

from .base import (
    PartitionOperation, PartitionOperationResult, PartitionInfo,
    PartitionStrategy, PartitioningStrategy
)
from ..permissions.rbac import Permission

logger = logging.getLogger(__name__)


class CreatePartitionOperation(PartitionOperation):
    """Handles partition creation operations."""

    async def create_partitioned_table(self,
                                     table_name: str,
                                     columns: List[Dict[str, Any]],
                                     partition_key: str,
                                     strategy: PartitionStrategy,
                                     role_name: str = "default",
                                     schema_name: str = "public",
                                     tablespace: Optional[str] = None,
                                     constraints: Optional[List[Dict[str, Any]]] = None) -> PartitionOperationResult:
        """Create a new partitioned table.

        Args:
            table_name: Name of the table to create
            columns: List of column definitions
            partition_key: Column(s) to partition by
            strategy: Partitioning strategy (range, list, hash)
            role_name: Role for permission checking
            schema_name: Schema for the table
            tablespace: Optional tablespace
            constraints: Optional table constraints

        Returns:
            PartitionOperationResult with creation details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.CREATE_TABLE, Permission.CREATE_PARTITION])
            self._validate_identifier(table_name)
            self._validate_identifier(schema_name)
            self._validate_identifier(partition_key)

            async with self.connection_pool.acquire() as connection:
                full_table_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"

                # Check if table already exists
                if await self._table_exists(connection, table_name, schema_name):
                    return PartitionOperationResult(
                        success=False,
                        message=f"Table '{full_table_name}' already exists",
                        duration_seconds=time.time() - start_time,
                        errors=[f"Table '{full_table_name}' exists"]
                    )

                # Validate partition key exists in columns
                column_names = [col['name'] for col in columns]
                if partition_key not in column_names:
                    return PartitionOperationResult(
                        success=False,
                        message=f"Partition key '{partition_key}' not found in table columns",
                        duration_seconds=time.time() - start_time,
                        errors=[f"Column '{partition_key}' not found"]
                    )

                # Build column definitions
                column_defs = []
                for col in columns:
                    col_def = f"{self._quote_identifier(col['name'])} {col['type']}"

                    if col.get('not_null', False):
                        col_def += " NOT NULL"

                    if col.get('default') is not None:
                        col_def += f" DEFAULT {col['default']}"

                    column_defs.append(col_def)

                # Build constraints
                constraint_defs = []
                if constraints:
                    for constraint in constraints:
                        if constraint['type'] == 'PRIMARY KEY':
                            cols = ', '.join(self._quote_identifier(c) for c in constraint['columns'])
                            constraint_defs.append(f"PRIMARY KEY ({cols})")
                        elif constraint['type'] == 'UNIQUE':
                            cols = ', '.join(self._quote_identifier(c) for c in constraint['columns'])
                            constraint_defs.append(f"UNIQUE ({cols})")
                        elif constraint['type'] == 'CHECK':
                            constraint_defs.append(f"CHECK ({constraint['expression']})")

                # Build CREATE TABLE statement
                create_parts = [f"CREATE TABLE {full_table_name} ("]
                create_parts.append("  " + ",\n  ".join(column_defs + constraint_defs))
                create_parts.append(f") PARTITION BY {strategy.value.upper()} ({self._quote_identifier(partition_key)})")

                if tablespace:
                    create_parts.append(f"TABLESPACE {self._quote_identifier(tablespace)}")

                create_query = "\n".join(create_parts)

                logger.info(f"Creating partitioned table: {full_table_name}")

                # Execute table creation
                await connection.execute(create_query)

                duration = time.time() - start_time

                await self.audit_logger.audit_log(
                    operation="create_partitioned_table",
                    role=role_name,
                    details={
                        "table_name": full_table_name,
                        "strategy": strategy.value,
                        "partition_key": partition_key,
                        "columns": len(columns),
                        "duration_seconds": duration
                    }
                )

                return PartitionOperationResult(
                    success=True,
                    message=f"Partitioned table '{full_table_name}' created successfully",
                    duration_seconds=duration
                )

        except Exception as e:
            logger.error(f"Error creating partitioned table: {e}")
            await self.audit_logger.audit_log(
                operation="create_partitioned_table",
                role=role_name,
                details={
                    "table_name": f"{schema_name}.{table_name}",
                    "error": str(e)
                }
            )
            return PartitionOperationResult(
                success=False,
                message=f"Failed to create partitioned table: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def create_partition(self,
                             parent_table: str,
                             partition_name: str,
                             bounds: Dict[str, Any],
                             role_name: str = "default",
                             schema_name: str = "public",
                             tablespace: Optional[str] = None,
                             inherit_constraints: bool = True) -> PartitionOperationResult:
        """Create a new partition.

        Args:
            parent_table: Name of the parent partitioned table
            partition_name: Name of the new partition
            bounds: Partition bounds (strategy-specific)
            role_name: Role for permission checking
            schema_name: Schema for the partition
            tablespace: Optional tablespace for partition
            inherit_constraints: Whether to inherit parent constraints

        Returns:
            PartitionOperationResult with creation details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.CREATE_TABLE, Permission.CREATE_PARTITION])
            self._validate_identifier(parent_table)
            self._validate_identifier(partition_name)
            self._validate_identifier(schema_name)

            async with self.connection_pool.acquire() as connection:
                full_parent_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(parent_table)}"
                full_partition_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(partition_name)}"

                # Check if parent table exists and is partitioned
                if not await self._table_exists(connection, parent_table, schema_name):
                    return PartitionOperationResult(
                        success=False,
                        message=f"Parent table '{full_parent_name}' does not exist",
                        duration_seconds=time.time() - start_time,
                        errors=[f"Parent table '{full_parent_name}' not found"]
                    )

                if not await self._is_partitioned_table(connection, parent_table, schema_name):
                    return PartitionOperationResult(
                        success=False,
                        message=f"Table '{full_parent_name}' is not a partitioned table",
                        duration_seconds=time.time() - start_time,
                        errors=[f"Table '{full_parent_name}' is not partitioned"]
                    )

                # Check if partition already exists
                if await self._table_exists(connection, partition_name, schema_name):
                    return PartitionOperationResult(
                        success=False,
                        message=f"Partition '{full_partition_name}' already exists",
                        duration_seconds=time.time() - start_time,
                        errors=[f"Partition '{full_partition_name}' exists"]
                    )

                # Get parent table partitioning strategy
                strategy_query = """
                    SELECT pt.partstrat, pg_get_partkeydef(pt.partrelid) as partition_key
                    FROM pg_class c
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    JOIN pg_partitioned_table pt ON c.oid = pt.partrelid
                    WHERE n.nspname = $1 AND c.relname = $2
                """
                parent_info = await connection.fetchrow(strategy_query, schema_name, parent_table)

                if not parent_info:
                    return PartitionOperationResult(
                        success=False,
                        message=f"Could not retrieve partitioning information for '{full_parent_name}'",
                        duration_seconds=time.time() - start_time,
                        errors=["Failed to get parent partition info"]
                    )

                # Map strategy code to enum
                strategy_map = {'r': PartitionStrategy.RANGE, 'l': PartitionStrategy.LIST, 'h': PartitionStrategy.HASH}
                strategy = strategy_map.get(parent_info['partstrat'], PartitionStrategy.RANGE)

                # Get column type for validation
                partition_key = parent_info['partition_key'].split('(')[1].split(')')[0].strip()
                column_type_query = """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
                """
                column_type = await connection.fetchval(column_type_query, schema_name, parent_table, partition_key)

                # Validate bounds using strategy
                strategy_impl = self.get_strategy(strategy)
                strategy_impl.validate_bounds(bounds, column_type or 'text')

                # Build partition bounds SQL
                bounds_sql = strategy_impl.format_bounds_for_sql(bounds)

                # Build CREATE TABLE statement
                create_parts = [f"CREATE TABLE {full_partition_name}"]
                create_parts.append(f"PARTITION OF {full_parent_name}")
                create_parts.append(f"FOR VALUES {bounds_sql}")

                if tablespace:
                    create_parts.append(f"TABLESPACE {self._quote_identifier(tablespace)}")

                create_query = " ".join(create_parts)

                logger.info(f"Creating partition: {full_partition_name}")

                # Execute partition creation
                await connection.execute(create_query)

                # Get partition info
                partition_info = await self._get_partition_info(connection, partition_name, schema_name)

                duration = time.time() - start_time

                await self.audit_logger.audit_log(
                    operation="create_partition",
                    role=role_name,
                    details={
                        "parent_table": full_parent_name,
                        "partition_name": full_partition_name,
                        "strategy": strategy.value,
                        "bounds": bounds,
                        "duration_seconds": duration
                    }
                )

                return PartitionOperationResult(
                    success=True,
                    message=f"Partition '{full_partition_name}' created successfully",
                    partition_info=partition_info,
                    duration_seconds=duration
                )

        except Exception as e:
            logger.error(f"Error creating partition: {e}")
            await self.audit_logger.audit_log(
                operation="create_partition",
                role=role_name,
                details={
                    "parent_table": f"{schema_name}.{parent_table}",
                    "partition_name": f"{schema_name}.{partition_name}",
                    "error": str(e)
                }
            )
            return PartitionOperationResult(
                success=False,
                message=f"Failed to create partition: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def create_default_partition(self,
                                     parent_table: str,
                                     partition_name: Optional[str] = None,
                                     role_name: str = "default",
                                     schema_name: str = "public",
                                     tablespace: Optional[str] = None) -> PartitionOperationResult:
        """Create a default partition to catch unmatched values.

        Args:
            parent_table: Name of the parent partitioned table
            partition_name: Name of the default partition (auto-generated if None)
            role_name: Role for permission checking
            schema_name: Schema for the partition
            tablespace: Optional tablespace for partition

        Returns:
            PartitionOperationResult with creation details
        """
        try:
            if not partition_name:
                partition_name = f"{parent_table}_default"

            async with self.connection_pool.acquire() as connection:
                full_parent_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(parent_table)}"
                full_partition_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(partition_name)}"

                # Build CREATE TABLE statement for default partition
                create_parts = [f"CREATE TABLE {full_partition_name}"]
                create_parts.append(f"PARTITION OF {full_parent_name}")
                create_parts.append("DEFAULT")

                if tablespace:
                    create_parts.append(f"TABLESPACE {self._quote_identifier(tablespace)}")

                create_query = " ".join(create_parts)

                logger.info(f"Creating default partition: {full_partition_name}")

                # Execute partition creation
                await connection.execute(create_query)

                # Get partition info
                partition_info = await self._get_partition_info(connection, partition_name, schema_name)
                if partition_info:
                    partition_info.is_default = True

                await self.audit_logger.audit_log(
                    operation="create_default_partition",
                    role=role_name,
                    details={
                        "parent_table": full_parent_name,
                        "partition_name": full_partition_name
                    }
                )

                return PartitionOperationResult(
                    success=True,
                    message=f"Default partition '{full_partition_name}' created successfully",
                    partition_info=partition_info
                )

        except Exception as e:
            logger.error(f"Error creating default partition: {e}")
            return PartitionOperationResult(
                success=False,
                message=f"Failed to create default partition: {e}",
                errors=[str(e)]
            )

    async def create_multiple_partitions(self,
                                       parent_table: str,
                                       partitions: List[Dict[str, Any]],
                                       role_name: str = "default",
                                       schema_name: str = "public",
                                       continue_on_error: bool = False) -> PartitionOperationResult:
        """Create multiple partitions at once.

        Args:
            parent_table: Name of the parent partitioned table
            partitions: List of partition definitions
            role_name: Role for permission checking
            schema_name: Schema for the partitions
            continue_on_error: Whether to continue if one partition fails

        Returns:
            PartitionOperationResult with creation details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.CREATE_TABLE, Permission.CREATE_PARTITION])

            created_partitions = []
            failed_partitions = []
            errors = []

            for partition_def in partitions:
                try:
                    partition_name = partition_def['name']
                    bounds = partition_def['bounds']
                    tablespace = partition_def.get('tablespace')

                    result = await self.create_partition(
                        parent_table=parent_table,
                        partition_name=partition_name,
                        bounds=bounds,
                        role_name=role_name,
                        schema_name=schema_name,
                        tablespace=tablespace
                    )

                    if result.success:
                        created_partitions.append(partition_name)
                    else:
                        failed_partitions.append(partition_name)
                        if result.errors:
                            errors.extend(result.errors)

                        if not continue_on_error:
                            break

                except Exception as e:
                    error_msg = f"Failed to create partition '{partition_def.get('name', 'unknown')}': {e}"
                    errors.append(error_msg)
                    failed_partitions.append(partition_def.get('name', 'unknown'))
                    logger.error(error_msg)

                    if not continue_on_error:
                        break

            duration = time.time() - start_time

            success = len(failed_partitions) == 0
            message = f"Created {len(created_partitions)} partitions"
            if failed_partitions:
                message += f", {len(failed_partitions)} failed"

            await self.audit_logger.audit_log(
                operation="create_multiple_partitions",
                role=role_name,
                details={
                    "parent_table": f"{schema_name}.{parent_table}",
                    "requested_partitions": len(partitions),
                    "created_partitions": created_partitions,
                    "failed_partitions": failed_partitions,
                    "duration_seconds": duration
                }
            )

            return PartitionOperationResult(
                success=success,
                message=message,
                affected_partitions=created_partitions,
                duration_seconds=duration,
                errors=errors if errors else None
            )

        except Exception as e:
            logger.error(f"Error creating multiple partitions: {e}")
            return PartitionOperationResult(
                success=False,
                message=f"Failed to create multiple partitions: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def execute(self, operation_type: str, **kwargs) -> PartitionOperationResult:
        """Execute the specified partition creation operation."""
        if operation_type == "create_table":
            return await self.create_partitioned_table(**kwargs)
        elif operation_type == "create_partition":
            return await self.create_partition(**kwargs)
        elif operation_type == "create_default":
            return await self.create_default_partition(**kwargs)
        elif operation_type == "create_multiple":
            return await self.create_multiple_partitions(**kwargs)
        else:
            raise ValueError(f"Unknown partition creation operation: {operation_type}")