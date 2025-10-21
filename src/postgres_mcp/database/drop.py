"""Drop database operations for PostgreSQL."""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional

from .base import DatabaseOperation, DatabaseOperationResult
from ..permissions.rbac import Permission

logger = logging.getLogger(__name__)


class DropDatabaseOperation(DatabaseOperation):
    """Handles database deletion operations."""

    async def drop_database(self,
                           database_name: str,
                           role_name: str = "default",
                           force: bool = False,
                           if_exists: bool = False) -> DatabaseOperationResult:
        """Drop a database.

        Args:
            database_name: Name of the database to drop
            role_name: Role for permission checking
            force: Force drop even with active connections
            if_exists: Don't error if database doesn't exist

        Returns:
            DatabaseOperationResult with deletion details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.DROP_DATABASE])
            self._validate_identifier(database_name)

            async with self.connection_pool.acquire() as connection:
                # Check if database exists
                if not await self._database_exists(connection, database_name):
                    if if_exists:
                        return DatabaseOperationResult(
                            success=True,
                            message=f"Database '{database_name}' does not exist (IF EXISTS used)",
                            duration_seconds=time.time() - start_time
                        )
                    else:
                        return DatabaseOperationResult(
                            success=False,
                            message=f"Database '{database_name}' does not exist",
                            duration_seconds=time.time() - start_time,
                            errors=[f"Database '{database_name}' not found"]
                        )

                # Check if it's a system database
                if database_name.lower() in ['postgres', 'template0', 'template1']:
                    return DatabaseOperationResult(
                        success=False,
                        message=f"Cannot drop system database '{database_name}'",
                        duration_seconds=time.time() - start_time,
                        errors=[f"'{database_name}' is a system database"]
                    )

                # Get database info before deletion
                db_info = await self._get_database_info(connection, database_name)

                # Check for active connections
                activity = await self._check_database_activity(connection, database_name)
                active_connections = activity.get('active_connections', 0)

                if active_connections > 0 and not force:
                    return DatabaseOperationResult(
                        success=False,
                        message=f"Database '{database_name}' has {active_connections} active connections. Use force=True to terminate them.",
                        duration_seconds=time.time() - start_time,
                        errors=[f"{active_connections} active connections prevent drop"]
                    )

                # Terminate connections if force is enabled
                terminated_connections = 0
                if force and activity.get('total_connections', 0) > 0:
                    terminated_connections = await self._terminate_database_connections(connection, database_name)
                    logger.info(f"Terminated {terminated_connections} connections to database '{database_name}'")

                    # Wait a moment for connections to fully terminate
                    await asyncio.sleep(0.5)

                # Build DROP DATABASE command
                drop_query = f"DROP DATABASE"
                if if_exists:
                    drop_query += " IF EXISTS"
                drop_query += f" {self._quote_identifier(database_name)}"

                logger.info(f"Dropping database: {database_name}")

                # Execute database drop (must be outside transaction)
                await connection.execute(drop_query)

                duration = time.time() - start_time

                await self.audit_logger.audit_log(
                    operation="drop_database",
                    role=role_name,
                    details={
                        "database_name": database_name,
                        "force": force,
                        "terminated_connections": terminated_connections,
                        "duration_seconds": duration
                    }
                )

                message = f"Database '{database_name}' dropped successfully"
                if terminated_connections > 0:
                    message += f" (terminated {terminated_connections} connections)"

                return DatabaseOperationResult(
                    success=True,
                    message=message,
                    database_info=db_info,
                    duration_seconds=duration
                )

        except Exception as e:
            logger.error(f"Error dropping database: {e}")
            await self.audit_logger.audit_log(
                operation="drop_database",
                role=role_name,
                details={
                    "database_name": database_name,
                    "error": str(e)
                }
            )
            return DatabaseOperationResult(
                success=False,
                message=f"Failed to drop database: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def drop_multiple_databases(self,
                                    database_names: List[str],
                                    role_name: str = "default",
                                    force: bool = False,
                                    if_exists: bool = False,
                                    continue_on_error: bool = False) -> DatabaseOperationResult:
        """Drop multiple databases.

        Args:
            database_names: List of database names to drop
            role_name: Role for permission checking
            force: Force drop even with active connections
            if_exists: Don't error if database doesn't exist
            continue_on_error: Continue dropping other databases if one fails

        Returns:
            DatabaseOperationResult with deletion details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.DROP_DATABASE])

            dropped_databases = []
            failed_databases = []
            warnings = []
            errors = []

            for database_name in database_names:
                try:
                    result = await self.drop_database(
                        database_name=database_name,
                        role_name=role_name,
                        force=force,
                        if_exists=if_exists
                    )

                    if result.success:
                        dropped_databases.append(database_name)
                        if result.warnings:
                            warnings.extend(result.warnings)
                    else:
                        failed_databases.append(database_name)
                        if result.errors:
                            errors.extend(result.errors)

                        if not continue_on_error:
                            break

                except Exception as e:
                    error_msg = f"Failed to drop database '{database_name}': {e}"
                    errors.append(error_msg)
                    failed_databases.append(database_name)
                    logger.error(error_msg)

                    if not continue_on_error:
                        break

            duration = time.time() - start_time

            success = len(failed_databases) == 0
            message = f"Dropped {len(dropped_databases)} databases"
            if failed_databases:
                message += f", {len(failed_databases)} failed"

            await self.audit_logger.audit_log(
                operation="drop_multiple_databases",
                role=role_name,
                details={
                    "requested_databases": database_names,
                    "dropped_databases": dropped_databases,
                    "failed_databases": failed_databases,
                    "duration_seconds": duration
                }
            )

            return DatabaseOperationResult(
                success=success,
                message=message,
                affected_databases=dropped_databases,
                duration_seconds=duration,
                warnings=warnings,
                errors=errors
            )

        except Exception as e:
            logger.error(f"Error dropping multiple databases: {e}")
            return DatabaseOperationResult(
                success=False,
                message=f"Failed to drop databases: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def drop_database_cascade(self,
                                  database_name: str,
                                  role_name: str = "default",
                                  backup_first: bool = True,
                                  backup_path: Optional[str] = None) -> DatabaseOperationResult:
        """Drop database with cascade behavior and optional backup.

        Args:
            database_name: Name of the database to drop
            role_name: Role for permission checking
            backup_first: Create backup before dropping
            backup_path: Path for backup file

        Returns:
            DatabaseOperationResult with deletion details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.DROP_DATABASE])

            if backup_first:
                self._validate_permissions(role_name, [Permission.CREATE_BACKUP])

            async with self.connection_pool.acquire() as connection:
                if not await self._database_exists(connection, database_name):
                    return DatabaseOperationResult(
                        success=False,
                        message=f"Database '{database_name}' does not exist",
                        duration_seconds=time.time() - start_time,
                        errors=[f"Database '{database_name}' not found"]
                    )

                backup_info = None
                warnings = []

                # Create backup if requested
                if backup_first:
                    try:
                        # Import backup operation
                        from ..backup.create import CreateBackupOperation

                        backup_op = CreateBackupOperation(
                            self.connection_pool,
                            self.rbac_manager,
                            self.audit_logger
                        )

                        if not backup_path:
                            from datetime import datetime
                            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                            backup_path = f"/tmp/{database_name}_backup_{timestamp}.dump"

                        backup_result = await backup_op.create_database_backup(
                            database_name=database_name,
                            output_path=backup_path,
                            role_name=role_name,
                            description=f"Backup before dropping database {database_name}"
                        )

                        if backup_result.success:
                            backup_info = backup_result.backup_metadata
                            logger.info(f"Created backup at: {backup_path}")
                        else:
                            warnings.append(f"Backup failed: {backup_result.message}")

                    except Exception as e:
                        warnings.append(f"Backup failed: {e}")
                        logger.warning(f"Failed to create backup: {e}")

                # Drop the database
                drop_result = await self.drop_database(
                    database_name=database_name,
                    role_name=role_name,
                    force=True  # Force to handle any remaining connections
                )

                if not drop_result.success:
                    return drop_result

                duration = time.time() - start_time

                message = f"Database '{database_name}' dropped successfully"
                if backup_info:
                    message += f" (backup saved to: {backup_path})"

                await self.audit_logger.audit_log(
                    operation="drop_database_cascade",
                    role=role_name,
                    details={
                        "database_name": database_name,
                        "backup_created": backup_info is not None,
                        "backup_path": backup_path if backup_info else None,
                        "duration_seconds": duration
                    }
                )

                return DatabaseOperationResult(
                    success=True,
                    message=message,
                    database_info=drop_result.database_info,
                    duration_seconds=duration,
                    warnings=warnings
                )

        except Exception as e:
            logger.error(f"Error dropping database with cascade: {e}")
            return DatabaseOperationResult(
                success=False,
                message=f"Failed to drop database with cascade: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def execute(self, operation_type: str, **kwargs) -> DatabaseOperationResult:
        """Execute the specified database drop operation."""
        if operation_type == "drop":
            return await self.drop_database(**kwargs)
        elif operation_type == "drop_multiple":
            return await self.drop_multiple_databases(**kwargs)
        elif operation_type == "drop_cascade":
            return await self.drop_database_cascade(**kwargs)
        else:
            raise ValueError(f"Unknown database drop operation: {operation_type}")