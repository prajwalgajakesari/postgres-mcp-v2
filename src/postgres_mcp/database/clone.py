"""Database cloning operations for PostgreSQL."""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional

from .base import DatabaseOperation, DatabaseOperationResult
from .create import CreateDatabaseOperation
from ..permissions.rbac import Permission

logger = logging.getLogger(__name__)


class CloneDatabaseOperation(DatabaseOperation):
    """Handles database cloning operations."""

    async def clone_database(self,
                           source_database: str,
                           target_database: str,
                           role_name: str = "default",
                           include_data: bool = True,
                           include_schema: bool = True,
                           include_indexes: bool = True,
                           include_constraints: bool = True,
                           include_triggers: bool = True,
                           include_functions: bool = True,
                           include_permissions: bool = False,
                           owner: Optional[str] = None,
                           tablespace: Optional[str] = None) -> DatabaseOperationResult:
        """Clone a database to create an exact copy.

        Args:
            source_database: Name of the source database
            target_database: Name of the target database
            role_name: Role for permission checking
            include_data: Whether to copy data
            include_schema: Whether to copy schema structures
            include_indexes: Whether to copy indexes
            include_constraints: Whether to copy constraints
            include_triggers: Whether to copy triggers
            include_functions: Whether to copy functions
            include_permissions: Whether to copy permissions
            owner: Owner of the new database
            tablespace: Tablespace for the new database

        Returns:
            DatabaseOperationResult with cloning details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.CREATE_DATABASE, Permission.CLONE_DATABASE])
            self._validate_identifier(source_database)
            self._validate_identifier(target_database)

            async with self.connection_pool.acquire() as connection:
                # Check if source database exists
                if not await self._database_exists(connection, source_database):
                    return DatabaseOperationResult(
                        success=False,
                        message=f"Source database '{source_database}' does not exist",
                        duration_seconds=time.time() - start_time,
                        errors=[f"Source database '{source_database}' not found"]
                    )

                # Check if target database already exists
                if await self._database_exists(connection, target_database):
                    return DatabaseOperationResult(
                        success=False,
                        message=f"Target database '{target_database}' already exists",
                        duration_seconds=time.time() - start_time,
                        errors=[f"Target database '{target_database}' exists"]
                    )

                # Get source database information
                source_info = await self._get_database_info(connection, source_database)
                if not source_info:
                    return DatabaseOperationResult(
                        success=False,
                        message=f"Could not retrieve information for source database '{source_database}'",
                        duration_seconds=time.time() - start_time,
                        errors=["Failed to get source database info"]
                    )

                logger.info(f"Starting clone of database '{source_database}' to '{target_database}'")

                # Method 1: Simple template-based clone (fastest, but limited)
                if (include_data and include_schema and include_indexes and
                    include_constraints and include_triggers and include_functions):

                    result = await self._clone_using_template(
                        source_database, target_database, source_info, owner, tablespace, role_name
                    )

                    if result.success:
                        return result

                # Method 2: pg_dump/pg_restore based clone (more flexible)
                result = await self._clone_using_dump_restore(
                    source_database=source_database,
                    target_database=target_database,
                    role_name=role_name,
                    include_data=include_data,
                    include_schema=include_schema,
                    include_indexes=include_indexes,
                    include_constraints=include_constraints,
                    include_triggers=include_triggers,
                    include_functions=include_functions,
                    include_permissions=include_permissions,
                    owner=owner,
                    tablespace=tablespace,
                    start_time=start_time
                )

                return result

        except Exception as e:
            logger.error(f"Error cloning database: {e}")
            await self.audit_logger.audit_log(
                operation="clone_database",
                role=role_name,
                details={
                    "source_database": source_database,
                    "target_database": target_database,
                    "error": str(e)
                }
            )
            return DatabaseOperationResult(
                success=False,
                message=f"Failed to clone database: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def clone_database_structure(self,
                                     source_database: str,
                                     target_database: str,
                                     role_name: str = "default",
                                     include_indexes: bool = True,
                                     include_constraints: bool = True,
                                     include_triggers: bool = True,
                                     include_functions: bool = True,
                                     owner: Optional[str] = None) -> DatabaseOperationResult:
        """Clone database structure without data.

        Args:
            source_database: Name of the source database
            target_database: Name of the target database
            role_name: Role for permission checking
            include_indexes: Whether to copy indexes
            include_constraints: Whether to copy constraints
            include_triggers: Whether to copy triggers
            include_functions: Whether to copy functions
            owner: Owner of the new database

        Returns:
            DatabaseOperationResult with cloning details
        """
        return await self.clone_database(
            source_database=source_database,
            target_database=target_database,
            role_name=role_name,
            include_data=False,
            include_schema=True,
            include_indexes=include_indexes,
            include_constraints=include_constraints,
            include_triggers=include_triggers,
            include_functions=include_functions,
            include_permissions=False,
            owner=owner
        )

    async def clone_database_subset(self,
                                  source_database: str,
                                  target_database: str,
                                  role_name: str = "default",
                                  include_schemas: Optional[List[str]] = None,
                                  exclude_schemas: Optional[List[str]] = None,
                                  include_tables: Optional[List[str]] = None,
                                  exclude_tables: Optional[List[str]] = None,
                                  where_conditions: Optional[Dict[str, str]] = None,
                                  owner: Optional[str] = None) -> DatabaseOperationResult:
        """Clone a subset of a database.

        Args:
            source_database: Name of the source database
            target_database: Name of the target database
            role_name: Role for permission checking
            include_schemas: List of schemas to include
            exclude_schemas: List of schemas to exclude
            include_tables: List of tables to include
            exclude_tables: List of tables to exclude
            where_conditions: WHERE conditions for specific tables
            owner: Owner of the new database

        Returns:
            DatabaseOperationResult with cloning details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.CREATE_DATABASE, Permission.CLONE_DATABASE])

            # Use pg_dump with filters for subset cloning
            result = await self._clone_subset_using_dump(
                source_database=source_database,
                target_database=target_database,
                role_name=role_name,
                include_schemas=include_schemas,
                exclude_schemas=exclude_schemas,
                include_tables=include_tables,
                exclude_tables=exclude_tables,
                where_conditions=where_conditions,
                owner=owner,
                start_time=start_time
            )

            return result

        except Exception as e:
            logger.error(f"Error cloning database subset: {e}")
            return DatabaseOperationResult(
                success=False,
                message=f"Failed to clone database subset: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def _clone_using_template(self,
                                  source_database: str,
                                  target_database: str,
                                  source_info,
                                  owner: Optional[str],
                                  tablespace: Optional[str],
                                  role_name: str) -> DatabaseOperationResult:
        """Clone database using PostgreSQL template mechanism."""
        try:
            logger.info(f"Attempting template-based clone from '{source_database}' to '{target_database}'")

            # Ensure no active connections to source database
            async with self.connection_pool.acquire() as connection:
                activity = await self._check_database_activity(connection, source_database)
                if activity.get('active_connections', 0) > 0:
                    logger.warning(f"Source database has active connections, cannot use template clone")
                    raise Exception("Active connections prevent template clone")

                # Create database using source as template
                create_op = CreateDatabaseOperation(self.connection_pool, self.rbac_manager, self.audit_logger)

                result = await create_op.create_database(
                    database_name=target_database,
                    role_name=role_name,
                    owner=owner,
                    template=source_database,
                    encoding=source_info.encoding,
                    collate=source_info.collate,
                    ctype=source_info.ctype,
                    tablespace=tablespace
                )

                if result.success:
                    logger.info(f"Successfully cloned database using template method")

                return result

        except Exception as e:
            logger.warning(f"Template-based clone failed: {e}")
            raise e

    async def _clone_using_dump_restore(self,
                                      source_database: str,
                                      target_database: str,
                                      role_name: str,
                                      include_data: bool,
                                      include_schema: bool,
                                      include_indexes: bool,
                                      include_constraints: bool,
                                      include_triggers: bool,
                                      include_functions: bool,
                                      include_permissions: bool,
                                      owner: Optional[str],
                                      tablespace: Optional[str],
                                      start_time: float) -> DatabaseOperationResult:
        """Clone database using pg_dump and pg_restore."""
        try:
            logger.info(f"Using pg_dump/pg_restore method for cloning")

            # Import backup operations
            from ..backup.create import CreateBackupOperation
            from ..backup.restore import RestoreBackupOperation
            from ..backup.base import BackupFormat

            # Create temporary backup
            backup_op = CreateBackupOperation(self.connection_pool, self.rbac_manager, self.audit_logger)
            restore_op = RestoreBackupOperation(self.connection_pool, self.rbac_manager, self.audit_logger)

            # Generate temporary backup path
            import tempfile
            import os
            temp_backup = os.path.join(tempfile.gettempdir(), f"{source_database}_clone_temp.dump")

            try:
                # Create backup of source database
                backup_result = await backup_op.create_database_backup(
                    database_name=source_database,
                    output_path=temp_backup,
                    role_name=role_name,
                    backup_format=BackupFormat.CUSTOM,
                    schema_only=not include_data,
                    data_only=not include_schema,
                    description=f"Temporary backup for cloning to {target_database}"
                )

                if not backup_result.success:
                    return DatabaseOperationResult(
                        success=False,
                        message=f"Failed to create backup for cloning: {backup_result.message}",
                        duration_seconds=time.time() - start_time,
                        errors=backup_result.errors
                    )

                # Create target database first
                create_op = CreateDatabaseOperation(self.connection_pool, self.rbac_manager, self.audit_logger)

                create_result = await create_op.create_database(
                    database_name=target_database,
                    role_name=role_name,
                    owner=owner,
                    tablespace=tablespace
                )

                if not create_result.success:
                    return create_result

                # Restore backup to target database
                restore_result = await restore_op.restore_backup(
                    backup_path=temp_backup,
                    target_database=target_database,
                    role_name=role_name,
                    clean_before_restore=False,
                    create_database=False,  # Already created
                    no_owner=owner is None,
                    no_privileges=not include_permissions,
                    single_transaction=True
                )

                if not restore_result.success:
                    # Try to clean up the created database
                    try:
                        from .drop import DropDatabaseOperation
                        drop_op = DropDatabaseOperation(self.connection_pool, self.rbac_manager, self.audit_logger)
                        await drop_op.drop_database(target_database, role_name, force=True, if_exists=True)
                    except Exception:
                        pass  # Ignore cleanup errors

                    return DatabaseOperationResult(
                        success=False,
                        message=f"Failed to restore backup for cloning: {restore_result.message}",
                        duration_seconds=time.time() - start_time,
                        errors=restore_result.errors
                    )

                # Get final database info
                async with self.connection_pool.acquire() as connection:
                    target_info = await self._get_database_info(connection, target_database)

                duration = time.time() - start_time

                await self.audit_logger.audit_log(
                    operation="clone_database_dump_restore",
                    role=role_name,
                    details={
                        "source_database": source_database,
                        "target_database": target_database,
                        "include_data": include_data,
                        "include_schema": include_schema,
                        "duration_seconds": duration
                    }
                )

                return DatabaseOperationResult(
                    success=True,
                    message=f"Database '{source_database}' successfully cloned to '{target_database}'",
                    database_info=target_info,
                    duration_seconds=duration,
                    warnings=restore_result.warnings
                )

            finally:
                # Clean up temporary backup file
                try:
                    if os.path.exists(temp_backup):
                        os.remove(temp_backup)
                        # Also clean up metadata file
                        metadata_file = f"{temp_backup}.metadata.json"
                        if os.path.exists(metadata_file):
                            os.remove(metadata_file)
                except Exception as e:
                    logger.warning(f"Failed to clean up temporary backup: {e}")

        except Exception as e:
            logger.error(f"pg_dump/pg_restore clone failed: {e}")
            raise e

    async def _clone_subset_using_dump(self,
                                     source_database: str,
                                     target_database: str,
                                     role_name: str,
                                     include_schemas: Optional[List[str]],
                                     exclude_schemas: Optional[List[str]],
                                     include_tables: Optional[List[str]],
                                     exclude_tables: Optional[List[str]],
                                     where_conditions: Optional[Dict[str, str]],
                                     owner: Optional[str],
                                     start_time: float) -> DatabaseOperationResult:
        """Clone database subset using filtered pg_dump."""
        try:
            # This would require implementing filtered pg_dump operations
            # For now, we'll use a simplified approach

            logger.info(f"Cloning database subset from '{source_database}' to '{target_database}'")

            # Import necessary operations
            from ..backup.create import CreateBackupOperation
            from ..backup.restore import RestoreBackupOperation
            from ..backup.base import BackupFormat
            from .create import CreateDatabaseOperation

            # Create backup with filters
            backup_op = CreateBackupOperation(self.connection_pool, self.rbac_manager, self.audit_logger)
            restore_op = RestoreBackupOperation(self.connection_pool, self.rbac_manager, self.audit_logger)
            create_op = CreateDatabaseOperation(self.connection_pool, self.rbac_manager, self.audit_logger)

            # Generate temporary backup path
            import tempfile
            import os
            temp_backup = os.path.join(tempfile.gettempdir(), f"{source_database}_subset_clone.dump")

            try:
                # Create filtered backup
                backup_result = await backup_op.create_database_backup(
                    database_name=source_database,
                    output_path=temp_backup,
                    role_name=role_name,
                    backup_format=BackupFormat.CUSTOM,
                    include_schemas=include_schemas,
                    exclude_schemas=exclude_schemas,
                    include_tables=include_tables,
                    exclude_tables=exclude_tables,
                    description=f"Subset backup for cloning to {target_database}"
                )

                if not backup_result.success:
                    return DatabaseOperationResult(
                        success=False,
                        message=f"Failed to create subset backup: {backup_result.message}",
                        duration_seconds=time.time() - start_time,
                        errors=backup_result.errors
                    )

                # Create target database
                create_result = await create_op.create_database(
                    database_name=target_database,
                    role_name=role_name,
                    owner=owner
                )

                if not create_result.success:
                    return create_result

                # Restore subset to target
                restore_result = await restore_op.restore_backup(
                    backup_path=temp_backup,
                    target_database=target_database,
                    role_name=role_name,
                    clean_before_restore=False,
                    create_database=False,
                    include_schemas=include_schemas,
                    include_tables=include_tables
                )

                if not restore_result.success:
                    # Cleanup on failure
                    try:
                        from .drop import DropDatabaseOperation
                        drop_op = DropDatabaseOperation(self.connection_pool, self.rbac_manager, self.audit_logger)
                        await drop_op.drop_database(target_database, role_name, force=True, if_exists=True)
                    except Exception:
                        pass

                    return DatabaseOperationResult(
                        success=False,
                        message=f"Failed to restore subset backup: {restore_result.message}",
                        duration_seconds=time.time() - start_time,
                        errors=restore_result.errors
                    )

                duration = time.time() - start_time

                await self.audit_logger.audit_log(
                    operation="clone_database_subset",
                    role=role_name,
                    details={
                        "source_database": source_database,
                        "target_database": target_database,
                        "include_schemas": include_schemas,
                        "exclude_schemas": exclude_schemas,
                        "include_tables": include_tables,
                        "exclude_tables": exclude_tables,
                        "duration_seconds": duration
                    }
                )

                return DatabaseOperationResult(
                    success=True,
                    message=f"Database subset successfully cloned from '{source_database}' to '{target_database}'",
                    duration_seconds=duration
                )

            finally:
                # Clean up temporary files
                try:
                    if os.path.exists(temp_backup):
                        os.remove(temp_backup)
                        metadata_file = f"{temp_backup}.metadata.json"
                        if os.path.exists(metadata_file):
                            os.remove(metadata_file)
                except Exception as e:
                    logger.warning(f"Failed to clean up temporary files: {e}")

        except Exception as e:
            logger.error(f"Subset clone failed: {e}")
            raise e

    async def execute(self, operation_type: str, **kwargs) -> DatabaseOperationResult:
        """Execute the specified database cloning operation."""
        if operation_type == "clone":
            return await self.clone_database(**kwargs)
        elif operation_type == "clone_structure":
            return await self.clone_database_structure(**kwargs)
        elif operation_type == "clone_subset":
            return await self.clone_database_subset(**kwargs)
        else:
            raise ValueError(f"Unknown database cloning operation: {operation_type}")