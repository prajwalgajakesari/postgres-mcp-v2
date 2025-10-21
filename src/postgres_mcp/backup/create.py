"""Create backup operations for PostgreSQL."""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

from .base import BackupOperation, BackupOperationResult, BackupMetadata, BackupFormat, BackupScope
from ..permissions.rbac import Permission

logger = logging.getLogger(__name__)


class CreateBackupOperation(BackupOperation):
    """Handles creation of database backups using pg_dump."""

    async def create_database_backup(self,
                                   database_name: str,
                                   output_path: str,
                                   role_name: str = "default",
                                   backup_format: BackupFormat = BackupFormat.CUSTOM,
                                   compression_level: int = 6,
                                   include_schemas: Optional[List[str]] = None,
                                   exclude_schemas: Optional[List[str]] = None,
                                   include_tables: Optional[List[str]] = None,
                                   exclude_tables: Optional[List[str]] = None,
                                   schema_only: bool = False,
                                   data_only: bool = False,
                                   no_owner: bool = True,
                                   no_privileges: bool = False,
                                   description: Optional[str] = None) -> BackupOperationResult:
        """Create a database backup using pg_dump.

        Args:
            database_name: Name of database to backup
            output_path: Output file/directory path
            role_name: Role for permission checking
            backup_format: Backup format (custom, tar, plain, directory)
            compression_level: Compression level (0-9, only for custom/tar)
            include_schemas: List of schemas to include
            exclude_schemas: List of schemas to exclude
            include_tables: List of tables to include
            exclude_tables: List of tables to exclude
            schema_only: Include only schema, no data
            data_only: Include only data, no schema
            no_owner: Skip ownership information
            no_privileges: Skip privilege information
            description: Optional backup description

        Returns:
            BackupOperationResult with backup details
        """
        start_time = time.time()
        backup_id = self._generate_backup_id()

        try:
            self._validate_permissions(role_name, [Permission.BACKUP, Permission.CREATE_BACKUP])
            self._validate_identifier(database_name)
            self._validate_file_path(output_path, operation="write")

            # Check if pg_dump is available
            if not await self._check_pg_tools("pg_dump"):
                raise RuntimeError("pg_dump command not found. Please install PostgreSQL client tools.")

            # Get database connection info
            async with self.connection_pool.acquire() as connection:
                pg_version = await self._get_database_version(connection)
                current_db = await self._get_database_name(connection)

                # Build database URL from connection
                db_url = self._build_database_url(database_name)

                # Prepare pg_dump options
                options = {
                    "verbose": True,
                    "compress_level": compression_level if backup_format in [BackupFormat.CUSTOM, BackupFormat.TAR] else None,
                    "schema_only": schema_only,
                    "data_only": data_only,
                    "no_owner": no_owner,
                    "no_privileges": no_privileges,
                    "include_schemas": include_schemas or [],
                    "exclude_schemas": exclude_schemas or [],
                    "include_tables": include_tables or [],
                    "exclude_tables": exclude_tables or []
                }

                # Handle directory format
                if backup_format == BackupFormat.DIRECTORY:
                    output_dir = Path(output_path)
                    if output_dir.exists():
                        import shutil
                        shutil.rmtree(output_dir)
                    output_dir.mkdir(parents=True, exist_ok=True)

                # Build and execute pg_dump command
                cmd = self._build_pg_dump_command(db_url, output_path, backup_format, options)

                success, stdout, stderr = await self._execute_command(cmd, timeout=7200)  # 2 hour timeout

                if not success:
                    return BackupOperationResult(
                        success=False,
                        message=f"Backup failed: {stderr}",
                        duration_seconds=time.time() - start_time,
                        errors=[stderr] if stderr else []
                    )

                # Get file size
                file_size = None
                if backup_format == BackupFormat.DIRECTORY:
                    file_size = sum(f.stat().st_size for f in Path(output_path).rglob('*') if f.is_file())
                else:
                    file_size = Path(output_path).stat().st_size

                # Create metadata
                metadata = BackupMetadata(
                    backup_id=backup_id,
                    timestamp=datetime.utcnow(),
                    database_name=database_name,
                    backup_format=backup_format,
                    backup_scope=BackupScope.DATABASE,
                    file_path=output_path,
                    file_size=file_size,
                    compression=f"level-{compression_level}" if compression_level and backup_format in [BackupFormat.CUSTOM, BackupFormat.TAR] else None,
                    pg_version=pg_version,
                    description=description,
                    includes=include_schemas or include_tables,
                    excludes=exclude_schemas or exclude_tables
                )

                # Save metadata file
                metadata_path = f"{output_path}.metadata.json"
                await self._save_metadata(metadata, metadata_path)

                duration = time.time() - start_time

                await self.audit_logger.audit_log(
                    operation="create_database_backup",
                    role=role_name,
                    details={
                        "backup_id": backup_id,
                        "database_name": database_name,
                        "output_path": output_path,
                        "format": backup_format.value,
                        "file_size": file_size,
                        "duration_seconds": duration
                    }
                )

                warnings = []
                if stderr and "WARNING" in stderr.upper():
                    warnings = [line.strip() for line in stderr.split('\n') if 'WARNING' in line.upper()]

                return BackupOperationResult(
                    success=True,
                    message=f"Database backup created successfully: {output_path}",
                    backup_metadata=metadata,
                    duration_seconds=duration,
                    warnings=warnings
                )

        except Exception as e:
            logger.error(f"Error creating database backup: {e}")
            await self.audit_logger.audit_log(
                operation="create_database_backup",
                role=role_name,
                details={
                    "database_name": database_name,
                    "output_path": output_path,
                    "error": str(e)
                }
            )
            return BackupOperationResult(
                success=False,
                message=f"Failed to create backup: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def create_schema_backup(self,
                                 database_name: str,
                                 schema_name: str,
                                 output_path: str,
                                 role_name: str = "default",
                                 backup_format: BackupFormat = BackupFormat.CUSTOM,
                                 include_data: bool = True,
                                 description: Optional[str] = None) -> BackupOperationResult:
        """Create backup of a specific schema.

        Args:
            database_name: Database containing the schema
            schema_name: Schema name to backup
            output_path: Output file path
            role_name: Role for permission checking
            backup_format: Backup format
            include_data: Whether to include data
            description: Optional backup description

        Returns:
            BackupOperationResult with backup details
        """
        try:
            self._validate_identifier(schema_name)

            # Use database backup with schema filter
            return await self.create_database_backup(
                database_name=database_name,
                output_path=output_path,
                role_name=role_name,
                backup_format=backup_format,
                include_schemas=[schema_name],
                data_only=not include_data,
                description=description or f"Schema backup: {schema_name}"
            )

        except Exception as e:
            logger.error(f"Error creating schema backup: {e}")
            return BackupOperationResult(
                success=False,
                message=f"Failed to create schema backup: {e}",
                errors=[str(e)]
            )

    async def create_table_backup(self,
                                database_name: str,
                                table_name: str,
                                output_path: str,
                                role_name: str = "default",
                                schema_name: str = "public",
                                backup_format: BackupFormat = BackupFormat.CUSTOM,
                                include_schema: bool = True,
                                description: Optional[str] = None) -> BackupOperationResult:
        """Create backup of a specific table.

        Args:
            database_name: Database containing the table
            table_name: Table name to backup
            output_path: Output file path
            role_name: Role for permission checking
            schema_name: Schema containing the table
            backup_format: Backup format
            include_schema: Whether to include table schema
            description: Optional backup description

        Returns:
            BackupOperationResult with backup details
        """
        try:
            self._validate_identifier(table_name)
            self._validate_identifier(schema_name)

            full_table_name = f"{schema_name}.{table_name}"

            # Use database backup with table filter
            return await self.create_database_backup(
                database_name=database_name,
                output_path=output_path,
                role_name=role_name,
                backup_format=backup_format,
                include_tables=[full_table_name],
                data_only=not include_schema,
                description=description or f"Table backup: {full_table_name}"
            )

        except Exception as e:
            logger.error(f"Error creating table backup: {e}")
            return BackupOperationResult(
                success=False,
                message=f"Failed to create table backup: {e}",
                errors=[str(e)]
            )

    async def create_globals_backup(self,
                                  output_path: str,
                                  role_name: str = "default",
                                  description: Optional[str] = None) -> BackupOperationResult:
        """Create backup of global objects (roles, tablespaces, etc.) using pg_dumpall.

        Args:
            output_path: Output file path
            role_name: Role for permission checking
            description: Optional backup description

        Returns:
            BackupOperationResult with backup details
        """
        start_time = time.time()
        backup_id = self._generate_backup_id()

        try:
            self._validate_permissions(role_name, [Permission.BACKUP, Permission.CREATE_BACKUP])
            self._validate_file_path(output_path, operation="write")

            # Check if pg_dumpall is available
            if not await self._check_pg_tools("pg_dumpall"):
                raise RuntimeError("pg_dumpall command not found. Please install PostgreSQL client tools.")

            # Get database connection info
            async with self.connection_pool.acquire() as connection:
                pg_version = await self._get_database_version(connection)

                # Build database URL
                db_url = self._build_database_url()

                # Build pg_dumpall command for globals only
                cmd = [
                    "pg_dumpall",
                    "--globals-only",
                    "--verbose",
                    "--file", output_path,
                    "--host", self._get_db_host(),
                    "--port", str(self._get_db_port()),
                    "--username", self._get_db_user()
                ]

                success, stdout, stderr = await self._execute_command(cmd, timeout=1800)  # 30 minute timeout

                if not success:
                    return BackupOperationResult(
                        success=False,
                        message=f"Globals backup failed: {stderr}",
                        duration_seconds=time.time() - start_time,
                        errors=[stderr] if stderr else []
                    )

                # Get file size
                file_size = Path(output_path).stat().st_size

                # Create metadata
                metadata = BackupMetadata(
                    backup_id=backup_id,
                    timestamp=datetime.utcnow(),
                    database_name="*globals*",
                    backup_format=BackupFormat.PLAIN,
                    backup_scope=BackupScope.GLOBALS,
                    file_path=output_path,
                    file_size=file_size,
                    pg_version=pg_version,
                    description=description or "Global objects backup"
                )

                # Save metadata file
                metadata_path = f"{output_path}.metadata.json"
                await self._save_metadata(metadata, metadata_path)

                duration = time.time() - start_time

                await self.audit_logger.audit_log(
                    operation="create_globals_backup",
                    role=role_name,
                    details={
                        "backup_id": backup_id,
                        "output_path": output_path,
                        "file_size": file_size,
                        "duration_seconds": duration
                    }
                )

                return BackupOperationResult(
                    success=True,
                    message=f"Globals backup created successfully: {output_path}",
                    backup_metadata=metadata,
                    duration_seconds=duration
                )

        except Exception as e:
            logger.error(f"Error creating globals backup: {e}")
            await self.audit_logger.audit_log(
                operation="create_globals_backup",
                role=role_name,
                details={
                    "output_path": output_path,
                    "error": str(e)
                }
            )
            return BackupOperationResult(
                success=False,
                message=f"Failed to create globals backup: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    def _build_database_url(self, database_name: Optional[str] = None) -> str:
        """Build database connection URL from pool configuration."""
        # This is a simplified version - in a real implementation,
        # you would extract connection details from the connection pool
        pool_config = getattr(self.connection_pool, '_config', {})

        host = pool_config.get('host', 'localhost')
        port = pool_config.get('port', 5432)
        user = pool_config.get('user', 'postgres')
        password = pool_config.get('password', '')
        db_name = database_name or pool_config.get('database', 'postgres')

        if password:
            return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        else:
            return f"postgresql://{user}@{host}:{port}/{db_name}"

    def _get_db_host(self) -> str:
        """Get database host from pool configuration."""
        return getattr(self.connection_pool, '_config', {}).get('host', 'localhost')

    def _get_db_port(self) -> int:
        """Get database port from pool configuration."""
        return getattr(self.connection_pool, '_config', {}).get('port', 5432)

    def _get_db_user(self) -> str:
        """Get database user from pool configuration."""
        return getattr(self.connection_pool, '_config', {}).get('user', 'postgres')

    async def _save_metadata(self, metadata: BackupMetadata, metadata_path: str) -> None:
        """Save backup metadata to JSON file."""
        try:
            import json
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata.to_dict(), f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save backup metadata: {e}")

    async def execute(self, operation_type: str, **kwargs) -> BackupOperationResult:
        """Execute the specified backup operation."""
        if operation_type == "database":
            return await self.create_database_backup(**kwargs)
        elif operation_type == "schema":
            return await self.create_schema_backup(**kwargs)
        elif operation_type == "table":
            return await self.create_table_backup(**kwargs)
        elif operation_type == "globals":
            return await self.create_globals_backup(**kwargs)
        else:
            raise ValueError(f"Unknown backup operation type: {operation_type}")