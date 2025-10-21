"""Restore backup operations for PostgreSQL."""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

from .base import BackupOperation, BackupOperationResult, BackupMetadata, BackupFormat
from ..permissions.rbac import Permission

logger = logging.getLogger(__name__)


class RestoreBackupOperation(BackupOperation):
    """Handles restoration of database backups using pg_restore."""

    async def restore_backup(self,
                           backup_path: str,
                           target_database: str,
                           role_name: str = "default",
                           clean_before_restore: bool = False,
                           create_database: bool = False,
                           no_owner: bool = True,
                           no_privileges: bool = False,
                           single_transaction: bool = True,
                           exit_on_error: bool = True,
                           parallel_jobs: int = 1,
                           include_schemas: Optional[List[str]] = None,
                           include_tables: Optional[List[str]] = None,
                           if_exists: bool = False) -> BackupOperationResult:
        """Restore a database backup using pg_restore.

        Args:
            backup_path: Path to backup file/directory
            target_database: Target database name
            role_name: Role for permission checking
            clean_before_restore: Drop database objects before recreating
            create_database: Create target database if it doesn't exist
            no_owner: Skip ownership information
            no_privileges: Skip privilege information
            single_transaction: Perform restore in single transaction
            exit_on_error: Stop on first error
            parallel_jobs: Number of parallel jobs (for directory format)
            include_schemas: List of schemas to restore (optional)
            include_tables: List of tables to restore (optional)
            if_exists: Use IF EXISTS when dropping objects

        Returns:
            BackupOperationResult with restore details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.RESTORE_BACKUP, Permission.CREATE_DATABASE if create_database else Permission.INSERT])
            self._validate_identifier(target_database)
            self._validate_file_path(backup_path, operation="read")

            # Load backup metadata if available
            metadata = await self._load_metadata(backup_path)

            # Check if pg_restore is available
            if not await self._check_pg_tools("pg_restore"):
                raise RuntimeError("pg_restore command not found. Please install PostgreSQL client tools.")

            # Determine backup format
            backup_format = await self._detect_backup_format(backup_path, metadata)

            # Handle plain SQL format differently
            if backup_format == BackupFormat.PLAIN:
                return await self._restore_plain_backup(
                    backup_path, target_database, role_name, start_time
                )

            # Get database connection info
            async with self.connection_pool.acquire() as connection:
                # Create target database if requested
                if create_database:
                    await self._create_target_database(connection, target_database)

                # Build database URL
                db_url = self._build_database_url(target_database)

                # Prepare pg_restore options
                options = {
                    "verbose": True,
                    "clean": clean_before_restore,
                    "create": create_database,
                    "if_exists": if_exists,
                    "no_owner": no_owner,
                    "no_privileges": no_privileges,
                    "single_transaction": single_transaction,
                    "exit_on_error": exit_on_error,
                    "jobs": parallel_jobs if parallel_jobs > 1 and backup_format == BackupFormat.DIRECTORY else None,
                    "include_schemas": include_schemas or [],
                    "include_tables": include_tables or []
                }

                # Build and execute pg_restore command
                cmd = self._build_pg_restore_command(backup_path, db_url, options)

                success, stdout, stderr = await self._execute_command(cmd, timeout=7200)  # 2 hour timeout

                duration = time.time() - start_time

                if not success:
                    await self.audit_logger.audit_log(
                        operation="restore_backup",
                        role=role_name,
                        details={
                            "backup_path": backup_path,
                            "target_database": target_database,
                            "error": stderr,
                            "duration_seconds": duration
                        }
                    )

                    return BackupOperationResult(
                        success=False,
                        message=f"Restore failed: {stderr}",
                        duration_seconds=duration,
                        errors=[stderr] if stderr else []
                    )

                await self.audit_logger.audit_log(
                    operation="restore_backup",
                    role=role_name,
                    details={
                        "backup_path": backup_path,
                        "target_database": target_database,
                        "backup_format": backup_format.value,
                        "duration_seconds": duration,
                        "parallel_jobs": parallel_jobs
                    }
                )

                warnings = []
                if stderr and "WARNING" in stderr.upper():
                    warnings = [line.strip() for line in stderr.split('\n') if 'WARNING' in line.upper()]

                return BackupOperationResult(
                    success=True,
                    message=f"Backup restored successfully to database: {target_database}",
                    backup_metadata=metadata,
                    duration_seconds=duration,
                    warnings=warnings
                )

        except Exception as e:
            logger.error(f"Error restoring backup: {e}")
            await self.audit_logger.audit_log(
                operation="restore_backup",
                role=role_name,
                details={
                    "backup_path": backup_path,
                    "target_database": target_database,
                    "error": str(e)
                }
            )
            return BackupOperationResult(
                success=False,
                message=f"Failed to restore backup: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def restore_globals(self,
                            backup_path: str,
                            role_name: str = "default",
                            if_exists: bool = True) -> BackupOperationResult:
        """Restore global objects (roles, tablespaces) from backup.

        Args:
            backup_path: Path to globals backup file
            role_name: Role for permission checking
            if_exists: Use IF EXISTS when dropping objects

        Returns:
            BackupOperationResult with restore details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.RESTORE_BACKUP, Permission.CREATE_USER, Permission.ALTER_SYSTEM])
            self._validate_file_path(backup_path, operation="read")

            # Check if psql is available
            if not await self._check_pg_tools("psql"):
                raise RuntimeError("psql command not found. Please install PostgreSQL client tools.")

            # Build database URL for template1 (for global operations)
            db_url = self._build_database_url("template1")

            # Build psql command to execute globals backup
            cmd = [
                "psql",
                "-v", "ON_ERROR_STOP=1" if not if_exists else "ON_ERROR_STOP=0",
                "-f", backup_path,
                db_url
            ]

            success, stdout, stderr = await self._execute_command(cmd, timeout=1800)  # 30 minute timeout

            duration = time.time() - start_time

            if not success:
                await self.audit_logger.audit_log(
                    operation="restore_globals",
                    role=role_name,
                    details={
                        "backup_path": backup_path,
                        "error": stderr,
                        "duration_seconds": duration
                    }
                )

                return BackupOperationResult(
                    success=False,
                    message=f"Globals restore failed: {stderr}",
                    duration_seconds=duration,
                    errors=[stderr] if stderr else []
                )

            await self.audit_logger.audit_log(
                operation="restore_globals",
                role=role_name,
                details={
                    "backup_path": backup_path,
                    "duration_seconds": duration
                }
            )

            warnings = []
            if stderr and "WARNING" in stderr.upper():
                warnings = [line.strip() for line in stderr.split('\n') if 'WARNING' in line.upper()]

            return BackupOperationResult(
                success=True,
                message=f"Global objects restored successfully from: {backup_path}",
                duration_seconds=duration,
                warnings=warnings
            )

        except Exception as e:
            logger.error(f"Error restoring globals: {e}")
            await self.audit_logger.audit_log(
                operation="restore_globals",
                role=role_name,
                details={
                    "backup_path": backup_path,
                    "error": str(e)
                }
            )
            return BackupOperationResult(
                success=False,
                message=f"Failed to restore globals: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def restore_point_in_time(self,
                                  target_database: str,
                                  base_backup_path: str,
                                  wal_archive_path: str,
                                  recovery_target_time: datetime,
                                  role_name: str = "default") -> BackupOperationResult:
        """Restore database to a specific point in time (PITR).

        Note: This is a simplified implementation. Full PITR requires proper
        WAL archiving setup and recovery.conf configuration.

        Args:
            target_database: Target database name
            base_backup_path: Path to base backup
            wal_archive_path: Path to WAL archive directory
            recovery_target_time: Target recovery time
            role_name: Role for permission checking

        Returns:
            BackupOperationResult with restore details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.RESTORE_BACKUP, Permission.ALTER_SYSTEM])
            self._validate_identifier(target_database)

            # This is a placeholder implementation
            # Real PITR would require:
            # 1. Stop PostgreSQL
            # 2. Replace data directory with base backup
            # 3. Create recovery.conf with target time
            # 4. Start PostgreSQL in recovery mode
            # 5. Wait for recovery to complete

            logger.warning("Point-in-time recovery is not fully implemented in this version")

            return BackupOperationResult(
                success=False,
                message="Point-in-time recovery requires manual setup and is not fully automated in this implementation",
                duration_seconds=time.time() - start_time,
                errors=["PITR not fully implemented"]
            )

        except Exception as e:
            logger.error(f"Error in point-in-time recovery: {e}")
            return BackupOperationResult(
                success=False,
                message=f"Failed point-in-time recovery: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def _restore_plain_backup(self,
                                  backup_path: str,
                                  target_database: str,
                                  role_name: str,
                                  start_time: float) -> BackupOperationResult:
        """Restore plain SQL backup using psql."""
        try:
            # Check if psql is available
            if not await self._check_pg_tools("psql"):
                raise RuntimeError("psql command not found. Please install PostgreSQL client tools.")

            # Build database URL
            db_url = self._build_database_url(target_database)

            # Build psql command
            cmd = [
                "psql",
                "-v", "ON_ERROR_STOP=1",
                "-f", backup_path,
                db_url
            ]

            success, stdout, stderr = await self._execute_command(cmd, timeout=7200)

            duration = time.time() - start_time

            if not success:
                return BackupOperationResult(
                    success=False,
                    message=f"Plain backup restore failed: {stderr}",
                    duration_seconds=duration,
                    errors=[stderr] if stderr else []
                )

            return BackupOperationResult(
                success=True,
                message=f"Plain backup restored successfully to database: {target_database}",
                duration_seconds=duration
            )

        except Exception as e:
            return BackupOperationResult(
                success=False,
                message=f"Failed to restore plain backup: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def _detect_backup_format(self, backup_path: str, metadata: Optional[BackupMetadata]) -> BackupFormat:
        """Detect backup format from file or metadata."""
        if metadata:
            return metadata.backup_format

        path = Path(backup_path)

        if path.is_dir():
            return BackupFormat.DIRECTORY

        # Try to detect from file content/extension
        if path.suffix == '.sql':
            return BackupFormat.PLAIN

        # For binary formats, we'd need to read the file header
        # This is a simplified detection
        try:
            with open(backup_path, 'rb') as f:
                header = f.read(5)
                if header.startswith(b'PGDMP'):
                    # Check for custom format magic number
                    return BackupFormat.CUSTOM
        except Exception:
            pass

        # Default assumption
        return BackupFormat.CUSTOM

    async def _load_metadata(self, backup_path: str) -> Optional[BackupMetadata]:
        """Load backup metadata from .metadata.json file."""
        try:
            metadata_path = f"{backup_path}.metadata.json"
            if Path(metadata_path).exists():
                import json
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return BackupMetadata.from_dict(data)
        except Exception as e:
            logger.warning(f"Failed to load backup metadata: {e}")

        return None

    async def _create_target_database(self, connection, database_name: str) -> None:
        """Create target database if it doesn't exist."""
        try:
            # Check if database exists
            exists_query = "SELECT 1 FROM pg_database WHERE datname = $1"
            exists = await connection.fetchval(exists_query, database_name)

            if not exists:
                # Create database (cannot be done in a transaction)
                await connection.execute(f'CREATE DATABASE {self._quote_identifier(database_name)}')
                logger.info(f"Created target database: {database_name}")

        except Exception as e:
            logger.warning(f"Failed to create target database: {e}")
            # Continue anyway - might already exist or be created by backup

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

    async def execute(self, operation_type: str, **kwargs) -> BackupOperationResult:
        """Execute the specified restore operation."""
        if operation_type == "backup":
            return await self.restore_backup(**kwargs)
        elif operation_type == "globals":
            return await self.restore_globals(**kwargs)
        elif operation_type == "point_in_time":
            return await self.restore_point_in_time(**kwargs)
        else:
            raise ValueError(f"Unknown restore operation type: {operation_type}")