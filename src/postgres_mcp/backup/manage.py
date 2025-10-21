"""Backup management operations for PostgreSQL."""

import asyncio
import json
import logging
import shutil
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from datetime import datetime, timedelta

from .base import BackupOperation, BackupOperationResult, BackupMetadata, BackupFormat, BackupScope
from ..permissions.rbac import Permission

logger = logging.getLogger(__name__)


class ManageBackupsOperation(BackupOperation):
    """Handles backup management operations like listing, cleaning, and validation."""

    def __init__(self, connection_pool, rbac_manager, audit_logger, backup_storage_path: str = "/var/backups/postgresql"):
        super().__init__(connection_pool, rbac_manager, audit_logger)
        self.backup_storage_path = Path(backup_storage_path)
        self.backup_storage_path.mkdir(parents=True, exist_ok=True)

    async def list_backups(self,
                          role_name: str = "default",
                          database_filter: Optional[str] = None,
                          format_filter: Optional[BackupFormat] = None,
                          scope_filter: Optional[BackupScope] = None,
                          limit: Optional[int] = None,
                          include_metadata: bool = True) -> BackupOperationResult:
        """List available backups.

        Args:
            role_name: Role for permission checking
            database_filter: Filter by database name
            format_filter: Filter by backup format
            scope_filter: Filter by backup scope
            limit: Maximum number of backups to return
            include_metadata: Whether to include full metadata

        Returns:
            BackupOperationResult with backup listing
        """
        try:
            self._validate_permissions(role_name, [Permission.MANAGE_BACKUPS])

            backups = []
            backup_files = []

            # Find all backup files and metadata files
            for file_path in self.backup_storage_path.rglob("*"):
                if file_path.is_file() and not file_path.name.endswith('.metadata.json'):
                    # Check if this looks like a backup file
                    if (file_path.suffix in ['.sql', '.dump', '.tar'] or
                        file_path.is_dir() or
                        'backup' in file_path.name.lower()):
                        backup_files.append(file_path)

            # Load metadata for each backup
            for backup_path in backup_files:
                try:
                    metadata = await self._load_backup_metadata(str(backup_path))

                    if metadata:
                        # Apply filters
                        if database_filter and metadata.database_name != database_filter:
                            continue

                        if format_filter and metadata.backup_format != format_filter:
                            continue

                        if scope_filter and metadata.backup_scope != scope_filter:
                            continue

                        backup_info = {
                            "backup_id": metadata.backup_id,
                            "timestamp": metadata.timestamp.isoformat(),
                            "database_name": metadata.database_name,
                            "format": metadata.backup_format.value,
                            "scope": metadata.backup_scope.value,
                            "file_path": metadata.file_path,
                            "file_size": metadata.file_size,
                            "description": metadata.description
                        }

                        if include_metadata:
                            backup_info["full_metadata"] = metadata.to_dict()

                        backups.append(backup_info)

                except Exception as e:
                    logger.warning(f"Failed to load metadata for backup {backup_path}: {e}")
                    # Add minimal info for backups without metadata
                    stat = backup_path.stat()
                    backups.append({
                        "backup_id": f"unknown_{backup_path.name}",
                        "timestamp": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                        "database_name": "unknown",
                        "format": "unknown",
                        "scope": "unknown",
                        "file_path": str(backup_path),
                        "file_size": stat.st_size,
                        "description": "No metadata available"
                    })

            # Sort by timestamp (newest first)
            backups.sort(key=lambda x: x["timestamp"], reverse=True)

            # Apply limit
            if limit:
                backups = backups[:limit]

            await self.audit_logger.audit_log(
                operation="list_backups",
                role=role_name,
                details={
                    "backups_found": len(backups),
                    "database_filter": database_filter,
                    "format_filter": format_filter.value if format_filter else None
                }
            )

            return BackupOperationResult(
                success=True,
                message=f"Found {len(backups)} backups",
                backup_metadata=None,
                duration_seconds=0,
                warnings=[],
                errors=[]
            )

        except Exception as e:
            logger.error(f"Error listing backups: {e}")
            return BackupOperationResult(
                success=False,
                message=f"Failed to list backups: {e}",
                errors=[str(e)]
            )

    async def validate_backup(self,
                            backup_path: str,
                            role_name: str = "default",
                            check_integrity: bool = True,
                            check_metadata: bool = True) -> BackupOperationResult:
        """Validate a backup file.

        Args:
            backup_path: Path to backup file/directory
            role_name: Role for permission checking
            check_integrity: Whether to check backup integrity
            check_metadata: Whether to validate metadata

        Returns:
            BackupOperationResult with validation results
        """
        try:
            self._validate_permissions(role_name, [Permission.MANAGE_BACKUPS])
            self._validate_file_path(backup_path, operation="read")

            path = Path(backup_path)
            validation_results = {
                "file_exists": path.exists(),
                "file_readable": path.exists() and (path.is_file() or path.is_dir()),
                "metadata_valid": False,
                "integrity_valid": None,
                "size_bytes": None,
                "format": None
            }

            warnings = []
            errors = []

            # Basic file checks
            if not validation_results["file_exists"]:
                errors.append(f"Backup file does not exist: {backup_path}")
                return BackupOperationResult(
                    success=False,
                    message="Backup validation failed: file not found",
                    errors=errors
                )

            if path.is_file():
                validation_results["size_bytes"] = path.stat().st_size
            elif path.is_dir():
                validation_results["size_bytes"] = sum(f.stat().st_size for f in path.rglob('*') if f.is_file())

            # Check metadata
            if check_metadata:
                try:
                    metadata = await self._load_backup_metadata(backup_path)
                    if metadata:
                        validation_results["metadata_valid"] = True
                        validation_results["format"] = metadata.backup_format.value
                    else:
                        warnings.append("No metadata file found")
                except Exception as e:
                    warnings.append(f"Failed to load metadata: {e}")

            # Check integrity
            if check_integrity:
                try:
                    format_type = await self._detect_backup_format(backup_path)
                    validation_results["format"] = format_type.value

                    if format_type == BackupFormat.CUSTOM:
                        # Use pg_restore to list contents
                        integrity_valid = await self._validate_custom_backup(backup_path)
                        validation_results["integrity_valid"] = integrity_valid

                    elif format_type == BackupFormat.TAR:
                        # Check tar file integrity
                        integrity_valid = await self._validate_tar_backup(backup_path)
                        validation_results["integrity_valid"] = integrity_valid

                    elif format_type == BackupFormat.PLAIN:
                        # Check if it's valid SQL
                        integrity_valid = await self._validate_plain_backup(backup_path)
                        validation_results["integrity_valid"] = integrity_valid

                    elif format_type == BackupFormat.DIRECTORY:
                        # Check directory structure
                        integrity_valid = await self._validate_directory_backup(backup_path)
                        validation_results["integrity_valid"] = integrity_valid

                except Exception as e:
                    errors.append(f"Integrity check failed: {e}")
                    validation_results["integrity_valid"] = False

            # Determine overall success
            success = (validation_results["file_readable"] and
                      (not check_metadata or validation_results["metadata_valid"] or len(warnings) > 0) and
                      (not check_integrity or validation_results["integrity_valid"]))

            message = f"Backup validation {'passed' if success else 'failed'}"
            if validation_results["size_bytes"]:
                size_mb = validation_results["size_bytes"] / (1024 * 1024)
                message += f" (size: {size_mb:.1f} MB)"

            await self.audit_logger.audit_log(
                operation="validate_backup",
                role=role_name,
                details={
                    "backup_path": backup_path,
                    "validation_results": validation_results,
                    "success": success
                }
            )

            return BackupOperationResult(
                success=success,
                message=message,
                backup_metadata=None,
                duration_seconds=0,
                warnings=warnings,
                errors=errors
            )

        except Exception as e:
            logger.error(f"Error validating backup: {e}")
            return BackupOperationResult(
                success=False,
                message=f"Failed to validate backup: {e}",
                errors=[str(e)]
            )

    async def cleanup_old_backups(self,
                                retention_days: int = 30,
                                role_name: str = "default",
                                database_filter: Optional[str] = None,
                                dry_run: bool = False) -> BackupOperationResult:
        """Clean up old backups based on retention policy.

        Args:
            retention_days: Number of days to retain backups
            role_name: Role for permission checking
            database_filter: Only clean backups for specific database
            dry_run: If True, only show what would be deleted

        Returns:
            BackupOperationResult with cleanup results
        """
        try:
            self._validate_permissions(role_name, [Permission.MANAGE_BACKUPS])

            if retention_days <= 0:
                raise ValueError("Retention days must be positive")

            cutoff_date = datetime.utcnow() - timedelta(days=retention_days)

            backups_to_delete = []
            total_size_to_delete = 0

            # Find old backups
            for file_path in self.backup_storage_path.rglob("*"):
                if file_path.is_file() and not file_path.name.endswith('.metadata.json'):
                    try:
                        metadata = await self._load_backup_metadata(str(file_path))

                        # Use metadata timestamp if available, otherwise file mtime
                        if metadata:
                            backup_date = metadata.timestamp
                            database_name = metadata.database_name
                        else:
                            backup_date = datetime.fromtimestamp(file_path.stat().st_mtime)
                            database_name = "unknown"

                        # Apply filters
                        if database_filter and database_name != database_filter:
                            continue

                        if backup_date < cutoff_date:
                            file_size = file_path.stat().st_size
                            backups_to_delete.append({
                                "path": str(file_path),
                                "database": database_name,
                                "date": backup_date.isoformat(),
                                "size": file_size
                            })
                            total_size_to_delete += file_size

                    except Exception as e:
                        logger.warning(f"Error processing backup file {file_path}: {e}")

            # Delete backups (unless dry run)
            deleted_count = 0
            errors = []

            if not dry_run:
                for backup in backups_to_delete:
                    try:
                        backup_path = Path(backup["path"])

                        # Delete backup file
                        if backup_path.is_file():
                            backup_path.unlink()
                        elif backup_path.is_dir():
                            shutil.rmtree(backup_path)

                        # Delete metadata file if exists
                        metadata_path = Path(f"{backup_path}.metadata.json")
                        if metadata_path.exists():
                            metadata_path.unlink()

                        deleted_count += 1
                        logger.info(f"Deleted old backup: {backup_path}")

                    except Exception as e:
                        error_msg = f"Failed to delete backup {backup['path']}: {e}"
                        errors.append(error_msg)
                        logger.error(error_msg)

            size_mb = total_size_to_delete / (1024 * 1024)
            action = "Would delete" if dry_run else "Deleted"
            message = f"{action} {len(backups_to_delete)} old backups ({size_mb:.1f} MB)"

            if not dry_run and errors:
                message += f", {len(errors)} failed"

            await self.audit_logger.audit_log(
                operation="cleanup_old_backups",
                role=role_name,
                details={
                    "retention_days": retention_days,
                    "backups_found": len(backups_to_delete),
                    "backups_deleted": deleted_count,
                    "total_size_mb": size_mb,
                    "dry_run": dry_run,
                    "database_filter": database_filter
                }
            )

            return BackupOperationResult(
                success=len(errors) == 0,
                message=message,
                backup_metadata=None,
                duration_seconds=0,
                warnings=[],
                errors=errors
            )

        except Exception as e:
            logger.error(f"Error cleaning up backups: {e}")
            return BackupOperationResult(
                success=False,
                message=f"Failed to cleanup backups: {e}",
                errors=[str(e)]
            )

    async def get_backup_info(self,
                            backup_path: str,
                            role_name: str = "default") -> BackupOperationResult:
        """Get detailed information about a specific backup.

        Args:
            backup_path: Path to backup file/directory
            role_name: Role for permission checking

        Returns:
            BackupOperationResult with detailed backup information
        """
        try:
            self._validate_permissions(role_name, [Permission.MANAGE_BACKUPS])
            self._validate_file_path(backup_path, operation="read")

            path = Path(backup_path)
            backup_info = {
                "path": str(path),
                "exists": path.exists(),
                "type": "directory" if path.is_dir() else "file",
                "size_bytes": None,
                "created": None,
                "modified": None,
                "format": None,
                "metadata": None,
                "contents": None
            }

            if path.exists():
                stat = path.stat()
                backup_info["created"] = datetime.fromtimestamp(stat.st_ctime).isoformat()
                backup_info["modified"] = datetime.fromtimestamp(stat.st_mtime).isoformat()

                if path.is_file():
                    backup_info["size_bytes"] = stat.st_size
                else:
                    backup_info["size_bytes"] = sum(f.stat().st_size for f in path.rglob('*') if f.is_file())

                # Load metadata
                try:
                    metadata = await self._load_backup_metadata(backup_path)
                    if metadata:
                        backup_info["metadata"] = metadata.to_dict()
                        backup_info["format"] = metadata.backup_format.value
                except Exception as e:
                    logger.warning(f"Failed to load metadata: {e}")

                # Detect format if not in metadata
                if not backup_info["format"]:
                    try:
                        format_type = await self._detect_backup_format(backup_path)
                        backup_info["format"] = format_type.value
                    except Exception:
                        backup_info["format"] = "unknown"

                # Get contents summary for some formats
                try:
                    if backup_info["format"] == BackupFormat.CUSTOM.value:
                        contents = await self._get_custom_backup_contents(backup_path)
                        backup_info["contents"] = contents
                except Exception as e:
                    logger.warning(f"Failed to get backup contents: {e}")

            return BackupOperationResult(
                success=True,
                message=f"Retrieved backup information for: {backup_path}",
                backup_metadata=None,
                duration_seconds=0
            )

        except Exception as e:
            logger.error(f"Error getting backup info: {e}")
            return BackupOperationResult(
                success=False,
                message=f"Failed to get backup info: {e}",
                errors=[str(e)]
            )

    async def _load_backup_metadata(self, backup_path: str) -> Optional[BackupMetadata]:
        """Load backup metadata from .metadata.json file."""
        try:
            metadata_path = f"{backup_path}.metadata.json"
            if Path(metadata_path).exists():
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return BackupMetadata.from_dict(data)
        except Exception as e:
            logger.debug(f"Failed to load backup metadata: {e}")

        return None

    async def _detect_backup_format(self, backup_path: str) -> BackupFormat:
        """Detect backup format from file characteristics."""
        path = Path(backup_path)

        if path.is_dir():
            return BackupFormat.DIRECTORY

        if path.suffix == '.sql':
            return BackupFormat.PLAIN

        # Try to detect from file header
        try:
            with open(backup_path, 'rb') as f:
                header = f.read(10)
                if header.startswith(b'PGDMP'):
                    return BackupFormat.CUSTOM
                elif header[:2] == b'\x1f\x8b':  # gzip magic
                    return BackupFormat.TAR
        except Exception:
            pass

        return BackupFormat.CUSTOM  # Default assumption

    async def _validate_custom_backup(self, backup_path: str) -> bool:
        """Validate custom format backup using pg_restore --list."""
        try:
            cmd = ["pg_restore", "--list", backup_path]
            success, stdout, stderr = await self._execute_command(cmd, timeout=60)
            return success and len(stdout) > 0
        except Exception:
            return False

    async def _validate_tar_backup(self, backup_path: str) -> bool:
        """Validate tar format backup."""
        try:
            import tarfile
            with tarfile.open(backup_path, 'r') as tar:
                # Just try to list contents
                tar.getnames()
            return True
        except Exception:
            return False

    async def _validate_plain_backup(self, backup_path: str) -> bool:
        """Validate plain SQL backup."""
        try:
            with open(backup_path, 'r', encoding='utf-8') as f:
                # Read first few lines to check for SQL content
                content = f.read(1000)
                return any(keyword in content.upper() for keyword in ['CREATE', 'INSERT', 'COPY', 'SELECT'])
        except Exception:
            return False

    async def _validate_directory_backup(self, backup_path: str) -> bool:
        """Validate directory format backup."""
        try:
            path = Path(backup_path)
            # Check for expected files in directory backup
            expected_files = ['toc.dat']
            return any((path / filename).exists() for filename in expected_files)
        except Exception:
            return False

    async def _get_custom_backup_contents(self, backup_path: str) -> Optional[List[str]]:
        """Get contents listing of custom format backup."""
        try:
            cmd = ["pg_restore", "--list", backup_path]
            success, stdout, stderr = await self._execute_command(cmd, timeout=60)
            if success:
                return stdout.strip().split('\n')[:20]  # First 20 lines
        except Exception:
            pass
        return None

    async def execute(self, operation_type: str, **kwargs) -> BackupOperationResult:
        """Execute the specified backup management operation."""
        if operation_type == "list":
            return await self.list_backups(**kwargs)
        elif operation_type == "validate":
            return await self.validate_backup(**kwargs)
        elif operation_type == "cleanup":
            return await self.cleanup_old_backups(**kwargs)
        elif operation_type == "info":
            return await self.get_backup_info(**kwargs)
        else:
            raise ValueError(f"Unknown backup management operation: {operation_type}")