"""Create database operations for PostgreSQL."""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional

from .base import DatabaseOperation, DatabaseOperationResult, DatabaseInfo, DatabaseEncoding, DatabaseTemplate
from ..permissions.rbac import Permission

logger = logging.getLogger(__name__)


class CreateDatabaseOperation(DatabaseOperation):
    """Handles database creation operations."""

    async def create_database(self,
                            database_name: str,
                            role_name: str = "default",
                            owner: Optional[str] = None,
                            template: Optional[str] = None,
                            encoding: Optional[str] = None,
                            collate: Optional[str] = None,
                            ctype: Optional[str] = None,
                            tablespace: Optional[str] = None,
                            connection_limit: Optional[int] = None,
                            allow_connections: bool = True,
                            is_template: bool = False) -> DatabaseOperationResult:
        """Create a new database.

        Args:
            database_name: Name of the database to create
            role_name: Role for permission checking
            owner: Database owner (defaults to current user)
            template: Template database to use (template0, template1, or custom)
            encoding: Character encoding (UTF8, LATIN1, etc.)
            collate: Collation order
            ctype: Character classification
            tablespace: Tablespace for the database
            connection_limit: Maximum concurrent connections (-1 for unlimited)
            allow_connections: Whether to allow connections to this database
            is_template: Whether this database can be used as a template

        Returns:
            DatabaseOperationResult with creation details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.CREATE_DATABASE])
            self._validate_identifier(database_name)

            async with self.connection_pool.acquire() as connection:
                # Check if database already exists
                if await self._database_exists(connection, database_name):
                    return DatabaseOperationResult(
                        success=False,
                        message=f"Database '{database_name}' already exists",
                        duration_seconds=time.time() - start_time
                    )

                # Validate template if specified
                if template:
                    self._validate_identifier(template)
                    if not await self._database_exists(connection, template):
                        return DatabaseOperationResult(
                            success=False,
                            message=f"Template database '{template}' does not exist",
                            duration_seconds=time.time() - start_time,
                            errors=[f"Template '{template}' not found"]
                        )

                # Validate owner if specified
                if owner:
                    self._validate_identifier(owner)
                    owner_exists_query = "SELECT 1 FROM pg_roles WHERE rolname = $1"
                    if not await connection.fetchval(owner_exists_query, owner):
                        return DatabaseOperationResult(
                            success=False,
                            message=f"Owner role '{owner}' does not exist",
                            duration_seconds=time.time() - start_time,
                            errors=[f"Role '{owner}' not found"]
                        )

                # Validate encoding
                if encoding:
                    await self._validate_encoding(encoding)

                # Validate tablespace if specified
                if tablespace:
                    self._validate_identifier(tablespace)
                    tablespace_query = "SELECT 1 FROM pg_tablespace WHERE spcname = $1"
                    if not await connection.fetchval(tablespace_query, tablespace):
                        logger.warning(f"Tablespace '{tablespace}' may not exist")

                # Build CREATE DATABASE command
                create_parts = [f"CREATE DATABASE {self._quote_identifier(database_name)}"]

                if owner:
                    create_parts.append(f"OWNER {self._quote_identifier(owner)}")

                if template:
                    create_parts.append(f"TEMPLATE {self._quote_identifier(template)}")

                if encoding:
                    create_parts.append(f"ENCODING '{encoding}'")

                if collate:
                    create_parts.append(f"LC_COLLATE '{collate}'")

                if ctype:
                    create_parts.append(f"LC_CTYPE '{ctype}'")

                if tablespace:
                    create_parts.append(f"TABLESPACE {self._quote_identifier(tablespace)}")

                if connection_limit is not None:
                    create_parts.append(f"CONNECTION LIMIT {connection_limit}")

                if not allow_connections:
                    create_parts.append("ALLOW_CONNECTIONS false")

                if is_template:
                    create_parts.append("IS_TEMPLATE true")

                create_query = " WITH ".join(create_parts[:2]) + (
                    " WITH " + " ".join(create_parts[2:]) if len(create_parts) > 2 else ""
                )

                logger.info(f"Creating database: {database_name}")

                # Execute database creation (must be outside transaction)
                await connection.execute(create_query)

                # Get database info
                db_info = await self._get_database_info(connection, database_name)

                duration = time.time() - start_time

                await self.audit_logger.audit_log(
                    operation="create_database",
                    role=role_name,
                    details={
                        "database_name": database_name,
                        "owner": owner,
                        "template": template,
                        "encoding": encoding,
                        "duration_seconds": duration
                    }
                )

                return DatabaseOperationResult(
                    success=True,
                    message=f"Database '{database_name}' created successfully",
                    database_info=db_info,
                    duration_seconds=duration
                )

        except Exception as e:
            logger.error(f"Error creating database: {e}")
            await self.audit_logger.audit_log(
                operation="create_database",
                role=role_name,
                details={
                    "database_name": database_name,
                    "error": str(e)
                }
            )
            return DatabaseOperationResult(
                success=False,
                message=f"Failed to create database: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def create_database_from_template(self,
                                          database_name: str,
                                          template_database: str,
                                          role_name: str = "default",
                                          owner: Optional[str] = None,
                                          new_encoding: Optional[str] = None) -> DatabaseOperationResult:
        """Create a database from an existing template.

        Args:
            database_name: Name of the new database
            template_database: Name of the template database
            role_name: Role for permission checking
            owner: Owner of the new database
            new_encoding: Optional new encoding (if different from template)

        Returns:
            DatabaseOperationResult with creation details
        """
        try:
            async with self.connection_pool.acquire() as connection:
                # Get template info
                template_info = await self._get_template_info(connection, template_database)
                if not template_info:
                    return DatabaseOperationResult(
                        success=False,
                        message=f"Template database '{template_database}' does not exist",
                        errors=[f"Template '{template_database}' not found"]
                    )

                # Use template's settings as defaults
                encoding = new_encoding or template_info.get('encoding')
                collate = template_info.get('datcollate')
                ctype = template_info.get('datctype')

                return await self.create_database(
                    database_name=database_name,
                    role_name=role_name,
                    owner=owner,
                    template=template_database,
                    encoding=encoding,
                    collate=collate,
                    ctype=ctype
                )

        except Exception as e:
            logger.error(f"Error creating database from template: {e}")
            return DatabaseOperationResult(
                success=False,
                message=f"Failed to create database from template: {e}",
                errors=[str(e)]
            )

    async def create_template_database(self,
                                     database_name: str,
                                     role_name: str = "default",
                                     base_template: str = "template1",
                                     owner: Optional[str] = None,
                                     description: Optional[str] = None) -> DatabaseOperationResult:
        """Create a template database.

        Args:
            database_name: Name of the template database
            role_name: Role for permission checking
            base_template: Base template to use
            owner: Owner of the template database
            description: Optional description

        Returns:
            DatabaseOperationResult with creation details
        """
        try:
            # Create database as template
            result = await self.create_database(
                database_name=database_name,
                role_name=role_name,
                owner=owner,
                template=base_template,
                is_template=True,
                allow_connections=False  # Template databases typically don't allow connections
            )

            if result.success and description:
                # Add description comment
                try:
                    async with self.connection_pool.acquire() as connection:
                        comment_query = f"COMMENT ON DATABASE {self._quote_identifier(database_name)} IS $1"
                        await connection.execute(comment_query, description)
                except Exception as e:
                    logger.warning(f"Failed to add database comment: {e}")

            return result

        except Exception as e:
            logger.error(f"Error creating template database: {e}")
            return DatabaseOperationResult(
                success=False,
                message=f"Failed to create template database: {e}",
                errors=[str(e)]
            )

    async def create_database_with_extensions(self,
                                            database_name: str,
                                            extensions: List[str],
                                            role_name: str = "default",
                                            **database_options) -> DatabaseOperationResult:
        """Create database and install specified extensions.

        Args:
            database_name: Name of the database to create
            extensions: List of extensions to install
            role_name: Role for permission checking
            **database_options: Additional database creation options

        Returns:
            DatabaseOperationResult with creation details
        """
        try:
            self._validate_permissions(role_name, [Permission.CREATE_DATABASE, Permission.CREATE_EXTENSION])

            # Create the database first
            result = await self.create_database(
                database_name=database_name,
                role_name=role_name,
                **database_options
            )

            if not result.success:
                return result

            # Install extensions
            installed_extensions = []
            failed_extensions = []

            async with self.connection_pool.acquire() as connection:
                # Connect to the new database
                await connection.execute(f"\\c {self._quote_identifier(database_name)}")

                for extension in extensions:
                    try:
                        self._validate_identifier(extension)

                        # Check if extension is available
                        available_query = "SELECT 1 FROM pg_available_extensions WHERE name = $1"
                        if not await connection.fetchval(available_query, extension):
                            failed_extensions.append(f"Extension '{extension}' is not available")
                            continue

                        # Install extension
                        create_ext_query = f"CREATE EXTENSION IF NOT EXISTS {self._quote_identifier(extension)}"
                        await connection.execute(create_ext_query)
                        installed_extensions.append(extension)

                        logger.info(f"Installed extension '{extension}' in database '{database_name}'")

                    except Exception as e:
                        error_msg = f"Failed to install extension '{extension}': {e}"
                        failed_extensions.append(error_msg)
                        logger.error(error_msg)

            # Update result with extension information
            warnings = []
            if failed_extensions:
                warnings.extend(failed_extensions)

            success_message = f"Database '{database_name}' created successfully"
            if installed_extensions:
                success_message += f" with extensions: {', '.join(installed_extensions)}"
            if failed_extensions:
                success_message += f" (some extensions failed to install)"

            await self.audit_logger.audit_log(
                operation="create_database_with_extensions",
                role=role_name,
                details={
                    "database_name": database_name,
                    "installed_extensions": installed_extensions,
                    "failed_extensions": failed_extensions
                }
            )

            return DatabaseOperationResult(
                success=result.success,
                message=success_message,
                database_info=result.database_info,
                duration_seconds=result.duration_seconds,
                warnings=warnings
            )

        except Exception as e:
            logger.error(f"Error creating database with extensions: {e}")
            return DatabaseOperationResult(
                success=False,
                message=f"Failed to create database with extensions: {e}",
                errors=[str(e)]
            )

    async def execute(self, operation_type: str, **kwargs) -> DatabaseOperationResult:
        """Execute the specified database creation operation."""
        if operation_type == "create":
            return await self.create_database(**kwargs)
        elif operation_type == "from_template":
            return await self.create_database_from_template(**kwargs)
        elif operation_type == "template":
            return await self.create_template_database(**kwargs)
        elif operation_type == "with_extensions":
            return await self.create_database_with_extensions(**kwargs)
        else:
            raise ValueError(f"Unknown database creation operation: {operation_type}")