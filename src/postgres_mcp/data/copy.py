"""COPY data operations for PostgreSQL."""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Union, Tuple
from pathlib import Path

import psycopg

from .base import DataOperation, DataOperationResult, DataFormat, FormatHandler
from ..permissions.rbac import Permission

logger = logging.getLogger(__name__)


class CopyDataOperation(DataOperation):
    """Handles COPY operations for data import/export."""

    async def copy_to_file(self,
                          table_name: str,
                          file_path: str,
                          role_name: str = "default",
                          schema_name: str = "public",
                          format_type: DataFormat = DataFormat.CSV,
                          columns: Optional[List[str]] = None,
                          where_clause: Optional[str] = None,
                          format_options: Optional[Dict[str, str]] = None) -> DataOperationResult:
        """Copy data from table to file using PostgreSQL COPY command.

        Args:
            table_name: Name of the table to copy from
            file_path: Path to output file
            role_name: Role for permission checking
            schema_name: Schema containing the table
            format_type: Output format (CSV, TSV, etc.)
            columns: Optional list of specific columns to copy
            where_clause: Optional WHERE clause for filtering
            format_options: Optional format-specific options

        Returns:
            DataOperationResult with operation details
        """
        try:
            self._validate_permissions(role_name, [Permission.SELECT, Permission.COPY_DATA, Permission.FILE_ACCESS])
            self._validate_identifier(table_name)
            self._validate_identifier(schema_name)

            await self.validate_file_access(file_path, operation="write")

            async with self.connection_pool.acquire() as connection:
                if not await self.validate_table_exists(connection, table_name, schema_name):
                    raise ValueError(f"Table {schema_name}.{table_name} does not exist")

                full_table_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"

                column_list = "*"
                if columns:
                    for col in columns:
                        self._validate_identifier(col)
                    column_list = ", ".join(self._quote_identifier(col) for col in columns)

                query_parts = [f"SELECT {column_list} FROM {full_table_name}"]

                if where_clause:
                    if any(dangerous in where_clause.upper() for dangerous in [';', 'DROP', 'DELETE', 'INSERT', 'UPDATE', 'CREATE']):
                        raise ValueError("Invalid characters in WHERE clause")
                    query_parts.append(f"WHERE {where_clause}")

                query = " ".join(query_parts)

                format_handler = self.get_format_handler(format_type)
                copy_options = format_handler.get_copy_options()

                if format_options:
                    copy_options.update(format_options)

                options_str = ", ".join(f"{k} {v}" for k, v in copy_options.items())
                copy_query = f"COPY ({query}) TO STDOUT WITH ({options_str})"

                logger.info(f"Executing COPY TO for table {full_table_name}")

                rows_copied = 0
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        with open(file_path, 'w', encoding='utf-8') as f:
                            async with cursor.copy(copy_query) as copy:
                                async for chunk in copy:
                                    f.write(chunk.decode('utf-8'))
                                    rows_copied += chunk.decode('utf-8').count('\n')

                await self.audit_logger.audit_log(
                    operation="copy_to_file",
                    role=role_name,
                    details={
                        "table": f"{schema_name}.{table_name}",
                        "file_path": file_path,
                        "format": format_type.value,
                        "rows_copied": rows_copied
                    }
                )

                return DataOperationResult(
                    success=True,
                    message=f"Successfully copied {rows_copied} rows to {file_path}",
                    rows_affected=rows_copied,
                    file_path=file_path,
                    format=format_type,
                    metadata={
                        "table": f"{schema_name}.{table_name}",
                        "columns": columns or "all",
                        "format_options": copy_options
                    }
                )

        except Exception as e:
            logger.error(f"Error copying data to file: {e}")
            await self.audit_logger.audit_log(
                operation="copy_to_file",
                role=role_name,
                details={
                    "table": f"{schema_name}.{table_name}",
                    "file_path": file_path,
                    "error": str(e)
                }
            )
            return DataOperationResult(
                success=False,
                message=f"Failed to copy data: {e}"
            )

    async def copy_from_file(self,
                           table_name: str,
                           file_path: str,
                           role_name: str = "default",
                           schema_name: str = "public",
                           format_type: DataFormat = DataFormat.CSV,
                           columns: Optional[List[str]] = None,
                           format_options: Optional[Dict[str, str]] = None,
                           on_conflict: str = "error") -> DataOperationResult:
        """Copy data from file to table using PostgreSQL COPY command.

        Args:
            table_name: Name of the target table
            file_path: Path to input file
            role_name: Role for permission checking
            schema_name: Schema containing the table
            format_type: Input format (CSV, TSV, etc.)
            columns: Optional list of target columns
            format_options: Optional format-specific options
            on_conflict: How to handle conflicts ("error", "ignore", "update")

        Returns:
            DataOperationResult with operation details
        """
        try:
            self._validate_permissions(role_name, [Permission.INSERT, Permission.COPY_DATA, Permission.FILE_ACCESS])
            self._validate_identifier(table_name)
            self._validate_identifier(schema_name)

            await self.validate_file_access(file_path, operation="read")

            if on_conflict not in ["error", "ignore", "update"]:
                raise ValueError(f"Invalid on_conflict value: {on_conflict}")

            async with self.connection_pool.acquire() as connection:
                if not await self.validate_table_exists(connection, table_name, schema_name):
                    raise ValueError(f"Table {schema_name}.{table_name} does not exist")

                full_table_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"

                column_list = ""
                if columns:
                    for col in columns:
                        self._validate_identifier(col)
                    column_list = f"({', '.join(self._quote_identifier(col) for col in columns)})"

                format_handler = self.get_format_handler(format_type)
                copy_options = format_handler.get_copy_options()

                if format_options:
                    copy_options.update(format_options)

                options_str = ", ".join(f"{k} {v}" for k, v in copy_options.items())

                temp_table = None
                if on_conflict != "error":
                    temp_table = f"temp_{table_name}_{hash(file_path) % 10000}"
                    await connection.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {full_table_name})")

                target_table = temp_table if temp_table else full_table_name
                copy_query = f"COPY {target_table}{column_list} FROM STDIN WITH ({options_str})"

                logger.info(f"Executing COPY FROM for table {full_table_name}")

                rows_inserted = 0
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            async with cursor.copy(copy_query) as copy:
                                while True:
                                    chunk = f.read(8192)
                                    if not chunk:
                                        break
                                    await copy.write(chunk.encode('utf-8'))
                                    rows_inserted += chunk.count('\n')

                    if temp_table and on_conflict == "ignore":
                        insert_query = f"""
                            INSERT INTO {full_table_name}
                            SELECT * FROM {temp_table}
                            ON CONFLICT DO NOTHING
                        """
                        result = await connection.execute(insert_query)
                        rows_inserted = int(result.split()[-1])

                    elif temp_table and on_conflict == "update":
                        table_columns = await self.get_table_columns(connection, table_name, schema_name)
                        pk_columns = await self._get_primary_key_columns(connection, table_name, schema_name)

                        if not pk_columns:
                            raise ValueError(f"Table {full_table_name} has no primary key for conflict resolution")

                        non_pk_columns = [col[0] for col in table_columns if col[0] not in pk_columns]

                        update_set = ", ".join(
                            f"{self._quote_identifier(col)} = EXCLUDED.{self._quote_identifier(col)}"
                            for col in non_pk_columns
                        )

                        conflict_columns = ", ".join(self._quote_identifier(col) for col in pk_columns)

                        upsert_query = f"""
                            INSERT INTO {full_table_name}
                            SELECT * FROM {temp_table}
                            ON CONFLICT ({conflict_columns}) DO UPDATE SET {update_set}
                        """
                        result = await connection.execute(upsert_query)
                        rows_inserted = int(result.split()[-1])

                await self.audit_logger.audit_log(
                    operation="copy_from_file",
                    role=role_name,
                    details={
                        "table": f"{schema_name}.{table_name}",
                        "file_path": file_path,
                        "format": format_type.value,
                        "rows_inserted": rows_inserted,
                        "on_conflict": on_conflict
                    }
                )

                return DataOperationResult(
                    success=True,
                    message=f"Successfully copied {rows_inserted} rows from {file_path}",
                    rows_affected=rows_inserted,
                    file_path=file_path,
                    format=format_type,
                    metadata={
                        "table": f"{schema_name}.{table_name}",
                        "columns": columns or "all",
                        "format_options": copy_options,
                        "on_conflict": on_conflict
                    }
                )

        except Exception as e:
            logger.error(f"Error copying data from file: {e}")
            await self.audit_logger.audit_log(
                operation="copy_from_file",
                role=role_name,
                details={
                    "table": f"{schema_name}.{table_name}",
                    "file_path": file_path,
                    "error": str(e)
                }
            )
            return DataOperationResult(
                success=False,
                message=f"Failed to copy data: {e}"
            )

    async def copy_between_tables(self,
                                source_table: str,
                                target_table: str,
                                role_name: str = "default",
                                source_schema: str = "public",
                                target_schema: str = "public",
                                columns: Optional[List[str]] = None,
                                where_clause: Optional[str] = None,
                                column_mapping: Optional[Dict[str, str]] = None) -> DataOperationResult:
        """Copy data between tables.

        Args:
            source_table: Source table name
            target_table: Target table name
            role_name: Role for permission checking
            source_schema: Source table schema
            target_schema: Target table schema
            columns: Optional list of columns to copy
            where_clause: Optional WHERE clause for filtering
            column_mapping: Optional mapping of source to target column names

        Returns:
            DataOperationResult with operation details
        """
        try:
            self._validate_permissions(role_name, [Permission.SELECT, Permission.INSERT, Permission.COPY_DATA])
            self._validate_identifier(source_table)
            self._validate_identifier(target_table)
            self._validate_identifier(source_schema)
            self._validate_identifier(target_schema)

            async with self.connection_pool.acquire() as connection:
                if not await self.validate_table_exists(connection, source_table, source_schema):
                    raise ValueError(f"Source table {source_schema}.{source_table} does not exist")

                if not await self.validate_table_exists(connection, target_table, target_schema):
                    raise ValueError(f"Target table {target_schema}.{target_table} does not exist")

                source_full = f"{self._quote_identifier(source_schema)}.{self._quote_identifier(source_table)}"
                target_full = f"{self._quote_identifier(target_schema)}.{self._quote_identifier(target_table)}"

                if columns:
                    for col in columns:
                        self._validate_identifier(col)
                    source_columns = ", ".join(self._quote_identifier(col) for col in columns)
                else:
                    source_columns = "*"

                target_columns = ""
                if column_mapping:
                    for src_col, tgt_col in column_mapping.items():
                        self._validate_identifier(src_col)
                        self._validate_identifier(tgt_col)

                    source_columns = ", ".join(self._quote_identifier(src) for src in column_mapping.keys())
                    target_columns = f"({', '.join(self._quote_identifier(tgt) for tgt in column_mapping.values())})"

                select_query = f"SELECT {source_columns} FROM {source_full}"

                if where_clause:
                    if any(dangerous in where_clause.upper() for dangerous in [';', 'DROP', 'DELETE', 'INSERT', 'UPDATE', 'CREATE']):
                        raise ValueError("Invalid characters in WHERE clause")
                    select_query += f" WHERE {where_clause}"

                insert_query = f"INSERT INTO {target_full}{target_columns} ({select_query})"

                logger.info(f"Copying data from {source_full} to {target_full}")

                async with connection.transaction():
                    result = await connection.execute(insert_query)
                    rows_copied = int(result.split()[-1]) if result else 0

                await self.audit_logger.audit_log(
                    operation="copy_between_tables",
                    role=role_name,
                    details={
                        "source_table": f"{source_schema}.{source_table}",
                        "target_table": f"{target_schema}.{target_table}",
                        "rows_copied": rows_copied,
                        "columns": columns,
                        "column_mapping": column_mapping
                    }
                )

                return DataOperationResult(
                    success=True,
                    message=f"Successfully copied {rows_copied} rows from {source_full} to {target_full}",
                    rows_affected=rows_copied,
                    metadata={
                        "source_table": f"{source_schema}.{source_table}",
                        "target_table": f"{target_schema}.{target_table}",
                        "columns": columns or "all",
                        "column_mapping": column_mapping
                    }
                )

        except Exception as e:
            logger.error(f"Error copying data between tables: {e}")
            await self.audit_logger.audit_log(
                operation="copy_between_tables",
                role=role_name,
                details={
                    "source_table": f"{source_schema}.{source_table}",
                    "target_table": f"{target_schema}.{target_table}",
                    "error": str(e)
                }
            )
            return DataOperationResult(
                success=False,
                message=f"Failed to copy data: {e}"
            )

    async def _get_primary_key_columns(self, connection, table_name: str, schema_name: str = "public") -> List[str]:
        """Get primary key column names for a table."""
        query = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = (
                SELECT oid FROM pg_class WHERE relname = $2 AND relnamespace = (
                    SELECT oid FROM pg_namespace WHERE nspname = $1
                )
            )
            AND i.indisprimary
            ORDER BY a.attnum
        """
        rows = await connection.fetch(query, schema_name, table_name)
        return [row['attname'] for row in rows]

    async def execute(self, operation_type: str, **kwargs) -> DataOperationResult:
        """Execute the specified COPY operation."""
        if operation_type == "to_file":
            return await self.copy_to_file(**kwargs)
        elif operation_type == "from_file":
            return await self.copy_from_file(**kwargs)
        elif operation_type == "between_tables":
            return await self.copy_between_tables(**kwargs)
        else:
            raise ValueError(f"Unknown operation type: {operation_type}")