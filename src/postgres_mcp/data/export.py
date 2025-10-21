"""Query export operations for PostgreSQL."""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Union, AsyncGenerator
from pathlib import Path
from datetime import datetime

from .base import DataOperation, DataOperationResult, DataFormat
from ..permissions.rbac import Permission

logger = logging.getLogger(__name__)


class ExportQueryOperation(DataOperation):
    """Handles exporting query results to various formats."""

    async def export_query(self,
                          query: str,
                          file_path: str,
                          role_name: str = "default",
                          format_type: DataFormat = DataFormat.CSV,
                          parameters: Optional[List[Any]] = None,
                          batch_size: int = 1000,
                          include_metadata: bool = True) -> DataOperationResult:
        """Export query results to file.

        Args:
            query: SQL query to execute
            file_path: Output file path
            role_name: Role for permission checking
            format_type: Export format
            parameters: Optional query parameters
            batch_size: Number of rows to process at once
            include_metadata: Whether to include metadata in output

        Returns:
            DataOperationResult with export details
        """
        try:
            self._validate_permissions(role_name, [Permission.SELECT, Permission.EXPORT_DATA, Permission.FILE_ACCESS])
            await self.validate_file_access(file_path, operation="write")

            if not query.strip():
                raise ValueError("Query cannot be empty")

            if any(dangerous in query.upper() for dangerous in ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'CREATE', 'ALTER']):
                if not self.rbac_manager.check_permission(role_name, Permission.DELETE):
                    raise ValueError("Query contains potentially dangerous operations")

            async with self.connection_pool.acquire() as connection:
                logger.info(f"Executing export query to {file_path}")

                format_handler = self.get_format_handler(format_type)
                total_rows = 0
                export_metadata = {
                    "export_time": datetime.utcnow().isoformat(),
                    "query": query,
                    "parameters": parameters,
                    "format": format_type.value
                }

                with open(file_path, 'w', encoding='utf-8') as f:
                    if format_type == DataFormat.JSON:
                        f.write('[\n')
                        first_row = True

                    async with connection.transaction():
                        cursor = await connection.execute(query, *(parameters or []))

                        column_names = [desc[0] for desc in cursor.description] if cursor.description else []
                        export_metadata["columns"] = column_names

                        if format_type == DataFormat.CSV and format_handler.has_header:
                            header_data = [dict(zip(column_names, column_names))]
                            header_content = await format_handler.format_data(header_data)
                            f.write(header_content.split('\n')[1])

                        batch = []
                        async for row in cursor:
                            row_dict = dict(zip(column_names, row))
                            batch.append(row_dict)
                            total_rows += 1

                            if len(batch) >= batch_size:
                                content = await self._format_batch(batch, format_handler, format_type, first_row)
                                f.write(content)
                                if format_type == DataFormat.JSON:
                                    first_row = False
                                batch = []

                        if batch:
                            content = await self._format_batch(batch, format_handler, format_type, first_row)
                            f.write(content)

                    if format_type == DataFormat.JSON:
                        f.write('\n]')

                    if include_metadata and format_type in [DataFormat.JSON, DataFormat.JSONL]:
                        metadata_path = file_path + '.metadata.json'
                        export_metadata["total_rows"] = total_rows
                        with open(metadata_path, 'w', encoding='utf-8') as meta_f:
                            json.dump(export_metadata, meta_f, indent=2)

                await self.audit_logger.audit_log(
                    operation="export_query",
                    role=role_name,
                    details={
                        "file_path": file_path,
                        "format": format_type.value,
                        "rows_exported": total_rows,
                        "query_hash": hash(query)
                    }
                )

                return DataOperationResult(
                    success=True,
                    message=f"Successfully exported {total_rows} rows to {file_path}",
                    rows_affected=total_rows,
                    file_path=file_path,
                    format=format_type,
                    metadata=export_metadata
                )

        except Exception as e:
            logger.error(f"Error exporting query: {e}")
            await self.audit_logger.audit_log(
                operation="export_query",
                role=role_name,
                details={
                    "file_path": file_path,
                    "error": str(e)
                }
            )
            return DataOperationResult(
                success=False,
                message=f"Failed to export query: {e}"
            )

    async def export_table(self,
                          table_name: str,
                          file_path: str,
                          role_name: str = "default",
                          schema_name: str = "public",
                          format_type: DataFormat = DataFormat.CSV,
                          columns: Optional[List[str]] = None,
                          where_clause: Optional[str] = None,
                          order_by: Optional[str] = None,
                          limit: Optional[int] = None,
                          batch_size: int = 1000) -> DataOperationResult:
        """Export table data to file.

        Args:
            table_name: Name of the table to export
            file_path: Output file path
            role_name: Role for permission checking
            schema_name: Schema containing the table
            format_type: Export format
            columns: Optional list of specific columns
            where_clause: Optional WHERE clause
            order_by: Optional ORDER BY clause
            limit: Optional row limit
            batch_size: Number of rows to process at once

        Returns:
            DataOperationResult with export details
        """
        try:
            self._validate_identifier(table_name)
            self._validate_identifier(schema_name)

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

                if order_by:
                    order_columns = [col.strip() for col in order_by.split(',')]
                    for col in order_columns:
                        col_name = col.split()[0]
                        self._validate_identifier(col_name)
                    query_parts.append(f"ORDER BY {order_by}")

                if limit:
                    if not isinstance(limit, int) or limit <= 0:
                        raise ValueError("Limit must be a positive integer")
                    query_parts.append(f"LIMIT {limit}")

                query = " ".join(query_parts)

                return await self.export_query(
                    query=query,
                    file_path=file_path,
                    role_name=role_name,
                    format_type=format_type,
                    batch_size=batch_size
                )

        except Exception as e:
            logger.error(f"Error exporting table: {e}")
            return DataOperationResult(
                success=False,
                message=f"Failed to export table: {e}"
            )

    async def export_schema_structure(self,
                                    file_path: str,
                                    role_name: str = "default",
                                    schema_name: str = "public",
                                    include_data: bool = False,
                                    include_constraints: bool = True,
                                    include_indexes: bool = True) -> DataOperationResult:
        """Export schema structure to JSON file.

        Args:
            file_path: Output file path
            role_name: Role for permission checking
            schema_name: Schema to export
            include_data: Whether to include sample data
            include_constraints: Whether to include constraints
            include_indexes: Whether to include indexes

        Returns:
            DataOperationResult with export details
        """
        try:
            self._validate_permissions(role_name, [Permission.SELECT, Permission.EXPORT_DATA, Permission.FILE_ACCESS])
            self._validate_identifier(schema_name)
            await self.validate_file_access(file_path, operation="write")

            async with self.connection_pool.acquire() as connection:
                schema_info = {
                    "schema_name": schema_name,
                    "export_time": datetime.utcnow().isoformat(),
                    "tables": [],
                    "views": [],
                    "functions": []
                }

                # Export table structures
                tables_query = """
                    SELECT table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema = $1
                    ORDER BY table_name
                """

                tables = await connection.fetch(tables_query, schema_name)

                for table_row in tables:
                    table_name = table_row['table_name']
                    table_type = table_row['table_type']

                    table_info = {
                        "name": table_name,
                        "type": table_type,
                        "columns": []
                    }

                    # Get columns
                    columns_query = """
                        SELECT column_name, data_type, is_nullable, column_default,
                               character_maximum_length, numeric_precision, numeric_scale
                        FROM information_schema.columns
                        WHERE table_schema = $1 AND table_name = $2
                        ORDER BY ordinal_position
                    """

                    columns = await connection.fetch(columns_query, schema_name, table_name)

                    for col in columns:
                        column_info = {
                            "name": col['column_name'],
                            "data_type": col['data_type'],
                            "nullable": col['is_nullable'] == 'YES',
                            "default": col['column_default']
                        }

                        if col['character_maximum_length']:
                            column_info["max_length"] = col['character_maximum_length']
                        if col['numeric_precision']:
                            column_info["precision"] = col['numeric_precision']
                        if col['numeric_scale']:
                            column_info["scale"] = col['numeric_scale']

                        table_info["columns"].append(column_info)

                    # Get constraints if requested
                    if include_constraints:
                        constraints_query = """
                            SELECT constraint_name, constraint_type
                            FROM information_schema.table_constraints
                            WHERE table_schema = $1 AND table_name = $2
                        """

                        constraints = await connection.fetch(constraints_query, schema_name, table_name)
                        table_info["constraints"] = [
                            {"name": c['constraint_name'], "type": c['constraint_type']}
                            for c in constraints
                        ]

                    # Get indexes if requested
                    if include_indexes:
                        indexes_query = """
                            SELECT indexname, indexdef
                            FROM pg_indexes
                            WHERE schemaname = $1 AND tablename = $2
                        """

                        indexes = await connection.fetch(indexes_query, schema_name, table_name)
                        table_info["indexes"] = [
                            {"name": idx['indexname'], "definition": idx['indexdef']}
                            for idx in indexes
                        ]

                    # Include sample data if requested
                    if include_data and table_type == 'BASE TABLE':
                        sample_query = f'SELECT * FROM {self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)} LIMIT 5'
                        sample_rows = await connection.fetch(sample_query)
                        table_info["sample_data"] = [dict(row) for row in sample_rows]

                    if table_type == 'BASE TABLE':
                        schema_info["tables"].append(table_info)
                    else:
                        schema_info["views"].append(table_info)

                # Export functions
                functions_query = """
                    SELECT routine_name, routine_type, data_type
                    FROM information_schema.routines
                    WHERE routine_schema = $1
                    ORDER BY routine_name
                """

                functions = await connection.fetch(functions_query, schema_name)

                for func in functions:
                    schema_info["functions"].append({
                        "name": func['routine_name'],
                        "type": func['routine_type'],
                        "return_type": func['data_type']
                    })

                # Write to file
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(schema_info, f, indent=2, default=str)

                total_objects = len(schema_info["tables"]) + len(schema_info["views"]) + len(schema_info["functions"])

                await self.audit_logger.audit_log(
                    operation="export_schema_structure",
                    role=role_name,
                    details={
                        "schema_name": schema_name,
                        "file_path": file_path,
                        "objects_exported": total_objects,
                        "include_data": include_data
                    }
                )

                return DataOperationResult(
                    success=True,
                    message=f"Successfully exported schema structure to {file_path}",
                    rows_affected=total_objects,
                    file_path=file_path,
                    format=DataFormat.JSON,
                    metadata={
                        "schema_name": schema_name,
                        "tables": len(schema_info["tables"]),
                        "views": len(schema_info["views"]),
                        "functions": len(schema_info["functions"])
                    }
                )

        except Exception as e:
            logger.error(f"Error exporting schema structure: {e}")
            await self.audit_logger.audit_log(
                operation="export_schema_structure",
                role=role_name,
                details={
                    "schema_name": schema_name,
                    "file_path": file_path,
                    "error": str(e)
                }
            )
            return DataOperationResult(
                success=False,
                message=f"Failed to export schema structure: {e}"
            )

    async def _format_batch(self,
                          batch: List[Dict[str, Any]],
                          format_handler,
                          format_type: DataFormat,
                          first_batch: bool = True) -> str:
        """Format a batch of rows according to the specified format."""
        if not batch:
            return ""

        if format_type == DataFormat.JSON:
            formatted_rows = []
            for i, row in enumerate(batch):
                prefix = "" if (first_batch and i == 0) else ",\n  "
                formatted_rows.append(f"{prefix}{json.dumps(row, default=str)}")
            return "".join(formatted_rows)

        elif format_type == DataFormat.JSONL:
            return "\n".join(json.dumps(row, default=str) for row in batch) + "\n"

        else:
            content = await format_handler.format_data(batch)
            if format_type in [DataFormat.CSV, DataFormat.TSV]:
                lines = content.strip().split('\n')
                return '\n'.join(lines[1:]) + '\n'

            return content

    async def execute(self, operation_type: str, **kwargs) -> DataOperationResult:
        """Execute the specified export operation."""
        if operation_type == "query":
            return await self.export_query(**kwargs)
        elif operation_type == "table":
            return await self.export_table(**kwargs)
        elif operation_type == "schema_structure":
            return await self.export_schema_structure(**kwargs)
        else:
            raise ValueError(f"Unknown operation type: {operation_type}")