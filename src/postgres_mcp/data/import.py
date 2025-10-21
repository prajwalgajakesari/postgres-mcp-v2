"""Data import operations for PostgreSQL."""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Union, Tuple
from pathlib import Path
from datetime import datetime

from .base import DataOperation, DataOperationResult, DataFormat
from ..permissions.rbac import Permission

logger = logging.getLogger(__name__)


class ImportDataOperation(DataOperation):
    """Handles importing data from various sources and formats."""

    async def import_from_file(self,
                             table_name: str,
                             file_path: str,
                             role_name: str = "default",
                             schema_name: str = "public",
                             format_type: DataFormat = DataFormat.CSV,
                             column_mapping: Optional[Dict[str, str]] = None,
                             create_table: bool = False,
                             truncate_first: bool = False,
                             batch_size: int = 1000,
                             on_error: str = "abort") -> DataOperationResult:
        """Import data from file to table.

        Args:
            table_name: Target table name
            file_path: Input file path
            role_name: Role for permission checking
            schema_name: Target schema
            format_type: Input file format
            column_mapping: Optional mapping from file columns to table columns
            create_table: Whether to auto-create the table
            truncate_first: Whether to truncate table before import
            batch_size: Number of rows to insert at once
            on_error: How to handle errors ('abort', 'skip', 'log')

        Returns:
            DataOperationResult with import details
        """
        try:
            required_perms = [Permission.INSERT, Permission.IMPORT_DATA, Permission.FILE_ACCESS]
            if create_table:
                required_perms.append(Permission.CREATE_TABLE)
            if truncate_first:
                required_perms.append(Permission.TRUNCATE)

            self._validate_permissions(role_name, required_perms)
            self._validate_identifier(table_name)
            self._validate_identifier(schema_name)
            await self.validate_file_access(file_path, operation="read")

            if on_error not in ["abort", "skip", "log"]:
                raise ValueError(f"Invalid on_error value: {on_error}")

            async with self.connection_pool.acquire() as connection:
                full_table_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"

                # Read and parse file
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                format_handler = self.get_format_handler(format_type)
                data_rows = await format_handler.parse_data(content)

                if not data_rows:
                    return DataOperationResult(
                        success=True,
                        message="No data to import",
                        rows_affected=0
                    )

                # Determine columns
                file_columns = list(data_rows[0].keys())

                if column_mapping:
                    for src_col, tgt_col in column_mapping.items():
                        self._validate_identifier(tgt_col)
                    target_columns = [column_mapping.get(col, col) for col in file_columns]
                else:
                    target_columns = file_columns

                # Create table if requested
                if create_table:
                    await self._create_table_from_data(
                        connection, table_name, schema_name, data_rows, target_columns
                    )
                elif not await self.validate_table_exists(connection, table_name, schema_name):
                    raise ValueError(f"Table {full_table_name} does not exist")

                # Truncate if requested
                if truncate_first:
                    await connection.execute(f"TRUNCATE TABLE {full_table_name}")
                    logger.info(f"Truncated table {full_table_name}")

                # Import data in batches
                total_imported = 0
                total_errors = 0
                error_details = []

                async with connection.transaction():
                    for i in range(0, len(data_rows), batch_size):
                        batch = data_rows[i:i + batch_size]

                        try:
                            imported_count = await self._import_batch(
                                connection, full_table_name, batch, target_columns, column_mapping
                            )
                            total_imported += imported_count

                        except Exception as batch_error:
                            total_errors += len(batch)
                            error_msg = f"Batch {i//batch_size + 1}: {str(batch_error)}"
                            error_details.append(error_msg)

                            if on_error == "abort":
                                raise batch_error
                            elif on_error == "log":
                                logger.error(f"Import batch error: {error_msg}")

                import_metadata = {
                    "file_path": file_path,
                    "format": format_type.value,
                    "total_rows": len(data_rows),
                    "imported_rows": total_imported,
                    "error_rows": total_errors,
                    "column_mapping": column_mapping,
                    "import_time": datetime.utcnow().isoformat()
                }

                if error_details:
                    import_metadata["errors"] = error_details

                await self.audit_logger.audit_log(
                    operation="import_from_file",
                    role=role_name,
                    details={
                        "table": full_table_name,
                        "file_path": file_path,
                        "format": format_type.value,
                        "rows_imported": total_imported,
                        "rows_failed": total_errors
                    }
                )

                success = total_errors == 0 or on_error != "abort"
                message = f"Imported {total_imported} rows"
                if total_errors > 0:
                    message += f", {total_errors} errors"

                return DataOperationResult(
                    success=success,
                    message=message,
                    rows_affected=total_imported,
                    file_path=file_path,
                    format=format_type,
                    metadata=import_metadata
                )

        except Exception as e:
            logger.error(f"Error importing data: {e}")
            await self.audit_logger.audit_log(
                operation="import_from_file",
                role=role_name,
                details={
                    "table": f"{schema_name}.{table_name}",
                    "file_path": file_path,
                    "error": str(e)
                }
            )
            return DataOperationResult(
                success=False,
                message=f"Failed to import data: {e}"
            )

    async def import_from_json_schema(self,
                                    table_name: str,
                                    json_file_path: str,
                                    role_name: str = "default",
                                    schema_name: str = "public",
                                    data_path: str = "",
                                    flatten_nested: bool = True,
                                    max_depth: int = 3) -> DataOperationResult:
        """Import data from JSON file with automatic schema detection.

        Args:
            table_name: Target table name
            json_file_path: JSON file path
            role_name: Role for permission checking
            schema_name: Target schema
            data_path: JSONPath to extract data (e.g., "$.results")
            flatten_nested: Whether to flatten nested objects
            max_depth: Maximum nesting depth to flatten

        Returns:
            DataOperationResult with import details
        """
        try:
            self._validate_permissions(role_name, [Permission.INSERT, Permission.IMPORT_DATA, Permission.FILE_ACCESS])
            await self.validate_file_access(json_file_path, operation="read")

            with open(json_file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)

            # Extract data using path if specified
            if data_path:
                json_data = self._extract_json_path(json_data, data_path)

            if not isinstance(json_data, list):
                json_data = [json_data]

            # Flatten nested structures if requested
            if flatten_nested:
                json_data = [self._flatten_dict(item, max_depth=max_depth) for item in json_data]

            # Convert to format handler data
            format_handler = self.get_format_handler(DataFormat.JSON)

            return await self.import_from_file(
                table_name=table_name,
                file_path=json_file_path,
                role_name=role_name,
                schema_name=schema_name,
                format_type=DataFormat.JSON,
                create_table=True
            )

        except Exception as e:
            logger.error(f"Error importing JSON with schema: {e}")
            return DataOperationResult(
                success=False,
                message=f"Failed to import JSON: {e}"
            )

    async def import_with_validation(self,
                                   table_name: str,
                                   file_path: str,
                                   validation_rules: Dict[str, Dict[str, Any]],
                                   role_name: str = "default",
                                   schema_name: str = "public",
                                   format_type: DataFormat = DataFormat.CSV,
                                   reject_file: Optional[str] = None) -> DataOperationResult:
        """Import data with validation rules.

        Args:
            table_name: Target table name
            file_path: Input file path
            validation_rules: Column validation rules
            role_name: Role for permission checking
            schema_name: Target schema
            format_type: Input file format
            reject_file: Optional file path for rejected rows

        Returns:
            DataOperationResult with import and validation details
        """
        try:
            self._validate_permissions(role_name, [Permission.INSERT, Permission.IMPORT_DATA, Permission.FILE_ACCESS])
            await self.validate_file_access(file_path, operation="read")

            if reject_file:
                await self.validate_file_access(reject_file, operation="write")

            # Read and parse file
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            format_handler = self.get_format_handler(format_type)
            data_rows = await format_handler.parse_data(content)

            if not data_rows:
                return DataOperationResult(
                    success=True,
                    message="No data to import",
                    rows_affected=0
                )

            # Validate data
            valid_rows = []
            invalid_rows = []

            for row_num, row in enumerate(data_rows, 1):
                validation_errors = self._validate_row(row, validation_rules)

                if validation_errors:
                    row['_validation_errors'] = validation_errors
                    row['_row_number'] = row_num
                    invalid_rows.append(row)
                else:
                    valid_rows.append(row)

            # Write rejected rows to file if specified
            if reject_file and invalid_rows:
                reject_data = [{k: v for k, v in row.items() if not k.startswith('_')} for row in invalid_rows]
                reject_content = await format_handler.format_data(reject_data)

                with open(reject_file, 'w', encoding='utf-8') as f:
                    f.write(reject_content)

            # Import valid rows only
            if valid_rows:
                # Create temporary file with valid data
                temp_file = file_path + '.valid_temp'
                valid_content = await format_handler.format_data(valid_rows)

                with open(temp_file, 'w', encoding='utf-8') as f:
                    f.write(valid_content)

                try:
                    result = await self.import_from_file(
                        table_name=table_name,
                        file_path=temp_file,
                        role_name=role_name,
                        schema_name=schema_name,
                        format_type=format_type
                    )

                    # Update metadata
                    if result.metadata:
                        result.metadata.update({
                            "validation_applied": True,
                            "valid_rows": len(valid_rows),
                            "invalid_rows": len(invalid_rows),
                            "validation_rules": validation_rules
                        })

                    return result

                finally:
                    # Clean up temp file
                    Path(temp_file).unlink(missing_ok=True)

            else:
                return DataOperationResult(
                    success=False,
                    message=f"All {len(invalid_rows)} rows failed validation",
                    metadata={
                        "validation_applied": True,
                        "valid_rows": 0,
                        "invalid_rows": len(invalid_rows),
                        "reject_file": reject_file
                    }
                )

        except Exception as e:
            logger.error(f"Error importing with validation: {e}")
            return DataOperationResult(
                success=False,
                message=f"Failed to import with validation: {e}"
            )

    async def _create_table_from_data(self,
                                    connection,
                                    table_name: str,
                                    schema_name: str,
                                    data_rows: List[Dict[str, Any]],
                                    columns: List[str]) -> None:
        """Create table with inferred schema from data."""
        if not data_rows:
            raise ValueError("Cannot create table from empty data")

        # Infer column types from sample data
        column_types = {}

        for column in columns:
            sample_values = [row.get(column) for row in data_rows[:100] if row.get(column) is not None]

            if not sample_values:
                column_types[column] = "TEXT"
                continue

            # Infer type based on sample values
            if all(isinstance(v, bool) for v in sample_values):
                column_types[column] = "BOOLEAN"
            elif all(isinstance(v, int) for v in sample_values):
                max_val = max(abs(v) for v in sample_values)
                if max_val < 2**31:
                    column_types[column] = "INTEGER"
                else:
                    column_types[column] = "BIGINT"
            elif all(isinstance(v, (int, float)) for v in sample_values):
                column_types[column] = "NUMERIC"
            elif all(isinstance(v, str) and len(v) <= 255 for v in sample_values):
                max_length = max(len(str(v)) for v in sample_values)
                column_types[column] = f"VARCHAR({min(max_length * 2, 1000)})"
            else:
                column_types[column] = "TEXT"

        # Create table
        full_table_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"

        column_defs = []
        for column in columns:
            column_def = f"{self._quote_identifier(column)} {column_types[column]}"
            column_defs.append(column_def)

        create_query = f"CREATE TABLE {full_table_name} ({', '.join(column_defs)})"
        await connection.execute(create_query)

        logger.info(f"Created table {full_table_name} with {len(columns)} columns")

    async def _import_batch(self,
                          connection,
                          table_name: str,
                          batch: List[Dict[str, Any]],
                          target_columns: List[str],
                          column_mapping: Optional[Dict[str, str]] = None) -> int:
        """Import a batch of rows."""
        if not batch:
            return 0

        placeholders = []
        values = []

        for row in batch:
            row_values = []
            for col in target_columns:
                source_col = col
                if column_mapping:
                    # Find source column for this target column
                    for src, tgt in column_mapping.items():
                        if tgt == col:
                            source_col = src
                            break

                value = row.get(source_col)
                row_values.append(value)

            values.extend(row_values)
            row_placeholders = ', '.join(f'${i}' for i in range(len(row_values)))
            placeholders.append(f"({row_placeholders})")

        # Adjust placeholder numbering
        placeholder_counter = 1
        for i, placeholder_group in enumerate(placeholders):
            group_placeholders = placeholder_group.count('$')
            new_group = placeholder_group
            for j in range(group_placeholders):
                new_group = new_group.replace(f'${j+1}', f'${placeholder_counter}', 1)
                placeholder_counter += 1
            placeholders[i] = new_group

        quoted_columns = [self._quote_identifier(col) for col in target_columns]
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(quoted_columns)})
            VALUES {', '.join(placeholders)}
        """

        result = await connection.execute(insert_query, *values)
        return len(batch)

    def _validate_row(self, row: Dict[str, Any], validation_rules: Dict[str, Dict[str, Any]]) -> List[str]:
        """Validate a single row against validation rules."""
        errors = []

        for column, rules in validation_rules.items():
            value = row.get(column)

            # Required field check
            if rules.get('required', False) and (value is None or value == ''):
                errors.append(f"{column}: required field is missing")
                continue

            if value is None:
                continue

            # Type validation
            expected_type = rules.get('type')
            if expected_type:
                if expected_type == 'int' and not isinstance(value, int):
                    try:
                        int(value)
                    except (ValueError, TypeError):
                        errors.append(f"{column}: not a valid integer")

                elif expected_type == 'float' and not isinstance(value, (int, float)):
                    try:
                        float(value)
                    except (ValueError, TypeError):
                        errors.append(f"{column}: not a valid number")

                elif expected_type == 'str' and not isinstance(value, str):
                    errors.append(f"{column}: not a string")

            # Range validation
            if 'min' in rules and value < rules['min']:
                errors.append(f"{column}: value {value} is below minimum {rules['min']}")

            if 'max' in rules and value > rules['max']:
                errors.append(f"{column}: value {value} is above maximum {rules['max']}")

            # Length validation
            if 'min_length' in rules and len(str(value)) < rules['min_length']:
                errors.append(f"{column}: length {len(str(value))} is below minimum {rules['min_length']}")

            if 'max_length' in rules and len(str(value)) > rules['max_length']:
                errors.append(f"{column}: length {len(str(value))} is above maximum {rules['max_length']}")

            # Pattern validation
            if 'pattern' in rules:
                import re
                if not re.match(rules['pattern'], str(value)):
                    errors.append(f"{column}: value '{value}' does not match pattern {rules['pattern']}")

            # Enum validation
            if 'enum' in rules and value not in rules['enum']:
                errors.append(f"{column}: value '{value}' not in allowed values {rules['enum']}")

        return errors

    def _extract_json_path(self, data: Any, path: str) -> Any:
        """Extract data using simple JSONPath-like syntax."""
        if not path or path == "$":
            return data

        # Simple implementation for basic paths like "$.results", "$.data.items"
        parts = path.split('.')[1:]  # Remove leading $

        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                current = current[int(part)]
            else:
                raise ValueError(f"Cannot navigate path {path} in data structure")

        return current

    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '_', max_depth: int = 3, current_depth: int = 0) -> Dict[str, Any]:
        """Flatten nested dictionary structure."""
        if current_depth >= max_depth:
            return {parent_key: str(d) if parent_key else str(d)}

        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep, max_depth, current_depth + 1).items())
            elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                # Handle list of dictionaries by taking first element
                items.extend(self._flatten_dict(v[0], new_key, sep, max_depth, current_depth + 1).items())
            else:
                items.append((new_key, v))

        return dict(items)

    async def execute(self, operation_type: str, **kwargs) -> DataOperationResult:
        """Execute the specified import operation."""
        if operation_type == "from_file":
            return await self.import_from_file(**kwargs)
        elif operation_type == "from_json_schema":
            return await self.import_from_json_schema(**kwargs)
        elif operation_type == "with_validation":
            return await self.import_with_validation(**kwargs)
        else:
            raise ValueError(f"Unknown operation type: {operation_type}")