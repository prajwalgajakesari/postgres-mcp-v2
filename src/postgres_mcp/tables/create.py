"""Create table operation implementation."""

from typing import Optional, Dict, Any, List
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import TableOperation


class CreateTableOperation(TableOperation):
    """Operation to create a new database table."""

    def get_required_permissions(self) -> List[Permission]:
        """Get required permissions for creating tables."""
        return [Permission.CREATE_TABLE]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        """Validate create table operation parameters.

        Args:
            table_name: The table name to create
            schema_name: The schema name (default: public)
            columns: List of column definitions
            constraints: Optional table constraints
            if_not_exists: Don't error if table exists
            inherits: Parent table to inherit from
            partition_by: Partitioning strategy
            tablespace: Tablespace to use
            storage_parameters: Storage parameters
            temporary: Create as temporary table
            unlogged: Create as unlogged table

        Returns:
            Tuple of (is_valid, error_message)
        """
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")

        if not table_name:
            return False, "Table name is required"

        # Validate table name
        is_valid, error = self.name_validator.validate_identifier(
            table_name,
            allow_quoted=False
        )
        if not is_valid:
            return False, f"Invalid table name: {error}"

        # Validate schema name
        if schema_name:
            is_valid, error = self.name_validator.validate_identifier(
                schema_name,
                allow_quoted=False
            )
            if not is_valid:
                return False, f"Invalid schema name: {error}"

            # Check if schema exists
            if not await self.check_schema_exists(schema_name):
                return False, f"Schema '{schema_name}' does not exist"

        # Check if table already exists (unless if_not_exists is True)
        if not kwargs.get("if_not_exists", False):
            if await self.check_table_exists(table_name, schema_name):
                return False, f"Table '{schema_name}.{table_name}' already exists"

        # Validate columns
        columns = kwargs.get("columns", [])
        if not columns:
            return False, "At least one column is required"

        column_names = set()
        for column in columns:
            if not isinstance(column, dict):
                return False, "Each column must be a dictionary"

            col_name = column.get("name")
            if not col_name:
                return False, "Column name is required"

            # Check for duplicate column names
            if col_name.lower() in column_names:
                return False, f"Duplicate column name: {col_name}"
            column_names.add(col_name.lower())

            # Validate column name
            is_valid, error = self.name_validator.validate_identifier(
                col_name,
                allow_quoted=False
            )
            if not is_valid:
                return False, f"Invalid column name '{col_name}': {error}"

            # Validate data type
            data_type = column.get("type")
            if not data_type:
                return False, f"Data type is required for column '{col_name}'"

            is_valid, error = self.validate_data_type(data_type)
            if not is_valid:
                return False, f"Invalid data type for column '{col_name}': {error}"

        # Validate parent table if inherits is specified
        if inherits := kwargs.get("inherits"):
            if isinstance(inherits, str):
                inherits = [inherits]
            for parent_table in inherits:
                # Parse schema.table format if present
                if "." in parent_table:
                    parent_schema, parent_name = parent_table.split(".", 1)
                else:
                    parent_schema = schema_name
                    parent_name = parent_table

                if not await self.check_table_exists(parent_name, parent_schema):
                    return False, f"Parent table '{parent_schema}.{parent_name}' does not exist"

        return True, None

    def build_create_table_sql(self, **kwargs) -> tuple[str, List[Any]]:
        """Build CREATE TABLE SQL statement.

        Args:
            table_name: Table name
            schema_name: Schema name
            columns: List of column definitions
            constraints: Optional table constraints
            if_not_exists: Include IF NOT EXISTS clause
            inherits: Parent table(s) to inherit from
            partition_by: Partitioning strategy
            tablespace: Tablespace name
            storage_parameters: Storage parameters
            temporary: Create as TEMPORARY
            unlogged: Create as UNLOGGED
            comment: Table comment

        Returns:
            Tuple of (sql_statement, parameters)
        """
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")
        columns = kwargs.get("columns", [])
        constraints = kwargs.get("constraints", [])
        if_not_exists = kwargs.get("if_not_exists", False)
        inherits = kwargs.get("inherits")
        partition_by = kwargs.get("partition_by")
        tablespace = kwargs.get("tablespace")
        storage_parameters = kwargs.get("storage_parameters", {})
        temporary = kwargs.get("temporary", False)
        unlogged = kwargs.get("unlogged", False)

        # Build CREATE TABLE statement
        sql_parts = []

        if temporary:
            sql_parts.append("CREATE TEMPORARY TABLE")
        elif unlogged:
            sql_parts.append("CREATE UNLOGGED TABLE")
        else:
            sql_parts.append("CREATE TABLE")

        if if_not_exists:
            sql_parts.append("IF NOT EXISTS")

        # Add schema.table name
        full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"
        sql_parts.append(full_name)

        # Build column and constraint definitions
        definitions = []

        # Add columns
        for column in columns:
            definitions.append(self.format_column_definition(column))

        # Add table-level constraints
        for constraint in constraints:
            definitions.append(self.format_constraint_definition(constraint))

        sql_parts.append(f"({', '.join(definitions)})")

        # Add INHERITS clause
        if inherits:
            if isinstance(inherits, str):
                inherits = [inherits]
            parent_tables = []
            for parent in inherits:
                if "." in parent:
                    parent_schema, parent_name = parent.split(".", 1)
                    parent_tables.append(
                        f"{self._quote_identifier(parent_schema)}.{self._quote_identifier(parent_name)}"
                    )
                else:
                    parent_tables.append(self._quote_identifier(parent))
            sql_parts.append(f"INHERITS ({', '.join(parent_tables)})")

        # Add PARTITION BY clause
        if partition_by:
            # Basic validation to prevent SQL injection in PARTITION BY
            if any(dangerous in partition_by.upper() for dangerous in [';', 'DROP', 'DELETE', 'INSERT', 'UPDATE', 'CREATE']):
                raise ValueError("Invalid characters in PARTITION BY expression")
            sql_parts.append(f"PARTITION BY {partition_by}")

        # Add TABLESPACE clause
        if tablespace:
            sql_parts.append(f"TABLESPACE {self._quote_identifier(tablespace)}")

        # Add WITH clause for storage parameters
        if storage_parameters:
            with_parts = []
            for key, value in storage_parameters.items():
                if isinstance(value, str):
                    with_parts.append(f"{key} = '{value}'")
                else:
                    with_parts.append(f"{key} = {value}")
            sql_parts.append(f"WITH ({', '.join(with_parts)})")

        return " ".join(sql_parts), []

    def format_constraint_definition(self, constraint: Dict[str, Any]) -> str:
        """Format a constraint definition for CREATE TABLE.

        Args:
            constraint: Dictionary with constraint properties

        Returns:
            SQL constraint definition string
        """
        parts = []

        # Constraint name (optional)
        if constraint.get("name"):
            parts.append(f"CONSTRAINT {self._quote_identifier(constraint['name'])}")

        constraint_type = constraint.get("type", "").upper()

        if constraint_type == "PRIMARY KEY":
            columns = constraint.get("columns", [])
            if columns:
                col_list = ", ".join(self._quote_identifier(c) for c in columns)
                parts.append(f"PRIMARY KEY ({col_list})")

        elif constraint_type == "UNIQUE":
            columns = constraint.get("columns", [])
            if columns:
                col_list = ", ".join(self._quote_identifier(c) for c in columns)
                parts.append(f"UNIQUE ({col_list})")

        elif constraint_type == "FOREIGN KEY":
            columns = constraint.get("columns", [])
            ref_table = constraint.get("references", {}).get("table")
            ref_columns = constraint.get("references", {}).get("columns", [])

            if columns and ref_table:
                col_list = ", ".join(self._quote_identifier(c) for c in columns)
                parts.append(f"FOREIGN KEY ({col_list})")

                ref_part = f"REFERENCES {self._quote_identifier(ref_table)}"
                if ref_columns:
                    ref_col_list = ", ".join(self._quote_identifier(c) for c in ref_columns)
                    ref_part += f" ({ref_col_list})"

                if on_delete := constraint.get("on_delete"):
                    # Validate ON DELETE action
                    valid_actions = ["CASCADE", "RESTRICT", "SET NULL", "SET DEFAULT", "NO ACTION"]
                    if on_delete.upper() not in valid_actions:
                        raise ValueError(f"Invalid ON DELETE action: {on_delete}")
                    ref_part += f" ON DELETE {on_delete.upper()}"
                if on_update := constraint.get("on_update"):
                    # Validate ON UPDATE action
                    valid_actions = ["CASCADE", "RESTRICT", "SET NULL", "SET DEFAULT", "NO ACTION"]
                    if on_update.upper() not in valid_actions:
                        raise ValueError(f"Invalid ON UPDATE action: {on_update}")
                    ref_part += f" ON UPDATE {on_update.upper()}"

                parts.append(ref_part)

        elif constraint_type == "CHECK":
            if check_expr := constraint.get("check"):
                # Basic validation to prevent SQL injection in CHECK constraints
                if any(dangerous in check_expr.upper() for dangerous in [';', 'DROP', 'DELETE', 'INSERT', 'UPDATE', 'CREATE']):
                    raise ValueError("Invalid characters in CHECK expression")
                parts.append(f"CHECK ({check_expr})")

        elif constraint_type == "EXCLUDE":
            if exclude_expr := constraint.get("exclude"):
                parts.append(f"EXCLUDE {exclude_expr}")

        # Deferrable options
        if constraint.get("deferrable"):
            parts.append("DEFERRABLE")
            if constraint.get("initially_deferred"):
                parts.append("INITIALLY DEFERRED")

        return " ".join(parts)

    async def execute(self, **kwargs) -> OperationResult:
        """Execute the create table operation.

        Args:
            Same as build_create_table_sql

        Returns:
            OperationResult with success status and details
        """
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")
        if_not_exists = kwargs.get("if_not_exists", False)
        comment = kwargs.get("comment")
        indexes = kwargs.get("indexes", [])

        try:
            # Check if table exists (for if_not_exists handling)
            table_exists = await self.check_table_exists(table_name, schema_name)

            if table_exists and if_not_exists:
                return OperationResult(
                    success=True,
                    operation_type=self.get_operation_type(),
                    operation_name="CreateTable",
                    affected_objects=[],
                    message=f"Table '{schema_name}.{table_name}' already exists, skipping",
                    details={"table": f"{schema_name}.{table_name}", "skipped": True}
                )

            # Build SQL statement
            sql, params = self.build_create_table_sql(**kwargs)

            # Execute the statement
            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Create the table
                    await cur.execute(sql, params)

                    # Add comment if provided
                    if comment:
                        comment_sql = f"""
                            COMMENT ON TABLE {self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}
                            IS %s
                        """
                        await cur.execute(comment_sql, (comment,))

                    # Create indexes if provided
                    created_indexes = []
                    for index in indexes:
                        index_name = index.get("name")
                        index_columns = index.get("columns", [])
                        index_type = index.get("type", "btree")
                        unique = index.get("unique", False)
                        where = index.get("where")

                        if not index_columns:
                            continue

                        if not index_name:
                            # Generate index name
                            index_name = f"{table_name}_{'_'.join(index_columns)}_idx"

                        index_sql_parts = ["CREATE"]
                        if unique:
                            index_sql_parts.append("UNIQUE")
                        index_sql_parts.append("INDEX")
                        index_sql_parts.append(self._quote_identifier(index_name))
                        index_sql_parts.append("ON")
                        index_sql_parts.append(f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}")

                        # Index method
                        if index_type.upper() != "BTREE":
                            index_sql_parts.append(f"USING {index_type}")

                        # Columns
                        col_list = ", ".join(
                            self._quote_identifier(c) if isinstance(c, str) else c
                            for c in index_columns
                        )
                        index_sql_parts.append(f"({col_list})")

                        # WHERE clause
                        if where:
                            index_sql_parts.append(f"WHERE {where}")

                        index_sql = " ".join(index_sql_parts)
                        await cur.execute(index_sql)
                        created_indexes.append(index_name)

                    # Get table info for response
                    table_info = await self.get_table_info(table_name, schema_name)

                    return OperationResult(
                        success=True,
                        operation_type=self.get_operation_type(),
                        operation_name="CreateTable",
                        affected_objects=[f"table:{schema_name}.{table_name}"],
                        message=f"Table '{schema_name}.{table_name}' created successfully",
                        details={
                            "table": f"{schema_name}.{table_name}",
                            "columns": len(kwargs.get("columns", [])),
                            "constraints": len(kwargs.get("constraints", [])),
                            "indexes_created": created_indexes,
                            "table_info": table_info
                        }
                    )

        except psycopg.errors.DuplicateTable as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="CreateTable",
                error=f"Table already exists: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )

        except psycopg.errors.UndefinedObject as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="CreateTable",
                error=f"Referenced object does not exist: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )

        except psycopg.errors.InvalidTableDefinition as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="CreateTable",
                error=f"Invalid table definition: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )

        except psycopg.errors.InsufficientPrivilege as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="CreateTable",
                error=f"Insufficient privileges: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )

        except Exception as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="CreateTable",
                error=f"Failed to create table: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )