"""Alter table operation implementation."""

from typing import Optional, Dict, Any, List
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import TableOperation


class AlterTableOperation(TableOperation):
    """Operation to alter an existing database table."""

    def get_required_permissions(self) -> List[Permission]:
        """Get required permissions for altering tables."""
        return [Permission.ALTER_TABLE]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        """Validate alter table operation parameters.

        Args:
            table_name: The table name to alter
            schema_name: The schema name (default: public)
            add_columns: Columns to add
            drop_columns: Columns to drop
            alter_columns: Columns to alter
            rename_columns: Columns to rename (dict of old_name: new_name)
            add_constraints: Constraints to add
            drop_constraints: Constraints to drop
            rename_to: New table name
            set_schema: New schema name
            owner_to: New owner
            set_tablespace: New tablespace
            cluster_on: Index to cluster on
            set_logged: Convert to logged table
            set_unlogged: Convert to unlogged table

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

        # Check if table exists
        if not await self.check_table_exists(table_name, schema_name):
            return False, f"Table '{schema_name}.{table_name}' does not exist"

        # Check if at least one alteration is specified
        has_alteration = any([
            kwargs.get("add_columns"),
            kwargs.get("drop_columns"),
            kwargs.get("alter_columns"),
            kwargs.get("rename_columns"),
            kwargs.get("add_constraints"),
            kwargs.get("drop_constraints"),
            kwargs.get("rename_to"),
            kwargs.get("set_schema"),
            kwargs.get("owner_to"),
            kwargs.get("set_tablespace"),
            kwargs.get("cluster_on"),
            kwargs.get("set_logged") is not None,
            kwargs.get("set_unlogged") is not None,
        ])

        if not has_alteration:
            return False, "No alterations specified"

        # Validate columns to add
        if add_columns := kwargs.get("add_columns", []):
            for column in add_columns:
                if not isinstance(column, dict):
                    return False, "Each column must be a dictionary"

                col_name = column.get("name")
                if not col_name:
                    return False, "Column name is required"

                # Check if column already exists
                if await self.check_column_exists(table_name, col_name, schema_name):
                    return False, f"Column '{col_name}' already exists"

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

        # Validate columns to drop
        if drop_columns := kwargs.get("drop_columns", []):
            for col_name in drop_columns:
                if not await self.check_column_exists(table_name, col_name, schema_name):
                    if not kwargs.get("if_exists", False):
                        return False, f"Column '{col_name}' does not exist"

        # Validate columns to alter
        if alter_columns := kwargs.get("alter_columns", []):
            for alteration in alter_columns:
                col_name = alteration.get("name")
                if not col_name:
                    return False, "Column name is required for alteration"

                if not await self.check_column_exists(table_name, col_name, schema_name):
                    return False, f"Column '{col_name}' does not exist"

        # Validate rename_to
        if rename_to := kwargs.get("rename_to"):
            is_valid, error = self.name_validator.validate_identifier(
                rename_to,
                allow_quoted=False
            )
            if not is_valid:
                return False, f"Invalid new table name: {error}"

            # Check if target name already exists
            if await self.check_table_exists(rename_to, schema_name):
                return False, f"Table '{schema_name}.{rename_to}' already exists"

        # Validate set_schema
        if set_schema := kwargs.get("set_schema"):
            if not await self.check_schema_exists(set_schema):
                return False, f"Schema '{set_schema}' does not exist"

            # Check if table already exists in target schema
            if await self.check_table_exists(table_name, set_schema):
                return False, f"Table '{set_schema}.{table_name}' already exists"

        return True, None

    async def execute(self, **kwargs) -> OperationResult:
        """Execute the alter table operation.

        Args:
            Same as validate method

        Returns:
            OperationResult with success status and details
        """
        table_name = kwargs.get("table_name")
        schema_name = kwargs.get("schema_name", "public")
        alterations_made = []

        try:
            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    full_table_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"

                    # Handle rename first (affects subsequent operations)
                    if rename_to := kwargs.get("rename_to"):
                        sql = f"ALTER TABLE {full_table_name} RENAME TO {self._quote_identifier(rename_to)}"
                        await cur.execute(sql)
                        alterations_made.append(f"Renamed table to '{rename_to}'")
                        table_name = rename_to
                        full_table_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"

                    # Handle schema change
                    if set_schema := kwargs.get("set_schema"):
                        sql = f"ALTER TABLE {full_table_name} SET SCHEMA {self._quote_identifier(set_schema)}"
                        await cur.execute(sql)
                        alterations_made.append(f"Moved to schema '{set_schema}'")
                        schema_name = set_schema
                        full_table_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"

                    # Add columns
                    if add_columns := kwargs.get("add_columns", []):
                        for column in add_columns:
                            col_def = self.format_column_definition(column)
                            sql = f"ALTER TABLE {full_table_name} ADD COLUMN {col_def}"
                            await cur.execute(sql)
                            alterations_made.append(f"Added column '{column['name']}'")

                    # Drop columns
                    if drop_columns := kwargs.get("drop_columns", []):
                        cascade = kwargs.get("cascade", False)
                        if_exists = kwargs.get("if_exists", False)
                        for col_name in drop_columns:
                            sql_parts = [f"ALTER TABLE {full_table_name} DROP COLUMN"]
                            if if_exists:
                                sql_parts.append("IF EXISTS")
                            sql_parts.append(self._quote_identifier(col_name))
                            if cascade:
                                sql_parts.append("CASCADE")
                            sql = " ".join(sql_parts)
                            await cur.execute(sql)
                            alterations_made.append(f"Dropped column '{col_name}'")

                    # Alter columns
                    if alter_columns := kwargs.get("alter_columns", []):
                        for alteration in alter_columns:
                            col_name = alteration["name"]
                            quoted_col = self._quote_identifier(col_name)

                            # Change data type
                            if new_type := alteration.get("type"):
                                sql = f"ALTER TABLE {full_table_name} ALTER COLUMN {quoted_col} TYPE {new_type}"
                                if using := alteration.get("using"):
                                    sql += f" USING {using}"
                                await cur.execute(sql)
                                alterations_made.append(f"Changed type of '{col_name}' to {new_type}")

                            # Set/drop NOT NULL
                            if "not_null" in alteration:
                                if alteration["not_null"]:
                                    sql = f"ALTER TABLE {full_table_name} ALTER COLUMN {quoted_col} SET NOT NULL"
                                else:
                                    sql = f"ALTER TABLE {full_table_name} ALTER COLUMN {quoted_col} DROP NOT NULL"
                                await cur.execute(sql)
                                alterations_made.append(f"Changed NULL constraint on '{col_name}'")

                            # Set/drop DEFAULT
                            if "default" in alteration:
                                if alteration["default"] is not None:
                                    default_val = alteration["default"]
                                    if isinstance(default_val, str):
                                        sql = f"ALTER TABLE {full_table_name} ALTER COLUMN {quoted_col} SET DEFAULT '{default_val}'"
                                    else:
                                        sql = f"ALTER TABLE {full_table_name} ALTER COLUMN {quoted_col} SET DEFAULT {default_val}"
                                else:
                                    sql = f"ALTER TABLE {full_table_name} ALTER COLUMN {quoted_col} DROP DEFAULT"
                                await cur.execute(sql)
                                alterations_made.append(f"Changed default for '{col_name}'")

                    # Rename columns
                    if rename_columns := kwargs.get("rename_columns", {}):
                        for old_name, new_name in rename_columns.items():
                            sql = f"""
                                ALTER TABLE {full_table_name}
                                RENAME COLUMN {self._quote_identifier(old_name)}
                                TO {self._quote_identifier(new_name)}
                            """
                            await cur.execute(sql)
                            alterations_made.append(f"Renamed column '{old_name}' to '{new_name}'")

                    # Add constraints
                    if add_constraints := kwargs.get("add_constraints", []):
                        for constraint in add_constraints:
                            constraint_def = self.format_constraint_definition(constraint)
                            sql = f"ALTER TABLE {full_table_name} ADD {constraint_def}"
                            await cur.execute(sql)
                            constraint_name = constraint.get("name", "unnamed")
                            alterations_made.append(f"Added constraint '{constraint_name}'")

                    # Drop constraints
                    if drop_constraints := kwargs.get("drop_constraints", []):
                        cascade = kwargs.get("cascade", False)
                        if_exists = kwargs.get("if_exists", False)
                        for constraint_name in drop_constraints:
                            sql_parts = [f"ALTER TABLE {full_table_name} DROP CONSTRAINT"]
                            if if_exists:
                                sql_parts.append("IF EXISTS")
                            sql_parts.append(self._quote_identifier(constraint_name))
                            if cascade:
                                sql_parts.append("CASCADE")
                            sql = " ".join(sql_parts)
                            await cur.execute(sql)
                            alterations_made.append(f"Dropped constraint '{constraint_name}'")

                    # Change owner
                    if owner_to := kwargs.get("owner_to"):
                        sql = f"ALTER TABLE {full_table_name} OWNER TO {self._quote_identifier(owner_to)}"
                        await cur.execute(sql)
                        alterations_made.append(f"Changed owner to '{owner_to}'")

                    # Change tablespace
                    if set_tablespace := kwargs.get("set_tablespace"):
                        sql = f"ALTER TABLE {full_table_name} SET TABLESPACE {self._quote_identifier(set_tablespace)}"
                        await cur.execute(sql)
                        alterations_made.append(f"Moved to tablespace '{set_tablespace}'")

                    # Cluster on index
                    if cluster_on := kwargs.get("cluster_on"):
                        sql = f"ALTER TABLE {full_table_name} CLUSTER ON {self._quote_identifier(cluster_on)}"
                        await cur.execute(sql)
                        alterations_made.append(f"Set clustering on index '{cluster_on}'")

                    # Set logged/unlogged
                    if kwargs.get("set_logged"):
                        sql = f"ALTER TABLE {full_table_name} SET LOGGED"
                        await cur.execute(sql)
                        alterations_made.append("Set table to LOGGED")
                    elif kwargs.get("set_unlogged"):
                        sql = f"ALTER TABLE {full_table_name} SET UNLOGGED"
                        await cur.execute(sql)
                        alterations_made.append("Set table to UNLOGGED")

                    # Get updated table info
                    table_info = await self.get_table_info(table_name, schema_name)

                    return OperationResult(
                        success=True,
                        operation_type=self.get_operation_type(),
                        operation_name="AlterTable",
                        affected_objects=[f"table:{schema_name}.{table_name}"],
                        message=f"Table '{schema_name}.{table_name}' altered successfully",
                        details={
                            "table": f"{schema_name}.{table_name}",
                            "alterations": alterations_made,
                            "table_info": table_info
                        }
                    )

        except psycopg.errors.UndefinedTable as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="AlterTable",
                error=f"Table does not exist: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )

        except psycopg.errors.DuplicateColumn as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="AlterTable",
                error=f"Column already exists: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )

        except psycopg.errors.UndefinedColumn as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="AlterTable",
                error=f"Column does not exist: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )

        except psycopg.errors.DependentObjectsStillExist as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="AlterTable",
                error=f"Dependent objects still exist. Use CASCADE option: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )

        except psycopg.errors.InsufficientPrivilege as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="AlterTable",
                error=f"Insufficient privileges: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )

        except Exception as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="AlterTable",
                error=f"Failed to alter table: {str(e)}",
                details={"table": f"{schema_name}.{table_name}"}
            )