"""Index and trigger management tools for PostgreSQL MCP Server."""

import logging
from typing import Dict, Any, List
from mcp.types import Tool, TextContent

from .sql import DbConnPool
from .indexes import CreateIndexOperation, DropIndexOperation, ReindexOperation, ManageTriggersOperation

logger = logging.getLogger(__name__)


def register_index_tools(mcp, db_connection: DbConnPool) -> None:
    """Register index and trigger management tools with the MCP server."""

    # Import common utilities
    from .common_utils import SimpleAuditLogger, create_default_rbac_manager
    rbac_manager = create_default_rbac_manager()

    @mcp.tool()
    async def create_index(
        index_name: str,
        table_name: str,
        columns: List[str],
        schema_name: str = "public",
        index_type: str = "btree",
        unique: bool = False,
        concurrent: bool = False,
        where_clause: str = "",
        include_columns: List[str] = None,
        with_options: Dict[str, Any] = None,
        tablespace: str = "",
        comment: str = ""
    ) -> List[TextContent]:
        """
        Create a new index on a table.

        Args:
            index_name: Name of the index to create
            table_name: Name of the target table
            columns: List of column names to index
            schema_name: Schema name (default: public)
            index_type: Index type (btree, hash, gin, gist, spgist, brin)
            unique: Whether to create a unique index
            concurrent: Whether to create concurrently (for production)
            where_clause: Partial index condition
            include_columns: Additional columns to include in index
            with_options: Index-specific options
            tablespace: Tablespace for the index
            comment: Comment for the index
        """
        try:
            operation = CreateIndexOperation(db_connection, rbac_manager, SimpleAuditLogger())
            result = await operation.execute(
                index_name=index_name,
                table_name=table_name,
                columns=columns,
                schema_name=schema_name,
                index_type=index_type,
                unique=unique,
                concurrent=concurrent,
                where_clause=where_clause,
                include_columns=include_columns or [],
                with_options=with_options or {},
                tablespace=tablespace,
                comment=comment
            )

            if result.success:
                logger.info(f"Index created successfully: {result.message}")
                return [TextContent(type="text", text=f"✅ {result.message}\n\nDetails: {result.details}")]
            else:
                logger.error(f"Failed to create index: {result.error}")
                return [TextContent(type="text", text=f"❌ Error: {result.error}\n\nDetails: {result.details}")]

        except Exception as e:
            logger.error(f"Error creating index: {str(e)}")
            return [TextContent(type="text", text=f"❌ Unexpected error: {str(e)}")]

    @mcp.tool()
    async def drop_index(
        index_name: str,
        schema_name: str = "public",
        concurrent: bool = False,
        if_exists: bool = False,
        cascade: bool = False
    ) -> List[TextContent]:
        """
        Drop an existing index.

        Args:
            index_name: Name of the index to drop
            schema_name: Schema name (default: public)
            concurrent: Whether to drop concurrently (for production)
            if_exists: Don't error if index doesn't exist
            cascade: Drop dependent objects
        """
        try:
            operation = DropIndexOperation(db_connection, rbac_manager, SimpleAuditLogger())
            result = await operation.execute(
                index_name=index_name,
                schema_name=schema_name,
                concurrent=concurrent,
                if_exists=if_exists,
                cascade=cascade
            )

            if result.success:
                logger.info(f"Index dropped successfully: {result.message}")
                return [TextContent(type="text", text=f"✅ {result.message}\n\nDetails: {result.details}")]
            else:
                logger.error(f"Failed to drop index: {result.error}")
                return [TextContent(type="text", text=f"❌ Error: {result.error}\n\nDetails: {result.details}")]

        except Exception as e:
            logger.error(f"Error dropping index: {str(e)}")
            return [TextContent(type="text", text=f"❌ Unexpected error: {str(e)}")]

    @mcp.tool()
    async def reindex(
        target_type: str,
        target_name: str = "",
        schema_name: str = "public",
        concurrent: bool = False,
        verbose: bool = False
    ) -> List[TextContent]:
        """
        Rebuild indexes for performance optimization.

        Args:
            target_type: Type to reindex (INDEX, TABLE, SCHEMA, DATABASE, SYSTEM)
            target_name: Name of the target (required for INDEX, TABLE, SCHEMA)
            schema_name: Schema name (default: public)
            concurrent: Whether to reindex concurrently
            verbose: Show verbose output
        """
        try:
            operation = ReindexOperation(db_connection, rbac_manager, SimpleAuditLogger())
            result = await operation.execute(
                target_type=target_type,
                target_name=target_name,
                schema_name=schema_name,
                concurrent=concurrent,
                verbose=verbose
            )

            if result.success:
                logger.info(f"Reindex completed successfully: {result.message}")
                return [TextContent(type="text", text=f"✅ {result.message}\n\nDetails: {result.details}")]
            else:
                logger.error(f"Failed to reindex: {result.error}")
                return [TextContent(type="text", text=f"❌ Error: {result.error}\n\nDetails: {result.details}")]

        except Exception as e:
            logger.error(f"Error during reindex: {str(e)}")
            return [TextContent(type="text", text=f"❌ Unexpected error: {str(e)}")]

    @mcp.tool()
    async def manage_triggers(
        operation: str,
        trigger_name: str,
        table_name: str,
        schema_name: str = "public",
        function_name: str = "",
        timing: str = "BEFORE",
        events: List[str] = None,
        for_each: str = "ROW",
        when_condition: str = "",
        rename_to: str = "",
        if_exists: bool = False,
        cascade: bool = False
    ) -> List[TextContent]:
        """
        Manage database triggers (CREATE, DROP, ALTER).

        Args:
            operation: Operation to perform (CREATE, DROP, ALTER)
            trigger_name: Name of the trigger
            table_name: Name of the target table
            schema_name: Schema name (default: public)
            function_name: Trigger function name (required for CREATE)
            timing: When to fire trigger (BEFORE, AFTER, INSTEAD OF)
            events: List of events (INSERT, UPDATE, DELETE, TRUNCATE)
            for_each: FOR EACH ROW or FOR EACH STATEMENT
            when_condition: WHEN condition for the trigger
            rename_to: New name for ALTER operation
            if_exists: Don't error if trigger doesn't exist (DROP)
            cascade: Drop dependent objects (DROP)
        """
        try:
            operation_obj = ManageTriggersOperation(db_connection, rbac_manager, SimpleAuditLogger())
            result = await operation_obj.execute(
                operation=operation,
                trigger_name=trigger_name,
                table_name=table_name,
                schema_name=schema_name,
                function_name=function_name,
                timing=timing,
                events=events or [],
                for_each=for_each,
                when_condition=when_condition,
                rename_to=rename_to,
                if_exists=if_exists,
                cascade=cascade
            )

            if result.success:
                logger.info(f"Trigger operation completed: {result.message}")
                return [TextContent(type="text", text=f"✅ {result.message}\n\nDetails: {result.details}")]
            else:
                logger.error(f"Failed to manage trigger: {result.error}")
                return [TextContent(type="text", text=f"❌ Error: {result.error}\n\nDetails: {result.details}")]

        except Exception as e:
            logger.error(f"Error managing trigger: {str(e)}")
            return [TextContent(type="text", text=f"❌ Unexpected error: {str(e)}")]

    # Register the tools with proper metadata
    mcp.tool(
        name="create_index",
        description="Create a new index on a table with comprehensive options including unique, partial, and covering indexes"
    )(create_index)

    mcp.tool(
        name="drop_index",
        description="Drop an existing index with options for concurrent dropping and cascade"
    )(drop_index)

    mcp.tool(
        name="reindex",
        description="Rebuild indexes for performance optimization on tables, schemas, or entire database"
    )(reindex)

    mcp.tool(
        name="manage_triggers",
        description="Comprehensive trigger management including CREATE, DROP, and ALTER operations"
    )(manage_triggers)

    logger.info("Index and trigger management tools registered successfully")