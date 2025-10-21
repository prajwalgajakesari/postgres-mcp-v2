"""Database management operations for PostgreSQL."""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Tuple

from .base import DatabaseOperation, DatabaseOperationResult, DatabaseInfo
from ..permissions.rbac import Permission

logger = logging.getLogger(__name__)


class ManageDatabaseOperation(DatabaseOperation):
    """Handles database management operations like listing, analyzing, and maintenance."""

    async def list_databases(self,
                           role_name: str = "default",
                           include_system: bool = False,
                           include_templates: bool = False,
                           include_statistics: bool = True,
                           order_by: str = "name") -> DatabaseOperationResult:
        """List all databases with detailed information.

        Args:
            role_name: Role for permission checking
            include_system: Whether to include system databases
            include_templates: Whether to include template databases
            include_statistics: Whether to include database statistics
            order_by: Order results by (name, size, owner, created)

        Returns:
            DatabaseOperationResult with database listing
        """
        try:
            self._validate_permissions(role_name, [Permission.VIEW_STATS])

            async with self.connection_pool.acquire() as connection:
                databases = []

                # Get basic database information
                query = """
                    SELECT
                        d.datname as name,
                        pg_catalog.pg_get_userbyid(d.datdba) as owner,
                        pg_catalog.pg_encoding_to_char(d.encoding) as encoding,
                        d.datcollate as collate,
                        d.datctype as ctype,
                        d.datconnlimit as connection_limit,
                        d.datistemplate as is_template,
                        d.datallowconn as allow_connections,
                        t.spcname as tablespace
                    FROM pg_catalog.pg_database d
                    LEFT JOIN pg_catalog.pg_tablespace t ON d.dattablespace = t.oid
                    WHERE 1=1
                """

                params = []

                # Apply filters
                if not include_system:
                    query += " AND d.datname NOT IN ('postgres', 'template0', 'template1')"

                if not include_templates:
                    query += " AND NOT d.datistemplate"

                # Add ordering
                if order_by == "name":
                    query += " ORDER BY d.datname"
                elif order_by == "owner":
                    query += " ORDER BY pg_catalog.pg_get_userbyid(d.datdba), d.datname"
                elif order_by == "size":
                    query += " ORDER BY pg_database_size(d.datname) DESC"
                else:
                    query += " ORDER BY d.datname"

                db_rows = await connection.fetch(query, *params)

                for row in db_rows:
                    db_name = row['name']

                    # Get database size
                    size_bytes = await self._get_database_size(connection, db_name)

                    # Create basic database info
                    db_info = {
                        "name": db_name,
                        "owner": row['owner'],
                        "encoding": row['encoding'],
                        "collate": row['collate'],
                        "ctype": row['ctype'],
                        "size_bytes": size_bytes,
                        "size_mb": round(size_bytes / (1024 * 1024), 2) if size_bytes else 0,
                        "connection_limit": row['connection_limit'],
                        "is_template": row['is_template'],
                        "allow_connections": row['allow_connections'],
                        "tablespace": row['tablespace']
                    }

                    # Add statistics if requested
                    if include_statistics:
                        try:
                            stats_query = """
                                SELECT
                                    numbackends,
                                    xact_commit,
                                    xact_rollback,
                                    blks_read,
                                    blks_hit,
                                    tup_returned,
                                    tup_fetched,
                                    tup_inserted,
                                    tup_updated,
                                    tup_deleted,
                                    conflicts,
                                    temp_files,
                                    temp_bytes,
                                    deadlocks,
                                    stats_reset
                                FROM pg_stat_database
                                WHERE datname = $1
                            """

                            stats = await connection.fetchrow(stats_query, db_name)

                            if stats:
                                db_info.update({
                                    "connections": stats['numbackends'],
                                    "transactions_committed": stats['xact_commit'],
                                    "transactions_rolled_back": stats['xact_rollback'],
                                    "blocks_read": stats['blks_read'],
                                    "blocks_hit": stats['blks_hit'],
                                    "cache_hit_ratio": round(
                                        (stats['blks_hit'] / max(stats['blks_hit'] + stats['blks_read'], 1)) * 100, 2
                                    ) if stats['blks_hit'] else 0,
                                    "tuples_returned": stats['tup_returned'],
                                    "tuples_fetched": stats['tup_fetched'],
                                    "tuples_inserted": stats['tup_inserted'],
                                    "tuples_updated": stats['tup_updated'],
                                    "tuples_deleted": stats['tup_deleted'],
                                    "conflicts": stats['conflicts'],
                                    "temp_files": stats['temp_files'],
                                    "temp_bytes": stats['temp_bytes'],
                                    "deadlocks": stats['deadlocks'],
                                    "stats_reset": stats['stats_reset'].isoformat() if stats['stats_reset'] else None
                                })

                        except Exception as e:
                            logger.warning(f"Failed to get statistics for database {db_name}: {e}")

                    databases.append(db_info)

                await self.audit_logger.audit_log(
                    operation="list_databases",
                    role=role_name,
                    details={
                        "databases_found": len(databases),
                        "include_system": include_system,
                        "include_templates": include_templates,
                        "include_statistics": include_statistics
                    }
                )

                return DatabaseOperationResult(
                    success=True,
                    message=f"Found {len(databases)} databases",
                    affected_databases=[db['name'] for db in databases]
                )

        except Exception as e:
            logger.error(f"Error listing databases: {e}")
            return DatabaseOperationResult(
                success=False,
                message=f"Failed to list databases: {e}",
                errors=[str(e)]
            )

    async def analyze_database(self,
                             database_name: str,
                             role_name: str = "default",
                             include_table_stats: bool = True,
                             include_index_stats: bool = True,
                             include_query_stats: bool = True) -> DatabaseOperationResult:
        """Analyze database performance and statistics.

        Args:
            database_name: Name of the database to analyze
            role_name: Role for permission checking
            include_table_stats: Whether to include table statistics
            include_index_stats: Whether to include index statistics
            include_query_stats: Whether to include query statistics

        Returns:
            DatabaseOperationResult with analysis details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.VIEW_STATS, Permission.ANALYZE])
            self._validate_identifier(database_name)

            async with self.connection_pool.acquire() as connection:
                if not await self._database_exists(connection, database_name):
                    return DatabaseOperationResult(
                        success=False,
                        message=f"Database '{database_name}' does not exist",
                        duration_seconds=time.time() - start_time,
                        errors=[f"Database '{database_name}' not found"]
                    )

                analysis = {
                    "database_name": database_name,
                    "analysis_time": time.time(),
                    "database_info": {},
                    "table_stats": [],
                    "index_stats": [],
                    "query_stats": [],
                    "recommendations": []
                }

                # Get detailed database information
                db_info = await self._get_database_info(connection, database_name)
                if db_info:
                    analysis["database_info"] = db_info.to_dict()

                # Get table statistics
                if include_table_stats:
                    table_stats = await self._get_table_statistics(connection, database_name)
                    analysis["table_stats"] = table_stats

                # Get index statistics
                if include_index_stats:
                    index_stats = await self._get_index_statistics(connection, database_name)
                    analysis["index_stats"] = index_stats

                # Get query statistics (requires pg_stat_statements)
                if include_query_stats:
                    query_stats = await self._get_query_statistics(connection)
                    analysis["query_stats"] = query_stats

                # Generate recommendations
                recommendations = await self._generate_recommendations(analysis)
                analysis["recommendations"] = recommendations

                duration = time.time() - start_time

                await self.audit_logger.audit_log(
                    operation="analyze_database",
                    role=role_name,
                    details={
                        "database_name": database_name,
                        "analysis_duration": duration,
                        "recommendations_count": len(recommendations)
                    }
                )

                return DatabaseOperationResult(
                    success=True,
                    message=f"Database analysis completed for '{database_name}'",
                    database_info=db_info,
                    duration_seconds=duration
                )

        except Exception as e:
            logger.error(f"Error analyzing database: {e}")
            return DatabaseOperationResult(
                success=False,
                message=f"Failed to analyze database: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def vacuum_database(self,
                            database_name: str,
                            role_name: str = "default",
                            full: bool = False,
                            analyze: bool = True,
                            verbose: bool = False,
                            tables: Optional[List[str]] = None) -> DatabaseOperationResult:
        """Perform VACUUM operation on database.

        Args:
            database_name: Name of the database
            role_name: Role for permission checking
            full: Whether to perform VACUUM FULL
            analyze: Whether to perform ANALYZE after VACUUM
            verbose: Whether to use verbose output
            tables: Optional list of specific tables to vacuum

        Returns:
            DatabaseOperationResult with vacuum details
        """
        start_time = time.time()

        try:
            self._validate_permissions(role_name, [Permission.VACUUM])
            self._validate_identifier(database_name)

            async with self.connection_pool.acquire() as connection:
                if not await self._database_exists(connection, database_name):
                    return DatabaseOperationResult(
                        success=False,
                        message=f"Database '{database_name}' does not exist",
                        duration_seconds=time.time() - start_time,
                        errors=[f"Database '{database_name}' not found"]
                    )

                # Note: VACUUM commands cannot run inside a transaction block
                vacuum_results = []

                if tables:
                    # Vacuum specific tables
                    for table in tables:
                        self._validate_identifier(table)
                        try:
                            vacuum_cmd = "VACUUM"
                            if full:
                                vacuum_cmd += " FULL"
                            if analyze:
                                vacuum_cmd += " ANALYZE"
                            if verbose:
                                vacuum_cmd += " VERBOSE"

                            vacuum_cmd += f" {self._quote_identifier(table)}"

                            logger.info(f"Running vacuum on table {table}")
                            await connection.execute(vacuum_cmd)
                            vacuum_results.append(f"Successfully vacuumed table: {table}")

                        except Exception as e:
                            error_msg = f"Failed to vacuum table {table}: {e}"
                            vacuum_results.append(error_msg)
                            logger.error(error_msg)

                else:
                    # Vacuum entire database
                    try:
                        vacuum_cmd = "VACUUM"
                        if full:
                            vacuum_cmd += " FULL"
                        if analyze:
                            vacuum_cmd += " ANALYZE"
                        if verbose:
                            vacuum_cmd += " VERBOSE"

                        logger.info(f"Running vacuum on database {database_name}")
                        await connection.execute(vacuum_cmd)
                        vacuum_results.append(f"Successfully vacuumed database: {database_name}")

                    except Exception as e:
                        error_msg = f"Failed to vacuum database: {e}"
                        vacuum_results.append(error_msg)
                        logger.error(error_msg)
                        return DatabaseOperationResult(
                            success=False,
                            message=error_msg,
                            duration_seconds=time.time() - start_time,
                            errors=[error_msg]
                        )

                duration = time.time() - start_time

                await self.audit_logger.audit_log(
                    operation="vacuum_database",
                    role=role_name,
                    details={
                        "database_name": database_name,
                        "full": full,
                        "analyze": analyze,
                        "tables": tables,
                        "duration_seconds": duration
                    }
                )

                return DatabaseOperationResult(
                    success=True,
                    message=f"Vacuum operation completed on '{database_name}'",
                    duration_seconds=duration,
                    warnings=vacuum_results if len(vacuum_results) > 1 else []
                )

        except Exception as e:
            logger.error(f"Error vacuuming database: {e}")
            return DatabaseOperationResult(
                success=False,
                message=f"Failed to vacuum database: {e}",
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def get_database_activity(self,
                                  database_name: str,
                                  role_name: str = "default") -> DatabaseOperationResult:
        """Get current database activity and connections.

        Args:
            database_name: Name of the database
            role_name: Role for permission checking

        Returns:
            DatabaseOperationResult with activity details
        """
        try:
            self._validate_permissions(role_name, [Permission.VIEW_ACTIVITY])
            self._validate_identifier(database_name)

            async with self.connection_pool.acquire() as connection:
                if not await self._database_exists(connection, database_name):
                    return DatabaseOperationResult(
                        success=False,
                        message=f"Database '{database_name}' does not exist",
                        errors=[f"Database '{database_name}' not found"]
                    )

                # Get detailed activity information
                activity_query = """
                    SELECT
                        pid,
                        usename,
                        application_name,
                        client_addr,
                        client_port,
                        backend_start,
                        xact_start,
                        query_start,
                        state_change,
                        state,
                        backend_xmin,
                        query,
                        wait_event_type,
                        wait_event
                    FROM pg_stat_activity
                    WHERE datname = $1
                    ORDER BY backend_start
                """

                activity_rows = await connection.fetch(activity_query, database_name)

                connections = []
                for row in activity_rows:
                    conn_info = {
                        "pid": row['pid'],
                        "user": row['usename'],
                        "application": row['application_name'],
                        "client_address": str(row['client_addr']) if row['client_addr'] else None,
                        "client_port": row['client_port'],
                        "backend_start": row['backend_start'].isoformat() if row['backend_start'] else None,
                        "transaction_start": row['xact_start'].isoformat() if row['xact_start'] else None,
                        "query_start": row['query_start'].isoformat() if row['query_start'] else None,
                        "state_change": row['state_change'].isoformat() if row['state_change'] else None,
                        "state": row['state'],
                        "backend_xmin": row['backend_xmin'],
                        "current_query": row['query'],
                        "wait_event_type": row['wait_event_type'],
                        "wait_event": row['wait_event']
                    }
                    connections.append(conn_info)

                # Get summary statistics
                summary = {
                    "total_connections": len(connections),
                    "active_connections": len([c for c in connections if c['state'] == 'active']),
                    "idle_connections": len([c for c in connections if c['state'] == 'idle']),
                    "idle_in_transaction": len([c for c in connections if c['state'] == 'idle in transaction']),
                    "waiting_connections": len([c for c in connections if c['wait_event'] is not None])
                }

                activity_info = {
                    "database_name": database_name,
                    "summary": summary,
                    "connections": connections
                }

                await self.audit_logger.audit_log(
                    operation="get_database_activity",
                    role=role_name,
                    details={
                        "database_name": database_name,
                        "total_connections": summary["total_connections"],
                        "active_connections": summary["active_connections"]
                    }
                )

                return DatabaseOperationResult(
                    success=True,
                    message=f"Retrieved activity for database '{database_name}' ({summary['total_connections']} connections)",
                )

        except Exception as e:
            logger.error(f"Error getting database activity: {e}")
            return DatabaseOperationResult(
                success=False,
                message=f"Failed to get database activity: {e}",
                errors=[str(e)]
            )

    async def _get_table_statistics(self, connection, database_name: str) -> List[Dict[str, Any]]:
        """Get table statistics for the database."""
        try:
            query = """
                SELECT
                    schemaname,
                    tablename,
                    n_tup_ins as inserts,
                    n_tup_upd as updates,
                    n_tup_del as deletes,
                    n_tup_hot_upd as hot_updates,
                    n_live_tup as live_tuples,
                    n_dead_tup as dead_tuples,
                    last_vacuum,
                    last_autovacuum,
                    last_analyze,
                    last_autoanalyze,
                    vacuum_count,
                    autovacuum_count,
                    analyze_count,
                    autoanalyze_count
                FROM pg_stat_user_tables
                ORDER BY n_live_tup DESC
                LIMIT 50
            """

            rows = await connection.fetch(query)
            return [dict(row) for row in rows]

        except Exception as e:
            logger.warning(f"Failed to get table statistics: {e}")
            return []

    async def _get_index_statistics(self, connection, database_name: str) -> List[Dict[str, Any]]:
        """Get index statistics for the database."""
        try:
            query = """
                SELECT
                    schemaname,
                    tablename,
                    indexname,
                    idx_scan as scans,
                    idx_tup_read as tuples_read,
                    idx_tup_fetch as tuples_fetched,
                    pg_size_pretty(pg_relation_size(indexrelid)) as size
                FROM pg_stat_user_indexes
                ORDER BY idx_scan DESC
                LIMIT 50
            """

            rows = await connection.fetch(query)
            return [dict(row) for row in rows]

        except Exception as e:
            logger.warning(f"Failed to get index statistics: {e}")
            return []

    async def _get_query_statistics(self, connection) -> List[Dict[str, Any]]:
        """Get query statistics if pg_stat_statements is available."""
        try:
            # Check if pg_stat_statements extension is installed
            ext_query = "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'"
            if not await connection.fetchval(ext_query):
                return []

            query = """
                SELECT
                    query,
                    calls,
                    total_time,
                    mean_time,
                    min_time,
                    max_time,
                    stddev_time,
                    rows,
                    shared_blks_hit,
                    shared_blks_read,
                    shared_blks_dirtied,
                    shared_blks_written
                FROM pg_stat_statements
                ORDER BY total_time DESC
                LIMIT 20
            """

            rows = await connection.fetch(query)
            return [dict(row) for row in rows]

        except Exception as e:
            logger.warning(f"Failed to get query statistics: {e}")
            return []

    async def _generate_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate performance recommendations based on analysis."""
        recommendations = []

        try:
            db_info = analysis.get("database_info", {})
            table_stats = analysis.get("table_stats", [])
            index_stats = analysis.get("index_stats", [])

            # Database size recommendations
            size_bytes = db_info.get("size_bytes", 0)
            if size_bytes > 10 * 1024 * 1024 * 1024:  # > 10GB
                recommendations.append("Consider archiving old data or partitioning large tables")

            # Cache hit ratio recommendations
            cache_hit_ratio = db_info.get("statistics", {}).get("blocks", {})
            if cache_hit_ratio:
                hit = cache_hit_ratio.get("hit", 0)
                read = cache_hit_ratio.get("read", 0)
                if hit + read > 0:
                    ratio = (hit / (hit + read)) * 100
                    if ratio < 95:
                        recommendations.append(f"Low cache hit ratio ({ratio:.1f}%) - consider increasing shared_buffers")

            # Dead tuple recommendations
            for table in table_stats[:10]:  # Check top 10 tables
                dead_tuples = table.get("dead_tuples", 0)
                live_tuples = table.get("live_tuples", 1)
                if dead_tuples > live_tuples * 0.1:  # > 10% dead tuples
                    recommendations.append(f"Table {table.get('tablename')} has high dead tuple ratio - consider VACUUM")

            # Unused index recommendations
            for index in index_stats:
                scans = index.get("scans", 0)
                if scans == 0:
                    recommendations.append(f"Index {index.get('indexname')} appears unused - consider dropping")

        except Exception as e:
            logger.warning(f"Failed to generate some recommendations: {e}")

        return recommendations[:10]  # Limit to top 10 recommendations

    async def execute(self, operation_type: str, **kwargs) -> DatabaseOperationResult:
        """Execute the specified database management operation."""
        if operation_type == "list":
            return await self.list_databases(**kwargs)
        elif operation_type == "analyze":
            return await self.analyze_database(**kwargs)
        elif operation_type == "vacuum":
            return await self.vacuum_database(**kwargs)
        elif operation_type == "activity":
            return await self.get_database_activity(**kwargs)
        else:
            raise ValueError(f"Unknown database management operation: {operation_type}")