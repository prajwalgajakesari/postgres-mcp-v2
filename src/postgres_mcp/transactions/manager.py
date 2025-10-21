"""Transaction management with advanced features like savepoints and two-phase commit."""

import asyncio
import uuid
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any
from enum import Enum
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class IsolationLevel(str, Enum):
    """PostgreSQL transaction isolation levels."""
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"


class TransactionState(str, Enum):
    """Transaction states."""
    IDLE = "idle"
    IN_TRANSACTION = "in_transaction"
    IN_ERROR = "in_error"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    PREPARED = "prepared"  # For two-phase commit


class SavepointManager:
    """Manages savepoints within a transaction."""

    def __init__(self):
        """Initialize savepoint manager."""
        self.savepoints: List[str] = []
        self.savepoint_counter = 0

    def create_savepoint_name(self) -> str:
        """Generate unique savepoint name.

        Returns:
            Unique savepoint name
        """
        self.savepoint_counter += 1
        return f"sp_{self.savepoint_counter}_{uuid.uuid4().hex[:8]}"

    async def create_savepoint(self, conn, name: Optional[str] = None) -> str:
        """Create a new savepoint.

        Args:
            conn: Database connection
            name: Optional savepoint name

        Returns:
            Savepoint name
        """
        savepoint_name = name or self.create_savepoint_name()

        # Ensure savepoint name is valid
        if not savepoint_name.replace('_', '').isalnum():
            raise ValueError(f"Invalid savepoint name: {savepoint_name}")

        await conn.execute(f"SAVEPOINT {savepoint_name}")
        self.savepoints.append(savepoint_name)
        logger.debug(f"Created savepoint: {savepoint_name}")
        return savepoint_name

    async def release_savepoint(self, conn, name: str):
        """Release a savepoint.

        Args:
            conn: Database connection
            name: Savepoint name to release
        """
        if name not in self.savepoints:
            raise ValueError(f"Savepoint {name} not found")

        await conn.execute(f"RELEASE SAVEPOINT {name}")

        # Remove this savepoint and all savepoints created after it
        index = self.savepoints.index(name)
        released = self.savepoints[index:]
        self.savepoints = self.savepoints[:index]

        logger.debug(f"Released savepoint: {name} (and {len(released)-1} nested savepoints)")

    async def rollback_to_savepoint(self, conn, name: str):
        """Rollback to a specific savepoint.

        Args:
            conn: Database connection
            name: Savepoint name to rollback to
        """
        if name not in self.savepoints:
            raise ValueError(f"Savepoint {name} not found")

        await conn.execute(f"ROLLBACK TO SAVEPOINT {name}")

        # Remove all savepoints after this one
        index = self.savepoints.index(name)
        rolled_back = self.savepoints[index + 1:]
        self.savepoints = self.savepoints[:index + 1]

        logger.debug(f"Rolled back to savepoint: {name} (discarded {len(rolled_back)} savepoints)")

    def get_active_savepoints(self) -> List[str]:
        """Get list of active savepoints.

        Returns:
            List of savepoint names
        """
        return self.savepoints.copy()


class TransactionContext:
    """Context object for transaction operations."""

    def __init__(self,
                 connection,
                 savepoint_manager: SavepointManager,
                 transaction_id: str,
                 isolation_level: Optional[IsolationLevel] = None,
                 readonly: bool = False):
        """Initialize transaction context.

        Args:
            connection: Database connection
            savepoint_manager: Savepoint manager instance
            transaction_id: Unique transaction ID
            isolation_level: Transaction isolation level
            readonly: Whether transaction is read-only
        """
        self.connection = connection
        self.savepoint_manager = savepoint_manager
        self.transaction_id = transaction_id
        self.isolation_level = isolation_level
        self.readonly = readonly
        self.start_time = datetime.now(timezone.utc)

    async def savepoint(self, name: Optional[str] = None) -> str:
        """Create a savepoint.

        Args:
            name: Optional savepoint name

        Returns:
            Savepoint name
        """
        return await self.savepoint_manager.create_savepoint(self.connection, name)

    async def release(self, savepoint_name: str):
        """Release a savepoint.

        Args:
            savepoint_name: Name of savepoint to release
        """
        await self.savepoint_manager.release_savepoint(self.connection, savepoint_name)

    async def rollback_to(self, savepoint_name: str):
        """Rollback to a savepoint.

        Args:
            savepoint_name: Name of savepoint to rollback to
        """
        await self.savepoint_manager.rollback_to_savepoint(self.connection, savepoint_name)

    @asynccontextmanager
    async def nested_transaction(self, name: Optional[str] = None):
        """Create a nested transaction using savepoints.

        Args:
            name: Optional savepoint name

        Yields:
            Savepoint name
        """
        sp_name = await self.savepoint(name)
        try:
            yield sp_name
            # If no exception, we could optionally release the savepoint
            # But keeping it might be useful for debugging
        except Exception:
            await self.rollback_to(sp_name)
            raise

    def get_elapsed_time(self) -> float:
        """Get elapsed time since transaction started.

        Returns:
            Elapsed time in seconds
        """
        return (datetime.now(timezone.utc) - self.start_time).total_seconds()


class TransactionManager:
    """Manages database transactions with advanced features."""

    def __init__(self, connection_pool):
        """Initialize transaction manager.

        Args:
            connection_pool: Database connection pool
        """
        self.connection_pool = connection_pool
        self.active_transactions: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    def _generate_transaction_id(self) -> str:
        """Generate unique transaction ID.

        Returns:
            Unique transaction ID
        """
        return f"txn_{uuid.uuid4().hex}"

    @asynccontextmanager
    async def transaction(self,
                         isolation_level: Optional[IsolationLevel] = None,
                         readonly: bool = False,
                         deferrable: bool = False,
                         auto_rollback: bool = True):
        """Create a transaction context.

        Args:
            isolation_level: Transaction isolation level
            readonly: Whether transaction is read-only
            deferrable: Whether transaction is deferrable (only for SERIALIZABLE)
            auto_rollback: Whether to automatically rollback on error

        Yields:
            TransactionContext object
        """
        transaction_id = self._generate_transaction_id()
        savepoint_manager = SavepointManager()
        conn = None

        try:
            # Get connection from pool
            conn = await self.connection_pool.connection()

            # Set transaction characteristics
            if isolation_level:
                await conn.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level.value}")
                logger.debug(f"Set isolation level: {isolation_level.value}")

            characteristics = []
            if readonly:
                characteristics.append("READ ONLY")
            else:
                characteristics.append("READ WRITE")

            if deferrable and isolation_level == IsolationLevel.SERIALIZABLE:
                characteristics.append("DEFERRABLE")

            if characteristics:
                await conn.execute(f"SET TRANSACTION {', '.join(characteristics)}")
                logger.debug(f"Set transaction characteristics: {', '.join(characteristics)}")

            # Start transaction
            await conn.execute("BEGIN")

            # Create context
            context = TransactionContext(
                connection=conn,
                savepoint_manager=savepoint_manager,
                transaction_id=transaction_id,
                isolation_level=isolation_level,
                readonly=readonly
            )

            # Register active transaction
            async with self._lock:
                self.active_transactions[transaction_id] = {
                    "connection": conn,
                    "state": TransactionState.IN_TRANSACTION,
                    "savepoint_manager": savepoint_manager,
                    "isolation_level": isolation_level,
                    "readonly": readonly,
                    "start_time": context.start_time
                }

            logger.info(f"Started transaction {transaction_id}")

            yield context

            # Commit if successful
            await conn.execute("COMMIT")

            async with self._lock:
                if transaction_id in self.active_transactions:
                    self.active_transactions[transaction_id]["state"] = TransactionState.COMMITTED

            logger.info(f"Committed transaction {transaction_id}")

        except Exception as e:
            # Rollback on error
            if conn and auto_rollback:
                try:
                    await conn.execute("ROLLBACK")
                    logger.info(f"Rolled back transaction {transaction_id}")

                    async with self._lock:
                        if transaction_id in self.active_transactions:
                            self.active_transactions[transaction_id]["state"] = TransactionState.ROLLED_BACK
                except Exception as rollback_error:
                    logger.error(f"Failed to rollback transaction {transaction_id}: {rollback_error}")

            logger.error(f"Transaction {transaction_id} failed: {e}")
            raise

        finally:
            # Cleanup
            async with self._lock:
                if transaction_id in self.active_transactions:
                    del self.active_transactions[transaction_id]

            # Return connection to pool
            if conn:
                await self.connection_pool.putconn(conn)

    async def prepare_two_phase(self, transaction_id: str, prepare_id: Optional[str] = None):
        """Prepare a transaction for two-phase commit.

        Args:
            transaction_id: Current transaction ID
            prepare_id: Optional prepare transaction ID

        Returns:
            Prepare transaction ID
        """
        async with self._lock:
            if transaction_id not in self.active_transactions:
                raise ValueError(f"Transaction {transaction_id} not found")

            txn_info = self.active_transactions[transaction_id]
            conn = txn_info["connection"]

        prepare_id = prepare_id or f"prep_{uuid.uuid4().hex}"

        await conn.execute(f"PREPARE TRANSACTION '{prepare_id}'")

        async with self._lock:
            txn_info["state"] = TransactionState.PREPARED
            txn_info["prepare_id"] = prepare_id

        logger.info(f"Prepared transaction {transaction_id} as '{prepare_id}'")
        return prepare_id

    async def commit_prepared(self, prepare_id: str):
        """Commit a prepared transaction.

        Args:
            prepare_id: Prepared transaction ID
        """
        conn = await self.connection_pool.connection()
        try:
            await conn.execute(f"COMMIT PREPARED '{prepare_id}'")
            logger.info(f"Committed prepared transaction '{prepare_id}'")
        finally:
            await self.connection_pool.putconn(conn)

    async def rollback_prepared(self, prepare_id: str):
        """Rollback a prepared transaction.

        Args:
            prepare_id: Prepared transaction ID
        """
        conn = await self.connection_pool.connection()
        try:
            await conn.execute(f"ROLLBACK PREPARED '{prepare_id}'")
            logger.info(f"Rolled back prepared transaction '{prepare_id}'")
        finally:
            await self.connection_pool.putconn(conn)

    async def list_prepared_transactions(self) -> List[Dict[str, Any]]:
        """List all prepared transactions.

        Returns:
            List of prepared transaction information
        """
        conn = await self.connection_pool.connection()
        try:
            cursor = await conn.execute("""
                SELECT gid, prepared, owner, database
                FROM pg_prepared_xacts
            """)
            rows = await cursor.fetchall()

            return [
                {
                    "gid": row[0],
                    "prepared_at": row[1],
                    "owner": row[2],
                    "database": row[3]
                }
                for row in rows
            ]
        finally:
            await self.connection_pool.putconn(conn)

    def get_active_transactions(self) -> List[Dict[str, Any]]:
        """Get information about active transactions.

        Returns:
            List of active transaction information
        """
        with self._lock:
            return [
                {
                    "transaction_id": txn_id,
                    "state": info["state"].value,
                    "isolation_level": info["isolation_level"].value if info["isolation_level"] else None,
                    "readonly": info["readonly"],
                    "start_time": info["start_time"].isoformat(),
                    "elapsed_seconds": (datetime.now(timezone.utc) - info["start_time"]).total_seconds(),
                    "savepoint_count": len(info["savepoint_manager"].get_active_savepoints())
                }
                for txn_id, info in self.active_transactions.items()
            ]

    async def kill_transaction(self, transaction_id: str):
        """Kill an active transaction.

        Args:
            transaction_id: Transaction ID to kill
        """
        async with self._lock:
            if transaction_id not in self.active_transactions:
                raise ValueError(f"Transaction {transaction_id} not found")

            txn_info = self.active_transactions[transaction_id]
            conn = txn_info["connection"]

        try:
            await conn.execute("ROLLBACK")
            logger.info(f"Killed transaction {transaction_id}")
        except Exception as e:
            logger.error(f"Failed to kill transaction {transaction_id}: {e}")
            raise

        async with self._lock:
            if transaction_id in self.active_transactions:
                del self.active_transactions[transaction_id]