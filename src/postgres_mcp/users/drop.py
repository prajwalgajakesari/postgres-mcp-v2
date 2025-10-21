"""Drop user operation implementation."""

from typing import Optional, Dict, Any, List
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import UserOperation


class DropUserOperation(UserOperation):
    """Operation to drop a database user or role."""

    def get_required_permissions(self) -> List[Permission]:
        """Get required permissions for dropping users."""
        return [Permission.DROP_USER]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        """Validate drop user operation parameters.

        Args:
            username: The username to drop
            cascade: Whether to drop owned objects
            reassign_to: User to reassign ownership to
            if_exists: Don't error if user doesn't exist

        Returns:
            Tuple of (is_valid, error_message)
        """
        username = kwargs.get("username")

        if not username:
            return False, "Username is required"

        # Validate identifier format
        is_valid, error = self.name_validator.validate_identifier(
            username,
            allow_quoted=False
        )
        if not is_valid:
            return False, f"Invalid username: {error}"

        # Check if user exists (unless if_exists is True)
        if not kwargs.get("if_exists", False):
            if not await self.check_user_exists(username):
                return False, f"User '{username}' does not exist"

        # Validate reassign_to if provided
        if reassign_to := kwargs.get("reassign_to"):
            is_valid, error = self.name_validator.validate_identifier(
                reassign_to,
                allow_quoted=False
            )
            if not is_valid:
                return False, f"Invalid reassign_to username: {error}"

            # Check if target user exists
            if not await self.check_user_exists(reassign_to):
                return False, f"User '{reassign_to}' does not exist"

            # Can't reassign to the same user
            if reassign_to.lower() == username.lower():
                return False, "Cannot reassign objects to the same user being dropped"

        # Can't use both CASCADE and reassign_to
        if kwargs.get("cascade") and kwargs.get("reassign_to"):
            return False, "Cannot use both CASCADE and reassign_to options"

        return True, None

    async def check_active_connections(self, username: str) -> int:
        """Check if user has active connections.

        Args:
            username: The username to check

        Returns:
            Number of active connections
        """
        query = """
            SELECT COUNT(*)
            FROM pg_stat_activity
            WHERE usename = %s
        """

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (username,))
                result = await cur.fetchone()
                return result[0] if result else 0

    async def terminate_user_connections(self, username: str) -> int:
        """Terminate all connections for a user.

        Args:
            username: The username whose connections to terminate

        Returns:
            Number of connections terminated
        """
        query = """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE usename = %s
            AND pid != pg_backend_pid()
        """

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (username,))
                results = await cur.fetchall()
                return len([r for r in results if r[0]])

    async def execute(self, **kwargs) -> OperationResult:
        """Execute the drop user operation.

        Args:
            username: The username to drop
            cascade: Drop owned objects (CASCADE)
            reassign_to: User to reassign ownership to
            if_exists: Don't error if user doesn't exist
            force: Terminate active connections before dropping

        Returns:
            OperationResult with success status and details
        """
        username = kwargs.get("username")
        cascade = kwargs.get("cascade", False)
        reassign_to = kwargs.get("reassign_to")
        if_exists = kwargs.get("if_exists", False)
        force = kwargs.get("force", False)

        try:
            # Check if user exists (for if_exists handling)
            user_exists = await self.check_user_exists(username)

            if not user_exists and if_exists:
                return OperationResult(
                    success=True,
                    operation_type=self.get_operation_type(),
                    operation_name="DropUser",
                    affected_objects=[],
                    message=f"User '{username}' does not exist, skipping",
                    details={"username": username, "skipped": True}
                )

            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Check for active connections
                    active_connections = await self.check_active_connections(username)

                    if active_connections > 0:
                        if force:
                            # Terminate connections
                            terminated = await self.terminate_user_connections(username)
                            details_msg = f"Terminated {terminated} connections"
                        else:
                            return OperationResult(
                                success=False,
                                operation_type=self.get_operation_type(),
                                operation_name="DropUser",
                                error=f"User has {active_connections} active connections. Use force=True to terminate them.",
                                details={
                                    "username": username,
                                    "active_connections": active_connections
                                }
                            )
                    else:
                        details_msg = "No active connections"

                    # Get owned objects before dropping
                    owned_objects = await self.get_owned_objects(username)
                    total_owned = sum(len(v) for v in owned_objects.values())

                    # Handle ownership reassignment
                    if reassign_to:
                        reassign_sql = f"""
                            REASSIGN OWNED BY {self._quote_identifier(username)}
                            TO {self._quote_identifier(reassign_to)}
                        """
                        await cur.execute(reassign_sql)
                        details_msg += f", reassigned {total_owned} objects to '{reassign_to}'"

                    # Handle cascade/drop owned objects
                    if cascade and not reassign_to:
                        drop_owned_sql = f"DROP OWNED BY {self._quote_identifier(username)}"
                        await cur.execute(drop_owned_sql)
                        details_msg += f", dropped {total_owned} owned objects"

                    # Build DROP USER statement
                    sql_parts = ["DROP USER"]

                    if if_exists:
                        sql_parts.append("IF EXISTS")

                    sql_parts.append(self._quote_identifier(username))

                    if cascade and not reassign_to:
                        sql_parts.append("CASCADE")

                    sql = " ".join(sql_parts)

                    # Execute DROP USER
                    await cur.execute(sql)

                    return OperationResult(
                        success=True,
                        operation_type=self.get_operation_type(),
                        operation_name="DropUser",
                        affected_objects=[f"user:{username}"],
                        message=f"User '{username}' dropped successfully",
                        details={
                            "username": username,
                            "cascade": cascade,
                            "reassign_to": reassign_to,
                            "owned_objects": owned_objects,
                            "details": details_msg
                        }
                    )

        except psycopg.errors.UndefinedObject as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="DropUser",
                error=f"User does not exist: {str(e)}",
                details={"username": username}
            )

        except psycopg.errors.DependentObjectsStillExist as e:
            # Get list of dependent objects
            owned = await self.get_owned_objects(username)

            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="DropUser",
                error=f"User owns database objects. Use CASCADE or reassign_to option: {str(e)}",
                details={
                    "username": username,
                    "owned_objects": owned
                }
            )

        except psycopg.errors.InsufficientPrivilege as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="DropUser",
                error=f"Insufficient privileges: {str(e)}",
                details={"username": username}
            )

        except Exception as e:
            return OperationResult(
                success=False,
                operation_type=self.get_operation_type(),
                operation_name="DropUser",
                error=f"Failed to drop user: {str(e)}",
                details={"username": username}
            )