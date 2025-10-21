"""Role-Based Access Control (RBAC) implementation for PostgreSQL operations."""

from enum import Enum
from typing import Dict, Set, Optional, List
from dataclasses import dataclass, field
import json
import logging

logger = logging.getLogger(__name__)


class Permission(str, Enum):
    """Database operation permissions."""

    # DDL Permissions
    CREATE_TABLE = "create_table"
    ALTER_TABLE = "alter_table"
    DROP_TABLE = "drop_table"
    CREATE_INDEX = "create_index"
    DROP_INDEX = "drop_index"
    CREATE_SCHEMA = "create_schema"
    DROP_SCHEMA = "drop_schema"

    # DML Permissions
    SELECT = "select"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    TRUNCATE = "truncate"

    # User Management
    CREATE_USER = "create_user"
    ALTER_USER = "alter_user"
    DROP_USER = "drop_user"
    GRANT = "grant"
    REVOKE = "revoke"

    # Maintenance
    VACUUM = "vacuum"
    ANALYZE = "analyze"
    REINDEX = "reindex"
    CLUSTER = "cluster"

    # Backup
    BACKUP = "backup"
    RESTORE = "restore"

    # Monitoring
    VIEW_ACTIVITY = "view_activity"
    VIEW_LOCKS = "view_locks"
    VIEW_STATS = "view_stats"
    EXPLAIN = "explain"

    # Extensions
    CREATE_EXTENSION = "create_extension"
    DROP_EXTENSION = "drop_extension"

    # Functions
    CREATE_FUNCTION = "create_function"
    DROP_FUNCTION = "drop_function"
    EXECUTE_FUNCTION = "execute_function"

    # Triggers
    CREATE_TRIGGER = "create_trigger"
    DROP_TRIGGER = "drop_trigger"

    # Data Operations (Phase 3)
    COPY_DATA = "copy_data"
    EXPORT_DATA = "export_data"
    IMPORT_DATA = "import_data"
    FILE_ACCESS = "file_access"

    # Backup & Restore (Phase 3)
    CREATE_BACKUP = "create_backup"
    RESTORE_BACKUP = "restore_backup"
    MANAGE_BACKUPS = "manage_backups"

    # Database Management (Phase 3)
    CREATE_DATABASE = "create_database"
    DROP_DATABASE = "drop_database"
    CLONE_DATABASE = "clone_database"

    # Partition Management (Phase 3)
    CREATE_PARTITION = "create_partition"
    MANAGE_PARTITIONS = "manage_partitions"

    # Configuration
    SHOW_CONFIG = "show_config"
    SET_CONFIG = "set_config"
    ALTER_SYSTEM = "alter_system"


@dataclass
class Role:
    """Role definition with permissions."""

    name: str
    permissions: Set[Permission] = field(default_factory=set)
    inherit_from: Optional[List[str]] = None
    description: Optional[str] = None
    is_system: bool = False

    def to_dict(self) -> Dict:
        """Convert role to dictionary."""
        return {
            "name": self.name,
            "permissions": [p.value for p in self.permissions],
            "inherit_from": self.inherit_from,
            "description": self.description,
            "is_system": self.is_system
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Role':
        """Create role from dictionary."""
        return cls(
            name=data["name"],
            permissions={Permission(p) for p in data.get("permissions", [])},
            inherit_from=data.get("inherit_from"),
            description=data.get("description"),
            is_system=data.get("is_system", False)
        )


class RBACManager:
    """Manages role-based access control."""

    def __init__(self, config_file: Optional[str] = None):
        """Initialize RBAC manager.

        Args:
            config_file: Optional path to JSON file with custom roles
        """
        self.roles: Dict[str, Role] = {}
        self._initialize_system_roles()

        if config_file:
            self._load_custom_roles(config_file)

    def _initialize_system_roles(self):
        """Initialize default system roles."""

        # Read-only role
        self.roles["readonly"] = Role(
            name="readonly",
            permissions={
                Permission.SELECT,
                Permission.VIEW_STATS,
                Permission.EXPLAIN,
                Permission.SHOW_CONFIG
            },
            description="Read-only access to database",
            is_system=True
        )

        # Developer role
        self.roles["developer"] = Role(
            name="developer",
            permissions={
                Permission.SELECT,
                Permission.INSERT,
                Permission.UPDATE,
                Permission.DELETE,
                Permission.CREATE_TABLE,
                Permission.ALTER_TABLE,
                Permission.DROP_TABLE,
                Permission.CREATE_INDEX,
                Permission.DROP_INDEX,
                Permission.VIEW_ACTIVITY,
                Permission.VIEW_STATS,
                Permission.VIEW_LOCKS,
                Permission.EXPLAIN,
                Permission.CREATE_FUNCTION,
                Permission.DROP_FUNCTION,
                Permission.EXECUTE_FUNCTION,
                Permission.CREATE_TRIGGER,
                Permission.DROP_TRIGGER,
                Permission.COPY_DATA,
                Permission.EXPORT_DATA,
                Permission.IMPORT_DATA,
                Permission.FILE_ACCESS,
                Permission.CREATE_BACKUP,
                Permission.RESTORE_BACKUP,
                Permission.MANAGE_BACKUPS,
                Permission.CREATE_DATABASE,
                Permission.DROP_DATABASE,
                Permission.CLONE_DATABASE,
                Permission.CREATE_PARTITION,
                Permission.MANAGE_PARTITIONS,
                Permission.SHOW_CONFIG
            },
            description="Developer with DDL and DML permissions",
            is_system=True
        )

        # DBA role - full access
        self.roles["dba"] = Role(
            name="dba",
            permissions=set(Permission),  # All permissions
            description="Database administrator with full access",
            is_system=True
        )

        # Analyst role
        self.roles["analyst"] = Role(
            name="analyst",
            permissions={
                Permission.SELECT,
                Permission.VIEW_STATS,
                Permission.VIEW_ACTIVITY,
                Permission.EXPLAIN,
                Permission.EXECUTE_FUNCTION,
                Permission.SHOW_CONFIG
            },
            description="Data analyst with read and monitoring permissions",
            is_system=True
        )

        # Operator role
        self.roles["operator"] = Role(
            name="operator",
            permissions={
                Permission.SELECT,
                Permission.VIEW_STATS,
                Permission.VIEW_ACTIVITY,
                Permission.VIEW_LOCKS,
                Permission.VACUUM,
                Permission.ANALYZE,
                Permission.REINDEX,
                Permission.BACKUP,
                Permission.SHOW_CONFIG
            },
            description="Operations team member with maintenance permissions",
            is_system=True
        )

        logger.info(f"Initialized {len(self.roles)} system roles")

    def _load_custom_roles(self, config_file: str):
        """Load custom roles from configuration file.

        Args:
            config_file: Path to JSON configuration file
        """
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)

            for role_data in config.get("roles", []):
                role = Role.from_dict(role_data)
                if not role.is_system:  # Don't override system roles
                    self.roles[role.name] = role
                    logger.info(f"Loaded custom role: {role.name}")

        except Exception as e:
            logger.error(f"Failed to load custom roles: {e}")

    def check_permission(self, role_name: str, permission: Permission) -> bool:
        """Check if a role has a specific permission.

        Args:
            role_name: Name of the role
            permission: Permission to check

        Returns:
            True if role has permission, False otherwise
        """
        if role_name not in self.roles:
            logger.warning(f"Role {role_name} not found")
            return False

        role = self.roles[role_name]

        # Check direct permissions
        if permission in role.permissions:
            return True

        # Check inherited permissions
        if role.inherit_from:
            for parent_role_name in role.inherit_from:
                if self.check_permission(parent_role_name, permission):
                    return True

        return False

    def check_permissions(self, role_name: str, permissions: List[Permission]) -> Dict[Permission, bool]:
        """Check multiple permissions for a role.

        Args:
            role_name: Name of the role
            permissions: List of permissions to check

        Returns:
            Dictionary mapping permissions to their check results
        """
        return {
            permission: self.check_permission(role_name, permission)
            for permission in permissions
        }

    def create_custom_role(self,
                          name: str,
                          permissions: Set[Permission],
                          inherit_from: Optional[List[str]] = None,
                          description: Optional[str] = None) -> Role:
        """Create a custom role.

        Args:
            name: Role name
            permissions: Set of permissions
            inherit_from: Optional list of parent roles
            description: Optional role description

        Returns:
            Created role

        Raises:
            ValueError: If role name conflicts with system role
        """
        if name in self.roles and self.roles[name].is_system:
            raise ValueError(f"Cannot override system role: {name}")

        role = Role(
            name=name,
            permissions=permissions,
            inherit_from=inherit_from,
            description=description,
            is_system=False
        )

        self.roles[name] = role
        logger.info(f"Created custom role: {name}")
        return role

    def delete_custom_role(self, name: str) -> bool:
        """Delete a custom role.

        Args:
            name: Role name to delete

        Returns:
            True if deleted, False otherwise

        Raises:
            ValueError: If attempting to delete system role
        """
        if name not in self.roles:
            return False

        if self.roles[name].is_system:
            raise ValueError(f"Cannot delete system role: {name}")

        del self.roles[name]
        logger.info(f"Deleted custom role: {name}")
        return True

    def get_effective_permissions(self, role_name: str) -> Set[Permission]:
        """Get all effective permissions for a role including inherited.

        Args:
            role_name: Name of the role

        Returns:
            Set of all effective permissions
        """
        if role_name not in self.roles:
            return set()

        permissions = set()
        role = self.roles[role_name]
        permissions.update(role.permissions)

        # Add inherited permissions
        if role.inherit_from:
            for parent_role_name in role.inherit_from:
                permissions.update(self.get_effective_permissions(parent_role_name))

        return permissions

    def get_role(self, role_name: str) -> Optional[Role]:
        """Get a role by name.

        Args:
            role_name: Name of the role

        Returns:
            Role if found, None otherwise
        """
        return self.roles.get(role_name)

    def list_roles(self, include_system: bool = True) -> List[str]:
        """List all role names.

        Args:
            include_system: Whether to include system roles

        Returns:
            List of role names
        """
        if include_system:
            return list(self.roles.keys())
        else:
            return [name for name, role in self.roles.items() if not role.is_system]

    def export_roles(self, include_system: bool = False) -> Dict:
        """Export roles to dictionary.

        Args:
            include_system: Whether to include system roles

        Returns:
            Dictionary with role configurations
        """
        roles = []
        for role in self.roles.values():
            if include_system or not role.is_system:
                roles.append(role.to_dict())

        return {"roles": roles}

    def validate_role_inheritance(self, role_name: str, visited: Optional[Set[str]] = None) -> bool:
        """Validate that role inheritance doesn't have cycles.

        Args:
            role_name: Role to validate
            visited: Set of already visited roles (for cycle detection)

        Returns:
            True if valid, False if cycle detected
        """
        if visited is None:
            visited = set()

        if role_name in visited:
            return False  # Cycle detected

        if role_name not in self.roles:
            return True

        visited.add(role_name)
        role = self.roles[role_name]

        if role.inherit_from:
            for parent_role in role.inherit_from:
                if not self.validate_role_inheritance(parent_role, visited.copy()):
                    return False

        return True