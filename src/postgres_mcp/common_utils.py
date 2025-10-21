"""Common utilities for MCP tools."""

from .permissions import RBACManager


class SimpleAuditLogger:
    """Simple audit logger for tools that don't need full audit functionality."""

    def audit_log(self, *args, **kwargs):
        """No-op audit logging."""
        pass


def create_default_rbac_manager() -> RBACManager:
    """Create a default RBAC manager with permissive permissions."""
    rbac_manager = RBACManager()
    rbac_manager.create_custom_role("default", permissions=["*"])
    return rbac_manager