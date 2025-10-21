"""Tests for RBAC (Role-Based Access Control) system."""

import pytest
from postgres_mcp.permissions import Permission, Role, RBACManager


class TestRBACManager:
    """Test RBAC functionality."""

    @pytest.fixture
    def rbac_manager(self):
        """Create RBAC manager instance."""
        return RBACManager()

    def test_default_roles_created(self, rbac_manager):
        """Test that default roles are created."""
        assert "readonly" in rbac_manager.roles
        assert "developer" in rbac_manager.roles
        assert "dba" in rbac_manager.roles
        assert "analyst" in rbac_manager.roles
        assert "operator" in rbac_manager.roles

    def test_permission_check_dba(self, rbac_manager):
        """Test DBA has all permissions."""
        # DBA should have all permissions
        assert rbac_manager.check_permission("dba", Permission.CREATE_TABLE)
        assert rbac_manager.check_permission("dba", Permission.DROP_TABLE)
        assert rbac_manager.check_permission("dba", Permission.BACKUP)
        assert rbac_manager.check_permission("dba", Permission.ALTER_SYSTEM)

    def test_permission_check_readonly(self, rbac_manager):
        """Test readonly role restrictions."""
        # Read-only should not have write permissions
        assert not rbac_manager.check_permission("readonly", Permission.CREATE_TABLE)
        assert not rbac_manager.check_permission("readonly", Permission.INSERT)
        assert not rbac_manager.check_permission("readonly", Permission.UPDATE)
        assert not rbac_manager.check_permission("readonly", Permission.DELETE)

        # But should have read permissions
        assert rbac_manager.check_permission("readonly", Permission.SELECT)
        assert rbac_manager.check_permission("readonly", Permission.VIEW_STATS)

    def test_permission_check_developer(self, rbac_manager):
        """Test developer role permissions."""
        # Developer should have DDL permissions
        assert rbac_manager.check_permission("developer", Permission.CREATE_TABLE)
        assert rbac_manager.check_permission("developer", Permission.ALTER_TABLE)
        assert rbac_manager.check_permission("developer", Permission.CREATE_INDEX)

        # And DML permissions
        assert rbac_manager.check_permission("developer", Permission.INSERT)
        assert rbac_manager.check_permission("developer", Permission.UPDATE)
        assert rbac_manager.check_permission("developer", Permission.DELETE)

        # But not admin permissions
        assert not rbac_manager.check_permission("developer", Permission.CREATE_USER)
        assert not rbac_manager.check_permission("developer", Permission.ALTER_SYSTEM)

    def test_custom_role_creation(self, rbac_manager):
        """Test creating custom roles."""
        custom_role = rbac_manager.create_custom_role(
            name="custom_analyst",
            permissions={Permission.SELECT, Permission.VIEW_STATS},
            description="Custom analyst role"
        )

        assert custom_role.name == "custom_analyst"
        assert "custom_analyst" in rbac_manager.roles
        assert rbac_manager.check_permission("custom_analyst", Permission.SELECT)
        assert not rbac_manager.check_permission("custom_analyst", Permission.INSERT)

    def test_permission_inheritance(self, rbac_manager):
        """Test permission inheritance."""
        # Create a role that inherits from developer
        rbac_manager.create_custom_role(
            name="senior_dev",
            permissions={Permission.BACKUP},
            inherit_from=["developer"]
        )

        # Should have both inherited and direct permissions
        assert rbac_manager.check_permission("senior_dev", Permission.BACKUP)
        assert rbac_manager.check_permission("senior_dev", Permission.CREATE_TABLE)
        assert rbac_manager.check_permission("senior_dev", Permission.INSERT)

    def test_get_effective_permissions(self, rbac_manager):
        """Test getting all effective permissions."""
        # Create role with inheritance
        rbac_manager.create_custom_role(
            name="test_role",
            permissions={Permission.BACKUP},
            inherit_from=["readonly"]
        )

        effective_perms = rbac_manager.get_effective_permissions("test_role")

        assert Permission.BACKUP in effective_perms
        assert Permission.SELECT in effective_perms
        assert Permission.VIEW_STATS in effective_perms

    def test_multiple_permission_check(self, rbac_manager):
        """Test checking multiple permissions at once."""
        permissions_to_check = [
            Permission.SELECT,
            Permission.INSERT,
            Permission.CREATE_TABLE,
            Permission.DROP_TABLE
        ]

        results = rbac_manager.check_permissions("developer", permissions_to_check)

        assert results[Permission.SELECT] is True
        assert results[Permission.INSERT] is True
        assert results[Permission.CREATE_TABLE] is True
        assert results[Permission.DROP_TABLE] is True

    def test_delete_custom_role(self, rbac_manager):
        """Test deleting custom roles."""
        # Create a custom role
        rbac_manager.create_custom_role(
            name="temp_role",
            permissions={Permission.SELECT}
        )

        assert "temp_role" in rbac_manager.roles

        # Delete it
        assert rbac_manager.delete_custom_role("temp_role")
        assert "temp_role" not in rbac_manager.roles

    def test_cannot_delete_system_role(self, rbac_manager):
        """Test that system roles cannot be deleted."""
        with pytest.raises(ValueError, match="Cannot delete system role"):
            rbac_manager.delete_custom_role("dba")

    def test_cannot_override_system_role(self, rbac_manager):
        """Test that system roles cannot be overridden."""
        with pytest.raises(ValueError, match="Cannot override system role"):
            rbac_manager.create_custom_role(
                name="dba",
                permissions={Permission.SELECT}
            )

    def test_role_validation_no_cycles(self, rbac_manager):
        """Test role inheritance validation."""
        # Create roles
        rbac_manager.create_custom_role(
            name="role_a",
            permissions={Permission.SELECT}
        )

        rbac_manager.create_custom_role(
            name="role_b",
            permissions={Permission.INSERT},
            inherit_from=["role_a"]
        )

        # Validate no cycles
        assert rbac_manager.validate_role_inheritance("role_a")
        assert rbac_manager.validate_role_inheritance("role_b")

    def test_export_roles(self, rbac_manager):
        """Test exporting roles to dictionary."""
        # Create a custom role
        rbac_manager.create_custom_role(
            name="export_test",
            permissions={Permission.SELECT}
        )

        # Export without system roles
        exported = rbac_manager.export_roles(include_system=False)

        assert "roles" in exported
        role_names = [r["name"] for r in exported["roles"]]
        assert "export_test" in role_names
        assert "dba" not in role_names  # System role excluded

    def test_list_roles(self, rbac_manager):
        """Test listing roles."""
        # Create custom role
        rbac_manager.create_custom_role(
            name="list_test",
            permissions={Permission.SELECT}
        )

        # List all roles
        all_roles = rbac_manager.list_roles(include_system=True)
        assert "dba" in all_roles
        assert "list_test" in all_roles

        # List only custom roles
        custom_roles = rbac_manager.list_roles(include_system=False)
        assert "dba" not in custom_roles
        assert "list_test" in custom_roles

    def test_get_role(self, rbac_manager):
        """Test getting role by name."""
        role = rbac_manager.get_role("developer")

        assert role is not None
        assert role.name == "developer"
        assert role.is_system is True
        assert Permission.CREATE_TABLE in role.permissions

        # Non-existent role
        assert rbac_manager.get_role("nonexistent") is None