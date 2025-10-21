"""Configuration management system for PostgreSQL MCP Server."""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from enum import Enum
import logging
from dataclasses import dataclass, asdict, field
import yaml

logger = logging.getLogger(__name__)


class ConfigCategory(str, Enum):
    """Configuration categories."""
    SERVER = "server"
    DATABASE = "database"
    SECURITY = "security"
    PERFORMANCE = "performance"
    LOGGING = "logging"
    BACKUP = "backup"
    MONITORING = "monitoring"
    CUSTOM = "custom"


@dataclass
class ConfigItem:
    """Configuration item."""
    key: str
    value: Any
    category: ConfigCategory
    description: Optional[str] = None
    default_value: Any = None
    required: bool = False
    secret: bool = False
    validation_regex: Optional[str] = None
    allowed_values: Optional[List[Any]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None


class ConfigurationManager:
    """Manages application configuration."""

    DEFAULT_CONFIG = {
        ConfigCategory.SERVER: {
            "host": "localhost",
            "port": 5432,
            "max_connections": 100,
            "connection_timeout": 30,
            "query_timeout": 120,
            "enable_ssl": False
        },
        ConfigCategory.DATABASE: {
            "default_database": "postgres",
            "default_schema": "public",
            "application_name": "postgres_mcp",
            "search_path": ["public"]
        },
        ConfigCategory.SECURITY: {
            "enable_rbac": True,
            "enable_audit": True,
            "password_min_length": 8,
            "password_require_special": True,
            "max_failed_logins": 5,
            "session_timeout": 3600
        },
        ConfigCategory.PERFORMANCE: {
            "connection_pool_size": 20,
            "connection_pool_max_overflow": 10,
            "statement_timeout": 300000,  # 5 minutes in milliseconds
            "lock_timeout": 10000,  # 10 seconds in milliseconds
            "idle_in_transaction_timeout": 60000,  # 1 minute in milliseconds
            "enable_query_cache": True,
            "query_cache_size": 1000
        },
        ConfigCategory.LOGGING: {
            "log_level": "INFO",
            "log_file": "postgres_mcp.log",
            "log_rotation": "daily",
            "log_retention_days": 30,
            "audit_log_file": "postgres_mcp_audit.jsonl",
            "enable_query_logging": False
        },
        ConfigCategory.BACKUP: {
            "backup_directory": "/var/backups/postgres_mcp",
            "backup_retention_days": 30,
            "backup_compression": True,
            "backup_parallel_jobs": 4
        },
        ConfigCategory.MONITORING: {
            "enable_metrics": True,
            "metrics_port": 9090,
            "health_check_interval": 60,
            "alert_email": None,
            "alert_webhook": None
        }
    }

    def __init__(self, config_file: Optional[str] = None, auto_create: bool = True):
        """Initialize configuration manager.

        Args:
            config_file: Path to configuration file
            auto_create: Whether to create config file if it doesn't exist
        """
        self.config_file = Path(config_file) if config_file else Path("config/postgres_mcp.yaml")
        self.config: Dict[ConfigCategory, Dict[str, Any]] = {}
        self.custom_validators: Dict[str, callable] = {}

        # Load default configuration
        self._load_defaults()

        # Load from file if exists
        if self.config_file.exists():
            self.load_from_file()
        elif auto_create:
            self.save_to_file()

        # Load from environment variables
        self._load_from_env()

    def _load_defaults(self):
        """Load default configuration."""
        for category, values in self.DEFAULT_CONFIG.items():
            self.config[category] = values.copy()

    def load_from_file(self):
        """Load configuration from file."""
        try:
            if self.config_file.suffix == '.json':
                with open(self.config_file, 'r') as f:
                    file_config = json.load(f)
            elif self.config_file.suffix in ['.yaml', '.yml']:
                with open(self.config_file, 'r') as f:
                    file_config = yaml.safe_load(f)
            else:
                raise ValueError(f"Unsupported config file format: {self.config_file.suffix}")

            # Merge with existing config
            for category_str, values in file_config.items():
                try:
                    category = ConfigCategory(category_str)
                    if category not in self.config:
                        self.config[category] = {}
                    self.config[category].update(values)
                except ValueError:
                    # Handle custom categories
                    self.config[ConfigCategory.CUSTOM] = self.config.get(ConfigCategory.CUSTOM, {})
                    self.config[ConfigCategory.CUSTOM][category_str] = values

            logger.info(f"Loaded configuration from {self.config_file}")

        except Exception as e:
            logger.error(f"Failed to load configuration from {self.config_file}: {e}")

    def save_to_file(self):
        """Save configuration to file."""
        try:
            # Ensure directory exists
            self.config_file.parent.mkdir(parents=True, exist_ok=True)

            # Prepare config for serialization
            serializable_config = {}
            for category, values in self.config.items():
                serializable_config[category.value] = values

            if self.config_file.suffix == '.json':
                with open(self.config_file, 'w') as f:
                    json.dump(serializable_config, f, indent=2)
            elif self.config_file.suffix in ['.yaml', '.yml']:
                with open(self.config_file, 'w') as f:
                    yaml.safe_dump(serializable_config, f, default_flow_style=False)

            logger.info(f"Saved configuration to {self.config_file}")

        except Exception as e:
            logger.error(f"Failed to save configuration to {self.config_file}: {e}")

    def _load_from_env(self):
        """Load configuration from environment variables."""
        env_prefix = "POSTGRES_MCP_"

        for env_var, value in os.environ.items():
            if not env_var.startswith(env_prefix):
                continue

            # Parse environment variable name
            # Format: POSTGRES_MCP_CATEGORY_KEY
            parts = env_var[len(env_prefix):].split('_', 1)
            if len(parts) != 2:
                continue

            category_str, key = parts
            try:
                category = ConfigCategory(category_str.lower())
            except ValueError:
                category = ConfigCategory.CUSTOM
                key = f"{category_str.lower()}_{key}"

            # Parse value
            parsed_value = self._parse_env_value(value)

            # Set value
            if category not in self.config:
                self.config[category] = {}
            self.config[category][key.lower()] = parsed_value

            logger.debug(f"Loaded from env: {category}.{key} = {parsed_value}")

    def _parse_env_value(self, value: str) -> Any:
        """Parse environment variable value.

        Args:
            value: String value from environment

        Returns:
            Parsed value
        """
        # Try to parse as JSON first (for lists, dicts, etc.)
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            pass

        # Try to parse as boolean
        if value.lower() in ['true', 'yes', '1']:
            return True
        elif value.lower() in ['false', 'no', '0']:
            return False

        # Try to parse as number
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            pass

        # Return as string
        return value

    def get(self, category: ConfigCategory, key: str, default: Any = None) -> Any:
        """Get configuration value.

        Args:
            category: Configuration category
            key: Configuration key
            default: Default value if not found

        Returns:
            Configuration value
        """
        if category in self.config and key in self.config[category]:
            return self.config[category][key]
        return default

    def set(self, category: ConfigCategory, key: str, value: Any):
        """Set configuration value.

        Args:
            category: Configuration category
            key: Configuration key
            value: Configuration value
        """
        if category not in self.config:
            self.config[category] = {}
        self.config[category][key] = value
        logger.debug(f"Set config: {category}.{key} = {value}")

    def get_category(self, category: ConfigCategory) -> Dict[str, Any]:
        """Get all configuration for a category.

        Args:
            category: Configuration category

        Returns:
            Dictionary of configuration values
        """
        return self.config.get(category, {}).copy()

    def update_category(self, category: ConfigCategory, values: Dict[str, Any]):
        """Update configuration for a category.

        Args:
            category: Configuration category
            values: Dictionary of values to update
        """
        if category not in self.config:
            self.config[category] = {}
        self.config[category].update(values)

    def get_database_url(self, include_password: bool = False) -> str:
        """Get database connection URL.

        Args:
            include_password: Whether to include password in URL

        Returns:
            Database URL
        """
        db_config = self.get_category(ConfigCategory.DATABASE)
        server_config = self.get_category(ConfigCategory.SERVER)

        host = server_config.get("host", "localhost")
        port = server_config.get("port", 5432)
        database = db_config.get("default_database", "postgres")
        username = db_config.get("username", "")
        password = db_config.get("password", "")

        if include_password and password:
            return f"postgresql://{username}:{password}@{host}:{port}/{database}"
        elif username:
            return f"postgresql://{username}@{host}:{port}/{database}"
        else:
            return f"postgresql://{host}:{port}/{database}"

    def validate_configuration(self) -> List[str]:
        """Validate current configuration.

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Validate server configuration
        server_config = self.get_category(ConfigCategory.SERVER)
        if server_config.get("port", 0) < 1 or server_config.get("port", 0) > 65535:
            errors.append("Server port must be between 1 and 65535")

        if server_config.get("max_connections", 0) < 1:
            errors.append("Max connections must be positive")

        # Validate security configuration
        security_config = self.get_category(ConfigCategory.SECURITY)
        if security_config.get("password_min_length", 0) < 8:
            errors.append("Password minimum length should be at least 8")

        # Validate performance configuration
        perf_config = self.get_category(ConfigCategory.PERFORMANCE)
        if perf_config.get("connection_pool_size", 0) < 1:
            errors.append("Connection pool size must be positive")

        # Validate backup configuration
        backup_config = self.get_category(ConfigCategory.BACKUP)
        backup_dir = backup_config.get("backup_directory")
        if backup_dir and not Path(backup_dir).exists():
            errors.append(f"Backup directory does not exist: {backup_dir}")

        return errors

    def register_validator(self, key: str, validator: callable):
        """Register a custom validator for a configuration key.

        Args:
            key: Configuration key (format: category.key)
            validator: Validation function
        """
        self.custom_validators[key] = validator

    def export_config(self, include_secrets: bool = False) -> Dict[str, Any]:
        """Export configuration to dictionary.

        Args:
            include_secrets: Whether to include secret values

        Returns:
            Configuration dictionary
        """
        exported = {}

        for category, values in self.config.items():
            category_dict = {}

            for key, value in values.items():
                # Mask secrets if requested
                if not include_secrets and key in ['password', 'secret', 'token', 'key']:
                    category_dict[key] = "***MASKED***"
                else:
                    category_dict[key] = value

            exported[category.value] = category_dict

        return exported

    def reset_to_defaults(self, category: Optional[ConfigCategory] = None):
        """Reset configuration to defaults.

        Args:
            category: Optional category to reset (all if None)
        """
        if category:
            if category in self.DEFAULT_CONFIG:
                self.config[category] = self.DEFAULT_CONFIG[category].copy()
                logger.info(f"Reset category {category} to defaults")
        else:
            self._load_defaults()
            logger.info("Reset all configuration to defaults")

    def reload(self):
        """Reload configuration from file and environment."""
        self._load_defaults()

        if self.config_file.exists():
            self.load_from_file()

        self._load_from_env()

        logger.info("Reloaded configuration")