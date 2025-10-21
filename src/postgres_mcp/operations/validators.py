"""Validators for database operations."""

import re
from typing import Optional, Tuple, List, Any, Dict
import logging

logger = logging.getLogger(__name__)


class NameValidator:
    """Validator for database object names."""

    # PostgreSQL identifier rules
    MAX_IDENTIFIER_LENGTH = 63
    RESERVED_KEYWORDS = {
        'select', 'insert', 'update', 'delete', 'from', 'where', 'join',
        'table', 'view', 'index', 'sequence', 'function', 'procedure',
        'trigger', 'schema', 'database', 'user', 'role', 'grant', 'revoke',
        'create', 'alter', 'drop', 'truncate', 'commit', 'rollback',
        'begin', 'end', 'if', 'else', 'case', 'when', 'then', 'and', 'or',
        'not', 'null', 'true', 'false', 'default', 'primary', 'foreign',
        'key', 'references', 'unique', 'check', 'constraint', 'cascade'
    }

    @classmethod
    def validate_identifier(cls, name: str, allow_quoted: bool = True) -> Tuple[bool, Optional[str]]:
        """Validate a PostgreSQL identifier.

        Args:
            name: Identifier name
            allow_quoted: Whether to allow quoted identifiers

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not name:
            return False, "Name cannot be empty"

        if len(name) > cls.MAX_IDENTIFIER_LENGTH:
            return False, f"Name too long (max {cls.MAX_IDENTIFIER_LENGTH} characters)"

        # Check if it's a quoted identifier
        if name.startswith('"') and name.endswith('"'):
            if not allow_quoted:
                return False, "Quoted identifiers not allowed"
            # For quoted identifiers, almost anything goes
            inner_name = name[1:-1]
            if '""' not in inner_name and '"' in inner_name:
                return False, "Invalid quoted identifier"
            return True, None

        # Unquoted identifier rules
        if not re.match(r'^[a-z_][a-z0-9_$]*$', name.lower()):
            return False, "Invalid identifier format (must start with letter or underscore, contain only letters, numbers, underscores, or $)"

        # Check reserved keywords
        if name.lower() in cls.RESERVED_KEYWORDS:
            return False, f"'{name}' is a reserved keyword"

        return True, None

    @classmethod
    def validate_qualified_name(cls, name: str) -> Tuple[bool, Optional[str]]:
        """Validate a qualified name (e.g., schema.table).

        Args:
            name: Qualified name

        Returns:
            Tuple of (is_valid, error_message)
        """
        parts = name.split('.')
        if len(parts) > 3:  # database.schema.object is maximum
            return False, "Too many name parts (maximum 3)"

        for part in parts:
            is_valid, error = cls.validate_identifier(part)
            if not is_valid:
                return False, f"Invalid name part '{part}': {error}"

        return True, None


class SQLValidator:
    """Validator for SQL statements."""

    DANGEROUS_PATTERNS = [
        r';\s*DROP\s+',
        r';\s*DELETE\s+',
        r';\s*TRUNCATE\s+',
        r';\s*UPDATE\s+',
        r'--.*DROP\s+',
        r'/\*.*\*/\s*DROP\s+',
    ]

    TRANSACTION_COMMANDS = [
        'BEGIN', 'COMMIT', 'ROLLBACK', 'START TRANSACTION',
        'SAVEPOINT', 'RELEASE', 'PREPARE TRANSACTION'
    ]

    @classmethod
    def check_sql_injection(cls, sql: str) -> Tuple[bool, Optional[str]]:
        """Check for potential SQL injection patterns.

        Args:
            sql: SQL statement

        Returns:
            Tuple of (is_safe, warning_message)
        """
        sql_upper = sql.upper()

        # Check for multiple statements
        if sql.count(';') > 1:
            return False, "Multiple statements detected"

        # Check dangerous patterns
        for pattern in cls.DANGEROUS_PATTERNS:
            if re.search(pattern, sql, re.IGNORECASE):
                return False, f"Dangerous SQL pattern detected: {pattern}"

        # Check for comment injection
        if '--' in sql or '/*' in sql:
            # Allow comments but check what follows
            if re.search(r'(--|/\*).*(DROP|DELETE|TRUNCATE|UPDATE)', sql, re.IGNORECASE):
                return False, "Suspicious comment pattern detected"

        return True, None

    @classmethod
    def validate_select_statement(cls, sql: str) -> Tuple[bool, Optional[str]]:
        """Validate a SELECT statement.

        Args:
            sql: SELECT statement

        Returns:
            Tuple of (is_valid, error_message)
        """
        sql_stripped = sql.strip()
        if not sql_stripped.upper().startswith('SELECT'):
            return False, "Not a SELECT statement"

        # Check for SQL injection
        is_safe, warning = cls.check_sql_injection(sql)
        if not is_safe:
            return False, warning

        return True, None

    @classmethod
    def validate_dml_statement(cls, sql: str, allowed_operations: List[str] = None) -> Tuple[bool, Optional[str]]:
        """Validate a DML statement.

        Args:
            sql: DML statement
            allowed_operations: List of allowed operations (INSERT, UPDATE, DELETE)

        Returns:
            Tuple of (is_valid, error_message)
        """
        if allowed_operations is None:
            allowed_operations = ['INSERT', 'UPDATE', 'DELETE']

        sql_stripped = sql.strip().upper()
        operation = sql_stripped.split()[0] if sql_stripped else ""

        if operation not in allowed_operations:
            return False, f"Operation {operation} not allowed"

        # Check for SQL injection
        is_safe, warning = cls.check_sql_injection(sql)
        if not is_safe:
            return False, warning

        return True, None

    @classmethod
    def contains_transaction_commands(cls, sql: str) -> bool:
        """Check if SQL contains transaction commands.

        Args:
            sql: SQL statement

        Returns:
            True if contains transaction commands
        """
        sql_upper = sql.upper()
        for command in cls.TRANSACTION_COMMANDS:
            if command in sql_upper:
                return True
        return False


class ParameterValidator:
    """Validator for operation parameters."""

    @classmethod
    def validate_positive_integer(cls, value: Any, name: str) -> Tuple[bool, Optional[str]]:
        """Validate a positive integer parameter.

        Args:
            value: Value to validate
            name: Parameter name

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            int_value = int(value)
            if int_value <= 0:
                return False, f"{name} must be positive"
            return True, None
        except (TypeError, ValueError):
            return False, f"{name} must be an integer"

    @classmethod
    def validate_boolean(cls, value: Any, name: str) -> Tuple[bool, Optional[str]]:
        """Validate a boolean parameter.

        Args:
            value: Value to validate
            name: Parameter name

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not isinstance(value, bool):
            return False, f"{name} must be a boolean"
        return True, None

    @classmethod
    def validate_enum(cls, value: Any, name: str, allowed_values: List[Any]) -> Tuple[bool, Optional[str]]:
        """Validate an enum parameter.

        Args:
            value: Value to validate
            name: Parameter name
            allowed_values: List of allowed values

        Returns:
            Tuple of (is_valid, error_message)
        """
        if value not in allowed_values:
            return False, f"{name} must be one of: {allowed_values}"
        return True, None

    @classmethod
    def validate_connection_string(cls, conn_string: str) -> Tuple[bool, Optional[str]]:
        """Validate a PostgreSQL connection string.

        Args:
            conn_string: Connection string

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Basic validation
        if not conn_string:
            return False, "Connection string cannot be empty"

        # Check for required components
        if not conn_string.startswith(('postgresql://', 'postgres://')):
            return False, "Connection string must start with postgresql:// or postgres://"

        # Check for dangerous patterns
        if ';' in conn_string:
            return False, "Connection string cannot contain semicolons"

        return True, None


class OperationValidator:
    """Main validator for operations."""

    def __init__(self):
        """Initialize operation validator."""
        self.name_validator = NameValidator()
        self.sql_validator = SQLValidator()
        self.param_validator = ParameterValidator()

    async def validate_create_table(self,
                                   table_name: str,
                                   columns: List[Dict[str, Any]],
                                   schema: str = "public") -> Tuple[bool, Optional[str]]:
        """Validate CREATE TABLE parameters.

        Args:
            table_name: Table name
            columns: Column definitions
            schema: Schema name

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Validate table name
        is_valid, error = self.name_validator.validate_identifier(table_name)
        if not is_valid:
            return False, f"Invalid table name: {error}"

        # Validate schema name
        is_valid, error = self.name_validator.validate_identifier(schema)
        if not is_valid:
            return False, f"Invalid schema name: {error}"

        # Validate columns
        if not columns:
            return False, "Table must have at least one column"

        column_names = set()
        for col in columns:
            if 'name' not in col:
                return False, "Column must have a name"

            col_name = col['name']
            is_valid, error = self.name_validator.validate_identifier(col_name)
            if not is_valid:
                return False, f"Invalid column name '{col_name}': {error}"

            if col_name.lower() in column_names:
                return False, f"Duplicate column name: {col_name}"
            column_names.add(col_name.lower())

            if 'type' not in col:
                return False, f"Column '{col_name}' must have a type"

        return True, None

    async def validate_user_creation(self,
                                    username: str,
                                    password: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        """Validate user creation parameters.

        Args:
            username: Username
            password: Optional password

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Validate username
        is_valid, error = self.name_validator.validate_identifier(username, allow_quoted=False)
        if not is_valid:
            return False, f"Invalid username: {error}"

        # Validate password if provided
        if password:
            if len(password) < 8:
                return False, "Password must be at least 8 characters"

            if password == username:
                return False, "Password cannot be the same as username"

            # Check for common weak passwords
            weak_passwords = ['password', '12345678', 'password123', 'admin123']
            if password.lower() in weak_passwords:
                return False, "Password is too weak"

        return True, None

    async def validate_backup_operation(self,
                                       backup_path: str,
                                       backup_type: str = "full") -> Tuple[bool, Optional[str]]:
        """Validate backup operation parameters.

        Args:
            backup_path: Backup file path
            backup_type: Type of backup

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Validate backup path
        if not backup_path:
            return False, "Backup path is required"

        if '..' in backup_path:
            return False, "Path traversal not allowed in backup path"

        # Validate backup type
        allowed_types = ['full', 'incremental', 'differential']
        if backup_type not in allowed_types:
            return False, f"Backup type must be one of: {allowed_types}"

        return True, None