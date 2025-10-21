"""PostgreSQL parameter management and recommendations."""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Tuple
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ParameterContext(str, Enum):
    """PostgreSQL parameter context."""
    POSTMASTER = "postmaster"  # Requires restart
    SIGHUP = "sighup"  # Can reload
    BACKEND = "backend"  # New connections only
    SUPERUSER = "superuser"  # Superuser only
    USER = "user"  # Any user can set


class ParameterType(str, Enum):
    """PostgreSQL parameter types."""
    BOOLEAN = "boolean"
    INTEGER = "integer"
    REAL = "real"
    STRING = "string"
    ENUM = "enum"


@dataclass
class ParameterInfo:
    """Information about a PostgreSQL parameter."""
    name: str
    current_value: Any
    default_value: Any
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    unit: Optional[str] = None
    context: Optional[ParameterContext] = None
    param_type: Optional[ParameterType] = None
    description: Optional[str] = None
    category: Optional[str] = None
    requires_restart: bool = False


class PostgreSQLParameters:
    """PostgreSQL parameter management."""

    # Common tuning parameters with recommendations
    TUNING_PARAMETERS = {
        "shared_buffers": {
            "category": "Memory",
            "description": "Main cache for database pages",
            "unit": "MB",
            "recommendation": lambda total_ram: min(total_ram * 0.25, 8192)  # 25% of RAM, max 8GB
        },
        "effective_cache_size": {
            "category": "Memory",
            "description": "Estimate of OS cache available",
            "unit": "MB",
            "recommendation": lambda total_ram: total_ram * 0.75  # 75% of RAM
        },
        "work_mem": {
            "category": "Memory",
            "description": "Memory for sorts and hash tables",
            "unit": "MB",
            "recommendation": lambda total_ram, max_connections: min(
                (total_ram * 0.25) / max_connections,
                64  # Typical max for OLTP
            )
        },
        "maintenance_work_mem": {
            "category": "Memory",
            "description": "Memory for maintenance operations",
            "unit": "MB",
            "recommendation": lambda total_ram: min(total_ram * 0.05, 2048)  # 5% of RAM, max 2GB
        },
        "max_connections": {
            "category": "Connections",
            "description": "Maximum concurrent connections",
            "unit": None,
            "recommendation": lambda workload_type: {
                "oltp": 200,
                "olap": 50,
                "web": 300,
                "mixed": 150
            }.get(workload_type, 100)
        },
        "checkpoint_segments": {  # Pre-9.5
            "category": "Checkpoints",
            "description": "Checkpoint frequency (pre-9.5)",
            "unit": None,
            "recommendation": lambda workload_type: {
                "oltp": 32,
                "olap": 256,
                "mixed": 64
            }.get(workload_type, 32)
        },
        "max_wal_size": {  # 9.5+
            "category": "Checkpoints",
            "description": "Maximum WAL size between checkpoints",
            "unit": "GB",
            "recommendation": lambda workload_type: {
                "oltp": 2,
                "olap": 16,
                "mixed": 4
            }.get(workload_type, 1)
        },
        "min_wal_size": {  # 9.5+
            "category": "Checkpoints",
            "description": "Minimum WAL size",
            "unit": "GB",
            "recommendation": lambda max_wal_size: max_wal_size * 0.25
        },
        "checkpoint_completion_target": {
            "category": "Checkpoints",
            "description": "Checkpoint spread factor",
            "unit": None,
            "recommendation": lambda: 0.9
        },
        "random_page_cost": {
            "category": "Query Planner",
            "description": "Cost of random page access",
            "unit": None,
            "recommendation": lambda storage_type: {
                "ssd": 1.1,
                "hdd": 4.0,
                "nvme": 1.0
            }.get(storage_type, 4.0)
        },
        "effective_io_concurrency": {
            "category": "I/O",
            "description": "Concurrent I/O operations",
            "unit": None,
            "recommendation": lambda storage_type: {
                "ssd": 200,
                "hdd": 2,
                "nvme": 300
            }.get(storage_type, 1)
        },
        "autovacuum": {
            "category": "Autovacuum",
            "description": "Enable autovacuum",
            "unit": None,
            "recommendation": lambda: True
        },
        "autovacuum_max_workers": {
            "category": "Autovacuum",
            "description": "Maximum autovacuum workers",
            "unit": None,
            "recommendation": lambda cpu_cores: min(max(cpu_cores // 4, 3), 8)
        }
    }

    def __init__(self, connection_pool):
        """Initialize parameter manager.

        Args:
            connection_pool: Database connection pool
        """
        self.connection_pool = connection_pool

    async def get_current_settings(self) -> Dict[str, ParameterInfo]:
        """Get current PostgreSQL settings.

        Returns:
            Dictionary of parameter info
        """
        query = """
            SELECT
                name,
                setting,
                unit,
                category,
                short_desc,
                context,
                vartype,
                min_val,
                max_val,
                boot_val
            FROM pg_settings
            ORDER BY category, name
        """

        parameters = {}

        async with self.connection_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                rows = await cur.fetchall()

                for row in rows:
                    name = row[0]
                    parameters[name] = ParameterInfo(
                        name=name,
                        current_value=row[1],
                        unit=row[2],
                        category=row[3],
                        description=row[4],
                        context=ParameterContext(row[5]) if row[5] else None,
                        param_type=self._map_vartype(row[6]),
                        min_value=row[7],
                        max_value=row[8],
                        default_value=row[9],
                        requires_restart=(row[5] == 'postmaster')
                    )

        return parameters

    def _map_vartype(self, vartype: str) -> ParameterType:
        """Map PostgreSQL vartype to ParameterType.

        Args:
            vartype: PostgreSQL variable type

        Returns:
            Mapped parameter type
        """
        mapping = {
            'bool': ParameterType.BOOLEAN,
            'integer': ParameterType.INTEGER,
            'real': ParameterType.REAL,
            'string': ParameterType.STRING,
            'enum': ParameterType.ENUM
        }
        return mapping.get(vartype, ParameterType.STRING)

    async def get_parameter(self, param_name: str) -> Optional[ParameterInfo]:
        """Get information about a specific parameter.

        Args:
            param_name: Parameter name

        Returns:
            Parameter info if found
        """
        settings = await self.get_current_settings()
        return settings.get(param_name)

    async def set_parameter(self,
                          param_name: str,
                          value: Any,
                          scope: str = "session") -> bool:
        """Set a PostgreSQL parameter.

        Args:
            param_name: Parameter name
            value: Parameter value
            scope: 'session' or 'local'

        Returns:
            True if successful
        """
        try:
            async with self.connection_pool.connection() as conn:
                if scope == "session":
                    await conn.execute(f"SET {param_name} = %s", (value,))
                elif scope == "local":
                    await conn.execute(f"SET LOCAL {param_name} = %s", (value,))
                else:
                    raise ValueError(f"Invalid scope: {scope}")

                logger.info(f"Set parameter {param_name} = {value} ({scope})")
                return True

        except Exception as e:
            logger.error(f"Failed to set parameter {param_name}: {e}")
            return False

    async def alter_system_set(self, param_name: str, value: Any) -> bool:
        """Set a parameter using ALTER SYSTEM (persists across restarts).

        Args:
            param_name: Parameter name
            value: Parameter value

        Returns:
            True if successful
        """
        try:
            async with self.connection_pool.connection() as conn:
                if value is None:
                    await conn.execute(f"ALTER SYSTEM RESET {param_name}")
                    logger.info(f"Reset system parameter {param_name}")
                else:
                    await conn.execute(f"ALTER SYSTEM SET {param_name} = %s", (value,))
                    logger.info(f"Set system parameter {param_name} = {value}")

                return True

        except Exception as e:
            logger.error(f"Failed to alter system parameter {param_name}: {e}")
            return False

    async def reload_configuration(self) -> bool:
        """Reload PostgreSQL configuration.

        Returns:
            True if successful
        """
        try:
            async with self.connection_pool.connection() as conn:
                await conn.execute("SELECT pg_reload_conf()")
                logger.info("Reloaded PostgreSQL configuration")
                return True

        except Exception as e:
            logger.error(f"Failed to reload configuration: {e}")
            return False

    async def get_system_info(self) -> Dict[str, Any]:
        """Get system information for tuning recommendations.

        Returns:
            System information dictionary
        """
        queries = {
            "total_ram": """
                SELECT setting::bigint * 8192 / 1024 / 1024 as total_ram_mb
                FROM pg_settings
                WHERE name = 'shared_buffers'
            """,
            "cpu_cores": "SELECT COUNT(*) FROM pg_stat_activity",
            "storage_type": "SELECT 'ssd' as storage_type",  # Simplified
            "workload_type": "SELECT 'mixed' as workload_type"  # Simplified
        }

        system_info = {}

        async with self.connection_pool.connection() as conn:
            for key, query in queries.items():
                async with conn.cursor() as cur:
                    await cur.execute(query)
                    result = await cur.fetchone()
                    if result:
                        system_info[key] = result[0]

        # Get OS-level information if possible
        try:
            import psutil
            system_info["total_ram"] = psutil.virtual_memory().total // (1024 * 1024)
            system_info["cpu_cores"] = psutil.cpu_count()
        except ImportError:
            logger.debug("psutil not available, using estimates")

        return system_info

    def generate_recommendations(self,
                                system_info: Dict[str, Any],
                                current_settings: Dict[str, ParameterInfo]) -> List[Tuple[str, Any, str]]:
        """Generate parameter tuning recommendations.

        Args:
            system_info: System information
            current_settings: Current parameter settings

        Returns:
            List of (parameter, recommended_value, reason) tuples
        """
        recommendations = []

        for param_name, param_config in self.TUNING_PARAMETERS.items():
            if param_name not in current_settings:
                continue

            current_param = current_settings[param_name]
            recommendation_func = param_config.get("recommendation")

            if not recommendation_func:
                continue

            # Calculate recommended value based on parameter
            if param_name == "shared_buffers":
                recommended = recommendation_func(system_info.get("total_ram", 4096))
            elif param_name == "effective_cache_size":
                recommended = recommendation_func(system_info.get("total_ram", 4096))
            elif param_name == "work_mem":
                recommended = recommendation_func(
                    system_info.get("total_ram", 4096),
                    int(current_settings.get("max_connections", ParameterInfo(
                        name="max_connections",
                        current_value=100,
                        default_value=100
                    )).current_value)
                )
            elif param_name == "maintenance_work_mem":
                recommended = recommendation_func(system_info.get("total_ram", 4096))
            elif param_name == "max_connections":
                recommended = recommendation_func(system_info.get("workload_type", "mixed"))
            elif param_name in ["checkpoint_segments", "max_wal_size"]:
                recommended = recommendation_func(system_info.get("workload_type", "mixed"))
            elif param_name == "min_wal_size":
                max_wal = current_settings.get("max_wal_size")
                if max_wal:
                    recommended = recommendation_func(float(max_wal.current_value))
                else:
                    continue
            elif param_name == "checkpoint_completion_target":
                recommended = recommendation_func()
            elif param_name == "random_page_cost":
                recommended = recommendation_func(system_info.get("storage_type", "hdd"))
            elif param_name == "effective_io_concurrency":
                recommended = recommendation_func(system_info.get("storage_type", "hdd"))
            elif param_name == "autovacuum":
                recommended = recommendation_func()
            elif param_name == "autovacuum_max_workers":
                recommended = recommendation_func(system_info.get("cpu_cores", 4))
            else:
                continue

            # Compare with current value
            current_value = self._parse_value(current_param.current_value, current_param.unit)

            if recommended != current_value:
                reason = self._generate_reason(
                    param_name,
                    current_value,
                    recommended,
                    param_config
                )
                recommendations.append((param_name, recommended, reason))

        return recommendations

    def _parse_value(self, value: str, unit: Optional[str]) -> Any:
        """Parse parameter value.

        Args:
            value: String value
            unit: Optional unit

        Returns:
            Parsed value
        """
        if value.lower() in ['on', 'true']:
            return True
        elif value.lower() in ['off', 'false']:
            return False

        try:
            if unit in ['MB', 'GB', 'kB']:
                # Convert to consistent unit (MB)
                numeric_value = float(value)
                if unit == 'GB':
                    return numeric_value * 1024
                elif unit == 'kB':
                    return numeric_value / 1024
                return numeric_value
            else:
                return float(value) if '.' in value else int(value)
        except ValueError:
            return value

    def _generate_reason(self,
                        param_name: str,
                        current_value: Any,
                        recommended_value: Any,
                        param_config: Dict) -> str:
        """Generate recommendation reason.

        Args:
            param_name: Parameter name
            current_value: Current value
            recommended_value: Recommended value
            param_config: Parameter configuration

        Returns:
            Reason string
        """
        description = param_config.get("description", param_name)

        if isinstance(current_value, bool):
            return f"{description}: Currently {current_value}, recommended {recommended_value}"
        elif isinstance(current_value, (int, float)):
            diff_pct = abs((recommended_value - current_value) / current_value * 100)
            if recommended_value > current_value:
                return f"{description}: Increase from {current_value} to {recommended_value} ({diff_pct:.0f}% increase)"
            else:
                return f"{description}: Decrease from {current_value} to {recommended_value} ({diff_pct:.0f}% decrease)"
        else:
            return f"{description}: Change from {current_value} to {recommended_value}"