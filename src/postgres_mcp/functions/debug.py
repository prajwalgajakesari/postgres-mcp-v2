"""Debug function operation implementation."""

from typing import Optional, List, Dict, Any
import psycopg
from ..permissions import Permission
from ..operations import OperationResult
from .base import FunctionOperation


class DebugFunctionOperation(FunctionOperation):
    """Operation to debug and analyze database functions."""

    def get_required_permissions(self) -> List[Permission]:
        return [Permission.SELECT]

    async def validate(self, **kwargs) -> tuple[bool, Optional[str]]:
        function_name = kwargs.get("function_name")
        if not function_name:
            return False, "Function name is required"

        action = kwargs.get("action", "").upper()
        if action not in ["ANALYZE", "EXPLAIN", "PROFILE", "DEPENDENCIES"]:
            return False, "Action must be ANALYZE, EXPLAIN, PROFILE, or DEPENDENCIES"

        return True, None

    async def execute(self, **kwargs) -> OperationResult:
        action = kwargs.get("action", "").upper()
        function_name = kwargs.get("function_name")
        schema_name = kwargs.get("schema_name", "public")

        try:
            async with self.connection_pool.connection() as conn:
                async with conn.cursor() as cur:
                    if action == "ANALYZE":
                        return await self._analyze_function(cur, **kwargs)
                    elif action == "EXPLAIN":
                        return await self._explain_function(cur, **kwargs)
                    elif action == "PROFILE":
                        return await self._profile_function(cur, **kwargs)
                    elif action == "DEPENDENCIES":
                        return await self._get_dependencies(cur, **kwargs)

        except Exception as e:
            return OperationResult(
                success=False, operation_type=self.get_operation_type(),
                operation_name="DebugFunction", error=f"Failed to {action.lower()} function: {str(e)}",
                details={"function": function_name, "schema": schema_name, "action": action}
            )

    async def _analyze_function(self, cur, **kwargs) -> OperationResult:
        function_name = kwargs.get("function_name")
        schema_name = kwargs.get("schema_name", "public")

        # Get function information
        function_info = await self.get_function_info(function_name, schema_name)
        if not function_info:
            return OperationResult(
                success=False, operation_type=self.get_operation_type(),
                operation_name="AnalyzeFunction", error=f"Function '{function_name}' not found",
                details={"function": function_name, "schema": schema_name}
            )

        # Get function source
        source_query = """
            SELECT pg_get_functiondef(p.oid)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = %s AND n.nspname = %s
        """
        await cur.execute(source_query, (function_name, schema_name))
        source_result = await cur.fetchone()
        function_source = source_result[0] if source_result else ""

        # Analyze function complexity
        complexity_analysis = self._analyze_complexity(function_source, function_info)

        # Check for common issues
        issues = self._check_common_issues(function_source, function_info)

        return OperationResult(
            success=True, operation_type=self.get_operation_type(),
            operation_name="AnalyzeFunction",
            affected_objects=[f"function:{schema_name}.{function_name}"],
            message=f"Function '{schema_name}.{function_name}' analyzed successfully",
            details={
                "function": f"{schema_name}.{function_name}",
                "function_info": function_info,
                "source": function_source,
                "complexity": complexity_analysis,
                "issues": issues
            }
        )

    async def _explain_function(self, cur, **kwargs) -> OperationResult:
        function_name = kwargs.get("function_name")
        schema_name = kwargs.get("schema_name", "public")
        parameters = kwargs.get("parameters", [])

        # Build function call
        full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(function_name)}"
        param_values = [str(p.get("value", "NULL")) for p in parameters]
        function_call = f"{full_name}({', '.join(param_values)})"

        # Try to explain the function execution
        try:
            explain_query = f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT {function_call}"
            await cur.execute(explain_query)
            explain_result = await cur.fetchall()

            execution_plan = [row[0] for row in explain_result]
        except Exception as e:
            execution_plan = [f"Could not explain function: {str(e)}"]

        return OperationResult(
            success=True, operation_type=self.get_operation_type(),
            operation_name="ExplainFunction",
            affected_objects=[f"function:{schema_name}.{function_name}"],
            message=f"Function '{schema_name}.{function_name}' execution plan generated",
            details={
                "function": f"{schema_name}.{function_name}",
                "function_call": function_call,
                "execution_plan": execution_plan
            }
        )

    async def _profile_function(self, cur, **kwargs) -> OperationResult:
        function_name = kwargs.get("function_name")
        schema_name = kwargs.get("schema_name", "public")
        parameters = kwargs.get("parameters", [])
        iterations = kwargs.get("iterations", 100)

        # Build function call
        full_name = f"{self._quote_identifier(schema_name)}.{self._quote_identifier(function_name)}"
        param_values = [str(p.get("value", "NULL")) for p in parameters]
        function_call = f"{full_name}({', '.join(param_values)})"

        # Profile function performance
        import time
        timings = []

        try:
            for _ in range(iterations):
                start_time = time.time()
                await cur.execute(f"SELECT {function_call}")
                await cur.fetchall()  # Consume results
                end_time = time.time()
                timings.append((end_time - start_time) * 1000)  # Convert to milliseconds

            avg_time = sum(timings) / len(timings)
            min_time = min(timings)
            max_time = max(timings)

            performance_stats = {
                "iterations": iterations,
                "average_time_ms": round(avg_time, 3),
                "min_time_ms": round(min_time, 3),
                "max_time_ms": round(max_time, 3),
                "total_time_ms": round(sum(timings), 3)
            }

        except Exception as e:
            performance_stats = {
                "error": f"Could not profile function: {str(e)}"
            }

        return OperationResult(
            success=True, operation_type=self.get_operation_type(),
            operation_name="ProfileFunction",
            affected_objects=[f"function:{schema_name}.{function_name}"],
            message=f"Function '{schema_name}.{function_name}' profiled successfully",
            details={
                "function": f"{schema_name}.{function_name}",
                "function_call": function_call,
                "performance": performance_stats
            }
        )

    async def _get_dependencies(self, cur, **kwargs) -> OperationResult:
        function_name = kwargs.get("function_name")
        schema_name = kwargs.get("schema_name", "public")

        # Get function dependencies
        dependencies_query = """
            SELECT DISTINCT
                d.classid::regclass::text as object_type,
                CASE
                    WHEN d.classid = 'pg_class'::regclass THEN c.relname
                    WHEN d.classid = 'pg_proc'::regclass THEN p.proname
                    WHEN d.classid = 'pg_type'::regclass THEN t.typname
                    ELSE d.objid::text
                END as object_name,
                CASE
                    WHEN d.classid = 'pg_class'::regclass THEN nc.nspname
                    WHEN d.classid = 'pg_proc'::regclass THEN np.nspname
                    WHEN d.classid = 'pg_type'::regclass THEN nt.nspname
                    ELSE 'unknown'
                END as schema_name,
                d.deptype
            FROM pg_depend d
            JOIN pg_proc pf ON pf.oid = d.refobjid
            JOIN pg_namespace nf ON nf.oid = pf.pronamespace
            LEFT JOIN pg_class c ON c.oid = d.objid AND d.classid = 'pg_class'::regclass
            LEFT JOIN pg_namespace nc ON nc.oid = c.relnamespace
            LEFT JOIN pg_proc p ON p.oid = d.objid AND d.classid = 'pg_proc'::regclass
            LEFT JOIN pg_namespace np ON np.oid = p.pronamespace
            LEFT JOIN pg_type t ON t.oid = d.objid AND d.classid = 'pg_type'::regclass
            LEFT JOIN pg_namespace nt ON nt.oid = t.typnamespace
            WHERE pf.proname = %s AND nf.nspname = %s
            ORDER BY object_type, schema_name, object_name
        """

        await cur.execute(dependencies_query, (function_name, schema_name))
        dependencies = await cur.fetchall()

        dependency_list = []
        for dep in dependencies:
            dependency_list.append({
                "object_type": dep[0],
                "object_name": dep[1],
                "schema": dep[2],
                "dependency_type": dep[3]
            })

        # Get functions that depend on this function
        dependents_query = """
            SELECT DISTINCT
                pf.proname as function_name,
                nf.nspname as schema_name,
                d.deptype
            FROM pg_depend d
            JOIN pg_proc pr ON pr.oid = d.refobjid
            JOIN pg_namespace nr ON nr.oid = pr.pronamespace
            JOIN pg_proc pf ON pf.oid = d.objid
            JOIN pg_namespace nf ON nf.oid = pf.pronamespace
            WHERE pr.proname = %s AND nr.nspname = %s AND d.classid = 'pg_proc'::regclass
            ORDER BY schema_name, function_name
        """

        await cur.execute(dependents_query, (function_name, schema_name))
        dependents = await cur.fetchall()

        dependent_list = []
        for dep in dependents:
            dependent_list.append({
                "function_name": dep[0],
                "schema": dep[1],
                "dependency_type": dep[2]
            })

        return OperationResult(
            success=True, operation_type=self.get_operation_type(),
            operation_name="GetFunctionDependencies",
            affected_objects=[f"function:{schema_name}.{function_name}"],
            message=f"Dependencies for function '{schema_name}.{function_name}' retrieved",
            details={
                "function": f"{schema_name}.{function_name}",
                "dependencies": dependency_list,
                "dependents": dependent_list
            }
        )

    def _analyze_complexity(self, source: str, function_info: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze function complexity."""
        if not source:
            return {"error": "No source available"}

        # Simple complexity metrics
        lines = source.split('\n')
        code_lines = [line.strip() for line in lines if line.strip() and not line.strip().startswith('--')]

        # Count various constructs
        keywords = ['IF', 'CASE', 'LOOP', 'FOR', 'WHILE', 'BEGIN', 'EXCEPTION']
        complexity_score = 1  # Base complexity

        for line in code_lines:
            line_upper = line.upper()
            for keyword in keywords:
                if keyword in line_upper:
                    complexity_score += 1

        return {
            "total_lines": len(lines),
            "code_lines": len(code_lines),
            "complexity_score": complexity_score,
            "language": function_info.get("language", "unknown"),
            "volatility": function_info.get("volatility", "unknown"),
            "estimated_complexity": "low" if complexity_score <= 5 else "medium" if complexity_score <= 15 else "high"
        }

    def _check_common_issues(self, source: str, function_info: Dict[str, Any]) -> List[str]:
        """Check for common function issues."""
        issues = []

        if not source:
            issues.append("No source code available for analysis")
            return issues

        source_upper = source.upper()

        # Check for potential issues
        if "EXCEPTION WHEN OTHERS" in source_upper:
            issues.append("Generic exception handling found - consider specific exception types")

        if function_info.get("volatility") == "v" and "SELECT" in source_upper:
            issues.append("Function marked as VOLATILE but may be STABLE or IMMUTABLE")

        if not function_info.get("is_strict") and "NULL" not in source_upper:
            issues.append("Function not marked as STRICT but may not handle NULL inputs")

        if function_info.get("security_definer") and "SECURITY" not in source_upper:
            issues.append("Function is SECURITY DEFINER - ensure proper input validation")

        # Check for performance issues
        if "LOOP" in source_upper and "EXIT" not in source_upper:
            issues.append("Loop found without explicit exit condition")

        if source.count("SELECT") > 5:
            issues.append("Multiple SELECT statements found - consider performance impact")

        return issues if issues else ["No common issues detected"]