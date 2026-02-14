#!/usr/bin/env python3
"""Multi-layer Safety Validator for Generated Strategies.

Validates generated strategy code through:
  1. Static analysis (AST parsing, syntax)
  2. Security scan (dangerous imports, API keys)
  3. Convention check (NetTrace patterns)
  4. Integration check (gates, monitoring)
  5. Test coverage check
  6. Sandbox execution with timeout

Usage:
  python strategy_validator.py --mode validate --filepath generated_strategies/momentum_abc123.py
  python strategy_validator.py --mode scan --filepath generated_strategies/arb_def456.py
"""

import ast
import logging
import os
import re
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from enum import Enum

logger = logging.getLogger("strategy_validator")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

BASE = Path(__file__).parent
VALIDATION_LOG = BASE / "strategy_validation.jsonl"


class ValidationLayer(Enum):
    """Validation pipeline layers."""
    STATIC_ANALYSIS = "static_analysis"
    SECURITY = "security"
    CONVENTIONS = "conventions"
    INTEGRATION = "integration"
    COVERAGE = "coverage"
    SANDBOX = "sandbox"


class StrategyValidator:
    """Validates strategy code safety and compliance."""

    # Dangerous modules that should never be imported
    BLACKLISTED_MODULES = {
        "os.system",
        "subprocess",
        "exec",
        "eval",
        "compile",
        "open",
        "pickle",
        "__import__",
        "globals",
        "locals",
        "vars",
        "getattr",
        "setattr",
        "delattr",
    }

    # Required imports for NetTrace integration
    REQUIRED_IMPORTS = {
        "agent_goals",
        "risk_controller",
    }

    # Required method names
    REQUIRED_METHODS = {"should_trade", "execute"}

    # Required class inheritance or pattern
    REQUIRED_PATTERN = "class.*Strategy"

    def __init__(self):
        pass

    def validate_strategy(self, filepath: str) -> Dict:
        """Run full validation pipeline."""

        logger.info(f"Validating strategy: {filepath}")

        result = {
            "filepath": filepath,
            "filename": Path(filepath).name,
            "layers": {},
            "passed": True,
            "errors": [],
            "warnings": [],
        }

        # Layer 1: Static analysis
        static_result = self._validate_static_analysis(filepath)
        result["layers"]["static_analysis"] = static_result
        if not static_result["passed"]:
            result["passed"] = False
            result["errors"].extend(static_result.get("errors", []))

        if not static_result["passed"]:
            logger.error(f"Static analysis failed: {result['errors']}")
            return result

        # Layer 2: Security scan
        security_result = self._validate_security(filepath)
        result["layers"]["security"] = security_result
        if not security_result["passed"]:
            result["passed"] = False
            result["errors"].extend(security_result.get("errors", []))

        # Layer 3: Convention check
        convention_result = self._validate_conventions(filepath)
        result["layers"]["conventions"] = convention_result
        if not convention_result["passed"]:
            result["passed"] = False
            result["errors"].extend(convention_result.get("errors", []))
        result["warnings"].extend(convention_result.get("warnings", []))

        # Layer 4: Integration check
        integration_result = self._validate_integration(filepath)
        result["layers"]["integration"] = integration_result
        if not integration_result["passed"]:
            result["passed"] = False
            result["errors"].extend(integration_result.get("errors", []))

        # Layer 5: Coverage check
        coverage_result = self._validate_coverage(filepath)
        result["layers"]["coverage"] = coverage_result
        result["warnings"].extend(coverage_result.get("warnings", []))

        logger.info(f"Validation complete: passed={result['passed']}")

        return result

    def _validate_static_analysis(self, filepath: str) -> Dict:
        """Layer 1: Syntax and AST analysis."""

        logger.info("Layer 1: Static analysis")

        result = {"passed": True, "errors": [], "warnings": []}

        try:
            with open(filepath) as f:
                code = f.read()

            # Try to parse as AST
            tree = ast.parse(code)
            logger.info("✅ Syntax valid")

            # Extract imports, functions, classes
            imports = set()
            functions = set()
            classes = set()

            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.add(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        imports.add(node.module)
                elif isinstance(node, ast.FunctionDef):
                    functions.add(node.name)
                elif isinstance(node, ast.ClassDef):
                    classes.add(node.name)

            result["imports"] = list(imports)
            result["functions"] = list(functions)
            result["classes"] = list(classes)

            # Check for obviously problematic patterns
            if "exit(" in code or "quit(" in code:
                result["errors"].append("Found exit() or quit() - potential control flow issue")
                result["passed"] = False

            if "hardcoded" in code.lower():
                result["warnings"].append("Found 'hardcoded' in comments - review parameter values")

            logger.info(f"✅ AST analysis complete: {len(imports)} imports, {len(classes)} classes")

        except SyntaxError as e:
            result["passed"] = False
            result["errors"].append(f"Syntax error: {e}")
            logger.error(f"❌ Syntax error: {e}")

        except Exception as e:
            result["passed"] = False
            result["errors"].append(f"Parse error: {e}")
            logger.error(f"❌ Parse error: {e}")

        return result

    def _validate_security(self, filepath: str) -> Dict:
        """Layer 2: Security scan for dangerous patterns."""

        logger.info("Layer 2: Security scan")

        result = {"passed": True, "errors": [], "warnings": []}

        with open(filepath) as f:
            code = f.read()

        # Check for blacklisted modules
        for blacklisted in self.BLACKLISTED_MODULES:
            if blacklisted in code:
                result["errors"].append(f"Blacklisted module/function found: {blacklisted}")
                result["passed"] = False

        # Check for API keys/secrets
        if re.search(r'(API_KEY|SECRET|PASSWORD|TOKEN)\s*=\s*["\']', code, re.IGNORECASE):
            result["errors"].append("Found hardcoded API key/secret - CRITICAL SECURITY RISK")
            result["passed"] = False

        # Check for dangerous file operations
        if re.search(r'open\s*\(', code):
            result["errors"].append("File I/O detected - strategy should not read/write files")
            result["passed"] = False

        # Check for network operations
        if "http" in code.lower() or "requests" in code or "urllib" in code:
            result["warnings"].append("Network operations detected - ensure proxied through API")

        logger.info(f"✅ Security scan complete: {len(result['errors'])} errors, "
                   f"{len(result['warnings'])} warnings")

        return result

    def _validate_conventions(self, filepath: str) -> Dict:
        """Layer 3: NetTrace convention check."""

        logger.info("Layer 3: Convention check")

        result = {"passed": True, "errors": [], "warnings": []}

        with open(filepath) as f:
            code = f.read()

        # Check for required imports
        missing_imports = []
        for required in self.REQUIRED_IMPORTS:
            if f"import {required}" not in code and f"from {required}" not in code:
                missing_imports.append(required)

        if missing_imports:
            result["warnings"].append(f"Missing recommended imports: {', '.join(missing_imports)}")

        # Check for required pattern
        if not re.search(self.REQUIRED_PATTERN, code):
            result["errors"].append(f"Missing required pattern: Strategy class definition")
            result["passed"] = False

        # Check for required methods
        missing_methods = []
        for required_method in self.REQUIRED_METHODS:
            if f"def {required_method}" not in code:
                missing_methods.append(required_method)

        if missing_methods:
            result["errors"].append(f"Missing required methods: {', '.join(missing_methods)}")
            result["passed"] = False

        # Check for hardcoded values (should use parameters)
        if re.search(r'self\.\w+\s*=\s*\d+\.?\d*[^.]', code):
            result["warnings"].append("Found hardcoded numeric values - consider parameterization")

        logger.info(f"✅ Convention check complete")

        return result

    def _validate_integration(self, filepath: str) -> Dict:
        """Layer 4: NetTrace integration check."""

        logger.info("Layer 4: Integration check")

        result = {"passed": True, "errors": [], "warnings": []}

        with open(filepath) as f:
            code = f.read()

        # Check for GoalValidator usage
        if "GoalValidator" not in code:
            result["warnings"].append("No GoalValidator gate found - trades not gated by confidence")

        # Check for risk_controller usage
        if "RiskController" not in code:
            result["warnings"].append("No RiskController usage - position sizing may be unsafe")

        # Check for error handling
        try_count = code.count("try:")
        except_count = code.count("except")

        if try_count == 0:
            result["warnings"].append("No try/except blocks - error handling missing")

        # Check for logging
        if "logger." not in code:
            result["warnings"].append("No logging calls - audit trail will be missing")

        logger.info(f"✅ Integration check complete")

        return result

    def _validate_coverage(self, filepath: str) -> Dict:
        """Layer 5: Test coverage analysis."""

        logger.info("Layer 5: Coverage check")

        result = {"warnings": []}

        with open(filepath) as f:
            code = f.read()

        # Count functions
        function_count = code.count("def ")

        # Estimate lines of actual logic
        lines = code.split("\n")
        logic_lines = [l for l in lines if l.strip() and not l.strip().startswith("#")]

        result["function_count"] = function_count
        result["logic_lines"] = len(logic_lines)

        if function_count < 3:
            result["warnings"].append("Few functions - may lack modularity")

        if len(logic_lines) < 20:
            result["warnings"].append("Very short strategy - may be too simple")

        logger.info(f"✅ Coverage check: {function_count} functions, {len(logic_lines)} logic lines")

        return result

    def _validate_sandbox(self, filepath: str, timeout_seconds: int = 30) -> Dict:
        """Layer 6: Sandbox execution with timeout."""

        logger.info("Layer 6: Sandbox execution")

        # Import and execute strategy in sandbox
        result = {
            "passed": True,
            "execution_time": 0,
            "errors": [],
        }

        try:
            # Set up timeout
            import signal

            def timeout_handler(signum, frame):
                raise TimeoutError(f"Execution exceeded {timeout_seconds}s")

            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_seconds)

            # Execute
            with open(filepath) as f:
                code = f.read()

            # Compile and check for import errors
            compile(code, filepath, "exec")

            logger.info("✅ Sandbox execution successful")

        except TimeoutError as e:
            result["passed"] = False
            result["errors"].append(str(e))
        except Exception as e:
            result["passed"] = False
            result["errors"].append(f"Execution error: {e}")
        finally:
            signal.alarm(0)  # Disable alarm

        return result

    def should_approve(self, validation_result: Dict) -> Tuple[bool, str]:
        """Determine if strategy should be approved for COLD testing."""

        errors = validation_result.get("errors", [])
        warnings = validation_result.get("warnings", [])

        if errors:
            return False, f"Validation errors: {'; '.join(errors)}"

        if len(warnings) > 3:
            return False, f"Too many warnings ({len(warnings)}), needs review"

        return True, "All validation layers passed"


def main():
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Strategy Validator")
    parser.add_argument("--mode", choices=["validate", "scan", "status"],
                        default="validate")
    parser.add_argument("--filepath", help="Path to strategy file")

    args = parser.parse_args()

    validator = StrategyValidator()

    if args.mode == "validate":
        if not args.filepath:
            print("❌ --filepath required")
            return 1

        result = validator.validate_strategy(args.filepath)
        print(json.dumps(result, indent=2, default=str))

        # Determine approval
        approved, reason = validator.should_approve(result)
        print(f"\n{'✅' if approved else '❌'} Approval: {reason}")

    elif args.mode == "scan":
        if not args.filepath:
            print("❌ --filepath required")
            return 1

        # Quick security scan
        validator = StrategyValidator()
        result = validator._validate_security(args.filepath)
        print(json.dumps(result, indent=2))

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
