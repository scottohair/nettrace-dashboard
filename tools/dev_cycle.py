#!/usr/bin/env python3.11
"""Fast local validation loop for development iterations."""

from __future__ import annotations

import argparse
import ast
import contextlib
import hashlib
import importlib.util
import inspect
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path
from typing import Any
from unittest import TextTestRunner

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_DIR = REPO_ROOT / "tests"
PY_EXT = ".py"

HAS_PYTEST = False
CACHE_DIR = REPO_ROOT / ".cache" / "dev_cycle"
IMPORT_GRAPH_CACHE = CACHE_DIR / "import_graph.json"


def run_cmd(cmd: list[str], label: str) -> int:
    """Run a command and stream output."""
    print(f"\n[run] {label}")
    print(" ".join(str(item) for item in cmd))
    proc = subprocess.run(cmd, cwd=REPO_ROOT, text=True)
    return proc.returncode


def has_pytest() -> bool:
    return bool(importlib.util.find_spec("pytest"))


class _MiniMonkeyPatch:
    """Minimal subset of pytest.MonkeyPatch."""

    def __init__(self) -> None:
        self._undo = []

    def setattr(self, target, name, value):
        had = hasattr(target, name)
        old = getattr(target, name, None)
        self._undo.append(("setattr", target, name, old, had))
        setattr(target, name, value)

    def setitem(self, mapping, key, value):
        if not hasattr(mapping, "__setitem__"):
            raise TypeError("target does not support item assignment")
        had = key in mapping
        old = mapping.get(key) if hasattr(mapping, "get") else None
        self._undo.append(("setitem", mapping, key, old, had))
        mapping[key] = value

    def delitem(self, mapping, key):
        had = key in mapping
        old = mapping.get(key) if hasattr(mapping, "get") else None
        self._undo.append(("delitem", mapping, key, old, had))
        del mapping[key]

    def setenv(self, name, value):
        had = name in os.environ
        old = os.environ.get(name)
        self._undo.append(("setenv", name, old, had))
        os.environ[name] = value

    def undo(self):
        while self._undo:
            action = self._undo.pop()
            typ = action[0]
            if typ == "setattr":
                _, target, name, old, had = action
                if had:
                    setattr(target, name, old)
                else:
                    delattr(target, name)
            elif typ == "setitem":
                _, mapping, key, old, had = action
                if had:
                    mapping[key] = old
                elif hasattr(mapping, "__delitem__"):
                    del mapping[key]
            elif typ == "delitem":
                _, mapping, key, old, had = action
                if had:
                    mapping[key] = old
            elif typ == "setenv":
                _, name, old, had = action
                if had:
                    os.environ[name] = old
                else:
                    os.environ.pop(name, None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.undo()
        return False


def list_changed_py_files(include_untracked: bool) -> list[Path]:
    """Return changed Python files relative to HEAD."""
    changed = set[str]()
    try:
        tracked = subprocess.check_output(
            ["git", "-C", str(REPO_ROOT), "diff", "--name-only", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        for line in tracked.splitlines():
            if line.strip().endswith(PY_EXT):
                changed.add(line.strip())
    except subprocess.CalledProcessError:
        pass

    if include_untracked:
        try:
            untracked = subprocess.check_output(
                [
                    "git",
                    "-C",
                    str(REPO_ROOT),
                    "ls-files",
                    "--others",
                    "--exclude-standard",
                ],
                text=True,
                stderr=subprocess.DEVNULL,
            )
            for line in untracked.splitlines():
                if line.strip().endswith(PY_EXT):
                    changed.add(line.strip())
                if line.strip().endswith(".py"):
                    changed.add(line.strip())
        except subprocess.CalledProcessError:
            pass

    return sorted((REPO_ROOT / path).resolve() for path in changed if path.strip())


def _repo_relative(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))


def _module_name(path: Path) -> str:
    return ".".join(path.relative_to(REPO_ROOT).with_suffix("").parts)


def _module_index() -> dict[str, Path]:
    """Map module names to paths for project-local Python files."""
    modules: dict[str, Path] = {}
    for file_path in REPO_ROOT.rglob(f"*{PY_EXT}"):
        rel_parts = set(file_path.relative_to(REPO_ROOT).parts)
        if {".git", ".mypy_cache", ".venv", "__pycache__", ".cache"} & rel_parts:
            continue
        if file_path.name == "pycocotools" and file_path.is_dir():
            continue
        modules[_module_name(file_path)] = file_path
    return modules


def _resolve_import_name(raw_name: str, current_module: str, modules: set[str]) -> list[str]:
    candidates: set[str] = set()
    if not raw_name:
        return []
    if raw_name in modules:
        candidates.add(raw_name)
    if "." in raw_name:
        parts = raw_name.split(".")
        for i in range(len(parts) - 1, 0, -1):
            prefix = ".".join(parts[:i])
            if prefix in modules:
                candidates.add(prefix)
    parts = raw_name.split(".")
    if parts:
        if parts[0] in modules:
            candidates.add(parts[0])
        if current_module.endswith(parts[0]) and current_module in modules:
            candidates.add(current_module)
        if current_module in modules and f"{current_module}.{raw_name}" in modules:
            candidates.add(f"{current_module}.{raw_name}")
    return sorted(candidates)


def _extract_imports(file_path: Path, modules: dict[str, Path]) -> set[str]:
    """Best-effort AST import extraction for local-impact mapping."""
    module_names = set(modules.keys())
    try:
        source = file_path.read_text(encoding="utf-8", errors="ignore")
        tree = ast.parse(source)
    except (SyntaxError, OSError, UnicodeDecodeError):
        return set()

    current_module = _module_name(file_path)
    current_parts = current_module.split(".")
    imports: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                for resolved in _resolve_import_name(alias.name, current_module, module_names):
                    imports.add(resolved)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            base_parts = current_parts[: max(len(current_parts) - node.level, 0)]
            if module:
                for resolved in _resolve_import_name(module, current_module, module_names):
                    imports.add(resolved)
                base_prefix = ".".join(base_parts + module.split("."))
            else:
                base_prefix = ".".join(base_parts)
            for alias in node.names:
                if alias.name == "*":
                    continue
                if module:
                    combined = f"{module}.{alias.name}"
                else:
                    combined = alias.name if not base_prefix else f"{base_prefix}.{alias.name}"
                for resolved in _resolve_import_name(combined, current_module, module_names):
                    imports.add(resolved)

    return imports


def _build_import_graph() -> dict[str, set[str]]:
    """Build dependency graph: importer module -> imported local module."""
    modules = _module_index()
    graph: dict[str, set[str]] = {}
    for module_name, module_path in modules.items():
        graph[module_name] = _extract_imports(module_path, modules)
    return graph


def _hash_file(file_path: Path) -> str:
    """Stable signature for cache invalidation."""
    stat = file_path.stat()
    signature = f"{_repo_relative(file_path)}|{stat.st_size}|{int(stat.st_mtime_ns)}"
    return hashlib.sha1(signature.encode()).hexdigest()


def _load_import_graph() -> dict[str, set[str]]:
    """Load cached import graph when possible."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    modules = _module_index()
    module_files = {str(path): _hash_file(path) for path in modules.values()}
    module_items = sorted(module_files.items())
    manifest = json.dumps(module_items, sort_keys=True)

    if IMPORT_GRAPH_CACHE.exists():
        try:
            cached = json.loads(IMPORT_GRAPH_CACHE.read_text(encoding="utf-8"))
            if cached.get("manifest") == manifest:
                raw_graph = cached.get("graph", {})
                return {
                    module: set(deps) for module, deps in raw_graph.items()
                }
        except (OSError, json.JSONDecodeError, ValueError):
            pass

    graph = _build_import_graph()
    payload = {
        "manifest": manifest,
        "graph": {module: sorted(deps) for module, deps in graph.items()},
        "generated_at": time.time(),
    }
    try:
        IMPORT_GRAPH_CACHE.write_text(json.dumps(payload), encoding="utf-8")
    except OSError:
        pass
    return graph


def _reverse_import_graph(graph: dict[str, set[str]]) -> dict[str, set[str]]:
    reverse: dict[str, set[str]] = {}
    for importer, imports in graph.items():
        for imported in imports:
            reverse.setdefault(imported, set()).add(importer)
    return reverse


def map_changed_file_to_tests(changed: list[Path]) -> list[Path]:
    """Map changed modules to candidate tests using static import graph and fallback matching."""
    if not TEST_DIR.exists():
        return []

    all_tests = sorted(TEST_DIR.glob("test*.py"))
    if not all_tests:
        return []

    modules = _module_index()
    module_names = {path: name for name, path in modules.items()}
    reverse_graph = _reverse_import_graph(_load_import_graph())
    test_modules = {
        module
        for module, path in modules.items()
        if path.is_relative_to(TEST_DIR)
    }

    test_contents = {}
    for test in all_tests:
        try:
            test_contents[test] = test.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            test_contents[test] = ""

    direct = []
    affected_modules: set[str] = set()
    for path in changed:
        if path.parent == TEST_DIR and path.name.startswith("test_"):
            direct.append(path.resolve())
            continue
        direct_test = TEST_DIR / f"test_{path.stem}.py"
        if direct_test.exists():
            direct.append(direct_test.resolve())
        if path in module_names:
            affected_modules.add(module_names[path])
            for importer in reverse_graph.get(module_names[path], set()):
                if importer in test_modules:
                    affected_modules.add(importer)

    for test_module in affected_modules:
        if not test_module.startswith("tests."):
            continue
        test_file = modules.get(test_module)
        if test_file:
            direct.append(test_file.resolve())

    # Fallback only runs when direct/static matching is empty.
    # It covers dynamic imports and edge cases without bloating normal signal sets.
    if not direct:
        for path in changed:
            module_name = path.stem
            dotted = _module_name(path)
            stem_re = re.compile(rf"\b{re.escape(module_name)}\b")
            dotted_re = re.compile(rf"\b{re.escape(dotted)}\b")
            for test_file, content in test_contents.items():
                if test_file in direct:
                    continue
                if stem_re.search(content) or dotted_re.search(content):
                    direct.append(test_file)

    # keep tests deterministic
    mapped = sorted(set(direct))
    return [p for p in mapped if p.exists()]


def _compile_file(file_path: Path) -> int:
    import py_compile

    try:
        py_compile.compile(str(file_path), doraise=True)
        return 0
    except (SyntaxError, OSError, UnicodeError) as exc:
        print(f"[compile] syntax error in {file_path}: {exc}")
        return 1


def run_py_compile(files: list[Path], workers: int) -> bool:
    """Compile Python files in parallel. Return True when all compile."""
    if not files:
        return True
    print(f"\n[compile] {len(files)} file(s) with {workers} worker(s)")
    with ThreadPoolExecutor(max_workers=workers) as exe:
        results = list(exe.map(_compile_file, files))
    if any(code != 0 for code in results):
        print("[compile] failed")
        return False
    return True


def _build_fake_pytest():
    class _Mark:
        def parametrize(self, argnames, argvalues, ids=None):  # noqa: ARG002
            def decorator(func):
                existing = getattr(func, "_pytest_parametrize", [])
                cases = list(existing) + [(argnames, argvalues)]
                setattr(func, "_pytest_parametrize", cases)
                return func

            return decorator

        def skip(self, reason):  # noqa: ARG002
            def decorator(func):
                setattr(func, "_pytest_skip_reason", reason)
                return func

            return decorator

    class _Approx:
        def __init__(self, expected: float, *, abs: float | None = None):
            self.expected = expected
            self.abs = 0.0 if abs is None else float(abs)

        def __eq__(self, other: object) -> bool:
            if not isinstance(other, (int, float)):
                return False
            return abs(other - self.expected) <= self.abs

    fake = types.ModuleType("pytest")
    fake.mark = _Mark()
    fake.fixture = lambda *args, **kwargs: (  # noqa: E731
        lambda func: _attach_fixture(func, **kwargs)
    )
    fake.approx = lambda expected, *, abs: _Approx(expected, abs=abs)
    fake.monkeypatch = _MiniMonkeyPatch

    def _attach_fixture(func, *args, **kwargs):
        _ = args, kwargs
        setattr(func, "_pytest_is_fixture", True)
        return func

    return fake


def _iter_param_cases(func):
    specs = getattr(func, "_pytest_parametrize", [])
    if not specs:
        return [()]
    cases = [()]
    for argnames, argvalues in specs:
        names = [name.strip() for name in str(argnames).split(",")]
        expanded = []
        for base in cases:
            for val in argvalues:
                if len(names) == 1:
                    entry = dict(base)
                    entry[names[0]] = val
                    expanded.append(entry)
                else:
                    row = tuple(val)
                    if len(row) != len(names):
                        continue
                    entry = dict(base)
                    entry.update({n: v for n, v in zip(names, row)})
                    expanded.append(entry)
        cases = expanded
    return cases


def _resolve_fixture(
    fn_name: str,
    cache: dict[str, object],
    tmp_path: Path,
    fixtures: dict[str, Any],
):
    if fn_name in cache:
        return cache[fn_name]
    if fn_name not in fixtures:
        raise RuntimeError(f"fixture '{fn_name}' not defined")
    fn = fixtures[fn_name]
    sig = inspect.signature(fn)
    kwargs = {}
    for param in sig.parameters.values():
        if param.name == "tmp_path":
            kwargs[param.name] = tmp_path
        elif param.name == "monkeypatch":
            kwargs[param.name] = _MiniMonkeyPatch()
        elif param.name in cache:
            kwargs[param.name] = cache[param.name]
        elif param.name in fixtures:
            kwargs[param.name] = _resolve_fixture(param.name, cache, tmp_path, fixtures)
        elif param.default is not inspect.Parameter.empty:
            kwargs[param.name] = param.default
    result = fn(**kwargs)
    cache[fn_name] = result
    return result


def _run_function_tests(module, namespace: Path) -> bool:  # noqa: ARG001
    module_functions = {
        name: obj for name, obj in inspect.getmembers(module, inspect.isfunction)
    }
    fixtures = {
        name: fn for name, fn in module_functions.items() if getattr(fn, "_pytest_is_fixture", False)
    }

    ok = True
    for name, fn in module_functions.items():
        if not name.startswith("test_"):
            continue
        if not callable(fn):
            continue
        reason = getattr(fn, "_pytest_skip_reason", None)
        if reason:
            print(f"[test] skip {name}: {reason}")
            continue
        cases = _iter_param_cases(fn)
        if cases == [()]:
            cases = [()]
        for idx, case in enumerate(cases):
            try:
                mp = _build_fake_pytest().monkeypatch()  # type: ignore[call-arg]
            except Exception:
                mp = None
            with tempfile.TemporaryDirectory(prefix="devcycle-") as td:
                tmp_path = Path(td)
                fixture_cache: dict[str, object] = {}
                kwargs = {}
                for param in inspect.signature(fn).parameters.values():
                    if param.name == "tmp_path":
                        kwargs[param.name] = tmp_path
                        continue
                    if param.name == "monkeypatch":
                        kwargs[param.name] = mp
                        continue
                    if param.name in case:
                        kwargs[param.name] = case[param.name]
                        continue
                    if param.name in fixtures:
                        kwargs[param.name] = _resolve_fixture(
                            param.name, fixture_cache, tmp_path, fixtures
                        )
                        continue
                    if param.default is inspect.Parameter.empty and param.kind != inspect.Parameter.VAR_KEYWORD:
                        raise AssertionError(f"Missing fixture/arg '{param.name}' for {name}")

                with contextlib.ExitStack() as stack:
                    if mp is not None:
                        stack.enter_context(mp)
                    try:
                        fn(**kwargs)
                        if len(cases) > 1:
                            print(f"[test] pass {name}[{idx}]")
                    except Exception as exc:
                        print(f"[test] fail {name}[{idx}]: {exc}")
                        import traceback

                        traceback.print_exc()
                        ok = False
            if not ok:
                break
    return ok


def run_functional_test_file(test_file: Path) -> bool:
    fake_pytest = _build_fake_pytest()
    previous = sys.modules.get("pytest")
    sys.modules["pytest"] = fake_pytest
    original_syspath = list(sys.path)
    if str(REPO_ROOT) not in original_syspath:
        sys.path.insert(0, str(REPO_ROOT))
    try:
        loader = importlib.machinery.SourceFileLoader(
            f"dev_cycle_{test_file.stem}_{abs(hash(test_file))}",
            str(test_file),
        )
        spec = importlib.util.spec_from_loader(loader.name, loader)
        if spec is None or spec.loader is None:
            return False
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        loader.exec_module(module)
        return _run_function_tests(module, test_file)
    except Exception as exc:
        print(f"[test] error loading {test_file}: {exc}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        sys.path[:] = original_syspath
        if previous is None:
            sys.modules.pop("pytest", None)
        else:
            sys.modules["pytest"] = previous


def run_unittest_file(test_file: Path) -> bool:
    loader = unittest.defaultTestLoader
    suite = loader.discover(str(test_file.parent), pattern=test_file.name)
    result = TextTestRunner(verbosity=1, stream=sys.stdout).run(suite)
    return result.wasSuccessful()


def run_tests_for_file(test_file: Path) -> bool:
    if HAS_PYTEST:
        return (
            run_cmd([sys.executable, "-m", "pytest", "-q", str(test_file)], "pytest")
            == 0
        )
    return run_functional_test_file(test_file) and run_unittest_file(test_file)


def _run_tests_file_subprocess(test_file: Path) -> bool:
    return (
        run_cmd(
            [
                sys.executable,
                str(REPO_ROOT / "tools/dev_cycle.py"),
                "--internal-test-file",
                str(test_file),
            ],
            f"test {test_file.name} (isolated)",
        )
        == 0
    )


def run_tests(tests: list[Path], workers: int, parallel: bool) -> bool:
    if not tests:
        print("\n[test] no mapped tests found; skipping")
        return True

    print(f"\n[test] running {len(tests)} mapped test file(s)")
    for test in tests:
        print(f" - {test}")

    if not parallel or len(tests) == 1 or HAS_PYTEST:
        for test_file in tests:
            if not run_tests_for_file(test_file):
                return False
        return True

    with ThreadPoolExecutor(max_workers=min(workers, len(tests))) as exe:
        futures = {exe.submit(_run_tests_file_subprocess, test): test for test in tests}
        ok = True
        for future in as_completed(futures):
            if not future.result():
                ok = False
    return ok


def run_full_suite(workers: int, parallel: bool) -> bool:
    if not TEST_DIR.exists():
        return True
    test_files = sorted(TEST_DIR.glob("test*.py"))
    if not test_files:
        return True
    print(f"\n[suite] running full suite ({len(test_files)} files)")
    return run_tests(list(test_files), workers=workers, parallel=parallel)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast local validation workflow.")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full suite after mapped tests.",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=6,
        help="Parallel workers for compile/tests.",
    )
    parser.add_argument(
        "--include-untracked",
        action="store_true",
        help="Include untracked Python files in changed-file scan.",
    )
    parser.add_argument(
        "--internal-test-file",
        type=Path,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--no-parallel-tests",
        action="store_true",
        help="Disable test subprocess parallelization.",
    )
    return parser.parse_args()


def main() -> int:
    global HAS_PYTEST
    HAS_PYTEST = has_pytest()
    args = parse_args()

    if args.internal_test_file is not None:
        return 0 if run_tests_for_file(args.internal_test_file) else 1

    changed_files = list_changed_py_files(args.include_untracked)
    changed_files = [f for f in changed_files if f.exists() and not f.name.startswith(".")]
    print(f"[scan] changed files: {len(changed_files)}")
    if not changed_files:
        print("[scan] no changed python files found.")
    else:
        for changed_file in changed_files:
            print(f" - {changed_file}")

    compile_workers = max(1, min(args.jobs, 8))
    t0 = time.perf_counter()
    if not run_py_compile(changed_files, compile_workers):
        return 1
    compile_seconds = time.perf_counter() - t0

    map_workers = max(1, min(args.jobs, 8))
    t1 = time.perf_counter()
    mapped_tests = map_changed_file_to_tests(changed_files)
    mapping_seconds = time.perf_counter() - t1

    test_workers = max(1, min(args.jobs, 8))
    t2 = time.perf_counter()
    if not run_tests(
        mapped_tests,
        workers=test_workers,
        parallel=not args.no_parallel_tests,
    ):
        return 1
    mapped_test_seconds = time.perf_counter() - t2

    if args.full:
        t3 = time.perf_counter()
        if not run_full_suite(max(1, min(args.jobs, 8)), parallel=not args.no_parallel_tests):
            return 1
        full_suite_seconds = time.perf_counter() - t3
    else:
        full_suite_seconds = 0.0

    print(
        "\n[devcycle] timing s | compile={0:.2f} | map={1:.2f} | mapped_tests={2:.2f} | full_suite={3:.2f}".format(
            compile_seconds, mapping_seconds, mapped_test_seconds, full_suite_seconds
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
