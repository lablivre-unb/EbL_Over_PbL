"""Local coverage executors — Priority 3 in the extraction waterfall.

Each executor handles one language/build-system combination:
  PythonExecutor      python  / pytest
  JavaMavenExecutor   java    / maven
  JavaGradleExecutor  java    / gradle
  JavaScriptExecutor  js, ts  / npm or yarn
  GoExecutor          go      / go

ExecutorFactory.get(language, build_system) returns the right executor or None.

All executors accept a _subprocess_run parameter for dependency injection in
tests; production code uses subprocess.run by default.

Timeouts
--------
Each executor receives an exec_timeout (seconds) applied per subprocess call.
subprocess.TimeoutExpired is re-raised as ExecutionTimeout so callers can
distinguish it from generic execution failures.
"""
import logging
import shutil
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from journal.extractors.coverage.models import CoverageMetrics
from journal.extractors.coverage.parsers import (
    parse_go_coverprofile,
    parse_go_cover_func,
    parse_jacoco_xml,
    parse_lcov,
    parse_pytest_json,
)

logger = logging.getLogger(__name__)


class ExecutionTimeout(Exception):
    """Raised when a subprocess exceeds its time limit."""


@dataclass
class ExecutionResult:
    metrics: CoverageMetrics
    method: str       # full command string for reproducibility
    provider: str     # tool name, e.g. "pytest-cov"


# ---------------------------------------------------------------------------
# Base executor
# ---------------------------------------------------------------------------

class BaseExecutor(ABC):
    language: str
    provider_name: str

    def __init__(self, _subprocess_run: Optional[Callable] = None) -> None:
        self._run = _subprocess_run or subprocess.run

    @abstractmethod
    def run(
        self,
        repo_path: Path,
        exec_timeout: int = 300,
        install_deps: bool = True,
    ) -> ExecutionResult:
        """Execute coverage measurement and return an ExecutionResult."""

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _exec(
        self, cmd: List[str], cwd: Path, timeout: int, check: bool = False
    ) -> subprocess.CompletedProcess:
        """Run a command. Raises ExecutionTimeout on timeout."""
        try:
            return self._run(
                cmd,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=check,
            )
        except subprocess.TimeoutExpired as exc:
            raise ExecutionTimeout(
                f"Command timed out after {timeout}s: {' '.join(cmd)}"
            ) from exc


# ---------------------------------------------------------------------------
# Python
# ---------------------------------------------------------------------------

class PythonExecutor(BaseExecutor):
    language = "python"
    provider_name = "pytest-cov"

    def run(
        self, repo_path: Path, exec_timeout: int = 300, install_deps: bool = True
    ) -> ExecutionResult:
        cmd = ["pytest", "--cov=.", "--cov-report=json", "--no-header", "-q",
               "--tb=no", "-p", "no:warnings"]
        method = " ".join(cmd)

        if install_deps:
            self._install_python_deps(repo_path, exec_timeout)

        # Run pytest; tolerate test failures — we still get coverage data
        result = self._exec(cmd, repo_path, exec_timeout)

        coverage_file = repo_path / "coverage.json"
        if not coverage_file.exists():
            stderr_snippet = (result.stderr or "")[:500]
            raise RuntimeError(
                f"coverage.json not found after pytest (exit {result.returncode}). "
                f"stderr: {stderr_snippet}"
            )

        metrics = parse_pytest_json(coverage_file.read_text())
        return ExecutionResult(metrics=metrics, method=method, provider=self.provider_name)

    def _install_python_deps(self, repo_path: Path, timeout: int) -> None:
        install_timeout = min(timeout, 180)

        # Ensure pytest-cov is available
        self._exec(["pip", "install", "pytest", "pytest-cov", "-q"],
                   repo_path, install_timeout)

        req_file = repo_path / "requirements.txt"
        if req_file.exists():
            self._exec(["pip", "install", "-r", "requirements.txt", "-q"],
                       repo_path, install_timeout)
            return

        # Try editable install with optional test extras
        for extra in ["[test,testing,dev]", "[test]", ""]:
            pkg = f".[{extra.strip('[]')}]" if extra else "."
            try:
                self._exec(["pip", "install", "-e", pkg, "-q"], repo_path, install_timeout)
                return
            except Exception:
                continue


# ---------------------------------------------------------------------------
# Java — Maven
# ---------------------------------------------------------------------------

class JavaMavenExecutor(BaseExecutor):
    language = "java"
    provider_name = "jacoco"

    # Runs JaCoCo even when it is not declared in the project's pom.xml
    _JACOCO_VERSION = "0.8.11"
    _JACOCO_PLUGIN = f"org.jacoco:jacoco-maven-plugin:{_JACOCO_VERSION}"

    def run(
        self, repo_path: Path, exec_timeout: int = 300, install_deps: bool = True
    ) -> ExecutionResult:
        jacoco_xml = self._find_jacoco_report(repo_path)
        if jacoco_xml and jacoco_xml.exists():
            metrics = parse_jacoco_xml(jacoco_xml.read_text())
            return ExecutionResult(
                metrics=metrics,
                method="pre-existing jacoco.xml",
                provider=self.provider_name,
            )

        mvn_cmd = self._resolve_mvn(repo_path)

        cmd = [
            mvn_cmd, "-B", "-q",
            f"{self._JACOCO_PLUGIN}:prepare-agent",
            "test",
            f"{self._JACOCO_PLUGIN}:report",
            "-Dmaven.test.failure.ignore=true",
        ]
        method = " ".join(cmd)
        self._exec(cmd, repo_path, exec_timeout)

        jacoco_xml = self._find_jacoco_report(repo_path)
        if jacoco_xml is None or not jacoco_xml.exists():
            raise RuntimeError("jacoco.xml not found after mvn test")

        metrics = parse_jacoco_xml(jacoco_xml.read_text())
        return ExecutionResult(metrics=metrics, method=method, provider=self.provider_name)

    @staticmethod
    def _resolve_mvn(repo_path: Path) -> str:
        """Return path to mvnw wrapper if present, else 'mvn' (or raise if absent)."""
        mvnw = repo_path / "mvnw"
        if mvnw.exists():
            mvnw.chmod(mvnw.stat().st_mode | 0o111)
            return str(mvnw)
        if shutil.which("mvn") is None:
            raise RuntimeError("'mvn' not found in PATH and no 'mvnw' wrapper in repository")
        return "mvn"

    @staticmethod
    def _find_jacoco_report(repo_path: Path) -> Optional[Path]:
        candidates = [
            repo_path / "target" / "site" / "jacoco" / "jacoco.xml",
            repo_path / "target" / "jacoco.xml",
        ]
        for c in candidates:
            if c.exists():
                return c
        # Search recursively (multi-module projects)
        hits = list(repo_path.rglob("jacoco.xml"))
        return hits[0] if hits else None


# ---------------------------------------------------------------------------
# Java — Gradle
# ---------------------------------------------------------------------------

class JavaGradleExecutor(BaseExecutor):
    language = "java"
    provider_name = "jacoco"

    def run(
        self, repo_path: Path, exec_timeout: int = 300, install_deps: bool = True
    ) -> ExecutionResult:
        gradlew = repo_path / "gradlew"
        gradle_cmd = str(gradlew) if gradlew.exists() else "gradle"

        cmd = [gradle_cmd, "test", "jacocoTestReport", "--no-daemon", "-q",
               "-x", "check"]
        method = " ".join(cmd)

        if gradlew.exists():
            gradlew.chmod(gradlew.stat().st_mode | 0o111)  # ensure executable

        self._exec(cmd, repo_path, exec_timeout)

        # Gradle multi-project may produce multiple reports
        candidates = list(repo_path.rglob("jacocoTestReport.xml"))
        if not candidates:
            raise RuntimeError("jacocoTestReport.xml not found after gradle test")

        metrics = parse_jacoco_xml(candidates[0].read_text())
        return ExecutionResult(metrics=metrics, method=method, provider=self.provider_name)


# ---------------------------------------------------------------------------
# JavaScript / TypeScript
# ---------------------------------------------------------------------------

class JavaScriptExecutor(BaseExecutor):
    language = "javascript"  # also handles typescript
    provider_name = "jest"

    def run(
        self, repo_path: Path, exec_timeout: int = 300, install_deps: bool = True
    ) -> ExecutionResult:
        if install_deps:
            self._install_js_deps(repo_path, exec_timeout)

        for cmd, method in [
            (["npm", "test", "--", "--coverage", "--ci",
               "--coverageReporters=lcov", "--coverageReporters=json-summary",
               "--passWithNoTests"],
             "npm test -- --coverage --ci"),
            (["npx", "jest", "--coverage", "--ci",
               "--coverageReporters=lcov", "--coverageReporters=json-summary",
               "--passWithNoTests"],
             "npx jest --coverage --ci"),
        ]:
            result = self._exec(cmd, repo_path, exec_timeout)
            metrics = self._parse_js_output(repo_path)
            if metrics is not None:
                return ExecutionResult(metrics=metrics, method=method, provider=self.provider_name)

        raise RuntimeError("No parseable coverage output from npm/jest")

    def _install_js_deps(self, repo_path: Path, timeout: int) -> None:
        lock_file = repo_path / "yarn.lock"
        if lock_file.exists():
            self._exec(["yarn", "install", "--frozen-lockfile", "--silent"],
                       repo_path, timeout)
        else:
            self._exec(["npm", "ci", "--silent"], repo_path, timeout)

    @staticmethod
    def _parse_js_output(repo_path: Path) -> Optional[CoverageMetrics]:
        lcov_path = repo_path / "coverage" / "lcov.info"
        if lcov_path.exists():
            return parse_lcov(lcov_path.read_text())
        return None


# ---------------------------------------------------------------------------
# Go
# ---------------------------------------------------------------------------

class GoExecutor(BaseExecutor):
    language = "go"
    provider_name = "go-cover"

    def run(
        self, repo_path: Path, exec_timeout: int = 300, install_deps: bool = True
    ) -> ExecutionResult:
        cover_out = repo_path / "coverage.out"

        test_cmd = [
            "go", "test", "./...",
            f"-coverprofile={cover_out}",
            "-covermode=atomic",
        ]
        method = " ".join(test_cmd)
        self._exec(test_cmd, repo_path, exec_timeout)

        if not cover_out.exists():
            raise RuntimeError("coverage.out not generated by go test")

        func_result = self._exec(
            ["go", "tool", "cover", f"-func={cover_out}"],
            repo_path,
            30,
        )

        # Prefer statement-based percentage from `go tool cover -func`
        func_metrics = parse_go_cover_func(func_result.stdout)
        # Get statement counts from the coverprofile
        profile_metrics = parse_go_coverprofile(cover_out.read_text())

        return ExecutionResult(
            metrics=CoverageMetrics(
                coverage_percent=func_metrics.coverage_percent,
                lines_covered=profile_metrics.lines_covered,
                lines_missed=profile_metrics.lines_missed,
                branches_covered=None,
                branches_missed=None,
            ),
            method=method,
            provider=self.provider_name,
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

class ExecutorFactory:
    """Return the appropriate executor for (language, build_system)."""

    _REGISTRY: dict = {
        ("python",     "pytest"):    PythonExecutor,
        ("python",     "unittest"):  PythonExecutor,
        ("python",     None):        PythonExecutor,
        ("java",       "maven"):     JavaMavenExecutor,
        ("java",       "gradle"):    JavaGradleExecutor,
        ("java",       None):        JavaMavenExecutor,   # try maven first
        ("javascript", "npm"):       JavaScriptExecutor,
        ("javascript", "yarn"):      JavaScriptExecutor,
        ("javascript", None):        JavaScriptExecutor,
        ("typescript", "npm"):       JavaScriptExecutor,
        ("typescript", "yarn"):      JavaScriptExecutor,
        ("typescript", None):        JavaScriptExecutor,
        ("go",         "go"):        GoExecutor,
        ("go",         None):        GoExecutor,
    }

    @classmethod
    def get(
        cls,
        language: Optional[str],
        build_system: Optional[str],
        _subprocess_run: Optional[Callable] = None,
    ) -> Optional[BaseExecutor]:
        if language is None:
            return None
        executor_cls = (
            cls._REGISTRY.get((language, build_system))
            or cls._REGISTRY.get((language, None))
        )
        if executor_cls is None:
            return None
        return executor_cls(_subprocess_run=_subprocess_run)
