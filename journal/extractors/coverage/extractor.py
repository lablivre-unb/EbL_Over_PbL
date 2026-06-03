"""Main coverage extraction orchestrator.

Priority waterfall
------------------
1. Online services  — Codecov, then Coveralls (no code execution)
2. Remote artifacts — known coverage files fetched via the platform REST API
3. Local execution  — clone + detect language + run test suite

Any failure at a priority level falls through to the next.
If all three fail, a CoverageResult with coverage_source="unavailable" is
returned and the main extraction workflow is never interrupted.

Reproducibility fields captured for every extraction
----------------------------------------------------
  repository, platform, commit_sha, default_branch,
  coverage_collection_date, coverage_source, tool_used, execution_status

These are sufficient to re-run the same measurement on the same code state.
"""
import base64
import logging
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

from journal.extractors.coverage.detector import LanguageDetector, ProviderDetector
from journal.extractors.coverage.executors import ExecutionTimeout, ExecutorFactory
from journal.extractors.coverage.models import CoverageMetrics, CoverageResult
from journal.extractors.coverage.parsers import PARSERS
from journal.extractors.coverage.providers import CodecovProvider, CoverallsProvider, GitLabCIProvider

logger = logging.getLogger(__name__)

# Coverage files to probe in the repository tree (checked in priority order)
_ARTIFACT_CANDIDATES: List[str] = [
    "coverage.json",
    "coverage.xml",
    "cobertura.xml",
    "lcov.info",
    "jacoco.xml",
    "coverage/lcov.info",
    "coverage/coverage-summary.json",
    "target/site/jacoco/jacoco.xml",
    "build/reports/jacoco/test/jacocoTestReport.xml",
]

_REST_TIMEOUT = 20   # seconds for REST API calls
_CLONE_TIMEOUT = 300  # seconds for git clone


class CoverageExtractor:
    """Extract coverage for one or many repositories following the priority waterfall."""

    def __init__(
        self,
        github_token: str = "",
        gitlab_token: str = "",
        codecov_token: str = "",
        exec_timeout: int = 300,
        install_deps: bool = True,
    ) -> None:
        self.github_token = github_token
        self.gitlab_token = gitlab_token
        self.exec_timeout = exec_timeout
        self.install_deps = install_deps

        self._providers = [
            CodecovProvider(token=codecov_token),
            CoverallsProvider(),
            GitLabCIProvider(token=gitlab_token),
        ]
        self._provider_detector = ProviderDetector()
        self._language_detector = LanguageDetector()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(
        self,
        org: str,
        repo: str,
        platform: str = "github",
        skip_local: bool = False,
        manual_coverage: Optional[float] = None,
        manual_coverage_source: str = "manual",
    ) -> CoverageResult:
        """Extract coverage for a single repository.

        manual_coverage: if set, returned immediately as Priority 0 without
          any network calls.  Useful for repos that don't publish coverage
          anywhere but whose value is known (e.g. measured once manually, or
          taken from an upstream project as a proxy).

        skip_local=True skips Priority 3 (clone + run tests).  Use for repos
        where local execution is known to be infeasible (no build tool, huge
        test suite) and no online service has the data either.
        """
        repo_slug = f"{org}/{repo}"
        collection_date = datetime.now(timezone.utc).isoformat()

        # ------ Priority 0: manually pinned value ---------------------------
        if manual_coverage is not None:
            logger.info(
                "%s: using pinned coverage %.1f%% [%s]",
                repo_slug, manual_coverage, manual_coverage_source,
            )
            return CoverageResult.from_metrics(
                repository=repo_slug,
                platform=platform,
                metrics=CoverageMetrics(coverage_percent=round(float(manual_coverage), 2)),
                coverage_source="manual",
                coverage_provider=manual_coverage_source,
                coverage_generation_method=None,
                coverage_collection_date=collection_date,
                tool_used=None,
                commit_sha=None,
                default_branch=None,
            )

        metadata = self._fetch_repo_metadata(org, repo, platform)
        commit_sha = metadata.get("commit_sha")
        default_branch = metadata.get("default_branch")

        # ------ Priority 1: online services --------------------------------
        for provider in self._providers:
            try:
                metrics = provider.fetch(org, repo, platform)
                if metrics is not None:
                    logger.info(
                        "%s: coverage %.1f%% via %s",
                        repo_slug, metrics.coverage_percent or 0, provider.provider_name,
                    )
                    return CoverageResult.from_metrics(
                        repository=repo_slug,
                        platform=platform,
                        metrics=metrics,
                        coverage_source=provider.source_name,
                        coverage_provider=provider.provider_name,
                        coverage_generation_method=None,
                        coverage_collection_date=collection_date,
                        tool_used=provider.provider_name,
                        commit_sha=commit_sha,
                        default_branch=default_branch,
                    )
            except Exception as exc:
                logger.debug("Provider %s failed for %s: %s", provider.provider_name, repo_slug, exc)

        # ------ Priority 2: remote artifacts --------------------------------
        try:
            artifact = self._check_remote_artifacts(
                org, repo, platform, default_branch, collection_date,
                commit_sha, repo_slug,
            )
            if artifact is not None:
                return artifact
        except Exception as exc:
            logger.debug("Remote artifact check failed for %s: %s", repo_slug, exc)

        # ------ Priority 3: local execution ---------------------------------
        if skip_local:
            return CoverageResult.unavailable(
                repo_slug, platform, "local execution skipped (skip_local=True)",
                commit_sha=commit_sha, default_branch=default_branch,
                execution_status="skipped",
            )
        try:
            return self._extract_locally(
                org, repo, platform, commit_sha, default_branch, collection_date, repo_slug
            )
        except ExecutionTimeout as exc:
            logger.warning("Timeout extracting %s: %s", repo_slug, exc)
            return CoverageResult.unavailable(
                repo_slug, platform, str(exc),
                commit_sha=commit_sha, default_branch=default_branch,
                execution_status="timeout",
            )
        except Exception as exc:
            logger.warning("Local extraction failed for %s: %s", repo_slug, exc)
            return CoverageResult.unavailable(
                repo_slug, platform, str(exc),
                commit_sha=commit_sha, default_branch=default_branch,
                execution_status="failed",
            )

    def extract_batch(
        self, repos: List[Dict[str, str]]
    ) -> List[CoverageResult]:
        """Extract coverage for multiple repositories.

        Each entry in repos must have keys: org, repo, and optionally platform.
        """
        results = []
        for entry in repos:
            org = entry["org"]
            repo = entry["repo"]
            platform = entry.get("platform", "github")
            skip_local = entry.get("skip_local", False)
            manual_coverage = entry.get("manual_coverage")
            manual_source = entry.get("manual_coverage_source", "manual")
            logger.info("Processing %s/%s [%s]", org, repo, platform)
            result = self.extract(
                org, repo, platform,
                skip_local=skip_local,
                manual_coverage=manual_coverage,
                manual_coverage_source=manual_source,
            )
            results.append(result)
        return results

    # ------------------------------------------------------------------
    # Priority 2: remote artifacts
    # ------------------------------------------------------------------

    def _check_remote_artifacts(
        self,
        org: str,
        repo: str,
        platform: str,
        default_branch: Optional[str],
        collection_date: str,
        commit_sha: Optional[str],
        repo_slug: str,
    ) -> Optional[CoverageResult]:
        branch = default_branch or "main"

        for artifact_path in _ARTIFACT_CANDIDATES:
            parser = PARSERS.get(artifact_path) or PARSERS.get(Path(artifact_path).name)
            if parser is None:
                continue

            content = self._fetch_file(org, repo, platform, artifact_path, branch)
            if content is None:
                continue

            try:
                metrics = parser(content)
                if metrics.coverage_percent is None:
                    continue
                logger.info(
                    "%s: coverage %.1f%% via artifact %s",
                    repo_slug, metrics.coverage_percent, artifact_path,
                )
                return CoverageResult.from_metrics(
                    repository=repo_slug,
                    platform=platform,
                    metrics=metrics,
                    coverage_source="repository_artifact",
                    coverage_provider=artifact_path,
                    coverage_generation_method=None,
                    coverage_collection_date=collection_date,
                    tool_used=f"parser:{Path(artifact_path).name}",
                    commit_sha=commit_sha,
                    default_branch=default_branch,
                )
            except Exception as exc:
                logger.debug("Could not parse %s for %s: %s", artifact_path, repo_slug, exc)
                continue

        return None

    # ------------------------------------------------------------------
    # Priority 3: local execution
    # ------------------------------------------------------------------

    def _extract_locally(
        self,
        org: str,
        repo: str,
        platform: str,
        commit_sha: Optional[str],
        default_branch: Optional[str],
        collection_date: str,
        repo_slug: str,
    ) -> CoverageResult:
        clone_url = self._get_clone_url(org, repo, platform)

        with tempfile.TemporaryDirectory(prefix="journal_cov_") as tmpdir:
            repo_path = Path(tmpdir)
            logger.info("Cloning %s ...", clone_url)
            self._clone(clone_url, repo_path)

            # Capture actual commit SHA if not yet known
            if commit_sha is None:
                commit_sha = self._git_head_sha(repo_path)

            # Check for committed coverage files before running tests
            file_list = [str(p.relative_to(repo_path)) for p in repo_path.rglob("*") if p.is_file()]
            for artifact_path in _ARTIFACT_CANDIDATES:
                fpath = repo_path / artifact_path
                if not fpath.exists():
                    continue
                parser = PARSERS.get(artifact_path) or PARSERS.get(fpath.name)
                if parser is None:
                    continue
                try:
                    metrics = parser(fpath.read_text())
                    if metrics.coverage_percent is not None:
                        logger.info("%s: coverage %.1f%% from committed artifact %s",
                                    repo_slug, metrics.coverage_percent, artifact_path)
                        return CoverageResult.from_metrics(
                            repository=repo_slug,
                            platform=platform,
                            metrics=metrics,
                            coverage_source="repository_artifact",
                            coverage_provider=artifact_path,
                            coverage_generation_method=None,
                            coverage_collection_date=collection_date,
                            tool_used=f"parser:{fpath.name}",
                            commit_sha=commit_sha,
                            default_branch=default_branch,
                        )
                except Exception:
                    pass

            # Detect language and get executor
            language, build_system = self._language_detector.detect(file_list)
            if language is None:
                return CoverageResult.unavailable(
                    repo_slug, platform, "Could not detect repository language",
                    commit_sha=commit_sha, default_branch=default_branch,
                    execution_status="unsupported",
                )

            executor = ExecutorFactory.get(language, build_system)
            if executor is None:
                return CoverageResult.unavailable(
                    repo_slug, platform,
                    f"No executor available for {language}/{build_system}",
                    commit_sha=commit_sha, default_branch=default_branch,
                    language=language, build_system=build_system,
                    execution_status="unsupported",
                )

            logger.info("%s: executing %s/%s coverage locally", repo_slug, language, build_system)
            exec_result = executor.run(
                repo_path,
                exec_timeout=self.exec_timeout,
                install_deps=self.install_deps,
            )

            logger.info(
                "%s: coverage %.1f%% via local execution (%s)",
                repo_slug,
                exec_result.metrics.coverage_percent or 0,
                exec_result.provider,
            )
            return CoverageResult.from_metrics(
                repository=repo_slug,
                platform=platform,
                metrics=exec_result.metrics,
                coverage_source="local_execution",
                coverage_provider=exec_result.provider,
                coverage_generation_method=exec_result.method,
                coverage_collection_date=collection_date,
                tool_used=exec_result.provider,
                commit_sha=commit_sha,
                default_branch=default_branch,
                language=language,
                build_system=build_system,
            )

    # ------------------------------------------------------------------
    # Platform REST helpers
    # ------------------------------------------------------------------

    def _fetch_repo_metadata(
        self, org: str, repo: str, platform: str
    ) -> Dict[str, Optional[str]]:
        """Return {commit_sha, default_branch}. Empty dict on failure."""
        try:
            if platform.lower() == "github":
                return self._github_repo_metadata(org, repo)
            if platform.lower() == "gitlab":
                return self._gitlab_repo_metadata(org, repo)
        except Exception as exc:
            logger.debug("Could not fetch metadata for %s/%s: %s", org, repo, exc)
        return {}

    def _github_repo_metadata(self, org: str, repo: str) -> Dict[str, Optional[str]]:
        resp = requests.get(
            f"https://api.github.com/repos/{org}/{repo}",
            headers=self._github_headers(),
            timeout=_REST_TIMEOUT,
        )
        if not resp.ok:
            return {}
        data = resp.json()
        branch = data.get("default_branch")
        sha = None
        if branch:
            br = requests.get(
                f"https://api.github.com/repos/{org}/{repo}/branches/{branch}",
                headers=self._github_headers(),
                timeout=_REST_TIMEOUT,
            )
            if br.ok:
                sha = (br.json().get("commit") or {}).get("sha")
        return {"commit_sha": sha, "default_branch": branch}

    def _gitlab_repo_metadata(self, org: str, repo: str) -> Dict[str, Optional[str]]:
        full_path = f"{org}/{repo}".replace("/", "%2F")
        resp = requests.get(
            f"https://gitlab.com/api/v4/projects/{full_path}",
            headers=self._gitlab_headers(),
            timeout=_REST_TIMEOUT,
        )
        if not resp.ok:
            return {}
        data = resp.json()
        branch = data.get("default_branch")
        sha = None
        if branch:
            branch_resp = requests.get(
                f"https://gitlab.com/api/v4/projects/{full_path}/repository/branches/{branch}",
                headers=self._gitlab_headers(),
                timeout=_REST_TIMEOUT,
            )
            if branch_resp.ok:
                sha = (branch_resp.json().get("commit") or {}).get("id")
        return {"commit_sha": sha, "default_branch": branch}

    def _fetch_file(
        self,
        org: str,
        repo: str,
        platform: str,
        path: str,
        branch: str,
    ) -> Optional[str]:
        """Fetch raw content of a file from the remote repository. Returns None if not found."""
        try:
            if platform.lower() == "github":
                return self._github_file(org, repo, path, branch)
            if platform.lower() == "gitlab":
                return self._gitlab_file(org, repo, path, branch)
        except Exception as exc:
            logger.debug("Could not fetch %s from %s/%s: %s", path, org, repo, exc)
        return None

    def _github_file(self, org: str, repo: str, path: str, branch: str) -> Optional[str]:
        resp = requests.get(
            f"https://api.github.com/repos/{org}/{repo}/contents/{path}",
            headers=self._github_headers(),
            params={"ref": branch},
            timeout=_REST_TIMEOUT,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        if data.get("encoding") == "base64":
            return base64.b64decode(data["content"]).decode("utf-8", errors="replace")
        return data.get("content")

    def _gitlab_file(self, org: str, repo: str, path: str, branch: str) -> Optional[str]:
        full_path = f"{org}/{repo}".replace("/", "%2F")
        encoded_path = path.replace("/", "%2F")
        resp = requests.get(
            f"https://gitlab.com/api/v4/projects/{full_path}/repository/files/{encoded_path}/raw",
            headers=self._gitlab_headers(),
            params={"ref": branch},
            timeout=_REST_TIMEOUT,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.text

    # ------------------------------------------------------------------
    # Git helpers
    # ------------------------------------------------------------------

    def _get_clone_url(self, org: str, repo: str, platform: str) -> str:
        if platform.lower() == "github":
            return f"https://github.com/{org}/{repo}.git"
        if platform.lower() == "gitlab":
            return f"https://gitlab.com/{org}/{repo}.git"
        return f"https://github.com/{org}/{repo}.git"

    def _clone(self, url: str, target: Path) -> None:
        result = subprocess.run(
            ["git", "clone", "--depth=1", "--single-branch", url, str(target)],
            capture_output=True,
            text=True,
            timeout=_CLONE_TIMEOUT,
        )
        if result.returncode != 0:
            raise RuntimeError(f"git clone failed: {result.stderr[:300]}")

    @staticmethod
    def _git_head_sha(repo_path: Path) -> Optional[str]:
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo_path,
                capture_output=True,
                text=True,
                timeout=10,
            )
            return result.stdout.strip() or None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # HTTP header builders
    # ------------------------------------------------------------------

    def _github_headers(self) -> Dict[str, str]:
        h: Dict[str, str] = {"Accept": "application/vnd.github.v3+json"}
        if self.github_token:
            h["Authorization"] = f"Bearer {self.github_token}"
        return h

    def _gitlab_headers(self) -> Dict[str, str]:
        h: Dict[str, str] = {}
        if self.gitlab_token:
            h["PRIVATE-TOKEN"] = self.gitlab_token
        return h
