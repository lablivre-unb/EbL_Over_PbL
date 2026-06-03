"""Coverage service providers.

Priority 1 in the extraction waterfall.
Each provider contacts a public API before any code is executed locally.

Adding a new provider
---------------------
1. Subclass BaseProvider.
2. Set class attributes source_name and provider_name.
3. Implement fetch().
4. Register in CoverageExtractor._providers.
"""
import logging
import time
from abc import ABC, abstractmethod
from typing import Optional, Tuple

import requests

from journal.extractors.coverage.models import CoverageMetrics

logger = logging.getLogger(__name__)

_TIMEOUT = 20  # seconds per HTTP request
_RETRY_DELAYS: Tuple[int, ...] = (2, 4)  # seconds between retry attempts on ConnectionError


class BaseProvider(ABC):
    source_name: str    # stored in CoverageResult.coverage_source
    provider_name: str  # stored in CoverageResult.coverage_provider

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self._session = session or requests.Session()

    @abstractmethod
    def fetch(
        self, org: str, repo: str, platform: str = "github"
    ) -> Optional[CoverageMetrics]:
        """Fetch coverage metrics from the service.

        Returns None when:
        - the repository is not found (404)
        - no coverage data has been reported yet
        - the service is unavailable (network error / unexpected response)
        """

    def _get(self, url: str, **kwargs) -> requests.Response:
        """GET with automatic retry on transient ConnectionError.

        Retries up to len(_RETRY_DELAYS) times with delays between attempts.
        Any non-connection RequestException is raised immediately (no retry).
        """
        kwargs.setdefault("timeout", _TIMEOUT)
        last_exc: Exception = RuntimeError("unreachable")
        for attempt, delay in enumerate((0,) + _RETRY_DELAYS):
            if delay:
                time.sleep(delay)
            try:
                return self._session.get(url, **kwargs)
            except requests.exceptions.ConnectionError as exc:
                last_exc = exc
                logger.debug(
                    "Transient connection error for %s (attempt %d/%d): %s",
                    url, attempt + 1, len(_RETRY_DELAYS) + 1, exc,
                )
        raise last_exc


class CodecovProvider(BaseProvider):
    """Fetches coverage from the Codecov v2 API.

    Public repositories do not require a token.
    Set CODECOV_TOKEN for private repositories or higher rate limits.

    API reference: https://api.codecov.io/api/v2/
    """

    source_name = "codecov"
    provider_name = "Codecov"

    _SERVICE_MAP = {"github": "github", "gitlab": "gitlab", "bitbucket": "bitbucket"}

    def __init__(self, token: str = "", session: Optional[requests.Session] = None) -> None:
        super().__init__(session=session)
        if token:
            self._session.headers["Authorization"] = f"Bearer {token}"

    def fetch(
        self, org: str, repo: str, platform: str = "github"
    ) -> Optional[CoverageMetrics]:
        service = self._SERVICE_MAP.get(platform.lower(), "github")
        url = f"https://api.codecov.io/api/v2/{service}/{org}/repos/{repo}/"

        try:
            resp = self._get(url)
        except requests.RequestException as exc:
            logger.warning("Codecov network error for %s/%s: %s", org, repo, exc)
            return None

        if resp.status_code == 404:
            logger.debug("Codecov: %s/%s not found", org, repo)
            return None
        if not resp.ok:
            logger.warning("Codecov HTTP %d for %s/%s", resp.status_code, org, repo)
            return None

        try:
            data = resp.json()
        except ValueError as exc:
            logger.warning("Codecov JSON parse error for %s/%s: %s", org, repo, exc)
            return None

        totals = data.get("totals") or {}
        raw_pct = totals.get("coverage")
        if raw_pct is None:
            return None

        try:
            pct = float(raw_pct)
        except (ValueError, TypeError):
            return None

        hits = totals.get("hits")
        misses = totals.get("misses")

        return CoverageMetrics(
            coverage_percent=round(pct, 2),
            lines_covered=int(hits) if hits is not None else None,
            lines_missed=int(misses) if misses is not None else None,
            branches_covered=None,
            branches_missed=None,
        )


class CoverallsProvider(BaseProvider):
    """Fetches coverage from the Coveralls API.

    Works for public GitHub repositories.  GitLab support on Coveralls is
    limited; the request is still attempted but may return 404.

    API reference: https://coveralls.io/
    """

    source_name = "coveralls"
    provider_name = "Coveralls"

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        super().__init__(session=session)

    def fetch(
        self, org: str, repo: str, platform: str = "github"
    ) -> Optional[CoverageMetrics]:
        url = f"https://coveralls.io/github/{org}/{repo}.json"

        try:
            resp = self._get(url)
        except requests.RequestException as exc:
            logger.warning("Coveralls network error for %s/%s: %s", org, repo, exc)
            return None

        if resp.status_code == 404:
            logger.debug("Coveralls: %s/%s not found", org, repo)
            return None
        if not resp.ok:
            logger.warning("Coveralls HTTP %d for %s/%s", resp.status_code, org, repo)
            return None

        try:
            data = resp.json()
        except ValueError as exc:
            logger.warning("Coveralls JSON parse error for %s/%s: %s", org, repo, exc)
            return None

        raw_pct = data.get("covered_percent")
        if raw_pct is None:
            return None

        try:
            pct = float(raw_pct)
        except (ValueError, TypeError):
            return None

        return CoverageMetrics(
            coverage_percent=round(pct, 2),
            lines_covered=None,
            lines_missed=None,
            branches_covered=None,
            branches_missed=None,
        )


class GitLabCIProvider(BaseProvider):
    """Reads the coverage value stored in GitLab CI pipeline results.

    GitLab can parse a coverage percentage out of job logs via a regex defined
    in `.gitlab-ci.yml`.  The value is stored on each pipeline object and
    accessible without cloning the repository.

    Only active for platform="gitlab"; returns None immediately for GitHub repos.
    """

    source_name = "gitlab_ci"
    provider_name = "GitLabCI"

    _BASE = "https://gitlab.com/api/v4"
    _PIPELINES_PER_PAGE = 5  # check the last N successful pipelines for coverage

    def __init__(self, token: str = "", session: Optional[requests.Session] = None) -> None:
        super().__init__(session=session)
        if token:
            self._session.headers["PRIVATE-TOKEN"] = token

    def fetch(
        self, org: str, repo: str, platform: str = "github"
    ) -> Optional[CoverageMetrics]:
        if platform.lower() != "gitlab":
            return None

        project_id = self._resolve_project_id(org, repo)
        if project_id is None:
            return None

        return self._coverage_from_pipelines(project_id, org, repo)

    # ------------------------------------------------------------------

    def _resolve_project_id(self, org: str, repo: str) -> Optional[int]:
        encoded = f"{org}/{repo}".replace("/", "%2F")
        try:
            resp = self._get(f"{self._BASE}/projects/{encoded}")
        except requests.RequestException as exc:
            logger.warning("GitLabCI network error resolving %s/%s: %s", org, repo, exc)
            return None

        if resp.status_code == 404:
            logger.debug("GitLabCI: project %s/%s not found", org, repo)
            return None
        if not resp.ok:
            logger.warning("GitLabCI HTTP %d resolving %s/%s", resp.status_code, org, repo)
            return None

        try:
            return int(resp.json()["id"])
        except (ValueError, KeyError, TypeError) as exc:
            logger.warning("GitLabCI: could not parse project ID for %s/%s: %s", org, repo, exc)
            return None

    def _coverage_from_pipelines(
        self, project_id: int, org: str, repo: str
    ) -> Optional[CoverageMetrics]:
        try:
            resp = self._get(
                f"{self._BASE}/projects/{project_id}/pipelines",
                params={
                    "scope": "finished",
                    "status": "success",
                    "per_page": str(self._PIPELINES_PER_PAGE),
                },
            )
        except requests.RequestException as exc:
            logger.warning("GitLabCI pipeline error for %s/%s: %s", org, repo, exc)
            return None

        if not resp.ok:
            logger.debug("GitLabCI: no pipelines for %s/%s (%d)", org, repo, resp.status_code)
            return None

        try:
            pipelines = resp.json()
        except ValueError:
            return None

        for pipeline in pipelines:
            raw = pipeline.get("coverage")
            if raw is None:
                continue
            try:
                pct = float(raw)
                logger.debug(
                    "GitLabCI: %s/%s coverage %.1f%% from pipeline %s",
                    org, repo, pct, pipeline.get("id"),
                )
                return CoverageMetrics(
                    coverage_percent=round(pct, 2),
                    lines_covered=None,
                    lines_missed=None,
                    branches_covered=None,
                    branches_missed=None,
                )
            except (ValueError, TypeError):
                continue

        logger.debug("GitLabCI: no pipeline with coverage data for %s/%s", org, repo)
        return None
