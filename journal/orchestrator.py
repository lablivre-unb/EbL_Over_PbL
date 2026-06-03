"""
Extraction orchestrator.

Iterates over all TARGETS, dispatches to the appropriate platform extractor,
and flushes to CSV in batches of SAVE_BATCH_SIZE to limit memory use.
Batches are accumulated across repos within a single target and flushed either
when the batch reaches the size threshold or at the end of each target.
"""
import logging
from typing import List, Optional, Set

from journal.clients.graphql_client import GraphQLClient
from journal.config import settings
from journal.config.targets import Target, TARGETS
from journal.extractors.github import GitHubExtractor
from journal.extractors.gitlab import GitLabExtractor
from journal.models.pr_metrics import PRMetrics
from journal.persistence.csv_store import DataPersistence

logger = logging.getLogger(__name__)


class ExtractionOrchestrator:

    def __init__(self, output_file: Optional[str] = None) -> None:
        out = output_file or settings.OUTPUT_FILE
        self.persistence = DataPersistence(out)

        self.github_client: Optional[GraphQLClient] = (
            GraphQLClient("https://api.github.com/graphql", settings.GITHUB_TOKEN, "GitHub")
            if settings.GITHUB_TOKEN
            else None
        )
        self.gitlab_client: Optional[GraphQLClient] = (
            GraphQLClient("https://gitlab.com/api/graphql", settings.GITLAB_TOKEN, "GitLab")
            if settings.GITLAB_TOKEN
            else None
        )

    def run(self, targets: Optional[List[Target]] = None) -> None:
        """Run extraction for all targets (or a provided subset)."""
        targets = targets or TARGETS
        processed = self.persistence.get_processed_keys()
        logger.info("Already processed: %d PRs", len(processed))

        for target in targets:
            try:
                if target.platform == "github":
                    self._process_github_target(target, processed)
                elif target.platform == "gitlab":
                    self._process_gitlab_target(target, processed)
                else:
                    logger.warning("Unknown platform '%s' — skipping", target.platform)
            except Exception as e:
                logger.error("Fatal error processing target %s: %s", target, e)

    # ------------------------------------------------------------------
    # Platform dispatchers
    # ------------------------------------------------------------------

    def _process_github_target(self, target: Target, processed: Set[str]) -> None:
        if not self.github_client:
            logger.error("GITHUB_TOKEN not configured — skipping GitHub target %s", target.org)
            return

        logger.info("=" * 60)
        logger.info("GitHub: %s", target.org)
        logger.info("=" * 60)

        extractor = GitHubExtractor(self.github_client)
        repos = target.repos

        if not repos:
            logger.info("Listing repositories for %s ...", target.org)
            repos = extractor.list_repos(target.org)
            logger.info("Found %d repos", len(repos))

        self._extract_and_flush(
            extractor=extractor,
            org=target.org,
            repos=repos,
            since=target.since,
            processed=processed,
        )

    def _process_gitlab_target(self, target: Target, processed: Set[str]) -> None:
        if not self.gitlab_client:
            logger.error("GITLAB_TOKEN not configured — skipping GitLab target %s", target.org)
            return

        logger.info("=" * 60)
        logger.info("GitLab: %s", target.org)
        logger.info("=" * 60)

        extractor = GitLabExtractor(self.gitlab_client)
        repos = target.repos

        if not repos:
            logger.info("Listing projects for %s ...", target.org)
            repos = extractor.list_projects(target.org)
            logger.info("Found %d projects", len(repos))

        self._extract_and_flush(
            extractor=extractor,
            org=target.org,
            repos=repos,
            since=target.since,
            processed=processed,
        )

    # ------------------------------------------------------------------
    # Shared accumulation + flush logic
    # ------------------------------------------------------------------

    def _extract_and_flush(
        self,
        extractor,
        org: str,
        repos: List[str],
        since: Optional[str],
        processed: Set[str],
    ) -> None:
        """Extract from each repo and flush in batches."""
        pending: List[PRMetrics] = []

        for repo in repos:
            metrics = extractor.extract(
                org=org,
                repo=repo,
                since_date=since,
                processed_keys=processed,
            )
            pending.extend(metrics)

            if len(pending) >= settings.SAVE_BATCH_SIZE:
                self.persistence.save_batch(pending)
                pending = []

        if pending:
            self.persistence.save_batch(pending)
