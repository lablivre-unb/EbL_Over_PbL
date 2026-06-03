from abc import ABC, abstractmethod
from typing import List, Optional, Set

from journal.models.pr_metrics import PRMetrics


class BaseExtractor(ABC):
    """Protocol for platform-specific PR/MR extractors."""

    @abstractmethod
    def extract(
        self,
        org: str,
        repo: str,
        since_date: Optional[str] = None,
        processed_keys: Optional[Set[str]] = None,
    ) -> List[PRMetrics]:
        """Extract all merged PRs/MRs from one repository.

        Args:
            org: GitHub org login or GitLab group full path.
            repo: Repository / project name.
            since_date: ISO 8601 datetime string. Extraction stops when a PR's
                createdAt is earlier than this value (assumes DESC order).
            processed_keys: Set of already-persisted PR keys; matching PRs are
                skipped for incremental extraction.

        Returns:
            List of PRMetrics, one per successfully processed PR/MR.
        """
