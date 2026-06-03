"""Persistence for CoverageResult records.

Writes to a CSV file in append mode (same pattern as DataPersistence for PRMetrics).
get_extracted_repos() allows incremental runs that skip already-processed repos.
"""
import logging
import os
from typing import List, Set

import pandas as pd

from journal.extractors.coverage.models import CoverageResult

logger = logging.getLogger(__name__)


class CoverageStore:

    def __init__(self, output_file: str) -> None:
        self.output_file = output_file
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

    def get_extracted_repos(self) -> Set[str]:
        """Return the set of repository slugs already written (for incremental runs)."""
        if not os.path.exists(self.output_file):
            return set()
        try:
            df = pd.read_csv(self.output_file, usecols=["repository"])
            return set(df["repository"].dropna().unique())
        except Exception as exc:
            logger.warning("Could not read existing coverage store: %s", exc)
            return set()

    def save(self, results: List[CoverageResult]) -> None:
        """Append a list of results to the CSV."""
        if not results:
            return
        df = pd.DataFrame([r.to_dict() for r in results])
        header = not os.path.exists(self.output_file)
        df.to_csv(self.output_file, mode="a", index=False, header=header)
        logger.info("Saved %d coverage records to %s", len(results), self.output_file)
