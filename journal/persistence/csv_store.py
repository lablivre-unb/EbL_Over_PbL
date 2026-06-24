"""
Incremental CSV persistence for PRMetrics.

Append-only writes allow resumable extraction: on restart the store reads the
existing file to rebuild the set of already-processed PR keys and skips them.

Derived time / density columns are computed at write time (not stored in the
PRMetrics dataclass) so the dataclass stays free of pandas dependencies.
"""
import logging
import os
from typing import List, Set, Tuple

import pandas as pd

from journal.models.pr_metrics import PRMetrics, compute_derived_fields

logger = logging.getLogger(__name__)


class DataPersistence:

    def __init__(self, output_file: str) -> None:
        self.output_file = output_file
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

    def get_processed_keys(self) -> Set[str]:
        """Return the set of PR keys already written to the CSV.

        Key format:
            GitHub/{org}/{repo}/#{id}
            GitLab/{org}/{repo}/!{id}

        Uses vectorised string operations instead of iterrows() for performance.
        """
        if not os.path.exists(self.output_file):
            return set()

        try:
            df = pd.read_csv(
                self.output_file, usecols=["platform", "org", "repo", "id"]
            )
            prefix = df["platform"].map(lambda p: "#" if p == "GitHub" else "!")
            keys = (
                df["platform"]
                + "/"
                + df["org"]
                + "/"
                + df["repo"]
                + "/"
                + prefix
                + df["id"].astype(str)
            )
            return set(keys)
        except Exception as e:
            logger.warning("Could not read processed keys: %s", e)
            return set()

    def remove_records(self, targets: List[Tuple[str, str]]) -> int:
        """Delete existing rows matching the given (org, repo) pairs.

        Rewrites the CSV in place, keeping every row that does NOT match.
        A repo of "*" removes all rows for that org (used for targets whose
        repo list is resolved dynamically). Returns the number of rows removed.
        """
        if not os.path.exists(self.output_file) or not targets:
            return 0

        df = pd.read_csv(self.output_file)
        mask = pd.Series(False, index=df.index)
        for org, repo in targets:
            m = df["org"] == org
            if repo != "*":
                m &= df["repo"] == repo
            mask |= m

        removed = int(mask.sum())
        if removed:
            df[~mask].to_csv(self.output_file, index=False)
            logger.info("Removed %d existing records from %s", removed, self.output_file)
        return removed

    def save_batch(self, metrics_list: List[PRMetrics]) -> None:
        """Append a batch of PRMetrics to the CSV, including derived columns."""
        if not metrics_list:
            return

        df = pd.DataFrame([m.to_dict() for m in metrics_list])
        df = compute_derived_fields(df)

        header = not os.path.exists(self.output_file)
        df.to_csv(self.output_file, mode="a", index=False, header=header)
        logger.info("Saved %d records to %s", len(metrics_list), self.output_file)
