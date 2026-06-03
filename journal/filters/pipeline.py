"""
Bronze → Silver filter pipeline.

Applies two independent criteria (AND):

1. Whitelist: PR must belong to a whitelisted org/repo combination.
   - For academic targets (filter_by_repo=True): org AND repo must match.
   - For benchmark/EbL targets (filter_by_repo=False): org match suffices.

2. Temporal: PR's created_at OR merged_at falls within at least one semester
   window (OR across semesters).  A PR created before a semester but merged
   during it (or vice versa) is included; this is intentional to capture
   cross-boundary work.

Both criteria use data from config/targets.py so there is a single source of
truth shared with the extractor.
"""
import logging
import os
from typing import Optional

import pandas as pd

from journal.config.targets import SEMESTERS, TARGETS, Target

logger = logging.getLogger(__name__)


def filter_bronze_to_silver(
    input_path: str,
    output_path: str,
) -> None:
    """Read the bronze CSV, apply whitelist + semester filters, write silver CSV."""
    if not os.path.exists(input_path):
        logger.error("Bronze file not found: %s", input_path)
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df = pd.read_csv(input_path)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["merged_at"] = pd.to_datetime(df["merged_at"], errors="coerce")

    # ------------------------------------------------------------------
    # 1. Whitelist mask
    # ------------------------------------------------------------------
    whitelist_masks = []
    for target in TARGETS:
        platform_mask = df["platform"].str.lower() == target.platform.lower()
        org_mask = df["org"].str.lower() == target.org.lower()

        if target.filter_by_repo and target.repos:
            repo_mask = df["repo"].isin(target.repos)
            whitelist_masks.append(platform_mask & org_mask & repo_mask)
        else:
            whitelist_masks.append(platform_mask & org_mask)

    mask_whitelist = pd.concat(whitelist_masks, axis=1).any(axis=1)

    # ------------------------------------------------------------------
    # 2. Semester temporal mask
    # ------------------------------------------------------------------
    tz_aware = bool(df["created_at"].dt.tz)

    semester_masks = []
    for semester in SEMESTERS:
        start = pd.to_datetime(semester["start"])
        end = pd.to_datetime(semester["end"])
        if tz_aware:
            start = start.tz_localize("UTC")
            end = end.tz_localize("UTC")

        mask_time = (
            (df["created_at"] >= start) & (df["created_at"] <= end)
        ) | (
            (df["merged_at"] >= start) & (df["merged_at"] <= end)
        )
        semester_masks.append(mask_time)

    mask_any_semester = pd.concat(semester_masks, axis=1).any(axis=1)

    # ------------------------------------------------------------------
    # 3. Combined filter
    # ------------------------------------------------------------------
    filtered_df = df[mask_whitelist & mask_any_semester]

    filtered_df.to_csv(output_path, index=False)

    logger.info("Filter complete")
    logger.info("  Bronze total : %d", len(df))
    logger.info("  Silver total : %d", len(filtered_df))
    logger.info("  Semesters    : %s", [s["name"] for s in SEMESTERS])
    logger.info("  Output       : %s", output_path)
