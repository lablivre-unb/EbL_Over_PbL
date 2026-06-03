from dataclasses import dataclass, field, asdict
from typing import Optional, List

import pandas as pd


@dataclass
class PRMetrics:
    """Raw metrics for a single merged PR or MR.

    Field semantics are platform-consistent except where noted. The following
    cross-platform differences exist and are preserved intentionally:

    reviews_count
        GitHub: count of sampled review events (≤ MAX_REVIEWS_PER_QUERY = 15).
        GitLab: count of formal approvals only (approvedBy nodes).

    comments
        GitHub: totalCount(PR comments) + totalCount(reviewThreads) — exact totals.
        GitLab: count of non-system discussion notes in sampled discussions
                (bounded by 50 discussions × 20 notes).

    first_review_at
        GitHub: timestamp of the first formal review event.
        GitLab: timestamp of the first non-author, non-system discussion note.

    is_doc_pr / doc_files_count / file_extensions / file_hashes
        GitHub: derived from sampled file paths (≤ MAX_FILES_PER_QUERY = 20).
        GitLab: doc_files_count = 0, file_extensions = "", file_hashes = "";
                is_doc_pr uses title/description keyword fallback.
    """

    platform: str
    org: str
    repo: str
    id: int
    author: str
    created_at: str
    merged_at: str
    first_review_at: Optional[str] = None
    first_human_response_at: Optional[str] = None
    reviewers: str = ""
    commenters: str = ""
    commit_authors: str = ""
    commits: int = 0
    avg_commit_message_length: float = 0.0
    reviews_count: int = 0
    comments: int = 0
    files_changed: int = 0
    additions: int = 0
    deletions: int = 0
    churn: int = 0
    doc_files_count: int = 0
    is_doc_pr: bool = False
    file_extensions: str = ""
    file_hashes: str = ""
    title_length: int = 0
    description_length: int = 0
    labels_count: int = 0
    labels: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def compute_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Add time-based and density derived columns to a DataFrame of PRMetrics rows.

    These columns are NOT stored in PRMetrics; they are computed at persistence
    time to keep the dataclass free of pandas dependencies.

    Derived columns added:
        lead_time_hours
        time_to_first_review_hours
        time_to_first_human_response_hours
        discussion_density
    """
    date_cols = [
        "created_at",
        "merged_at",
        "first_review_at",
        "first_human_response_at",
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    if {"merged_at", "created_at"}.issubset(df.columns):
        df["lead_time_hours"] = (
            df["merged_at"] - df["created_at"]
        ).dt.total_seconds() / 3600

    if {"first_review_at", "created_at"}.issubset(df.columns):
        df["time_to_first_review_hours"] = (
            df["first_review_at"] - df["created_at"]
        ).dt.total_seconds() / 3600

    if {"first_human_response_at", "created_at"}.issubset(df.columns):
        df["time_to_first_human_response_hours"] = (
            df["first_human_response_at"] - df["created_at"]
        ).dt.total_seconds() / 3600

    if {"churn", "comments"}.issubset(df.columns):
        df["discussion_density"] = df.apply(
            lambda x: x["comments"] / x["churn"] if x["churn"] > 0 else 0,
            axis=1,
        )

    return df
