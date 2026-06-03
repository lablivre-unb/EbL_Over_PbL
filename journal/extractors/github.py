"""
GitHub PR extractor.

Uses a single unified GraphQL query per page of 25 PRs so that list +
details are fetched in one round-trip (no N+1 calls).

Pagination limits are passed as proper GraphQL Int! variables (fixed from
original where they were injected via string.replace()).  The produced API
calls and returned data are identical.

Cross-platform notes (see PRMetrics docstring for full list):
  - reviews_count: count of sampled review nodes (≤ MAX_REVIEWS_PER_QUERY).
  - comments: totalCount(PR comments) + totalCount(reviewThreads) — exact.
  - first_review_at: first formal review event timestamp.
  - is_doc_pr: file-based (>50% of sampled files are doc files).
"""
import logging
import time
from typing import Dict, List, Optional, Set

from journal.clients.graphql_client import GraphQLClient
from journal.config import settings
from journal.extractors.base import BaseExtractor
from journal.models.pr_metrics import PRMetrics
from journal.utils.bot_detection import is_bot_user
from journal.utils.file_analysis import analyze_files, compute_is_doc_pr

logger = logging.getLogger(__name__)

_QUERY_LIST_PRS = """
query(
  $org: String!,
  $repo: String!,
  $cursor: String,
  $maxCommits: Int!,
  $maxReviews: Int!,
  $maxComments: Int!,
  $maxFiles: Int!
) {
  repository(owner: $org, name: $repo) {
    pullRequests(
      first: 25,
      after: $cursor,
      states: MERGED,
      orderBy: {field: CREATED_AT, direction: DESC}
    ) {
      pageInfo { endCursor hasNextPage }
      nodes {
        number
        createdAt
        mergedAt
        additions
        deletions
        changedFiles
        title
        body
        author { login }
        labels(first: 10) { nodes { name } }
        commits(first: $maxCommits) {
          totalCount
          nodes {
            commit {
              message
              author { user { login } }
            }
          }
        }
        reviews(first: $maxReviews) {
          nodes { author { login } createdAt }
        }
        comments(first: $maxComments) {
          totalCount
          nodes { author { login } createdAt }
        }
        reviewThreads(first: $maxComments) {
          totalCount
          nodes {
            comments(first: 5) {
              nodes { author { login } createdAt }
            }
          }
        }
        files(first: $maxFiles) {
          totalCount
          nodes { path }
        }
      }
    }
  }
}
"""

_QUERY_LIST_REPOS = """
query($org: String!, $cursor: String) {
  organization(login: $org) {
    repositories(first: 100, after: $cursor) {
      pageInfo { endCursor hasNextPage }
      nodes { name }
    }
  }
}
"""

_PAGE_VARIABLES = {
    "maxCommits": settings.MAX_COMMITS_PER_QUERY,
    "maxReviews": settings.MAX_REVIEWS_PER_QUERY,
    "maxComments": settings.MAX_COMMENTS_PER_QUERY,
    "maxFiles": settings.MAX_FILES_PER_QUERY,
}


class GitHubExtractor(BaseExtractor):

    def __init__(self, client: GraphQLClient) -> None:
        self.client = client

    def list_repos(self, org: str) -> List[str]:
        """Return all repository names in an organisation."""
        repos: List[str] = []
        cursor = None

        while True:
            data = self.client.execute(
                _QUERY_LIST_REPOS,
                {"org": org, "cursor": cursor},
                f"GitHub/{org}/list_repos",
            )
            if not data or "errors" in data:
                break

            org_data = data.get("data", {}).get("organization", {})
            if not org_data:
                break

            repo_data = org_data.get("repositories", {})
            for node in repo_data.get("nodes", []):
                repos.append(node["name"])

            if not repo_data.get("pageInfo", {}).get("hasNextPage"):
                break
            cursor = repo_data["pageInfo"]["endCursor"]

        return repos

    def extract(
        self,
        org: str,
        repo: str,
        since_date: Optional[str] = None,
        processed_keys: Optional[Set[str]] = None,
    ) -> List[PRMetrics]:
        results: List[PRMetrics] = []
        cursor = None
        pr_count = 0
        skipped = 0

        logger.info("Extracting: %s/%s", org, repo)

        while True:
            variables = {
                "org": org,
                "repo": repo,
                "cursor": cursor,
                **_PAGE_VARIABLES,
            }
            data = self.client.execute(
                _QUERY_LIST_PRS,
                variables,
                f"GitHub/{org}/{repo}/list_prs",
            )

            if not data:
                logger.warning("Failed to list PRs for %s/%s", org, repo)
                break

            repo_data = data.get("data", {}).get("repository", {})
            if not repo_data:
                logger.warning("Repository %s/%s not found or empty", org, repo)
                break

            pr_data = repo_data.get("pullRequests", {})
            pr_nodes = pr_data.get("nodes", []) or []

            for pr in pr_nodes:
                if since_date and pr.get("createdAt", "") < since_date:
                    logger.info(
                        "Reached temporal cutoff %s for %s/%s", since_date[:10], org, repo
                    )
                    return results

                pr_number = pr.get("number")
                pr_key = f"GitHub/{org}/{repo}/#{pr_number}"

                if processed_keys and pr_key in processed_keys:
                    continue

                pr_count += 1
                if pr_count % 25 == 0:
                    logger.info(
                        "Processing PR #%s [%d PRs, %d skipped]",
                        pr_number, pr_count, skipped,
                    )

                metrics = self._process_pr(org, repo, pr)
                if metrics:
                    results.append(metrics)
                else:
                    skipped += 1

            page_info = pr_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            time.sleep(0.5)

        logger.info(
            "Done %s/%s: %d PRs extracted, %d skipped", org, repo, len(results), skipped
        )
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _process_pr(self, org: str, repo: str, pr: Dict) -> Optional[PRMetrics]:
        pr_number = pr.get("number")
        try:
            return self._build_metrics(org, repo, pr)
        except Exception as e:
            logger.error("Error processing GitHub/%s/%s/PR#%s: %s", org, repo, pr_number, e)
            return None

    def _build_metrics(self, org: str, repo: str, pr: Dict) -> PRMetrics:
        pr_number = pr.get("number")
        author = pr["author"]["login"] if pr.get("author") else "deleted_user"

        # --- reviews ---
        reviews_data = (pr.get("reviews") or {}).get("nodes", []) or []
        reviewers: set = set()
        first_review_at = None
        if reviews_data:
            reviews_data = sorted(reviews_data, key=lambda x: x.get("createdAt", ""))
            first_review_at = reviews_data[0].get("createdAt")
            for r in reviews_data:
                if r.get("author"):
                    reviewers.add(r["author"]["login"])

        # --- PR comments (issue-style) ---
        comments_data = pr.get("comments") or {}
        comment_nodes = comments_data.get("nodes", []) or []
        comments_total = comments_data.get("totalCount", 0) or 0
        commenters: set = set()
        for c in comment_nodes:
            if c.get("author"):
                commenters.add(c["author"]["login"])

        # --- review threads (inline code review comments) ---
        threads_data = pr.get("reviewThreads") or {}
        threads_total = threads_data.get("totalCount", 0) or 0
        thread_nodes = threads_data.get("nodes", []) or []
        for thread in thread_nodes:
            for tc in (thread.get("comments") or {}).get("nodes", []) or []:
                if tc.get("author"):
                    commenters.add(tc["author"]["login"])

        # --- first human response (non-author, non-bot, earliest timestamp) ---
        all_responses = []
        for r in reviews_data:
            if r.get("author") and r["author"]["login"] != author:
                all_responses.append({"user": r["author"]["login"], "ts": r["createdAt"]})
        for c in comment_nodes:
            if c.get("author") and c["author"]["login"] != author:
                all_responses.append({"user": c["author"]["login"], "ts": c["createdAt"]})
        for thread in thread_nodes:
            for tc in (thread.get("comments") or {}).get("nodes", []) or []:
                if tc.get("author") and tc["author"]["login"] != author:
                    all_responses.append(
                        {"user": tc["author"]["login"], "ts": tc["createdAt"]}
                    )
        all_responses.sort(key=lambda x: x["ts"])
        first_human_response_at = None
        for resp in all_responses:
            if not is_bot_user(resp["user"]):
                first_human_response_at = resp["ts"]
                break

        # --- commits ---
        commits_data = pr.get("commits") or {}
        commits_count = commits_data.get("totalCount", 0) or 0
        commit_nodes = commits_data.get("nodes", []) or []
        commit_authors: set = set()
        commit_msg_lengths = []
        for cn in commit_nodes:
            commit = cn.get("commit") or {}
            commit_user = (commit.get("author") or {}).get("user") or {}
            if commit_user and commit_user.get("login"):
                commit_authors.add(commit_user["login"])
            msg = commit.get("message", "")
            if msg:
                commit_msg_lengths.append(len(msg))
        avg_commit_msg_len = (
            sum(commit_msg_lengths) / len(commit_msg_lengths)
            if commit_msg_lengths
            else 0.0
        )

        # --- files ---
        files_data = pr.get("files") or {}
        file_nodes = files_data.get("nodes", []) or []
        file_paths = [f["path"] for f in file_nodes if f.get("path")]
        repo_prefix = f"{org}/{repo}"
        doc_count, extensions, file_hashes = analyze_files(file_paths, repo_prefix)

        # --- labels ---
        label_nodes = (pr.get("labels") or {}).get("nodes", []) or []
        label_names = [lbl["name"] for lbl in label_nodes if lbl.get("name")]

        title = pr.get("title") or ""
        body = pr.get("body") or ""

        return PRMetrics(
            platform="GitHub",
            org=org,
            repo=repo,
            id=pr_number,
            author=author,
            created_at=pr.get("createdAt"),
            merged_at=pr.get("mergedAt"),
            first_review_at=first_review_at,
            first_human_response_at=first_human_response_at,
            reviewers=",".join(reviewers),
            commenters=",".join(commenters),
            commit_authors=",".join(commit_authors),
            commits=commits_count,
            avg_commit_message_length=round(avg_commit_msg_len, 2),
            reviews_count=len(reviews_data),
            comments=comments_total + threads_total,
            files_changed=pr.get("changedFiles", 0),
            additions=pr.get("additions", 0),
            deletions=pr.get("deletions", 0),
            churn=pr.get("additions", 0) + pr.get("deletions", 0),
            doc_files_count=doc_count,
            is_doc_pr=compute_is_doc_pr(file_paths, title, body),
            file_extensions=extensions,
            file_hashes=file_hashes,
            title_length=len(title),
            description_length=len(body),
            labels_count=len(label_names),
            labels=",".join(label_names),
        )
