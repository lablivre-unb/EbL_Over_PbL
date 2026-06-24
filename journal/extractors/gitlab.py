"""
GitLab MR extractor — 2-phase extraction.

Phase 1: lightweight list query (basic MR fields, no discussion details).
Phase 2: per-MR detail query (commits, discussions, approvals).

Cross-platform notes (see PRMetrics docstring for full list):
  - reviews_count: count of formal approvals (approvedBy nodes only).
  - comments: count of non-system discussion notes in sampled discussions
              (50 discussions × 20 notes per discussion).
  - first_review_at: first non-author, non-system note timestamp — broader
    than GitHub's formal-review-event definition.
  - doc_files_count / file_extensions / file_hashes: populated via a
    supplementary REST API call (GET /projects/:id/merge_requests/:iid/changes)
    since the GitLab GraphQL API does not expose file diffs directly.
"""
import logging
import time
from typing import Dict, List, Optional, Set

from journal.clients.graphql_client import GraphQLClient
from journal.extractors.base import BaseExtractor
from journal.models.pr_metrics import PRMetrics
from journal.utils.bot_detection import is_bot_user
from journal.utils.file_analysis import analyze_files, compute_is_doc_pr

logger = logging.getLogger(__name__)

_QUERY_LIST_MRS = """
query($path: ID!, $cursor: String) {
  project(fullPath: $path) {
    mergeRequests(state: merged, first: 25, after: $cursor) {
      pageInfo { endCursor hasNextPage }
      nodes {
        iid
        createdAt
        mergedAt
        commitCount
        title
        description
        author { username }
        diffStatsSummary { additions deletions fileCount }
        labels { nodes { title } }
      }
    }
  }
}
"""

_QUERY_MR_DETAILS = """
query($path: ID!, $mrIid: String!) {
  project(fullPath: $path) {
    mergeRequest(iid: $mrIid) {
      commits {
        nodes {
          author { username }
          message
        }
      }
      discussions(first: 50) {
        nodes {
          notes(first: 20) {
            nodes { author { username } createdAt system }
          }
        }
      }
      approvedBy { nodes { username } }
    }
  }
}
"""

_QUERY_LIST_PROJECTS = """
query($group: ID!, $cursor: String) {
  group(fullPath: $group) {
    projects(includeSubgroups: true, first: 50, after: $cursor) {
      pageInfo { endCursor hasNextPage }
      nodes { fullPath name }
    }
  }
}
"""


class GitLabExtractor(BaseExtractor):

    def __init__(self, client: GraphQLClient) -> None:
        self.client = client

    def list_projects(self, group_path: str) -> List[str]:
        """Return project names in a GitLab group (including subgroups)."""
        projects: List[str] = []
        cursor = None

        while True:
            data = self.client.execute(
                _QUERY_LIST_PROJECTS,
                {"group": group_path, "cursor": cursor},
                f"GitLab/{group_path}/list_projects",
            )
            if not data:
                break

            group_data = data.get("data", {}).get("group", {})
            if not group_data:
                break

            proj_data = group_data.get("projects", {})
            for node in proj_data.get("nodes", []):
                projects.append(node["name"])

            if not proj_data.get("pageInfo", {}).get("hasNextPage"):
                break
            cursor = proj_data["pageInfo"]["endCursor"]

        return projects

    def extract(
        self,
        org: str,
        repo: str,
        since_date: Optional[str] = None,
        processed_keys: Optional[Set[str]] = None,
    ) -> List[PRMetrics]:
        """Extract all merged MRs from one GitLab project (2-phase)."""
        results: List[PRMetrics] = []
        cursor = None
        mr_count = 0
        skipped = 0
        project_path = f"{org}/{repo}"

        logger.info("Extracting: %s", project_path)

        while True:
            data = self.client.execute(
                _QUERY_LIST_MRS,
                {"path": project_path, "cursor": cursor},
                f"GitLab/{project_path}/list_mrs",
            )

            if not data:
                logger.warning("Failed to list MRs for %s", project_path)
                break

            project_data = data.get("data", {}).get("project", {})
            if not project_data:
                logger.warning("Project %s not found", project_path)
                break

            mr_data = project_data.get("mergeRequests", {})
            mr_nodes = mr_data.get("nodes", []) or []

            for mr_basic in mr_nodes:
                if since_date and mr_basic.get("createdAt", "") < since_date:
                    logger.info(
                        "Reached temporal cutoff %s for %s", since_date[:10], project_path
                    )
                    return results

                mr_iid = mr_basic.get("iid")
                mr_key = f"GitLab/{org}/{repo}/!{mr_iid}"

                if processed_keys and mr_key in processed_keys:
                    continue

                mr_count += 1
                if mr_count % 10 == 0:
                    logger.info(
                        "Processing MR !%s [%d MRs, %d skipped]",
                        mr_iid, mr_count, skipped,
                    )

                details_data = self._fetch_mr_details(project_path, mr_iid)
                if not details_data:
                    logger.error(
                        "Failed to fetch details for GitLab/%s/MR!%s", project_path, mr_iid
                    )
                    skipped += 1
                    continue

                mr_details = (
                    (details_data.get("data") or {}).get("project") or {}
                ).get("mergeRequest") or {}

                metrics = self._process_mr(org, repo, mr_basic, mr_details, project_path)
                if metrics:
                    results.append(metrics)
                else:
                    skipped += 1

                time.sleep(0.3)

            page_info = mr_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            time.sleep(0.5)

        logger.info(
            "Done %s: %d MRs extracted, %d skipped", project_path, len(results), skipped
        )
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_mr_details(self, project_path: str, mr_iid: int) -> Optional[Dict]:
        return self.client.execute(
            _QUERY_MR_DETAILS,
            {"path": project_path, "mrIid": str(mr_iid)},
            f"GitLab/{project_path}/MR!{mr_iid}/details",
        )

    def _fetch_mr_files_rest(self, project_path: str, mr_iid: int) -> List[str]:
        """Fetch modified file paths via GitLab REST API.

        The GraphQL API does not expose file diffs directly, so we use
        GET /projects/:id/merge_requests/:iid/changes to retrieve them.
        Capped at 100 files per MR for performance.
        """
        try:
            project_encoded = project_path.replace("/", "%2F")
            url = (
                f"https://gitlab.com/api/v4/projects/{project_encoded}"
                f"/merge_requests/{mr_iid}/changes"
            )
            response = self.client.session.get(
                url, headers=self.client.headers, timeout=30,
            )
            if response.status_code == 200:
                changes = response.json().get("changes", [])
                return [
                    c["new_path"] for c in changes[:100] if "new_path" in c
                ]
            logger.warning(
                "REST /changes returned %d for %s/MR!%s",
                response.status_code, project_path, mr_iid,
            )
            return []
        except Exception as e:
            logger.error(
                "Error fetching files for GitLab/%s/MR!%s: %s",
                project_path, mr_iid, e,
            )
            return []

    def _process_mr(
        self, group: str, repo: str, mr_basic: Dict, mr_details: Dict,
        project_path: str,
    ) -> Optional[PRMetrics]:
        mr_iid = mr_basic.get("iid")
        try:
            return self._build_metrics(group, repo, mr_basic, mr_details, project_path)
        except Exception as e:
            logger.error(
                "Error processing GitLab/%s/%s/MR!%s: %s", group, repo, mr_iid, e
            )
            return None

    def _build_metrics(
        self, group: str, repo: str, mr_basic: Dict, mr_details: Dict,
        project_path: str,
    ) -> PRMetrics:
        mr_iid = mr_basic.get("iid")
        author = (
            mr_basic["author"]["username"] if mr_basic.get("author") else "deleted_user"
        )

        # --- approvals (formal reviewers on GitLab) ---
        reviewers: set = set()
        approved_by = mr_details.get("approvedBy") or {}
        if approved_by and approved_by.get("nodes"):
            for app in approved_by["nodes"]:
                if app.get("username"):
                    reviewers.add(app["username"])

        # --- discussions (non-system notes) ---
        all_notes: List[Dict] = []
        external_notes: List[Dict] = []
        commenters: set = set()
        discussions = (mr_details.get("discussions") or {}).get("nodes", []) or []
        for disc in discussions:
            for note in (disc.get("notes") or {}).get("nodes", []) or []:
                if note.get("system", False):
                    continue
                if note.get("author"):
                    note_author = note["author"]["username"]
                    all_notes.append(note)
                    commenters.add(note_author)
                    if note_author != author:
                        external_notes.append(note)
                        reviewers.add(note_author)

        # --- first review / first human response ---
        first_review_at = None
        first_human_response_at = None
        if external_notes:
            external_notes.sort(key=lambda x: x.get("createdAt", ""))
            first_review_at = external_notes[0].get("createdAt")
            for note in external_notes:
                if not is_bot_user(note["author"]["username"]):
                    first_human_response_at = note["createdAt"]
                    break

        # --- commits ---
        commit_nodes = (mr_details.get("commits") or {}).get("nodes", []) or []
        commit_authors: set = set()
        commit_msg_lengths = []
        for cn in commit_nodes:
            if (cn.get("author") or {}).get("username"):
                commit_authors.add(cn["author"]["username"])
            msg = cn.get("message", "")
            if msg:
                commit_msg_lengths.append(len(msg))
        avg_commit_msg_len = (
            sum(commit_msg_lengths) / len(commit_msg_lengths)
            if commit_msg_lengths
            else 0.0
        )

        # --- diff stats ---
        diff_stats = mr_basic.get("diffStatsSummary") or {}
        additions = diff_stats.get("additions", 0) or 0
        deletions = diff_stats.get("deletions", 0) or 0
        file_count = diff_stats.get("fileCount", 0) or 0

        # --- file paths (via REST API) ---
        file_paths = self._fetch_mr_files_rest(project_path, mr_iid)
        repo_prefix = f"{group}/{repo}"
        doc_count, extensions, file_hashes = analyze_files(file_paths, repo_prefix)

        # --- labels ---
        label_nodes = (mr_basic.get("labels") or {}).get("nodes", []) or []
        label_names = [lbl["title"] for lbl in label_nodes if lbl.get("title")]

        title = mr_basic.get("title") or ""
        description = mr_basic.get("description") or ""

        return PRMetrics(
            platform="GitLab",
            org=group,
            repo=repo,
            id=mr_iid,
            author=author,
            created_at=mr_basic.get("createdAt"),
            merged_at=mr_basic.get("mergedAt"),
            first_review_at=first_review_at,
            first_human_response_at=first_human_response_at,
            reviewers=",".join(reviewers),
            commenters=",".join(commenters),
            commit_authors=",".join(commit_authors),
            commits=mr_basic.get("commitCount", 0),
            avg_commit_message_length=round(avg_commit_msg_len, 2),
            reviews_count=len(approved_by.get("nodes", [])) if approved_by else 0,
            comments=len(all_notes),
            files_changed=file_count,
            additions=additions,
            deletions=deletions,
            churn=additions + deletions,
            doc_files_count=doc_count,
            is_doc_pr=compute_is_doc_pr(file_paths, title, description),
            file_extensions=extensions,
            file_hashes=file_hashes,
            title_length=len(title),
            description_length=len(description),
            labels_count=len(label_names),
            labels=",".join(label_names),
        )
