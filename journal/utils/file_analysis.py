import hashlib
import os
from typing import List, Tuple

DOC_EXTENSIONS: frozenset = frozenset(
    [".md", ".txt", ".rst", ".pdf", ".docx", ".adoc"]
)


def hash_file_path(path: str, repo_prefix: str = "") -> str:
    """SHA256[:8] of '{repo_prefix}:{path}', or just path when no prefix."""
    full_path = f"{repo_prefix}:{path}" if repo_prefix else path
    return hashlib.sha256(full_path.encode()).hexdigest()[:8]


def analyze_files(
    file_paths: List[str], repo_prefix: str = ""
) -> Tuple[int, str, str]:
    """
    Returns (doc_count, extensions_csv, file_hashes_csv).

    doc_count counts files whose extension is in DOC_EXTENSIONS or whose
    path contains 'docs/' or 'documentation/'.

    extensions_csv: unique extensions, comma-separated.
    file_hashes_csv: one hash per file, comma-separated.
    """
    if not file_paths:
        return 0, "", ""

    doc_count = 0
    extensions: set = set()
    hashes: List[str] = []

    for path in file_paths:
        ext = os.path.splitext(path)[1].lower()
        if ext:
            extensions.add(ext)

        if (
            ext in DOC_EXTENSIONS
            or "docs/" in path.lower()
            or "documentation/" in path.lower()
        ):
            doc_count += 1

        hashes.append(hash_file_path(path, repo_prefix))

    return doc_count, ",".join(extensions), ",".join(hashes)


def compute_is_doc_pr(
    file_paths: List[str],
    title: str,
    description: str,
) -> bool:
    """
    Unified is_doc_pr computation.

    When file_paths is available (GitHub): doc PR when >50% of sampled files
    are documentation files.

    When file_paths is empty (GitLab — no file data returned by the API):
    falls back to keyword matching in title + description.
    """
    if file_paths:
        doc_count = sum(
            1
            for p in file_paths
            if os.path.splitext(p)[1].lower() in DOC_EXTENSIONS
            or "docs/" in p.lower()
            or "documentation/" in p.lower()
        )
        return doc_count > 0 and (doc_count / len(file_paths)) > 0.5

    # Fallback: text-based heuristic (same keywords as original GitLab logic)
    text = (title + " " + description).lower()
    return "doc" in text or "readme" in text
