"""
Extrator de métricas de PRs/MRs de GitHub e GitLab.

Otimizado para escala (~400K PRs) com:
- GitHub: Query unificada (lista + detalhes em 1 request por página de 25 PRs)
- GitLab: Extração em 2 fases (lista leve + detalhes por MR)
- Limites reduzidos de paginação interna para queries mais leves
- Sem paginação extra de arquivos (amostra + totalCount)
- Tratamento de erros por PR (skip em caso de falha)
- Hash de arquivos para eficiência de storage
- Rate limit handling robusto
"""

import hashlib
import json
import os
import time
from dataclasses import dataclass, field, asdict
from typing import Optional, Set, List, Dict, Any
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
OUTPUT_FILE = "metrics/data/bronze/prs.csv"
ERROR_LOG_FILE = "metrics/data/bronze/extraction_errors.log"

MAX_COMMITS_PER_QUERY = 10  # era 50 — maioria dos PRs tem <10 commits
MAX_REVIEWS_PER_QUERY = 15  # era 30 — suficiente para capturar reviewers
MAX_COMMENTS_PER_QUERY = 15  # era 30 — suficiente para first_response e commenters
MAX_FILES_PER_QUERY = 20  # era 100 — amostra de arquivos para extensões/docs

SAVE_BATCH_SIZE = 50

BOT_KEYWORDS = frozenset(
    [
        "bot",
        "dependabot",
        "renovate",
        "github-actions",
        "codecov",
        "greenkeeper",
        "snyk",
        "pyup",
        "automated",
        "ci-",
        "action",
        "github-advanced-security",
        "copilot-pull-request",
        "[bot]",
    ]
)

DOC_EXTENSIONS = frozenset([".md", ".txt", ".rst", ".pdf", ".docx", ".adoc"])

TARGETS = [
    {
        "type": "gitlab",
        "group_path": "lappis-unb/decidimbr",
        "repos": ["decidim-govbr"],
    },
    {
        "type": "github",
        "org": "unb-mds",
        "repos": [
            "2025-2-Mural-UnB",
            "Sonorus-2025.1",
            "2024-2-AcheiUnB",
            "2024-1-forUnB",
        ],
    },
    {
        "type": "github",
        "org": "mdsreq-fga-unb",
        "repos": [
            "REQ-2025.2-T02-RxHospitalar",
            "2025.1-T01-VidracariaModelo",
            "2024.2-T03-CafeDoSitio",
            "2024.1-ObjeX",
        ],
    },
    {
        "type": "github",
        "org": "fga-eps-mds",
        "repos": [
            "2025.2-Valhalla",
            "2025.2-Valhalla-Docs",
            "2025.1-VaiPelaSombra-docs",
            "2025.1-VaiPelaSombra-FrontEnd",
            "2025.1-VaiPelaSombra-BackEnd,",
            "2025.1-VaiPelaSombra-API",
            "2024-2-GEROcuidado-Docs",
            "2024-2-GEROcuidado-APIForum",
            "2024-2-GEROcuidado-Front",
            "2024-2-GEROcuidado-APIUsuario",
            "2024-2-GEROcuidado-APISaude",
            "2024-1-GEROcuidado-Front",
            "2024-1-GEROcuidado-Doc",
            "2024-1-GEROcuidado-APISaude",
            "2024-1-GEROcuidado-APIUsuario",
            "2024-1-GEROcuidado-APIForum",
        ],
    },
    {
        "type": "github",
        "org": "decidim",
        "repos": ["decidim"],
        "since": "2024-01-01T00:00:00Z",
    },
    {
        "type": "github",
        "org": "microsoft",
        "repos": ["vscode"],
        "since": "2024-01-01T00:00:00Z",
    },
    {
        "type": "github",
        "org": "flutter",
        "repos": ["flutter"],
        "since": "2024-01-01T00:00:00Z",
    },
    {
        "type": "github",
        "org": "facebook",
        "repos": ["react"],
        "since": "2024-01-01T00:00:00Z",
    },
    {
        "type": "github",
        "org": "kubernetes",
        "repos": ["kubernetes"],
        "since": "2024-01-01T00:00:00Z",
    },
    {
        "type": "github",
        "org": "tensorflow",
        "repos": ["tensorflow"],
        "since": "2024-01-01T00:00:00Z",
    },
]


@dataclass
class PRMetrics:
    """Métricas extraídas de um PR/MR."""

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


def is_bot_user(username: Optional[str]) -> bool:
    """Identifica se o usuário é um bot baseado em palavras-chave."""
    if not username:
        return True
    username_lower = username.lower()
    return any(kw in username_lower for kw in BOT_KEYWORDS)


def hash_file_path(path: str, repo_prefix: str = "") -> str:
    """
    Gera hash curto (8 chars) do path do arquivo.
    Inclui prefixo do repo para evitar colisões entre repos.
    """
    full_path = f"{repo_prefix}:{path}" if repo_prefix else path
    return hashlib.sha256(full_path.encode()).hexdigest()[:8]


def analyze_files(file_paths: List[str], repo_prefix: str = "") -> tuple:
    """
    Analisa lista de arquivos e retorna:
    - doc_count: quantidade de arquivos de documentação
    - extensions: extensões únicas (string separada por vírgula)
    - file_hashes: hashes dos arquivos (string separada por vírgula)
    """
    if not file_paths:
        return 0, "", ""

    doc_count = 0
    extensions = set()
    hashes = []

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


def log_error(context: str, error: str):
    """Registra erro em arquivo de log."""
    timestamp = datetime.now().isoformat()
    with open(ERROR_LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] {context}: {error}\n")


class GraphQLClient:
    """Cliente HTTP otimizado para GraphQL com retry e rate limit handling."""

    def __init__(self, base_url: str, token: str, platform: str = ""):
        self.base_url = base_url
        self.platform = platform
        self.headers = {"Authorization": f"Bearer {token}"}
        self.session = self._create_session()
        self._request_count = 0

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        return session

    def _wait_for_rate_limit(self, response: requests.Response):
        """Espera até o reset do rate limit."""
        reset_ts = response.headers.get("x-ratelimit-reset") or response.headers.get(
            "ratelimit-reset"
        )
        retry_after = response.headers.get("retry-after")

        wait_seconds = 60  # fallback

        if retry_after:
            try:
                wait_seconds = int(retry_after)
            except ValueError:
                pass
        elif reset_ts:
            try:
                wait_seconds = max(int(reset_ts) - int(time.time()), 0)
            except ValueError:
                pass

        wait_seconds = min(wait_seconds + 10, 3600)
        print(f"    Rate limit. Aguardando {wait_seconds}s...")
        time.sleep(wait_seconds)

    def execute(
        self,
        query: str,
        variables: Dict[str, Any],
        context: str = "",
        max_retries: int = 3,
    ) -> Optional[Dict]:
        """
        Executa query GraphQL com retry e tratamento de erros.
        Retorna None em caso de falha permanente.
        """
        self._request_count += 1

        for attempt in range(max_retries + 1):
            try:
                response = self.session.post(
                    self.base_url,
                    json={"query": query, "variables": variables},
                    headers=self.headers,
                    timeout=60,
                )

                if response.status_code in (403, 429):
                    print(f"    Rate limit HTTP {response.status_code} ({context})")
                    self._wait_for_rate_limit(response)
                    continue

                response.raise_for_status()
                data = response.json()

                if "errors" in data:
                    error_msg = data["errors"][0].get("message", "")
                    if "rate limit" in error_msg.lower():
                        print(f"    Rate limit GraphQL ({context})")
                        self._wait_for_rate_limit(response)
                        continue

                    if attempt == max_retries:
                        log_error(context, f"GraphQL error: {error_msg}")
                        return None
                    time.sleep(2**attempt)
                    continue

                return data

            except requests.exceptions.Timeout:
                print(f"    Timeout ({context}), tentativa {attempt + 1}")
                if attempt == max_retries:
                    log_error(context, "Timeout após retries")
                    return None
                time.sleep(5)

            except requests.exceptions.RequestException as e:
                print(f"    Erro de rede ({context}): {e}")
                if attempt == max_retries:
                    log_error(context, str(e))
                    return None
                time.sleep(5)

        return None


class GitHubExtractor:
    """Extrator de PRs do GitHub com query unificada."""

    QUERY_LIST_PRS = """
    query($org: String!, $repo: String!, $cursor: String) {
      repository(owner: $org, name: $repo) {
        pullRequests(first: 25, after: $cursor, states: MERGED, 
                     orderBy: {field: CREATED_AT, direction: DESC}) {
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
    QUERY_LIST_REPOS = """
    query($org: String!, $cursor: String) {
      organization(login: $org) {
        repositories(first: 100, after: $cursor) {
          pageInfo { endCursor hasNextPage }
          nodes { name }
        }
      }
    }
    """

    def __init__(self, client: GraphQLClient):
        self.client = client

    def list_repos(self, org: str) -> List[str]:
        """Lista todos os repositórios de uma organização."""
        repos = []
        cursor = None

        while True:
            data = self.client.execute(
                self.QUERY_LIST_REPOS,
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

    def _process_pr(self, org: str, repo: str, pr: Dict) -> Optional[PRMetrics]:
        """Processa um PR a partir dos dados já disponíveis na query unificada."""
        pr_number = pr.get("number")

        try:
            author = pr["author"]["login"] if pr.get("author") else "deleted_user"

            reviews_data = (pr.get("reviews") or {}).get("nodes", []) or []
            reviewers = set()
            first_review_at = None
            if reviews_data:
                reviews_data.sort(key=lambda x: x.get("createdAt", ""))
                first_review_at = reviews_data[0].get("createdAt")
                for r in reviews_data:
                    if r.get("author"):
                        reviewers.add(r["author"]["login"])

            comments_data = pr.get("comments") or {}
            comment_nodes = comments_data.get("nodes", []) or []
            comments_count = comments_data.get("totalCount", 0) or 0
            commenters = set()
            for c in comment_nodes:
                if c.get("author"):
                    commenters.add(c["author"]["login"])

            threads_data = pr.get("reviewThreads") or {}
            threads_count = threads_data.get("totalCount", 0) or 0
            thread_nodes = threads_data.get("nodes", []) or []
            for thread in thread_nodes:
                for tc in (thread.get("comments") or {}).get("nodes", []) or []:
                    if tc.get("author"):
                        commenters.add(tc["author"]["login"])

            all_responses = []
            for r in reviews_data:
                if r.get("author") and r["author"]["login"] != author:
                    all_responses.append(
                        {"user": r["author"]["login"], "ts": r["createdAt"]}
                    )
            for c in comment_nodes:
                if c.get("author") and c["author"]["login"] != author:
                    all_responses.append(
                        {"user": c["author"]["login"], "ts": c["createdAt"]}
                    )
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

            commits_data = pr.get("commits") or {}
            commits_count = commits_data.get("totalCount", 0) or 0
            commit_nodes = commits_data.get("nodes", []) or []
            commit_authors = set()
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
                else 0
            )

            files_data = pr.get("files") or {}
            file_nodes = files_data.get("nodes", []) or []
            file_paths = [f["path"] for f in file_nodes if f.get("path")]

            repo_prefix = f"{org}/{repo}"
            doc_count, extensions, file_hashes = analyze_files(file_paths, repo_prefix)

            label_nodes = (pr.get("labels") or {}).get("nodes", []) or []
            label_names = [l["name"] for l in label_nodes if l.get("name")]

            body = pr.get("body") or ""
            title = pr.get("title") or ""

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
                comments=comments_count + threads_count,
                files_changed=pr.get("changedFiles", 0),
                additions=pr.get("additions", 0),
                deletions=pr.get("deletions", 0),
                churn=pr.get("additions", 0) + pr.get("deletions", 0),
                doc_files_count=doc_count,
                is_doc_pr=doc_count > 0
                and len(file_paths) > 0
                and (doc_count / len(file_paths) > 0.5),
                file_extensions=extensions,
                file_hashes=file_hashes,
                title_length=len(title),
                description_length=len(body),
                labels_count=len(label_names),
                labels=",".join(label_names),
            )

        except Exception as e:
            log_error(f"GitHub/{org}/{repo}/PR#{pr_number}", str(e))
            print(f"    Erro no PR #{pr_number}: {e}")
            return None

    def extract_repo(
        self,
        org: str,
        repo: str,
        since_date: Optional[str] = None,
        processed_prs: Optional[Set[str]] = None,
    ) -> List[PRMetrics]:
        """Extrai métricas de todos os PRs de um repositório."""
        results = []
        cursor = None
        pr_count = 0
        skipped = 0

        query = self.QUERY_LIST_PRS.replace("$maxCommits", str(MAX_COMMITS_PER_QUERY))
        query = query.replace("$maxReviews", str(MAX_REVIEWS_PER_QUERY))
        query = query.replace("$maxComments", str(MAX_COMMENTS_PER_QUERY))
        query = query.replace("$maxFiles", str(MAX_FILES_PER_QUERY))

        print(f"  Extraindo: {org}/{repo}")

        while True:
            data = self.client.execute(
                query,
                {"org": org, "repo": repo, "cursor": cursor},
                f"GitHub/{org}/{repo}/list_prs",
            )

            if not data:
                print(f"    Falha ao listar PRs de {repo}")
                break

            repo_data = data.get("data", {}).get("repository", {})
            if not repo_data:
                print(f"    Repositório {repo} não encontrado ou vazio")
                break

            pr_data = repo_data.get("pullRequests", {})
            pr_nodes = pr_data.get("nodes", []) or []

            for pr in pr_nodes:
                if since_date and pr.get("createdAt", "") < since_date:
                    print(f"    ✓ Chegou ao limite temporal ({since_date[:10]})")
                    return results

                pr_number = pr.get("number")
                pr_key = f"GitHub/{org}/{repo}/#{pr_number}"

                if processed_prs and pr_key in processed_prs:
                    continue

                pr_count += 1
                if pr_count % 25 == 0:
                    print(
                        f"    → Processando PR #{pr_number} [{pr_count} PRs, {skipped} skipped]"
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

        print(f"    ✓ Concluído: {len(results)} PRs extraídos, {skipped} skipped")
        return results


class GitLabExtractor:
    """Extrator de MRs do GitLab com extração em 2 fases."""

    QUERY_LIST_MRS = """
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

    QUERY_MR_DETAILS = """
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

    QUERY_LIST_PROJECTS = """
    query($group: ID!, $cursor: String) {
      group(fullPath: $group) {
        projects(includeSubgroups: true, first: 50, after: $cursor) {
          pageInfo { endCursor hasNextPage }
          nodes { fullPath name }
        }
      }
    }
    """

    def __init__(self, client: GraphQLClient):
        self.client = client

    def list_projects(self, group_path: str) -> List[str]:
        """Lista todos os projetos de um grupo."""
        projects = []
        cursor = None

        while True:
            data = self.client.execute(
                self.QUERY_LIST_PROJECTS,
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

    def _fetch_mr_details(self, project_path: str, mr_iid: int) -> Optional[Dict]:
        """Busca detalhes de um MR específico (discussões, commits, approvals)."""
        return self.client.execute(
            self.QUERY_MR_DETAILS,
            {"path": project_path, "mrIid": str(mr_iid)},
            f"GitLab/{project_path}/MR!{mr_iid}/details",
        )

    def _process_mr(
        self, group: str, repo: str, mr_basic: Dict, mr_details: Dict
    ) -> Optional[PRMetrics]:
        """Processa um MR a partir dos dados básicos (fase 1) + detalhes (fase 2)."""
        mr_iid = mr_basic.get("iid")

        try:
            author = (
                mr_basic["author"]["username"]
                if mr_basic.get("author")
                else "deleted_user"
            )

            reviewers = set()
            approved_by = mr_details.get("approvedBy") or {}
            if approved_by and approved_by.get("nodes"):
                for app in approved_by["nodes"]:
                    if app.get("username"):
                        reviewers.add(app["username"])

            all_notes = []
            external_notes = []
            commenters = set()
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

            first_review_at = None
            first_human_response_at = None
            if external_notes:
                external_notes.sort(key=lambda x: x.get("createdAt", ""))
                first_review_at = external_notes[0].get("createdAt")
                for note in external_notes:
                    if not is_bot_user(note["author"]["username"]):
                        first_human_response_at = note["createdAt"]
                        break

            commit_nodes = (mr_details.get("commits") or {}).get("nodes", []) or []
            commit_authors = set()
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
                else 0
            )

            diff_stats = mr_basic.get("diffStatsSummary") or {}
            additions = diff_stats.get("additions", 0) or 0
            deletions = diff_stats.get("deletions", 0) or 0
            file_count = diff_stats.get("fileCount", 0) or 0

            label_nodes = (mr_basic.get("labels") or {}).get("nodes", []) or []
            label_names = [l["title"] for l in label_nodes if l.get("title")]

            title = mr_basic.get("title") or ""
            description = mr_basic.get("description") or ""
            title_desc = (title + " " + description).lower()
            is_doc_pr = "doc" in title_desc or "readme" in title_desc

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
                doc_files_count=0,
                is_doc_pr=is_doc_pr,
                file_extensions="",
                file_hashes="",
                title_length=len(title),
                description_length=len(description),
                labels_count=len(label_names),
                labels=",".join(label_names),
            )

        except Exception as e:
            log_error(f"GitLab/{group}/{repo}/MR!{mr_iid}", str(e))
            print(f"    Erro no MR !{mr_iid}: {e}")
            return None

    def extract_project(
        self,
        group_path: str,
        repo: str,
        since_date: Optional[str] = None,
        processed_prs: Optional[Set[str]] = None,
    ) -> List[PRMetrics]:
        """Extrai métricas de todos os MRs de um projeto (2 fases)."""
        results = []
        cursor = None
        mr_count = 0
        skipped = 0
        project_path = f"{group_path}/{repo}"

        print(f"  Extraindo: {project_path}")

        while True:
            data = self.client.execute(
                self.QUERY_LIST_MRS,
                {"path": project_path, "cursor": cursor},
                f"GitLab/{project_path}/list_mrs",
            )

            if not data:
                print(f"    Falha ao listar MRs de {repo}")
                break

            project_data = data.get("data", {}).get("project", {})
            if not project_data:
                print(f"    Projeto {repo} não encontrado")
                break

            mr_data = project_data.get("mergeRequests", {})
            mr_nodes = mr_data.get("nodes", []) or []

            for mr_basic in mr_nodes:
                if since_date and mr_basic.get("createdAt", "") < since_date:
                    print(f"    ✓ Chegou ao limite temporal ({since_date[:10]})")
                    return results

                mr_iid = mr_basic.get("iid")
                mr_key = f"GitLab/{group_path}/{repo}/!{mr_iid}"

                if processed_prs and mr_key in processed_prs:
                    continue

                mr_count += 1
                if mr_count % 10 == 0:
                    print(
                        f"    → Processando MR !{mr_iid} [{mr_count} MRs, {skipped} skipped]"
                    )

                details_data = self._fetch_mr_details(project_path, mr_iid)
                if not details_data:
                    log_error(
                        f"GitLab/{project_path}/MR!{mr_iid}",
                        "Falha ao buscar detalhes",
                    )
                    skipped += 1
                    continue

                mr_details = (
                    (details_data.get("data") or {}).get("project") or {}
                ).get("mergeRequest") or {}

                metrics = self._process_mr(group_path, repo, mr_basic, mr_details)
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

        print(f"    ✓ Concluído: {len(results)} MRs extraídos, {skipped} skipped")
        return results


class DataPersistence:
    """Gerencia persistência de dados com append incremental."""

    def __init__(self, output_file: str):
        self.output_file = output_file
        self._ensure_dir()

    def _ensure_dir(self):
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

    def get_processed_prs(self) -> Set[str]:
        """Retorna conjunto de PRs já processados (para skip)."""
        if not os.path.exists(self.output_file):
            return set()
        try:
            df = pd.read_csv(
                self.output_file, usecols=["platform", "org", "repo", "id"]
            )
            processed = set()
            for _, row in df.iterrows():
                prefix = "#" if row["platform"] == "GitHub" else "!"
                key = (
                    f"{row['platform']}/{row['org']}/{row['repo']}/{prefix}{row['id']}"
                )
                processed.add(key)
            return processed
        except Exception as e:
            print(f"⚠️ Erro ao ler processados: {e}")
            return set()

    def save_batch(self, metrics_list: List[PRMetrics]):
        """Salva batch de métricas no CSV."""
        if not metrics_list:
            return

        data = [asdict(m) for m in metrics_list]
        df = pd.DataFrame(data)

        date_cols = [
            "created_at",
            "merged_at",
            "first_review_at",
            "first_human_response_at",
        ]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

        if "merged_at" in df.columns and "created_at" in df.columns:
            df["lead_time_hours"] = (
                df["merged_at"] - df["created_at"]
            ).dt.total_seconds() / 3600

        if "first_review_at" in df.columns and "created_at" in df.columns:
            df["time_to_first_review_hours"] = (
                df["first_review_at"] - df["created_at"]
            ).dt.total_seconds() / 3600

        if "first_human_response_at" in df.columns and "created_at" in df.columns:
            df["time_to_first_human_response_hours"] = (
                df["first_human_response_at"] - df["created_at"]
            ).dt.total_seconds() / 3600

        if "churn" in df.columns and "comments" in df.columns:
            df["discussion_density"] = df.apply(
                lambda x: x["comments"] / x["churn"] if x["churn"] > 0 else 0, axis=1
            )

        header = not os.path.exists(self.output_file)
        df.to_csv(self.output_file, mode="a", index=False, header=header)
        print(f"  Salvos {len(metrics_list)} registros")


class ExtractionOrchestrator:
    """Orquestra a extração de todos os targets."""

    def __init__(self):
        self.persistence = DataPersistence(OUTPUT_FILE)
        self.github_client = (
            GraphQLClient("https://api.github.com/graphql", GITHUB_TOKEN, "GitHub")
            if GITHUB_TOKEN
            else None
        )
        self.gitlab_client = (
            GraphQLClient("https://gitlab.com/api/graphql", GITLAB_TOKEN, "GitLab")
            if GITLAB_TOKEN
            else None
        )

    def run(self, targets: List[Dict] = None):
        """Executa extração para todos os targets."""
        targets = targets or TARGETS
        processed = self.persistence.get_processed_prs()
        print(f"PRs já processados: {len(processed)}")

        for target in targets:
            try:
                if target["type"] == "github":
                    self._process_github_target(target, processed)
                elif target["type"] == "gitlab":
                    self._process_gitlab_target(target, processed)
            except Exception as e:
                print(f"Erro fatal em {target}: {e}")
                log_error(f"Target {target}", str(e))

    def _process_github_target(self, target: Dict, processed: Set[str]):
        """Processa target do GitHub."""
        if not self.github_client:
            print("GITHUB_TOKEN não configurado")
            return

        org = target["org"]
        repos = target.get("repos")
        since = target.get("since")

        print(f"\n{'=' * 60}")
        print(f"GitHub: {org}")
        print(f"{'=' * 60}")

        extractor = GitHubExtractor(self.github_client)

        if not repos:
            print("  Listando repositórios...")
            repos = extractor.list_repos(org)
            print(f"  Encontrados: {len(repos)} repos")

        all_metrics = []

        for repo in repos:
            metrics = extractor.extract_repo(org, repo, since, processed)
            all_metrics.extend(metrics)

            if len(all_metrics) >= SAVE_BATCH_SIZE:
                self.persistence.save_batch(all_metrics)
                all_metrics = []

        if all_metrics:
            self.persistence.save_batch(all_metrics)

    def _process_gitlab_target(self, target: Dict, processed: Set[str]):
        """Processa target do GitLab."""
        if not self.gitlab_client:
            print("GITLAB_TOKEN não configurado")
            return

        group = target["group_path"]
        repos = target.get("repos")
        since = target.get("since")

        print(f"\n{'=' * 60}")
        print(f"GitLab: {group}")
        print(f"{'=' * 60}")

        extractor = GitLabExtractor(self.gitlab_client)

        if not repos:
            print("  Listando projetos...")
            repos = extractor.list_projects(group)
            print(f"  Encontrados: {len(repos)} projetos")

        all_metrics = []

        for repo in repos:
            metrics = extractor.extract_project(group, repo, since, processed)
            all_metrics.extend(metrics)

            if len(all_metrics) >= SAVE_BATCH_SIZE:
                self.persistence.save_batch(all_metrics)
                all_metrics = []

        if all_metrics:
            self.persistence.save_batch(all_metrics)


def main():
    print("Iniciando extração de métricas de PRs")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Error log: {ERROR_LOG_FILE}")
    print()

    orchestrator = ExtractionOrchestrator()
    orchestrator.run()

    print("\nExtração concluída!")


if __name__ == "__main__":
    main()
