import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
OUTPUT_FILE = "metrics/data/bronze/prs.csv"

# Palavras-chave para identificar bots
BOT_KEYWORDS = [
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
]


def is_bot_user(username):
    """Identifica se o usuário é um bot baseado em palavras-chave comuns"""
    if not username:
        return True
    username_lower = str(username).lower()
    return any(keyword in username_lower for keyword in BOT_KEYWORDS)


TARGETS = [
    # {"type": "github", "org": "GovHub-br"},
    # {"type": "github", "org": "lablivre-unb"},
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
]


def get_session():
    session = requests.Session()
    retry = Retry(
        total=10,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST", "GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


session = get_session()


def get_processed_repos():
    if not os.path.exists(OUTPUT_FILE):
        return set()
    try:
        df = pd.read_csv(OUTPUT_FILE, usecols=["platform", "repo"])
        processed = set(df["platform"] + "/" + df["repo"])
        return processed
    except Exception:
        return set()


def save_chunk(new_data):
    if not new_data:
        return

    df = pd.DataFrame(new_data)

    # Converter colunas de data
    cols_date = [
        "created_at",
        "merged_at",
        "first_review_at",
        "first_human_response_at",
    ]
    for col in cols_date:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    # Lead time (tempo total do PR)
    if "merged_at" in df.columns and "created_at" in df.columns:
        df["lead_time_hours"] = (
            df["merged_at"] - df["created_at"]
        ).dt.total_seconds() / 3600

    # Tempo até primeira review
    if "first_review_at" in df.columns and "created_at" in df.columns:
        df["time_to_first_review_hours"] = (
            df["first_review_at"] - df["created_at"]
        ).dt.total_seconds() / 3600

    # Tempo até primeira resposta humana (não-bot)
    if "first_human_response_at" in df.columns and "created_at" in df.columns:
        df["time_to_first_human_response_hours"] = (
            df["first_human_response_at"] - df["created_at"]
        ).dt.total_seconds() / 3600

    # Densidade de discussão
    if "churn" in df.columns:
        df["discussion_density"] = df.apply(
            lambda x: x["comments"] / x["churn"] if x.get("churn", 0) > 0 else 0, axis=1
        )

    header = not os.path.exists(OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, mode="a", index=False, header=header)
    print(f"      [Salvo] {len(new_data)} registros adicionados ao disco.")


def _wait_for_rate_limit_reset(response):
    """Detecta rate limit e espera até o reset + margem de 10s."""
    reset_ts = response.headers.get("x-ratelimit-reset") or response.headers.get(
        "ratelimit-reset"
    )
    retry_after = response.headers.get("retry-after")

    wait_seconds = None

    if retry_after:
        # retry-after pode vir em segundos diretamente
        try:
            wait_seconds = int(retry_after)
        except ValueError:
            pass

    if wait_seconds is None and reset_ts:
        try:
            wait_seconds = max(int(reset_ts) - int(time.time()), 0)
        except ValueError:
            pass

    if wait_seconds is None:
        wait_seconds = 60  # fallback conservador

    wait_seconds += 10  # margem de segurança
    print(f"    Rate limit atingido. Aguardando {wait_seconds}s até reset...")
    time.sleep(wait_seconds)


def run_query(url, json_body, headers, context="", max_retries=3):
    for attempt in range(max_retries + 1):
        try:
            response = session.post(url, json=json_body, headers=headers, timeout=120)

            # Rate limit via status HTTP (403 ou 429)
            if response.status_code in (403, 429):
                print(f"    ! Rate limit HTTP {response.status_code} ({context})")
                _wait_for_rate_limit_reset(response)
                continue

            response.raise_for_status()

            # Rate limit via body GraphQL (API retorna 200 mas com erro)
            json_data = response.json()
            if "errors" in json_data:
                error_msg = json_data["errors"][0].get("message", "")
                if "rate limit" in error_msg.lower():
                    print(f"    ! Rate limit GraphQL: {error_msg}")
                    _wait_for_rate_limit_reset(response)
                    continue

            return response

        except requests.exceptions.RequestException as e:
            print(f"    ! Erro de rede ({context}), tentativa {attempt + 1}: {e}")
            time.sleep(5)

    print(f"    Falha após {max_retries + 1} tentativas ({context})")
    return None


def analyze_files(file_list):
    if not file_list:
        return 0, "", ""

    doc_extensions = [".md", ".txt", ".rst", ".pdf", ".docx"]
    doc_count = 0
    extensions = set()

    for path in file_list:
        ext = os.path.splitext(path)[1].lower()
        if ext:
            extensions.add(ext)

        if (
            ext in doc_extensions
            or "docs/" in path.lower()
            or "documentation/" in path.lower()
        ):
            doc_count += 1

    paths_str = ";".join(file_list[:20])
    if len(file_list) > 20:
        paths_str += ";..."

    extensions_str = ",".join(list(extensions))

    return doc_count, extensions_str, paths_str


def process_github(target, processed_set):
    org_name = target["org"]
    specific_repos = target.get("repos")  # Lista de repos específicos (opcional)
    since_date = target.get("since")  # Filtro temporal (opcional)

    filter_info = ""
    if specific_repos:
        filter_info += f" [repos: {', '.join(specific_repos)}]"
    if since_date:
        filter_info += f" [desde: {since_date[:10]}]"

    print(f"\n--- [GitHub] Iniciando: {org_name}{filter_info} ---")
    url = "https://api.github.com/graphql"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}

    repo_names = []

    # Se repos específicos foram informados, usar diretamente
    if specific_repos:
        repo_names = specific_repos
        print(f"  -> Usando repositórios específicos: {repo_names}")
    else:
        # Caso contrário, listar todos os repos da organização
        cursor = None
        has_next = True

        print("  -> Listando repositórios da organização...")
        while has_next:
            query = """
            query($org: String!, $cursor: String) {
              organization(login: $org) {
                repositories(first: 100, after: $cursor) {
                  pageInfo { endCursor hasNextPage }
                  nodes { name }
                }
              }
            }
            """
            resp = run_query(
                url,
                {"query": query, "variables": {"org": org_name, "cursor": cursor}},
                headers,
            )
            if not resp:
                break
            data = resp.json()
            if "errors" in data:
                print(f"    Erro ao listar repos: {data['errors'][0]['message']}")
                break

            repos = data["data"]["organization"]["repositories"]
            for node in repos["nodes"]:
                repo_names.append(node["name"])
            cursor = repos["pageInfo"]["endCursor"]
            has_next = repos["pageInfo"]["hasNextPage"]

    for i, repo in enumerate(repo_names):
        identifier = f"GitHub/{repo}"
        if identifier in processed_set:
            continue

        print(f"  [{i + 1}/{len(repo_names)}] Baixando: {repo}")
        repo_data_chunk = []
        cursor = None
        has_next_pr = True
        pr_count = 0

        while has_next_pr:
            query = """
            query($org: String!, $repo: String!, $cursor: String) {
              repository(owner: $org, name: $repo) {
                pullRequests(first: 10, after: $cursor, states: MERGED, orderBy: {field: CREATED_AT, direction: DESC}) {
                  pageInfo { endCursor hasNextPage }
                  nodes {
                    number createdAt mergedAt additions deletions changedFiles
                    title body 
                    author { login }
                    labels(first: 20) {
                      totalCount
                      nodes { name }
                    }
                    commits(first: 100) { 
                      totalCount
                      nodes {
                        commit {
                          message
                          author { user { login } }
                        }
                      }
                    }
                    reviews(first: 50) {
                      nodes { author { login } createdAt }
                    }
                    comments(first: 50) {
                      totalCount
                      nodes { author { login } createdAt }
                    }
                    reviewThreads(first: 50) { 
                      totalCount
                      nodes {
                        comments(first: 20) {
                          nodes { author { login } createdAt }
                        }
                      }
                    }
                    files(first: 50) {
                      nodes { path }
                    }
                  }
                }
              }
            }
            """
            resp = run_query(
                url,
                {
                    "query": query,
                    "variables": {"org": org_name, "repo": repo, "cursor": cursor},
                },
                headers,
            )
            if not resp:
                break

            json_res = resp.json()
            if "errors" in json_res:
                print(
                    f"    ! Erro GraphQL no repo {repo}: {json_res['errors'][0]['message']}"
                )
                break

            data_node = json_res.get("data")
            if not data_node:
                print(f"    ! Resposta sem dados para {repo}. Pulando.")
                break

            repo_node = data_node.get("repository")
            if not repo_node:
                print(
                    f"    ! Repositório {repo} retornou vazio (provavelmente sem branch/commits). Pulando."
                )
                break

            pr_data = repo_node["pullRequests"]

            for pr in pr_data["nodes"]:
                # Aplicar filtro temporal se especificado
                if since_date and pr["createdAt"] < since_date:
                    # Se chegamos em PRs mais antigas que a data limite, parar paginação
                    has_next_pr = False
                    break

                pr_count += 1
                print(
                    f"    -> Processando PR #{pr.get('number')} [{pr_count} PRs processados]"
                )

                pr_author = (
                    pr["author"]["login"] if pr.get("author") else "deleted_user"
                )

                # Processar reviews
                reviews_data = pr.get("reviews") or {}
                review_nodes = reviews_data.get("nodes", []) or []
                reviewers = set()
                first_review_at = None
                if review_nodes:
                    review_nodes.sort(key=lambda x: x["createdAt"])
                    first_review_at = review_nodes[0]["createdAt"]
                    for r in review_nodes:
                        if r.get("author"):
                            reviewers.add(r["author"]["login"])

                # Processar comentários (issues comments)
                comments_data = pr.get("comments") or {}
                comment_nodes = comments_data.get("nodes", []) or []
                comments_count = comments_data.get("totalCount", 0) or 0
                commenters = set()
                for c in comment_nodes:
                    if c.get("author"):
                        commenters.add(c["author"]["login"])

                # Processar review threads (inline comments)
                review_threads_data = pr.get("reviewThreads") or {}
                review_threads_count = review_threads_data.get("totalCount", 0) or 0
                thread_nodes = review_threads_data.get("nodes", []) or []
                for thread in thread_nodes:
                    thread_comments = thread.get("comments", {}).get("nodes", []) or []
                    for tc in thread_comments:
                        if tc.get("author"):
                            commenters.add(tc["author"]["login"])

                # Coletar todas as respostas (reviews + comments) para calcular tempo até primeira resposta humana
                all_responses = []
                for r in review_nodes:
                    if r.get("author") and r["author"]["login"] != pr_author:
                        all_responses.append(
                            {"user": r["author"]["login"], "created_at": r["createdAt"]}
                        )
                for c in comment_nodes:
                    if c.get("author") and c["author"]["login"] != pr_author:
                        all_responses.append(
                            {"user": c["author"]["login"], "created_at": c["createdAt"]}
                        )
                for thread in thread_nodes:
                    thread_comments = thread.get("comments", {}).get("nodes", []) or []
                    for tc in thread_comments:
                        if tc.get("author") and tc["author"]["login"] != pr_author:
                            all_responses.append(
                                {
                                    "user": tc["author"]["login"],
                                    "created_at": tc["createdAt"],
                                }
                            )

                # Ordenar e encontrar primeira resposta humana (não-bot)
                all_responses.sort(key=lambda x: x["created_at"])
                first_human_response_at = None
                for resp in all_responses:
                    if not is_bot_user(resp["user"]):
                        first_human_response_at = resp["created_at"]
                        break

                # Processar commits (autores e mensagens)
                commits_data = pr.get("commits") or {}
                commits_count = commits_data.get("totalCount", 0) or 0
                commit_nodes = commits_data.get("nodes", []) or []
                commit_authors = set()
                commit_message_lengths = []
                for cn in commit_nodes:
                    commit = cn.get("commit", {})
                    # Autor do commit
                    commit_author_data = commit.get("author", {}) or {}
                    commit_user = commit_author_data.get("user", {})
                    if commit_user and commit_user.get("login"):
                        commit_authors.add(commit_user["login"])
                    # Mensagem do commit
                    msg = commit.get("message", "") or ""
                    if msg:
                        commit_message_lengths.append(len(msg))

                avg_commit_msg_len = (
                    sum(commit_message_lengths) / len(commit_message_lengths)
                    if commit_message_lengths
                    else 0
                )

                # Processar files
                files_data = pr.get("files") or {}
                file_nodes = files_data.get("nodes", []) or []
                file_paths_list = [f["path"] for f in file_nodes if f.get("path")]
                doc_count, extensions_str, paths_str = analyze_files(file_paths_list)

                # Comprimentos de texto
                body_len = len(pr["body"]) if pr.get("body") else 0
                title_len = len(pr["title"]) if pr.get("title") else 0

                # Labels
                labels_data = pr.get("labels") or {}
                labels_count = labels_data.get("totalCount", 0) or 0
                label_nodes = labels_data.get("nodes", []) or []
                label_names = [l["name"] for l in label_nodes if l.get("name")]

                repo_data_chunk.append(
                    {
                        "platform": "GitHub",
                        "org": org_name,
                        "repo": repo,
                        "id": pr.get("number"),
                        "author": pr_author,
                        "created_at": pr.get("createdAt"),
                        "merged_at": pr.get("mergedAt"),
                        "first_review_at": first_review_at,
                        "first_human_response_at": first_human_response_at,
                        "reviewers": ",".join(reviewers),
                        "commenters": ",".join(commenters),
                        "commit_authors": ",".join(commit_authors),
                        "commits": commits_count,
                        "avg_commit_message_length": round(avg_commit_msg_len, 2),
                        "reviews_count": len(review_nodes),
                        "comments": comments_count + review_threads_count,
                        "files_changed": pr.get("changedFiles", 0) or 0,
                        "additions": pr.get("additions", 0) or 0,
                        "deletions": pr.get("deletions", 0) or 0,
                        "churn": (pr.get("additions", 0) or 0)
                        + (pr.get("deletions", 0) or 0),
                        "doc_files_count": doc_count,
                        "is_doc_pr": doc_count > 0
                        and len(file_paths_list) > 0
                        and (doc_count / len(file_paths_list) > 0.5),
                        "file_extensions": extensions_str,
                        "file_paths": paths_str,
                        "title_length": title_len,
                        "description_length": body_len,
                        "labels_count": labels_count,
                        "labels": ",".join(label_names),
                    }
                )

            has_next_pr = pr_data["pageInfo"]["hasNextPage"]
            cursor = pr_data["pageInfo"]["endCursor"]
            time.sleep(0.5)

        if repo_data_chunk:
            save_chunk(repo_data_chunk)


def process_gitlab(target, processed_set):
    group = target["group_path"]
    specific_repos = target.get("repos")  # Lista de repos específicos (opcional)
    since_date = target.get("since")  # Filtro temporal (opcional)

    filter_info = ""
    if specific_repos:
        filter_info += f" [repos: {', '.join(specific_repos)}]"
    if since_date:
        filter_info += f" [desde: {since_date[:10]}]"

    print(f"\n--- [GitLab] Iniciando: {group}{filter_info} ---")
    url = "https://gitlab.com/api/graphql"
    headers = {"Authorization": f"Bearer {GITLAB_TOKEN}"}

    projects = []

    # Se repos específicos foram informados, usar diretamente
    if specific_repos:
        projects = specific_repos
        print(f"  -> Usando repositórios específicos: {projects}")
    else:
        # Caso contrário, listar todos os projetos do grupo
        cursor = None
        has_next = True
        print("  -> Listando projetos do grupo...")
        while has_next:
            query = """
            query($group: ID!, $cursor: String) {
              group(fullPath: $group) {
                projects(includeSubgroups: true, first: 50, after: $cursor) {
                  pageInfo { endCursor hasNextPage }
                  nodes { fullPath name }
                }
              }
            }
            """
            resp = run_query(
                url,
                {"query": query, "variables": {"group": group, "cursor": cursor}},
                headers,
            )
            if not resp:
                break
            data = resp.json()
            if not data.get("data", {}).get("group"):
                break
            projs = data["data"]["group"]["projects"]
            for node in projs["nodes"]:
                projects.append(node["name"])
            cursor = projs["pageInfo"]["endCursor"]
            has_next = projs["pageInfo"]["hasNextPage"]

    for i, repo in enumerate(projects):
        identifier = f"GitLab/{repo}"
        if identifier in processed_set:
            continue

        print(f"  [{i + 1}/{len(projects)}] Baixando: {repo}")
        repo_data_chunk = []
        cursor = None
        has_next_mr = True
        mr_count = 0
        project_full_path = f"{group}/{repo}"

        while has_next_mr:
            query = """
            query($path: ID!, $cursor: String) {
              project(fullPath: $path) {
                mergeRequests(state: merged, first: 10, after: $cursor) {
                  pageInfo { endCursor hasNextPage }
                  nodes {
                    iid createdAt mergedAt commitCount description title
                    author { username }
                    diffStatsSummary { additions deletions fileCount }
                    labels {
                      nodes { title }
                    }
                    commits {
                      nodes {
                        author { username }
                        message
                      }
                    }
                    discussions(first: 50) {
                      nodes {
                        notes(first: 20) {
                          nodes { author { username } createdAt }
                        }
                      }
                    }
                    approvedBy { nodes { username } }
                  }
                }
              }
            }
            """
            resp = run_query(
                url,
                {
                    "query": query,
                    "variables": {"path": project_full_path, "cursor": cursor},
                },
                headers,
            )
            if not resp:
                print(f"    Erro na requisição para {project_full_path}")
                break

            json_res = resp.json()
            if "errors" in json_res:
                print(f"    Erro GraphQL: {json_res['errors'][0]['message']}")
                break
            if not json_res.get("data", {}).get("project"):
                print(f"    Projeto não encontrado: {project_full_path}")
                break

            mrs = json_res["data"]["project"]["mergeRequests"]

            for mr in mrs["nodes"]:
                # Aplicar filtro temporal se especificado
                if since_date and mr["createdAt"] < since_date:
                    # Se chegamos em MRs mais antigas que a data limite, parar paginação
                    has_next_mr = False
                    break

                mr_count += 1
                print(
                    f"    -> Processando MR !{mr.get('iid')} [{mr_count} MRs processados]"
                )

                author_username = mr["author"]["username"] if mr["author"] else None

                # Processar reviewers (quem aprovou)
                reviewers = set()
                if mr["approvedBy"] and mr["approvedBy"]["nodes"]:
                    for app in mr["approvedBy"]["nodes"]:
                        reviewers.add(app["username"])

                # Processar discussions/notes (quem comentou)
                external_notes = []
                commenters = set()
                for disc in mr["discussions"]["nodes"] if mr["discussions"] else []:
                    for note in disc["notes"]["nodes"] if disc["notes"] else []:
                        if note.get("author"):
                            note_author = note["author"]["username"]
                            if note_author != author_username:
                                external_notes.append(note)
                                commenters.add(note_author)
                                reviewers.add(
                                    note_author
                                )  # Quem comenta também é reviewer

                # Encontrar primeira resposta humana (não-bot, não-autor)
                first_review_at = None
                first_human_response_at = None
                if external_notes:
                    external_notes.sort(key=lambda x: x["createdAt"])
                    first_review_at = external_notes[0]["createdAt"]
                    # Encontrar primeira resposta humana
                    for note in external_notes:
                        if not is_bot_user(note["author"]["username"]):
                            first_human_response_at = note["createdAt"]
                            break

                # Processar commits (autores e mensagens)
                commits_data = mr.get("commits") or {}
                commit_nodes = commits_data.get("nodes", []) or []
                commit_authors = set()
                commit_message_lengths = []
                for cn in commit_nodes:
                    # Autor do commit
                    commit_author_data = cn.get("author", {}) or {}
                    if commit_author_data.get("username"):
                        commit_authors.add(commit_author_data["username"])
                    # Mensagem do commit
                    msg = cn.get("message", "") or ""
                    if msg:
                        commit_message_lengths.append(len(msg))

                avg_commit_msg_len = (
                    sum(commit_message_lengths) / len(commit_message_lengths)
                    if commit_message_lengths
                    else 0
                )

                # Diff stats
                add = (
                    mr["diffStatsSummary"]["additions"]
                    if mr.get("diffStatsSummary")
                    else 0
                )
                dele = (
                    mr["diffStatsSummary"]["deletions"]
                    if mr.get("diffStatsSummary")
                    else 0
                )
                file_count = (
                    mr["diffStatsSummary"]["fileCount"]
                    if mr.get("diffStatsSummary")
                    else 0
                )

                # Heurística para doc PR
                title_desc = (mr["title"] + " " + (mr["description"] or "")).lower()
                is_doc_heuristic = "doc" in title_desc or "readme" in title_desc

                # Comprimentos de texto
                title_len = len(mr["title"]) if mr.get("title") else 0
                description_len = len(mr["description"]) if mr.get("description") else 0

                # Labels
                labels_data = mr.get("labels") or {}
                label_nodes = labels_data.get("nodes", []) or []
                labels_count = len(label_nodes)
                label_names = [l["title"] for l in label_nodes if l.get("title")]

                repo_data_chunk.append(
                    {
                        "platform": "GitLab",
                        "org": group,
                        "repo": repo,
                        "id": mr["iid"],
                        "author": author_username or "deleted_user",
                        "created_at": mr["createdAt"],
                        "merged_at": mr["mergedAt"],
                        "first_review_at": first_review_at,
                        "first_human_response_at": first_human_response_at,
                        "reviewers": ",".join(reviewers),
                        "commenters": ",".join(commenters),
                        "commit_authors": ",".join(commit_authors),
                        "commits": mr["commitCount"],
                        "avg_commit_message_length": round(avg_commit_msg_len, 2),
                        "reviews_count": len(mr["approvedBy"]["nodes"])
                        if mr["approvedBy"]
                        else 0,
                        "comments": len(external_notes),
                        "files_changed": file_count,
                        "additions": add,
                        "deletions": dele,
                        "churn": add + dele,
                        "doc_files_count": 0,
                        "is_doc_pr": is_doc_heuristic,
                        "file_extensions": "",
                        "file_paths": "",
                        "title_length": title_len,
                        "description_length": description_len,
                        "labels_count": labels_count,
                        "labels": ",".join(label_names),
                    }
                )

            has_next_mr = mrs["pageInfo"]["hasNextPage"]
            cursor = mrs["pageInfo"]["endCursor"]
            time.sleep(0.5)

        if repo_data_chunk:
            save_chunk(repo_data_chunk)


def main():
    processed = get_processed_repos()
    print(f"Registros anteriores detectados: {len(processed)}")
    for target in TARGETS:
        try:
            if target["type"] == "github":
                process_github(target, processed)
            elif target["type"] == "gitlab":
                process_gitlab(target, processed)
        except Exception as e:
            print(f"Erro fatal em {target}: {e}")


if __name__ == "__main__":
    main()
