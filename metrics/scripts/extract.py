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

TARGETS = [
    {"type": "github", "org": "GovHub-br"},
    {"type": "github", "org": "lablivre-unb"},
    {"type": "gitlab", "group_path": "lappis-unb"},
    {"type": "github", "org": "unb-mds"},
    {"type": "github", "org": "mdsreq-fga-unb"}
]

def get_session():
    session = requests.Session()
    retry = Retry(
        total=10,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST", "GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session

session = get_session()

def get_processed_repos():
    if not os.path.exists(OUTPUT_FILE):
        return set()
    try:
        df = pd.read_csv(OUTPUT_FILE, usecols=['platform', 'repo'])
        processed = set(df['platform'] + '/' + df['repo'])
        return processed
    except Exception:
        return set()

def save_chunk(new_data):
    if not new_data: return
    
    df = pd.DataFrame(new_data)
    
    cols_date = ['created_at', 'merged_at', 'first_review_at']
    for col in cols_date:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')

    if 'merged_at' in df.columns and 'created_at' in df.columns:
        df['lead_time_hours'] = (df['merged_at'] - df['created_at']).dt.total_seconds() / 3600
        
    if 'first_review_at' in df.columns and 'created_at' in df.columns:
        df['time_to_first_review_hours'] = (df['first_review_at'] - df['created_at']).dt.total_seconds() / 3600

    if 'churn' in df.columns:
        df['discussion_density'] = df.apply(lambda x: x['comments'] / x['churn'] if x.get('churn', 0) > 0 else 0, axis=1)

    header = not os.path.exists(OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, mode='a', index=False, header=header)
    print(f"      [Salvo] {len(new_data)} registros adicionados ao disco.")

def run_query(url, json_body, headers, context=""):
    try:
        response = session.post(url, json=json_body, headers=headers, timeout=120)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"    ! Erro de rede ({context}): {e}")
        time.sleep(5)
        return None

def analyze_files(file_list):
    if not file_list:
        return 0, "", ""
    
    doc_extensions = ['.md', '.txt', '.rst', '.pdf', '.docx']
    doc_count = 0
    extensions = set()
    
    for path in file_list:
        ext = os.path.splitext(path)[1].lower()
        if ext: extensions.add(ext)
        
        if ext in doc_extensions or 'docs/' in path.lower() or 'documentation/' in path.lower():
            doc_count += 1
            
    paths_str = ";".join(file_list[:20]) 
    if len(file_list) > 20: paths_str += ";..."
    
    extensions_str = ",".join(list(extensions))
    
    return doc_count, extensions_str, paths_str

def process_github(target, processed_set):
    org_name = target['org']
    print(f"\n--- [GitHub] Iniciando: {org_name} ---")
    url = 'https://api.github.com/graphql'
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    
    repo_names = []
    cursor = None
    has_next = True
    
    print("  -> Listando repositórios...")
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
        resp = run_query(url, {'query': query, 'variables': {'org': org_name, 'cursor': cursor}}, headers)
        if not resp: break
        data = resp.json()
        if 'errors' in data: 
            print(f"    Erro ao listar repos: {data['errors'][0]['message']}")
            break
            
        repos = data['data']['organization']['repositories']
        for node in repos['nodes']:
            repo_names.append(node['name'])
        cursor = repos['pageInfo']['endCursor']
        has_next = repos['pageInfo']['hasNextPage']
    
    for i, repo in enumerate(repo_names):
        identifier = f"GitHub/{repo}"
        if identifier in processed_set:
            continue
            
        print(f"  [{i+1}/{len(repo_names)}] Baixando: {repo}")
        repo_data_chunk = []
        cursor = None
        has_next_pr = True
        
        while has_next_pr:
            query = """
            query($org: String!, $repo: String!, $cursor: String) {
              repository(owner: $org, name: $repo) {
                pullRequests(first: 10, after: $cursor, states: MERGED, orderBy: {field: CREATED_AT, direction: DESC}) {
                  pageInfo { endCursor hasNextPage }
                  nodes {
                    number createdAt mergedAt additions deletions changedFiles
                    body 
                    author { login }
                    commits { totalCount }
                    reviews(first: 20) {
                      nodes { author { login } createdAt }
                    }
                    comments { totalCount }
                    reviewThreads { totalCount }
                    files(first: 50) {
                      nodes { path }
                    }
                  }
                }
              }
            }
            """
            resp = run_query(url, {'query': query, 'variables': {'org': org_name, 'repo': repo, 'cursor': cursor}}, headers)
            if not resp: break
            
            json_res = resp.json()
            if 'errors' in json_res: 
                print(f"    ! Erro GraphQL no repo {repo}: {json_res['errors'][0]['message']}")
                break
            
            data_node = json_res.get('data')
            if not data_node:
                print(f"    ! Resposta sem dados para {repo}. Pulando.")
                break

            repo_node = data_node.get('repository')
            if not repo_node:
                print(f"    ! Repositório {repo} retornou vazio (provavelmente sem branch/commits). Pulando.")
                break
                
            pr_data = repo_node['pullRequests']
            
            for pr in pr_data['nodes']:
                review_nodes = pr.get('reviews', {}).get('nodes', [])
                reviewers = set()
                first_review_at = None
                if review_nodes:
                    review_nodes.sort(key=lambda x: x['createdAt'])
                    first_review_at = review_nodes[0]['createdAt']
                    for r in review_nodes:
                        if r.get('author'): reviewers.add(r['author']['login'])

                file_nodes = pr.get('files', {}).get('nodes', [])
                file_paths_list = [f['path'] for f in file_nodes]
                doc_count, extensions_str, paths_str = analyze_files(file_paths_list)
                
                body_len = len(pr['body']) if pr['body'] else 0

                repo_data_chunk.append({
                    "platform": "GitHub", "org": org_name, "repo": repo,
                    "id": pr['number'], 
                    "author": pr['author']['login'] if pr['author'] else "deleted_user",
                    "created_at": pr['createdAt'], 
                    "merged_at": pr['mergedAt'],
                    "first_review_at": first_review_at,
                    "reviewers": ",".join(reviewers),
                    "commits": pr['commits']['totalCount'], 
                    "reviews_count": len(review_nodes),
                    "comments": pr['comments']['totalCount'] + pr['reviewThreads']['totalCount'],
                    "files_changed": pr['changedFiles'],
                    "additions": pr['additions'],
                    "deletions": pr['deletions'],
                    "churn": pr['additions'] + pr['deletions'],
                    "doc_files_count": doc_count,
                    "is_doc_pr": doc_count > 0 and (doc_count / len(file_paths_list) > 0.5),
                    "file_extensions": extensions_str,
                    "file_paths": paths_str,
                    "description_length": body_len
                })
            
            has_next_pr = pr_data['pageInfo']['hasNextPage']
            cursor = pr_data['pageInfo']['endCursor']
            time.sleep(0.5) 
            
        if repo_data_chunk:
            save_chunk(repo_data_chunk)

def process_gitlab(target, processed_set):
    group = target['group_path']
    print(f"\n--- [GitLab] Iniciando: {group} ---")
    url = 'https://gitlab.com/api/graphql'
    headers = {"Authorization": f"Bearer {GITLAB_TOKEN}"}
    
    projects = []
    cursor = None
    has_next = True
    
    print("  -> Listando projetos...")
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
        resp = run_query(url, {'query': query, 'variables': {'group': group, 'cursor': cursor}}, headers)
        if not resp: break
        data = resp.json()
        if not data.get('data', {}).get('group'): break
        projs = data['data']['group']['projects']
        projects.extend(projs['nodes'])
        cursor = projs['pageInfo']['endCursor']
        has_next = projs['pageInfo']['hasNextPage']

    for i, proj in enumerate(projects):
        identifier = f"GitLab/{proj['name']}"
        if identifier in processed_set: continue
            
        print(f"  [{i+1}/{len(projects)}] Baixando: {proj['name']}")
        repo_data_chunk = []
        cursor = None
        has_next_mr = True
        
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
            resp = run_query(url, {'query': query, 'variables': {'path': proj['fullPath'], 'cursor': cursor}}, headers)
            if not resp:
                break
            
            json_res = resp.json()
            if 'errors' in json_res: break
            if not json_res.get('data', {}).get('project'): break
            
            mrs = json_res['data']['project']['mergeRequests']
            
            for mr in mrs['nodes']:
                author_username = mr['author']['username'] if mr['author'] else None
                reviewers = set()
                if mr['approvedBy'] and mr['approvedBy']['nodes']:
                    for app in mr['approvedBy']['nodes']: reviewers.add(app['username'])
                
                external_notes = []
                for disc in (mr['discussions']['nodes'] if mr['discussions'] else []):
                    for note in (disc['notes']['nodes'] if disc['notes'] else []):
                        if note.get('author') and note['author']['username'] != author_username:
                            external_notes.append(note)
                            reviewers.add(note['author']['username'])

                first_review_at = None
                if external_notes:
                    external_notes.sort(key=lambda x: x['createdAt'])
                    first_review_at = external_notes[0]['createdAt']

                add = mr['diffStatsSummary']['additions'] if mr.get('diffStatsSummary') else 0
                dele = mr['diffStatsSummary']['deletions'] if mr.get('diffStatsSummary') else 0
                file_count = mr['diffStatsSummary']['fileCount'] if mr.get('diffStatsSummary') else 0
                
                title_desc = (mr['title'] + " " + (mr['description'] or "")).lower()
                is_doc_heuristic = 'doc' in title_desc or 'readme' in title_desc

                repo_data_chunk.append({
                    "platform": "GitLab", "org": group, "repo": proj['name'],
                    "id": mr['iid'], 
                    "author": author_username or "deleted_user",
                    "created_at": mr['createdAt'], 
                    "merged_at": mr['mergedAt'],
                    "first_review_at": first_review_at,
                    "reviewers": ",".join(reviewers),
                    "commits": mr['commitCount'], 
                    "reviews_count": len(mr['approvedBy']['nodes']) if mr['approvedBy'] else 0, 
                    "comments": len(external_notes),
                    "files_changed": file_count,
                    "additions": add, "deletions": dele, "churn": add + dele,
                    "doc_files_count": 0, 
                    "is_doc_pr": is_doc_heuristic, 
                    "file_extensions": "", 
                    "file_paths": "",      
                    "description_length": len(mr['description']) if mr['description'] else 0
                })
            
            has_next_mr = mrs['pageInfo']['hasNextPage']
            cursor = mrs['pageInfo']['endCursor']
            time.sleep(0.5)

        if repo_data_chunk:
            save_chunk(repo_data_chunk)

def main():
    processed = get_processed_repos()
    print(f"Registros anteriores detectados: {len(processed)}")
    for target in TARGETS:
        try:
            if target['type'] == 'github': process_github(target, processed)
            elif target['type'] == 'gitlab': process_gitlab(target, processed)
        except Exception as e:
            print(f"Erro fatal em {target}: {e}")

if __name__ == "__main__":
    main()