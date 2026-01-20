import json
import os
import glob
from itertools import combinations

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '../pipeline/data')
OUTPUT_FILE = os.path.join(BASE_DIR, '../pipeline/data/graph_interactions.json')

BOTS = {'sonarqubecloud', 'github-actions', 'dependabot', 'renovate', 'dependabot[bot]', 'gitlab-bot', 'actions-user'}

def clean_username(username):
    if not username:
        return None
    clean = str(username).replace('email::', '').split('@')[0].strip()
    if clean.lower() in BOTS or 'bot' in clean.lower():
        return None
    return clean

def run_pipeline():
    json_files = glob.glob(os.path.join(DATA_DIR, '*.json'))
    
    if not json_files:
        print(f"NENHUM ARQUIVO EM: {DATA_DIR}")
        return

    print(f"Processando interações e repositórios compartilhados em {len(json_files)} arquivos...")

    nodes_map = {} 
    links_map = {} 

    def touch_node(uid, org):
        """Garante que o nó (usuário) existe"""
        if uid not in nodes_map:
            nodes_map[uid] = {
                'id': uid, 
                'group': 'user', 
                'val': 1,
                'img': f"https://github.com/{uid}.png",
                'sources': set()
            }
        nodes_map[uid]['sources'].add(org)
    
    def get_or_create_link(u1, u2):
        """Garante que o link existe e retorna a referência para ele"""
        if u1 == u2:
            return None
        
        p1, p2 = sorted((u1, u2))
        key = f"{p1}|{p2}"
        
        if key not in links_map:
            links_map[key] = {
                'source': p1,
                'target': p2,
                'value': 0,
                'shared_repos': set(), 
                'interactions': [] 
            }
        return links_map[key]

    def add_interaction(actor, target, action_type, repo_name):
        """Registra uma ação específica (Merge, Close, etc)"""
        link = get_or_create_link(actor, target)
        if link:
            link['value'] += 2 
            link['interactions'].append({
                'actor': actor,
                'target': target,
                'type': action_type,
                'repo': repo_name
            })
            
            link['shared_repos'].add(repo_name)

    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            org = data.get('organization', 'Unknown')
            
            for repo in data.get('repositories', []):
                repo_name = repo['name']
                
                repo_participants = set()

                
                for c in repo.get('contributors', []):
                    if u := clean_username(c):
                        repo_participants.add(u)
                
                for pr in repo.get('pull_requests', []) + repo.get('merge_requests', []):
                    if u := clean_username(pr.get('author')):
                        repo_participants.add(u)
                    if u := clean_username(pr.get('merged_by')):
                        repo_participants.add(u)
                    for r in pr.get('reviewers', []):
                        if u := clean_username(r):
                            repo_participants.add(u)
                
                for iss in repo.get('issues', []):
                    if u := clean_username(iss.get('author')):
                        repo_participants.add(u)
                    if u := clean_username(iss.get('closed_by')):
                        repo_participants.add(u)

                for p in repo_participants:
                    touch_node(p, org)
                    nodes_map[p]['val'] += 0.2 

                users_list = sorted(list(repo_participants))
                if len(users_list) >= 2:
                    for p1, p2 in combinations(users_list, 2):
                        link = get_or_create_link(p1, p2)
                        if link:
                            link['value'] += 0.5
                            link['shared_repos'].add(repo_name)

                # PRs / MRs
                for pr in repo.get('pull_requests', []) + repo.get('merge_requests', []):
                    author = clean_username(pr.get('author'))
                    merger = clean_username(pr.get('merged_by'))
                    
                    if author and merger:
                        add_interaction(merger, author, 'MERGED_PR', repo_name)

                    for rev in pr.get('reviewers', []):
                        reviewer = clean_username(rev)
                        if reviewer and author:
                            add_interaction(reviewer, author, 'REVIEWED_PR', repo_name)

                # Issues
                for issue in repo.get('issues', []):
                    author = clean_username(issue.get('author'))
                    closer = clean_username(issue.get('closed_by'))
                    
                    if author and closer:
                        add_interaction(closer, author, 'CLOSED_ISSUE', repo_name)

        except Exception as e:
            print(f"Erro em {file_path}: {e}")

    final_nodes = []
    for uid, data in nodes_map.items():
        data['sources'] = list(data['sources'])
        final_nodes.append(data)

    final_links = []
    for data in links_map.values():
        data['shared_repos'] = list(data['shared_repos'])
        final_links.append(data)

    output = {"nodes": final_nodes, "links": final_links}

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

    print(f"\nSucesso, arquivo gerado em: {OUTPUT_FILE}")
    print(f"   - Pessoas: {len(final_nodes)}")
    print(f"   - Conexões: {len(final_links)}")

if __name__ == "__main__":
    run_pipeline()