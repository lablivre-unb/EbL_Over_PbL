import requests
import time
import json
from typing import Dict, List, Set
import config
from queries import gitlab_queries as queries

def run_gitlab_query(query: str, variables: Dict) -> Dict:
    response = requests.post(
        config.GITLAB_API_URL,
        json={"query": query, "variables": variables},
        headers=config.get_headers('gitlab'),
    )
    if response.status_code == 200:
        return response.json()
    return None

def extract_members(group_path: str) -> List[Dict]:
    print(f"Fetching members from GitLab Group: {group_path}...")
    members = []
    cursor = None
    has_next = True
    
    while has_next:
        data = run_gitlab_query(queries.GET_GROUP_MEMBERS, {"groupPath": group_path, "cursor": cursor})
        if not data or 'errors' in data: break
        
        try:
            raw = data['data']['group']['groupMembers']
            for m in raw['nodes']:
                if m['user']:
                    members.append({
                        "login": m['user']['username'],
                        "name": m['user']['name'],
                        "email": m['user']['publicEmail']
                    })
            has_next = raw['pageInfo']['hasNextPage']
            cursor = raw['pageInfo']['endCursor']
        except (KeyError, TypeError):
            break
            
    return members

def extract_mrs(project_path: str) -> List[Dict]:
    mrs_data = []
    cursor = None
    has_next = True
    current_page = 0
    
    while has_next and current_page < config.MAX_PAGES:
        data = run_gitlab_query(queries.GET_MERGE_REQUESTS, {"fullPath": project_path, "cursor": cursor})
        if not data: break
        
        try:
            raw_mrs = data['data']['project']['mergeRequests']
            for mr in raw_mrs['nodes']:
                # Autores de comentários (Discussion notes)
                commenters = set()
                if mr['discussions']['nodes']:
                    for disc in mr['discussions']['nodes']:
                        if disc['notes']['nodes']:
                            note_author = disc['notes']['nodes'][0]['author']
                            if note_author: commenters.add(note_author['username'])

                # Aprovadores
                approvers = set()
                if mr['approvedBy']['nodes']:
                    for u in mr['approvedBy']['nodes']:
                        approvers.add(u['username'])
                
                mrs_data.append({
                    "number": mr['iid'],
                    "title": mr['title'],
                    "author": mr['author']['username'] if mr['author'] else "unknown",
                    "reviewers": list(approvers),
                    "commenters": list(commenters)
                })
            
            has_next = raw_mrs['pageInfo']['hasNextPage']
            cursor = raw_mrs['pageInfo']['endCursor']
            current_page += 1
        except (KeyError, TypeError):
            has_next = False
            
    return mrs_data

def process_gitlab_group(group_path: str) -> Dict:
    members = extract_members(group_path)
    
    print(f"Fetching projects from {group_path}...")
    projects_list = []
    cursor = None
    has_next = True
    
    while has_next:
        data = run_gitlab_query(queries.GET_PROJECTS, {"groupPath": group_path, "cursor": cursor})
        if not data: break
        
        raw_projects = data['data']['group']['projects']
        
        for p in raw_projects['nodes']:
            p_name = p['name']
            p_path = p['fullPath'] # Necessário para queries subsequentes
            
            if p['archived']: continue
            print(f"   -> Processing GitLab Project: {p_name}")
            
            mrs = extract_mrs(p_path)
            # Voce pode adicionar extract_commits e issues aqui seguindo a mesma logica
            
            projects_list.append({
                "name": p_name,
                "full_path": p_path,
                "merge_requests": mrs
            })
            time.sleep(config.RATE_LIMIT_DELAY)
            
        has_next = raw_projects['pageInfo']['hasNextPage']
        cursor = raw_projects['pageInfo']['endCursor']

    return {
        "platform": "gitlab",
        "group": group_path,
        "members": members,
        "repositories": projects_list
    }