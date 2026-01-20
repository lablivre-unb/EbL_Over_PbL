import requests
import time
import json
from typing import Dict, List, Set

import config
from queries import github_queries as queries


def run_query(query: str, variables: Dict) -> Dict:
    response = requests.post(
        config.GITHUB_API_URL,
        json={"query": query, "variables": variables},
        headers=config.get_headers("github"),
    )

    if response.status_code == 200:
        data = response.json()
        if "errors" in data:
            print(f"GraphQL Error: {data['errors'][0]['message']}")
            return None
        return data
    else:
        raise Exception(
            f"Query failed with status {response.status_code}: {response.text}"
        )


def extract_members(org_name: str) -> List[Dict]:
    print(f"Fetching members from {org_name}...")
    members = []
    cursor = None
    has_next = True

    while has_next:
        data = run_query(queries.GET_MEMBERS, {"org": org_name, "cursor": cursor})
        if not data:
            break

        raw_members = data["data"]["organization"]["membersWithRole"]

        for m in raw_members["nodes"]:
            if m:
                members.append(
                    {"login": m["login"], "name": m["name"], "email": m["email"]}
                )

        has_next = raw_members["pageInfo"]["hasNextPage"]
        cursor = raw_members["pageInfo"]["endCursor"]

    return members


def extract_contributors(org: str, repo_name: str, default_branch: str) -> List[str]:
    if not default_branch:
        return []

    contributors: Set[str] = set()
    cursor = None
    has_next = True
    current_page = 0

    while has_next and current_page < config.MAX_COMMIT_PAGES:
        variables = {
            "owner": org,
            "name": repo_name,
            "branch": default_branch,
            "cursor": cursor,
        }
        data = run_query(queries.GET_COMMITS, variables)

        try:
            history = data["data"]["repository"]["ref"]["target"]["history"]
            for commit in history["nodes"]:
                if commit["author"]["user"] and commit["author"]["user"]["login"]:
                    contributors.add(commit["author"]["user"]["login"])
                else:
                    contributors.add(f"email::{commit['author']['email']}")

            has_next = history["pageInfo"]["hasNextPage"]
            cursor = history["pageInfo"]["endCursor"]
            current_page += 1
        except (KeyError, TypeError):
            has_next = False

    return list(contributors)


def extract_pull_requests(org: str, repo_name: str) -> List[Dict]:
    prs_data = []
    cursor = None
    has_next = True
    current_page = 0

    while has_next and current_page < config.MAX_PR_PAGES:
        data = run_query(
            queries.GET_PRS, {"owner": org, "name": repo_name, "cursor": cursor}
        )
        if not data:
            break

        try:
            raw_prs = data["data"]["repository"]["pullRequests"]

            for pr in raw_prs["nodes"]:
                commenters = set()
                if pr["comments"]["nodes"]:
                    for comment in pr["comments"]["nodes"]:
                        if comment["author"]:
                            commenters.add(comment["author"]["login"])

                reviewers = set()
                if pr["reviews"]["nodes"]:
                    for review in pr["reviews"]["nodes"]:
                        if review["author"]:
                            reviewers.add(review["author"]["login"])

                prs_data.append(
                    {
                        "number": pr["number"],
                        "title": pr["title"],
                        "author": pr["author"]["login"] if pr["author"] else "unknown",
                        "merged_by": pr["mergedBy"]["login"]
                        if pr["mergedBy"]
                        else None,
                        "reviewers": list(reviewers),
                        "commenters": list(commenters),
                    }
                )

            has_next = raw_prs["pageInfo"]["hasNextPage"]
            cursor = raw_prs["pageInfo"]["endCursor"]
            current_page += 1
        except KeyError:
            has_next = False

    return prs_data


def extract_issues(org: str, repo_name: str) -> List[Dict]:
    issues_data = []
    cursor = None
    has_next = True
    current_page = 0

    while has_next and current_page < config.MAX_ISSUE_PAGES:
        data = run_query(
            queries.GET_ISSUES, {"owner": org, "name": repo_name, "cursor": cursor}
        )
        if not data:
            break

        try:
            raw_issues = data["data"]["repository"]["issues"]

            for issue in raw_issues["nodes"]:
                commenters = set()
                if issue["comments"]["nodes"]:
                    for comment in issue["comments"]["nodes"]:
                        if comment["author"]:
                            commenters.add(comment["author"]["login"])

                assignees = set()
                if issue["assignees"]["nodes"]:
                    for assignee in issue["assignees"]["nodes"]:
                        assignees.add(assignee["login"])

                issues_data.append(
                    {
                        "number": issue["number"],
                        "title": issue["title"],
                        "state": issue["state"],
                        "created_at": issue["createdAt"],
                        "closed_at": issue["closedAt"],
                        "author": issue["author"]["login"]
                        if issue["author"]
                        else "unknown",
                        "assignees": list(assignees),
                        "commenters": list(commenters),
                    }
                )

            has_next = raw_issues["pageInfo"]["hasNextPage"]
            cursor = raw_issues["pageInfo"]["endCursor"]
            current_page += 1
        except KeyError:
            has_next = False

    return issues_data


def process_organization(org_name: str) -> Dict:
    members = extract_members(org_name)

    print(f"Fetching repositories from {org_name}...")
    repositories = []
    cursor = None
    has_next = True

    while has_next:
        data = run_query(queries.GET_REPOS, {"org": org_name, "cursor": cursor})
        if not data:
            break

        raw_repos = data["data"]["organization"]["repositories"]

        for r in raw_repos["nodes"]:
            repo_name = r["name"]
            if r["isArchived"]:
                continue

            print(f"   -> Processing repository: {repo_name}")

            default_branch = (
                r["defaultBranchRef"]["name"] if r["defaultBranchRef"] else None
            )
            langs = [l["name"] for l in r["languages"]["nodes"]]

            contributors = extract_contributors(org_name, repo_name, default_branch)
            prs = extract_pull_requests(org_name, repo_name)
            issues = extract_issues(org_name, repo_name)

            repositories.append(
                {
                    "name": repo_name,
                    "languages": langs,
                    "contributors": contributors,
                    "pull_requests": prs,
                    "issues": issues,
                }
            )

            time.sleep(config.RATE_LIMIT_DELAY)

        has_next = raw_repos["pageInfo"]["hasNextPage"]
        cursor = raw_repos["pageInfo"]["endCursor"]

    return {
        "organization": org_name,
        "extracted_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "members": members,
        "repositories": repositories,
    }


if __name__ == "__main__":
    try:
        config.validate()

        result = process_organization(config.TARGET_ORG)

        filename = f"{config.TARGET_ORG}_data.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        print(f"\nExtraction completed! Data saved to: {filename}")

    except ValueError as e:
        print(f"\nConfiguration Error: {e}")
        exit(1)
    except Exception as e:
        print(f"\nFatal Error: {e}")
        exit(1)
