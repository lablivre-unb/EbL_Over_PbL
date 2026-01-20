GET_MEMBERS = """
query ($org: String!, $cursor: String) {
  organization(login: $org) {
    membersWithRole(first: 100, after: $cursor) {
      pageInfo { endCursor hasNextPage }
      nodes { login name email }
    }
  }
}
"""

GET_REPOS = """
query ($org: String!, $cursor: String) {
  organization(login: $org) {
    repositories(first: 50, after: $cursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { endCursor hasNextPage }
      nodes {
        name
        isArchived
        languages(first: 5, orderBy: {field: SIZE, direction: DESC}) {
          nodes { name }
        }
        defaultBranchRef { name } 
      }
    }
  }
}
"""

GET_COMMITS = """
query ($owner: String!, $name: String!, $branch: String!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    ref(qualifiedName: $branch) {
      target {
        ... on Commit {
          history(first: 100, after: $cursor) {
            pageInfo { endCursor hasNextPage }
            nodes {
              author {
                user { login }
                name
                email
              }
            }
          }
        }
      }
    }
  }
}
"""

GET_PRS = """
query ($owner: String!, $name: String!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(first: 50, after: $cursor, states: [MERGED, CLOSED]) {
      pageInfo { endCursor hasNextPage }
      nodes {
        number
        title
        state
        createdAt
        author { login }
        mergedBy { login }
        reviews(first: 10, states: APPROVED) {
          nodes { author { login } }
        }
        comments(first: 10) {
          nodes { author { login } }
        }
      }
    }
  }
}
"""

GET_ISSUES = """
query ($owner: String!, $name: String!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    issues(first: 50, after: $cursor, orderBy: {field: CREATED_AT, direction: DESC}) {
      pageInfo { endCursor hasNextPage }
      nodes {
        number
        title
        state
        createdAt
        closedAt
        
        author { login }
        
        assignees(first: 10) {
          nodes { login }
        }
        
        comments(first: 10) {
          nodes { author { login } }
        }
      }
    }
  }
}
"""

