GET_GROUP_MEMBERS = """
query ($groupPath: ID!, $cursor: String) {
  group(fullPath: $groupPath) {
    groupMembers(first: 100, after: $cursor) {
      pageInfo { endCursor hasNextPage }
      nodes {
        user { username name publicEmail }
      }
    }
  }
}
"""

GET_PROJECTS = """
query ($groupPath: ID!, $cursor: String) {
  group(fullPath: $groupPath) {
    projects(includeSubgroups: true, first: 50, after: $cursor) {
      pageInfo { endCursor hasNextPage }
      nodes {
        name
        fullPath
        archived
        repository {
          rootRef # Branch principal
        }
      }
    }
  }
}
"""

GET_COMMITS = """
query ($fullPath: ID!, $branch: String!, $cursor: String) {
  project(fullPath: $fullPath) {
    repository {
      tree(ref: $branch) {
        lastCommit {
          history(first: 50, after: $cursor) {
            pageInfo { endCursor hasNextPage }
            nodes {
              authorName
              authorEmail
            }
          }
        }
      }
    }
  }
}
"""

GET_MERGE_REQUESTS = """
query ($fullPath: ID!, $cursor: String) {
  project(fullPath: $fullPath) {
    mergeRequests(first: 50, after: $cursor, state: merged) {
      pageInfo { endCursor hasNextPage }
      nodes {
        iid
        title
        state
        createdAt
        mergedAt
        author { username }
        approvedBy(first: 10) {
          nodes { username }
        }
        discussions(first: 10) {
          nodes {
            notes(first: 1) {
              nodes { author { username } }
            }
          }
        }
      }
    }
  }
}
"""