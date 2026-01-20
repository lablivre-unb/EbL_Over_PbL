import os
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_API_URL = "https://api.github.com/graphql"
GITHUB_ORG = "GovHub-br"

GITLAB_TOKEN = os.getenv("GITLAB_TOKEN", "")
GITLAB_API_URL = "https://gitlab.com/api/graphql"
GITLAB_ORG = "lappis-unb"

MAX_COMMIT_PAGES = 4
MAX_PR_PAGES = 5
MAX_ISSUE_PAGES = 5
RATE_LIMIT_DELAY = 0.5
MAX_PAGES = 5
DAYS_LOOKBACK = 365  # Janela temporal de contribuições (últimos 365 dias)


def get_headers(plataform: str) -> dict:
    if plataform == "github":
        return {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Content-Type": "application/json",
        }
    elif plataform == "gitlab":
        return {
            "Authorization": f"Bearer {GITLAB_TOKEN}",
            "Content-Type": "application/json",
        }
    return {}


def validate():
    if not GITHUB_TOKEN:
        raise ValueError("GITHUB_TOKEN not configured. Set it in .env file.")
