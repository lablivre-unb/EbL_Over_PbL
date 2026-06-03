import os

from dotenv import load_dotenv

load_dotenv()

# --- Tokens ---
GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "")
GITLAB_TOKEN: str = os.getenv("GITLAB_TOKEN", "")

# --- Output paths ---
OUTPUT_FILE: str = "journal/data/bronze/prs.csv"
ERROR_LOG_FILE: str = "journal/data/bronze/extraction_errors.log"

# --- GraphQL pagination limits ---
# Intentionally kept small to reduce query cost at scale (~400 K PRs).
# The resulting averages (e.g. avg_commit_message_length) are over sampled
# nodes only, not the full population. See extract.py for original rationale.
MAX_COMMITS_PER_QUERY: int = 10
MAX_REVIEWS_PER_QUERY: int = 15
MAX_COMMENTS_PER_QUERY: int = 15
MAX_FILES_PER_QUERY: int = 20

# --- Persistence ---
SAVE_BATCH_SIZE: int = 50
