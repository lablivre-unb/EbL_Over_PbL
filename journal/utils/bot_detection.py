from typing import Optional

BOT_KEYWORDS: frozenset = frozenset(
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


def is_bot_user(username: Optional[str]) -> bool:
    """Returns True if username is absent or matches any bot keyword (substring match, case-insensitive)."""
    if not username:
        return True
    username_lower = username.lower()
    return any(kw in username_lower for kw in BOT_KEYWORDS)
