import logging
import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class GraphQLClient:
    """HTTP client for GraphQL endpoints with retry and rate-limit handling.

    Rate-limit wait strategy (preserved from original):
        1. Read retry-after header (seconds).
        2. Else read x-ratelimit-reset / ratelimit-reset (Unix timestamp).
        3. Else fall back to 60 s.
        Add 10 s buffer; cap at 3600 s.
    """

    def __init__(self, base_url: str, token: str, platform: str = "") -> None:
        self.base_url = base_url
        self.platform = platform
        self.headers = {"Authorization": f"Bearer {token}"}
        self.session = self._create_session()
        self._request_count = 0

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        return session

    def _wait_for_rate_limit(self, response: requests.Response) -> None:
        reset_ts = response.headers.get("x-ratelimit-reset") or response.headers.get(
            "ratelimit-reset"
        )
        retry_after = response.headers.get("retry-after")

        wait_seconds = 60  # fallback

        if retry_after:
            try:
                wait_seconds = int(retry_after)
            except ValueError:
                pass
        elif reset_ts:
            try:
                wait_seconds = max(int(reset_ts) - int(time.time()), 0)
            except ValueError:
                pass

        wait_seconds = min(wait_seconds + 10, 3600)
        logger.info("Rate limit — waiting %ds", wait_seconds)
        time.sleep(wait_seconds)

    def execute(
        self,
        query: str,
        variables: Dict[str, Any],
        context: str = "",
        max_retries: int = 3,
    ) -> Optional[Dict]:
        """Execute a GraphQL query. Returns None on permanent failure."""
        self._request_count += 1

        for attempt in range(max_retries + 1):
            try:
                response = self.session.post(
                    self.base_url,
                    json={"query": query, "variables": variables},
                    headers=self.headers,
                    timeout=60,
                )

                if response.status_code in (403, 429):
                    logger.warning(
                        "Rate limit HTTP %d (%s)", response.status_code, context
                    )
                    self._wait_for_rate_limit(response)
                    continue

                response.raise_for_status()
                data = response.json()

                if "errors" in data:
                    error_msg = data["errors"][0].get("message", "")
                    if "rate limit" in error_msg.lower():
                        logger.warning("Rate limit GraphQL (%s)", context)
                        self._wait_for_rate_limit(response)
                        continue

                    if attempt == max_retries:
                        logger.error("GraphQL error (%s): %s", context, error_msg)
                        return None
                    time.sleep(2**attempt)
                    continue

                return data

            except requests.exceptions.Timeout:
                logger.warning(
                    "Timeout (%s), attempt %d/%d", context, attempt + 1, max_retries + 1
                )
                if attempt == max_retries:
                    logger.error("Timeout after all retries (%s)", context)
                    return None
                time.sleep(5)

            except requests.exceptions.RequestException as e:
                logger.warning("Network error (%s): %s", context, e)
                if attempt == max_retries:
                    logger.error("Permanent network error (%s): %s", context, e)
                    return None
                time.sleep(5)

        return None
