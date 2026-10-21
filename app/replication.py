"""Helpers for authenticated communication with the standby banking server."""

import hmac
import json
import os
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TOKEN_FILE = PROJECT_ROOT / ".replication-token"


def get_replication_token() -> str:
    """Read the shared replication token without exposing it in logs."""
    token = os.environ.get("BANK_REPLICATION_TOKEN")

    if not token and TOKEN_FILE.is_file():
        token = TOKEN_FILE.read_text(encoding="utf-8").strip()

    if not token:
        raise RuntimeError("Replication token is not configured.")

    return token


def token_matches(candidate: str) -> bool:
    """Constant-time comparison for the internal replication token."""
    if not isinstance(candidate, str):
        return False

    try:
        expected = get_replication_token()
    except (OSError, RuntimeError):
        return False

    return hmac.compare_digest(candidate, expected)


def post_replication_event(
    standby_url: str,
    event: dict,
    timeout: float = 3.0,
) -> dict:
    """Send one event to the standby and require a valid JSON response."""
    token = get_replication_token()
    body = json.dumps(event).encode("utf-8")
    request = Request(
        standby_url.rstrip("/") + "/internal/replicate",
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-Replication-Token": token,
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            result = json.loads(response.read().decode("utf-8"))
    except (URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("Standby replication request failed.") from exc

    if not isinstance(result, dict) or result.get("status") != "ok":
        raise RuntimeError("Standby did not acknowledge replication.")

    return result
