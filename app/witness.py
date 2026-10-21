"""Fail-closed client for the third-party banking lease witness."""
import json
import os
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TOKEN_FILE = PROJECT_ROOT / ".witness-token"


def _post(witness_url, path, payload, timeout=1.5):
    token_file = Path(os.environ.get(
        "BANK_WITNESS_TOKEN_FILE", str(DEFAULT_TOKEN_FILE)
    ))
    token = os.environ.get("BANK_WITNESS_TOKEN")
    if not token:
        try:
            token = token_file.read_text(encoding="utf-8").strip()
        except OSError:
            return None, None

    if not token:
        return None, None

    request = Request(
        witness_url.rstrip("/") + path,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Witness-Token": token,
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
            return response.status, data
    except HTTPError as exc:
        try:
            data = json.loads(exc.read().decode("utf-8"))
        except Exception:
            data = {}
        return exc.code, data
    except (URLError, TimeoutError, OSError, json.JSONDecodeError):
        return None, None


def claim_lease(witness_url, node_id, timeout=1.5):
    """Claim or renew a lease. None means the witness could not be trusted."""
    status, data = _post(
        witness_url, "/lease/claim", {"node_id": node_id}, timeout
    )
    if status in (200, 409) and isinstance(data, dict):
        return data
    return None


def validate_lease(
    witness_url, node_id, epoch, min_remaining=2.0, timeout=1.5
):
    """Confirm this exact node and epoch still have a usable lease."""
    if epoch is None:
        return False

    status, data = _post(
        witness_url,
        "/lease/validate",
        {
            "node_id": node_id,
            "epoch": int(epoch),
            "min_remaining": float(min_remaining),
        },
        timeout,
    )
    return (
        status == 200
        and isinstance(data, dict)
        and data.get("valid") is True
    )
