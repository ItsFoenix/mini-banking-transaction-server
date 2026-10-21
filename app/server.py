import json
import logging
import os
import time
import uuid
from urllib.request import urlopen
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import BoundedSemaphore, Lock, Thread

from app.banking import (
    BankingError,
    create_account,
    deposit,
    get_balance,
    withdraw,
)
from app.database import initialize_database
from app.logger_config import configure_logging
from app.replication import post_replication_event, token_matches
from app.witness import claim_lease, validate_lease
from app.replication_store import (
    apply_replication_event,
    initialize_replication_store,
)

HOST = os.environ.get("BANK_HOST", "0.0.0.0")
PORT = int(os.environ.get("BANK_PORT", "8000"))
MAX_CONCURRENT = max(1, int(os.environ.get("BANK_MAX_CONCURRENT", "10")))
REQUEST_SLOTS = BoundedSemaphore(MAX_CONCURRENT)
MUTATION_LOCK = Lock()
ROLE_LOCK = Lock()
SERVER_ROLE = os.environ.get("BANK_ROLE", "primary").strip().lower()
STANDBY_URL = os.environ.get(
    "BANK_STANDBY_URL", "http://192.168.11.130:8000"
)
PRIMARY_URL = os.environ.get(
    "BANK_PRIMARY_URL", "http://192.168.11.128:8000"
)
NODE_ID = os.environ.get(
    "BANK_NODE_ID", "primary" if SERVER_ROLE == "primary" else "backup"
).strip().lower()
WITNESS_URL = os.environ.get(
    "BANK_WITNESS_URL", "http://192.168.11.129:8765"
).rstrip("/")
WITNESS_MIN_REMAINING = float(
    os.environ.get("BANK_WITNESS_MIN_REMAINING", "2")
)
LEASE_EPOCH = None

logger = logging.getLogger("bank.server")


def lease_is_valid() -> bool:
    """Authorize a write only while this node's current lease is valid."""
    global SERVER_ROLE
    with ROLE_LOCK:
        role = SERVER_ROLE
        epoch = LEASE_EPOCH

    if role != "primary" or epoch is None:
        return False

    valid = validate_lease(
        WITNESS_URL, NODE_ID, epoch,
        min_remaining=WITNESS_MIN_REMAINING,
    )
    if not valid:
        with ROLE_LOCK:
            if SERVER_ROLE == "primary":
                SERVER_ROLE = "fenced"
        logger.error(
            "Witness lease validation failed; fencing node %s", NODE_ID
        )
    return valid


def witness_lease_manager() -> None:
    """Renew leadership leases; fail closed when ownership cannot be proved."""
    global SERVER_ROLE, LEASE_EPOCH
    while True:
        with ROLE_LOCK:
            role = SERVER_ROLE

        if role in ("primary", "fenced"):
            grant = claim_lease(WITNESS_URL, NODE_ID)
            if grant and grant.get("granted"):
                try:
                    new_epoch = int(grant["epoch"])
                except (KeyError, TypeError, ValueError):
                    new_epoch = None

                if new_epoch is not None:
                    with ROLE_LOCK:
                        if SERVER_ROLE in ("primary", "fenced"):
                            LEASE_EPOCH = new_epoch
                            if SERVER_ROLE == "fenced":
                                SERVER_ROLE = "primary"
                                logger.warning(
                                    "Witness lease acquired; node %s is active "
                                    "at epoch %s", NODE_ID, LEASE_EPOCH
                                )
            elif role == "primary":
                with ROLE_LOCK:
                    if SERVER_ROLE == "primary":
                        SERVER_ROLE = "fenced"
                logger.error(
                    "Witness lease could not be renewed; writes are disabled "
                    "on node %s", NODE_ID
                )

        time.sleep(2)



class LimitedThreadingHTTPServer(ThreadingHTTPServer):
    def process_request(self, request, client_address):
        REQUEST_SLOTS.acquire()
        try:
            super().process_request(request, client_address)
        except Exception:
            REQUEST_SLOTS.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            REQUEST_SLOTS.release()


class BankingHandler(BaseHTTPRequestHandler):
    server_version = "MiniBank/1.0"

    def send_json(self, status_code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        started = time.perf_counter()
        request_id = self.headers.get("X-Request-ID") or str(uuid.uuid4())
        action = "health_check" if self.path == "/health" else "balance_inquiry"
        account_id = "-"
        result = "success"

        try:
            if self.path == "/health":
                status = 200
                with ROLE_LOCK:
                    current_role = SERVER_ROLE
                response = {"status": "ok", "role": current_role}
            elif self.path.startswith("/balance"):
                from urllib.parse import parse_qs, urlparse

                query = parse_qs(urlparse(self.path).query)
                account_id = query.get("account_id", [None])[0] or "-"

                if account_id == "-":
                    raise BankingError(
                        "MISSING_FIELDS",
                        "Query parameter account_id is required.",
                    )

                response = get_balance(account_id)
                status = 200
            else:
                status = 404
                response = {
                    "error": "NOT_FOUND",
                    "message": "Endpoint does not exist.",
                }

        except BankingError as exc:
            status = 404 if exc.code == "ACCOUNT_NOT_FOUND" else 400
            response = {"error": exc.code, "message": str(exc)}

        except Exception:
            logger.exception("Unhandled GET request error")
            status = 500
            response = {
                "error": "INTERNAL_ERROR",
                "message": "An internal server error occurred.",
            }

        if status >= 400:
            result = response.get("error", "error")
        self.send_json(status, response)
        self.log_bank_request(
            request_id, started, status, action, account_id, result
        )

    def handle_replication(self) -> None:
        with ROLE_LOCK:
            receiver_role = SERVER_ROLE
        if receiver_role != "standby":
            started = time.perf_counter()
            request_id = self.headers.get("X-Request-ID") or str(uuid.uuid4())
            response = {
                "error": "REPLICATION_ROLE_MISMATCH",
                "message": "Replication events are accepted only by a standby.",
                "role": receiver_role,
            }
            self.send_json(503, response)
            self.log_bank_request(
                request_id, started, 503, "replication", "-",
                "REPLICATION_ROLE_MISMATCH"
            )
            return
        started = time.perf_counter()
        request_id = self.headers.get("X-Request-ID") or str(uuid.uuid4())
        status = 200
        action = "replication"
        account_id = "-"
        result = "success"

        try:
            candidate = self.headers.get("X-Replication-Token", "")
            if not token_matches(candidate):
                status = 401
                result = "UNAUTHORIZED"
                response = {
                    "error": "UNAUTHORIZED",
                    "message": "Invalid replication credentials.",
                }
            else:
                content_length = self.headers.get("Content-Length")
                if content_length is None:
                    raise BankingError(
                        "MISSING_FIELDS", "Content-Length header is required."
                    )

                try:
                    length = int(content_length)
                except ValueError as exc:
                    raise BankingError(
                        "INVALID_REQUEST",
                        "Content-Length must be an integer.",
                    ) from exc

                if length <= 0:
                    raise BankingError(
                        "INVALID_REQUEST", "Request body must not be empty."
                    )

                data = json.loads(self.rfile.read(length).decode("utf-8"))
                if not isinstance(data, dict):
                    raise BankingError(
                        "INVALID_REQUEST", "JSON body must be an object."
                    )

                account_id = str(data.get("account_id", "-"))
                event_response = apply_replication_event(data)
                response = {"status": "ok", "event": event_response}

        except (BankingError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            status = 400
            result = getattr(exc, "code", "INVALID_REQUEST")
            response = {
                "error": result,
                "message": str(exc) if isinstance(exc, BankingError)
                else "Invalid JSON request body.",
            }
        except Exception:
            logger.exception("Internal replication request failed")
            status = 500
            result = "REPLICATION_ERROR"
            response = {
                "error": "REPLICATION_ERROR",
                "message": "The replication event could not be applied.",
            }

        self.send_json(status, response)
        self.log_bank_request(
            request_id, started, status, action, account_id, result
        )

    def do_POST(self) -> None:
        if self.path == "/internal/replicate":
            self.handle_replication()
            return

        started = time.perf_counter()
        request_id = self.headers.get("X-Request-ID") or str(uuid.uuid4())

        with ROLE_LOCK:
            current_role = SERVER_ROLE

        if current_role == "primary" and not lease_is_valid():
            with ROLE_LOCK:
                current_role = SERVER_ROLE

        if current_role != "primary":
            response = {
                "error": "STANDBY_SERVER",
                "message": "This server is not currently accepting banking operations.",
                "role": current_role,
            }
            self.send_json(503, response)
            self.log_bank_request(
                request_id, started, 503, "role_check", "-", "STANDBY_SERVER"
            )
            return

        status = 200
        action = "-"
        account_id = "-"
        result = "success"

        try:
            content_length = self.headers.get("Content-Length")
            if content_length is None:
                raise BankingError(
                    "MISSING_FIELDS",
                    "Content-Length header is required.",
                )

            try:
                length = int(content_length)
            except ValueError as exc:
                raise BankingError(
                    "INVALID_REQUEST",
                    "Content-Length must be an integer.",
                ) from exc

            if length <= 0 or length > 1_000_000:
                raise BankingError(
                    "INVALID_REQUEST",
                    "Request body length is invalid.",
                )

            try:
                data = json.loads(self.rfile.read(length))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise BankingError(
                    "MALFORMED_JSON",
                    "Request body must contain valid JSON.",
                ) from exc

            if not isinstance(data, dict):
                raise BankingError(
                    "INVALID_REQUEST",
                    "JSON body must be an object.",
                )

            action = data.get("action") or "-"
            account_id = data.get("account_id") or "-"
            body_request_id = data.get("request_id") or request_id

            if action == "create_account":
                if account_id == "-":
                    raise BankingError(
                        "MISSING_FIELDS",
                        "account_id is required.",
                    )
                if not isinstance(body_request_id, str) or not body_request_id.strip():
                    raise BankingError(
                        "MISSING_FIELDS",
                        "request_id is required.",
                    )

                initial_balance = data.get("initial_balance", 0)
                event = {
                    "event_id": body_request_id,
                    "request_id": body_request_id,
                    "action": action,
                    "account_id": account_id,
                    "initial_balance": initial_balance,
                }

                with MUTATION_LOCK:
                    if STANDBY_URL:
                        post_replication_event(STANDBY_URL, event)
                    response = apply_replication_event(event)
                status = 201

            elif action in ("deposit", "withdraw"):
                missing = []
                if account_id == "-":
                    missing.append("account_id")
                if not data.get("request_id") and not self.headers.get("X-Request-ID"):
                    missing.append("request_id")
                if "amount" not in data:
                    missing.append("amount")

                if missing:
                    raise BankingError(
                        "MISSING_FIELDS",
                        "Required fields are missing: " + ", ".join(missing),
                    )

                event = {
                    "event_id": body_request_id,
                    "request_id": body_request_id,
                    "action": action,
                    "account_id": account_id,
                    "amount": data.get("amount"),
                }

                with MUTATION_LOCK:
                    if STANDBY_URL:
                        post_replication_event(STANDBY_URL, event)
                    response = apply_replication_event(event)

            else:
                raise BankingError(
                    "UNSUPPORTED_ACTION",
                    "Supported actions: create_account, deposit, withdraw.",
                )

        except BankingError as exc:
            status = 404 if exc.code == "ACCOUNT_NOT_FOUND" else 400
            response = {"error": exc.code, "message": str(exc)}

        except RuntimeError:
            logger.exception("Banking mutation could not be replicated")
            status = 503
            response = {
                "error": "REPLICATION_UNAVAILABLE",
                "message": "The standby did not confirm the operation. Retry using the same request ID.",
            }

        except Exception:
            logger.exception("Unhandled POST request error")
            status = 500
            response = {
                "error": "INTERNAL_ERROR",
                "message": "An internal server error occurred.",
            }

        if status >= 400:
            result = response.get("error", "error")
        self.send_json(status, response)
        self.log_bank_request(
            request_id, started, status, str(action),
            str(account_id), result
        )

    def log_bank_request(
        self,
        request_id: str,
        started: float,
        status: int,
        action: str = "-",
        account_id: str = "-",
        result: str = "-",
    ) -> None:
        latency_ms = (time.perf_counter() - started) * 1000
        logger.info(
            "request_id=%s client_ip=%s action=%s account_id=%s "
            "result=%s method=%s path=%s status=%s latency_ms=%.2f",
            request_id,
            self.client_address[0],
            action,
            account_id,
            result,
            self.command,
            self.path,
            status,
            latency_ms,
        )

    def log_message(self, format_string: str, *args) -> None:
        return



def monitor_primary() -> None:
    """Promote only after the witness grants exclusive leadership."""
    global SERVER_ROLE, STANDBY_URL, LEASE_EPOCH
    consecutive_failures = 0

    while True:
        with ROLE_LOCK:
            if SERVER_ROLE != "standby":
                return

        try:
            with urlopen(PRIMARY_URL.rstrip("/") + "/health", timeout=1.0) as response:
                health = json.loads(response.read().decode("utf-8"))
            if (
                response.status == 200
                and health.get("status") == "ok"
                and health.get("role") == "primary"
            ):
                consecutive_failures = 0
            else:
                consecutive_failures += 1
        except Exception:
            consecutive_failures += 1

        if consecutive_failures >= 3:
            grant = claim_lease(WITNESS_URL, NODE_ID)
            if grant and grant.get("granted"):
                try:
                    new_epoch = int(grant["epoch"])
                except (KeyError, TypeError, ValueError):
                    new_epoch = None

                promoted = False
                if new_epoch is not None:
                    with ROLE_LOCK:
                        if SERVER_ROLE == "standby":
                            LEASE_EPOCH = new_epoch
                            STANDBY_URL = ""
                            SERVER_ROLE = "primary"
                            promoted = True

                if promoted:
                    logger.warning(
                        "Primary heartbeat lost; witness lease acquired; "
                        "standby promoted at epoch %s", new_epoch
                    )
                    return
            else:
                logger.warning(
                    "Primary unavailable, but witness lease was not granted; "
                    "node remains standby"
                )
            consecutive_failures = 0

        time.sleep(2)


def main() -> None:
    global SERVER_ROLE, LEASE_EPOCH
    configure_logging()
    initialize_database()
    initialize_replication_store()

    if SERVER_ROLE == "primary":
        initial_lease = claim_lease(WITNESS_URL, NODE_ID)
        if initial_lease and initial_lease.get("granted"):
            LEASE_EPOCH = int(initial_lease["epoch"])
            logger.info(
                "Witness lease acquired at startup: node=%s epoch=%s",
                NODE_ID, LEASE_EPOCH
            )
        else:
            SERVER_ROLE = "fenced"
            logger.error(
                "Could not obtain a witness lease at startup; writes disabled"
            )

    server = LimitedThreadingHTTPServer((HOST, PORT), BankingHandler)
    logger.info(
        "Banking server starting with role=%s on %s:%s",
        SERVER_ROLE, HOST, PORT
    )

    Thread(target=witness_lease_manager, daemon=True).start()
    if SERVER_ROLE == "standby":
        Thread(target=monitor_primary, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
