import json
import logging
import os
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import BoundedSemaphore

from app.banking import (
    BankingError,
    create_account,
    deposit,
    get_balance,
    withdraw,
)
from app.database import initialize_database
from app.logger_config import configure_logging

HOST = os.environ.get("BANK_HOST", "0.0.0.0")
PORT = int(os.environ.get("BANK_PORT", "8000"))
MAX_CONCURRENT = max(1, int(os.environ.get("BANK_MAX_CONCURRENT", "10")))
REQUEST_SLOTS = BoundedSemaphore(MAX_CONCURRENT)

logger = logging.getLogger("bank.server")


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
                response = {"status": "ok"}
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

    def do_POST(self) -> None:
        started = time.perf_counter()
        request_id = self.headers.get("X-Request-ID") or str(uuid.uuid4())
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
                if account_id is None:
                    raise BankingError(
                        "MISSING_FIELDS",
                        "account_id is required.",
                    )

                initial_balance = data.get("initial_balance", 0)
                response = create_account(account_id, initial_balance)
                status = 201

            elif action in ("deposit", "withdraw"):
                missing = []
                if not account_id:
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

                operation = deposit if action == "deposit" else withdraw
                response = operation(
                    body_request_id,
                    account_id,
                    data.get("amount"),
                )

            else:
                raise BankingError(
                    "UNSUPPORTED_ACTION",
                    "Supported actions: create_account, deposit, withdraw.",
                )

        except BankingError as exc:
            status = 404 if exc.code == "ACCOUNT_NOT_FOUND" else 400
            response = {"error": exc.code, "message": str(exc)}

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


def main() -> None:
    configure_logging()
    initialize_database()

    server = LimitedThreadingHTTPServer((HOST, PORT), BankingHandler)
    logger.info("Banking server listening on %s:%s", HOST, PORT)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
