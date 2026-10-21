#!/usr/bin/env python3
import hmac
import json
import logging
import os
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HOST = "0.0.0.0"
PORT = int(os.environ.get("WITNESS_PORT", "8765"))
LEASE_SECONDS = float(os.environ.get("WITNESS_LEASE_SECONDS", "10"))
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "witness.db"
TOKEN_PATH = BASE_DIR / ".witness-token"
VALID_NODES = {"primary", "backup"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s witness %(message)s",
)
logger = logging.getLogger("witness")


def connect_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=5)
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def initialize_db():
    conn = connect_db()
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lease_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                holder TEXT,
                epoch INTEGER NOT NULL,
                lease_until REAL NOT NULL,
                updated_at REAL NOT NULL
            )
        """)
        conn.execute("""
            INSERT OR IGNORE INTO lease_state
                (singleton, holder, epoch, lease_until, updated_at)
            VALUES (1, NULL, 0, 0, ?)
        """, (time.time(),))
        conn.commit()
    finally:
        conn.close()


def token_matches(candidate):
    try:
        expected = TOKEN_PATH.read_text(encoding="utf-8").strip()
    except OSError:
        return False
    return bool(expected) and isinstance(candidate, str) and hmac.compare_digest(
        candidate, expected
    )


class WitnessHandler(BaseHTTPRequestHandler):
    server_version = "BankWitness/1.0"

    def send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def read_json(self):
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise ValueError("Content-Length is required")
        length = int(raw_length)
        if length <= 0 or length > 65536:
            raise ValueError("Invalid request body length")
        value = json.loads(self.rfile.read(length).decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("JSON body must be an object")
        return value

    def authenticated(self):
        return token_matches(self.headers.get("X-Witness-Token", ""))

    def do_GET(self):
        if self.path == "/health":
            self.send_json(200, {"status": "ok", "service": "witness"})
            return

        if self.path == "/lease/status":
            if not self.authenticated():
                self.send_json(401, {"error": "UNAUTHORIZED"})
                return

            conn = connect_db()
            try:
                row = conn.execute(
                    "SELECT holder, epoch, lease_until FROM lease_state "
                    "WHERE singleton=1"
                ).fetchone()
            finally:
                conn.close()

            now = time.time()
            self.send_json(200, {
                "holder": row[0],
                "epoch": row[1],
                "lease_remaining": max(0.0, row[2] - now),
            })
            return

        self.send_json(404, {"error": "NOT_FOUND"})

    def do_POST(self):
        if not self.authenticated():
            self.send_json(401, {"error": "UNAUTHORIZED"})
            return

        try:
            data = self.read_json()
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            self.send_json(400, {"error": "INVALID_REQUEST", "message": str(exc)})
            return

        if self.path == "/lease/claim":
            self.claim(data)
        elif self.path == "/lease/validate":
            self.validate(data)
        else:
            self.send_json(404, {"error": "NOT_FOUND"})

    def claim(self, data):
        node = data.get("node_id")
        if node not in VALID_NODES:
            self.send_json(400, {"error": "INVALID_NODE"})
            return

        now = time.time()
        conn = connect_db()
        try:
            conn.execute("BEGIN IMMEDIATE")
            holder, epoch, lease_until = conn.execute(
                "SELECT holder, epoch, lease_until FROM lease_state "
                "WHERE singleton=1"
            ).fetchone()

            if holder is None or lease_until <= now:
                epoch += 1
                holder = node
                lease_until = now + LEASE_SECONDS
                conn.execute(
                    "UPDATE lease_state SET holder=?, epoch=?, lease_until=?, "
                    "updated_at=? WHERE singleton=1",
                    (holder, epoch, lease_until, now),
                )
                conn.commit()
                logger.warning("Lease granted to %s at epoch %s", node, epoch)
                self.send_json(200, {
                    "granted": True,
                    "node_id": node,
                    "epoch": epoch,
                    "lease_until": lease_until,
                    "lease_seconds": LEASE_SECONDS,
                })
                return

            if holder == node:
                lease_until = now + LEASE_SECONDS
                conn.execute(
                    "UPDATE lease_state SET lease_until=?, updated_at=? "
                    "WHERE singleton=1",
                    (lease_until, now),
                )
                conn.commit()
                self.send_json(200, {
                    "granted": True,
                    "node_id": node,
                    "epoch": epoch,
                    "lease_until": lease_until,
                    "lease_seconds": LEASE_SECONDS,
                })
                return

            conn.commit()
            self.send_json(409, {
                "granted": False,
                "holder": holder,
                "epoch": epoch,
                "lease_until": lease_until,
                "lease_remaining": max(0.0, lease_until - now),
            })
        except Exception:
            conn.rollback()
            logger.exception("Lease claim failed")
            self.send_json(500, {"error": "WITNESS_INTERNAL_ERROR"})
        finally:
            conn.close()

    def validate(self, data):
        node = data.get("node_id")
        try:
            epoch = int(data.get("epoch"))
            minimum = float(data.get("min_remaining", 0))
        except (TypeError, ValueError):
            self.send_json(400, {"error": "INVALID_LEASE_FIELDS"})
            return

        if node not in VALID_NODES or epoch < 1 or not 0 <= minimum <= 5:
            self.send_json(400, {"error": "INVALID_LEASE_FIELDS"})
            return

        conn = connect_db()
        try:
            row = conn.execute(
                "SELECT holder, epoch, lease_until FROM lease_state "
                "WHERE singleton=1"
            ).fetchone()
        finally:
            conn.close()

        now = time.time()
        holder, current_epoch, lease_until = row
        remaining = max(0.0, lease_until - now)
        valid = (
            holder == node
            and current_epoch == epoch
            and remaining >= minimum
        )
        self.send_json(200, {
            "valid": valid,
            "holder": holder,
            "epoch": current_epoch,
            "lease_remaining": remaining,
        })

    def log_message(self, fmt, *args):
        logger.info("%s - %s", self.client_address[0], fmt % args)


if __name__ == "__main__":
    if not TOKEN_PATH.is_file():
        raise SystemExit("Witness token file is missing.")
    initialize_db()
    logger.info("Starting witness on %s:%s; lease=%ss", HOST, PORT, LEASE_SECONDS)
    ThreadingHTTPServer((HOST, PORT), WitnessHandler).serve_forever()
