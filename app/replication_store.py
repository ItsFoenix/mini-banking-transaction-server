"""Idempotent, atomic event application for the standby database."""

import json
import sqlite3

from app.banking import BankingError
from app.database import get_connection, initialize_database


def initialize_replication_store() -> None:
    initialize_database()
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS replication_events (
                event_id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                response_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                )
            )
            """
        )


def apply_replication_event(event: dict) -> dict:
    """Apply one create-account, deposit, or withdraw event exactly once."""
    if not isinstance(event, dict):
        raise BankingError("INVALID_EVENT", "Event must be a JSON object.")

    event_id = event.get("event_id")
    action = event.get("action")
    account_id = event.get("account_id")

    if not isinstance(event_id, str) or not event_id.strip():
        raise BankingError("INVALID_EVENT", "event_id is required.")
    event_id = event_id.strip()

    if not isinstance(account_id, str) or not account_id.strip():
        raise BankingError("INVALID_EVENT", "account_id is required.")
    account_id = account_id.strip()

    if action not in ("create_account", "deposit", "withdraw"):
        raise BankingError("INVALID_EVENT", "Unsupported replication action.")

    if action == "create_account":
        amount = event.get("initial_balance", 0)
        if type(amount) is not int or amount < 0:
            raise BankingError(
                "INVALID_AMOUNT",
                "Initial balance must be a non-negative integer.",
            )
    else:
        amount = event.get("amount")
        if type(amount) is not int or amount <= 0:
            raise BankingError(
                "INVALID_AMOUNT",
                "Amount must be a positive integer.",
            )

    payload = dict(event)
    payload["event_id"] = event_id
    payload["account_id"] = account_id
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))

    initialize_replication_store()

    with get_connection() as connection:
        try:
            connection.execute("BEGIN IMMEDIATE")

            previous_event = connection.execute(
                """
                SELECT action, payload_json, response_json
                FROM replication_events
                WHERE event_id = ?
                """,
                (event_id,),
            ).fetchone()

            if previous_event is not None:
                if (
                    previous_event["action"] != action
                    or previous_event["payload_json"] != payload_json
                ):
                    raise BankingError(
                        "EVENT_ID_REUSED",
                        "Event ID was reused for a different operation.",
                    )
                connection.commit()
                response = json.loads(previous_event["response_json"])
                response["duplicate"] = True
                return response

            if action == "create_account":
                connection.execute(
                    "INSERT INTO accounts (account_id, balance) VALUES (?, ?)",
                    (account_id, amount),
                )
                response = {
                    "event_id": event_id,
                    "action": action,
                    "account_id": account_id,
                    "balance": amount,
                    "duplicate": False,
                }

            else:
                request_id = event.get("request_id")
                if not isinstance(request_id, str) or not request_id.strip():
                    raise BankingError(
                        "INVALID_EVENT",
                        "request_id is required for a transfer.",
                    )
                request_id = request_id.strip()

                previous_transaction = connection.execute(
                    """
                    SELECT account_id, action, amount, balance_after
                    FROM transactions
                    WHERE request_id = ?
                    """,
                    (request_id,),
                ).fetchone()

                if previous_transaction is not None:
                    if (
                        previous_transaction["account_id"] != account_id
                        or previous_transaction["action"] != action
                        or previous_transaction["amount"] != amount
                    ):
                        raise BankingError(
                            "REQUEST_ID_REUSED",
                            "Request ID belongs to a different transaction.",
                        )
                    balance = previous_transaction["balance_after"]
                    duplicate = True
                else:
                    account = connection.execute(
                        "SELECT balance FROM accounts WHERE account_id = ?",
                        (account_id,),
                    ).fetchone()

                    if account is None:
                        raise BankingError(
                            "ACCOUNT_NOT_FOUND",
                            "Account does not exist on the standby.",
                        )

                    current_balance = account["balance"]
                    if action == "withdraw" and current_balance < amount:
                        raise BankingError(
                            "INSUFFICIENT_FUNDS",
                            "Insufficient funds for this withdrawal.",
                        )

                    balance = (
                        current_balance + amount
                        if action == "deposit"
                        else current_balance - amount
                    )

                    connection.execute(
                        "UPDATE accounts SET balance = ? WHERE account_id = ?",
                        (balance, account_id),
                    )
                    connection.execute(
                        """
                        INSERT INTO transactions (
                            request_id, account_id, action, amount, balance_after
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (request_id, account_id, action, amount, balance),
                    )
                    duplicate = False

                response = {
                    "event_id": event_id,
                    "request_id": request_id,
                    "action": action,
                    "account_id": account_id,
                    "amount": amount,
                    "balance": balance,
                    "duplicate": duplicate,
                }

            connection.execute(
                """
                INSERT INTO replication_events (
                    event_id, action, payload_json, response_json
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    event_id,
                    action,
                    payload_json,
                    json.dumps(response, sort_keys=True),
                ),
            )
            connection.commit()
            return response

        except sqlite3.IntegrityError as exc:
            if connection.in_transaction:
                connection.rollback()
            raise BankingError(
                "REPLICATION_CONFLICT",
                "Replication event conflicts with existing database state.",
            ) from exc
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
