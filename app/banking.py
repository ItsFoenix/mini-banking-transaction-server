import sqlite3
from dataclasses import dataclass
from typing import Optional

from app.database import get_connection, initialize_database


@dataclass
class BankingError(Exception):
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


def create_account(account_id: str, initial_balance: int = 0) -> dict:
    """Create a bank account with an optional initial balance."""
    if not isinstance(account_id, str) or not account_id.strip():
        raise BankingError("INVALID_ACCOUNT_ID", "Account ID must not be empty.")

    if type(initial_balance) is not int or initial_balance < 0:
        raise BankingError(
            "INVALID_AMOUNT",
            "Initial balance must be a non-negative integer.",
        )

    initialize_database()

    try:
        with get_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO accounts (account_id, balance) VALUES (?, ?)",
                (account_id.strip(), initial_balance),
            )
            connection.commit()
    except sqlite3.IntegrityError as exc:
        if "accounts.account_id" in str(exc):
            raise BankingError(
                "ACCOUNT_EXISTS",
                "An account with this ID already exists.",
            ) from exc
        raise

    return {
        "account_id": account_id.strip(),
        "balance": initial_balance,
    }


def get_balance(account_id: str) -> dict:
    """Return the current balance for an existing account."""
    with get_connection() as connection:
        row = connection.execute(
            "SELECT account_id, balance FROM accounts WHERE account_id = ?",
            (account_id,),
        ).fetchone()

    if row is None:
        raise BankingError("ACCOUNT_NOT_FOUND", "Account does not exist.")

    return {
        "account_id": row["account_id"],
        "balance": row["balance"],
    }


def transfer_operation(
    request_id: str,
    account_id: str,
    action: str,
    amount: int,
) -> dict:
    """Apply one deposit or withdrawal atomically and idempotently."""
    if not isinstance(request_id, str) or not request_id.strip():
        raise BankingError("MISSING_REQUEST_ID", "Request ID is required.")

    if not isinstance(account_id, str) or not account_id.strip():
        raise BankingError("INVALID_ACCOUNT_ID", "Account ID is required.")

    if action not in ("deposit", "withdraw"):
        raise BankingError("UNSUPPORTED_ACTION", "Unsupported banking action.")

    if type(amount) is not int or amount <= 0:
        raise BankingError(
            "INVALID_AMOUNT",
            "Amount must be a positive integer.",
        )

    request_id = request_id.strip()
    account_id = account_id.strip()

    with get_connection() as connection:
        try:
            connection.execute("BEGIN IMMEDIATE")

            previous = connection.execute(
                """
                SELECT account_id, action, amount, balance_after
                FROM transactions
                WHERE request_id = ?
                """,
                (request_id,),
            ).fetchone()

            if previous is not None:
                if (
                    previous["account_id"] != account_id
                    or previous["action"] != action
                    or previous["amount"] != amount
                ):
                    raise BankingError(
                        "REQUEST_ID_REUSED",
                        "Request ID was already used for a different operation.",
                    )

                connection.commit()
                return {
                    "request_id": request_id,
                    "account_id": account_id,
                    "action": action,
                    "amount": amount,
                    "balance": previous["balance_after"],
                    "duplicate": True,
                }

            account = connection.execute(
                "SELECT balance FROM accounts WHERE account_id = ?",
                (account_id,),
            ).fetchone()

            if account is None:
                raise BankingError(
                    "ACCOUNT_NOT_FOUND",
                    "Account does not exist.",
                )

            current_balance = account["balance"]

            if action == "withdraw" and current_balance < amount:
                raise BankingError(
                    "INSUFFICIENT_FUNDS",
                    "Insufficient funds for this withdrawal.",
                )

            if action == "deposit":
                new_balance = current_balance + amount
            else:
                new_balance = current_balance - amount

            connection.execute(
                "UPDATE accounts SET balance = ? WHERE account_id = ?",
                (new_balance, account_id),
            )

            connection.execute(
                """
                INSERT INTO transactions (
                    request_id, account_id, action, amount, balance_after
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (request_id, account_id, action, amount, new_balance),
            )

            connection.commit()

            return {
                "request_id": request_id,
                "account_id": account_id,
                "action": action,
                "amount": amount,
                "balance": new_balance,
                "duplicate": False,
            }

        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise


def deposit(
    request_id: str,
    account_id: str,
    amount: int,
) -> dict:
    return transfer_operation(request_id, account_id, "deposit", amount)


def withdraw(
    request_id: str,
    account_id: str,
    amount: int,
) -> dict:
    return transfer_operation(request_id, account_id, "withdraw", amount)
