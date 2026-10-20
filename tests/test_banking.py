import tempfile
import unittest
from pathlib import Path

import app.database as database
from app.banking import (
    BankingError,
    create_account,
    deposit,
    get_balance,
    withdraw,
)


class BankingTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = database.DB_PATH
        database.DB_PATH = Path(self.temp_dir.name) / "test_bank.db"
        database.initialize_database()

    def tearDown(self):
        database.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_create_account_and_get_balance(self):
        create_account("ACC-001", 1000)
        result = get_balance("ACC-001")
        self.assertEqual(result["balance"], 1000)

    def test_deposit_increases_balance(self):
        create_account("ACC-002", 1000)
        result = deposit("REQ-002", "ACC-002", 300)
        self.assertEqual(result["balance"], 1300)
        self.assertEqual(get_balance("ACC-002")["balance"], 1300)

    def test_withdraw_decreases_balance(self):
        create_account("ACC-003", 1000)
        result = withdraw("REQ-003", "ACC-003", 250)
        self.assertEqual(result["balance"], 750)

    def test_insufficient_funds_is_rejected(self):
        create_account("ACC-004", 100)
        with self.assertRaises(BankingError) as context:
            withdraw("REQ-004", "ACC-004", 200)
        self.assertEqual(context.exception.code, "INSUFFICIENT_FUNDS")
        self.assertEqual(get_balance("ACC-004")["balance"], 100)

    def test_unknown_account_is_rejected(self):
        with self.assertRaises(BankingError) as context:
            get_balance("DOES-NOT-EXIST")
        self.assertEqual(context.exception.code, "ACCOUNT_NOT_FOUND")

    def test_zero_amount_is_rejected(self):
        create_account("ACC-005", 100)
        with self.assertRaises(BankingError) as context:
            deposit("REQ-005", "ACC-005", 0)
        self.assertEqual(context.exception.code, "INVALID_AMOUNT")

    def test_duplicate_request_does_not_deposit_twice(self):
        create_account("ACC-006", 100)
        first = deposit("REQ-006", "ACC-006", 50)
        second = deposit("REQ-006", "ACC-006", 50)

        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(get_balance("ACC-006")["balance"], 150)

    def test_reusing_request_id_with_different_operation_is_rejected(self):
        create_account("ACC-007", 100)
        deposit("REQ-007", "ACC-007", 20)

        with self.assertRaises(BankingError) as context:
            withdraw("REQ-007", "ACC-007", 10)

        self.assertEqual(context.exception.code, "REQUEST_ID_REUSED")
        self.assertEqual(get_balance("ACC-007")["balance"], 120)


if __name__ == "__main__":
    unittest.main()
