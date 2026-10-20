import json
import urllib.error
import urllib.parse
import urllib.request
import uuid

SERVER_URL = "http://192.168.11.128:8000"


def show_response(response, status):
    print(f"\nHTTP Status: {status}")
    print(json.dumps(response, indent=2))


def get_balance(account_id):
    query = urllib.parse.urlencode({"account_id": account_id})
    request = urllib.request.Request(f"{SERVER_URL}/balance?{query}")

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            data = json.loads(response.read().decode("utf-8"))
            show_response(data, response.status)
    except urllib.error.HTTPError as error:
        data = json.loads(error.read().decode("utf-8"))
        show_response(data, error.code)
    except (urllib.error.URLError, TimeoutError) as error:
        print(f"Connection error: {error}")


def send_request(payload):
    payload["request_id"] = str(uuid.uuid4())

    request = urllib.request.Request(
        f"{SERVER_URL}/",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            data = json.loads(response.read().decode("utf-8"))
            show_response(data, response.status)
    except urllib.error.HTTPError as error:
        data = json.loads(error.read().decode("utf-8"))
        show_response(data, error.code)
    except (urllib.error.URLError, TimeoutError) as error:
        print(f"Connection error: {error}")


def main():
    while True:
        print("\n=== Mini Banking Client ===")
        print("1. Check balance")
        print("2. Create account")
        print("3. Deposit")
        print("4. Withdraw")
        print("5. Exit")

        choice = input("Select an option: ").strip()

        if choice == "5":
            print("Goodbye!")
            break

        if choice not in ("1", "2", "3", "4"):
            print("Invalid option.")
            continue

        account_id = input("Account ID: ").strip()
        if not account_id:
            print("Account ID is required.")
            continue

        if choice == "1":
            get_balance(account_id)
            continue

        if choice == "2":
            raw_balance = input("Initial balance (default 0): ").strip()
            try:
                initial_balance = int(raw_balance) if raw_balance else 0
            except ValueError:
                print("Balance must be an integer.")
                continue

            send_request({
                "action": "create_account",
                "account_id": account_id,
                "initial_balance": initial_balance,
            })
            continue

        raw_amount = input("Amount: ").strip()
        try:
            amount = int(raw_amount)
        except ValueError:
            print("Amount must be an integer.")
            continue

        action = "deposit" if choice == "3" else "withdraw"
        send_request({
            "action": action,
            "account_id": account_id,
            "amount": amount,
        })


if __name__ == "__main__":
    main()
