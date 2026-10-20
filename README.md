# Mini Banking Transaction Server

A simple HTTP/JSON banking server implemented in Python using SQLite.

## Features
- Create accounts, check balances, deposit, and withdraw
- Handle concurrent requests with a configurable limit
- Use SQLite transactions for consistent account balances
- Persist data across server restarts
- Prevent duplicate transactions using request IDs
- Return structured errors and log request details and latency

## Requirements
- Python 3.10 or newer
- No third-party Python packages required

## Start the Server
From the project root directory, run:
python -m app.server

## Configuration
- BANK_HOST: Listening address (default: 0.0.0.0)
- BANK_PORT: Listening port (default: 8000)
- BANK_MAX_CONCURRENT: Maximum concurrent handlers (default: 10)
- BANK_DB_PATH: Optional custom SQLite database path

## API Endpoints
- GET /health: Health check
- GET /balance?account_id=ACCOUNT_ID: Check balance
- POST /: Create account, deposit, or withdraw using a JSON action

## Run Tests
python -m unittest discover -s tests -v

## Data and Logs
- Database: data/bank.db
- Log file: logs/bank-server.log
