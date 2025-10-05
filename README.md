# sol_cgt

`sol_cgt` is a command line tool for computing capital gains from Solana (SPL)
token trading with an emphasis on auditability and Australian tax reporting.
The project ingests transactions via the Helius Enhanced API, normalises them
into a canonical event model, performs lot based cost base tracking and emits
CSV/Parquet reports as well as pretty console summaries.

## Quick start

```bash
poetry install
cp .env.example .env
# populate the .env file with your API keys
poetry run solcgt fetch -w <WALLET>
poetry run solcgt report -w <WALLET> --fy "2024-2025"
```

## Features

- Multi-wallet aggregation with self-transfer reconciliation.
- Deterministic lot matching using FIFO/LIFO/HIFO/Specific ID.
- Cached price and FX lookups to avoid repeated API calls.
- Structured CSV/Parquet exports and console summaries via `rich`.

## Development

Tests can be executed with:

```bash
poetry run pytest
```

The repository stores normalised transactions and API payloads in `./cache` to
make runs idempotent and auditable.

> **Disclaimer:** This tool assists with record keeping but is not tax advice.
