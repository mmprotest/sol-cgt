# sol_cgt

`sol_cgt` is a command line tool for computing capital gains from Solana (SPL)
token trading with an emphasis on auditability and Australian tax reporting.
The project ingests transactions via the Helius Enhanced API, normalises them
into a canonical event model, performs lot based cost base tracking and emits
CSV/Parquet reports as well as pretty console summaries.

## Quick start

If you're new to Python tooling, follow these steps exactly.

1. **Install prerequisites**

   - Install [Python 3.11](https://www.python.org/downloads/).
   - Install [Poetry](https://python-poetry.org/docs/#installation) (the package manager we use).

2. **Clone the repository**

   ```bash
   git clone https://github.com/<you>/sol-cgt.git
   cd sol-cgt
   ```

3. **Install the Python dependencies**

   ```bash
   poetry install
   ```

   The first time this runs it may take a few minutes while Poetry creates an isolated virtual environment.

4. **Provide your API keys**

   ```bash
   cp .env.example .env
   ```

   Open `.env` in a text editor and paste your [Helius](https://www.helius.dev/) API key (this one is required). If you also add a [Birdeye](https://birdeye.so/) key the tool can source token prices directly from Birdeye. Don't have a Birdeye key? No problem—the app will fall back to on-chain swap prices and CoinGecko where available, you just get better coverage and fewer "missing price" warnings when a Birdeye key is present.

5. **(Optional) Create a config file**

   ```bash
   cp config.example.yaml config.yaml
   ```

   Edit `config.yaml` to list the wallet addresses you care about and tweak defaults such as the tax method or timezone. Anything you set on the command line overrides this file.

6. **Fetch raw transactions for your wallet(s)**

   ```bash
   poetry run solcgt fetch -w <YOUR_WALLET_ADDRESS>
   ```

   Replace `<YOUR_WALLET_ADDRESS>` with an actual Solana address. The tool pulls transactions via the Helius Enhanced API and caches them in `./cache/raw/` so you only need to refetch when new activity occurs.

7. **Generate the capital gains report**

   ```bash
   poetry run solcgt report \
     -w <YOUR_WALLET_ADDRESS> \
     --fy "2024-2025" \
     --format both
   ```

   This runs the entire pipeline (normalisation → reconciliation → accounting → reporting). Reports are written to `./reports/<wallet_or_combined>/<financial_year>/` in both CSV and Parquet format. The console also prints rich tables summarising gains/losses and any items needing attention.

Need a reminder of the available options? Use:

```bash
poetry run solcgt --help
poetry run solcgt report --help
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
