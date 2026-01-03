# sol_cgt

`sol_cgt` is a command line tool for computing capital gains from Solana (SPL)
token trading with an emphasis on auditability and Australian tax reporting.
The project ingests transactions via the Helius Enhanced API, normalises them
into a canonical event model, performs lot based cost base tracking and emits
CSV/Parquet/XLSX reports as well as pretty console summaries.

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

4. **Provide your API keys (optional for pricing)**

   ```bash
   cp .env.example .env
   ```

   Open `.env` in a text editor and paste your [Helius](https://www.helius.dev/) API key (required only for `fetch`). If you also add a [Birdeye](https://birdeye.so/) key the tool can source token prices directly from Birdeye. Don't have a Birdeye key? No problem — the app uses free price and FX sources by default.

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
   poetry run solcgt compute \
     -w <YOUR_WALLET_ADDRESS> \
     --fy "2024-2025" \
     --format both \
     --xlsx out.xlsx
   ```

   This runs the entire pipeline (normalisation → reconciliation → accounting → reporting). Reports are written to `./reports/<wallet_or_combined>/<financial_year>/` in CSV/Parquet format, and the XLSX is written to the path you pass via `--xlsx`. The console also prints rich tables summarising gains/losses and any items needing attention.

Need a reminder of the available options? Use:

```bash
poetry run solcgt --help
poetry run solcgt report --help
```

## Features

- Multi-wallet aggregation with self-transfer reconciliation.
- Deterministic lot matching using FIFO/LIFO/HIFO/Specific ID.
- Cached price and FX lookups to avoid repeated API calls (CoinGecko + Jupiter + frankfurter.app; Birdeye optional).
- Structured CSV/Parquet/XLSX exports and console summaries via `rich`.

## CLI examples

```bash
poetry run solcgt compute \
  --wallet <ADDR1> \
  --wallet <ADDR2> \
  --fy "2024-2025" \
  --method FIFO \
  --xlsx out.xlsx \
  --outdir out/
```

Use `--dry-run` to normalize without accounting.

Config flags:
- `external_lot_tracking`: attempt to match transfers returning from external wallets (default true).
- `fx_source`: `frankfurter` (default) or `rba`.

## Migration notes

- Transfers between included wallets are now treated as non-taxable lot moves.
- Transfers to external wallets are treated as out-of-scope moves with warnings (no disposal).

## Development

Tests can be executed with:

```bash
poetry run pytest
```

The repository stores normalised transactions and API payloads in `./cache` to
make runs idempotent and auditable.

> **Disclaimer:** This tool assists with record keeping but is not tax advice.
