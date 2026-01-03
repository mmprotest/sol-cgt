## sol_cgt pipeline overview

This document describes the current pipeline and the invariants enforced for deterministic,
multi-wallet CGT processing.

### 1) Fetch + raw cache
- **Source:** Helius Enhanced API.
- **Cache:** JSONL files under `./cache/raw/<wallet>.jsonl`.
- **Invariant:** cache loader de-duplicates by transaction signature/id and keeps the first-seen entry.

### 2) Normalization
- **Module:** `sol_cgt/ingestion/normalize.py`
- **Input:** raw Helius enhanced transactions.
- **Output:** `NormalizedEvent` records with deterministic IDs and timestamps.
- **Invariant:** transactions are normalized in deterministic `(timestamp, id)` order.
- **Swap handling:** swaps are canonicalized into net mint deltas; transfer events that match swap legs
  are suppressed to avoid double counting.
- **Fees:** network fees are assigned only to the fee payer (or inferred payer), and only once per tx.

### 3) Reconciliation (self-transfers)
- **Module:** `sol_cgt/reconciliation/transfers.py`
- Detects transfer-in/out pairs between wallets in the provided set.
- Annotates events and returns matched pairs for downstream lot movement.

### 4) Accounting engine
- **Module:** `sol_cgt/accounting/engine.py`
- **Lots:** FIFO/LIFO/HIFO (and Specific ID) lot allocation.
- **Self-transfers:** move lots between wallets (same acquisition date and unit cost) without taxable events.
- **External transfers:** move lots to an out-of-scope bucket with warnings (external tracking enables returns).
- **Fee treatment:** fees reduce proceeds for disposals and increase cost base for acquisitions.
- **Invariant:** events are processed in deterministic order; lots are updated consistently.

### 5) Pricing + FX
- **Module:** `sol_cgt/pricing.py`
- **Primary sources:** CoinGecko for SOL/USD historical, Jupiter for SPL token prices, frankfurter.app FX.
- **Fallback:** Birdeye (if API key provided) and RBA FX.
- **Invariant:** prices are cached by timestamp bucket and FX by date.

### 6) Reporting
- **Module:** `sol_cgt/reporting/`
- **Outputs:** CSV/Parquet (existing) plus XLSX (new).
- **Tabs:** Overview, Transactions, Lots, Disposals, Summary by token, Wallet summary, Lot moves, Warnings.

### Key invariants
- Canonical swap handling prevents double counting.
- Transfer between included wallets is non-taxable and preserves lot metadata.
- Deterministic ordering and de-duplication ensure stable outputs.
- AU financial year boundaries are evaluated in Australia/Melbourne time.
