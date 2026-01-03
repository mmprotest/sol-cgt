## AU CGT treatment defaults

This document describes how `sol_cgt` classifies events for Australian CGT reporting. It is
not tax advice.

### Event taxonomy and treatment

| Event type | Treatment | Notes |
| --- | --- | --- |
| `swap` | Disposal of input mints + acquisition of output mints | Net deltas per mint are used. |
| `sell` | Disposal | Proceeds determined in AUD. |
| `buy` | Acquisition | Cost base determined in AUD. |
| `transfer_in` | Acquisition | Treated as acquisition at FMV unless matched by external tracking. |
| `transfer_out` | Out-of-scope move | Lots moved to external bucket, not a disposal. |
| `airdrop` / staking rewards | Ordinary income acquisition | FMV at receipt becomes cost base. |
| `mint` | Acquisition | Cost base as FMV at receipt. |
| `burn` | Disposal | Proceeds based on FMV at burn time. |
| Liquidity add/remove | Disposal / acquisition | Controlled by settings where applicable. |

### Self-transfers between included wallets
- Transfers between provided wallets are **non-taxable**.
- Lots are moved from sender to receiver and retain:
  - original acquisition timestamp
  - unit cost in AUD
  - remaining quantity
- A lot-move audit record is created for reporting.
- Network fees on self-transfers are allocated across the moved lots, increasing cost base
  without changing acquisition time.

### Transfers to/from external wallets (out of scope)
- **Outgoing transfers** to wallets outside the provided set are treated as out-of-scope moves.
  Lots are moved to an external bucket (`__external__:<counterparty_or_unknown>`) and a warning is emitted.
  Network fees on the move are allocated across the moved lots so cost base is preserved if/when
  the lots return.
- **Incoming transfers** from external wallets are treated as new acquisitions at FMV **unless**
  external tracking is enabled (optional) and lots can be matched.
  When matched, lots are moved back in with original acquisition timestamps, unit cost, and
  attached fees.

### Fee attribution
- Network fees are applied **only** to the fee payer.
- Fees reduce proceeds for disposals and increase cost base for acquisitions.
- `unit_cost_aud` **excludes** acquisition fees. Historical fees are stored separately on each
  lot as `fees_aud` and prorated into disposal cost base based on the lot quantity consumed.
- Self-transfer and out-of-scope move fees are attached to the moved lots and tracked in the
  lot-move audit log; they are not treated as taxable disposals.

### Swap valuation hints
- Swap normalization derives proceeds and cost hints when price data is available in the swap
  payload. Outgoing disposal legs use the incoming consideration value, allocated proportionally
  across outgoing mints, and incoming acquisition legs use the outgoing value similarly.
- If price data is missing for some swap mints, the engine falls back to spot pricing and emits
  a warning.

### Pricing defaults
- SOL historical pricing uses Kraken daily OHLC closes (SOL/USD) mapped to Australia/Melbourne
  dates, then converted to AUD using the daily USD→AUD FX rate for that local date.
- CoinGecko is only used for SOL pricing if a CoinGecko API key is configured.

### CGT discount eligibility
- Disposals held for **≥ 12 months** are flagged as discount-eligible.
- The 50% discount is **not** applied automatically unless configured by the user.
