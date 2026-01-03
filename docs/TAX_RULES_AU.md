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

### Transfers to/from external wallets (out of scope)
- **Outgoing transfers** to wallets outside the provided set are treated as out-of-scope moves.
  Lots are moved to an external bucket and a warning is emitted.
- **Incoming transfers** from external wallets are treated as new acquisitions at FMV **unless**
  external tracking is enabled (optional) and lots can be matched.

### Fee attribution
- Network fees are applied **only** to the fee payer.
- Fees reduce proceeds for disposals and increase cost base for acquisitions.
- Self-transfer fees are tracked in the lot-move audit log and are not treated as taxable events.

### CGT discount eligibility
- Disposals held for **≥ 12 months** are flagged as discount-eligible.
- The 50% discount is **not** applied automatically unless configured by the user.
