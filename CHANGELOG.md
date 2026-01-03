## Unreleased

### Summary
- Canonicalized swaps to prevent double counting and added fee-payer attribution.
- Added multi-wallet lot moves for self-transfers and out-of-scope external transfer warnings.
- Implemented free pricing/FX fallbacks with cached providers and XLSX export.

### How to run (FY 2024-2025)
```bash
poetry run solcgt compute \
  --wallet <ADDR1> \
  --wallet <ADDR2> \
  --fy "2024-2025" \
  --method FIFO \
  --xlsx out.xlsx \
  --outdir out/
```
