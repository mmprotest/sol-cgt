## Unreleased

### Summary
- Canonicalized swaps to prevent double counting and added fee-payer attribution.
- Added multi-wallet lot moves for self-transfers and out-of-scope external transfer warnings.
- Implemented free pricing/FX fallbacks with cached providers and XLSX export.
- Lot fees now prorate into disposal cost base and are no longer baked into unit costs.
- Removed pandas/numpy/pyarrow/rich from the default install; Parquet and rich console output now require extras.

### How to run (FY 2024-2025)
```bash
solcgt compute \
  --wallet <ADDR1> \
  --wallet <ADDR2> \
  --fy "2024-2025" \
  --method FIFO \
  --xlsx out.xlsx \
  --outdir out/
```
