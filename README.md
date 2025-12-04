# nse-option-signals


Automated pipeline to fetch NSE F&O bhavcopy, parse, maintain history, compute option-buying signals, and generate a static dashboard.


## Usage
1. Push repository to GitHub.
2. Add workflow (`.github/workflows/run-signals.yml`).
3. Enable GitHub Pages on `docs/` if you want a dashboard.


The workflow will run daily (20:00 IST) and update `data/history` and `data/signals`.
