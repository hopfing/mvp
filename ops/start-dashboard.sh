#!/usr/bin/env bash
# Start the analysis dashboard, accessible on the LAN.
# Streamlit defaults to port 8501.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
exec poetry run python -m streamlit run src/mvp/analysis/dashboard/app.py \
    --server.address 0.0.0.0 \
    --server.headless=true
