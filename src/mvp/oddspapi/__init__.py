"""oddspapi source: raw capture -> stage parquet for evaluation.

Historical odds with a sharp reference book, used by the evaluation backtest. The
scrapers remain the live pipeline's source; these two are separate jobs over
separate data and are never reconciled.

Spec: mvp-docs/specs/2026-07-26-oddspapi-odds-ingest.md
"""
