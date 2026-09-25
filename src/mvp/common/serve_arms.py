"""The per-arm stores' schema, shared by their writer and their readers.

Lives in `mvp.common` (a leaf package) for the reason `chain_shape` does: the
projection writers (`mvp.projection.iid.artifacts`) produce these files and the
`chain_arm` transform and promotion (`mvp.model`) read them, and the two sides
must not import each other at module scope.
"""

from __future__ import annotations

FOLD_SERVE_ARMS_PARQUET = "fold_serve_arms.parquet"
SERVE_ARMS_PARQUET = "serve_arms.parquet"  # forward rows, written by run_projection
ARM_STORE_FILES = (FOLD_SERVE_ARMS_PARQUET, SERVE_ARMS_PARQUET)

FOLD_SERVE_ARM_COLUMNS = [
    "match_uid", "server_id", "returner_id", "effective_match_date",
    "fold_idx", "scoreable", "chain_fi_rate", "chain_w1_prob", "chain_w2_prob",
]
FORWARD_SERVE_ARM_COLUMNS = [c for c in FOLD_SERVE_ARM_COLUMNS if c != "fold_idx"]
# The stored values, in the order `prior_naming.ARM_VALUES` names what the
# transform emits from each.
ARM_STORE_VALUES = ("chain_fi_rate", "chain_w1_prob", "chain_w2_prob")
