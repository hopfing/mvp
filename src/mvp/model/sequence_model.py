"""Sequence model: GRU encoder over per-player last-N match history.

For each match, encodes the player's and opponent's prior match histories
(as ordered sequences of per-match feature vectors) via a shared GRU encoder.
The player and opponent state vectors are concatenated with the current
match's features and fed through an MLP head to produce P(win).

Implements `BaseModel` (`fit(X, y)`, `predict_proba(X)`) so it slots into the
existing model runner without changes to abstractions.

Identifier columns expected at end of X (indices supplied via params):
    - player_id_col_idx
    - opp_id_col_idx
    - match_date_col_idx  (integer days-since-epoch)

These are appended by the runner just before calling fit/predict.

History dict is built once per fold via `set_history_features(df)` BEFORE
fit. The runner is responsible for constructing the history DataFrame —
typically pre-2020-seeded matches up through the training fold boundary.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import polars as pl

from mvp.model.models import BaseModel


# =============================================================================
# Per-historical-match feature schema
# =============================================================================
# These columns are extracted from the history DataFrame supplied by the
# runner. Anything not directly in matches.parquet is computed in
# `_project_history_features`. The integer days-since-epoch date is prepended
# as column 0 of the stored history matrix for binary-search lookup.

# Raw columns the runner is expected to supply in the history DataFrame.
# These are all directly present in matches.parquet (no feature engine compute).
HISTORY_RAW_COLUMNS: tuple[str, ...] = (
    "player_id",
    "effective_match_date",
    "won",
    "best_of",
    "reason",
    "surface",
    "tournament_level",
    "player_set1_games", "player_set2_games", "player_set3_games",
    "player_set4_games", "player_set5_games",
    "opp_set1_games", "opp_set2_games", "opp_set3_games",
    "opp_set4_games", "opp_set5_games",
    "player_elo", "opp_elo",
    "player_glicko_mu", "player_glicko_rd",
    "opp_glicko_mu", "opp_glicko_rd",
)

# Projected history feature names (output of _project_history_features, in order).
# `days_ago` is NOT pre-projected — it's computed at lookup time from the
# current match's date minus the historical match's date.
PROJECTED_HISTORY_FEATURES: tuple[str, ...] = (
    # Outcome / context (7)
    "won",
    "is_best_of_5",
    "match_completed",
    "score_margin",
    "total_games_won_in_match",
    "total_games_lost_in_match",
    "straight_sets",
    # Ratings at time of match (6)
    "player_elo", "opp_elo",
    "player_glicko_mu", "player_glicko_rd",
    "opp_glicko_mu", "opp_glicko_rd",
    # Surface one-hot (4)
    "is_hard", "is_clay", "is_grass", "is_carpet",
    # Tier one-hot (7)
    "is_slam", "is_masters", "is_atp500", "is_atp250",
    "is_chal_high", "is_chal_low", "is_itf",
)

# Final per-match dim includes `days_ago_log` added at lookup time
HIST_FEAT_DIM_PROJECTED = len(PROJECTED_HISTORY_FEATURES)  # 28
HIST_FEAT_DIM = HIST_FEAT_DIM_PROJECTED + 1  # +1 for days_ago_log at lookup


def _project_history_features(df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Project the raw history DataFrame to the per-match feature matrix.

    Returns
    -------
    player_ids : (M,) int64
    dates_int  : (M,) int64 days-since-epoch
    features   : (M, HIST_FEAT_DIM_PROJECTED) float32
    """
    # Compute derived columns.
    invalid_reasons = {"W/O", "RET", "DEF", "UNP"}
    expressions: list[pl.Expr] = []

    # Outcome / context
    expressions.append(pl.col("won").cast(pl.Float32).alias("_won"))
    expressions.append((pl.col("best_of") == 5).cast(pl.Float32).alias("_is_best_of_5"))
    expressions.append(
        (~pl.col("reason").fill_null("").is_in(list(invalid_reasons))).cast(pl.Float32).alias("_match_completed")
    )

    # Score margin: sets won by player − sets won by opp (from per-set game counts)
    set_player_cols = [f"player_set{i}_games" for i in range(1, 6)]
    set_opp_cols = [f"opp_set{i}_games" for i in range(1, 6)]
    # Per-set: 1 if player_games > opp_games AND both played, -1 if reverse, 0 if not played
    per_set_diffs = []
    sets_played_expr = pl.lit(0.0, dtype=pl.Float32)
    for pc, oc in zip(set_player_cols, set_opp_cols):
        is_played = pl.col(pc).is_not_null() & pl.col(oc).is_not_null()
        pc_filled = pl.col(pc).fill_null(0)
        oc_filled = pl.col(oc).fill_null(0)
        diff = (
            pl.when(is_played)
            .then(
                pl.when(pc_filled > oc_filled).then(pl.lit(1.0, dtype=pl.Float32))
                .when(pc_filled < oc_filled).then(pl.lit(-1.0, dtype=pl.Float32))
                .otherwise(pl.lit(0.0, dtype=pl.Float32))
            )
            .otherwise(pl.lit(0.0, dtype=pl.Float32))
            .cast(pl.Float32)
        )
        per_set_diffs.append(diff)
        sets_played_expr = sets_played_expr + is_played.cast(pl.Float32)
    score_margin_expr = per_set_diffs[0]
    for d in per_set_diffs[1:]:
        score_margin_expr = score_margin_expr + d
    expressions.append(score_margin_expr.alias("_score_margin"))

    # Total games won/lost in match
    total_won_expr = pl.lit(0.0, dtype=pl.Float32)
    total_lost_expr = pl.lit(0.0, dtype=pl.Float32)
    for pc, oc in zip(set_player_cols, set_opp_cols):
        total_won_expr = total_won_expr + pl.col(pc).fill_null(0).cast(pl.Float32)
        total_lost_expr = total_lost_expr + pl.col(oc).fill_null(0).cast(pl.Float32)
    expressions.append(total_won_expr.alias("_total_games_won"))
    expressions.append(total_lost_expr.alias("_total_games_lost"))
    expressions.append((sets_played_expr <= 2.0).cast(pl.Float32).alias("_straight_sets"))

    # Ratings (pass through with NaN-impute)
    for col in (
        "player_elo", "opp_elo",
        "player_glicko_mu", "player_glicko_rd", "opp_glicko_mu", "opp_glicko_rd",
    ):
        expressions.append(pl.col(col).fill_null(0).cast(pl.Float32).alias(f"_{col}"))

    # Surface one-hot
    expressions.append((pl.col("surface") == "Hard").cast(pl.Float32).alias("_is_hard"))
    expressions.append((pl.col("surface") == "Clay").cast(pl.Float32).alias("_is_clay"))
    expressions.append((pl.col("surface") == "Grass").cast(pl.Float32).alias("_is_grass"))
    expressions.append((pl.col("surface") == "Carpet").cast(pl.Float32).alias("_is_carpet"))

    # Tier one-hot from tournament_level (string with codes like "GS", "M1000", etc.)
    # Defensive: fill_null first so comparisons don't propagate nulls.
    tl = pl.col("tournament_level").fill_null("")
    expressions.append((tl == "GS").cast(pl.Float32).alias("_is_slam"))
    expressions.append((tl == "M1000").cast(pl.Float32).alias("_is_masters"))
    expressions.append((tl == "ATP500").cast(pl.Float32).alias("_is_atp500"))
    expressions.append((tl == "ATP250").cast(pl.Float32).alias("_is_atp250"))
    expressions.append(tl.is_in(["CH175", "CH125", "CH100"]).cast(pl.Float32).alias("_is_chal_high"))
    expressions.append(tl.is_in(["CH75", "CH50"]).cast(pl.Float32).alias("_is_chal_low"))
    expressions.append((tl == "FU").cast(pl.Float32).alias("_is_itf"))

    projected = df.select(
        pl.col("player_id").cast(pl.Int64).alias("_player_id"),
        pl.col("effective_match_date").cast(pl.Date).alias("_date"),
        *expressions,
    )

    # Convert date to days-since-epoch int
    epoch_days = (
        projected["_date"].cast(pl.Int64)
        # Polars Date casts to days since 1970-01-01
    ).to_numpy().astype(np.int64)
    player_ids = projected["_player_id"].to_numpy().astype(np.int64)

    # Stack the projected feature columns in PROJECTED_HISTORY_FEATURES order
    feature_col_names = [f"_{n}" if not n.startswith("is_") else f"_{n}" for n in []]
    # Map from PROJECTED_HISTORY_FEATURES name → underscored column name
    name_map = {
        "won": "_won",
        "is_best_of_5": "_is_best_of_5",
        "match_completed": "_match_completed",
        "score_margin": "_score_margin",
        "total_games_won_in_match": "_total_games_won",
        "total_games_lost_in_match": "_total_games_lost",
        "straight_sets": "_straight_sets",
        "player_elo": "_player_elo",
        "opp_elo": "_opp_elo",
        "player_glicko_mu": "_player_glicko_mu",
        "player_glicko_rd": "_player_glicko_rd",
        "opp_glicko_mu": "_opp_glicko_mu",
        "opp_glicko_rd": "_opp_glicko_rd",
        "is_hard": "_is_hard",
        "is_clay": "_is_clay",
        "is_grass": "_is_grass",
        "is_carpet": "_is_carpet",
        "is_slam": "_is_slam",
        "is_masters": "_is_masters",
        "is_atp500": "_is_atp500",
        "is_atp250": "_is_atp250",
        "is_chal_high": "_is_chal_high",
        "is_chal_low": "_is_chal_low",
        "is_itf": "_is_itf",
    }
    feature_arrays = [
        projected[name_map[n]].to_numpy().astype(np.float32)
        for n in PROJECTED_HISTORY_FEATURES
    ]
    features = np.stack(feature_arrays, axis=1)
    return player_ids, epoch_days, features


def _make_sequence_module():
    """Lazy factory for the torch sequence module."""
    import torch
    import torch.nn as nn

    class SequenceModule(nn.Module):
        def __init__(
            self,
            n_match_features: int,
            hist_feat_dim: int,
            encoder_hidden: int,
            encoder_layers: int,
            encoder_dropout: float,
            head_hidden: list[int],
            head_dropout: float,
            layer_norm: bool,
        ):
            super().__init__()
            # Shared encoder applied to both player and opponent sequences
            self.encoder = nn.GRU(
                input_size=hist_feat_dim,
                hidden_size=encoder_hidden,
                num_layers=encoder_layers,
                batch_first=True,
                dropout=encoder_dropout if encoder_layers > 1 else 0.0,
            )
            head_in = n_match_features + 2 * encoder_hidden
            layers: list[nn.Module] = []
            in_dim = head_in
            for h in head_hidden:
                layers.append(nn.Linear(in_dim, h))
                if layer_norm:
                    layers.append(nn.LayerNorm(h))
                layers.append(nn.ReLU())
                if head_dropout > 0:
                    layers.append(nn.Dropout(head_dropout))
                in_dim = h
            layers.append(nn.Linear(in_dim, 1))
            self.head = nn.Sequential(*layers)
            self.encoder_hidden = encoder_hidden

        def _encode(self, seq: "torch.Tensor", mask: "torch.Tensor") -> "torch.Tensor":
            """Run GRU over the padded sequence and read out the last non-pad timestep.

            seq: (B, T, D)
            mask: (B, T) float, 1.0 for real / 0.0 for pad
            returns: (B, encoder_hidden)
            """
            outputs, _ = self.encoder(seq)  # (B, T, H)
            # Find last non-pad index per row. If row has zero true length,
            # use index 0 (the all-zero state is the GRU's response to all-zero input).
            true_len = mask.sum(dim=1)  # (B,)
            has_history = true_len > 0
            last_idx = (true_len - 1).clamp(min=0).long()  # (B,)
            # Gather: outputs[range(B), last_idx, :]
            batch_idx = torch.arange(outputs.size(0), device=outputs.device)
            state = outputs[batch_idx, last_idx, :]  # (B, H)
            # Zero out rows that had no history (cold-start)
            state = state * has_history.float().unsqueeze(1)
            return state

        def forward(
            self,
            x_match: "torch.Tensor",
            player_seq: "torch.Tensor",
            player_mask: "torch.Tensor",
            opp_seq: "torch.Tensor",
            opp_mask: "torch.Tensor",
        ) -> "torch.Tensor":
            player_state = self._encode(player_seq, player_mask)
            opp_state = self._encode(opp_seq, opp_mask)
            combined = torch.cat([x_match, player_state, opp_state], dim=1)
            return self.head(combined)

    return SequenceModule


class SequenceModel(BaseModel):
    """GRU sequence model over per-player match history."""

    def __init__(self, params: dict[str, Any]) -> None:
        # Identifier column indices (set by runner before fit)
        self.player_id_col_idx: int | None = params.get("player_id_col_idx")
        self.opp_id_col_idx: int | None = params.get("opp_id_col_idx")
        self.match_date_col_idx: int | None = params.get("match_date_col_idx")

        # Sequence / encoder hyperparameters
        self.seq_len: int = params.get("seq_len", 20)
        self.encoder_hidden: int = params.get("encoder_hidden", 64)
        self.encoder_layers: int = params.get("encoder_layers", 1)
        self.encoder_dropout: float = params.get("encoder_dropout", 0.1)
        self.head_hidden: list[int] = params.get("head_hidden", [64, 32])
        self.head_dropout: float = params.get("head_dropout", 0.3)
        self.layer_norm: bool = params.get("layer_norm", True)

        # Training hyperparameters (mirror NeuralNetModel)
        self.learning_rate: float = params.get("learning_rate", 0.001)
        self.batch_size: int = params.get("batch_size", 512)
        self.epochs: int = params.get("epochs", 100)
        self.patience: int = params.get("patience", 10)
        self.weight_decay: float = params.get("weight_decay", 0.0)
        self.grad_clip_norm: float | None = params.get("grad_clip_norm", None)
        self.label_smoothing: float = params.get("label_smoothing", 0.0)
        self.lr_scheduler: str | None = params.get("lr_scheduler", None)
        self.lr_scheduler_factor: float = params.get("lr_scheduler_factor", 0.5)
        self.lr_scheduler_patience: int = params.get("lr_scheduler_patience", 5)
        self.device: str | None = params.get("device", None)
        self.random_state: int | None = params.get("random_state", None)

        # State
        self._module = None
        self._device = None
        self._n_match_features: int | None = None
        self._history: dict[int, np.ndarray] = {}  # player_id -> (M, HIST_FEAT_DIM_PROJECTED+1) sorted by date
        self._impute_medians: np.ndarray | None = None  # per-feature training medians for NaN passthrough

    # ------------------------------------------------------------------
    # History dict construction (called by runner before fit)
    # ------------------------------------------------------------------

    def set_history_features(self, df: pl.DataFrame) -> None:
        """Build the per-player history dict from a DataFrame of historical matches.

        Expected columns: see HISTORY_RAW_COLUMNS at module level.
        """
        missing = [c for c in HISTORY_RAW_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(
                f"history DataFrame missing required columns: {missing}"
            )

        player_ids, dates, features = _project_history_features(df)

        # Group by player_id and sort each group's rows by date ascending.
        # Use polars for the grouping itself (efficient on large data).
        history: dict[int, np.ndarray] = {}
        # Pack [date_int | features] per row
        packed = np.empty((player_ids.shape[0], 1 + HIST_FEAT_DIM_PROJECTED), dtype=np.float32)
        packed[:, 0] = dates.astype(np.float32)
        packed[:, 1:] = features

        # Sort by (player_id, date) to make per-player slicing contiguous
        sort_order = np.lexsort((dates, player_ids))
        sorted_pids = player_ids[sort_order]
        sorted_packed = packed[sort_order]

        # Find run boundaries
        if sorted_pids.size > 0:
            change_idx = np.concatenate(
                ([0], np.where(np.diff(sorted_pids) != 0)[0] + 1, [sorted_pids.size])
            )
            for start, end in zip(change_idx[:-1], change_idx[1:]):
                pid = int(sorted_pids[start])
                history[pid] = sorted_packed[start:end]

        self._history = history

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _split_id_cols(
        self, X: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Separate identifier columns from feature columns.

        Returns (X_features, player_ids, opp_ids, dates).
        Identifier columns are removed from X_features.
        """
        if self.player_id_col_idx is None or self.opp_id_col_idx is None or self.match_date_col_idx is None:
            raise ValueError(
                "SequenceModel requires player_id_col_idx, opp_id_col_idx, "
                "and match_date_col_idx in params (runner sets these)"
            )
        player_ids = X[:, self.player_id_col_idx].astype(np.int64)
        opp_ids = X[:, self.opp_id_col_idx].astype(np.int64)
        dates = X[:, self.match_date_col_idx].astype(np.int64)
        cols_to_remove = sorted(
            [self.player_id_col_idx, self.opp_id_col_idx, self.match_date_col_idx],
            reverse=True,
        )
        X_features = X
        for idx in cols_to_remove:
            X_features = np.delete(X_features, idx, axis=1)
        return X_features, player_ids, opp_ids, dates

    def _lookup_sequence(
        self, player_id: int, before_date: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Return (seq, mask) for one player's history strictly before `before_date`.

        seq: (seq_len, HIST_FEAT_DIM) float32, left-padded with zeros.
        mask: (seq_len,) float32, 1.0 for real / 0.0 for pad.

        The HIST_FEAT_DIM dimension is HIST_FEAT_DIM_PROJECTED + 1, with the
        appended `days_ago_log` feature in the LAST column.
        """
        seq = np.zeros((self.seq_len, HIST_FEAT_DIM), dtype=np.float32)
        mask = np.zeros(self.seq_len, dtype=np.float32)
        rows = self._history.get(player_id)
        if rows is None or rows.shape[0] == 0:
            return seq, mask
        # Binary search for the cutoff (first row with date >= before_date)
        dates = rows[:, 0]
        cutoff = int(np.searchsorted(dates, before_date, side="left"))
        if cutoff == 0:
            return seq, mask
        # Most recent N matches strictly before before_date
        start = max(0, cutoff - self.seq_len)
        slice_rows = rows[start:cutoff]
        n_real = slice_rows.shape[0]
        # Place at the END of seq (left-pad with zeros)
        pad_start = self.seq_len - n_real
        # The first column of slice_rows is date; remaining are features.
        # We rebuild as (n_real, HIST_FEAT_DIM) where:
        #   first HIST_FEAT_DIM_PROJECTED columns = the projected features
        #   last column = days_ago_log
        feat = slice_rows[:, 1:]
        days_ago = (before_date - slice_rows[:, 0]).astype(np.float32)
        days_ago_log = np.log1p(np.maximum(days_ago, 0.0)).reshape(-1, 1)
        # Normalize log days roughly to [0, 1] range — log1p(2000d) ≈ 7.6
        days_ago_log = days_ago_log / 8.0
        seq[pad_start:, :HIST_FEAT_DIM_PROJECTED] = feat
        seq[pad_start:, HIST_FEAT_DIM_PROJECTED:] = days_ago_log
        mask[pad_start:] = 1.0
        return seq, mask

    def _build_batch_sequences(
        self,
        player_ids: np.ndarray,
        opp_ids: np.ndarray,
        dates: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Vectorized batch lookup: returns (player_seqs, player_masks, opp_seqs, opp_masks)."""
        n = player_ids.shape[0]
        player_seqs = np.zeros((n, self.seq_len, HIST_FEAT_DIM), dtype=np.float32)
        player_masks = np.zeros((n, self.seq_len), dtype=np.float32)
        opp_seqs = np.zeros((n, self.seq_len, HIST_FEAT_DIM), dtype=np.float32)
        opp_masks = np.zeros((n, self.seq_len), dtype=np.float32)
        for i in range(n):
            player_seqs[i], player_masks[i] = self._lookup_sequence(int(player_ids[i]), int(dates[i]))
            opp_seqs[i], opp_masks[i] = self._lookup_sequence(int(opp_ids[i]), int(dates[i]))
        return player_seqs, player_masks, opp_seqs, opp_masks

    def _get_device(self):
        import torch

        if self.device is not None:
            if self.device == "cuda" and not torch.cuda.is_available():
                return torch.device("cpu")
            return torch.device(self.device)
        if torch.cuda.is_available():
            return torch.device("cuda")
        return torch.device("cpu")

    def _make_optimizer(self, params, lr: float):
        import torch

        if self.weight_decay > 0:
            return torch.optim.AdamW(params, lr=lr, weight_decay=self.weight_decay)
        return torch.optim.Adam(params, lr=lr)

    # ------------------------------------------------------------------
    # fit / predict_proba
    # ------------------------------------------------------------------

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        sample_weight: np.ndarray | None = None,
    ) -> None:
        import torch
        from torch.utils.data import DataLoader, TensorDataset

        if self.random_state is not None:
            torch.manual_seed(self.random_state)
        self._device = self._get_device()

        X_features, player_ids, opp_ids, dates = self._split_id_cols(X)
        # Median-impute NaN on the feature matrix (IDs/dates excluded). Fit
        # once on training, reuse at predict time.
        from mvp.model.models import _apply_median_imputer, _fit_median_imputer
        self._impute_medians = _fit_median_imputer(X_features)
        X_features = _apply_median_imputer(X_features, self._impute_medians)
        self._n_match_features = X_features.shape[1]

        SequenceModule = _make_sequence_module()
        self._module = SequenceModule(
            n_match_features=self._n_match_features,
            hist_feat_dim=HIST_FEAT_DIM,
            encoder_hidden=self.encoder_hidden,
            encoder_layers=self.encoder_layers,
            encoder_dropout=self.encoder_dropout,
            head_hidden=self.head_hidden,
            head_dropout=self.head_dropout,
            layer_norm=self.layer_norm,
        ).to(self._device)

        # Pre-compute sequences for all training rows (one-time cost; reused
        # across epochs). For very large datasets this can be batched/lazy
        # later, but at 250k rows × 20 × 29 × 4 bytes ≈ 580 MB it fits in RAM.
        player_seqs, player_masks, opp_seqs, opp_masks = self._build_batch_sequences(
            player_ids, opp_ids, dates,
        )

        # Temporal train/val split (last 15%)
        val_size = max(1, int(len(X_features) * 0.15))
        idx_train = slice(None, -val_size)
        idx_val = slice(-val_size, None)

        def _to_tensor(a, dtype):
            return torch.tensor(a, dtype=dtype, device=self._device)

        X_t = _to_tensor(X_features[idx_train], torch.float32)
        y_t = _to_tensor(y[idx_train], torch.float32).unsqueeze(1)
        ps_t = _to_tensor(player_seqs[idx_train], torch.float32)
        pm_t = _to_tensor(player_masks[idx_train], torch.float32)
        os_t = _to_tensor(opp_seqs[idx_train], torch.float32)
        om_t = _to_tensor(opp_masks[idx_train], torch.float32)
        if sample_weight is not None:
            w_t = _to_tensor(sample_weight[idx_train], torch.float32).unsqueeze(1)
        else:
            w_t = None

        X_v = _to_tensor(X_features[idx_val], torch.float32)
        y_v = _to_tensor(y[idx_val], torch.float32).unsqueeze(1)
        ps_v = _to_tensor(player_seqs[idx_val], torch.float32)
        pm_v = _to_tensor(player_masks[idx_val], torch.float32)
        os_v = _to_tensor(opp_seqs[idx_val], torch.float32)
        om_v = _to_tensor(opp_masks[idx_val], torch.float32)

        optimizer = self._make_optimizer(self._module.parameters(), self.learning_rate)
        scheduler = None
        if self.lr_scheduler == "plateau":
            scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
                optimizer,
                mode="min",
                factor=self.lr_scheduler_factor,
                patience=self.lr_scheduler_patience,
            )
        loss_fn = torch.nn.BCEWithLogitsLoss(reduction="none")

        tensors = [X_t, y_t, ps_t, pm_t, os_t, om_t]
        if w_t is not None:
            tensors.append(w_t)
        dataset = TensorDataset(*tensors)
        loader = DataLoader(dataset, batch_size=self.batch_size, shuffle=True)

        best_val_loss = float("inf")
        best_state = None
        wait = 0
        has_w = w_t is not None

        for _epoch in range(self.epochs):
            self._module.train()
            for batch in loader:
                xb, yb, psb, pmb, osb, omb = batch[:6]
                wb = batch[6] if has_w else None
                if self.label_smoothing > 0:
                    yb = yb * (1 - self.label_smoothing) + 0.5 * self.label_smoothing
                optimizer.zero_grad()
                pred = self._module(xb, psb, pmb, osb, omb)
                loss = loss_fn(pred, yb)
                if wb is not None:
                    loss = (loss * wb).mean()
                else:
                    loss = loss.mean()
                loss.backward()
                if self.grad_clip_norm is not None:
                    torch.nn.utils.clip_grad_norm_(
                        self._module.parameters(), max_norm=self.grad_clip_norm
                    )
                optimizer.step()

            self._module.eval()
            with torch.no_grad():
                val_pred = self._module(X_v, ps_v, pm_v, os_v, om_v)
                val_loss = loss_fn(val_pred, y_v).mean().item()

            if scheduler is not None:
                scheduler.step(val_loss)
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_state = {
                    k: v.cpu().clone() for k, v in self._module.state_dict().items()
                }
                wait = 0
            else:
                wait += 1
                if wait >= self.patience:
                    break

        if best_state is not None:
            self._module.load_state_dict(best_state)
        self._module.eval()

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        import torch

        if self._module is None:
            raise RuntimeError("SequenceModel not fitted")
        self._module.eval()

        X_features, player_ids, opp_ids, dates = self._split_id_cols(X)
        if self._impute_medians is not None:
            from mvp.model.models import _apply_median_imputer
            X_features = _apply_median_imputer(X_features, self._impute_medians)
        player_seqs, player_masks, opp_seqs, opp_masks = self._build_batch_sequences(
            player_ids, opp_ids, dates,
        )

        with torch.no_grad():
            X_t = torch.tensor(X_features, dtype=torch.float32, device=self._device)
            ps_t = torch.tensor(player_seqs, dtype=torch.float32, device=self._device)
            pm_t = torch.tensor(player_masks, dtype=torch.float32, device=self._device)
            os_t = torch.tensor(opp_seqs, dtype=torch.float32, device=self._device)
            om_t = torch.tensor(opp_masks, dtype=torch.float32, device=self._device)
            # Batch through to avoid OOM on large test sets
            probs = []
            bs = max(self.batch_size, 1024)
            for i in range(0, X_t.shape[0], bs):
                pred = self._module(X_t[i:i + bs], ps_t[i:i + bs], pm_t[i:i + bs], os_t[i:i + bs], om_t[i:i + bs])
                probs.append(torch.sigmoid(pred).cpu().numpy().squeeze(-1))
        return np.concatenate(probs, axis=0)

    # ------------------------------------------------------------------
    # Pickle support (don't serialize live torch module / device)
    # ------------------------------------------------------------------

    def __getstate__(self):
        state = self.__dict__.copy()
        # Drop live torch module and device; reconstruct on load
        state["_module_state_dict"] = (
            {k: v.cpu() for k, v in self._module.state_dict().items()}
            if self._module is not None
            else None
        )
        state["_module"] = None
        state["_device"] = None
        return state

    def __setstate__(self, state):
        module_state = state.pop("_module_state_dict", None)
        self.__dict__.update(state)
        self._module = None
        self._device = None
        if module_state is not None and self._n_match_features is not None:
            self._device = self._get_device()
            SequenceModule = _make_sequence_module()
            self._module = SequenceModule(
                n_match_features=self._n_match_features,
                hist_feat_dim=HIST_FEAT_DIM,
                encoder_hidden=self.encoder_hidden,
                encoder_layers=self.encoder_layers,
                encoder_dropout=self.encoder_dropout,
                head_hidden=self.head_hidden,
                head_dropout=self.head_dropout,
                layer_norm=self.layer_norm,
            ).to(self._device)
            self._module.load_state_dict(
                {k: v.to(self._device) for k, v in module_state.items()}
            )
            self._module.eval()
