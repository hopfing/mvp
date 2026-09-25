"""chain_arm: a source chain's per-arm outputs as features on a target frame.

Covers the splice honesty (per-fold train ends, forward rows after the
evaluation window, overlap and leak refusals), the refusals (missing stores
name their producer, model-kind stems), the join (player_ columns on the row's
player, opp_ on the opponent) and the salt."""

import math
import os
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from mvp.model.features import prior


def _arm_frame(
    uids: list[str], days: list[date], folds: list[int] | None,
    w1: float = 0.7,
) -> pl.DataFrame:
    """Two rows per match (A{i} and B{i} serving), as the stores write them."""
    rows = []
    for i, (uid, day) in enumerate(zip(uids, days, strict=True)):
        sides = ((f"A{i}", f"B{i}", 0.0), (f"B{i}", f"A{i}", 0.05))
        for server, returner, bump in sides:
            row = {
                "match_uid": uid, "server_id": server, "returner_id": returner,
                "effective_match_date": day, "scoreable": 1,
                "chain_fi_rate": 0.6 + bump, "chain_w1_prob": w1 + bump,
                "chain_w2_prob": 0.5 + bump,
            }
            if folds is not None:
                row["fold_idx"] = folds[i]
            rows.append(row)
    df = pl.DataFrame(rows).with_columns(pl.col("scoreable").cast(pl.Int8))
    if folds is not None:
        df = df.with_columns(pl.col("fold_idx").cast(pl.Int32))
    return df


def _source(tmp_path: Path) -> prior.PriorSource:
    src = prior.PriorSource(
        model="src_chain", config_path=tmp_path / "src_chain.yaml",
        fp="abc123abc123", eval_dir=tmp_path / "pe" / "abc123abc123",
        kind="projection", forward_train_end=date(2025, 12, 31),
    )
    src.eval_dir.mkdir(parents=True)
    return src


_UIDS = ["M0", "M1", "M2", "M3"]
_DAYS = [date(2025, 1, 5), date(2025, 1, 9), date(2025, 7, 2), date(2025, 7, 8)]
_FOLDS = [1, 1, 2, 2]


@pytest.fixture
def stores(tmp_path, monkeypatch):
    prior._cached_arm_frame.cache_clear()
    src = _source(tmp_path)
    _arm_frame(_UIDS, _DAYS, _FOLDS).write_parquet(src.fold_serve_arms)
    _arm_frame(["F0"], [date(2026, 2, 1)], None).write_parquet(src.serve_arms_forward)
    monkeypatch.setattr(prior, "resolve_prior", lambda m, *a, **kw: src)
    monkeypatch.setattr(prior, "ensure_prior_artifacts", lambda s, regenerate: None)
    yield src
    prior._cached_arm_frame.cache_clear()


class TestSplice:
    def test_fold_rows_are_dated_after_their_own_folds_train_end(self, stores):
        _, frame = prior.arm_frame("src_chain")
        fold = frame.filter(pl.col("match_uid").is_in(_UIDS))
        assert (fold["day"] > fold["arm_train_end"]).all()
        m2 = fold.filter(pl.col("match_uid") == "M2")
        assert (m2["arm_train_end"] == date(2025, 7, 1)).all()

    def test_forward_rows_train_end_is_the_config_end(self, stores):
        _, frame = prior.arm_frame("src_chain")
        f0 = frame.filter(pl.col("match_uid") == "F0")
        assert f0.height == 2
        assert (f0["arm_train_end"] == date(2025, 12, 31)).all()

    def test_overlap_between_the_stores_is_refused(self, stores):
        # M3's own (match, server) rows, re-dated into the forward window.
        pl.read_parquet(stores.fold_serve_arms).filter(
            pl.col("match_uid") == "M3"
        ).drop("fold_idx").with_columns(
            pl.lit(date(2026, 2, 1)).alias("effective_match_date")
        ).write_parquet(stores.serve_arms_forward)
        with pytest.raises(ValueError, match="in both the fold OOF and the forward"):
            prior.arm_frame("src_chain")

    def test_a_forward_row_inside_the_training_window_is_refused(self, stores):
        _arm_frame(["F1"], [date(2025, 6, 1)], None).write_parquet(
            stores.serve_arms_forward
        )
        with pytest.raises(ValueError, match="on/before their train end"):
            prior.arm_frame("src_chain")


class TestRefusals:
    def test_a_missing_fold_store_names_iid_project(self, stores):
        stores.fold_serve_arms.unlink()
        with pytest.raises(FileNotFoundError, match="iid-project src_chain"):
            prior.arm_frame("src_chain")

    def test_a_missing_forward_store_names_iid_backtest(self, stores):
        stores.serve_arms_forward.unlink()
        with pytest.raises(FileNotFoundError, match="iid-backtest src_chain"):
            prior.arm_frame("src_chain")

    def test_a_store_missing_a_column_is_refused(self, stores):
        pl.read_parquet(stores.fold_serve_arms).drop("chain_w2_prob").write_parquet(
            stores.fold_serve_arms
        )
        with pytest.raises(FileNotFoundError, match="fold_serve_arms.parquet"):
            prior.arm_frame("src_chain")

    def test_a_model_kind_stem_is_refused(self, tmp_path, monkeypatch):
        prior._cached_arm_frame.cache_clear()
        model_src = prior.PriorSource(
            model="some_model", config_path=tmp_path / "some_model.yaml",
            fp="def456def456", eval_dir=tmp_path / "me" / "def456def456",
        )
        monkeypatch.setattr(prior, "resolve_prior", lambda m, *a, **kw: model_src)
        with pytest.raises(ValueError, match="arm outputs only exist for projection"):
            prior.arm_frame("some_model")
        prior._cached_arm_frame.cache_clear()

    def test_the_arm_reader_resolves_with_arms_true(self, stores, monkeypatch):
        seen = {}

        def resolve(m, *a, **kw):
            seen.update(kw)
            return stores

        monkeypatch.setattr(prior, "resolve_prior", resolve)
        prior.arm_frame("src_chain")
        assert seen.get("arms") is True


class TestTransform:
    @staticmethod
    def _rows():
        # One row per perspective, as the engine hands a transform its frame.
        return pl.DataFrame({
            "match_uid": ["M0", "M0", "F0", "ZZ"],
            "player_id": ["A0", "B0", "B0", "Q"],
            "opp_id": ["B0", "A0", "A0", "R"],
        })

    def test_player_columns_join_on_the_row_player_and_opp_on_the_opponent(
        self, stores,
    ):
        out = prior._chain_arm_transform(self._rows(), "src_chain")
        r = out.row(0, named=True)  # M0 with A0 as the player
        assert r["player_chain_fi_rate"] == pytest.approx(0.6)
        assert r["opp_chain_fi_rate"] == pytest.approx(0.65)
        assert r["player_chain_w1_logit"] == pytest.approx(math.log(0.7 / 0.3))
        assert r["opp_chain_w1_logit"] == pytest.approx(math.log(0.75 / 0.25))
        mirror = out.row(1, named=True)  # the same match seen from B0
        assert mirror["player_chain_w2_logit"] == pytest.approx(r["opp_chain_w2_logit"])
        assert mirror["opp_chain_w2_logit"] == pytest.approx(r["player_chain_w2_logit"])

    def test_forward_rows_are_served(self, stores):
        out = prior._chain_arm_transform(self._rows(), "src_chain")
        assert out.row(2, named=True)["player_chain_w1_logit"] is not None

    def test_null_where_the_source_has_no_row(self, stores):
        out = prior._chain_arm_transform(self._rows(), "src_chain")
        r = out.row(3, named=True)
        assert all(r[c] is None for c in prior._ARM_OUTPUTS)

    def test_returns_the_keys_and_six_outputs_without_opp_id(self, stores):
        out = prior._chain_arm_transform(self._rows(), "src_chain")
        assert out.columns == ["match_uid", "player_id", *prior._ARM_OUTPUTS]
        assert "opp_id" not in out.columns
        assert out.height == 4

    def test_a_saturated_probability_is_clipped_before_the_logit(
        self, tmp_path, monkeypatch,
    ):
        prior._cached_arm_frame.cache_clear()
        src = _source(tmp_path)
        _arm_frame(_UIDS, _DAYS, _FOLDS, w1=1.0).write_parquet(src.fold_serve_arms)
        _arm_frame(["F0"], [date(2026, 2, 1)], None).write_parquet(
            src.serve_arms_forward
        )
        monkeypatch.setattr(prior, "resolve_prior", lambda m, *a, **kw: src)
        monkeypatch.setattr(prior, "ensure_prior_artifacts", lambda s, regenerate: None)
        out = prior._chain_arm_transform(self._rows(), "src_chain")
        eps = prior._LOGIT_EPS
        assert out["player_chain_w1_logit"][0] == pytest.approx(
            math.log((1 - eps) / eps)
        )
        prior._cached_arm_frame.cache_clear()

    def test_registered_with_both_sides_as_outputs(self):
        from mvp.model.registry import get_registry

        for c in prior._ARM_OUTPUTS:
            assert get_registry().transform_for_output(c) is not None, c


class TestSalt:
    def test_salt_tracks_the_arm_files(self, stores):
        before = prior._chain_arm_salt("src_chain")
        st = stores.fold_serve_arms.stat()
        os.utime(stores.fold_serve_arms, (st.st_atime, st.st_mtime + 100))
        assert prior._chain_arm_salt("src_chain") != before


class TestNaming:
    @pytest.mark.parametrize("spelling", [
        "chain_arm(model=src_chain)",
        "player_chain_w1_logit(model=src_chain)",
        "opp_chain_fi_rate(model=src_chain)",
        "player_chain_w2_logit_src_chain",
        "opp_chain_w1_logit_src_chain",
    ])
    def test_prior_stem_of_reads_every_arm_spelling(self, spelling):
        from mvp.model.prior_naming import prior_kind_of
        from mvp.model.prior_promotion import prior_stem_of

        assert prior_kind_of(spelling) == ("chain_arm", "src_chain")
        assert prior_stem_of(spelling) == "src_chain"
