"""Serving a projection-kind prior for pending matches.

The defect being pinned: a stage can declare `player_prior_logit(model=X)` for a
projection X that is not its offset. `_fill_prior_logit` binds exactly one column
(the offset's), and X's artifacts come from an evaluation that calls
`resolve_targets`, so a pending match never has a row. The column reached the
model as NaN on every live prediction while being present for nearly all of
training.
"""

import polars as pl
import pytest

from mvp.model.predictor import _fill_prior_column, _projection_priors_of


class TestFillPriorColumn:
    """The trained value wins; the injection only fills nulls.

    This is the OPPOSITE of `_fill_prior_logit`'s order, and the asymmetry is
    the whole point. Injected-wins is right for the offset — the upstream
    model's live probability is what the stage conditions on at bet time. Here
    it silently corrupts backtests: `backtest.py` predicts with
    `include_settled=True`, so the uid set contains settled matches whose column
    already holds the honest walk-forward OOF value. Overwriting those with a
    projection from a single fit through the config's `date_range.end` puts an
    IN-SAMPLE number in every fold at or before that date, in the direction that
    flatters the model.
    """

    def _frame(self, existing):
        return pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "player_id": ["a", "b"],
            "player_prior_logit_x": existing,
        })

    def test_existing_value_is_not_overwritten(self):
        df = self._frame([0.25, 0.75])
        out = _fill_prior_column(
            df, {("m1", "a"): 9.0, ("m2", "b"): 9.0}, "player_prior_logit_x"
        )
        assert out["player_prior_logit_x"].to_list() == [0.25, 0.75], (
            "injection overwrote a trained value — settled backtest rows would "
            "read an in-sample projection instead of walk-forward OOF"
        )

    def test_null_is_filled(self):
        df = self._frame([None, 0.75])
        out = _fill_prior_column(
            df, {("m1", "a"): 9.0, ("m2", "b"): 9.0}, "player_prior_logit_x"
        )
        assert out["player_prior_logit_x"].to_list() == [9.0, 0.75]

    def test_unkeyed_row_keeps_its_null(self):
        # A match the projection could not score (out of its domain) must stay
        # null rather than acquiring a value from somewhere else.
        df = self._frame([None, None])
        out = _fill_prior_column(df, {("m1", "a"): 9.0}, "player_prior_logit_x")
        assert out["player_prior_logit_x"].to_list() == [9.0, None]

    def test_absent_column_is_created(self):
        df = pl.DataFrame({"match_uid": ["m1"], "player_id": ["a"]})
        out = _fill_prior_column(df, {("m1", "a"): 1.5}, "player_prior_logit_x")
        assert out["player_prior_logit_x"].to_list() == [1.5]

    def test_empty_values_is_a_noop(self):
        df = self._frame([0.25, 0.75])
        assert _fill_prior_column(df, {}, "player_prior_logit_x").equals(df)

    def test_row_count_is_preserved(self):
        # The fill joins; a many-to-one would silently duplicate bet rows.
        df = self._frame([None, None])
        out = _fill_prior_column(
            df, {("m1", "a"): 1.0, ("m2", "b"): 2.0}, "player_prior_logit_x"
        )
        assert out.height == df.height


class TestPendingFrame:
    """The projection's own filters and collapse, without `resolve_targets`."""

    def _cfg(self, filters):
        from types import SimpleNamespace

        return SimpleNamespace(data=SimpleNamespace(filters=filters))

    def _df(self):
        return pl.DataFrame({
            "match_uid": ["m1", "m1", "m2", "m3"],
            "player_id": ["b", "a", "a", "a"],
            "opp_id": ["a", "b", "b", "b"],
            "best_of": [3, 3, 5, 3],
            "draw_type": ["singles", "singles", "singles", "doubles"],
        })

    def test_collapses_to_lower_player_id(self):
        # `_collapse_to_match_rows` defines the a/b convention the projection is
        # expressed in, and `p_match_win_a` indexes by it.
        from mvp.model.projection_serving import _pending_frame

        out = _pending_frame(self._cfg(None), self._df(), ["m1"])
        assert out.height == 1
        assert out["player_id"].to_list() == ["a"]

    def test_applies_the_projections_own_filters(self):
        from mvp.model.projection_serving import _pending_frame

        out = _pending_frame(
            self._cfg({"draw_type": "singles"}), self._df(), ["m1", "m3"]
        )
        assert out["match_uid"].to_list() == ["m1"]

    def test_best_of_five_is_dropped_by_the_configs_filter(self):
        # The projection never evaluated best_of 5 (its config filters to 3), so
        # a slam row must get no fill and keep its null rather than a value from
        # a domain the model never saw. The frame builder's own is_in([3, 5])
        # mirrors the runner and is deliberately the looser of the two.
        from mvp.model.projection_serving import _pending_frame

        assert _pending_frame(self._cfg({"best_of": 3}), self._df(), ["m2"]).height == 0
        assert _pending_frame(self._cfg(None), self._df(), ["m2"]).height == 1

    def test_scopes_to_the_requested_uids(self):
        from mvp.model.projection_serving import _pending_frame

        out = _pending_frame(self._cfg(None), self._df(), ["m3"])
        assert out["match_uid"].to_list() == ["m3"]


class TestProjectionPriorDetection:
    """Which declared priors need serving: projection-kind, and not the offset."""

    def test_offset_prior_is_excluded(self, monkeypatch):
        from types import SimpleNamespace

        monkeypatch.setattr(
            "mvp.model.features.prior.resolve_prior",
            lambda m: SimpleNamespace(kind="projection"),
        )
        cfg = SimpleNamespace(
            data=SimpleNamespace(filters=None),
            offset=SimpleNamespace(feature="player_prior_logit(model=lead)"),
            features=SimpleNamespace(compute_only=None, include=[
                "player_prior_logit(model=lead)",
                "player_prior_logit(model=proj)",
            ]),
        )
        # The offset is filled from the upstream model's live probability, so it
        # must not also be served here.
        assert _projection_priors_of(cfg) == {"proj": {"prior"}}

    def test_model_kind_priors_are_excluded(self, monkeypatch):
        from types import SimpleNamespace

        monkeypatch.setattr(
            "mvp.model.features.prior.resolve_prior",
            lambda m: SimpleNamespace(kind="model"),
        )
        cfg = SimpleNamespace(
            data=SimpleNamespace(filters=None),
            offset=None,
            features=SimpleNamespace(
                include=["player_prior_logit(model=other)"], compute_only=None,
            ),
        )
        assert _projection_priors_of(cfg) == {}

    def test_no_priors_declared(self):
        from types import SimpleNamespace

        cfg = SimpleNamespace(
            data=SimpleNamespace(filters=None), offset=None,
            features=SimpleNamespace(include=["player_melo_diff"], compute_only=None),
        )
        assert _projection_priors_of(cfg) == {}


class TestCheckServedProjector:
    """`mvp train` refuses rather than fitting half the artifact pair."""

    def test_missing_artifact_names_the_command_that_makes_both(
        self, monkeypatch, tmp_path
    ):
        from types import SimpleNamespace

        import mvp.model.projection_serving as ps
        from mvp.projection.iid import projection_run

        cfg_path = tmp_path / "proj.yaml"
        cfg_path.write_text("x: 1\n", encoding="utf-8")
        monkeypatch.setattr(
            ps, "_pending_frame", lambda *a, **k: None, raising=False,
        )
        monkeypatch.setattr(
            "mvp.model.features.prior.resolve_prior",
            lambda m: SimpleNamespace(
                config_path=cfg_path, eval_dir=tmp_path / "evals"
            ),
        )
        monkeypatch.setattr(
            "mvp.projection.iid.config.IIDProjectionConfig.from_file",
            staticmethod(lambda p: SimpleNamespace()),
        )
        monkeypatch.setattr(
            projection_run, "_load_artifact", lambda c, p, path=None: None
        )

        with pytest.raises(FileNotFoundError) as exc:
            ps.check_served_projector("proj")
        msg = str(exc.value)
        # Promotion (`mvp train`) is what produces the projector and the pmf
        # together; the message must not send a human to fit half the pair.
        assert "mvp train" in msg
        assert "iid-backtest" not in msg


class TestServingUidSet:
    """Which matches a projection prior is served for."""

    def _df(self):
        from datetime import datetime

        return pl.DataFrame({
            "match_uid": ["s1", "s1", "p1", "p1", "p2", "p2", "p3", "p3"],
            "player_id": ["a", "b", "a", "b", "c", "d", "e", "f"],
            "won": [True, False, None, None, None, None, None, None],
            "effective_match_date": [
                datetime(2026, 8, 1), datetime(2026, 8, 1),
                datetime(2026, 9, 5), datetime(2026, 9, 5),
                datetime(2026, 9, 6), datetime(2026, 9, 6),
                datetime(2025, 9, 6), datetime(2025, 9, 6),
            ],
            "tournament_id": ["t", "t", "t", "t", "u", "u", "t", "t"],
        })

    def test_historical_runs_serve_nothing(self):
        from mvp.model.predictor import _serving_uid_set

        # include_settled marks the lead backtest and the offline scripts: they
        # read the TRAINED column, so an injection there would be in-sample.
        assert _serving_uid_set(self._df(), include_settled=True) == []

    def test_pending_only(self):
        from mvp.model.predictor import _serving_uid_set

        assert _serving_uid_set(self._df(), include_settled=False) == ["p1", "p2", "p3"]

    def test_narrowed_by_uids_dates_and_tournaments(self):
        from datetime import datetime

        from mvp.model.predictor import _serving_uid_set

        df = self._df()
        assert _serving_uid_set(
            df, include_settled=False, match_uids={"p2", "s1"},
        ) == ["p2"]
        assert _serving_uid_set(
            df, include_settled=False,
            date_window=(datetime(2026, 9, 1), datetime(2026, 9, 5)),
        ) == ["p1"]
        # `won` is null forever on a schedule row that never resolved (p3, a
        # year old); tournament scoping is what keeps it off the live card.
        assert _serving_uid_set(
            df, include_settled=False, tournament_keys=[("t", 2026)],
        ) == ["p1"]


class TestScopeToTournaments:
    def test_semi_join_on_tournament_and_year(self):
        from datetime import datetime

        from mvp.model.predictor import _scope_to_tournaments

        df = pl.DataFrame({
            "match_uid": ["a", "b", "c"],
            "tournament_id": ["t", "t", "u"],
            "effective_match_date": [
                datetime(2026, 1, 1), datetime(2025, 1, 1), datetime(2026, 1, 1),
            ],
        })
        out = _scope_to_tournaments(df, [("t", 2026)])
        assert out["match_uid"].to_list() == ["a"]
        assert "_year" not in out.columns
        assert out.columns == df.columns


class TestProjectionPriorDetectionSites:
    """Every place a prior can enter the design matrix is scanned."""

    def _patch(self, monkeypatch, kinds: dict[str, str]):
        from types import SimpleNamespace

        def _resolve(m, **kw):
            if m not in kinds:
                raise FileNotFoundError(m)
            return SimpleNamespace(kind=kinds[m])

        monkeypatch.setattr("mvp.model.features.prior.resolve_prior", _resolve)

    def test_compute_only_filter_keys_and_ensemble_union(self, monkeypatch):
        from types import SimpleNamespace

        self._patch(
            monkeypatch, {"a": "projection", "b": "projection", "c": "projection"},
        )
        cfg = SimpleNamespace(
            data=SimpleNamespace(filters={"player_prior_logit_b": "not_null"}),
            offset=None,
            features=SimpleNamespace(
                include=[], compute_only=["player_prior_logit(model=a)"],
            ),
        )
        assert _projection_priors_of(
            cfg, ["player_prior_logit(model=c)"],
        ) == {"a": {"prior"}, "b": {"prior"}, "c": {"prior"}}

    def test_chain_outputs_are_served_from_their_output_spellings(self, monkeypatch):
        from types import SimpleNamespace

        self._patch(monkeypatch, {"a": "projection", "b": "projection"})
        cfg = SimpleNamespace(
            data=SimpleNamespace(filters=None), offset=None,
            features=SimpleNamespace(
                include=[
                    "player_chain_egames(model=a)",
                    "player_chain_w1_logit(model=b)",
                    "opp_chain_w1_logit(model=b)",
                    "chain_shape(model=b)",
                ],
                compute_only=None,
            ),
        )
        assert _projection_priors_of(cfg) == {
            "a": {"chain_shape"}, "b": {"chain_arm", "chain_shape"},
        }

    def test_an_offset_stem_still_gets_its_arm_and_shape_fills(self, monkeypatch):
        from types import SimpleNamespace

        self._patch(monkeypatch, {"t": "projection"})
        cfg = SimpleNamespace(
            data=SimpleNamespace(filters=None),
            offset=SimpleNamespace(feature="player_prior_logit(model=t)"),
            features=SimpleNamespace(
                include=[
                    "player_prior_logit(model=t)",
                    "player_chain_egames(model=t)",
                    "player_chain_fi_rate(model=t)",
                ],
                compute_only=None,
            ),
        )
        assert _projection_priors_of(cfg) == {"t": {"chain_shape", "chain_arm"}}

    def test_unresolvable_prior_is_loud(self, monkeypatch):
        from types import SimpleNamespace

        self._patch(monkeypatch, {})
        cfg = SimpleNamespace(
            data=SimpleNamespace(filters=None), offset=None,
            features=SimpleNamespace(
                include=["player_prior_logit(model=ghost)"], compute_only=None,
            ),
        )
        with pytest.raises(RuntimeError, match="cannot resolve declared prior 'ghost'"):
            _projection_priors_of(cfg)


# --- chain into chain: serving arm values for pending matches ------------------

import math  # noqa: E402
from types import SimpleNamespace  # noqa: E402

import numpy as np  # noqa: E402


def _chain_cfg(arm_offset=None, include=(), filters=None):
    from mvp.projection.iid.config import IIDProjectionConfig

    return IIDProjectionConfig.model_validate({
        "data": {
            "date_range": {"start": "2024-01-01", "end": "2025-12-31"},
            "filters": filters or {},
        },
        "features": {"include": ["pts_service_won_pct(days=90)", *include]},
        "serve_model": {
            "type": "two_level", "model_type": "xgboost",
            "arm_offset": arm_offset or {},
        },
    })


def _pending_df():
    """Two pending matches, both perspectives; ids sort `a` < `b`."""
    return pl.DataFrame({
        "match_uid": ["m1", "m1", "m2", "m2", "old", "old"],
        "player_id": ["a1", "b1", "a2", "b2", "a3", "b3"],
        "opp_id": ["b1", "a1", "b2", "a2", "b3", "a3"],
        "best_of": [3] * 6,
        "won": [None, None, None, None, True, False],
    })


class _ArmsModel:
    """predict_arms with per-server values; optionally a function of an input
    column, so a test can see that column was filled before prediction."""

    parity_columns: list[str] = []

    def __init__(self, base: float, reads: str | None = None):
        self.base, self.reads = base, reads
        self.seen: list[pl.DataFrame] = []

    def predict_arms(self, pending):
        self.seen.append(pending)
        bump = pending[self.reads].to_numpy() if self.reads else np.zeros(len(pending))

        def side(server, returner, extra):
            return pending.select(
                "match_uid", pl.col(server).alias("server_id"),
                pl.col(returner).alias("returner_id"),
            ).with_columns(
                chain_fi_rate=pl.Series(self.base + extra + bump),
                chain_w1_prob=pl.Series(np.full(len(pending), 0.7) + extra),
                chain_w2_prob=pl.Series(np.full(len(pending), 0.5) + extra),
            )

        return pl.concat([
            side("player_id", "opp_id", 0.0), side("opp_id", "player_id", 0.05),
        ])


def _install(monkeypatch, configs: dict, models: dict):
    import mvp.model.projection_serving as ps

    monkeypatch.setattr(ps, "_config_of", lambda stem: configs[stem])
    monkeypatch.setattr(
        ps, "_load_source",
        lambda stem: (configs[stem], SimpleNamespace(serve_model=models[stem])),
    )
    return ps


class TestPendingArmValues:
    def test_both_servers_per_pending_match_in_the_transforms_units(self, monkeypatch):
        ps = _install(monkeypatch, {"src": _chain_cfg()}, {"src": _ArmsModel(0.6)})
        vals = ps.pending_arm_values("src", ["m1", "m2"], _pending_df())
        assert set(vals) == {("m1", "a1"), ("m1", "b1"), ("m2", "a2"), ("m2", "b2")}
        fi, w1, w2 = vals[("m1", "b1")]
        assert fi == pytest.approx(0.65)
        assert w1 == pytest.approx(math.log(0.75 / 0.25))
        assert w2 == pytest.approx(math.log(0.55 / 0.45))

    def test_no_uids_loads_nothing(self, monkeypatch):
        import mvp.model.projection_serving as ps

        monkeypatch.setattr(
            ps, "_load_source",
            lambda s: (_ for _ in ()).throw(AssertionError("loaded")),
        )
        assert ps.pending_arm_values("src", [], _pending_df()) == {}


class TestFillPendingArms:
    _S_COL = "player_chain_fi_rate_src"

    def _chain(self, monkeypatch):
        """S -> T (arm offset on S's first-in) -> U (arm offset on T's)."""
        configs = {
            "src": _chain_cfg(),
            "tgt": _chain_cfg({"first_in": "player_chain_fi_rate(model=src)"}),
            "top": _chain_cfg({"first_in": "player_chain_fi_rate(model=tgt)"}),
        }
        models = {
            "src": _ArmsModel(0.6),
            # T's arms read S's filled column: null there would propagate.
            "tgt": _ArmsModel(0.0, reads=self._S_COL),
            "top": _ArmsModel(0.9),
        }
        return _install(monkeypatch, configs, models), models

    def _df(self):
        null = pl.lit(None, dtype=pl.Float64)
        return _pending_df().with_columns(
            null.alias(self._S_COL), null.alias("opp_chain_fi_rate_src"),
            null.alias("player_chain_fi_rate_tgt"),
            null.alias("opp_chain_fi_rate_tgt"),
        )

    def test_a_chain_of_three_fills_the_sources_inputs_before_it_predicts(
        self, monkeypatch,
    ):
        ps, models = self._chain(monkeypatch)
        out = ps.fill_pending_arms("top", ["m1", "m2"], self._df())
        # T predicted on a frame whose S column was filled (0.6 on the a side).
        assert models["tgt"].seen[0][self._S_COL].to_list() == [0.6, 0.6]
        row = out.filter(pl.col("player_id") == "a1").row(0, named=True)
        assert row["player_chain_fi_rate_tgt"] == pytest.approx(0.6)

    def test_the_opp_pass_keys_on_the_opponent(self, monkeypatch):
        ps, _ = self._chain(monkeypatch)
        out = ps.fill_pending_arms("tgt", ["m1"], self._df())
        a = out.filter(pl.col("player_id") == "a1").row(0, named=True)
        b = out.filter(pl.col("player_id") == "b1").row(0, named=True)
        assert a[self._S_COL] == pytest.approx(0.6)
        assert a["opp_chain_fi_rate_src"] == pytest.approx(0.65)
        assert b[self._S_COL] == pytest.approx(0.65)
        assert b["opp_chain_fi_rate_src"] == pytest.approx(0.6)

    def test_settled_rows_are_untouched(self, monkeypatch):
        ps, _ = self._chain(monkeypatch)
        out = ps.fill_pending_arms("tgt", ["m1"], self._df())
        assert out.filter(pl.col("match_uid") == "old")[self._S_COL].null_count() == 2

    def test_an_offset_source_is_injected_wins(self, monkeypatch):
        ps, _ = self._chain(monkeypatch)
        df = self._df().with_columns(pl.lit(0.1).alias(self._S_COL))
        out = ps.fill_pending_arms("tgt", ["m1"], df)
        a1 = out.filter(pl.col("player_id") == "a1")
        assert a1[self._S_COL][0] == pytest.approx(0.6)

    def test_a_plain_chain_arm_source_is_existing_wins(self, monkeypatch):
        configs = {
            "src": _chain_cfg(),
            "tgt": _chain_cfg(include=["player_chain_fi_rate(model=src)"]),
        }
        ps = _install(
            monkeypatch, configs,
            {"src": _ArmsModel(0.6), "tgt": _ArmsModel(0.0)},
        )
        df = self._df().with_columns(
            pl.when(pl.col("player_id") == "a1").then(0.1).otherwise(None)
            .alias(self._S_COL)
        )
        out = ps.fill_pending_arms("tgt", ["m1"], df)
        assert out.filter(pl.col("player_id") == "a1")[self._S_COL][0] == 0.1
        b1 = out.filter(pl.col("player_id") == "b1")
        assert b1[self._S_COL][0] == pytest.approx(0.65)


class TestServingRequirementsChain:
    def test_a_chain_of_three_unions_every_projections_specs(self, monkeypatch):
        configs = {
            "src": _chain_cfg(include=["player_glicko_rd"]),
            "tgt": _chain_cfg(
                {"first_in": "player_chain_fi_rate(model=src)"},
                include=["player_elo"],
            ),
            "top": _chain_cfg({"win_first": "player_chain_w1_logit(model=tgt)"}),
        }
        ps = _install(monkeypatch, configs, {})
        specs, _cols = ps.serving_requirements("top")
        for spec in (
            "player_glicko_rd", "player_elo",
            "player_chain_fi_rate(model=src)", "player_chain_w1_logit(model=tgt)",
        ):
            assert spec in specs, spec

    def test_a_projection_without_arm_sources_is_its_own(self, monkeypatch):
        ps = _install(monkeypatch, {"src": _chain_cfg(include=["player_elo"])}, {})
        specs, _ = ps.serving_requirements("src")
        assert specs == ["pts_service_won_pct(days=90)", "player_elo"]


class TestPendingMatchWinLogitsFillsFirst:
    def test_a_row_with_a_null_arm_input_survives_the_not_null_filter(
        self, monkeypatch,
    ):
        import mvp.model.projection_serving as ps

        col = "player_chain_fi_rate_src"
        configs = {
            "src": _chain_cfg(),
            "tgt": _chain_cfg({"first_in": "player_chain_fi_rate(model=src)"}),
        }
        projected: list[pl.DataFrame] = []

        class _Projector:
            serve_model = _ArmsModel(0.0)

            def project(self, pending):
                projected.append(pending)
                return SimpleNamespace(
                    distribution=SimpleNamespace(
                        p_match_win_a=np.full(len(pending), 0.6),
                    ),
                )

        monkeypatch.setattr(ps, "_config_of", lambda stem: configs[stem])
        monkeypatch.setattr(
            ps, "_load_source",
            lambda stem: (
                configs[stem],
                _Projector() if stem == "tgt"
                else SimpleNamespace(serve_model=_ArmsModel(0.6)),
            ),
        )
        monkeypatch.setattr(
            ps, "_forward_calibrator",
            lambda stem: SimpleNamespace(transform=lambda p: p),
        )
        df = _pending_df().with_columns(
            pl.lit(None, dtype=pl.Float64).alias(col),
            pl.lit(None, dtype=pl.Float64).alias("opp_chain_fi_rate_src"),
        )
        fill = ps.pending_match_win_logits("tgt", ["m1", "m2"], df)
        assert set(k[0] for k in fill) == {"m1", "m2"}
        assert projected[0][col].to_list() == [0.6, 0.6]


class TestFillProjectionColumns:
    """Both fill sites serve every projection column an entry declares, one
    projection per stem, existing-wins."""

    def _df(self):
        null = pl.lit(None, dtype=pl.Float64)
        return pl.DataFrame({
            "match_uid": ["m1", "m1", "m2", "m2", "old", "old"],
            "player_id": ["a1", "b1", "a2", "b2", "a3", "b3"],
            "opp_id": ["b1", "a1", "b2", "a2", "b3", "a3"],
            "best_of": [3] * 6,
        }).with_columns(
            null.alias("player_prior_logit_t"),
            pl.when(pl.col("match_uid") == "old").then(9.0).otherwise(None)
            .alias("player_chain_egames_t"),
            null.alias("player_chain_hold_asym_t"),
            null.alias("player_chain_fi_rate_t"),
            null.alias("opp_chain_fi_rate_t"),
        )

    def _patch(self, monkeypatch):
        import mvp.model.projection_serving as ps
        from tests.model.features.test_chain_shape import _fake_out

        calls = {"projections": 0}
        pending = pl.DataFrame({
            "match_uid": ["m1", "m2"], "player_id": ["a1", "a2"],
            "opp_id": ["b1", "b2"],
        })

        def projection(stem, uids, df):
            calls["projections"] += 1
            return pending, _fake_out()

        monkeypatch.setattr(ps, "_pending_projection", projection)
        monkeypatch.setattr(
            ps, "_forward_calibrator",
            lambda stem: SimpleNamespace(transform=lambda p: p),
        )
        monkeypatch.setattr(
            ps, "pending_arm_values",
            lambda stem, uids, df: {
                ("m1", "a1"): (0.61, 0.0, 0.0), ("m1", "b1"): (0.58, 0.0, 0.0),
            },
        )
        return calls

    def test_prior_shape_and_arm_columns_from_one_projection(self, monkeypatch):
        from mvp.model.predictor import _fill_projection_columns

        calls = self._patch(monkeypatch)
        out = _fill_projection_columns(
            self._df(), {"t": {"prior", "chain_shape", "chain_arm"}}, ["m1", "m2"],
        )
        assert calls["projections"] == 1
        a1 = out.filter(pl.col("player_id") == "a1").row(0, named=True)
        b1 = out.filter(pl.col("player_id") == "b1").row(0, named=True)
        assert a1["player_prior_logit_t"] == pytest.approx(-b1["player_prior_logit_t"])
        assert a1["player_chain_egames_t"] == pytest.approx(2.0)
        assert b1["player_chain_egames_t"] == pytest.approx(2.0)
        # Antisymmetric: negated on the mirror row.
        assert a1["player_chain_hold_asym_t"] == pytest.approx(0.3)
        assert b1["player_chain_hold_asym_t"] == pytest.approx(-0.3)
        # Arm columns: player_ on the row's player, opp_ on the opponent.
        assert a1["player_chain_fi_rate_t"] == pytest.approx(0.61)
        assert a1["opp_chain_fi_rate_t"] == pytest.approx(0.58)
        assert b1["player_chain_fi_rate_t"] == pytest.approx(0.58)

    def test_a_settled_row_keeps_the_transforms_value(self, monkeypatch):
        from mvp.model.predictor import _fill_projection_columns

        self._patch(monkeypatch)
        out = _fill_projection_columns(
            self._df(), {"t": {"chain_shape"}}, ["m1", "m2"],
        )
        assert out.filter(pl.col("match_uid") == "old")[
            "player_chain_egames_t"
        ].to_list() == [9.0, 9.0]

    def test_an_arm_only_stem_projects_nothing(self, monkeypatch):
        from mvp.model.predictor import _fill_projection_columns

        calls = self._patch(monkeypatch)
        _fill_projection_columns(self._df(), {"t": {"chain_arm"}}, ["m1"])
        assert calls["projections"] == 0

    def test_both_fill_sites_use_it(self):
        import inspect

        import mvp.model.predictor as pred

        src = inspect.getsource(pred)
        assert src.count("_fill_projection_columns(") == 3  # def + two sites
        assert "pending_match_win_logits(" not in src
