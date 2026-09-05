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
        assert _projection_priors_of(cfg) == ["proj"]

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
        assert _projection_priors_of(cfg) == []

    def test_no_priors_declared(self):
        from types import SimpleNamespace

        cfg = SimpleNamespace(
            data=SimpleNamespace(filters=None), offset=None,
            features=SimpleNamespace(include=["player_melo_diff"], compute_only=None),
        )
        assert _projection_priors_of(cfg) == []


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
        ) == ["a", "b", "c"]

    def test_chain_shape_is_not_a_served_prior(self, monkeypatch):
        from types import SimpleNamespace

        self._patch(monkeypatch, {"a": "projection"})
        cfg = SimpleNamespace(
            data=SimpleNamespace(filters=None), offset=None,
            features=SimpleNamespace(
                include=["chain_shape(model=a)"], compute_only=None,
            ),
        )
        # Its columns are shape scalars this path does not produce.
        assert _projection_priors_of(cfg) == []

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
