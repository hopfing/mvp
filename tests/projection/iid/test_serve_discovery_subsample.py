"""FS train subsampling, point path and chain path: the count cap and the fraction cap.

The count caps (`fs_train_subsample`, `fs_match_subsample`) put the same
ceiling on every fold, which flattens an expanding window into a fixed-volume
one. The fraction caps (`fs_train_subsample_frac`, `fs_match_subsample_frac`)
thin every fold by the same ratio, so fold sizes keep the window's ratios. Both
draw without replacement from a fixed seed and sort back into time order; test
indices are never touched.

The draw is exercised through the shared helper and through each method, on
synthetic index lists, with a stand-in `self` that carries only the config
fields the method reads. No data, no B:/.
"""

from types import SimpleNamespace

import pytest
import yaml

from mvp.projection.iid.config import ServeDiscoveryConfig
from mvp.projection.iid.serve_discovery import (
    ServeDiscoverySelector,
    _subsample_train_splits,
)
from tests.projection.iid.test_serve_discovery_prefit import (
    SHAPES,
    _two_level_config,
)

# An expanding window: train grows fold by fold, test is the next block.
SPLITS = [
    (list(range(0, 100)), list(range(100, 150))),
    (list(range(0, 200)), list(range(200, 250))),
    (list(range(0, 400)), list(range(400, 450))),
]


def _draw(splits, *, cap=None, frac=None, seed=42):
    return _subsample_train_splits(splits, cap=cap, frac=frac, seed=seed, label="t")


class TestDraw:
    def test_no_cap_returns_the_splits_untouched(self):
        assert _draw(SPLITS) is SPLITS

    def test_count_cap_leaves_small_folds_and_flattens_the_rest(self):
        out = _draw(SPLITS, cap=150)
        assert [len(t) for t, _ in out] == [100, 150, 150]

    def test_fraction_cap_keeps_the_ratios_between_folds(self):
        out = _draw(SPLITS, frac=0.5)
        assert [len(t) for t, _ in out] == [50, 100, 200]

    def test_fraction_cap_never_empties_a_fold(self):
        out = _draw([([0, 1, 2], [3])], frac=0.01)
        assert [len(t) for t, _ in out] == [1]

    def test_test_indices_are_unchanged_under_both_forms(self):
        for kw in ({"cap": 150}, {"frac": 0.5}):
            out = _draw(SPLITS, **kw)
            assert [te for _, te in out] == [te for _, te in SPLITS]

    def test_the_draw_is_a_sorted_subset_and_repeats_under_the_seed(self):
        a = _draw(SPLITS, frac=0.5)
        b = _draw(SPLITS, frac=0.5)
        assert a == b
        for (tr, _), (full, _) in zip(a, SPLITS):
            assert tr == sorted(tr)
            assert set(tr) <= set(full)

    def test_a_different_seed_draws_differently(self):
        a = _draw(SPLITS, frac=0.5, seed=1)
        b = _draw(SPLITS, frac=0.5, seed=2)
        assert a != b


def _fake_self(**fields):
    base = dict(
        fs_train_subsample=None, fs_train_subsample_frac=None,
        fs_match_subsample=None, fs_match_subsample_frac=None,
        fs_subsample_seed=42,
    )
    base.update(fields)
    return SimpleNamespace(config=SimpleNamespace(**base))


class TestEachPathReadsItsOwnKnobs:
    def test_point_path_uses_the_train_knobs_only(self):
        fake = _fake_self(fs_train_subsample_frac=0.5, fs_match_subsample=10)
        out = ServeDiscoverySelector._maybe_subsample_splits(fake, SPLITS)
        assert [len(t) for t, _ in out] == [50, 100, 200]

    def test_chain_path_uses_the_match_knobs_only(self):
        fake = _fake_self(fs_match_subsample_frac=0.5, fs_train_subsample=10)
        out = ServeDiscoverySelector._maybe_subsample_match_splits(fake, SPLITS)
        assert [len(t) for t, _ in out] == [50, 100, 200]

    def test_count_form_still_works_on_both_paths(self):
        p = ServeDiscoverySelector._maybe_subsample_splits(
            _fake_self(fs_train_subsample=150), SPLITS,
        )
        c = ServeDiscoverySelector._maybe_subsample_match_splits(
            _fake_self(fs_match_subsample=150), SPLITS,
        )
        assert [len(t) for t, _ in p] == [100, 150, 150]
        assert [len(t) for t, _ in c] == [100, 150, 150]


def _config_with(tmp_path, **extra) -> ServeDiscoveryConfig:
    path = _two_level_config(tmp_path, SHAPES["joint"])
    cfg = yaml.safe_load(open(path, encoding="utf-8"))
    cfg.update(extra)
    return ServeDiscoveryConfig.from_yaml(yaml.safe_dump(cfg, sort_keys=False))


PAIRS = [
    ("fs_train_subsample", "fs_train_subsample_frac"),
    ("fs_match_subsample", "fs_match_subsample_frac"),
]


class TestConfig:
    @pytest.mark.parametrize("count_name,frac_name", PAIRS)
    def test_fraction_inside_the_open_interval_is_accepted(
        self, tmp_path, count_name, frac_name,
    ):
        cfg = _config_with(tmp_path, **{frac_name: 0.5})
        assert getattr(cfg, frac_name) == 0.5
        assert getattr(cfg, count_name) is None

    @pytest.mark.parametrize("count_name,frac_name", PAIRS)
    @pytest.mark.parametrize("bad", [0.0, 1.0, -0.2, 1.5])
    def test_fraction_outside_the_open_interval_is_rejected(
        self, tmp_path, count_name, frac_name, bad,
    ):
        with pytest.raises(ValueError, match=rf"{frac_name} must be in \(0, 1\)"):
            _config_with(tmp_path, **{frac_name: bad})

    @pytest.mark.parametrize("count_name,frac_name", PAIRS)
    def test_count_and_fraction_together_are_rejected(
        self, tmp_path, count_name, frac_name,
    ):
        with pytest.raises(
            ValueError, match=f"{count_name} and {frac_name} are mutually exclusive",
        ):
            _config_with(tmp_path, **{count_name: 6250, frac_name: 0.5})
