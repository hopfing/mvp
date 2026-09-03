"""Granular feature-cache invalidation (plan: 2026-09-03, rev 5).

Covers: the AST import closure (all scopes, first-party only, cycles), the
per-feature code signatures (module edits, cross-module helper edits, dep
walk, factory closures), _is_cached's per-entry semantics, manifest v2
migration + corrupt-manifest tolerance + atomic writes, and the salt lint
(every external-file-reading transform declares a cache_salt)."""

import json
import re

import pytest

import mvp.model.features  # noqa: F401  (populate the registry)
from mvp.model import engine as eng
from mvp.model.engine import (
    _first_party_imports,
    _module_closure_hash,
    feature_code_signature,
)
from mvp.model.registry import get_registry


@pytest.fixture(autouse=True)
def _fresh_sig_caches():
    eng._clear_signature_caches()
    yield
    eng._clear_signature_caches()


class TestImportClosure:
    def test_module_local_helpers_are_covered_by_module_grain(self):
        # style features call style.py's _rolling; whole-module hashing means
        # no import edge is even needed — closure of style.py includes its own
        # source by construction.
        assert "mvp.model.features.style" in self._closure("mvp.model.features.style")

    @staticmethod
    def _closure(root):
        # reconstruct the visited set the closure hash walks
        visited, stack = set(), [root]
        while stack:
            m = stack.pop()
            if m in visited:
                continue
            visited.add(m)
            stack.extend(_first_party_imports(m) - visited)
        return visited

    def test_cross_module_import_edges(self):
        # round-2 findings, verbatim
        assert "mvp.model.features.elo" in self._closure("mvp.model.features.tier")
        assert "mvp.model.features._score_helpers" in self._closure(
            "mvp.model.features.score_depth"
        )
        assert "mvp.common.chain_shape" in self._closure("mvp.model.features.prior")

    def test_function_body_lazy_imports_are_reached(self):
        # round-3 finding: prior.py imports its calibrator only at call sites
        assert "mvp.model.calibration" in _first_party_imports(
            "mvp.model.features.prior"
        )

    def test_any_first_party_package_not_just_two_prefixes(self):
        # round-3 finding: market.py imports mvp.oddspapi.paths
        imports = _first_party_imports("mvp.model.features.market")
        assert any(m.startswith("mvp.oddspapi") for m in imports)

    def test_third_party_imports_excluded(self):
        assert not any(
            m.startswith(("polars", "numpy"))
            for m in _first_party_imports("mvp.model.features.style")
        )

    def test_closure_hash_is_stable(self):
        a = _module_closure_hash("mvp.model.features.style")
        eng._clear_signature_caches()
        b = _module_closure_hash("mvp.model.features.style")
        assert a == b


class TestFeatureSignatures:
    def test_module_edit_invalidates_own_features_only(self, monkeypatch):
        reg = get_registry()
        before_style = feature_code_signature(reg, "style_easy_hold_pct")
        before_elo = feature_code_signature(reg, "elo")
        eng._clear_signature_caches()
        orig = eng._module_source

        def patched(modname):
            src = orig(modname)
            if modname == "mvp.model.features.style":
                return src + "\n# edit"
            return src

        monkeypatch.setattr(eng, "_module_source", patched)
        assert feature_code_signature(reg, "style_easy_hold_pct") != before_style
        assert feature_code_signature(reg, "elo") == before_elo

    def test_cross_module_helper_edit_invalidates_importers(self, monkeypatch):
        reg = get_registry()
        before = feature_code_signature(reg, "total_games_won")  # score_depth family
        eng._clear_signature_caches()
        orig = eng._module_source

        def patched(modname):
            src = orig(modname)
            if modname == "mvp.model.features._score_helpers":
                return src + "\n# fix"
            return src

        monkeypatch.setattr(eng, "_module_source", patched)
        assert feature_code_signature(reg, "total_games_won") != before

    def test_base_edit_invalidates_diff_via_dep_walk(self, monkeypatch):
        # a diff's defining module is registry.py (factory closure); only the
        # dep walk connects it to its base's module
        reg = get_registry()
        before = feature_code_signature(reg, "win_pct_diff")
        eng._clear_signature_caches()
        orig = eng._module_source

        def patched(modname):
            src = orig(modname)
            if modname == "mvp.model.features.win_rate":
                return src + "\n# base edit"
            return src

        monkeypatch.setattr(eng, "_module_source", patched)
        assert feature_code_signature(reg, "win_pct_diff") != before

    def test_registry_edit_invalidates_factory_closures(self, monkeypatch):
        reg = get_registry()
        before = feature_code_signature(reg, "win_pct_diff")
        eng._clear_signature_caches()
        orig = eng._module_source

        def patched(modname):
            src = orig(modname)
            if modname == "mvp.model.registry":
                return src + "\n# factory fix"
            return src

        monkeypatch.setattr(eng, "_module_source", patched)
        assert feature_code_signature(reg, "win_pct_diff") != before

    def test_unrelated_module_edit_leaves_signature_alone(self, monkeypatch):
        # "unrelated" must be OUTSIDE the victim's import closure (rev-2's
        # test would have certified the tier<-elo bug as correct behavior)
        reg = get_registry()
        before = feature_code_signature(reg, "style_easy_hold_pct")
        eng._clear_signature_caches()
        orig = eng._module_source

        def patched(modname):
            src = orig(modname)
            if modname == "mvp.model.features.market":
                return src + "\n# edit"
            return src

        monkeypatch.setattr(eng, "_module_source", patched)
        assert feature_code_signature(reg, "style_easy_hold_pct") == before


class TestManifest:
    def _engine(self, tmp_path):
        # __init__ needs a matches path but only hashes it lazily; cache-layer
        # methods run without touching it
        e = object.__new__(eng.FeatureEngine)
        e.cache_dir = tmp_path
        e.cache_dir.mkdir(exist_ok=True)
        e._registry = get_registry()
        e._manifest_path = tmp_path / "manifest.json"
        e._manifest = e._load_manifest()
        e._feature_timings = []
        e._stale_code_specs = set()
        return e

    def test_v1_manifest_migrates_to_full_rebuild(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            json.dumps({"cache_key": "old", "features": {"player_x": {}}}),
            encoding="utf-8",
        )
        e = self._engine(tmp_path)
        assert e._manifest["schema"] == 2
        assert e._manifest["features"] == {}

    def test_corrupt_manifest_starts_fresh_not_crash(self, tmp_path):
        (tmp_path / "manifest.json").write_text("{truncated", encoding="utf-8")
        e = self._engine(tmp_path)
        assert e._manifest["schema"] == 2

    def test_save_is_atomic_no_tmp_left(self, tmp_path):
        e = self._engine(tmp_path)
        e._save_manifest()
        assert (tmp_path / "manifest.json").exists()
        assert not (tmp_path / "manifest.json.tmp").exists()
        assert json.loads((tmp_path / "manifest.json").read_text())["schema"] == 2

    def test_is_cached_per_entry_code(self, tmp_path):
        import polars as pl

        e = self._engine(tmp_path)
        df = pl.DataFrame({
            "match_uid": ["m"], "player_id": ["p"], "player_win_pct": [0.5],
        })
        e._cache_feature("player_win_pct", df, ["player_win_pct"], "DK", "win_pct")
        assert e._is_cached("player_win_pct", "DK", "win_pct")
        # wrong data key -> global miss
        assert not e._is_cached("player_win_pct", "DK2", "win_pct")
        # stale code -> per-entry miss, tracked
        e._manifest["features"]["player_win_pct"]["code"] = "stale"
        assert not e._is_cached("player_win_pct", "DK", "win_pct")
        assert ("player_win_pct", "win_pct") in e._stale_code_specs

    def test_salt_mismatch_recomputes_independent_of_code(self, tmp_path):
        # plan test workflow: an external-artifact rebuild (mtime salt moves)
        # must miss even when the code signature is identical — the mechanism
        # market.py/lead_prior.py's build-item-5 salts depend on
        import polars as pl

        e = self._engine(tmp_path)
        df = pl.DataFrame({
            "match_uid": ["m"], "player_id": ["p"], "player_market_prob": [0.5],
        })
        e._cache_feature(
            "player_market_prob", df, ["player_market_prob"], "DK",
            "market_prior", salt="100",
        )
        assert e._is_cached("player_market_prob", "DK", "market_prior", salt="100")
        assert not e._is_cached("player_market_prob", "DK", "market_prior", salt="200")
        # salt-agnostic call sites (plain features) still hit
        assert e._is_cached("player_market_prob", "DK", "market_prior")


class TestImportTimingIndependence:
    """The implementation-review blocker: signatures must be a pure function
    of the code ON DISK, never of which modules this process imported."""

    def test_unimported_module_source_resolves_from_disk(self):
        import sys

        # pick a first-party module prior.py lazy-imports; if the test
        # process already imported it, the disk-read property still holds —
        # assert source is non-empty EITHER way, and specifically that the
        # resolution does not consult sys.modules by checking a module we
        # remove from it.
        modname = "mvp.model.backtest"
        src = eng._module_source(modname)
        assert "def " in src  # real source, not ""
        saved = sys.modules.pop(modname, None)
        try:
            assert eng._module_source(modname) == src
        finally:
            if saved is not None:
                sys.modules[modname] = saved

    def test_lazy_imported_modules_join_the_closure(self):
        # prior.py imports these only inside function bodies; a sys.modules-
        # gated walk truncated them silently (the demonstrated false-VALID)
        closure = TestImportClosure._closure("mvp.model.features.prior")
        assert "mvp.model.backtest" in closure
        assert "mvp.model.calibration" in closure

    def test_signature_stable_across_import_state(self, tmp_path):
        # real files on disk: module a lazily imports module b inside a
        # function; b is NEVER imported. Editing b's file must change a's
        # closure hash — computed purely from disk.
        import sys

        pkg = tmp_path / "mvp_sigtest"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("", encoding="utf-8")
        (pkg / "a.py").write_text(
            "def f():\n    from mvp_sigtest import b\n    return b.g()\n",
            encoding="utf-8",
        )
        (pkg / "b.py").write_text("def g():\n    return 1\n", encoding="utf-8")
        sys.path.insert(0, str(tmp_path))
        try:
            import mvp_sigtest.a  # noqa: F401 -- a imported, b deliberately NOT
            assert "mvp_sigtest.b" not in sys.modules
            orig_fp = eng._FIRST_PARTY
            eng._FIRST_PARTY = "mvp_sigtest"
            try:
                h1 = eng._module_closure_hash("mvp_sigtest.a")
                assert "mvp_sigtest.b" not in sys.modules  # still never imported
                eng._clear_signature_caches()
                (pkg / "b.py").write_text(
                    "def g():\n    return 2  # edit\n", encoding="utf-8"
                )
                h2 = eng._module_closure_hash("mvp_sigtest.a")
                assert h1 != h2
            finally:
                eng._FIRST_PARTY = orig_fp
        finally:
            sys.path.remove(str(tmp_path))
            for m in list(sys.modules):
                if m.startswith("mvp_sigtest"):
                    del sys.modules[m]


class TestDependsOnAudit:
    def test_all_factory_derived_features_declare_deps(self):
        """Mechanical audit (plan test workflow): every register_diff/
        matchup/sum product must carry depends_on — dep-hashing is only as
        sound as this graph, since factory closures all share registry.py
        as their defining module."""
        import inspect as _inspect

        reg = get_registry()
        offenders = []
        for name in reg.list_features():
            feat = reg.get(name)
            mod = _inspect.getmodule(feat.func)
            if mod and mod.__name__ == "mvp.model.registry" and not feat.depends_on:
                offenders.append(name)
        assert not offenders, (
            f"registry-factory features without depends_on: {offenders[:10]}"
        )


class TestSaltLint:
    def test_external_file_reading_transforms_declare_salts(self):
        """Any registered transform whose defining module performs external
        file reads must declare a cache_salt. Exemptions must be listed HERE
        with their reason, not discovered."""
        import inspect as _inspect

        # style_matchup_retrieval's reads live in an offline build entrypoint
        # the engine-path transform never calls.
        exempt = {"style_matchup"}
        reads = re.compile(r"read_parquet|read_csv|scan_parquet|scan_csv")
        reg = get_registry()
        offenders = []
        for name in reg.list_features():
            feat = reg.get(name)
            if not feat.transform:
                continue
            mod = _inspect.getmodule(feat.func)
            src = _inspect.getsource(mod) if mod else ""
            if reads.search(src) and feat.cache_salt is None and name not in exempt:
                offenders.append(name)
        assert not offenders, (
            f"transforms reading external files without cache_salt: {offenders}"
        )
