"""Tests for the Cloudflare challenge detection and BaseExtractor fallback.

The browser (undetected_chromedriver) is always mocked here — these tests
cover the detection heuristic and the two-tier fallback control flow in
BaseExtractor, not the real solver.
"""

from unittest.mock import MagicMock, patch

import pytest
from curl_cffi import requests as curl_requests

from mvp.common.base_extractor import BaseExtractor
from mvp.common.cf_solver import (
    CloudflareChallengeError,
    body_has_challenge,
    is_cf_challenge,
)


class FallbackExtractor(BaseExtractor):
    """Cookie tier only (like MatchStatsExtractor): no per-URL browser fetch."""

    def __init__(self, tmp_path):
        super().__init__(
            domain="test", data_root=tmp_path, cloudflare_fallback=True
        )


class FullFallbackExtractor(BaseExtractor):
    """Cookie tier + per-URL browser fetch (like the five critical extractors)."""

    def __init__(self, tmp_path):
        super().__init__(
            domain="test",
            data_root=tmp_path,
            cloudflare_fallback=True,
            cloudflare_browser_fetch=True,
        )


class PlainExtractor(BaseExtractor):
    """Test subclass with the fallback disabled (default)."""

    def __init__(self, tmp_path):
        super().__init__(domain="test", data_root=tmp_path)


def _resp(status_code, text):
    r = MagicMock()
    r.status_code = status_code
    r.text = text
    return r


class TestChallengeDetection:
    def test_403_with_just_a_moment_is_challenge(self):
        assert is_cf_challenge(403, "<title>Just a moment...</title>")

    def test_403_with_cf_marker_is_challenge(self):
        assert is_cf_challenge(403, "<div id='cf-browser-verification'>")

    def test_200_with_challenge_text_is_not_challenge(self):
        # A real payload that merely mentions cloudflare must not trip on 200.
        assert not is_cf_challenge(200, "Just a moment, loading data")

    def test_403_without_marker_is_not_challenge(self):
        assert not is_cf_challenge(403, '{"error": "forbidden"}')

    def test_body_has_challenge_is_case_insensitive(self):
        assert body_has_challenge("JUST A MOMENT")

    def test_empty_body_is_not_challenge(self):
        assert not body_has_challenge("")


class TestFetchFallback:
    def test_challenge_then_cookie_harvest_succeeds(self, tmp_path):
        ext = FallbackExtractor(tmp_path)
        challenge = _resp(403, "Just a moment...")
        cleared = _resp(200, "{}")
        ext.session.get = MagicMock(side_effect=[challenge, cleared])

        solver = MagicMock()
        solver.solve.return_value = ([], "UA/1.0")
        with patch("mvp.common.base_extractor.get_solver", return_value=solver):
            with patch("mvp.common.base_extractor.time.sleep"):
                result = ext._fetch("https://example.com")

        assert result is cleared
        solver.solve.assert_called_once_with("https://example.com")

    def test_challenge_persists_after_harvest_raises(self, tmp_path):
        ext = FallbackExtractor(tmp_path)
        challenge = _resp(403, "Just a moment...")
        still = _resp(403, "Just a moment...")
        ext.session.get = MagicMock(side_effect=[challenge, still])

        solver = MagicMock()
        solver.solve.return_value = ([], "UA/1.0")
        with patch("mvp.common.base_extractor.get_solver", return_value=solver):
            with patch("mvp.common.base_extractor.time.sleep"):
                with pytest.raises(CloudflareChallengeError):
                    ext._fetch("https://example.com")

    def test_harvested_cookies_applied_to_session(self, tmp_path):
        ext = FallbackExtractor(tmp_path)
        challenge = _resp(403, "Just a moment...")
        cleared = _resp(200, "{}")
        ext.session.get = MagicMock(side_effect=[challenge, cleared])
        ext.session.cookies.set = MagicMock()

        solver = MagicMock()
        solver.solve.return_value = (
            [{"name": "cf_clearance", "value": "abc", "domain": ".example.com"}],
            "UA/1.0",
        )
        with patch("mvp.common.base_extractor.get_solver", return_value=solver):
            with patch("mvp.common.base_extractor.time.sleep"):
                ext._fetch("https://example.com")

        assert ext.session.headers["User-Agent"] == "UA/1.0"
        ext.session.cookies.set.assert_called_once_with(
            "cf_clearance", "abc", domain=".example.com"
        )

    def test_fetch_json_falls_back_to_browser_fetch(self, tmp_path):
        ext = FullFallbackExtractor(tmp_path)
        solver = MagicMock()
        solver.fetch_text.return_value = '{"ok": true}'
        with patch("mvp.common.base_extractor.get_solver", return_value=solver):
            with patch.object(
                ext, "_fetch", side_effect=CloudflareChallengeError("u")
            ):
                result = ext.fetch_json("https://example.com/api")
        assert result == {"ok": True}
        solver.fetch_text.assert_called_once_with("https://example.com/api")

    def test_fetch_html_falls_back_to_browser_fetch(self, tmp_path):
        ext = FullFallbackExtractor(tmp_path)
        solver = MagicMock()
        solver.fetch_text.return_value = "<html>ok</html>"
        with patch("mvp.common.base_extractor.get_solver", return_value=solver):
            with patch.object(
                ext, "_fetch", side_effect=CloudflareChallengeError("u")
            ):
                result = ext.fetch_html("https://example.com")
        assert result == "<html>ok</html>"

    def test_cookie_only_fetch_json_reraises_without_browser(self, tmp_path):
        # MatchStats-style: cookie tier on, browser fetch off. A persistent
        # challenge must re-raise (caller skips), never invoke the browser.
        ext = FallbackExtractor(tmp_path)
        solver = MagicMock()
        with patch("mvp.common.base_extractor.get_solver", return_value=solver):
            with patch.object(
                ext, "_fetch", side_effect=CloudflareChallengeError("u")
            ):
                with pytest.raises(CloudflareChallengeError):
                    ext.fetch_json("https://example.com/api")
        solver.fetch_text.assert_not_called()

    def test_cookie_only_fetch_html_reraises_without_browser(self, tmp_path):
        ext = FallbackExtractor(tmp_path)
        solver = MagicMock()
        with patch("mvp.common.base_extractor.get_solver", return_value=solver):
            with patch.object(
                ext, "_fetch", side_effect=CloudflareChallengeError("u")
            ):
                with pytest.raises(CloudflareChallengeError):
                    ext.fetch_html("https://example.com")
        solver.fetch_text.assert_not_called()

    def test_disabled_flag_skips_fallback(self, tmp_path):
        ext = PlainExtractor(tmp_path)
        challenge = _resp(403, "Just a moment...")
        challenge.raise_for_status.side_effect = curl_requests.RequestsError(
            "403"
        )
        ext.session.get = MagicMock(return_value=challenge)

        with patch("mvp.common.base_extractor.get_solver") as get_solver:
            with patch("mvp.common.base_extractor.time.sleep"):
                with pytest.raises(curl_requests.RequestsError):
                    ext._fetch("https://example.com", retries=0)
        get_solver.assert_not_called()
