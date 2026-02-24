"""Tests for BaseExtractor HTTP session and fetch logic."""

from unittest.mock import MagicMock, patch

import pytest

from mvp.common.base_extractor import BaseExtractor


class ConcreteExtractor(BaseExtractor):
    """Test subclass."""

    pass


@pytest.fixture
def extractor(tmp_path):
    return ConcreteExtractor(domain="test", data_root=tmp_path)


class TestBaseExtractor:
    def test_creates_session_with_headers(self, extractor):
        assert extractor.session is not None
        assert "User-Agent" in extractor.session.headers

    def test_default_timeout(self, extractor):
        assert extractor.timeout == 30

    def test_custom_timeout(self, tmp_path):
        ext = ConcreteExtractor(
            domain="test", data_root=tmp_path, timeout=60
        )
        assert ext.timeout == 60

    def test_inherits_base_job(self, extractor):
        assert extractor.domain == "test"
        assert hasattr(extractor, "build_path")

    def test_fetch_json_returns_parsed(self, extractor):
        mock_response = MagicMock()
        mock_response.json.return_value = {"key": "value"}
        mock_response.headers = {"content-type": "application/json"}
        with patch.object(
            extractor, "_fetch", return_value=mock_response
        ):
            result = extractor.fetch_json("https://example.com/api")
        assert result == {"key": "value"}

    def test_fetch_json_returns_list(self, extractor):
        mock_response = MagicMock()
        mock_response.json.return_value = [1, 2, 3]
        mock_response.headers = {"content-type": "application/json"}
        with patch.object(
            extractor, "_fetch", return_value=mock_response
        ):
            result = extractor.fetch_json("https://example.com/api")
        assert result == [1, 2, 3]

    def test_fetch_json_returns_none_for_null(self, extractor):
        mock_response = MagicMock()
        mock_response.json.return_value = None
        mock_response.headers = {"content-type": "application/json"}
        with patch.object(
            extractor, "_fetch", return_value=mock_response
        ):
            result = extractor.fetch_json("https://example.com/api")
        assert result is None

    def test_fetch_json_raises_on_non_json_content_type(self, extractor):
        mock_response = MagicMock()
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = "<html>not json</html>"
        with patch.object(
            extractor, "_fetch", return_value=mock_response
        ):
            with pytest.raises(ValueError, match="Expected JSON"):
                extractor.fetch_json("https://example.com/api")

    def test_fetch_json_accepts_json_charset_content_type(
        self, extractor
    ):
        mock_response = MagicMock()
        mock_response.json.return_value = {"ok": True}
        mock_response.headers = {
            "content-type": "application/json; charset=utf-8"
        }
        with patch.object(
            extractor, "_fetch", return_value=mock_response
        ):
            result = extractor.fetch_json("https://example.com/api")
        assert result == {"ok": True}

    def test_fetch_html_returns_text(self, extractor):
        mock_response = MagicMock()
        mock_response.text = "<html><body>Hello</body></html>"
        with patch.object(
            extractor, "_fetch", return_value=mock_response
        ):
            result = extractor.fetch_html("https://example.com")
        assert result == "<html><body>Hello</body></html>"

    def test_fetch_passes_json_accept_header(self, extractor):
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.headers = {"content-type": "application/json"}
        with patch.object(
            extractor, "_fetch", return_value=mock_response
        ) as mock_fetch:
            extractor.fetch_json("https://example.com/api")
        mock_fetch.assert_called_once_with(
            "https://example.com/api",
            headers={"Accept": "application/json"},
        )

    def test_fetch_html_passes_no_extra_headers(self, extractor):
        mock_response = MagicMock()
        mock_response.text = "<html></html>"
        with patch.object(
            extractor, "_fetch", return_value=mock_response
        ) as mock_fetch:
            extractor.fetch_html("https://example.com")
        mock_fetch.assert_called_once_with("https://example.com")


class TestFetchRetry:
    """Tests for _fetch retry and backoff logic."""

    def test_fetch_succeeds_first_try(self, extractor):
        mock_response = MagicMock()
        mock_response.status_code = 200
        with patch.object(
            extractor.session, "get", return_value=mock_response
        ):
            with patch("mvp.common.base_extractor.time.sleep"):
                result = extractor._fetch("https://example.com")
        assert result is mock_response

    def test_fetch_retries_on_failure(self, extractor):
        import requests

        mock_response = MagicMock()
        mock_response.status_code = 200
        extractor.session.get = MagicMock(
            side_effect=[
                requests.RequestException("fail"),
                mock_response,
            ]
        )
        with patch("mvp.common.base_extractor.time.sleep"):
            result = extractor._fetch(
                "https://example.com", retries=1
            )
        assert result is mock_response

    def test_fetch_raises_after_exhausted_retries(self, extractor):
        import requests

        extractor.session.get = MagicMock(
            side_effect=requests.RequestException("fail")
        )
        with patch("mvp.common.base_extractor.time.sleep"):
            with pytest.raises(
                requests.RequestException, match="fail"
            ):
                extractor._fetch(
                    "https://example.com", retries=2
                )
        assert extractor.session.get.call_count == 3

    def test_fetch_calls_raise_for_status(self, extractor):
        mock_response = MagicMock()
        with patch.object(
            extractor.session, "get", return_value=mock_response
        ):
            with patch("mvp.common.base_extractor.time.sleep"):
                extractor._fetch("https://example.com")
        mock_response.raise_for_status.assert_called_once()

    def test_fetch_sleeps_before_request(self, extractor):
        mock_response = MagicMock()
        call_order = []

        def record_sleep(x):
            call_order.append("sleep")

        def record_get(*a, **kw):
            call_order.append("get")
            return mock_response

        with patch(
            "mvp.common.base_extractor.time.sleep",
            side_effect=record_sleep,
        ):
            with patch.object(
                extractor.session, "get", side_effect=record_get
            ):
                extractor._fetch("https://example.com")
        assert call_order == ["sleep", "get"]

    def test_fetch_merges_extra_headers(self, extractor):
        mock_response = MagicMock()
        with patch.object(
            extractor.session, "get", return_value=mock_response
        ) as mock_get:
            with patch("mvp.common.base_extractor.time.sleep"):
                extractor._fetch(
                    "https://example.com",
                    headers={"Accept": "application/json"},
                )
            _, kwargs = mock_get.call_args
            assert kwargs["headers"] == {
                "Accept": "application/json"
            }
