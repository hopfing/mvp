"""Tests for MatchBeats decryption utilities."""

import base64

import pytest

from mvp.atptour.extractors.match_beats_decrypt import decrypt_response, derive_key


class TestDeriveKey:
    """Tests for derive_key function."""

    def test_derive_key_known_value(self):
        """Test key derivation with a known timestamp."""
        # lastModified from a real API response
        last_modified_ms = 1677609600000  # 2023-03-01 00:00:00 UTC
        key = derive_key(last_modified_ms)

        assert len(key) == 16
        assert key.startswith("#")
        assert key.endswith("$")

    def test_derive_key_format(self):
        """Key should be 16 chars: # + 14 chars + $."""
        key = derive_key(1700000000000)

        assert len(key) == 16
        assert key[0] == "#"
        assert key[-1] == "$"


class TestDecryptResponse:
    """Tests for decrypt_response function."""

    def test_decrypt_response_invalid_base64(self):
        """Invalid base64 should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid base64"):
            decrypt_response("not-valid-base64!!!", 1677609600000)

    def test_decrypt_response_invalid_padding(self):
        """Valid base64 but invalid AES data should raise ValueError."""
        # Valid base64 but not valid AES encrypted data
        invalid_data = base64.b64encode(b"short").decode()
        with pytest.raises(ValueError):
            decrypt_response(invalid_data, 1677609600000)
