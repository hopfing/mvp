"""Tests for MatchBeats decryption utilities."""

from mvp.atptour.extractors.match_beats_decrypt import derive_key


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
