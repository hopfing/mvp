"""Smoke test for CaesarsOddsMatcher — matches tests/betmgm/test_matcher.py
and tests/draftkings/test_matcher.py shape."""

from pathlib import Path

from mvp.caesars.matcher import CaesarsOddsMatcher


def test_matcher_has_correct_event_id_column(tmp_path):
    matcher = CaesarsOddsMatcher(data_root=tmp_path)
    assert matcher.event_id_column == "czr_event_id"


def test_matcher_has_correct_book_label(tmp_path):
    matcher = CaesarsOddsMatcher(data_root=tmp_path)
    assert matcher.book_label == "CZR"
