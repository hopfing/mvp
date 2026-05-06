"""Tests for Bet365 odds scraper and pipe-delimited parser."""

from datetime import UTC, datetime

import pytest


# -- Fractional-to-decimal conversion --


class TestFracToDecimal:
    def test_evens(self):
        from mvp.bet365.odds import _frac_to_decimal

        assert _frac_to_decimal("1/1") == pytest.approx(2.0)

    def test_odds_on(self):
        from mvp.bet365.odds import _frac_to_decimal

        assert _frac_to_decimal("4/6") == pytest.approx(1.6667, rel=1e-3)

    def test_odds_against(self):
        from mvp.bet365.odds import _frac_to_decimal

        assert _frac_to_decimal("7/2") == pytest.approx(4.5)

    def test_short_price(self):
        from mvp.bet365.odds import _frac_to_decimal

        assert _frac_to_decimal("1/4") == pytest.approx(1.25)

    def test_8_over_11(self):
        from mvp.bet365.odds import _frac_to_decimal

        assert _frac_to_decimal("8/11") == pytest.approx(1.7273, rel=1e-3)

    def test_10_over_3(self):
        from mvp.bet365.odds import _frac_to_decimal

        assert _frac_to_decimal("10/3") == pytest.approx(4.3333, rel=1e-3)

    def test_11_over_10(self):
        from mvp.bet365.odds import _frac_to_decimal

        assert _frac_to_decimal("11/10") == pytest.approx(2.1)

    def test_1_over_2(self):
        from mvp.bet365.odds import _frac_to_decimal

        assert _frac_to_decimal("1/2") == pytest.approx(1.5)


# -- Record parser --


class TestParseRecord:
    def test_basic(self):
        from mvp.bet365.odds import _parse_record

        rec_type, fields = _parse_record("PA;ID=123;NA=Foo;OD=1/2")
        assert rec_type == "PA"
        assert fields == {"ID": "123", "NA": "Foo", "OD": "1/2"}

    def test_empty_value(self):
        from mvp.bet365.odds import _parse_record

        rec_type, fields = _parse_record("MG;SY=fk;NA=;L3=R1")
        assert rec_type == "MG"
        assert fields["NA"] == ""
        assert fields["L3"] == "R1"

    def test_type_only(self):
        from mvp.bet365.odds import _parse_record

        rec_type, fields = _parse_record("F")
        assert rec_type == "F"
        assert fields == {}


# -- Pipe response parser --


# Minimal sample response with one tournament and one match
SAMPLE_RESPONSE = (
    "F|CL;ID=13;IT=test|"
    "MG;ID=83;OI=100;NA=ATP Test Open;SY=fk;L3=R1|"
    "MA;ID=M83;FI=100;CN=1;SY=ed;PY=eu;PF=2;MA=83|"
    "PA;ID=PC1;NA=Carlos Alcaraz;N2=Casper Ruud;FD=Carlos Alcaraz vs Casper Ruud;"
    "FI=100;BC=20260328170000;SY=ed;PZ=0|"
    "PA;ID=PC2;NA=Novak Djokovic;N2=Rafael Nadal;FD=Novak Djokovic vs Rafael Nadal;"
    "FI=200;BC=20260328190000;SY=ed;PZ=1|"
    "MA;ID=M83;NA=;FI=100;CN=1;SY=gb;PY=ed;PF=2;MA=83|"
    "PA;ID=1;FI=100;OD=1/4;SU=0;PZ=0|"
    "PA;ID=2;FI=200;OD=4/6;SU=0;PZ=1|"
    "MA;ID=M83;NA=;FI=100;CN=1;SY=gb;PY=ed;PF=2;MA=83|"
    "PA;ID=3;FI=100;OD=7/2;SU=0;PZ=0|"
    "PA;ID=4;FI=200;OD=11/10;SU=0;PZ=1|"
)


class TestParsePipeResponse:
    def test_basic_parse(self):
        from mvp.bet365.odds import _parse_pipe_response

        now = datetime.now(UTC)
        entries = _parse_pipe_response(SAMPLE_RESPONSE, now)

        # 2 matches × 2 sides = 4 entries
        assert len(entries) == 4

        # Check first match (Alcaraz vs Ruud)
        alcaraz = [e for e in entries if e.player_name == "Carlos Alcaraz"]
        assert len(alcaraz) == 1
        assert alcaraz[0].odds == pytest.approx(1.25)  # 1/4
        assert alcaraz[0].opponent_name == "Casper Ruud"
        assert alcaraz[0].b365_event_id == "100"
        assert alcaraz[0].tournament == "ATP Test Open"
        assert alcaraz[0].circuit == "atp"

        ruud = [e for e in entries if e.player_name == "Casper Ruud"]
        assert len(ruud) == 1
        assert ruud[0].odds == pytest.approx(4.5)  # 7/2

        # Check second match (Djokovic vs Nadal)
        djokovic = [e for e in entries if e.player_name == "Novak Djokovic"]
        assert len(djokovic) == 1
        assert djokovic[0].odds == pytest.approx(1.6667, rel=1e-3)  # 4/6
        assert djokovic[0].b365_event_id == "200"

        nadal = [e for e in entries if e.player_name == "Rafael Nadal"]
        assert len(nadal) == 1
        assert nadal[0].odds == pytest.approx(2.1)  # 11/10

    def test_doubles_filtered(self):
        from mvp.bet365.odds import _parse_pipe_response

        response = (
            "F|MG;ID=83;NA=ATP Miami Doubles;SY=fk|"
            "MA;ID=M83;FI=300;CN=1;SY=ed;PY=eu;PF=2;MA=83|"
            "PA;ID=PC5;NA=Heliovaara/Patten;N2=Bolelli/Vavassori;"
            "FI=300;BC=20260328170000;SY=ed;PZ=0|"
            "MA;ID=M83;NA=;FI=300;CN=1;SY=gb;PY=ed;PF=2;MA=83|"
            "PA;ID=5;FI=300;OD=1/2;SU=0;PZ=0|"
            "MA;ID=M83;NA=;FI=300;CN=1;SY=gb;PY=ed;PF=2;MA=83|"
            "PA;ID=6;FI=300;OD=6/4;SU=0;PZ=0|"
        )
        now = datetime.now(UTC)
        entries = _parse_pipe_response(response, now)
        assert len(entries) == 0

    def test_empty_response(self):
        from mvp.bet365.odds import _parse_pipe_response

        now = datetime.now(UTC)
        entries = _parse_pipe_response("F|CL;ID=13", now)
        assert len(entries) == 0

    def test_all_entries_have_book_b365(self):
        from mvp.bet365.odds import _parse_pipe_response

        now = datetime.now(UTC)
        entries = _parse_pipe_response(SAMPLE_RESPONSE, now)
        for e in entries:
            assert e.book == "b365"
            assert e.circuit == "atp"
            assert e.market == "moneyline"
            assert e.event_status == "NOT_STARTED"

    def test_wta_filtered_out(self):
        from mvp.bet365.odds import _parse_pipe_response

        response = (
            "F|MG;ID=83;NA=WTA Charleston - Round 1;SY=fk|"
            "PA;ID=PC1;NA=Bianca Andreescu;N2=Ashlyn Krueger;"
            "FI=400;BC=20260328170000;SY=ed;PZ=0|"
            "MA;ID=M83;NA=;FI=400;CN=1;SY=gb;PY=ed;PF=2;MA=83|"
            "PA;ID=1;FI=400;OD=1/2;SU=0;PZ=0|"
            "MA;ID=M83;NA=;FI=400;CN=1;SY=gb;PY=ed;PF=2;MA=83|"
            "PA;ID=2;FI=400;OD=6/4;SU=0;PZ=0|"
        )
        now = datetime.now(UTC)
        entries = _parse_pipe_response(response, now)
        assert len(entries) == 0

    def test_challenger_circuit_classified(self):
        from mvp.bet365.odds import _parse_pipe_response

        response = (
            "F|MG;ID=83;NA=Challenger Braga - Round 1;SY=fk|"
            "PA;ID=PC1;NA=Player A;N2=Player B;"
            "FI=500;BC=20260328170000;SY=ed;PZ=0|"
            "MA;ID=M83;NA=;FI=500;CN=1;SY=gb;PY=ed;PF=2;MA=83|"
            "PA;ID=1;FI=500;OD=1/1;SU=0;PZ=0|"
            "MA;ID=M83;NA=;FI=500;CN=1;SY=gb;PY=ed;PF=2;MA=83|"
            "PA;ID=2;FI=500;OD=1/1;SU=0;PZ=0|"
        )
        now = datetime.now(UTC)
        entries = _parse_pipe_response(response, now)
        assert len(entries) == 2
        assert all(e.circuit == "challenger" for e in entries)


# -- Mojibake fix --


class TestFixMojibake:
    def test_recovers_accented_player_name(self):
        from mvp.bet365.odds import _fix_mojibake

        assert _fix_mojibake("JosÃ© Pereira") == "José Pereira"

    def test_recovers_localized_tournament(self):
        from mvp.bet365.odds import _fix_mojibake

        assert (
            _fix_mojibake("Challenger de Santos - 1Â° ronda")
            == "Challenger de Santos - 1° ronda"
        )
        assert (
            _fix_mojibake("ATP Madrid - ClasificaciÃ³n")
            == "ATP Madrid - Clasificación"
        )
        assert _fix_mojibake("ATP MÃºnich - Semifinales") == "ATP Múnich - Semifinales"

    def test_clean_text_passthrough(self):
        from mvp.bet365.odds import _fix_mojibake

        # ASCII-only — no Ã/Â markers, returned unchanged.
        assert _fix_mojibake("Carlos Alcaraz") == "Carlos Alcaraz"
        assert _fix_mojibake("ATP Madrid - Round 1") == "ATP Madrid - Round 1"

    def test_already_correct_unicode_passthrough(self):
        from mvp.bet365.odds import _fix_mojibake

        # Already-correct accents (no mojibake markers) — unchanged.
        assert _fix_mojibake("José Pereira") == "José Pereira"

    def test_undecodable_falls_back_to_input(self):
        from mvp.bet365.odds import _fix_mojibake

        # 'Ã' followed by a char that doesn't form valid UTF-8 — keep as-is.
        s = "Ãx"
        assert _fix_mojibake(s) == s
