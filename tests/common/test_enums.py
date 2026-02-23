"""Tests for shared enums."""

from mvp.common.enums import (
    Circuit,
    DrawType,
    ResultType,
    Round,
    Surface,
    TournamentType,
)


class TestRound:
    def test_has_14_members(self):
        assert len(Round) == 14

    def test_values_are_uppercase_strings(self):
        for member in Round:
            assert member.value == member.name

    def test_str_serialization(self):
        assert str(Round.F) == "F"
        assert str(Round.R16) == "R16"
        assert str(Round.THIRDPLACE) == "THIRDPLACE"

    def test_bronze_member_exists(self):
        assert Round.BRONZE == "BRONZE"

    def test_member_order(self):
        members = list(Round)
        names = [m.name for m in members]
        assert names == [
            "F", "SF", "QF", "R16", "R32", "R64", "R128", "RR",
            "Q1", "Q2", "Q3", "BRONZE", "THIRDPLACE", "HCF",
        ]


class TestSurface:
    def test_has_4_members(self):
        assert len(Surface) == 4

    def test_values_are_mixed_case(self):
        assert Surface.HARD == "Hard"
        assert Surface.CLAY == "Clay"
        assert Surface.GRASS == "Grass"
        assert Surface.CARPET == "Carpet"

    def test_str_serialization(self):
        assert str(Surface.HARD) == "Hard"
        assert str(Surface.CLAY) == "Clay"


class TestDrawType:
    def test_values(self):
        assert DrawType.singles == "singles"
        assert DrawType.doubles == "doubles"

    def test_has_2_members(self):
        assert len(DrawType) == 2


class TestResultType:
    def test_values(self):
        assert ResultType.completed == "completed"
        assert ResultType.retirement == "retirement"
        assert ResultType.walkover == "walkover"

    def test_has_3_members(self):
        assert len(ResultType) == 3


class TestCircuit:
    def test_values(self):
        assert Circuit.tour == "tour"
        assert Circuit.chal == "chal"
        assert Circuit.team == "team"

    def test_has_3_members(self):
        assert len(Circuit) == 3

    def test_display_name(self):
        assert Circuit.tour.display_name == "ATP"
        assert Circuit.chal.display_name == "Challenger"
        assert Circuit.team.display_name == "Team"


class TestTournamentType:
    def test_has_14_members(self):
        assert len(TournamentType) == 14

    def test_api_values(self):
        assert TournamentType.GS == "GS"
        assert TournamentType.ATP_1000 == "1000"
        assert TournamentType.ATP_250 == "250"
        assert TournamentType.ATP_500 == "500"
        assert TournamentType.CH == "CH"
        assert TournamentType.DCR == "DCR"
        assert TournamentType.WC == "WC"
        assert TournamentType.LVR == "LVR"
        assert TournamentType.XXI == "XXI"
        assert TournamentType.UC == "UC"
        assert TournamentType.ATPC == "ATPC"
        assert TournamentType.OL == "OL"
        assert TournamentType.WT == "WT"
        assert TournamentType.WS == "WS"

    def test_circuit_property_tour(self):
        for tt in [
            TournamentType.GS, TournamentType.ATP_1000, TournamentType.ATP_250,
            TournamentType.ATP_500, TournamentType.DCR, TournamentType.WC,
            TournamentType.LVR, TournamentType.XXI, TournamentType.UC,
            TournamentType.ATPC, TournamentType.OL, TournamentType.WS,
        ]:
            assert tt.circuit == Circuit.tour, f"{tt} should map to tour"

    def test_circuit_property_chal(self):
        assert TournamentType.CH.circuit == Circuit.chal

    def test_circuit_property_team(self):
        assert TournamentType.WT.circuit == Circuit.team
