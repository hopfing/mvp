"""Tests for shared enums."""

from mvp.common.enums import Circuit, DrawType, ResultType, Round


class TestRound:
    def test_has_13_members(self):
        assert len(Round) == 13

    def test_values_are_lowercase_strings(self):
        for member in Round:
            assert member.value == member.name.lower()

    def test_str_serialization(self):
        assert str(Round.F) == "f"
        assert str(Round.R16) == "r16"
        assert str(Round.THIRDPLACE) == "thirdplace"


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

    def test_has_2_members(self):
        assert len(Circuit) == 2
