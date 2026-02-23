from __future__ import annotations

from enum import StrEnum, auto


class Round(StrEnum):
    """Canonical round values. All raw round name variants normalize to one of these."""

    @staticmethod
    def _generate_next_value_(name, start, count, last_values):
        return name

    F = auto()
    SF = auto()
    QF = auto()
    R16 = auto()
    R32 = auto()
    R64 = auto()
    R128 = auto()
    RR = auto()
    Q1 = auto()
    Q2 = auto()
    Q3 = auto()
    BRONZE = auto()
    THIRDPLACE = auto()
    HCF = auto()


class Surface(StrEnum):
    """Playing surface."""

    HARD = "Hard"
    CLAY = "Clay"
    GRASS = "Grass"
    CARPET = "Carpet"


class DrawType(StrEnum):
    """The draw within a tournament."""

    singles = auto()
    doubles = auto()


class ResultType(StrEnum):
    """Match completion status."""

    completed = auto()
    retirement = auto()
    walkover = auto()


class Circuit(StrEnum):
    """Tournament circuit."""

    tour = auto()
    chal = auto()
    team = auto()

    @property
    def display_name(self) -> str:
        return _CIRCUIT_DISPLAY[self]


_CIRCUIT_DISPLAY = {
    Circuit.tour: "ATP",
    Circuit.chal: "Challenger",
    Circuit.team: "Team",
}


class TournamentType(StrEnum):
    """Tournament type, mapping API EventType values to circuits."""

    GS = "GS"
    ATP_1000 = "1000"
    ATP_250 = "250"
    ATP_500 = "500"
    CH = "CH"
    DCR = "DCR"
    WC = "WC"
    LVR = "LVR"
    XXI = "XXI"
    UC = "UC"
    ATPC = "ATPC"
    OL = "OL"
    WT = "WT"
    WS = "WS"

    @property
    def circuit(self) -> Circuit:
        return _TOURNAMENT_TYPE_CIRCUIT[self]


_TOURNAMENT_TYPE_CIRCUIT = {
    TournamentType.GS: Circuit.tour,
    TournamentType.ATP_1000: Circuit.tour,
    TournamentType.ATP_250: Circuit.tour,
    TournamentType.ATP_500: Circuit.tour,
    TournamentType.CH: Circuit.chal,
    TournamentType.DCR: Circuit.tour,
    TournamentType.WC: Circuit.tour,
    TournamentType.LVR: Circuit.tour,
    TournamentType.XXI: Circuit.tour,
    TournamentType.UC: Circuit.tour,
    TournamentType.ATPC: Circuit.tour,
    TournamentType.OL: Circuit.tour,
    TournamentType.WT: Circuit.team,
    TournamentType.WS: Circuit.tour,
}
