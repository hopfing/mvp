from enum import StrEnum, auto


class Round(StrEnum):
    """Canonical round values. All raw round name variants normalize to one of these."""

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
    THIRDPLACE = auto()
    HCF = auto()


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
