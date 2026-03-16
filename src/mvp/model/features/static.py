"""Static player attributes (height, age, experience, handedness)."""


import polars as pl

from mvp.model.registry import feature


@feature(
    name="height_diff",
    params=[],
    description="Player height minus opponent height in cm",
    mirror=False,
    impute=0,
)
def height_diff() -> pl.Expr:
    """Height difference in cm (positive = player taller)."""
    return pl.col("player_height_cm") - pl.col("opp_height_cm")


@feature(
    name="age",
    params=[],
    description="Player age in years at match date",
    mirror=True,
)
def age() -> pl.Expr:
    """Player age in years (clipped to 14-55 range, null if outside)."""
    raw_age = (
        (pl.col("effective_match_date").cast(pl.Date) - pl.col("player_birth_date"))
        .dt.total_days() / 365.25
    )
    # Clip unrealistic ages - pros are typically 14-55
    return pl.when((raw_age >= 14) & (raw_age <= 55)).then(raw_age).otherwise(None)


@feature(
    name="age_diff",
    params=[],
    description="Player age minus opponent age in years",
    depends_on=["age"],
    mirror=False,
    impute=0,
)
def age_diff() -> pl.Expr:
    """Age difference (positive = player older)."""
    return pl.col("player_age") - pl.col("opp_age")


@feature(
    name="years_pro",
    params=[],
    description="Years since turning pro",
    mirror=True,
)
def years_pro() -> pl.Expr:
    """Years since turning professional."""
    return (
        pl.col("effective_match_date").dt.year() - pl.col("player_pro_year")
    ).cast(pl.Float64)


@feature(
    name="experience_diff",
    params=[],
    description="Player years pro minus opponent years pro",
    depends_on=["years_pro"],
    mirror=False,
    impute=0,
)
def experience_diff() -> pl.Expr:
    """Experience difference (positive = player more experienced)."""
    return pl.col("player_years_pro") - pl.col("opp_years_pro")


@feature(
    name="is_right_handed",
    params=[],
    description="1 if player is right-handed, 0 if left-handed",
    mirror=True,
    impute=0,
)
def is_right_handed() -> pl.Expr:
    """Right-handed indicator."""
    return pl.col("player_right_handed").cast(pl.Float64)


@feature(
    name="handedness_match",
    params=[],
    description="1 if same handedness, 0 if different",
    mirror=False,
    impute=0,
)
def handedness_match() -> pl.Expr:
    """Whether players have same handedness."""
    return (
        pl.col("player_right_handed") == pl.col("opp_right_handed")
    ).cast(pl.Float64)


@feature(
    name="lefty_vs_righty",
    params=[],
    description="1 if player is lefty facing righty, -1 if righty facing lefty, 0 if same",
    mirror=False,
    impute=0,
)
def lefty_vs_righty() -> pl.Expr:
    """Handedness matchup indicator."""
    return pl.when(
        ~pl.col("player_right_handed") & pl.col("opp_right_handed")
    ).then(1.0).when(
        pl.col("player_right_handed") & ~pl.col("opp_right_handed")
    ).then(-1.0).otherwise(0.0)
