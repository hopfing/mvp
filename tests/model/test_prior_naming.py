"""`prior_kind_of`: every spelling through which a stem enters a config is
seen, with its kind, so promotion and serving find the dependency however the
config lists it."""

import pytest

from mvp.common.chain_shape import SHAPE_COLUMNS
from mvp.model.prior_naming import ARM_OUTPUTS, prior_kind_of, prior_stem_of

_STEM = "src_chain"


def _spellings():
    yield "prior", f"player_prior_logit(model={_STEM})"
    yield "prior", f"player_prior_logit_{_STEM}"
    yield "chain_arm", f"chain_arm(model={_STEM})"
    for out in ARM_OUTPUTS:
        yield "chain_arm", f"{out}(model={_STEM})"
        yield "chain_arm", f"{out}_{_STEM}"
    yield "chain_shape", f"chain_shape(model={_STEM})"
    for c in SHAPE_COLUMNS:
        yield "chain_shape", f"player_{c}(model={_STEM})"
        yield "chain_shape", f"player_{c}_{_STEM}"


@pytest.mark.parametrize(("kind", "spelling"), list(_spellings()))
def test_every_spelling_returns_its_kind_and_stem(kind, spelling):
    assert prior_kind_of(spelling) == (kind, _STEM)
    assert prior_stem_of(spelling) == _STEM


def test_arm_output_list_is_both_sides_of_three_values():
    assert len(ARM_OUTPUTS) == 6
    assert len(SHAPE_COLUMNS) == 12


@pytest.mark.parametrize("spelling", [
    "player_elo",
    "pts_service_won_pct(days=90)",
    "player_age_diff",
    # No opp_ shape output is registered, so the spelling never reaches a frame.
    f"opp_chain_egames(model={_STEM})",
    f"opp_chain_egames_{_STEM}",
    # A bare output name carries no stem.
    "player_chain_w1_logit",
])
def test_other_specs_are_not_priors(spelling):
    assert prior_kind_of(spelling) is None


def test_a_stem_with_underscores_survives_the_column_form():
    assert prior_kind_of("player_chain_spread_std_a_b_c") == ("chain_shape", "a_b_c")
