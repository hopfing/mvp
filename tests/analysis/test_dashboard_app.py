"""Tests for dashboard app scaffold."""


def test_page_registry_has_expected_pages():
    from mvp.analysis.dashboard.app import PAGE_REGISTRY

    names = [p["name"] for p in PAGE_REGISTRY]
    assert "Overview" in names
    assert "Edge Analysis" in names
    assert "Odds" in names
    assert "Execution" in names
    assert "Book Sharpness" in names
    assert "Insights" in names


def test_page_registry_entries_have_render():
    from mvp.analysis.dashboard.app import PAGE_REGISTRY

    for page in PAGE_REGISTRY:
        assert callable(page["render"]), f"{page['name']} missing render()"
