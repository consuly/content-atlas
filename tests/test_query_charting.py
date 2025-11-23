from app.domain.queries.charting import build_chart_suggestion


def test_chart_rejected_without_prompt_signal_for_categories():
    csv_data = "label,value\nA,10\nB,15\nC,5\n"
    suggestion = build_chart_suggestion("show counts by label", csv_data)

    assert suggestion["should_display"] is False
    assert "Prompt did not request" in suggestion["reason"]


def test_time_series_chart_allowed_without_explicit_keyword():
    csv_data = "date,total\n2024-01-01,10\n2024-02-01,20\n2024-03-01,15\n"
    suggestion = build_chart_suggestion("revenue by month", csv_data)

    assert suggestion["should_display"] is True
    assert suggestion["spec"]["type"] == "line"
    assert suggestion["spec"]["datasets"][0]["data"] == [10.0, 20.0, 15.0]


def test_breakdown_chart_for_visual_prompt():
    csv_data = "channel,spend\nAds,1200\nEmail,800\nEvents,500\n"
    suggestion = build_chart_suggestion("visualize spend breakdown by channel", csv_data)

    assert suggestion["should_display"] is True
    assert suggestion["spec"]["type"] in ("bar", "pie")
    assert suggestion["spec"]["labels"][:2] == ["Ads", "Email"]
