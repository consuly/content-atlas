"""
Heuristics for suggesting lightweight Chart.js visualizations for query results.

Charts are only proposed when the user prompt implies a visual is helpful and
the data is small, numeric, and unambiguous enough for a clear chart.
"""

from __future__ import annotations

import csv
import math
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

MAX_CHART_ROWS = 50
MAX_CATEGORIES = 15

PROMPT_VISUAL_KEYWORDS = (
    "chart",
    "graph",
    "visual",
    "visualize",
    "plot",
    "trend",
    "over time",
    "timeline",
    "growth",
    "distribution",
    "breakdown",
    "compare",
    "by month",
    "by day",
    "per month",
    "per day",
    "daily",
    "weekly",
    "monthly",
)

PALETTE = [
    "#2563EB",  # blue
    "#16A34A",  # green
    "#F97316",  # orange
    "#9333EA",  # purple
    "#0891B2",  # teal
    "#DC2626",  # red
    "#0EA5E9",  # sky
    "#EAB308",  # yellow
]


def build_chart_suggestion(user_prompt: str, data_csv: Optional[str]) -> Dict[str, Any]:
    """
    Return a Chart.js-ready suggestion or a reason why no chart should be shown.
    """
    prompt_lower = (user_prompt or "").lower()
    wants_visual = any(keyword in prompt_lower for keyword in PROMPT_VISUAL_KEYWORDS)

    if not data_csv:
        return _no_chart("No result data available to visualize.")

    rows = _parse_csv_rows(data_csv)
    if len(rows) < 2:
        return _no_chart("Need at least two rows of data to build a chart.")
    if len(rows) > MAX_CHART_ROWS:
        return _no_chart("Result set is too large for an automatic chart (over 50 rows).")

    column_summaries = _summarize_columns(rows)
    numeric_columns = [col for col in column_summaries if col["numeric_ratio"] >= 0.6]
    if not numeric_columns:
        return _no_chart("Results do not contain a stable numeric column to chart.")

    time_columns = [col for col in column_summaries if col["time_ratio"] >= 0.6]
    category_columns = [col for col in column_summaries if col["numeric_ratio"] < 0.4]

    # Guardrails: only suggest when prompt implies visuals or time-series pattern exists.
    if not wants_visual and not time_columns:
        return _no_chart("Prompt did not request a visualization and no time series was detected.")

    if time_columns and numeric_columns:
        return _build_time_series_chart(time_columns[0], numeric_columns[0])

    if category_columns and numeric_columns:
        return _build_category_chart(category_columns[0], numeric_columns[0])

    return _no_chart("Data shape is ambiguous for a chart.")


def _no_chart(reason: str) -> Dict[str, Any]:
    return {
        "should_display": False,
        "reason": reason,
        "spec": None,
    }


def _parse_csv_rows(data_csv: str, limit: int = MAX_CHART_ROWS) -> List[Dict[str, str]]:
    reader = csv.DictReader(data_csv.splitlines())
    rows: List[Dict[str, str]] = []
    for idx, row in enumerate(reader):
        if idx >= limit:
            break
        rows.append(row)
    return rows


def _summarize_columns(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    summaries: List[Dict[str, Any]] = []
    headers = rows[0].keys()

    for header in headers:
        values = [row.get(header) for row in rows]
        numeric_values = [_to_number(value) for value in values]
        numeric_hits = sum(1 for v in numeric_values if v is not None)
        numeric_ratio = numeric_hits / max(1, len(values))

        parsed_times, time_ratio = _parse_datetimes(values)

        summaries.append(
            {
                "name": header,
                "raw_values": values,
                "numeric_values": numeric_values,
                "numeric_ratio": numeric_ratio,
                "time_values": parsed_times,
                "time_ratio": time_ratio,
            }
        )
    return summaries


def _parse_datetimes(values: Sequence[Optional[str]]) -> Tuple[List[Optional[str]], float]:
    parsed: List[Optional[str]] = []
    hits = 0

    for value in values:
        if value is None:
            parsed.append(None)
            continue

        parsed_value = _coerce_datetime(value)
        parsed.append(parsed_value)
        if parsed_value is not None:
            hits += 1

    ratio = hits / max(1, len(values))
    return parsed, ratio


def _coerce_datetime(value: str) -> Optional[str]:
    value = value.strip()
    if not value:
        return None

    try:
        # ISO-like strings parse cheaply
        return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
    except Exception:
        pass

    try:
        parsed = pd.to_datetime([value], errors="coerce")[0]
        if pd.notnull(parsed):
            return parsed.isoformat()
    except Exception:
        return None

    return None


def _to_number(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None

    cleaned = re.sub(r"[,$]", "", text)
    cleaned = cleaned.replace("%", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _build_time_series_chart(time_column: Dict[str, Any], numeric_column: Dict[str, Any]) -> Dict[str, Any]:
    labels: List[str] = []
    data: List[float] = []

    for label, value in zip(time_column["time_values"], numeric_column["numeric_values"]):
        if label is None or value is None:
            continue
        labels.append(label)
        data.append(value)

    if len(labels) < 2:
        return _no_chart("Not enough valid time series data to chart.")

    palette_color = PALETTE[0]
    return {
        "should_display": True,
        "reason": "Detected a time series with numeric values.",
        "spec": {
            "type": "line",
            "labels": labels,
            "datasets": [
                {
                    "label": numeric_column["name"],
                    "data": data,
                    "borderColor": palette_color,
                    "backgroundColor": palette_color + "33",
                    "fill": False,
                }
            ],
            "options": _base_chart_options(title="Trend over time"),
        },
    }


def _build_category_chart(category_column: Dict[str, Any], numeric_column: Dict[str, Any]) -> Dict[str, Any]:
    labels: List[str] = []
    data: List[float] = []

    for label, value in zip(category_column["raw_values"], numeric_column["numeric_values"]):
        if label is None or value is None:
            continue
        labels.append(str(label))
        data.append(value)
        if len(labels) >= MAX_CATEGORIES:
            break

    if len(labels) < 2:
        return _no_chart("Not enough category data to chart.")

    chart_type = "pie" if len(labels) <= 8 else "bar"
    palette = _expanded_palette(len(labels))

    dataset_config: Dict[str, Any] = {
        "label": numeric_column["name"],
        "data": data,
        "backgroundColor": palette,
    }
    if chart_type == "bar":
        dataset_config["borderColor"] = palette
        dataset_config["fill"] = False

    return {
        "should_display": True,
        "reason": "Detected a categorical breakdown with numeric values.",
        "spec": {
            "type": chart_type,
            "labels": labels,
            "datasets": [dataset_config],
            "options": _base_chart_options(title=f"{numeric_column['name']} by {category_column['name']}"),
        },
    }


def _base_chart_options(title: Optional[str] = None) -> Dict[str, Any]:
    options: Dict[str, Any] = {
        "responsive": True,
        "plugins": {
            "legend": {"display": True},
            "tooltip": {"enabled": True},
        },
    }
    if title:
        options["plugins"]["title"] = {"display": True, "text": title}
    return options


def _expanded_palette(size: int) -> List[str]:
    repeats = math.ceil(size / len(PALETTE))
    return (PALETTE * repeats)[:size]
