#!/usr/bin/env python3
"""Build an RC1 release dashboard in Metabase.

RC1 = bugs whose title contains 'RC1' AND severity IN ('1','2') (P1/P2).

Reads METABASE_URL and METABASE_API_KEY from .env. Creates 10 native-SQL
cards and a single dashboard pinning them at fixed positions.

Re-running prints the existing dashboard URL instead of creating duplicates
(detected by dashboard name match).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib import request, error

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

MB_URL = os.environ["METABASE_URL"].rstrip("/")
MB_KEY = os.environ["METABASE_API_KEY"]
DB_ID = 2  # postgres 'ado' database on this Metabase instance

DASHBOARD_NAME = "RC1 — Open Bugs (P1/P2)"
DASHBOARD_DESC = (
    "RC1 release tracker. Scope: bugs whose title contains 'RC1' and severity is P1 or P2. "
    "Auto-built via build_rc1_dashboard.py."
)
COLLECTION_ID: int | None = None  # None = root collection; set to an id to place elsewhere


# ---------- Metabase REST helpers ----------

def _req(method: str, path: str, body: Any | None = None) -> Any:
    data = None
    headers = {"x-api-key": MB_KEY, "accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["content-type"] = "application/json"
    req = request.Request(f"{MB_URL}{path}", data=data, headers=headers, method=method)
    try:
        with request.urlopen(req) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except error.HTTPError as e:
        sys.stderr.write(
            f"\nHTTP {e.code} on {method} {path}\n{e.read().decode(errors='replace')}\n"
        )
        raise


def get(path: str) -> Any:
    return _req("GET", path)


def post(path: str, body: Any) -> Any:
    return _req("POST", path, body)


def put(path: str, body: Any) -> Any:
    return _req("PUT", path, body)


# ---------- Shared SQL fragments ----------

RC1_FILTER = "b.title ILIKE '%RC1%' AND b.severity IN ('1','2')"
OPEN_STATES_EXCLUDED = "('Done','QA Completed','Removed','Not reproduced')"
CLOSED_NEW_VALUES = "('Done','QA Completed')"

# ---------- Widget specs ----------
# Each widget: (name, display, sql, visualization_settings, position)
# position = (row, col, size_x, size_y) on Metabase's 24-col grid.

WIDGETS: list[dict[str, Any]] = [
    {
        "name": "RC1 — Open bugs (P1/P2)",
        "display": "scalar",
        "sql": f"""
SELECT COUNT(*) AS open_now
FROM public.bugs b
WHERE {RC1_FILTER}
  AND b.state NOT IN {OPEN_STATES_EXCLUDED}
""",
        "viz": {},
        "pos": (0, 0, 6, 3),
    },
    {
        "name": "RC1 — Total bugs (P1/P2)",
        "display": "scalar",
        "sql": f"""
SELECT COUNT(*) AS total
FROM public.bugs b
WHERE {RC1_FILTER}
""",
        "viz": {},
        "pos": (0, 6, 6, 3),
    },
    {
        "name": "RC1 — Opened this week",
        "display": "scalar",
        "sql": f"""
SELECT COUNT(*) AS opened
FROM public.bugs b
WHERE {RC1_FILTER}
  AND b.created_date >= date_trunc('week', NOW())
""",
        "viz": {},
        "pos": (0, 12, 6, 3),
    },
    {
        "name": "RC1 — Closed this week",
        "display": "scalar",
        "sql": f"""
SELECT COUNT(DISTINCT ch.record_id) AS closed
FROM public.change_history ch
JOIN public.bugs b ON b.id = ch.record_id
WHERE {RC1_FILTER}
  AND ch.table_name = 'bugs'
  AND ch.field_changed = 'System.State'
  AND ch.new_value IN {CLOSED_NEW_VALUES}
  AND ch.changed_date >= date_trunc('week', NOW())
""",
        "viz": {},
        "pos": (0, 18, 6, 3),
    },
    {
        "name": "RC1 — Bug pace: Created vs Closed (weekly)",
        "display": "line",
        "sql": f"""
WITH rc1 AS (
  SELECT b.id, b.created_date
  FROM public.bugs b
  WHERE {RC1_FILTER}
)
SELECT week, metric, n FROM (
  SELECT date_trunc('week', created_date)::date AS week,
         'Created' AS metric,
         COUNT(*) AS n
  FROM rc1
  WHERE created_date >= NOW() - INTERVAL '6 months'
  GROUP BY 1
  UNION ALL
  SELECT date_trunc('week', ch.changed_date)::date AS week,
         'Closed' AS metric,
         COUNT(DISTINCT ch.record_id) AS n
  FROM public.change_history ch
  JOIN rc1 ON rc1.id = ch.record_id
  WHERE ch.table_name = 'bugs'
    AND ch.field_changed = 'System.State'
    AND ch.new_value IN {CLOSED_NEW_VALUES}
    AND ch.changed_date >= NOW() - INTERVAL '6 months'
  GROUP BY 1
) t
ORDER BY week, metric
""",
        "viz": {
            "graph.dimensions": ["week", "metric"],
            "graph.metrics": ["n"],
            "graph.x_axis.title_text": "week",
            "graph.y_axis.title_text": "bugs / week",
            "graph.show_values": True,
        },
        "pos": (3, 0, 24, 6),
    },
    {
        "name": "RC1 — Open bugs per team",
        "display": "bar",
        "sql": f"""
SELECT COALESCE(lt.category, b.area_path) AS team,
       COUNT(*) AS open_bugs
FROM public.bugs b
LEFT JOIN public.lookuptable lt ON lt.name = b.area_path
WHERE {RC1_FILTER}
  AND b.state NOT IN {OPEN_STATES_EXCLUDED}
GROUP BY 1
ORDER BY 2 DESC
""",
        "viz": {
            "graph.dimensions": ["team"],
            "graph.metrics": ["open_bugs"],
            "graph.show_values": True,
        },
        "pos": (9, 0, 12, 6),
    },
    {
        "name": "RC1 — Bugs per state and team",
        "display": "bar",
        "sql": f"""
SELECT COALESCE(lt.category, b.area_path) AS team,
       b.state,
       COUNT(*) AS n
FROM public.bugs b
LEFT JOIN public.lookuptable lt ON lt.name = b.area_path
WHERE {RC1_FILTER}
  AND b.state <> 'Removed'
GROUP BY 1, 2
ORDER BY 1, 3 DESC
""",
        "viz": {
            "graph.dimensions": ["team", "state"],
            "graph.metrics": ["n"],
            "stackable.stack_type": "stacked",
            "graph.show_values": True,
        },
        "pos": (9, 12, 12, 6),
    },
    {
        "name": "RC1 — New bugs (weekly trend)",
        "display": "bar",
        "sql": f"""
SELECT date_trunc('week', b.created_date)::date AS week,
       COUNT(*) AS new_bugs
FROM public.bugs b
WHERE {RC1_FILTER}
  AND b.created_date >= NOW() - INTERVAL '6 months'
GROUP BY 1
ORDER BY 1
""",
        "viz": {
            "graph.dimensions": ["week"],
            "graph.metrics": ["new_bugs"],
            "graph.show_values": True,
        },
        "pos": (15, 0, 24, 5),
    },
    {
        "name": "RC1 — Moved to In Progress (weekly)",
        "display": "bar",
        "sql": f"""
SELECT date_trunc('week', ch.changed_date)::date AS week,
       COUNT(DISTINCT ch.record_id) AS n
FROM public.change_history ch
JOIN public.bugs b ON b.id = ch.record_id
WHERE {RC1_FILTER}
  AND ch.table_name = 'bugs'
  AND ch.field_changed = 'System.State'
  AND ch.new_value = 'In Progress'
  AND ch.changed_date >= NOW() - INTERVAL '6 months'
GROUP BY 1
ORDER BY 1
""",
        "viz": {
            "graph.dimensions": ["week"],
            "graph.metrics": ["n"],
            "graph.show_values": True,
        },
        "pos": (20, 0, 8, 6),
    },
    {
        "name": "RC1 — Moved to Ready for QA (weekly)",
        "display": "bar",
        "sql": f"""
SELECT date_trunc('week', ch.changed_date)::date AS week,
       COUNT(DISTINCT ch.record_id) AS n
FROM public.change_history ch
JOIN public.bugs b ON b.id = ch.record_id
WHERE {RC1_FILTER}
  AND ch.table_name = 'bugs'
  AND ch.field_changed = 'System.State'
  AND ch.new_value = 'Ready for QA'
  AND ch.changed_date >= NOW() - INTERVAL '6 months'
GROUP BY 1
ORDER BY 1
""",
        "viz": {
            "graph.dimensions": ["week"],
            "graph.metrics": ["n"],
            "graph.show_values": True,
        },
        "pos": (20, 8, 8, 6),
    },
    {
        "name": "RC1 — Moved to QA Completed (weekly)",
        "display": "bar",
        "sql": f"""
SELECT date_trunc('week', ch.changed_date)::date AS week,
       COUNT(DISTINCT ch.record_id) AS n
FROM public.change_history ch
JOIN public.bugs b ON b.id = ch.record_id
WHERE {RC1_FILTER}
  AND ch.table_name = 'bugs'
  AND ch.field_changed = 'System.State'
  AND ch.new_value = 'QA Completed'
  AND ch.changed_date >= NOW() - INTERVAL '6 months'
GROUP BY 1
ORDER BY 1
""",
        "viz": {
            "graph.dimensions": ["week"],
            "graph.metrics": ["n"],
            "graph.show_values": True,
        },
        "pos": (20, 16, 8, 6),
    },
    {
        # Dev cycle time: from creation to first time the bug reached "Ready for QA".
        # Grouped by the week the bug reached Ready-for-QA. Shows avg AND median so
        # the median isn't skewed by outliers and the avg flags weeks with long tails.
        "name": "RC1 — Dev cycle time: New → Ready for QA (weekly avg / median days)",
        "display": "bar",
        "sql": f"""
WITH rc1 AS (
  SELECT b.id, b.created_date
  FROM public.bugs b
  WHERE {RC1_FILTER}
    AND b.created_date >= NOW() - INTERVAL '6 months'
),
r4qa AS (
  SELECT rc1.id,
         rc1.created_date,
         MIN(ch.changed_date) AS first_r4qa
  FROM rc1
  JOIN public.change_history ch ON ch.record_id = rc1.id
  WHERE ch.table_name = 'bugs'
    AND ch.field_changed = 'System.State'
    AND ch.new_value = 'Ready for QA'
  GROUP BY rc1.id, rc1.created_date
)
SELECT date_trunc('week', first_r4qa)::date AS week,
       COUNT(*) AS bugs,
       ROUND(AVG(EXTRACT(EPOCH FROM (first_r4qa - created_date)) / 86400)::numeric, 1) AS avg_days,
       ROUND(
         percentile_cont(0.5) WITHIN GROUP (
           ORDER BY EXTRACT(EPOCH FROM (first_r4qa - created_date)) / 86400
         )::numeric, 1
       ) AS median_days
FROM r4qa
GROUP BY 1
ORDER BY 1
""",
        "viz": {
            "graph.dimensions": ["week"],
            "graph.metrics": ["avg_days", "median_days"],
            "graph.x_axis.title_text": "week (when reached Ready for QA)",
            "graph.y_axis.title_text": "days from New → Ready for QA",
            "graph.show_values": True,
            "stackable.stack_type": None,
        },
        "pos": (26, 0, 24, 5),
    },
    {
        # Same cycle-time definition as the weekly widget, but grouped by team
        # (area_path → category via lookuptable). Ordered by median desc so the
        # slowest teams surface first. Teams without a lookuptable mapping show
        # their raw area_path so they're still visible.
        "name": "RC1 — Dev cycle time per team: New → Ready for QA (days)",
        "display": "bar",
        "sql": f"""
WITH rc1 AS (
  SELECT b.id, b.created_date, b.area_path
  FROM public.bugs b
  WHERE {RC1_FILTER}
    AND b.created_date >= NOW() - INTERVAL '6 months'
),
r4qa AS (
  SELECT rc1.id, rc1.created_date, rc1.area_path,
         MIN(ch.changed_date) AS first_r4qa
  FROM rc1
  JOIN public.change_history ch ON ch.record_id = rc1.id
  WHERE ch.table_name = 'bugs'
    AND ch.field_changed = 'System.State'
    AND ch.new_value = 'Ready for QA'
  GROUP BY rc1.id, rc1.created_date, rc1.area_path
)
SELECT COALESCE(lt.category, r4qa.area_path) AS team,
       COUNT(*) AS bugs,
       ROUND(AVG(EXTRACT(EPOCH FROM (first_r4qa - created_date)) / 86400)::numeric, 1) AS avg_days,
       ROUND(
         percentile_cont(0.5) WITHIN GROUP (
           ORDER BY EXTRACT(EPOCH FROM (first_r4qa - created_date)) / 86400
         )::numeric, 1
       ) AS median_days
FROM r4qa
LEFT JOIN public.lookuptable lt ON lt.name = r4qa.area_path
GROUP BY 1
ORDER BY median_days DESC
""",
        "viz": {
            "graph.dimensions": ["team"],
            "graph.metrics": ["avg_days", "median_days"],
            "graph.x_axis.title_text": "team",
            "graph.y_axis.title_text": "days from New → Ready for QA",
            "graph.show_values": True,
            "stackable.stack_type": None,
        },
        "pos": (31, 0, 24, 5),
    },
]


# ---------- Build / update ----------

def find_existing_dashboard(name: str) -> dict | None:
    for item in get("/api/search?q=" + request.quote(name)) .get("data", []):
        if item.get("model") == "dashboard" and item.get("name") == name:
            return item
    return None


def create_card(spec: dict[str, Any]) -> int:
    payload = {
        "name": spec["name"],
        "display": spec["display"],
        "collection_id": COLLECTION_ID,
        "dataset_query": {
            "database": DB_ID,
            "type": "native",
            "native": {"query": spec["sql"].strip()},
        },
        "visualization_settings": spec.get("viz") or {},
    }
    res = post("/api/card", payload)
    return res["id"]


def create_dashboard() -> int:
    res = post(
        "/api/dashboard",
        {"name": DASHBOARD_NAME, "description": DASHBOARD_DESC, "collection_id": COLLECTION_ID},
    )
    return res["id"]


def attach_cards(dashboard_id: int, specs: list[dict[str, Any]], card_ids: list[int]) -> None:
    dashcards = []
    for i, (spec, cid) in enumerate(zip(specs, card_ids)):
        row, col, sx, sy = spec["pos"]
        dashcards.append(
            {
                "id": -(i + 1),  # negative = new dashcard
                "card_id": cid,
                "dashboard_id": dashboard_id,
                "row": row,
                "col": col,
                "size_x": sx,
                "size_y": sy,
                "series": [],
                "parameter_mappings": [],
                "visualization_settings": {},
            }
        )
    put(f"/api/dashboard/{dashboard_id}", {"dashcards": dashcards})


def append_missing_widgets(dashboard_id: int) -> int:
    """Add any WIDGETS that aren't already on the dashboard (matched by card name).
    Existing dashcards are preserved as-is; new ones are appended with the positions
    specified in WIDGETS[*]['pos']. Returns number of cards added.
    """
    dash = get(f"/api/dashboard/{dashboard_id}")
    existing_dashcards = dash.get("dashcards") or []
    existing_names = {
        (dc.get("card") or {}).get("name")
        for dc in existing_dashcards
        if dc.get("card")
    }

    missing = [w for w in WIDGETS if w["name"] not in existing_names]
    if not missing:
        print("No new widgets to add — dashboard already has all WIDGETS.")
        return 0

    print(f"Adding {len(missing)} new widget(s) to dashboard {dashboard_id}:")
    new_card_ids: list[int] = []
    for spec in missing:
        cid = create_card(spec)
        new_card_ids.append(cid)
        print(f"  card {cid:>6}  {spec['display']:<7}  {spec['name']}")

    # Preserve existing dashcards verbatim (minus card payload which PUT doesn't need),
    # append new ones with user-specified positions.
    keep = []
    for dc in existing_dashcards:
        keep.append(
            {
                "id": dc["id"],
                "card_id": dc.get("card_id"),
                "dashboard_tab_id": dc.get("dashboard_tab_id"),
                "row": dc["row"],
                "col": dc["col"],
                "size_x": dc["size_x"],
                "size_y": dc["size_y"],
                "series": dc.get("series") or [],
                "parameter_mappings": dc.get("parameter_mappings") or [],
                "visualization_settings": dc.get("visualization_settings") or {},
            }
        )
    for i, (spec, cid) in enumerate(zip(missing, new_card_ids)):
        row, col, sx, sy = spec["pos"]
        keep.append(
            {
                "id": -(i + 1),
                "card_id": cid,
                "row": row,
                "col": col,
                "size_x": sx,
                "size_y": sy,
                "series": [],
                "parameter_mappings": [],
                "visualization_settings": {},
            }
        )
    put(f"/api/dashboard/{dashboard_id}", {"dashcards": keep})
    return len(missing)


def main() -> int:
    existing = find_existing_dashboard(DASHBOARD_NAME)
    if existing:
        added = append_missing_widgets(existing["id"])
        url = f"{MB_URL}/dashboard/{existing['id']}"
        print(f"\n{'Added ' + str(added) + ' widget(s).' if added else 'Nothing to do.'} → {url}")
        return 0

    print(f"Creating {len(WIDGETS)} cards …")
    card_ids: list[int] = []
    for spec in WIDGETS:
        cid = create_card(spec)
        card_ids.append(cid)
        print(f"  card {cid:>6}  {spec['display']:<7}  {spec['name']}")

    print("Creating dashboard …")
    dash_id = create_dashboard()
    print(f"  dashboard id = {dash_id}")

    print("Attaching cards to dashboard …")
    attach_cards(dash_id, WIDGETS, card_ids)

    print(f"\nDone → {MB_URL}/dashboard/{dash_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
