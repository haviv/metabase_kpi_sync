#!/usr/bin/env python3
"""Bug-quality chatbot web app.

Runs a small FastAPI server that exposes a chat UI at http://localhost:8787/.
The user can:
  * Ask free-form questions about ADO/JIRA/GitHub data in the local Postgres.
  * Ask the bot to generate a static HTML report (same style as
    analyze_bug_quality.py), saved under reports/ and served by this app.

Uses OpenAI chat-completions with function/tool calling. The LLM can introspect
the DB schema, run read-only SQL, invoke the full bug-quality analyzer, and
build arbitrary data-driven reports composed of scalars / tables / charts.

Run:
    source venv/bin/activate
    pip install -r requirements.txt
    export OPENAI_API_KEY=sk-...
    python3 chatbot_app.py        # or: uvicorn chatbot_app:app --port 8787

Env:
    OPENAI_API_KEY     required
    OPENAI_MODEL       default: gpt-5.3
    CHATBOT_PORT       default: 8787
    PG_*               Postgres creds (same as the rest of the repo)
"""

from __future__ import annotations

import json
import os
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from openai import OpenAI
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv(Path(__file__).parent / ".env")


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #


PORT = int(os.getenv("CHATBOT_PORT", "8787"))
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4-2026-03-05")
# GPT-5 reasoning effort: 'minimal' | 'low' | 'medium' | 'high'.
# 'medium' is a good default for data-analysis agents that iterate on SQL.
REASONING_EFFORT = os.getenv("OPENAI_REASONING_EFFORT", "medium")
MAX_TOOL_ITERATIONS = int(os.getenv("CHATBOT_MAX_ITERATIONS", "18"))
PUBLIC_BASE_PATH = "/" + os.getenv("CHATBOT_BASE_PATH", "").strip("/")
if PUBLIC_BASE_PATH == "/":
    PUBLIC_BASE_PATH = ""
REQUIRE_METABASE_AUTH = os.getenv(
    "CHATBOT_REQUIRE_METABASE_AUTH", "false"
).strip().lower() in {"1", "true", "yes", "on"}
METABASE_INTERNAL_URL = os.getenv(
    "METABASE_INTERNAL_URL", "http://metabase:3000"
).rstrip("/")
METABASE_AUTH_TIMEOUT = float(os.getenv("METABASE_AUTH_TIMEOUT", "5"))


def _public_url(path: str) -> str:
    """Return a URL path under the public chatbot prefix."""
    if not path.startswith("/"):
        path = "/" + path
    return f"{PUBLIC_BASE_PATH}{path}" if PUBLIC_BASE_PATH else path

def _sanitize_key(raw: Optional[str]) -> Optional[str]:
    """Strip all whitespace (incl. newlines/CR) that sometimes sneaks into
    pasted API keys. HTTP headers cannot contain '\\n', and httpx raises
    LocalProtocolError if we try."""
    if not raw:
        return raw
    cleaned = "".join(raw.split())
    return cleaned or None


OPENAI_API_KEY = _sanitize_key(os.getenv("OPENAI_API_KEY"))
if not OPENAI_API_KEY:
    print("WARNING: OPENAI_API_KEY is not set — chat endpoint will fail until it is.",
          file=sys.stderr)

PG_CONN = (
    f"postgresql://{os.getenv('PG_USERNAME')}:{os.getenv('PG_PASSWORD')}"
    f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT', '5432')}/{os.getenv('PG_DATABASE')}"
)

REPORTS_DIR = Path(__file__).parent / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

# Max rows returned to the model from ad-hoc SQL. Keeps token counts sane.
SQL_ROW_LIMIT = int(os.getenv("CHATBOT_SQL_ROW_LIMIT", "200"))


# --------------------------------------------------------------------------- #
# Engine + SQL safety
# --------------------------------------------------------------------------- #


_engine: Engine = create_engine(PG_CONN, pool_pre_ping=True)


SQL_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|truncate|alter|create|grant|revoke|"
    r"vacuum|reindex|copy|comment)\b",
    re.IGNORECASE,
)


def run_readonly_sql(sql: str, row_limit: int = SQL_ROW_LIMIT) -> dict[str, Any]:
    """Execute a read-only SQL statement and return {columns, rows, truncated}.

    Only SELECT / WITH statements are allowed. Every statement runs inside a
    read-only transaction so even SELECTs that call mutating functions fail.
    """
    trimmed = sql.strip().rstrip(";").strip()
    if not trimmed:
        return {"error": "empty SQL"}
    first = trimmed.split(None, 1)[0].lower()
    if first not in ("select", "with"):
        return {"error": "only SELECT/WITH statements are allowed"}
    if SQL_FORBIDDEN.search(trimmed):
        return {"error": "forbidden keyword detected"}

    with _engine.connect().execution_options(
        isolation_level="AUTOCOMMIT",
    ) as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        try:
            res = c.execute(text(trimmed))
        except Exception as e:  # return SQL errors to the model so it can recover
            return {"error": f"{type(e).__name__}: {e}"}
        columns = list(res.keys())
        raw_rows = res.fetchmany(row_limit + 1)
        truncated = len(raw_rows) > row_limit
        rows = [
            {col: _jsonable(v) for col, v in zip(columns, r)}
            for r in raw_rows[:row_limit]
        ]
    return {"columns": columns, "rows": rows, "truncated": truncated, "row_count": len(rows)}


def _jsonable(v: Any) -> Any:
    if isinstance(v, (datetime,)):
        return v.isoformat()
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except TypeError:
            pass
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="replace")
    if isinstance(v, (int, float, str, bool)) or v is None:
        return v
    return str(v)


# --------------------------------------------------------------------------- #
# Report builder (used by the `create_html_report` tool)
# --------------------------------------------------------------------------- #


REPORT_HTML_SHELL = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{title}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  :root {{ --bg:#0f1115; --panel:#161a22; --ink:#e7ecf3; --muted:#9aa5b5;
    --accent:#6aa9ff; --bad:#ff6b6b; --ok:#53c18d; --warn:#f0b429;
    --border:#232a36; }}
  html,body {{ background:var(--bg); color:var(--ink); margin:0;
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }}
  .wrap {{ max-width:1200px; margin:0 auto; padding:24px; }}
  h1 {{ font-size:22px; margin:0 0 4px; }}
  h2 {{ font-size:15px; margin:28px 0 10px; color:var(--muted);
    text-transform:uppercase; letter-spacing:.06em; font-weight:600; }}
  .sub {{ color:var(--muted); font-size:13px; margin-bottom:18px; }}
  .grid {{ display:grid; gap:16px; }}
  .k4 {{ grid-template-columns:repeat(4,1fr); }}
  .card {{ background:var(--panel); border:1px solid var(--border);
    border-radius:10px; padding:18px 20px; }}
  .metric {{ font-size:34px; font-weight:700; line-height:1.1; }}
  .metric small {{ display:block; color:var(--muted); font-weight:400;
    font-size:12px; margin-top:6px; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th,td {{ text-align:left; padding:6px 10px; border-bottom:1px solid var(--border); }}
  th {{ color:var(--muted); font-size:11px; text-transform:uppercase;
    letter-spacing:.06em; font-weight:600; }}
  td.n,th.n {{ text-align:right; font-variant-numeric:tabular-nums; }}
  tr:hover td {{ background:#1c2230; }}
  code {{ font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:12px;
    background:#1c2230; padding:1px 6px; border-radius:4px; }}
  a {{ color:var(--accent); text-decoration:none; }}
  a:hover {{ text-decoration:underline; }}
  p {{ line-height:1.5; }}
</style>
</head>
<body><div class="wrap">
<h1>{title}</h1>
<div class="sub">{subtitle}</div>
{body}
</div>
<script>
Chart.defaults.color = "#9aa5b5"; Chart.defaults.borderColor = "#232a36";
{chart_js}
</script>
</body></html>
"""


def _md_to_html(md: str) -> str:
    """Minimal markdown-ish to HTML (headings, bold, italic, code, lists)."""
    out = []
    in_list = False
    for line in (md or "").splitlines():
        s = line.rstrip()
        if s.startswith("### "):
            if in_list:
                out.append("</ul>"); in_list = False
            out.append(f"<h3>{_inline_md(s[4:])}</h3>")
        elif s.startswith("## "):
            if in_list:
                out.append("</ul>"); in_list = False
            out.append(f"<h2>{_inline_md(s[3:])}</h2>")
        elif s.startswith("# "):
            if in_list:
                out.append("</ul>"); in_list = False
            out.append(f"<h1>{_inline_md(s[2:])}</h1>")
        elif s.startswith("- ") or s.startswith("* "):
            if not in_list:
                out.append("<ul>"); in_list = True
            out.append(f"<li>{_inline_md(s[2:])}</li>")
        elif s == "":
            if in_list:
                out.append("</ul>"); in_list = False
            out.append("")
        else:
            if in_list:
                out.append("</ul>"); in_list = False
            out.append(f"<p>{_inline_md(s)}</p>")
    if in_list:
        out.append("</ul>")
    return "\n".join(out)


def _inline_md(s: str) -> str:
    s = re.sub(r"`([^`]+)`", r"<code>\1</code>", s)
    s = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", s)
    s = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"<i>\1</i>", s)
    s = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"<a href='\2' target='_blank'>\1</a>", s)
    return s


def build_report_html(spec: dict[str, Any]) -> str:
    """spec = {title, subtitle?, sections: [...]}
    Supported section types:
      {"type": "headline_grid", "items": [{"label":..., "value":...|sql, "accent":"bad|ok|warn"}]}
      {"type": "markdown", "content": "..."}
      {"type": "scalar", "label":..., "value":... or sql:...}
      {"type": "table", "title": "...", "sql": "...", "limit": 100, "columns": ["optional", "rename"]}
      {"type": "bar_chart", "title": "...", "sql": "...", "x": "col", "y": "col", "stacked": false}
      {"type": "line_chart", "title": "...", "sql": "...", "x": "col", "y": "col"}
      {"type": "pie_chart",  "title": "...", "sql": "...", "label": "col", "value": "col"}
    """
    title = spec.get("title") or "Untitled Report"
    subtitle = spec.get("subtitle") or ""
    sections = spec.get("sections") or []
    body_parts: list[str] = []
    chart_js_parts: list[str] = []
    chart_counter = 0

    def esc(v: Any) -> str:
        import html as _h
        return _h.escape("" if v is None else str(v))

    def _resolve_value(sec: dict) -> Any:
        if "value" in sec and sec["value"] is not None:
            return sec["value"]
        if sec.get("sql"):
            res = run_readonly_sql(sec["sql"], row_limit=2)
            if "error" in res:
                return f"SQL error: {res['error']}"
            if res["rows"]:
                first = res["rows"][0]
                return next(iter(first.values()))
        return "?"

    for sec in sections:
        t = sec.get("type")
        if t == "markdown":
            body_parts.append(f"<div class='card'>{_md_to_html(sec.get('content',''))}</div>")
        elif t == "headline_grid":
            items = sec.get("items") or []
            cards = []
            for it in items:
                val = _resolve_value(it) if "sql" in it or "value" in it else it.get("value", "?")
                accent = it.get("accent") or ""
                cls = f" {accent}" if accent in ("bad", "ok", "warn") else ""
                cards.append(
                    f"<div class='card'><div class='metric{cls}'>{esc(val)}"
                    f"<small>{esc(it.get('label',''))}</small></div></div>"
                )
            cols = max(1, min(4, len(cards)))
            body_parts.append(
                f"<div class='grid' style='grid-template-columns:repeat({cols},1fr)'>"
                + "".join(cards) + "</div>"
            )
        elif t == "scalar":
            val = _resolve_value(sec)
            accent = sec.get("accent") or ""
            cls = f" {accent}" if accent in ("bad", "ok", "warn") else ""
            body_parts.append(
                f"<div class='card'><div class='metric{cls}'>{esc(val)}"
                f"<small>{esc(sec.get('label',''))}</small></div></div>"
            )
        elif t == "table":
            if sec.get("title"):
                body_parts.append(f"<h2>{esc(sec['title'])}</h2>")
            limit = int(sec.get("limit", 100))
            res = run_readonly_sql(sec.get("sql", ""), row_limit=limit)
            if "error" in res:
                body_parts.append(f"<div class='card'>SQL error: {esc(res['error'])}</div>")
                continue
            cols = res["columns"]
            trunc_note = (
                f"<div class='sub'>Showing first {limit} rows — result was truncated.</div>"
                if res.get("truncated") else ""
            )
            head = "".join(f"<th>{esc(c)}</th>" for c in cols)
            body_rows = []
            for r in res["rows"]:
                body_rows.append(
                    "<tr>" + "".join(f"<td>{esc(r.get(c))}</td>" for c in cols) + "</tr>"
                )
            body_parts.append(
                f"<div class='card'>{trunc_note}"
                f"<table><thead><tr>{head}</tr></thead>"
                f"<tbody>{''.join(body_rows)}</tbody></table></div>"
            )
        elif t in ("bar_chart", "line_chart", "pie_chart"):
            if sec.get("title"):
                body_parts.append(f"<h2>{esc(sec['title'])}</h2>")
            res = run_readonly_sql(sec.get("sql", ""), row_limit=500)
            if "error" in res:
                body_parts.append(f"<div class='card'>SQL error: {esc(res['error'])}</div>")
                continue
            chart_counter += 1
            cid = f"chart_{chart_counter}"
            body_parts.append(f"<div class='card'><canvas id='{cid}' height='220'></canvas></div>")
            rows = res["rows"]
            if t == "pie_chart":
                label_col = sec.get("label") or (res["columns"][0] if res["columns"] else "")
                value_col = sec.get("value") or (res["columns"][1] if len(res["columns"]) > 1 else "")
                labels = [r.get(label_col) for r in rows]
                values = [r.get(value_col) for r in rows]
                chart_js_parts.append(
                    f"new Chart(document.getElementById('{cid}'), {{"
                    f"type:'doughnut', data:{{labels:{json.dumps(labels)},"
                    f"datasets:[{{data:{json.dumps(values)},"
                    f"backgroundColor:['#6aa9ff','#ff6b6b','#f0b429','#53c18d','#b28dff','#72c0ff','#ffb4a2']}}]}},"
                    f"options:{{plugins:{{legend:{{position:'bottom'}}}}}}}});"
                )
            else:
                x_col = sec.get("x") or (res["columns"][0] if res["columns"] else "")
                y_col = sec.get("y") or (res["columns"][1] if len(res["columns"]) > 1 else "")
                labels = [r.get(x_col) for r in rows]
                values = [r.get(y_col) for r in rows]
                ctype = "bar" if t == "bar_chart" else "line"
                stacked = "true" if sec.get("stacked") else "false"
                chart_js_parts.append(
                    f"new Chart(document.getElementById('{cid}'), {{"
                    f"type:'{ctype}', data:{{labels:{json.dumps(labels)},"
                    f"datasets:[{{label:{json.dumps(y_col)},"
                    f"data:{json.dumps(values)}, backgroundColor:'#6aa9ff',"
                    f"borderColor:'#6aa9ff', tension:.25}}]}},"
                    f"options:{{scales:{{x:{{stacked:{stacked}}},y:{{stacked:{stacked}}}}}}}}});"
                )
        else:
            body_parts.append(f"<div class='card'>Unknown section type: {esc(t)}</div>")

    return REPORT_HTML_SHELL.format(
        title=_html_escape(title),
        subtitle=_md_to_html(subtitle),
        body="\n".join(body_parts),
        chart_js="\n".join(chart_js_parts),
    )


def _html_escape(s: str) -> str:
    import html as _h
    return _h.escape(s or "")


# --------------------------------------------------------------------------- #
# Tool implementations
# --------------------------------------------------------------------------- #


def tool_list_tables(schema: str = "public") -> dict:
    res = run_readonly_sql(
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema='{schema}' ORDER BY table_name"
    )
    return res


def tool_describe_table(table: str, schema: str = "public") -> dict:
    # sanitize identifiers defensively
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table) or not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*", schema
    ):
        return {"error": "invalid identifier"}
    return run_readonly_sql(
        "SELECT column_name, data_type, is_nullable "
        "FROM information_schema.columns "
        f"WHERE table_schema='{schema}' AND table_name='{table}' "
        "ORDER BY ordinal_position"
    )


def tool_run_sql(sql: str, row_limit: int | None = None) -> dict:
    return run_readonly_sql(sql, row_limit=row_limit or SQL_ROW_LIMIT)


def tool_get_qa_logins(pattern: str = r"^qa$") -> dict:
    """Return QA member logins / names / emails from synced github_team_members."""
    res = run_readonly_sql(
        f"""
        SELECT DISTINCT u.login, u.name, u.email
        FROM public.github_team_members m
        JOIN public.github_teams t
          ON t.organization = m.organization AND t.slug = m.team_slug
        LEFT JOIN public.github_users u ON u.login = m.login
        WHERE t.slug ~* '{pattern}' OR COALESCE(t.name,'') ~* '{pattern}'
        ORDER BY u.name NULLS LAST
        """
    )
    return res


def tool_generate_bug_quality_report(days: int = 30) -> dict:
    """Run the full analyze_bug_quality.py pipeline (ADO + classification)
    and save its report under reports/. Returns the served URL.

    Delegated via subprocess so the importable module state (globals, config)
    stays isolated from the web server.
    """
    import subprocess
    rid = f"bug-quality-{days}d-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    out_path = REPORTS_DIR / f"{rid}.html"
    env = os.environ.copy()
    env["BUG_QUALITY_LOOKBACK_DAYS"] = str(days)
    # The analyzer always writes to bug_quality_report.html next to itself;
    # we copy it to reports/ afterwards.
    proc = subprocess.run(
        [sys.executable, str(Path(__file__).parent / "analyze_bug_quality.py")],
        env=env, capture_output=True, text=True, timeout=600,
    )
    src = Path(__file__).parent / "bug_quality_report.html"
    if not src.exists():
        return {
            "error": "analyzer did not produce bug_quality_report.html",
            "stdout": proc.stdout[-1500:], "stderr": proc.stderr[-1500:],
        }
    out_path.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
    return {
        "report_url": _public_url(f"/reports/{out_path.name}"),
        "stdout_tail": proc.stdout[-1200:],
    }


def tool_create_html_report(spec: dict) -> dict:
    """Build a custom data-driven HTML report from the given spec and save it.
    Returns {"report_url": ...}."""
    try:
        html = build_report_html(spec)
    except Exception as e:
        return {"error": f"render failed: {type(e).__name__}: {e}",
                "traceback": traceback.format_exc()[-1500:]}
    safe_title = re.sub(r"[^a-z0-9-]+", "-", (spec.get("title") or "report").lower()).strip("-")[:60]
    rid = f"{safe_title or 'report'}-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"
    out_path = REPORTS_DIR / f"{rid}.html"
    out_path.write_text(html, encoding="utf-8")
    return {"report_url": _public_url(f"/reports/{out_path.name}")}


TOOL_FUNCS = {
    "list_tables": tool_list_tables,
    "describe_table": tool_describe_table,
    "run_sql": tool_run_sql,
    "get_qa_logins": tool_get_qa_logins,
    "generate_bug_quality_report": tool_generate_bug_quality_report,
    "create_html_report": tool_create_html_report,
}


# Responses API tool schemas (flat shape — name/description/parameters at top).
# Kept descriptions tight; detailed guidance lives in the system prompt.
TOOLS_SCHEMA = [
    {
        "type": "function",
        "name": "list_tables",
        "description": "List table names in a Postgres schema. Default schema is 'public'. Call this BEFORE writing SQL against an unfamiliar table.",
        "parameters": {
            "type": "object",
            "properties": {"schema": {"type": "string"}},
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "describe_table",
        "description": "Return columns + data types for a table. Call this whenever you are not 100% sure of a table's columns before writing SQL.",
        "parameters": {
            "type": "object",
            "properties": {
                "table": {"type": "string"},
                "schema": {"type": "string"},
            },
            "required": ["table"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "run_sql",
        "description": (
            "Execute a read-only SELECT/WITH query against Postgres. "
            "Returns {columns, rows, truncated, row_count}. Only SELECT/WITH allowed. "
            "Iterate: if a query returns no rows or weird data, refine and rerun."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "A single SELECT or WITH statement."},
                "row_limit": {"type": "integer", "description": "Max rows to return (default 200)."},
            },
            "required": ["sql"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "get_qa_logins",
        "description": (
            "Return QA team member logins/names/emails from synced GitHub teams. "
            "Pattern is a POSIX regex matched against team slug/name (default '^qa$' = the `qa` team)."
        ),
        "parameters": {
            "type": "object",
            "properties": {"pattern": {"type": "string"}},
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "generate_bug_quality_report",
        "description": (
            "Run the full bug-quality analyzer pipeline (pulls CreatedBy + PR relations from ADO, "
            "filters to QA-filed internal bugs, classifies false positives, emits static HTML). "
            "Use when the user asks for 'the bug quality report' or 'QA false-positive analysis'. "
            "Returns a report_url under /reports/. Takes ~20–40 s on a 60-day window."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "lookback window, default 30"},
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "create_html_report",
        "description": (
            "Build a CUSTOM static HTML report from a structured spec. "
            "Use for ANY ad-hoc report that isn't the canned bug-quality one. "
            "Section types: headline_grid (KPI cards), markdown (prose), scalar, "
            "table (SQL-backed), bar_chart / line_chart / pie_chart (SQL-backed). "
            "Every data section carries its own SQL. Prefer 4–8 sections with a "
            "mix of scalars, one chart, and one table. Returns {report_url}. "
            "Verify your SQL with run_sql FIRST before putting it in the spec."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "description": "Report spec with title, subtitle, sections[].",
                    "properties": {
                        "title": {"type": "string"},
                        "subtitle": {"type": "string"},
                        "sections": {
                            "type": "array",
                            "items": {"type": "object"},
                        },
                    },
                    "required": ["title", "sections"],
                }
            },
            "required": ["spec"],
            "additionalProperties": False,
        },
    },
]


# --------------------------------------------------------------------------- #
# Chat loop
# --------------------------------------------------------------------------- #


SYSTEM_PROMPT = r"""You are a data analyst assistant for Pathlock's engineering
telemetry. You have tools that query a local Postgres DB (Azure DevOps + JIRA
+ GitHub + Copilot metrics, all already synced) and can build static HTML
reports. You must USE the tools for every factual claim. Never guess numbers.

This document is the source of truth for table names, column names, and
business definitions. Read it before deciding whether you need
describe_table.

================================================================================
DATA COVERAGE (as of latest sync)
================================================================================
- bugs table:         9,078 rows (7,371 internal, 1,707 customer-reported)
- work_items table:   30,615 rows (Task 16k, Bug 8.9k, PBI 3.6k, Feature 634,
                      Issue Report 585, Epic 167, ...)
- change_history:     ~215k rows (mostly System.State and System.IterationPath)
- Time range of bugs: 2022-04-06 to today
- Distinct area_paths: 44

================================================================================
MAIN TABLE: public.bugs   (one row per ADO Bug work item)
================================================================================
Columns (all in the public schema — just write `public.bugs` in SQL):
  id                         integer PK (= ADO work item id)
  parent_issue               integer (id of parent work_item), nullable
  title                      varchar
  description                text
  assigned_to                varchar — current ADO displayName (NOT the filer)
  severity                   varchar — '1'..'4' (P1/P2/P3/P4). '99999' is a
                             small legacy set, ignore or bucket as "other".
  state                      varchar — workflow state, see BUG STATES below
  customer_name              varchar — empty/null for internal bugs; a value
                             like "ZIM" means the bug was reported by that
                             customer.
  area_path                  varchar — the ADO team path, e.g.
                             "PathlockGRC\\Application Access Governance\\Connector Framework"
                             (note the backslash separator in source data).
  created_date               timestamp
  changed_date               timestamp — last mutation of the record
  iteration_path             varchar — sprint folder, e.g.
                             "PathlockGRC\\PLC Engineering Sprints\\2026 Sprint 32 - April 1st"
  hotfix_delivered_version   varchar
  target_date                timestamp
  hf_status                  varchar — hotfix status
  hf_requested_versions      varchar
IMPORTANT: bugs does NOT have created_by. If you need the filer, use ADO's
API (not available from this DB) — or for recent bugs, the chatbot's report
pipeline does that via the analyze_bug_quality.py script.

================================================================================
BUG STATES — the workflow of a bug
================================================================================
Historical counts (full DB, not last 30d):
  Removed           3,266   (triage rejected / invalid / dupe)
  Done              2,492   (shipped)
  QA Completed      2,067   (QA signed off)
  Not reproduced      462   (QA could not reproduce; an FP signal)
  New                 222
  Approved            211   (ready to be picked up)
  Ready for QA        100
  ON-HOLD              75
  In Progress          61   (dev is actively working)
  Waiting for PR       35   (dev finished code, PR pending)
  Reproduction         32
  In QA                17
  Dev Completed        16
  Issues Found         16   (QA found issues, bounced back)
  Committed             6   (code merged)

Canonical buckets we use in reporting:
  TERMINAL_STATES = ('Removed','Not reproduced','QA Completed','Done')
  DEV_STATES      = ('In Progress','Waiting for PR','Dev Completed','Committed')

State transitions are recorded in change_history with:
  field_changed = 'System.State', old_value = <prior>, new_value = <new>.
Use change_history to ask "when did bug X reach state Y?" — bugs.state is
only the CURRENT state.

================================================================================
change_history   (one row per field change on any work item)
================================================================================
Columns:
  record_id      integer — the bug/work_item id
  table_name     varchar — 'bugs' for bug rows, 'work_items' for PBI/Task/etc.
  field_changed  varchar — see distribution below
  old_value      varchar
  new_value      varchar
  changed_by     varchar — ADO displayName
  changed_date   timestamp

Distribution of field_changed over the last 30 days:
  System.State                        ~15,290
  System.IterationPath                 ~4,910
  Custom.HFstatus                        ~366
  Custom.HFrequestedversions             ~199
  Microsoft.VSTS.Scheduling.TargetDate   ~148

So: only a small subset of fields is tracked — mostly state and iteration.
Assignment changes and title edits are NOT in here.

Common patterns:
  # When did bug 12345 first reach "Ready for QA"?
  SELECT MIN(changed_date)
  FROM public.change_history
  WHERE table_name='bugs' AND field_changed='System.State'
    AND record_id=12345 AND new_value='Ready for QA';

  # Bugs that ever passed through any DEV state
  SELECT DISTINCT record_id FROM public.change_history
  WHERE table_name='bugs' AND field_changed='System.State'
    AND new_value IN ('In Progress','Waiting for PR','Dev Completed','Committed');

================================================================================
work_items   (PBIs, Tasks, Features, Epics, Issue Reports — everything non-bug)
================================================================================
Same shape as bugs PLUS richer fields: work_item_type, effort, effort_dev_estimate,
effort_dev_actual, qa_effort_estimation, qa_effort_actual, tshirt_estimation,
parent_work_item, ticket_type, freshdesk_ticket, target_version, tags, connector,
created_by, blocker, business_value.
  work_item_type in: Task, Bug, Product Backlog Item, Feature, Issue Report,
  Product New Feature Request, Epic, Test Case, ...
Note: work_items.created_by IS populated; bugs.created_by does NOT exist.

================================================================================
bugs_relations   (parent / related links between bugs and work_items)
================================================================================
  bug_id, issue_id, type in ('parent','related')
Only captures ADO hierarchical + "Related" links. Does NOT capture PR/commit
ArtifactLinks — those are only retrievable via the ADO API.

================================================================================
TEAM / PEOPLE STRUCTURE
================================================================================

There are TWO team universes and they do not share identifiers:

(A) ADO teams live inside bugs.area_path — a "\"-separated path. 44 distinct
    values. Top-10 by bug count last 90 days:
       PathlockGRC\\Application Access Governance\\Connector Framework     (~331)
       PathlockGRC\\Platform\\Nexus                                        (~234)
       PathlockGRC\\Application Access Governance\\Certifications          (~223)
       PathlockGRC\\Application Access Governance\\Compliant Provisioning  (~170)
       PathlockGRC\\Application Access Governance\\Elevated Access Mgmt    (~149)
       PathlockGRC\\Application Access Governance\\Access Risk Analysis    (~111)
       Flex Connectors\\Connectors CSharp                                  (~89)
       PathlockGRC\\Continuous Controls Monitoring                         (~83)
       Flex Connectors\\Connectors Fusion                                  (~83)
       Flex Connectors\\Connectors Crew                                    (~64)
    Tip: SPLIT_PART(area_path, '\\', 1) = top-level ("PathlockGRC" /
    "Flex Connectors"); SPLIT_PART(area_path, '\\', -1) = leaf team.
    Remember to escape the backslash in SQL strings: '\\'.

(B) GitHub teams in github_teams / github_team_members. 180 unique org
    members across 39 teams. Biggest teams:
       all-users(180) pathlock_qa_team(90) qa(45) connectors(43)
       developers(34) plc-pr-reviewers(22) plc-qa-automation(14)
       release-managers(14) pathlock-certificaiton(11) devops(10) ...
    Do NOT confuse `qa` with `pathlock_qa_team`:
      - `qa` (45 members) is the actual QA org team.
      - `pathlock_qa_team` (90 members) is polluted with dev leads who sit
        in both `developers` and `pathlock_qa_team`. When the user says
        "QA", filter on team slug = 'qa' or call get_qa_logins() with its
        default pattern '^qa$'.

Matching ADO users ↔ GitHub users is imperfect because only a handful
(~55/201) of GitHub profiles expose their real name and only ~9 expose a
public email. So:
  - bugs.assigned_to / change_history.changed_by are ADO displayNames.
  - github_users.{login, name, email} is often sparse.
  - The get_qa_logins tool returns the triple for QA members — use it for
    filtering, but don't expect every ADO name to resolve to a GitHub login.

================================================================================
GITHUB + COPILOT + TEST TABLES
================================================================================
pr_metrics_daily(metric_date, organization, repository, team_slug, metric_name, metric_value)
  metric_name in: team_size, merged_prs_count, avg_pr_cycle_time_hours,
                  avg_time_to_first_review_hours.
  repository is often literal 'all' (per-team rollup). Real repo names appear
  under specific rows; inspect with run_sql if unsure.

pr_metrics_member_daily(metric_date, organization, repository, member_login,
                        team_slug, merged_prs_count)

copilot_metrics_daily(metric_date, organization, team_slug, metric_name, metric_value)
  metric_name includes: acceptance_rate_pct, code_suggestions, code_acceptances,
  code_lines_suggested, code_lines_accepted, team_size, team_active_pct,
  team_engaged_pct, total_active_users, total_engaged_users, chat_to_code_pct,
  pr_summaries, ide_chats, dotcom_chats, ide_chat_engaged_users, ...

github_test_runs(run_id, workflow_name, repository, branch, commit_sha,
                 run_number, run_started_at, run_completed_at, conclusion,
                 test_run_id, test_run_name, computer_name, total_tests,
                 passed_tests, failed_tests, skipped_tests,
                 total_duration_seconds, artifact_name, created_date, team)
github_test_results(run_id, test_id, execution_id, test_name, test_class,
                    outcome, duration_ms, start_time, end_time, error_message,
                    stack_trace, stdout, created_date)

sprints(id, name, path, start_date, finish_date, state, ...)
history_snapshots(snapshot_date, name, number) — daily KPI snapshots.

================================================================================
BUSINESS DEFINITIONS — REUSE, DON'T REINVENT
================================================================================
- INTERNAL BUG          customer_name IS NULL OR TRIM(customer_name) = ''
- CUSTOMER BUG          NOT internal (customer_name is set, e.g. 'ZIM')
- TERMINAL STATE        state IN ('Removed','Not reproduced','QA Completed','Done')
- DEV STATE             state IN ('In Progress','Waiting for PR','Dev Completed','Committed')
- QA-FILED              creator is a member of the GitHub team 'qa'. Use
                        get_qa_logins() to retrieve; or in SQL join
                        github_team_members where team_slug='qa'.
- P1/P2 BUG             severity IN ('1','2')
- RC bug / hotfix bug   title ILIKE '%RC1%' / hf_status NOT NULL
- FALSE-POSITIVE BUG    For the dedicated bug-quality analyzer:
                        state='Removed' OR state='Not reproduced' OR
                        (state IN ('QA Completed','Done') AND no dev-state
                        transition AND no linked PR/commit in ADO). Use the
                        generate_bug_quality_report tool for this — it pulls
                        ArtifactLinks from ADO which this DB does not store.

================================================================================
HOW TO WORK — AGENT LOOP RULES
================================================================================
This is a multi-step agent loop. Expect to make several tool calls per turn.

1. CLARIFY ONCE, AT MOST. If the request is missing a critical dimension
   (time window, team, which metric), ask ONE concise question and stop
   (return a short text reply, no tool calls). Otherwise proceed.
   Default time window = last 30 days if not specified.
2. PREFER THIS PROMPT OVER describe_table. It already lists the real column
   names and allowed values for bugs, work_items, change_history, the
   GitHub tables, pr/copilot/test metrics. Only call describe_table for
   tables NOT listed here, or when a run_sql error suggests a column is off.
3. TEST SMALL, THEN SCALE. First run_sql with LIMIT 5-20 to see shape, then
   expand to the real aggregation. If a query returns 0 rows, check:
     - state spelling (exact case, 'QA Completed' not 'QA completed')
     - customer_name empty string vs NULL (always guard with both)
     - area_path backslash escaping in literals
     - date-window off by a day
   Fix and retry. Do NOT report 0 rows as the answer without verifying.
4. FOR REPORTS (create_html_report): test EVERY SQL with run_sql first, so
   the rendered report has no SQL errors. Pick 4-8 sections mixing a KPI
   (scalar or headline_grid), at least one chart, and at least one table.
   Give a short, dated subtitle ("Last 14 days, internal bugs only").
5. FOR THE CANNED bug-quality report, call generate_bug_quality_report —
   do NOT try to reimplement the false-positive logic in SQL (it needs ADO
   ArtifactLinks which aren't in this DB).
6. STOP when you have enough to answer. Never say "I think" — always ground
   numbers in a tool result.

================================================================================
OUTPUT STYLE
================================================================================
- Ask mode: 3-6 sentences, optionally one small markdown table. Quote the
  scope on every number ("396 of 864 internal bugs in the last 60 days, 45.8%").
- Report mode: short confirmation text + markdown link
  "Report → [Open report](/reports/xyz.html)" + a 3-4 row summary table of
  the headline numbers.
- Never dump raw tool traces in the reply; the UI already shows them in an
  expandable section.

================================================================================
SAFETY
================================================================================
- run_sql is read-only (SELECT/WITH only, READ ONLY transaction). You
  cannot mutate data.
- Do not echo the user's API keys, tokens, or connection strings.
"""


class ChatRequest(BaseModel):
    session_id: Optional[str] = None
    message: str
    mode: str = "ask"  # "ask" or "report"


class ChatReply(BaseModel):
    session_id: str
    reply: str
    report_url: Optional[str] = None
    tool_trace: list = []


# SESSIONS[session_id] = list of Responses-API input items:
#   {"role":"user","content":"..."}          — user messages
#   {"role":"assistant","content":"..."}     — assistant final text (for history)
#   reasoning / function_call items          — returned verbatim from resp.output
#   {"type":"function_call_output", ...}     — our tool execution results
# Reasoning items MUST be preserved across iterations for GPT-5 to reason well.
SESSIONS: dict[str, list[dict]] = {}


def _log(msg: str) -> None:
    print(f"[chatbot] {msg}", flush=True)


def _getattr(item: Any, key: str, default: Any = None) -> Any:
    """Uniform accessor for both Pydantic objects and plain dicts."""
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def _extract_final_text(resp: Any, output_items: list[Any]) -> Optional[str]:
    """Pull the final assistant text from a Responses-API result.

    Strategy:
      1. Use resp.output_text (SDK convenience — concatenates all output_text
         content blocks across message items).
      2. Fall back to walking output_items by hand for older/alt SDKs.
    """
    ot = getattr(resp, "output_text", None)
    if isinstance(ot, str) and ot.strip():
        return ot

    chunks: list[str] = []
    for item in output_items:
        if _getattr(item, "type") != "message":
            continue
        if _getattr(item, "role") not in (None, "assistant"):
            continue
        content = _getattr(item, "content") or []
        for c in content:
            ctype = _getattr(c, "type")
            if ctype in ("output_text", "text"):
                txt = _getattr(c, "text")
                if txt:
                    chunks.append(txt)
    if chunks:
        return "\n".join(chunks)
    return None


def _item_to_dict(item: Any) -> dict:
    """Serialize a Responses-API output item back to a plain dict so we can
    send it as input on the next call. Pydantic models have .model_dump()."""
    if hasattr(item, "model_dump"):
        return item.model_dump(exclude_none=True)
    if isinstance(item, dict):
        return item
    return json.loads(json.dumps(item, default=str))


def run_chat(
    session: list[dict], user_message: str, mode: str
) -> tuple[str, Optional[str], list[dict]]:
    """Drive the Responses-API tool loop for one user turn. Returns
    (final_text, report_url_if_any, tool_trace)."""
    client = OpenAI(api_key=OPENAI_API_KEY)

    mode_hint = (
        "[MODE=REPORT — the user wants a static HTML report. Favor "
        "generate_bug_quality_report for the canned one, or create_html_report "
        "for anything custom. Verify the SQLs with run_sql first.]"
        if mode == "report"
        else "[MODE=ASK — reply in chat with a concise, grounded answer. "
             "Only build a report if explicitly requested.]"
    )
    session.append({"role": "user", "content": f"{mode_hint}\n\n{user_message}"})

    trace: list[dict] = []
    report_url: Optional[str] = None

    for iteration in range(1, MAX_TOOL_ITERATIONS + 1):
        _log(f"iter {iteration}: calling {OPENAI_MODEL} (effort={REASONING_EFFORT}, session items={len(session)})")
        kwargs: dict[str, Any] = {
            "model": OPENAI_MODEL,
            "input": session,
            "tools": TOOLS_SCHEMA,
            "instructions": SYSTEM_PROMPT,
            "tool_choice": "auto",
            "parallel_tool_calls": True,
        }
        # reasoning_effort is only accepted by reasoning-family models. If the
        # server rejects it (e.g. user pointed OPENAI_MODEL at gpt-4o), retry
        # without it once and remember.
        if REASONING_EFFORT:
            kwargs["reasoning"] = {"effort": REASONING_EFFORT}
        try:
            resp = client.responses.create(**kwargs)
        except Exception as e:  # noqa: BLE001
            if "reasoning" in str(e).lower() and "reasoning" in kwargs:
                _log(f"  model rejected reasoning param; retrying without it")
                kwargs.pop("reasoning", None)
                resp = client.responses.create(**kwargs)
            else:
                raise

        output_items = list(resp.output or [])
        # Every output item (reasoning + function_call + message) must be
        # appended to the next input unchanged. This is what lets GPT-5 carry
        # its chain-of-thought across tool rounds.
        for item in output_items:
            session.append(_item_to_dict(item))

        # Collect function calls from this round (may be 0, 1, or many).
        tool_calls = [it for it in output_items if _getattr(it, "type") == "function_call"]
        final_text = _extract_final_text(resp, output_items)

        if not tool_calls:
            if final_text is None:
                # Nothing to reply with AND no tool calls. Dump what we got so
                # we can see why: usually it's a content-filter refusal, a
                # reasoning-only output, or an unexpected item shape.
                dump = []
                for it in output_items:
                    dump.append({
                        "type": _getattr(it, "type"),
                        "role": _getattr(it, "role"),
                        "keys": list((it.keys() if isinstance(it, dict) else it.model_dump().keys()) if hasattr(it, "model_dump") or isinstance(it, dict) else []),
                    })
                _log(f"  NO final text AND no tool calls. output items: {dump}")
                _log(f"  full resp preview: {_preview(resp, 800)}")
                return (
                    "(Model returned no tool calls and no text. Check server logs "
                    "for the raw output — most likely a content filter or a "
                    "rate-limit / model-availability issue.)",
                    report_url,
                    trace,
                )
            _log(f"  no tool calls → final text ({len(final_text)} chars)")
            return final_text, report_url, trace

        _log(f"  {len(tool_calls)} tool call(s): "
             + ", ".join(tc.name for tc in tool_calls))

        for tc in tool_calls:
            name = _getattr(tc, "name")
            raw_args = _getattr(tc, "arguments", "") or ""
            call_id = _getattr(tc, "call_id") or _getattr(tc, "id")
            try:
                args = json.loads(raw_args) if raw_args else {}
            except json.JSONDecodeError:
                args = {}
            fn = TOOL_FUNCS.get(name)
            if fn is None:
                result: Any = {"error": f"unknown tool: {name}"}
            else:
                try:
                    result = fn(**args)
                except TypeError as e:
                    result = {"error": f"bad args to {name}: {e}"}
                except Exception as e:  # noqa: BLE001
                    result = {"error": f"{type(e).__name__}: {e}",
                              "hint": "Fix the inputs and retry."}
            if isinstance(result, dict) and result.get("report_url"):
                report_url = result["report_url"]
            _log(f"    {name}({_short_args(args)}) → {_preview(result, 160)}")
            trace.append({
                "iteration": iteration,
                "tool": name,
                "args": args,
                "result_preview": _preview(result),
            })
            # Feed tool result back into the model's input for the next round.
            session.append({
                "type": "function_call_output",
                "call_id": call_id,
                "output": json.dumps(result, default=str)[:60000],
            })

    _log(f"  stopped after {MAX_TOOL_ITERATIONS} iterations without final text")
    return (
        f"(Stopped after {MAX_TOOL_ITERATIONS} tool-call iterations. "
        "The model kept requesting tools without producing a final answer — "
        "try narrowing the request.)",
        report_url,
        trace,
    )


def _short_args(args: dict) -> str:
    s = json.dumps(args, default=str)
    return s if len(s) <= 120 else s[:120] + "…"


def _preview(result: Any, limit: int = 500) -> str:
    """JSON-preview a value, handling Pydantic models via model_dump()."""
    if hasattr(result, "model_dump"):
        try:
            result = result.model_dump(exclude_none=True)
        except Exception:  # noqa: BLE001
            pass
    try:
        s = json.dumps(result, default=str, ensure_ascii=False)
    except Exception:  # noqa: BLE001
        s = str(result)
    return s if len(s) <= limit else s[:limit] + "…"


# --------------------------------------------------------------------------- #
# FastAPI app
# --------------------------------------------------------------------------- #


app = FastAPI(title="Bug Quality Chatbot")
app.mount("/reports", StaticFiles(directory=str(REPORTS_DIR)), name="reports")
if PUBLIC_BASE_PATH:
    app.mount(
        _public_url("/reports"),
        StaticFiles(directory=str(REPORTS_DIR)),
        name="prefixed_reports",
    )


def _is_health_path(path: str) -> bool:
    return path in {"/health", "/ready", _public_url("/health"), _public_url("/ready")}


def _metabase_current_user(cookie_header: str) -> dict[str, Any]:
    res = requests.get(
        f"{METABASE_INTERNAL_URL}/api/user/current",
        headers={"Cookie": cookie_header},
        timeout=METABASE_AUTH_TIMEOUT,
    )
    if res.status_code != 200:
        raise HTTPException(status_code=401, detail="Metabase login required")
    try:
        user = res.json()
    except ValueError as e:
        raise HTTPException(
            status_code=502, detail="Metabase returned an invalid auth response"
        ) from e
    if not user or not user.get("id"):
        raise HTTPException(status_code=401, detail="Metabase login required")
    return user


@app.middleware("http")
async def require_metabase_auth(request: Request, call_next):
    if not REQUIRE_METABASE_AUTH or _is_health_path(request.url.path):
        return await call_next(request)

    cookie_header = request.headers.get("cookie")
    if not cookie_header:
        if request.method == "GET":
            return RedirectResponse("/auth/login")
        return JSONResponse({"detail": "Metabase login required"}, status_code=401)

    try:
        request.state.metabase_user = _metabase_current_user(cookie_header)
    except HTTPException as e:
        if request.method == "GET" and e.status_code == 401:
            return RedirectResponse("/auth/login")
        return JSONResponse({"detail": e.detail}, status_code=e.status_code)
    except requests.RequestException as e:
        return JSONResponse(
            {"detail": f"Metabase auth check failed: {type(e).__name__}"},
            status_code=502,
        )

    return await call_next(request)


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML)


@app.post("/chat", response_model=ChatReply)
def chat(req: ChatRequest) -> ChatReply:
    if not OPENAI_API_KEY:
        raise HTTPException(500, "OPENAI_API_KEY is not configured on the server.")
    sid = req.session_id or uuid4().hex
    session = SESSIONS.setdefault(sid, [])
    try:
        reply, report_url, trace = run_chat(session, req.message, req.mode)
    except Exception as e:
        raise HTTPException(500, f"LLM call failed: {type(e).__name__}: {e}")
    return ChatReply(
        session_id=sid, reply=reply, report_url=report_url, tool_trace=trace
    )


@app.post("/reset")
def reset(payload: dict) -> JSONResponse:
    sid = payload.get("session_id")
    if sid and sid in SESSIONS:
        SESSIONS.pop(sid)
    return JSONResponse({"ok": True})


@app.get("/ready")
def ready() -> JSONResponse:
    return JSONResponse({"ok": True})


@app.get("/health")
def health() -> JSONResponse:
    """End-to-end sanity probe: DB reachable, tools importable, OpenAI creds
    present and a trivial model call succeeds. Use this to diagnose the chat
    endpoint without sending messages through the UI."""
    info: dict[str, Any] = {
        "model": OPENAI_MODEL,
        "reasoning_effort": REASONING_EFFORT,
        "max_iterations": MAX_TOOL_ITERATIONS,
        "tools": [t["name"] for t in TOOLS_SCHEMA],
    }
    try:
        res = run_readonly_sql("SELECT 1 AS ok")
        info["db_ok"] = res.get("rows") == [{"ok": 1}]
    except Exception as e:  # noqa: BLE001
        info["db_ok"] = False
        info["db_error"] = f"{type(e).__name__}: {e}"
    info["openai_key_set"] = bool(OPENAI_API_KEY)
    if OPENAI_API_KEY:
        try:
            client = OpenAI(api_key=OPENAI_API_KEY)
            probe_kwargs: dict[str, Any] = {
                "model": OPENAI_MODEL,
                "input": [{"role": "user", "content": "Reply with exactly: OK"}],
                "instructions": "You are a health probe. Answer tersely.",
            }
            if REASONING_EFFORT:
                probe_kwargs["reasoning"] = {"effort": "minimal"}
            try:
                probe = client.responses.create(**probe_kwargs)
            except Exception as e:  # noqa: BLE001
                if "reasoning" in str(e).lower() and "reasoning" in probe_kwargs:
                    probe_kwargs.pop("reasoning", None)
                    probe = client.responses.create(**probe_kwargs)
                else:
                    raise
            info["openai_ok"] = True
            info["openai_reply"] = (probe.output_text or "").strip()[:40]
        except Exception as e:  # noqa: BLE001
            info["openai_ok"] = False
            info["openai_error"] = f"{type(e).__name__}: {e}"
    return JSONResponse(info)


if PUBLIC_BASE_PATH:
    app.add_api_route(PUBLIC_BASE_PATH, index, methods=["GET"], response_class=HTMLResponse)
    app.add_api_route(f"{PUBLIC_BASE_PATH}/", index, methods=["GET"], response_class=HTMLResponse)
    app.add_api_route(_public_url("/chat"), chat, methods=["POST"], response_model=ChatReply)
    app.add_api_route(_public_url("/reset"), reset, methods=["POST"])
    app.add_api_route(_public_url("/ready"), ready, methods=["GET"])
    app.add_api_route(_public_url("/health"), health, methods=["GET"])


# --------------------------------------------------------------------------- #
# Frontend (inlined single-file HTML)
# --------------------------------------------------------------------------- #


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Bug Quality Chatbot</title>
<script src="https://cdn.jsdelivr.net/npm/marked@12.0.2/marked.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/dompurify@3.0.9/dist/purify.min.js"></script>
<style>
  :root { --bg:#f8fafc; --panel:#ffffff; --ink:#374151; --muted:#6b7280;
    --accent:#509ee3; --accent-soft:#e9f4ff; --border:#e5e7eb;
    --shadow:0 1px 2px rgba(16,24,40,.05),0 8px 24px rgba(16,24,40,.06);
    --user:#eef6ff; --bot:#ffffff; --code:#f3f6f9; }
  html,body { height:100%; margin:0; background:var(--bg); color:var(--ink);
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }
  body { background:linear-gradient(180deg,#fff 0,#f8fafc 160px); }
  .app { max-width:1040px; margin:0 auto; height:100vh; display:flex;
    flex-direction:column; padding:22px 28px 24px; box-sizing:border-box; gap:16px; }
  header { display:flex; align-items:flex-start; justify-content:space-between; gap:16px;
    border-bottom:1px solid var(--border); padding-bottom:14px; }
  h1 { font-size:20px; line-height:1.2; color:#263445; margin:0 0 4px; font-weight:700; }
  .modes { display:flex; background:#f3f6f9; border:1px solid var(--border);
    border-radius:9px; overflow:hidden; font-size:13px; box-shadow:inset 0 1px 0 rgba(255,255,255,.7); }
  .modes button { background:transparent; color:#516173; border:0;
    padding:8px 14px; cursor:pointer; transition:background .15s,color .15s; }
  .modes button:hover { background:#edf2f7; color:#263445; }
  .modes button.active { background:var(--accent); color:#fff; font-weight:600; }
  .reset { background:#fff; color:#516173; border:1px solid var(--border);
    border-radius:8px; padding:7px 11px; cursor:pointer; font-size:12px;
    box-shadow:0 1px 2px rgba(16,24,40,.04); }
  .reset:hover { border-color:#cbd5e1; color:#263445; }
  .chat { flex:1; overflow-y:auto; background:#fbfcfe; border:1px solid var(--border);
    border-radius:14px; padding:18px; display:flex; flex-direction:column; gap:12px;
    box-shadow:var(--shadow); }
  .msg { max-width:82%; padding:11px 15px; border-radius:14px; line-height:1.5; font-size:14px;
    word-wrap:break-word; overflow-wrap:anywhere; box-shadow:0 1px 2px rgba(16,24,40,.04); }
  .msg.user { align-self:flex-end; background:var(--user); border:1px solid #cfe7ff; color:#1f3b57; }
  .msg.bot { align-self:flex-start; background:var(--bot); border:1px solid var(--border);
    max-width:95%; color:#374151; }
  .msg.sys { align-self:center; font-size:12px; color:var(--muted); background:transparent;
    border:0; padding:2px 0; }
  .msg p { margin:0 0 8px; }
  .msg p:last-child { margin-bottom:0; }
  .msg ul, .msg ol { margin:6px 0 8px; padding-left:22px; }
  .msg li { margin:2px 0; }
  .msg h1, .msg h2, .msg h3 { margin:10px 0 6px; line-height:1.25; }
  .msg h1 { font-size:18px; } .msg h2 { font-size:16px; } .msg h3 { font-size:14px; }
  .msg pre { background:var(--code); padding:8px 10px; border-radius:8px; overflow:auto;
    border:1px solid var(--border); margin:6px 0; font-size:12px; }
  .msg code { background:var(--code); color:#263445; padding:1px 6px; border-radius:5px; font-size:12px;
    font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; }
  .msg pre code { background:transparent; padding:0; }
  .msg a { color:var(--accent); }
  .msg blockquote { border-left:3px solid var(--border); margin:6px 0; padding:2px 10px;
    color:var(--muted); }
  .msg .table-wrap { overflow-x:auto; margin:8px 0; border:1px solid var(--border);
    border-radius:8px; }
  .msg table { border-collapse:collapse; font-size:13px; width:100%;
    font-variant-numeric:tabular-nums; }
  .msg th, .msg td { border-bottom:1px solid var(--border); padding:6px 10px; text-align:left;
    vertical-align:top; }
  .msg tbody tr:last-child td { border-bottom:0; }
  .msg th { background:#f3f6f9; color:var(--muted); font-size:11px; text-transform:uppercase;
    letter-spacing:.06em; font-weight:600; white-space:nowrap; }
  .msg tbody tr:nth-child(even) td { background:#fafcff; }
  .msg tbody tr:hover td { background:#f1f7fd; }
  .report-card { margin-top:8px; background:var(--accent-soft); border:1px solid #b9dcff;
    border-radius:10px; padding:10px 14px; display:flex; align-items:center; gap:12px; }
  .report-card a { color:var(--accent); font-weight:600; text-decoration:none; }
  .form { display:flex; gap:10px; background:#fff; border:1px solid var(--border);
    border-radius:14px; padding:10px; box-shadow:var(--shadow); }
  .form textarea { flex:1; background:#fff; color:var(--ink); border:0;
    border-radius:10px; padding:10px 12px; font-size:14px; resize:none; line-height:1.4;
    min-height:48px; max-height:160px; font-family:inherit; outline:none; }
  .form textarea::placeholder { color:#94a3b8; }
  .form button { background:var(--accent); color:#fff; border:0; border-radius:10px;
    padding:0 20px; font-size:14px; font-weight:600; cursor:pointer;
    box-shadow:0 2px 6px rgba(80,158,227,.25); }
  .form button:hover { background:#3f91d5; }
  .form button:disabled { opacity:.4; cursor:progress; }
  .trace-toggle { margin-top:6px; font-size:11px; color:var(--muted); cursor:pointer; }
  .trace { font-size:11px; color:var(--muted); background:#f8fafc; border:1px solid var(--border);
    border-radius:8px; padding:8px; margin-top:6px; display:none; white-space:pre-wrap; }
  .trace.open { display:block; }
  .hint { color:var(--muted); font-size:12px; }
  .typing { color:var(--muted); font-size:12px; padding:4px 0; }
</style>
</head>
<body>
<div class="app">
  <header>
    <div>
      <h1>Bug Quality Chatbot</h1>
      <div class="hint">Postgres + ADO + GitHub · model: <span id="model"></span></div>
    </div>
    <div style="display:flex;gap:8px;align-items:center">
      <div class="modes">
        <button id="mode-ask" class="active" data-mode="ask">Ask questions</button>
        <button id="mode-report" data-mode="report">Generate report</button>
      </div>
      <button class="reset" id="reset">Reset</button>
    </div>
  </header>

  <div class="chat" id="chat">
    <div class="msg bot">
      Hi! I'm wired into your local Postgres (ADO + JIRA + GitHub data) and I
      can generate static HTML reports. Two modes:
      <ul>
        <li><b>Ask questions</b> — short answers in chat.</li>
        <li><b>Generate report</b> — I'll build a static HTML page under
            <code>__BASE_PATH__/reports/</code> and give you a link.</li>
      </ul>
      Try: <i>"QA bug false-positive rate, last 30 days, per team"</i> or
      <i>"Generate a report on PR throughput by repo for the last 8 weeks"</i>.
    </div>
  </div>

  <div class="form">
    <textarea id="msg" placeholder="Type your question or request. Shift+Enter for newline."></textarea>
    <button id="send">Send</button>
  </div>
</div>

<script>
const chat = document.getElementById('chat');
const msgInput = document.getElementById('msg');
const sendBtn = document.getElementById('send');
const modeBtns = document.querySelectorAll('.modes button');
const resetBtn = document.getElementById('reset');
const modelLabel = document.getElementById('model');
modelLabel.textContent = "__MODEL__";
const basePath = "__BASE_PATH__";

let sessionId = null;
let mode = 'ask';

modeBtns.forEach(b => b.addEventListener('click', () => {
  modeBtns.forEach(x => x.classList.remove('active'));
  b.classList.add('active');
  mode = b.dataset.mode;
  msgInput.placeholder = mode === 'report'
    ? "Describe the report you want (title, time window, teams, slices)…"
    : "Ask a question about bugs, PRs, teams, copilot, tests…";
}));

resetBtn.addEventListener('click', async () => {
  if (sessionId) { await fetch(`${basePath}/reset`, {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({session_id: sessionId})}); }
  sessionId = null;
  chat.querySelectorAll('.msg:not(:first-child)').forEach(n => n.remove());
});

function addMsg(role, html) {
  const el = document.createElement('div');
  el.className = 'msg ' + role;
  el.innerHTML = html;
  chat.appendChild(el);
  chat.scrollTop = chat.scrollHeight;
  return el;
}

// Full markdown -> sanitized HTML via marked + DOMPurify. Handles GFM tables,
// fenced code, lists, headings, etc. Falls back to escaped text if the CDN
// libs failed to load.
if (window.marked && typeof window.marked.setOptions === 'function') {
  window.marked.setOptions({ gfm: true, breaks: true, headerIds: false, mangle: false });
}
function escapeHtml(s) {
  return (s || '').replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[c]));
}
function mdToHtml(md) {
  if (!md) return '';
  let html;
  try {
    html = window.marked ? window.marked.parse(md) : '<p>'+escapeHtml(md).replace(/\n/g,'<br>')+'</p>';
  } catch (e) {
    html = '<p>'+escapeHtml(md).replace(/\n/g,'<br>')+'</p>';
  }
  if (window.DOMPurify) {
    html = window.DOMPurify.sanitize(html, { ADD_ATTR: ['target'] });
  }
  // Wrap tables so wide ones can scroll horizontally instead of overflowing.
  html = html.replace(/<table([\s\S]*?)<\/table>/g,
    (m) => '<div class="table-wrap">'+m+'</div>');
  // Force external links to open in a new tab.
  html = html.replace(/<a /g, '<a target="_blank" rel="noopener" ');
  return html;
}

async function send() {
  const text = msgInput.value.trim();
  if (!text) return;
  msgInput.value = '';
  addMsg('user', mdToHtml(text));
  sendBtn.disabled = true;
  const typing = addMsg('bot', '<span class="typing">thinking…</span>');
  try {
    const res = await fetch(`${basePath}/chat`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({session_id: sessionId, message: text, mode})
    });
    if (!res.ok) {
      const err = await res.text();
      typing.innerHTML = '<b>Error:</b> ' + err;
      return;
    }
    const data = await res.json();
    sessionId = data.session_id;
    let html = mdToHtml(data.reply || '(no reply)');
    if (data.report_url) {
      html += `<div class="report-card"><a href="${data.report_url}" target="_blank">Open static report: ${data.report_url}</a></div>`;
    }
    if (data.tool_trace && data.tool_trace.length) {
      const traceId = 'trace-' + Math.random().toString(36).slice(2,8);
      html += `<div class="trace-toggle" onclick="document.getElementById('${traceId}').classList.toggle('open')">tool trace (${data.tool_trace.length} call${data.tool_trace.length>1?'s':''})</div>`;
      html += `<div class="trace" id="${traceId}">` +
        data.tool_trace.map(t => t.tool + '(' + JSON.stringify(t.args) + ') → ' + t.result_preview).join('\n\n') +
        '</div>';
    }
    typing.innerHTML = html;
  } catch (e) {
    typing.innerHTML = '<b>Network error:</b> ' + e.message;
  } finally {
    sendBtn.disabled = false;
    msgInput.focus();
  }
}

sendBtn.addEventListener('click', send);
msgInput.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); }
});
msgInput.focus();
</script>
</body>
</html>
""".replace("__MODEL__", OPENAI_MODEL).replace("__BASE_PATH__", PUBLIC_BASE_PATH)


# --------------------------------------------------------------------------- #
# Entry
# --------------------------------------------------------------------------- #


def main() -> int:
    import uvicorn
    print(f"Starting Bug Quality Chatbot on http://localhost:{PORT}")
    print(f"Model: {OPENAI_MODEL}")
    print(f"Reports dir: {REPORTS_DIR}")
    uvicorn.run("chatbot_app:app", host="0.0.0.0", port=PORT, reload=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
