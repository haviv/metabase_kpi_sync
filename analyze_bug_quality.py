#!/usr/bin/env python3
"""Bug-quality / QA false-positive analyzer for the last 30 days.

Produces a self-contained static HTML report at bug_quality_report.html.

Composite metric ("false-positive rate"):
  denominator = bugs created in the last N days that reached a terminal state
                (Removed, Not reproduced, QA Completed, Done).
  numerator   = same bugs where ANY of the following is true:
    B1  state = 'Removed'
    B2  state = 'Not reproduced'
    B3  state in ('QA Completed','Done') AND *no evidence of a fix*, where
        "evidence of a fix" = the bug ever passed through a dev state
        (In Progress / Waiting for PR / Dev Completed / Committed) OR has a
        linked PR OR has a linked commit in ADO (ArtifactLink).

Each "bad" bug is attributed to a SINGLE primary reason, in the order
B1 > B2 > B3, so the breakdown sums to the composite numerator.

Why B3 requires BOTH a missing dev state AND a missing PR/commit
  Some teams skip intermediate states (e.g. New -> Done directly) while still
  linking a real GitHub PR/commit. Those bugs did have dev work; only if
  neither signal is present do we treat it as "closed without a fix landing".

Also reported
  * Duplicate candidates (simple heuristic: same area_path + near-identical
    title prefix) within the last 30 days.
  * Breakdown per team (area_path), severity, and assignee.
  * Weekly trend.

Dependencies: already in requirements.txt (requests, sqlalchemy, psycopg2,
python-dotenv).
"""

from __future__ import annotations

import base64
import html
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(Path(__file__).parent / ".env")

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

LOOKBACK_DAYS = int(os.getenv("BUG_QUALITY_LOOKBACK_DAYS", "30"))
OUTPUT_FILE = Path(__file__).parent / "bug_quality_report.html"

# QA filter — regex matched against github_teams.slug / github_teams.name.
# Default = exactly the `qa` team (45 members). The larger `pathlock_qa_team`
# is intentionally excluded because it contains ~25 members who are also in
# `developers` (dev leads, PR reviewers, etc.) — not pure QA.
QA_TEAM_PATTERN = os.getenv("QA_TEAM_PATTERN", r"^qa$")
# Users in any team matching this pattern are excluded from the QA set even
# if they also appear in a QA team (extra safety net).
QA_TEAM_EXCLUDE_PATTERN = os.getenv("QA_TEAM_EXCLUDE_PATTERN", r"^developers$")
# Extra emails/names to treat as QA (comma-separated, case-insensitive)
QA_EXTRA_EMAILS = {
    e.strip().lower() for e in os.getenv("QA_EXTRA_EMAILS", "").split(",") if e.strip()
}
QA_EXTRA_NAMES = {
    n.strip().lower() for n in os.getenv("QA_EXTRA_NAMES", "").split(",") if n.strip()
}

TERMINAL_STATES = ("Removed", "Not reproduced", "QA Completed", "Done")
DEV_STATES = ("In Progress", "Waiting for PR", "Dev Completed", "Committed")

ADO_ORG = os.environ["ADO_ORGANIZATION"]
ADO_PAT = os.environ["ADO_PERSONAL_ACCESS_TOKEN"]
ADO_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Basic " + base64.b64encode(f":{ADO_PAT}".encode()).decode(),
}

PG_CONN = (
    f"postgresql://{os.getenv('PG_USERNAME')}:{os.getenv('PG_PASSWORD')}"
    f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT', '5432')}/{os.getenv('PG_DATABASE')}"
)


# --------------------------------------------------------------------------- #
# Data extraction
# --------------------------------------------------------------------------- #


def fetch_bugs(engine) -> list[dict]:
    # Filter out customer-reported bugs — we're measuring internal bug quality.
    # A bug is "internal" when customer_name is NULL or empty.
    sql = text(
        """
        SELECT id, title, state, severity, assigned_to, area_path,
               customer_name, created_date, changed_date
        FROM public.bugs
        WHERE created_date >= NOW() - (:days || ' days')::interval
          AND (customer_name IS NULL OR TRIM(customer_name) = '')
        ORDER BY id
        """
    )
    with engine.connect() as c:
        rows = c.execute(sql, {"days": LOOKBACK_DAYS}).mappings().all()
    return [dict(r) for r in rows]


def _norm_name(s: str | None) -> str:
    """Lowercase + strip all non-alphanumerics. 'Tomer Ansher' -> 'tomeransher'."""
    if not s:
        return ""
    return "".join(ch.lower() for ch in s if ch.isalnum())


def build_qa_identity_set(engine) -> tuple[set[str], set[str], set[str], list[dict]]:
    """Pull QA identities from the synced GitHub data.

    Returns (qa_emails, qa_names_normalized, qa_login_fragments, raw_rows).
      qa_emails           lowercased full emails ('x@pathlock.com')
      qa_names_normalized lowercased, alnum-only display names
      qa_login_fragments  alnum-only login minus 'pathlock'/'plc'/digits suffix
    """
    sql = text(
        """
        WITH qa_members AS (
            SELECT DISTINCT m.login
            FROM public.github_team_members m
            JOIN public.github_teams t
              ON t.organization = m.organization AND t.slug = m.team_slug
            WHERE t.slug ~* :pat OR COALESCE(t.name,'') ~* :pat
        ),
        dev_members AS (
            SELECT DISTINCT m.login
            FROM public.github_team_members m
            JOIN public.github_teams t
              ON t.organization = m.organization AND t.slug = m.team_slug
            WHERE t.slug ~* :exc OR COALESCE(t.name,'') ~* :exc
        )
        SELECT DISTINCT u.login, u.name, u.email
        FROM qa_members q
        LEFT JOIN public.github_users u ON u.login = q.login
        WHERE q.login NOT IN (SELECT login FROM dev_members)
        """
    )
    with engine.connect() as c:
        rows = [
            dict(r)
            for r in c.execute(
                sql, {"pat": QA_TEAM_PATTERN, "exc": QA_TEAM_EXCLUDE_PATTERN}
            ).mappings()
        ]

    emails = set(QA_EXTRA_EMAILS)
    names = set(QA_EXTRA_NAMES)
    logins: set[str] = set()
    for r in rows:
        if r.get("email"):
            emails.add(r["email"].lower())
        if r.get("name"):
            names.add(_norm_name(r["name"]))
        login = r.get("login") or r.get("m_login") or ""
        if login:
            # strip noise so 'barak-zabari' -> 'barakzabari', 'tomeransherpathlock' -> 'tomeransher'
            frag = _norm_name(login)
            for suffix in ("pathlock", "plc", "pl", "pathlockgrc"):
                if frag.endswith(suffix):
                    frag = frag[: -len(suffix)]
            # drop trailing digits ('liorlevi1' -> 'liorlevi')
            while frag and frag[-1].isdigit():
                frag = frag[:-1]
            if len(frag) >= 4:  # ignore too-short fragments
                logins.add(frag)
    return emails, names, logins, rows


def is_qa_creator(
    display_name: str | None,
    email: str | None,
    qa_emails: set[str],
    qa_names: set[str],
    qa_login_frags: set[str],
) -> bool:
    email_l = (email or "").strip().lower()
    if email_l and email_l in qa_emails:
        return True
    # local part of email -> 'firstname.lastname' or similar
    if email_l and "@" in email_l:
        local = _norm_name(email_l.split("@", 1)[0])
        # direct name hit
        if local in qa_names:
            return True
        # login-fragment match (either direction: login subset of name or v.v.)
        for frag in qa_login_frags:
            if frag and (frag in local or local in frag):
                return True
    norm_dn = _norm_name(display_name)
    if norm_dn and norm_dn in qa_names:
        return True
    if norm_dn:
        for frag in qa_login_frags:
            if frag and (frag in norm_dn or norm_dn in frag):
                return True
    return False


def fetch_ado_bug_metadata(bug_ids: list[int]) -> dict[int, dict]:
    """Fetch System.CreatedBy + ArtifactLink relations for each bug id.

    Returns {bug_id: {"display_name": str, "email": str,
                      "pr": [urls], "commit": [urls]}}
    """
    out: dict[int, dict] = {}
    if not bug_ids:
        return out
    batch_size = 200
    total = (len(bug_ids) + batch_size - 1) // batch_size
    for idx in range(0, len(bug_ids), batch_size):
        batch = bug_ids[idx : idx + batch_size]
        url = (
            f"https://dev.azure.com/{ADO_ORG}/_apis/wit/workitems"
            f"?ids={','.join(map(str, batch))}"
            f"&$expand=relations&errorPolicy=omit&api-version=7.0"
        )
        r = requests.get(url, headers=ADO_HEADERS, timeout=60)
        r.raise_for_status()
        for item in [it for it in (r.json().get("value") or []) if it]:
            wid = item["id"]
            f = item.get("fields") or {}
            cb = f.get("System.CreatedBy") or {}
            dn = cb.get("displayName") if isinstance(cb, dict) else str(cb)
            email = cb.get("uniqueName") if isinstance(cb, dict) else None
            rec = {
                "display_name": dn or "",
                "email": (email or "").strip(),
                "pr": [],
                "commit": [],
            }
            for rel in item.get("relations") or []:
                kind = _is_source_control_link(rel)
                if kind:
                    rec[kind].append(rel.get("url", ""))
            out[wid] = rec
        done = idx // batch_size + 1
        print(f"  ADO meta: batch {done}/{total} ({len(batch)} items)")
        if done < total:
            time.sleep(0.4)
    return out


def fetch_state_history(engine, bug_ids: list[int]) -> dict[int, list[str]]:
    """Return {bug_id: [states it ever reached]}. Includes current state via
    bugs.state so a bug that was created directly in a dev state is not
    mis-classified because of missing history entries."""
    if not bug_ids:
        return {}
    sql = text(
        """
        SELECT record_id, new_value
        FROM public.change_history
        WHERE table_name='bugs'
          AND field_changed='System.State'
          AND record_id = ANY(:ids)
        """
    )
    out: dict[int, set[str]] = defaultdict(set)
    with engine.connect() as c:
        for row in c.execute(sql, {"ids": bug_ids}).mappings():
            out[row["record_id"]].add(row["new_value"])
    return {k: sorted(v) for k, v in out.items()}


def _is_source_control_link(rel: dict) -> str | None:
    """Return 'pr' | 'commit' | None for an ADO `relations[]` entry."""
    if rel.get("rel") != "ArtifactLink":
        return None
    url = rel.get("url") or ""
    name = (rel.get("attributes") or {}).get("name") or ""
    # PR: ADO Git, GitHub, BitBucket
    if (
        "PullRequestId" in url
        or "/PullRequest/" in url
        or "Pull Request" in name
    ):
        return "pr"
    # Commit
    if "/Commit/" in url or "Commit" == name or "Commit" in name:
        return "commit"
    return None




# --------------------------------------------------------------------------- #
# Classification
# --------------------------------------------------------------------------- #


def classify(
    bug: dict,
    ever_states: set[str],
    sc_links: dict[str, list[str]],
) -> tuple[bool, str | None, dict[str, bool]]:
    """Return (is_false_positive, primary_reason, all_buckets).

    sc_links = {"pr": [...], "commit": [...]} from ADO ArtifactLink relations.
    A bug is considered to have "a fix landed" if it has a PR *or* a commit link.
    """
    state = bug["state"]
    had_dev_state = any(s in ever_states for s in DEV_STATES)
    has_sc_link = bool(sc_links.get("pr")) or bool(sc_links.get("commit"))
    has_evidence_of_fix = had_dev_state or has_sc_link
    buckets = {
        "B1_removed": state == "Removed",
        "B2_not_reproduced": state == "Not reproduced",
        "B3_closed_no_fix_evidence": (
            state in ("QA Completed", "Done") and not has_evidence_of_fix
        ),
    }
    primary = None
    for key in ("B1_removed", "B2_not_reproduced", "B3_closed_no_fix_evidence"):
        if buckets[key]:
            primary = key
            break
    return any(buckets.values()), primary, buckets


def find_duplicate_clusters(bugs: list[dict]) -> list[list[dict]]:
    """Simple heuristic: bugs with the same area_path whose title's first 60
    non-bracket chars (lowercased, whitespace-collapsed) are identical."""
    def norm(title: str) -> str:
        t = (title or "").lower()
        # drop common prefix tags like "[RC1]", "[2026.1.1]"
        while t.startswith("["):
            end = t.find("]")
            if end == -1:
                break
            t = t[end + 1 :].lstrip()
        t = " ".join(t.split())
        return t[:60]

    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for b in bugs:
        key = ((b.get("area_path") or "").lower(), norm(b.get("title") or ""))
        if not key[1]:
            continue
        buckets[key].append(b)
    return [v for v in buckets.values() if len(v) > 1]


# --------------------------------------------------------------------------- #
# HTML rendering
# --------------------------------------------------------------------------- #


HTML_TMPL = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Bug Quality Report — last {days} days</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --bg: #0f1115; --panel: #161a22; --ink: #e7ecf3; --muted: #9aa5b5;
    --accent: #6aa9ff; --bad: #ff6b6b; --ok: #53c18d; --warn: #f0b429;
    --border: #232a36;
  }}
  html, body {{ background: var(--bg); color: var(--ink); margin: 0;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }}
  .wrap {{ max-width: 1280px; margin: 0 auto; padding: 24px; }}
  h1 {{ font-size: 22px; margin: 0 0 4px; }}
  h2 {{ font-size: 16px; margin: 32px 0 12px; color: var(--muted); font-weight: 600;
    text-transform: uppercase; letter-spacing: .06em; }}
  .sub {{ color: var(--muted); margin-bottom: 18px; font-size: 13px; }}
  .grid {{ display: grid; gap: 16px; }}
  .k4 {{ grid-template-columns: repeat(4, 1fr); }}
  .k2 {{ grid-template-columns: 2fr 1fr; }}
  .card {{ background: var(--panel); border: 1px solid var(--border);
    border-radius: 10px; padding: 18px 20px; }}
  .metric {{ font-size: 40px; font-weight: 700; line-height: 1.1; }}
  .metric small {{ font-size: 13px; color: var(--muted); display: block; font-weight: 400;
    margin-top: 6px; }}
  .bad {{ color: var(--bad); }} .ok {{ color: var(--ok); }} .warn {{ color: var(--warn); }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th, td {{ text-align: left; padding: 6px 10px; border-bottom: 1px solid var(--border); }}
  th {{ color: var(--muted); font-weight: 600; font-size: 11px; text-transform: uppercase;
    letter-spacing: .06em; }}
  td.n, th.n {{ text-align: right; font-variant-numeric: tabular-nums; }}
  tr:hover td {{ background: #1c2230; }}
  .pct-bar {{ background: #232a36; border-radius: 4px; height: 6px; overflow: hidden;
    display: inline-block; width: 80px; vertical-align: middle; margin-left: 8px; }}
  .pct-bar > span {{ display: block; height: 100%; background: var(--bad); }}
  .pill {{ display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 11px;
    background: #232a36; color: var(--muted); }}
  .pill.bad {{ background: #3a1f23; color: #ff9a9a; }}
  .pill.ok {{ background: #1f3a2b; color: #8ce0b3; }}
  details {{ margin-top: 12px; }}
  details > summary {{ cursor: pointer; color: var(--muted); font-size: 13px; }}
  a {{ color: var(--accent); text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .legend {{ font-size: 12px; color: var(--muted); margin-top: 8px; }}
</style>
</head>
<body>
<div class="wrap">

<h1 id="top">Bug Quality Report</h1>
<div class="sub">
  Scope: <b>internal</b> bugs created in the last {days} days (since {start_date}),
  filtered to <b>QA-filed only</b> via GitHub team membership
  ({qa_member_count} QA members; pattern /{qa_pattern}/ minus /{qa_exclude}/).
  &nbsp;·&nbsp;Generated {generated_at}.
</div>

<div class="grid k4">
  <div class="card">
    <div class="metric">{total_30d}</div>
    <small>Internal bugs created in window
      <br>(customer-reported bugs excluded)</small>
  </div>
  <div class="card">
    <div class="metric">{terminal_n}</div>
    <small>Of those, reached a terminal state
      ({terminal_pct}% of created)</small>
  </div>
  <div class="card">
    <div class="metric bad">{fp_pct}%</div>
    <small>Composite false-positive rate
      <br>({fp_n} of {terminal_n} closed bugs)</small>
  </div>
  <div class="card">
    <div class="metric warn">{dup_clusters}</div>
    <small>Duplicate clusters (heuristic, {dup_bugs} bugs in total)</small>
  </div>
</div>

<h2>Composite breakdown — primary reason per bug</h2>
<div class="grid k2">
  <div class="card"><canvas id="primaryPie" height="260"></canvas></div>
  <div class="card">
    <table>
      <thead><tr><th>Primary reason</th><th class="n">Bugs</th><th class="n">% of closed</th><th class="n">% of FP</th></tr></thead>
      <tbody>{primary_rows}</tbody>
    </table>
    <div class="legend">
      B1 Removed · B2 Not reproduced · B3 Closed as QA Completed/Done with no
      fix evidence (never reached a dev state AND no PR/commit linked in ADO).
      Priority B1 &gt; B2 &gt; B3 so each bug appears once. Bugs that went
      straight New→Done but have a PR or commit linked are NOT counted as FP.
    </div>
  </div>
</div>

<h2>Overlap — how many buckets each bad bug trips</h2>
<div class="card">
  <table>
    <thead><tr><th>Bucket</th><th class="n">Matches (bugs can match &gt;1)</th></tr></thead>
    <tbody>{bucket_overlap_rows}</tbody>
  </table>
</div>

<h2>Terminal state × had-dev-work</h2>
<div class="grid k2">
  <div class="card"><canvas id="terminalBar" height="260"></canvas></div>
  <div class="card">
    <table>
      <thead><tr><th>Terminal state</th><th class="n">Total</th><th class="n">No dev work</th><th class="n">Had dev work</th><th class="n">No PR/commit</th></tr></thead>
      <tbody>{terminal_rows}</tbody>
    </table>
  </div>
</div>

<h2>By team — false-positive rate</h2>
<div class="sub">Click a team to drill into its problematic bugs and reasons.</div>
<div class="card">
  <table>
    <thead><tr><th>Team (area_path)</th><th class="n">Closed bugs</th><th class="n">False positives</th><th class="n">% FP</th><th class="n">B1</th><th class="n">B2</th><th class="n">B3</th></tr></thead>
    <tbody>{team_rows}</tbody>
  </table>
</div>
<div>{team_detail_blocks}</div>

<h2>By severity</h2>
<div class="grid k2">
  <div class="card"><canvas id="sevBar" height="220"></canvas></div>
  <div class="card">
    <table>
      <thead><tr><th>Severity</th><th class="n">Closed</th><th class="n">False positives</th><th class="n">% FP</th></tr></thead>
      <tbody>{severity_rows}</tbody>
    </table>
  </div>
</div>

<h2>By assignee (top 20 by FP count)</h2>
<div class="sub">Assignee = current ADO AssignedTo. We don't sync the filer today, so this
reflects who the bug ended up with, not who opened it.</div>
<div class="card">
  <table>
    <thead><tr><th>Assignee</th><th class="n">Closed</th><th class="n">False positives</th><th class="n">% FP</th></tr></thead>
    <tbody>{assignee_rows}</tbody>
  </table>
</div>

<h2>Weekly trend</h2>
<div class="card"><canvas id="weeklyLine" height="220"></canvas></div>

<h2>Sample of false-positive bugs ({fp_sample_n} of {fp_n})</h2>
<div class="card">
  <table>
    <thead><tr><th>ID</th><th>State</th><th>Primary</th><th>Team</th><th>Assignee</th><th>Title</th></tr></thead>
    <tbody>{sample_rows}</tbody>
  </table>
  <details><summary>Show all {fp_n} false-positive bugs</summary>
    <table style="margin-top:10px">
      <thead><tr><th>ID</th><th>State</th><th>Primary</th><th>Team</th><th>Assignee</th><th>Title</th></tr></thead>
      <tbody>{all_fp_rows}</tbody>
    </table>
  </details>
</div>

<h2>Unmatched bug creators — skipped by QA filter</h2>
<div class="sub">People who filed internal bugs but didn't match any QA-team member via
email/name/login. Add emails or names to env vars
<code>QA_EXTRA_EMAILS</code> / <code>QA_EXTRA_NAMES</code> (comma-separated) to
include them on the next run.</div>
<div class="card">
  <details>
    <summary>Show top 50 unmatched creators ({unmatched_total} total)</summary>
    <table style="margin-top:10px">
      <thead><tr><th>Creator (name &lt;email&gt;)</th><th class="n">Bugs filed</th></tr></thead>
      <tbody>{unmatched_rows}</tbody>
    </table>
  </details>
</div>

<h2>Duplicate candidates ({dup_clusters} clusters, {dup_bugs} bugs)</h2>
<div class="sub">Heuristic: same area_path + identical first 60 chars of title (with
bracket prefixes stripped). Not authoritative; sanity-check before acting.</div>
<div class="card">
  <details open>
    <summary>Show clusters</summary>
    <table style="margin-top:10px">
      <thead><tr><th>Cluster key</th><th>IDs</th><th>Title</th></tr></thead>
      <tbody>{dup_rows}</tbody>
    </table>
  </details>
</div>

</div>

<script>
  const primaryData = {primary_data_json};
  const terminalData = {terminal_data_json};
  const severityData = {severity_data_json};
  const weeklyData = {weekly_data_json};

  Chart.defaults.color = "#9aa5b5";
  Chart.defaults.borderColor = "#232a36";

  new Chart(document.getElementById("primaryPie"), {{
    type: "doughnut",
    data: {{
      labels: primaryData.labels,
      datasets: [{{ data: primaryData.values,
        backgroundColor: ["#ff6b6b","#f0b429","#6aa9ff","#b28dff"] }}]
    }},
    options: {{ plugins: {{ legend: {{ position: "bottom" }} }} }}
  }});

  new Chart(document.getElementById("terminalBar"), {{
    type: "bar",
    data: {{
      labels: terminalData.labels,
      datasets: [
        {{ label: "No dev work", data: terminalData.no_dev, backgroundColor: "#ff6b6b" }},
        {{ label: "Had dev work", data: terminalData.had_dev, backgroundColor: "#53c18d" }},
      ]
    }},
    options: {{ scales: {{ x: {{ stacked: true }}, y: {{ stacked: true }} }} }}
  }});

  new Chart(document.getElementById("sevBar"), {{
    type: "bar",
    data: {{
      labels: severityData.labels,
      datasets: [
        {{ label: "Closed", data: severityData.closed, backgroundColor: "#6aa9ff" }},
        {{ label: "False positives", data: severityData.fp, backgroundColor: "#ff6b6b" }},
      ]
    }}
  }});

  new Chart(document.getElementById("weeklyLine"), {{
    type: "line",
    data: {{
      labels: weeklyData.labels,
      datasets: [
        {{ label: "Closed", data: weeklyData.closed, borderColor: "#6aa9ff", tension: .25 }},
        {{ label: "False positives", data: weeklyData.fp, borderColor: "#ff6b6b", tension: .25 }},
      ]
    }}
  }});
</script>
</body>
</html>
"""


def esc(s: Any) -> str:
    return html.escape("" if s is None else str(s))


def pct_bar(p: float) -> str:
    p = max(0.0, min(100.0, p))
    return f'<span class="pct-bar"><span style="width:{p:.1f}%"></span></span>'


def render(
    bugs: list[dict],
    bugs_terminal: list[dict],
    classifications: dict[int, tuple[bool, str | None, dict[str, bool]]],
    pr_links: dict[int, dict[str, list[str]]],
    history: dict[int, list[str]],
    dup_clusters: list[list[dict]],
    *,
    unmatched_creators: Counter | None = None,
    qa_member_count: int = 0,
) -> str:
    # -- headline numbers --
    total_30d = len(bugs)
    terminal_n = len(bugs_terminal)
    fp_bugs = [b for b in bugs_terminal if classifications[b["id"]][0]]
    fp_n = len(fp_bugs)
    fp_pct = (fp_n / terminal_n * 100) if terminal_n else 0
    terminal_pct = (terminal_n / total_30d * 100) if total_30d else 0

    # -- primary reason breakdown --
    primary_counts = Counter(classifications[b["id"]][1] for b in fp_bugs)
    primary_labels = {
        "B1_removed": "B1 — Removed",
        "B2_not_reproduced": "B2 — Not reproduced",
        "B3_closed_no_fix_evidence": "B3 — Closed, no fix evidence",
    }
    primary_rows = []
    pie_labels, pie_values = [], []
    for key in ("B1_removed", "B2_not_reproduced", "B3_closed_no_fix_evidence"):
        n = primary_counts.get(key, 0)
        of_closed = (n / terminal_n * 100) if terminal_n else 0
        of_fp = (n / fp_n * 100) if fp_n else 0
        primary_rows.append(
            f"<tr><td>{primary_labels[key]}</td>"
            f"<td class='n'>{n}</td>"
            f"<td class='n'>{of_closed:.1f}%{pct_bar(of_closed)}</td>"
            f"<td class='n'>{of_fp:.1f}%</td></tr>"
        )
        pie_labels.append(primary_labels[key])
        pie_values.append(n)

    # -- bucket overlap --
    overlap = Counter()
    for b in fp_bugs:
        _, _, bkt = classifications[b["id"]]
        for k, v in bkt.items():
            if v:
                overlap[k] += 1
    bucket_overlap_rows = "".join(
        f"<tr><td>{primary_labels[k]}</td><td class='n'>{overlap.get(k, 0)}</td></tr>"
        for k in ("B1_removed", "B2_not_reproduced", "B3_closed_no_fix_evidence")
    )

    # -- terminal state x dev work / no PR --
    term_stats: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "no_dev": 0, "had_dev": 0, "no_pr": 0})
    for b in bugs_terminal:
        st = b["state"]
        ever = set(history.get(b["id"], []))
        had_dev = any(s in ever for s in DEV_STATES)
        links = pr_links.get(b["id"]) or {"pr": [], "commit": []}
        has_fix = bool(links.get("pr")) or bool(links.get("commit"))
        term_stats[st]["total"] += 1
        term_stats[st]["no_dev"] += 0 if had_dev else 1
        term_stats[st]["had_dev"] += 1 if had_dev else 0
        if not has_fix:
            term_stats[st]["no_pr"] += 1
    term_order = [s for s in TERMINAL_STATES if s in term_stats]
    terminal_rows = "".join(
        f"<tr><td>{esc(s)}</td><td class='n'>{term_stats[s]['total']}</td>"
        f"<td class='n'>{term_stats[s]['no_dev']}</td>"
        f"<td class='n'>{term_stats[s]['had_dev']}</td>"
        f"<td class='n'>{term_stats[s]['no_pr']}</td></tr>"
        for s in term_order
    )
    terminal_data = {
        "labels": term_order,
        "no_dev": [term_stats[s]["no_dev"] for s in term_order],
        "had_dev": [term_stats[s]["had_dev"] for s in term_order],
    }

    # -- per team (summary + bucket counts + detail blocks) --
    team_closed: Counter = Counter()
    team_fp: Counter = Counter()
    team_buckets: dict[str, Counter] = defaultdict(Counter)
    team_fp_bugs: dict[str, list[dict]] = defaultdict(list)
    for b in bugs_terminal:
        team = b["area_path"] or "(unknown)"
        team_closed[team] += 1
        is_fp, primary, _ = classifications[b["id"]]
        if is_fp:
            team_fp[team] += 1
            team_buckets[team][primary] += 1
            team_fp_bugs[team].append(b)

    def team_anchor(team: str) -> str:
        # stable anchor id
        return "team-" + "".join(
            ch if ch.isalnum() else "-" for ch in (team or "unknown")
        ).strip("-").lower()[:80]

    team_rows_list = []
    for team, closed in team_closed.most_common():
        fp = team_fp.get(team, 0)
        rate = (fp / closed * 100) if closed else 0
        b = team_buckets.get(team, Counter())
        aid = team_anchor(team)
        team_rows_list.append(
            f"<tr><td><a href='#{aid}'>{esc(team)}</a></td>"
            f"<td class='n'>{closed}</td>"
            f"<td class='n'>{fp}</td>"
            f"<td class='n'>{rate:.1f}%{pct_bar(rate)}</td>"
            f"<td class='n'>{b.get('B1_removed', 0)}</td>"
            f"<td class='n'>{b.get('B2_not_reproduced', 0)}</td>"
            f"<td class='n'>{b.get('B3_closed_no_fix_evidence', 0)}</td></tr>"
        )
    team_rows = "".join(team_rows_list)

    # per-team drill-down blocks
    detail_parts: list[str] = []
    for team, _ in team_closed.most_common():
        if team not in team_fp_bugs:
            continue
        rows_html = []
        # sort bugs by bucket priority then id desc
        order = {"B1_removed": 0, "B2_not_reproduced": 1, "B3_closed_no_fix_evidence": 2}
        for bug in sorted(
            team_fp_bugs[team],
            key=lambda x: (order.get(classifications[x["id"]][1] or "", 9), -x["id"]),
        ):
            pid = classifications[bug["id"]][1]
            ado_url = f"https://dev.azure.com/{ADO_ORG}/_workitems/edit/{bug['id']}"
            rows_html.append(
                f"<tr><td><a href='{ado_url}' target='_blank'>{bug['id']}</a></td>"
                f"<td><span class='pill'>{esc(bug['state'])}</span></td>"
                f"<td><span class='pill bad'>{primary_labels.get(pid or '', '?')}</span></td>"
                f"<td>{esc(bug['severity'])}</td>"
                f"<td>{esc(bug['assigned_to'])}</td>"
                f"<td>{esc(bug['title'])}</td></tr>"
            )
        aid = team_anchor(team)
        detail_parts.append(
            f"<h2 id='{aid}'>{esc(team)} — {len(team_fp_bugs[team])} problematic bugs "
            f"<a href='#top' style='font-size:12px;font-weight:400'>↑ back to top</a></h2>"
            "<div class='card'><details open><summary>Show/hide bugs</summary>"
            "<table style='margin-top:10px'><thead><tr>"
            "<th>ID</th><th>State</th><th>Reason</th><th>Sev</th>"
            "<th>Assignee</th><th>Title</th></tr></thead><tbody>"
            + "".join(rows_html) + "</tbody></table></details></div>"
        )
    team_detail_blocks = "".join(detail_parts)

    # -- per severity --
    sev_closed: Counter = Counter()
    sev_fp: Counter = Counter()
    for b in bugs_terminal:
        sev = b["severity"] or "(n/a)"
        sev_closed[sev] += 1
        if classifications[b["id"]][0]:
            sev_fp[sev] += 1
    sev_order = sorted(sev_closed.keys())
    severity_rows = "".join(
        f"<tr><td>{esc(s)}</td><td class='n'>{sev_closed[s]}</td>"
        f"<td class='n'>{sev_fp.get(s, 0)}</td>"
        f"<td class='n'>{(sev_fp.get(s, 0) / sev_closed[s] * 100) if sev_closed[s] else 0:.1f}%</td></tr>"
        for s in sev_order
    )
    severity_data = {
        "labels": sev_order,
        "closed": [sev_closed[s] for s in sev_order],
        "fp": [sev_fp.get(s, 0) for s in sev_order],
    }

    # -- per assignee --
    asg_closed: Counter = Counter()
    asg_fp: Counter = Counter()
    for b in bugs_terminal:
        a = b["assigned_to"] or "(unassigned)"
        asg_closed[a] += 1
        if classifications[b["id"]][0]:
            asg_fp[a] += 1
    assignees_sorted = sorted(asg_fp.items(), key=lambda x: x[1], reverse=True)[:20]
    assignee_rows = "".join(
        f"<tr><td>{esc(a)}</td><td class='n'>{asg_closed[a]}</td>"
        f"<td class='n'>{fp}</td>"
        f"<td class='n'>{(fp / asg_closed[a] * 100) if asg_closed[a] else 0:.1f}%{pct_bar((fp / asg_closed[a] * 100) if asg_closed[a] else 0)}</td></tr>"
        for a, fp in assignees_sorted
    )

    # -- weekly trend --
    weekly_closed: Counter = Counter()
    weekly_fp: Counter = Counter()
    for b in bugs_terminal:
        d = b["changed_date"] or b["created_date"]
        if d is None:
            continue
        # ISO week start Monday
        monday = (d - timedelta(days=d.weekday())).date()
        weekly_closed[monday] += 1
        if classifications[b["id"]][0]:
            weekly_fp[monday] += 1
    weeks_sorted = sorted(weekly_closed.keys())
    weekly_data = {
        "labels": [w.isoformat() for w in weeks_sorted],
        "closed": [weekly_closed[w] for w in weeks_sorted],
        "fp": [weekly_fp.get(w, 0) for w in weeks_sorted],
    }

    # -- sample rows + all fp rows --
    def fp_row(b: dict) -> str:
        pid = classifications[b["id"]][1]
        pid_label = primary_labels.get(pid or "", "?")
        ado_url = f"https://dev.azure.com/{ADO_ORG}/_workitems/edit/{b['id']}"
        return (
            f"<tr><td><a href='{ado_url}' target='_blank'>{b['id']}</a></td>"
            f"<td><span class='pill'>{esc(b['state'])}</span></td>"
            f"<td><span class='pill bad'>{pid_label}</span></td>"
            f"<td>{esc(b['area_path'])}</td>"
            f"<td>{esc(b['assigned_to'])}</td>"
            f"<td>{esc(b['title'])}</td></tr>"
        )

    fp_sample = fp_bugs[:25]
    sample_rows = "".join(fp_row(b) for b in fp_sample)
    all_fp_rows = "".join(fp_row(b) for b in fp_bugs)

    # -- duplicate rows --
    dup_rows_list = []
    for cluster in sorted(dup_clusters, key=lambda c: -len(c))[:100]:
        ids = ", ".join(
            f"<a href='https://dev.azure.com/{ADO_ORG}/_workitems/edit/{b['id']}' target='_blank'>{b['id']}</a>"
            for b in cluster
        )
        title = esc(cluster[0]["title"])
        team = esc(cluster[0]["area_path"])
        dup_rows_list.append(
            f"<tr><td>{team}</td><td>{ids}</td><td>{title}</td></tr>"
        )
    dup_rows = "".join(dup_rows_list)
    dup_bugs_total = sum(len(c) for c in dup_clusters)

    start_date = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).date().isoformat()

    # -- unmatched creators panel --
    unmatched = unmatched_creators or Counter()
    unmatched_rows = "".join(
        f"<tr><td>{esc(name)}</td><td class='n'>{n}</td></tr>"
        for name, n in unmatched.most_common(50)
    )
    unmatched_total = sum(unmatched.values())

    return HTML_TMPL.format(
        days=LOOKBACK_DAYS,
        start_date=start_date,
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        total_30d=total_30d,
        terminal_n=terminal_n,
        terminal_pct=f"{terminal_pct:.1f}",
        fp_n=fp_n,
        fp_pct=f"{fp_pct:.1f}",
        primary_rows=primary_rows and "".join(primary_rows),
        bucket_overlap_rows=bucket_overlap_rows,
        terminal_rows=terminal_rows,
        team_rows=team_rows,
        team_detail_blocks=team_detail_blocks,
        severity_rows=severity_rows,
        assignee_rows=assignee_rows,
        sample_rows=sample_rows,
        all_fp_rows=all_fp_rows,
        fp_sample_n=len(fp_sample),
        dup_clusters=len(dup_clusters),
        dup_bugs=dup_bugs_total,
        dup_rows=dup_rows,
        unmatched_rows=unmatched_rows,
        unmatched_total=unmatched_total,
        qa_member_count=qa_member_count,
        qa_pattern=esc(QA_TEAM_PATTERN),
        qa_exclude=esc(QA_TEAM_EXCLUDE_PATTERN),
        primary_data_json=json.dumps({"labels": pie_labels, "values": pie_values}),
        terminal_data_json=json.dumps(terminal_data),
        severity_data_json=json.dumps(severity_data),
        weekly_data_json=json.dumps(weekly_data),
    )


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def main() -> int:
    print(f"Analyzing bugs created in the last {LOOKBACK_DAYS} days …")
    engine = create_engine(PG_CONN)

    bugs_all = fetch_bugs(engine)
    print(f"  {len(bugs_all)} internal bugs (customer-reported already excluded)")

    # ADO metadata: CreatedBy + PR/commit relations for every bug in scope.
    # We need CreatedBy for ALL bugs (to filter), not just terminal ones.
    print(f"  fetching CreatedBy + relations for {len(bugs_all)} bugs from ADO …")
    meta = fetch_ado_bug_metadata([b["id"] for b in bugs_all])

    # QA identity set from the synced GitHub data
    qa_emails, qa_names, qa_login_frags, qa_rows = build_qa_identity_set(engine)
    print(
        f"  QA identity set: {len(qa_rows)} github members, "
        f"{len(qa_emails)} emails, {len(qa_names)} names, "
        f"{len(qa_login_frags)} login fragments "
        f"(pattern /{QA_TEAM_PATTERN}/)"
    )

    # Attach creator + fix-evidence to each bug, then filter to QA-filed.
    unmatched_creators: Counter = Counter()
    qa_bugs: list[dict] = []
    non_qa_skipped = 0
    missing_meta = 0
    for b in bugs_all:
        m = meta.get(b["id"])
        if not m:
            missing_meta += 1
            continue
        b["created_by_name"] = m["display_name"]
        b["created_by_email"] = m["email"]
        b["_pr_links"] = {"pr": m["pr"], "commit": m["commit"]}
        if is_qa_creator(m["display_name"], m["email"], qa_emails, qa_names, qa_login_frags):
            qa_bugs.append(b)
        else:
            non_qa_skipped += 1
            key = f"{m['display_name']} <{m['email']}>"
            unmatched_creators[key] += 1
    print(
        f"  filter: kept {len(qa_bugs)} QA-filed bugs, "
        f"skipped {non_qa_skipped} non-QA, missing meta {missing_meta}"
    )

    bugs = qa_bugs  # downstream code treats `bugs` as the in-scope set
    bug_ids = [b["id"] for b in bugs]
    history = fetch_state_history(engine, bug_ids)

    bugs_terminal = [b for b in bugs if b["state"] in TERMINAL_STATES]
    print(f"  {len(bugs_terminal)} QA-filed bugs reached a terminal state")

    classifications: dict[int, tuple[bool, str | None, dict[str, bool]]] = {}
    for b in bugs_terminal:
        classifications[b["id"]] = classify(
            b,
            set(history.get(b["id"], [])),
            b["_pr_links"],
        )

    pr_links = {b["id"]: b["_pr_links"] for b in bugs}
    dup_clusters = find_duplicate_clusters(bugs)

    fp_n = sum(1 for b in bugs_terminal if classifications[b["id"]][0])
    if bugs_terminal:
        print(
            f"  Composite FP rate: {fp_n}/{len(bugs_terminal)} "
            f"= {fp_n / len(bugs_terminal) * 100:.1f}%"
        )

    html_out = render(
        bugs, bugs_terminal, classifications, pr_links, history, dup_clusters,
        unmatched_creators=unmatched_creators, qa_member_count=len(qa_rows),
    )
    OUTPUT_FILE.write_text(html_out, encoding="utf-8")
    print(f"\nReport written → {OUTPUT_FILE}")
    print(f"Open with: open {OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
