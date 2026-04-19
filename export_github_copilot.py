#!/usr/bin/env python3
"""
GitHub Copilot metrics per team (usage reports API)
============================================================================
Primary path: org user usage reports per day
  GET /orgs/{org}/copilot/metrics/reports/users-1-day?day=YYYY-MM-DD
  then GET each `download_links` URL (see GitHub Copilot usage metrics REST docs).

User rows are attributed to teams via org team membership (`login` in team).
Rows are rolled up into the same legacy day shape `extract_day_metrics` expects,
so `copilot_metrics_daily` / `metric_name` stay unchanged.

Team totals from this path are sums of per-user org report rows for members of each
GitHub team. They can differ from the older `/team/.../copilot/metrics` series
(attribution, deduplication, and how agent/CLI vs IDE fields are rolled up).
See https://docs.github.com/en/copilot/reference/copilot-usage-metrics/reconciling-usage-metrics

Optional: set GITHUB_COPILOT_USE_TEAM_METRICS_API=1 to try the direct team
`/team/{slug}/copilot/metrics` endpoint first (when GitHub still serves it).

Docs: https://docs.github.com/en/rest/copilot/copilot-usage-metrics

Can be run standalone or integrated with export_ado.py

Usage (standalone):
    python export_github_copilot.py --org Pathlock --last-days 7
    python export_github_copilot.py --org Pathlock --since 2026-01-01 --until 2026-01-31 --verbose

Env: GITHUB_COPILOT_VERBOSE=1 mirrors --verbose when not using the CLI.

Scheduled sync (e.g. export_ado.py):
  GITHUB_COPILOT_DAILY_SYNC_DAYS   — days ending yesterday to re-pull each run (default 3; catches late report fixes)
  GITHUB_COPILOT_INITIAL_SYNC_DAYS — first-run backfill window (default 30)
"""

import requests
import os
import json
import argparse
from typing import Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, text, Table, Column, Integer, String, DateTime, MetaData, Float, Date, Numeric
from sqlalchemy.dialects.postgresql import TEXT as PG_TEXT
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# Per GitHub REST docs (Copilot metrics + Copilot usage metrics reports)
COPILOT_METRICS_API_VERSION = "2026-03-10"


def _env_positive_int(var_name: str, default: int, maximum: Optional[int] = None) -> int:
    raw = os.getenv(var_name, "").strip()
    if not raw:
        n = default
    else:
        try:
            n = int(raw)
        except ValueError:
            n = default
    if n < 1:
        n = default
    if maximum is not None and n > maximum:
        n = maximum
    return n


def _parse_copilot_report_body(text: str, context: str, verbose: bool = False):
    """GitHub may return one JSON value or newline-delimited JSON (NDJSON)."""
    text = (text or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    rows = []
    for line_num, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as e:
            if verbose:
                print(f"------>Skipping bad report line {line_num} ({context}): {e}")
    return rows if rows else None


def _flatten_user_report_payload(payload):
    """Normalize downloaded user usage JSON into a list of per-user row dicts."""
    if payload is None:
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        users = payload.get("users")
        if isinstance(users, list):
            return [x for x in users if isinstance(x, dict)]
        if "user_login" in payload or "user_id" in payload:
            return [payload]
    return []


def _user_row_login(user_row):
    v = (
        user_row.get("user_login")
        or user_row.get("login")
        or user_row.get("username")
        or user_row.get("github_username")
        or ""
    )
    if not v and isinstance(user_row.get("user"), dict):
        v = user_row["user"].get("login") or user_row["user"].get("user_login") or ""
    return (v or "").strip().lower()


def _row_report_day_key(row: dict, requested_day: str) -> str:
    """Normalize `day` / `day_partition` from usage reports for comparison with YYYY-MM-DD."""
    raw = (row.get("day") or row.get("day_partition") or "").strip()
    if not raw:
        return requested_day
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        return raw[:10]
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 8:
        d = digits[:8]
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    return raw[:10]


def _sum_totals_by_feature_int(user_row: dict, field: str) -> int:
    n = 0
    for row in user_row.get("totals_by_feature") or []:
        if isinstance(row, dict):
            n += int(row.get(field) or 0)
    return n


def _usage_int(user_row: dict, field: str) -> int:
    """Prefer top-level counts; if zero, sum the same field across totals_by_feature (agent/chat breakdown)."""
    top = int(user_row.get(field) or 0)
    if top > 0:
        return top
    return _sum_totals_by_feature_int(user_row, field)


_MERGE_SUM_FIELDS = (
    "code_generation_activity_count",
    "code_acceptance_activity_count",
    "loc_suggested_to_add_sum",
    "loc_suggested_to_delete_sum",
    "loc_added_sum",
    "loc_deleted_sum",
    "user_initiated_interaction_count",
)


def _merge_user_rows_for_same_day(rows: list, verbose: bool = False) -> list:
    """Combine multiple rows for the same user_login (e.g. report shards) by summing numeric fields."""
    by_login: dict = {}
    no_login: list = []
    dupes = 0
    for r in rows:
        if not isinstance(r, dict):
            continue
        lg = _user_row_login(r)
        if not lg:
            no_login.append(r)
            continue
        if lg not in by_login:
            by_login[lg] = {k: int(r.get(k) or 0) for k in _MERGE_SUM_FIELDS}
            by_login[lg]["_totals_by_feature"] = []
            for row in r.get("totals_by_feature") or []:
                if isinstance(row, dict):
                    by_login[lg]["_totals_by_feature"].append(dict(row))
            pr = r.get("pull_requests")
            by_login[lg]["_pull_requests"] = dict(pr) if isinstance(pr, dict) else {}
            continue
        dupes += 1
        acc = by_login[lg]
        for k in _MERGE_SUM_FIELDS:
            acc[k] += int(r.get(k) or 0)
        for row in r.get("totals_by_feature") or []:
            if isinstance(row, dict):
                acc["_totals_by_feature"].append(dict(row))
        pr = r.get("pull_requests")
        if isinstance(pr, dict):
            target = acc["_pull_requests"]
            for pk, pv in pr.items():
                if isinstance(pv, (int, float)) and str(pk).startswith("total_"):
                    target[pk] = int(target.get(pk) or 0) + int(pv or 0)
                elif pk not in target:
                    target[pk] = pv
    if verbose and dupes:
        print(f"------>Merged {dupes} duplicate user row(s) (same login, same day)")
    out = []
    for lg, acc in by_login.items():
        merged = {k: acc[k] for k in _MERGE_SUM_FIELDS}
        merged["user_login"] = lg
        merged["totals_by_feature"] = acc["_totals_by_feature"]
        if acc["_pull_requests"]:
            merged["pull_requests"] = acc["_pull_requests"]
        out.append(merged)
    return out + no_login


_PR_ENGAGEMENT_FIELDS = (
    "total_reviewed_by_copilot",
    "total_copilot_applied_suggestions",
    "total_applied_suggestions",
)


def _user_has_any_engagement(u: dict, dotcom_user: bool) -> bool:
    """Matches GitHub's team-metrics `total_engaged_users` semantics:
    any non-zero Copilot activity in any feature (completions, chat, dotcom, PR)."""
    if _usage_int(u, "code_generation_activity_count") > 0:
        return True
    if _usage_int(u, "code_acceptance_activity_count") > 0:
        return True
    if _usage_int(u, "user_initiated_interaction_count") > 0:
        return True
    if dotcom_user:
        return True
    pr = u.get("pull_requests")
    if isinstance(pr, dict):
        for k in _PR_ENGAGEMENT_FIELDS:
            if int(pr.get(k) or 0) > 0:
                return True
    return False


def _aggregate_user_rows_to_legacy_day(date_str, user_rows):
    """Roll up user-level usage rows into one legacy copilot/metrics day object."""
    if not user_rows:
        return None
    day_key = (date_str or "")[:10]
    gen = sum(_usage_int(u, "code_generation_activity_count") for u in user_rows)
    acc = sum(_usage_int(u, "code_acceptance_activity_count") for u in user_rows)
    loc_sug = sum(
        _usage_int(u, "loc_suggested_to_add_sum") + _usage_int(u, "loc_suggested_to_delete_sum")
        for u in user_rows
    )
    loc_acc = sum(_usage_int(u, "loc_added_sum") for u in user_rows)
    n = len(user_rows)
    cc_engaged = sum(1 for u in user_rows if _usage_int(u, "code_generation_activity_count") > 0)
    ide_chats = sum(_usage_int(u, "user_initiated_interaction_count") for u in user_rows)
    ide_engaged = sum(1 for u in user_rows if _usage_int(u, "user_initiated_interaction_count") > 0)

    dotcom_chats = 0
    dotcom_engaged = 0
    dotcom_user_flags = []
    for u in user_rows:
        user_dot = False
        for row in u.get("totals_by_feature") or []:
            if not isinstance(row, dict):
                continue
            feat = (row.get("feature") or "").lower()
            if "dotcom" in feat or "github.com" in feat:
                c = int(row.get("code_generation_activity_count") or 0)
            elif feat in ("github_com_chat", "github_com", "dotcom_chat"):
                c = int(row.get("user_initiated_interaction_count") or 0) or int(
                    row.get("code_generation_activity_count") or 0
                )
            else:
                c = 0
            if c:
                dotcom_chats += c
                user_dot = True
        dotcom_user_flags.append(user_dot)
        if user_dot:
            dotcom_engaged += 1

    # Engaged = any Copilot feature activity that day (matches GitHub's team-metrics
    # definition). Previously this only counted acceptances, which under-reported
    # engagement and made sub-feature engaged counts exceed the overall total.
    engaged = sum(
        1
        for u, dot in zip(user_rows, dotcom_user_flags)
        if _user_has_any_engagement(u, dot)
    )

    pr_summaries = 0
    for u in user_rows:
        pr = u.get("pull_requests")
        if isinstance(pr, dict):
            pr_summaries += int(
                pr.get("total_reviewed_by_copilot")
                or pr.get("total_copilot_applied_suggestions")
                or pr.get("total_applied_suggestions")
                or 0
            )

    return {
        "date": day_key,
        "total_active_users": n,
        "total_engaged_users": engaged,
        "copilot_ide_code_completions": {
            "total_engaged_users": cc_engaged,
            "editors": [
                {
                    "models": [
                        {
                            "languages": [
                                {
                                    "total_code_suggestions": gen,
                                    "total_code_acceptances": acc,
                                    "total_code_lines_suggested": loc_sug,
                                    "total_code_lines_accepted": loc_acc,
                                }
                            ]
                        }
                    ]
                }
            ],
        },
        "copilot_ide_chat": {
            "total_engaged_users": ide_engaged,
            "editors": [
                {
                    "models": [
                        {
                            "total_chats": ide_chats,
                            "total_chat_insertion_events": 0,
                            "total_chat_copy_events": 0,
                        }
                    ]
                }
            ],
        },
        "copilot_dotcom_chat": {
            "total_engaged_users": dotcom_engaged,
            "models": ([{"total_chats": dotcom_chats}] if dotcom_chats else []),
        },
        "copilot_dotcom_pull_requests": {
            "total_engaged_users": 0,
            "repositories": [{"models": [{"total_pr_summaries_created": pr_summaries}]}],
        },
    }


class CopilotMetricsDatabase:
    """Database connection and operations for Copilot metrics"""
    
    def __init__(self, engine=None):
        if engine:
            self.engine = engine
        else:
            connection_string = (
                f"postgresql://{os.getenv('PG_USERNAME')}:{os.getenv('PG_PASSWORD')}@"
                f"{os.getenv('PG_HOST')}:{os.getenv('PG_PORT', '5432')}/"
                f"{os.getenv('PG_DATABASE')}"
            )
            self.engine = create_engine(connection_string)
        
        self.metadata = MetaData()
        self.setup_tables()
    
    def setup_tables(self):
        """Create copilot_metrics_daily table with flexible key-value format"""
        # New daily metrics table - one row per metric per day per team
        self.copilot_metrics_daily = Table(
            'copilot_metrics_daily', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('metric_date', Date, nullable=False),
            Column('organization', String(200), nullable=False),
            Column('team_slug', String(200), nullable=False),
            Column('metric_name', String(100), nullable=False),
            Column('metric_value', Numeric, nullable=True),
            Column('created_date', DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP')),
        )
        
        self.metadata.create_all(self.engine, checkfirst=True)
        
        # Create indexes and unique constraint
        with self.engine.connect() as connection:
            try:
                connection.execute(text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_copilot_daily_unique 
                    ON copilot_metrics_daily(metric_date, organization, team_slug, metric_name)
                """))
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_copilot_daily_date 
                    ON copilot_metrics_daily(metric_date)
                """))
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_copilot_daily_team 
                    ON copilot_metrics_daily(team_slug)
                """))
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_copilot_daily_metric 
                    ON copilot_metrics_daily(metric_name)
                """))
                connection.commit()
            except SQLAlchemyError as e:
                print(f"Error creating indexes: {str(e)}")
                connection.rollback()
    
    def upsert_daily_metrics(self, daily_records):
        """Insert or update daily copilot metrics in key-value format
        
        Args:
            daily_records: List of dicts with keys: metric_date, organization, team_slug, metric_name, metric_value
        """
        processed_count = 0
        
        with self.engine.connect() as connection:
            for record in daily_records:
                try:
                    # Upsert using ON CONFLICT
                    connection.execute(
                        text("""
                            INSERT INTO copilot_metrics_daily (
                                metric_date, organization, team_slug, metric_name, metric_value
                            ) VALUES (
                                :metric_date, :organization, :team_slug, :metric_name, :metric_value
                            )
                            ON CONFLICT (metric_date, organization, team_slug, metric_name) 
                            DO UPDATE SET metric_value = :metric_value
                        """),
                        record
                    )
                    processed_count += 1
                    
                except SQLAlchemyError as e:
                    print(f"Error upserting metric {record.get('metric_name', 'unknown')}: {str(e)}")
                    connection.rollback()
                    continue
            
            connection.commit()
        
        return processed_count


class GitHubCopilotExtractor:
    """Extract GitHub Copilot metrics from the API"""
    
    def __init__(self, organization, token=None, teams=None, team_sizes=None, db_engine=None, verbose=False):
        self.organization = organization
        self.token = token or os.getenv('GITHUB_TOKEN')
        self.verbose = verbose or os.getenv('GITHUB_COPILOT_VERBOSE', '').strip().lower() in ('1', 'true', 'yes')
        self.teams = teams.split(',') if teams else []
        self.team_sizes = {}
        # Reset at start of each fetch_all_metrics; used to shorten repeated identical Copilot API errors
        self._last_copilot_api_error_fingerprint = None
        
        if team_sizes and teams:
            sizes = team_sizes.split(',')
            for i, team in enumerate(self.teams):
                if i < len(sizes):
                    try:
                        self.team_sizes[team.strip()] = int(sizes[i].strip())
                    except ValueError:
                        pass
        
        self.base_url = "https://api.github.com"
        self.api_version = COPILOT_METRICS_API_VERSION
        self.headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "X-GitHub-Api-Version": self.api_version,
        }
        
        self.db = CopilotMetricsDatabase(engine=db_engine) if db_engine else None
    
    def _api_request(self, endpoint, context=None):
        """Make a request to the GitHub API. On failure, prints status, URL, and body (see context label)."""
        url = f"{self.base_url}{endpoint}"
        label = context or endpoint
        if self.verbose:
            print(f"------>GET {url}")
        try:
            response = requests.get(url, headers=self.headers, timeout=120)
        except requests.RequestException as e:
            print(f"------>GitHub API request failed ({label}): {type(e).__name__}: {e}")
            return None
        
        if response.status_code == 200:
            try:
                return response.json()
            except ValueError as e:
                print(f"------>GitHub API invalid JSON ({label}): {e}")
                print(f"         URL: {url}")
                snippet = (response.text or "")[:800]
                print(f"         body (first 800 chars): {snippet!r}")
                return None
        
        msg = ""
        docs = ""
        try:
            j = response.json()
            if isinstance(j, dict):
                msg = j.get("message") or ""
                docs = j.get("documentation_url") or ""
        except ValueError:
            pass
        
        body_snippet = (response.text or "")[:800]
        rl_rem = response.headers.get("X-RateLimit-Remaining")
        rl_reset = response.headers.get("X-RateLimit-Reset")
        is_copilot_metrics = context and "Copilot metrics" in context
        fingerprint = (response.status_code, msg or body_snippet[:240])
        if (
            is_copilot_metrics
            and not self.verbose
            and self._last_copilot_api_error_fingerprint == fingerprint
        ):
            print(
                f"------>GitHub API HTTP {response.status_code} ({label}) — "
                f"same error as previous team (omit body; see first occurrence above)"
            )
            return None
        if is_copilot_metrics:
            self._last_copilot_api_error_fingerprint = fingerprint
        
        print(f"------>GitHub API HTTP {response.status_code} ({label})")
        print(f"         URL: {url}")
        if msg:
            print(f"         message: {msg}")
        if docs:
            print(f"         documentation_url: {docs}")
        if rl_rem is not None:
            print(f"         X-RateLimit-Remaining: {rl_rem}  X-RateLimit-Reset: {rl_reset}")
        print(f"         body (first 800 chars): {body_snippet!r}")
        return None
    
    def _safe_div(self, num, den, default=0):
        """Safe division with default value"""
        if den and den > 0:
            return num / den
        return default
    
    def get_teams(self):
        """Fetch teams from the organization or use provided list"""
        if self.teams:
            return [t.strip() for t in self.teams]

        slugs = []
        page = 1
        while True:
            teams_data = self._api_request(
                f"/orgs/{self.organization}/teams?per_page=100&page={page}",
                context=f"List organization teams (page {page})",
            )
            if not teams_data:
                if page == 1:
                    print("Error fetching teams: No response")
                break
            if isinstance(teams_data, dict) and teams_data.get("message"):
                if page == 1:
                    print(f"Error fetching teams: {teams_data.get('message', 'Unknown error')}")
                break
            if not isinstance(teams_data, list):
                break
            slugs.extend(
                t["slug"] for t in teams_data if isinstance(t, dict) and t.get("slug")
            )
            if len(teams_data) < 100:
                break
            page += 1
        return slugs
    
    def get_team_size(self, team_slug):
        """Get team size from manual input or API"""
        if team_slug in self.team_sizes:
            return self.team_sizes[team_slug]
        return len(self.get_team_member_logins(team_slug))
    
    def get_team_member_logins(self, team_slug):
        """All member logins for a team (lowercased), with pagination."""
        logins = set()
        page = 1
        while True:
            data = self._api_request(
                f"/orgs/{self.organization}/teams/{team_slug}/members?per_page=100&page={page}&role=all",
                context=f"Team members page {page} / {team_slug}",
            )
            if data is None:
                break
            if isinstance(data, dict) and data.get("message"):
                break
            if not isinstance(data, list):
                break
            for m in data:
                if isinstance(m, dict) and m.get("login"):
                    logins.add(m["login"].strip().lower())
            if len(data) < 100:
                break
            page += 1
        return logins
    
    def _get_org_users_one_day_descriptor(self, day: str):
        """users-1-day report: JSON with download_links, or None if 204/missing."""
        endpoint = (
            f"/orgs/{self.organization}/copilot/metrics/reports/users-1-day?day={day}"
        )
        url = f"{self.base_url}{endpoint}"
        label = f"Copilot usage users-1-day / {day}"
        if self.verbose:
            print(f"------>GET {url}")
        try:
            response = requests.get(url, headers=self.headers, timeout=120)
        except requests.RequestException as e:
            print(f"------>GitHub API request failed ({label}): {type(e).__name__}: {e}")
            return None
        if response.status_code == 204:
            if self.verbose:
                print(f"------>HTTP 204 ({label}) — no report for this day")
            return None
        if response.status_code == 200:
            try:
                return response.json()
            except ValueError as e:
                print(f"------>GitHub API invalid JSON ({label}): {e}")
                return None
        msg = ""
        try:
            j = response.json()
            if isinstance(j, dict):
                msg = j.get("message") or ""
        except ValueError:
            pass
        print(f"------>GitHub API HTTP {response.status_code} ({label})")
        print(f"         URL: {url}")
        if msg:
            print(f"         message: {msg}")
        print(f"         body (first 800 chars): {(response.text or '')[:800]!r}")
        return None
    
    def _download_signed_report_json(self, link: str, context: str):
        try:
            response = requests.get(
                link,
                timeout=300,
                headers={"Accept": "application/json, */*"},
            )
        except requests.RequestException as e:
            print(f"------>Report download failed ({context}): {type(e).__name__}: {e}")
            return None
        if response.status_code != 200:
            print(
                f"------>Report download HTTP {response.status_code} ({context}) "
                f"body: {(response.text or '')[:400]!r}"
            )
            return None
        parsed = _parse_copilot_report_body(
            response.text, context=context, verbose=self.verbose
        )
        if parsed is None:
            print(f"------>Report empty or unparseable ({context})")
            return None
        return parsed
    
    def fetch_user_rows_for_day(self, day: str):
        """Download org user usage rows for a single calendar day."""
        meta = self._get_org_users_one_day_descriptor(day)
        if not meta or not isinstance(meta, dict):
            return []
        links = meta.get("download_links") or []
        if not links:
            return []
        combined = []
        for i, link in enumerate(links):
            payload = self._download_signed_report_json(link, context=f"{day} file {i + 1}")
            combined.extend(_flatten_user_report_payload(payload))
        out = []
        for row in combined:
            if not isinstance(row, dict):
                continue
            if _row_report_day_key(row, day) != day:
                continue
            out.append(row)
        return _merge_user_rows_for_same_day(out, verbose=self.verbose)
    
    def fetch_user_rows_by_day_range(self, since_date: str, until_date: str):
        """Map YYYY-MM-DD -> list of user usage rows for that day."""
        d0 = datetime.strptime(since_date, "%Y-%m-%d").date()
        d1 = datetime.strptime(until_date, "%Y-%m-%d").date()
        if d1 < d0:
            return {}
        by_day = {}
        n = (d1 - d0).days + 1
        print(f"------>Loading org user usage reports ({n} day(s))…")
        for i in range(n):
            day = (d0 + timedelta(days=i)).strftime("%Y-%m-%d")
            rows = self.fetch_user_rows_for_day(day)
            by_day[day] = rows
            if self.verbose or rows:
                print(f"         {day}: {len(rows)} user row(s)")
        return by_day
    
    def team_raw_data_from_user_reports(self, team_logins, by_day, since_date, until_date):
        """Build legacy-shaped day list for one team from cached user rows."""
        d0 = datetime.strptime(since_date, "%Y-%m-%d").date()
        d1 = datetime.strptime(until_date, "%Y-%m-%d").date()
        raw = []
        n = (d1 - d0).days + 1
        for i in range(n):
            day = (d0 + timedelta(days=i)).strftime("%Y-%m-%d")
            rows = by_day.get(day) or []
            team_rows = [r for r in rows if _user_row_login(r) in team_logins]
            legacy = _aggregate_user_rows_to_legacy_day(day, team_rows)
            if legacy:
                raw.append(legacy)
        return raw
    
    def fetch_team_copilot_metrics(self, team_slug, since_date, until_date):
        """Fetch Copilot metrics for one team (JSON array of daily objects)."""
        endpoint = (
            f"/orgs/{self.organization}/team/{team_slug}/copilot/metrics"
            f"?since={since_date}T00:00:00Z&until={until_date}T23:59:59Z&per_page=100"
        )
        return self._api_request(endpoint, context=f"Copilot metrics / {team_slug}")
    
    def extract_day_metrics(self, day_data):
        """Extract all metrics from a single day's API response
        
        Returns a dict with metric_name -> metric_value
        """
        metrics = {}
        
        # Top-level user counts (directly from API response)
        metrics['total_active_users'] = day_data.get('total_active_users', 0) or 0
        metrics['total_engaged_users'] = day_data.get('total_engaged_users', 0) or 0
        
        # IDE Code Completions
        code_completions = day_data.get('copilot_ide_code_completions') or {}
        metrics['code_completions_engaged_users'] = code_completions.get('total_engaged_users', 0) or 0
        
        # Sum nested code completion metrics (across all editors/models/languages)
        suggestions = 0
        acceptances = 0
        lines_suggested = 0
        lines_accepted = 0
        
        for editor in code_completions.get('editors', []):
            for model in editor.get('models', []):
                for lang in model.get('languages', []):
                    suggestions += lang.get('total_code_suggestions', 0) or 0
                    acceptances += lang.get('total_code_acceptances', 0) or 0
                    lines_suggested += lang.get('total_code_lines_suggested', 0) or 0
                    lines_accepted += lang.get('total_code_lines_accepted', 0) or 0
        
        metrics['code_suggestions'] = suggestions
        metrics['code_acceptances'] = acceptances
        metrics['code_lines_suggested'] = lines_suggested
        metrics['code_lines_accepted'] = lines_accepted
        
        # IDE Chat
        ide_chat = day_data.get('copilot_ide_chat') or {}
        metrics['ide_chat_engaged_users'] = ide_chat.get('total_engaged_users', 0) or 0
        
        ide_chats = 0
        chat_insertions = 0
        chat_copies = 0
        
        for editor in ide_chat.get('editors', []):
            for model in editor.get('models', []):
                ide_chats += model.get('total_chats', 0) or 0
                chat_insertions += model.get('total_chat_insertion_events', 0) or 0
                chat_copies += model.get('total_chat_copy_events', 0) or 0
        
        metrics['ide_chats'] = ide_chats
        metrics['ide_chat_insertions'] = chat_insertions
        metrics['ide_chat_copies'] = chat_copies
        
        # Dotcom Chat
        dotcom_chat = day_data.get('copilot_dotcom_chat') or {}
        metrics['dotcom_chat_engaged_users'] = dotcom_chat.get('total_engaged_users', 0) or 0
        
        web_chats = 0
        for model in dotcom_chat.get('models', []):
            web_chats += model.get('total_chats', 0) or 0
        metrics['dotcom_chats'] = web_chats
        
        # PR Summaries
        pr_data = day_data.get('copilot_dotcom_pull_requests') or {}
        metrics['pr_engaged_users'] = pr_data.get('total_engaged_users', 0) or 0
        
        pr_summaries = 0
        for repo in pr_data.get('repositories', []):
            for model in repo.get('models', []):
                pr_summaries += model.get('total_pr_summaries_created', 0) or 0
        metrics['pr_summaries'] = pr_summaries
        
        return metrics
    
    def process_daily_metrics(self, raw_data, team_slug, team_size=0):
        """Process raw API data into daily metric records for database storage
        
        Returns list of records in format: {metric_date, organization, team_slug, metric_name, metric_value}
        """
        if not raw_data:
            return []
        
        records = []
        
        for day_data in raw_data:
            metric_date = day_data.get('date')
            if not metric_date:
                continue
            
            day_metrics = self.extract_day_metrics(day_data)
            
            # Add team_size as a metric (for calculating percentages in Metabase)
            day_metrics['team_size'] = team_size
            
            # Calculate derived metrics per day
            if team_size > 0:
                day_metrics['team_active_pct'] = round((day_metrics['total_active_users'] / team_size) * 100, 1)
                day_metrics['team_engaged_pct'] = round((day_metrics['total_engaged_users'] / team_size) * 100, 1)
            else:
                day_metrics['team_active_pct'] = 0
                day_metrics['team_engaged_pct'] = 0
            
            # Acceptance rate
            if day_metrics['code_suggestions'] > 0:
                day_metrics['acceptance_rate_pct'] = round((day_metrics['code_acceptances'] / day_metrics['code_suggestions']) * 100, 1)
            else:
                day_metrics['acceptance_rate_pct'] = 0
            
            # Chat to code ratio
            total_chats = day_metrics['ide_chats'] + day_metrics['dotcom_chats']
            chat_to_code = day_metrics['ide_chat_insertions'] + day_metrics['ide_chat_copies']
            if total_chats > 0:
                day_metrics['chat_to_code_pct'] = round((chat_to_code / total_chats) * 100, 1)
            else:
                day_metrics['chat_to_code_pct'] = 0
            
            for metric_name, metric_value in day_metrics.items():
                records.append({
                    'metric_date': metric_date,
                    'organization': self.organization,
                    'team_slug': team_slug,
                    'metric_name': metric_name,
                    'metric_value': metric_value
                })
        
        return records
    
    def calculate_period_summary(self, raw_data, team_slug, team_size):
        """Calculate summary metrics for display/reporting (averages over period)
        
        This is used for console output and reporting, not for database storage.
        """
        if not raw_data:
            return None
        
        days = len(raw_data)
        if days == 0:
            return None
        
        # Sum all daily values
        totals = {
            'total_active_users': 0,
            'total_engaged_users': 0,
            'code_completions_engaged_users': 0,
            'ide_chat_engaged_users': 0,
            'dotcom_chat_engaged_users': 0,
            'pr_engaged_users': 0,
            'code_suggestions': 0,
            'code_acceptances': 0,
            'code_lines_suggested': 0,
            'code_lines_accepted': 0,
            'ide_chats': 0,
            'ide_chat_insertions': 0,
            'ide_chat_copies': 0,
            'dotcom_chats': 0,
            'pr_summaries': 0,
        }
        
        for day_data in raw_data:
            day_metrics = self.extract_day_metrics(day_data)
            for key in totals:
                totals[key] += day_metrics.get(key, 0)
        
        # Calculate averages
        avg_active = totals['total_active_users'] / days
        avg_engaged = totals['total_engaged_users'] / days
        avg_code_users = totals['code_completions_engaged_users'] / days
        avg_chat_users = totals['ide_chat_engaged_users'] / days
        
        # Team-based percentages (avg usage vs team size)
        team_active_pct = self._safe_div(avg_active * 100, team_size) if team_size > 0 else 0
        team_engaged_pct = self._safe_div(avg_engaged * 100, team_size) if team_size > 0 else 0
        
        # Acceptance rate (total acceptances / total suggestions)
        acceptance_rate = self._safe_div(totals['code_acceptances'] * 100, totals['code_suggestions'])
        
        # Chat to code ratio
        total_chats = totals['ide_chats'] + totals['dotcom_chats']
        chat_code = totals['ide_chat_insertions'] + totals['ide_chat_copies']
        chat_to_code_pct = self._safe_div(chat_code * 100, total_chats)
        
        return {
            'team_slug': team_slug,
            'team_size': team_size,
            'days_in_period': days,
            'avg_active_per_day': round(avg_active, 1),
            'avg_engaged_per_day': round(avg_engaged, 1),
            'team_active_pct': round(team_active_pct, 1),
            'team_engaged_pct': round(team_engaged_pct, 1),
            'acceptance_rate_pct': round(acceptance_rate, 1),
            'chat_to_code_pct': round(chat_to_code_pct, 0),
            'total_suggestions': totals['code_suggestions'],
            'total_acceptances': totals['code_acceptances'],
            'total_lines_accepted': totals['code_lines_accepted'],
            'total_chats': total_chats,
        }
    
    def _fetch_all_metrics_team_api(self, since_date, until_date):
        """Legacy path: GET /orgs/.../team/.../copilot/metrics (when available)."""
        total_records = 0
        all_summaries = []
        teams = self.get_teams()
        if not teams:
            print("------>No teams found or configured")
            return 0
        print(f"------>Processing {len(teams)} teams (team metrics API)")
        for team_slug in teams:
            print(f"------>Analyzing: {team_slug}...")
            team_size = self.get_team_size(team_slug)
            print(f"        Team size: {team_size}")
            raw_data = self.fetch_team_copilot_metrics(team_slug, since_date, until_date)
            if raw_data is None:
                print("        Skipped: Copilot metrics request failed (details above)")
                continue
            if isinstance(raw_data, dict) and "message" in raw_data:
                print(f"        Skipped: {raw_data['message']}")
                continue
            if not raw_data:
                print("        No data (empty series or below GitHub threshold)")
                continue
            daily_records = self.process_daily_metrics(raw_data, team_slug, team_size)
            if daily_records and self.db:
                processed = self.db.upsert_daily_metrics(daily_records)
                total_records += processed
                print(f"        ✓ Stored {processed} daily metric records ({len(raw_data)} days)")
            summary = self.calculate_period_summary(raw_data, team_slug, team_size)
            if summary:
                all_summaries.append(summary)
                print(
                    f"        Avg Engaged: {summary['avg_engaged_per_day']}/day, "
                    f"Accept Rate: {summary['acceptance_rate_pct']}%"
                )
        self._summaries = all_summaries
        return total_records
    
    def _fetch_all_metrics_user_reports(self, since_date, until_date):
        """Default: org users-1-day reports + team membership attribution."""
        teams = self.get_teams()
        if not teams:
            print("------>No teams found or configured")
            return 0
        
        by_day = self.fetch_user_rows_by_day_range(since_date, until_date)
        total_user_rows = sum(len(v) for v in by_day.values())
        if total_user_rows == 0:
            print(
                "------>No user usage rows in range "
                "(policy, 204, download errors, or empty reports). "
                "See https://docs.github.com/en/rest/copilot/copilot-usage-metrics"
            )
            return 0
        
        print(f"------>Attributing {total_user_rows} user-day row(s) across {len(teams)} teams")
        total_records = 0
        all_summaries = []
        
        for team_slug in teams:
            print(f"------>Analyzing: {team_slug}…")
            logins = self.get_team_member_logins(team_slug)
            team_size = self.team_sizes.get(team_slug, len(logins))
            print(f"        Team size: {team_size} ({len(logins)} member login(s) from API)")
            
            raw_data = self.team_raw_data_from_user_reports(
                logins, by_day, since_date, until_date
            )
            if not raw_data:
                print("        No data (no org users matched this team for any day)")
                continue
            
            daily_records = self.process_daily_metrics(raw_data, team_slug, team_size)
            if daily_records and self.db:
                processed = self.db.upsert_daily_metrics(daily_records)
                total_records += processed
                print(f"        ✓ Stored {processed} daily metric records ({len(raw_data)} days)")
            
            summary = self.calculate_period_summary(raw_data, team_slug, team_size)
            if summary:
                all_summaries.append(summary)
                print(
                    f"        Avg Engaged: {summary['avg_engaged_per_day']}/day, "
                    f"Accept Rate: {summary['acceptance_rate_pct']}%"
                )
        
        self._summaries = all_summaries
        return total_records
    
    def fetch_all_metrics(self, since_date=None, until_date=None):
        """Fetch metrics for all configured teams and store in database
        
        Returns the number of records processed
        """
        if not since_date:
            since_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not until_date:
            until_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        self._last_copilot_api_error_fingerprint = None
        use_team_api = os.getenv("GITHUB_COPILOT_USE_TEAM_METRICS_API", "").strip().lower() in (
            "1",
            "true",
            "yes",
        )
        print(f"------>Fetching GitHub Copilot metrics for org: {self.organization}")
        print(f"------>Date range: {since_date} to {until_date}")
        if use_team_api:
            print("------>Mode: team metrics API (GITHUB_COPILOT_USE_TEAM_METRICS_API)")
            return self._fetch_all_metrics_team_api(since_date, until_date)
        print("------>Mode: org users-1-day usage reports + team membership")
        return self._fetch_all_metrics_user_reports(since_date, until_date)
    
    def sync_to_database(self, db_connection=None, initial_sync=False):
        """Fetch metrics and store in database.

        Uses a sliding date window ending yesterday so scheduled runs stay fast:
        - Daily: GITHUB_COPILOT_DAILY_SYNC_DAYS (default 3)
        - First run: GITHUB_COPILOT_INITIAL_SYNC_DAYS (default 30, max 100)
        """
        if db_connection:
            self.db = CopilotMetricsDatabase(engine=db_connection.engine)
        elif not self.db:
            self.db = CopilotMetricsDatabase()
        
        until_d = datetime.now().date() - timedelta(days=1)
        if initial_sync:
            days = _env_positive_int("GITHUB_COPILOT_INITIAL_SYNC_DAYS", 30, maximum=100)
            print(f"------>Copilot sync: initial backfill, last {days} day(s)")
        else:
            days = _env_positive_int("GITHUB_COPILOT_DAILY_SYNC_DAYS", 3, maximum=100)
            print(f"------>Copilot sync: incremental, last {days} day(s)")
        
        since_d = until_d - timedelta(days=days - 1)
        since = since_d.strftime("%Y-%m-%d")
        until = until_d.strftime("%Y-%m-%d")
        
        processed = self.fetch_all_metrics(since_date=since, until_date=until)
        
        if processed > 0:
            print(f"------>Processed {processed} Copilot daily metrics records")
            return processed
        print("------>No Copilot metrics to process")
        return 0
    
    def print_report(self, summaries=None):
        """Print a formatted report of the metrics"""
        # Use stored summaries if not provided
        if summaries is None:
            summaries = getattr(self, '_summaries', [])
        
        if not summaries:
            print("No metrics to display")
            return
        
        # Colors for terminal
        RED = '\033[0;31m'
        GREEN = '\033[0;32m'
        YELLOW = '\033[1;33m'
        BLUE = '\033[0;34m'
        CYAN = '\033[0;36m'
        BOLD = '\033[1m'
        NC = '\033[0m'  # No Color
        
        print()
        print(f"{BOLD}{CYAN}╔════════════════════════════════════════════════════════════════════════════════╗{NC}")
        print(f"{BOLD}{CYAN}║                    🤖 GITHUB COPILOT USAGE REPORT                              ║{NC}")
        print(f"{BOLD}{CYAN}║                    Organization: {self.organization:<20}                      ║{NC}")
        print(f"{BOLD}{CYAN}╚════════════════════════════════════════════════════════════════════════════════╝{NC}")
        print()
        
        # Summary table
        print(f"{BOLD}{'Team':<25} {'Size':>6} {'Days':>5} {'Avg Active':>10} {'Avg Engaged':>11} {'Engaged %':>10} {'Accept %':>10}{NC}")
        print("-" * 85)
        
        for m in summaries:
            # Color code engagement
            eng_pct = m.get('team_engaged_pct', 0)
            if eng_pct >= 70:
                eng_color = GREEN
            elif eng_pct >= 40:
                eng_color = YELLOW
            else:
                eng_color = RED
            
            # Color code acceptance rate
            acc_pct = m.get('acceptance_rate_pct', 0)
            if acc_pct >= 35:
                acc_color = GREEN
            elif acc_pct >= 25:
                acc_color = YELLOW
            else:
                acc_color = RED
            
            print(f"{m['team_slug']:<25} {m.get('team_size', 0):>6} {m.get('days_in_period', 0):>5} "
                  f"{m.get('avg_active_per_day', 0):>10.1f} {m.get('avg_engaged_per_day', 0):>11.1f} "
                  f"{eng_color}{eng_pct:>9.1f}%{NC} {acc_color}{acc_pct:>9.1f}%{NC}")
        
        print()
        print(f"{BOLD}Totals:{NC}")
        print("-" * 85)
        
        for m in summaries:
            print(f"{m['team_slug']:<25} Suggestions: {m.get('total_suggestions', 0):>8,}  "
                  f"Acceptances: {m.get('total_acceptances', 0):>8,}  "
                  f"Lines Accepted: {m.get('total_lines_accepted', 0):>8,}  "
                  f"Chats: {m.get('total_chats', 0):>6,}")
        
        print()


def main():
    """Main function for standalone execution"""
    parser = argparse.ArgumentParser(description='GitHub Copilot Usage Metrics')
    parser.add_argument('--org', required=True, help='GitHub organization name')
    parser.add_argument('--teams', help='Comma-separated list of team slugs')
    parser.add_argument('--team-sizes', help='Comma-separated list of team sizes (same order as --teams)')
    parser.add_argument('--since', help='Start date (YYYY-MM-DD), default: 30 days ago')
    parser.add_argument('--until', help='End date (YYYY-MM-DD), default: yesterday')
    parser.add_argument(
        '--last-days',
        type=int,
        metavar='N',
        help='Last N calendar days ending yesterday (overrides --since and --until)',
    )
    parser.add_argument('--no-db', action='store_true', help='Skip database storage')
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Log each API URL before the request; show every Copilot error in full (no deduplication)',
    )
    
    args = parser.parse_args()
    
    # Validate token
    token = os.getenv('GITHUB_TOKEN')
    if not token:
        print("Error: GITHUB_TOKEN environment variable is not set")
        return 1
    
    # Initialize database if needed
    db_engine = None
    if not args.no_db:
        db = CopilotMetricsDatabase()
        db_engine = db.engine
    
    # Initialize extractor
    extractor = GitHubCopilotExtractor(
        organization=args.org,
        token=token,
        teams=args.teams,
        team_sizes=args.team_sizes,
        db_engine=db_engine,
        verbose=args.verbose,
    )
    
    # Set dates
    if args.last_days is not None:
        if args.last_days < 1:
            print('Error: --last-days must be >= 1')
            return 1
        until_d = datetime.now().date() - timedelta(days=1)
        since_d = until_d - timedelta(days=args.last_days - 1)
        since = since_d.strftime('%Y-%m-%d')
        until = until_d.strftime('%Y-%m-%d')
    else:
        since = args.since or (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        until = args.until or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Fetch metrics (stores to DB internally if db_engine provided)
    processed = extractor.fetch_all_metrics(since_date=since, until_date=until)
    
    # Print report from stored summaries
    extractor.print_report()
    
    if processed > 0:
        print(f"Stored {processed} daily metric records in database")
    
    return 0


if __name__ == "__main__":
    exit(main())

