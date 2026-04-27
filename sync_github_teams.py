#!/usr/bin/env python3
"""Sync GitHub org teams and their members into Postgres.

Creates / refreshes three tables in the `public` schema:

  github_teams
    organization  (PK part)
    slug          (PK part)
    name
    description
    privacy       ('closed' | 'secret')
    parent_slug
    html_url
    synced_at

  github_team_members
    organization  (PK part)
    team_slug     (PK part)
    login         (PK part, lowercased)
    role          ('member' | 'maintainer')
    synced_at

  github_users
    login         (PK, lowercased)
    name          — real name from GitHub profile (used to match ADO users)
    email         — public email, often empty for private profiles
    html_url
    synced_at

The sync is a full refresh: every run deletes the existing rows for the org
being synced and re-inserts what GitHub returns right now. Keeps things
simple and avoids drift.

Usage:
    python3 sync_github_teams.py
    python3 sync_github_teams.py --org Pathlock --verbose

Env (all already in .env):
    GITHUB_TOKEN       — PAT with read:org scope
    GITHUB_ORG         — default organization (fallback: Pathlock)
    PG_USERNAME/PG_PASSWORD/PG_HOST/PG_PORT/PG_DATABASE

Reuse from other scripts:
    from sync_github_teams import get_qa_logins
    qa_logins = get_qa_logins(engine, pattern=r"qa|test")
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(Path(__file__).parent / ".env")


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #


GITHUB_API = "https://api.github.com"
DEFAULT_ORG = os.getenv("GITHUB_ORG", "Pathlock")

PG_CONN = (
    f"postgresql://{os.getenv('PG_USERNAME')}:{os.getenv('PG_PASSWORD')}"
    f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT', '5432')}/"
    f"{os.getenv('PG_DATABASE')}"
)


# --------------------------------------------------------------------------- #
# GitHub API
# --------------------------------------------------------------------------- #


def _gh_headers(token: str) -> dict:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _paginate(url: str, headers: dict, verbose: bool = False) -> list:
    """Walk GitHub's page=X pagination until an empty or short page."""
    out: list = []
    page = 1
    while True:
        sep = "&" if "?" in url else "?"
        full = f"{url}{sep}per_page=100&page={page}"
        if verbose:
            print(f"  GET {full}")
        r = requests.get(full, headers=headers, timeout=60)
        if r.status_code == 403 and "rate limit" in (r.text or "").lower():
            reset = int(r.headers.get("X-RateLimit-Reset", "0"))
            wait = max(reset - int(time.time()), 10) + 5
            print(f"  rate limit hit; sleeping {wait}s …")
            time.sleep(wait)
            continue
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or not data:
            break
        out.extend(data)
        if len(data) < 100:
            break
        page += 1
    return out


def fetch_teams(org: str, headers: dict, verbose: bool = False) -> list[dict]:
    return _paginate(f"{GITHUB_API}/orgs/{org}/teams", headers, verbose)


def fetch_team_members(
    org: str, team_slug: str, headers: dict, verbose: bool = False
) -> list[dict]:
    """Include both members and maintainers, each annotated with `role`."""
    url = f"{GITHUB_API}/orgs/{org}/teams/{team_slug}/members?role=all"
    members = _paginate(url, headers, verbose)
    # role=all returns members, but GitHub doesn't tell us which are
    # maintainers in that response. Hit the maintainers-only endpoint to
    # enrich, non-fatal if it fails.
    maintainers: set[str] = set()
    try:
        maint = _paginate(
            f"{GITHUB_API}/orgs/{org}/teams/{team_slug}/members?role=maintainer",
            headers, verbose,
        )
        maintainers = {m["login"].lower() for m in maint if m.get("login")}
    except requests.HTTPError:
        pass
    for m in members:
        login = (m.get("login") or "").lower()
        m["_role"] = "maintainer" if login in maintainers else "member"
    return members


# --------------------------------------------------------------------------- #
# Postgres
# --------------------------------------------------------------------------- #


DDL_TEAMS = """
CREATE TABLE IF NOT EXISTS public.github_teams (
    organization VARCHAR(200) NOT NULL,
    slug         VARCHAR(200) NOT NULL,
    name         VARCHAR(400),
    description  TEXT,
    privacy      VARCHAR(20),
    parent_slug  VARCHAR(200),
    html_url     TEXT,
    synced_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (organization, slug)
);
"""

DDL_MEMBERS = """
CREATE TABLE IF NOT EXISTS public.github_team_members (
    organization VARCHAR(200) NOT NULL,
    team_slug    VARCHAR(200) NOT NULL,
    login        VARCHAR(200) NOT NULL,
    role         VARCHAR(20)  NOT NULL DEFAULT 'member',
    synced_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (organization, team_slug, login)
);
"""

DDL_USERS = """
CREATE TABLE IF NOT EXISTS public.github_users (
    login     VARCHAR(200) PRIMARY KEY,
    name      VARCHAR(400),
    email     VARCHAR(400),
    html_url  TEXT,
    synced_at TIMESTAMPTZ NOT NULL
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_gh_team_members_login ON public.github_team_members(login);",
    "CREATE INDEX IF NOT EXISTS idx_gh_team_members_team  ON public.github_team_members(organization, team_slug);",
    "CREATE INDEX IF NOT EXISTS idx_gh_users_name         ON public.github_users(LOWER(name));",
    "CREATE INDEX IF NOT EXISTS idx_gh_users_email        ON public.github_users(LOWER(email));",
]


def ensure_schema(engine) -> None:
    with engine.begin() as c:
        c.execute(text(DDL_TEAMS))
        c.execute(text(DDL_MEMBERS))
        c.execute(text(DDL_USERS))
        for stmt in DDL_INDEXES:
            c.execute(text(stmt))


def fetch_user_profile(login: str, headers: dict, verbose: bool = False) -> dict | None:
    url = f"{GITHUB_API}/users/{login}"
    if verbose:
        print(f"  GET {url}")
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code == 404:
        return None
    if r.status_code == 403 and "rate limit" in (r.text or "").lower():
        reset = int(r.headers.get("X-RateLimit-Reset", "0"))
        wait = max(reset - int(time.time()), 10) + 5
        print(f"  rate limit hit; sleeping {wait}s …")
        time.sleep(wait)
        r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def upsert_users(engine, users: list[dict]) -> int:
    if not users:
        return 0
    now = datetime.now(timezone.utc)
    rows = [
        {
            "login": (u.get("login") or "").lower(),
            "name": u.get("name"),
            "email": u.get("email"),
            "html_url": u.get("html_url"),
            "synced_at": now,
        }
        for u in users
        if u and u.get("login")
    ]
    with engine.begin() as c:
        c.execute(
            text(
                """
                INSERT INTO public.github_users (login, name, email, html_url, synced_at)
                VALUES (:login, :name, :email, :html_url, :synced_at)
                ON CONFLICT (login) DO UPDATE SET
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    html_url = EXCLUDED.html_url,
                    synced_at = EXCLUDED.synced_at
                """
            ),
            rows,
        )
    return len(rows)


def replace_org_data(
    engine,
    org: str,
    teams: list[dict],
    members_by_team: dict[str, list[dict]],
) -> tuple[int, int]:
    now = datetime.now(timezone.utc)
    with engine.begin() as c:
        c.execute(
            text(
                "DELETE FROM public.github_team_members WHERE organization = :org"
            ),
            {"org": org},
        )
        c.execute(
            text("DELETE FROM public.github_teams WHERE organization = :org"),
            {"org": org},
        )
        team_rows = [
            {
                "organization": org,
                "slug": t["slug"],
                "name": t.get("name"),
                "description": t.get("description"),
                "privacy": t.get("privacy"),
                "parent_slug": (t.get("parent") or {}).get("slug") if t.get("parent") else None,
                "html_url": t.get("html_url"),
                "synced_at": now,
            }
            for t in teams
        ]
        if team_rows:
            c.execute(
                text(
                    "INSERT INTO public.github_teams "
                    "(organization, slug, name, description, privacy, parent_slug, html_url, synced_at) "
                    "VALUES (:organization, :slug, :name, :description, :privacy, :parent_slug, :html_url, :synced_at)"
                ),
                team_rows,
            )
        member_rows = []
        for slug, members in members_by_team.items():
            seen: set[str] = set()
            for m in members:
                login = (m.get("login") or "").lower()
                if not login or login in seen:
                    continue
                seen.add(login)
                member_rows.append(
                    {
                        "organization": org,
                        "team_slug": slug,
                        "login": login,
                        "role": m.get("_role", "member"),
                        "synced_at": now,
                    }
                )
        if member_rows:
            c.execute(
                text(
                    "INSERT INTO public.github_team_members "
                    "(organization, team_slug, login, role, synced_at) "
                    "VALUES (:organization, :team_slug, :login, :role, :synced_at)"
                ),
                member_rows,
            )
    return len(team_rows), len(member_rows)


# --------------------------------------------------------------------------- #
# Reusable helper for other scripts
# --------------------------------------------------------------------------- #


def get_qa_logins(
    engine,
    pattern: str = r"qa|test",
    org: str | None = None,
) -> set[str]:
    """Return the union of member logins (lowercased) across all teams whose
    slug or name matches `pattern` (case-insensitive regex)."""
    org_clause = "AND m.organization = :org" if org else ""
    sql = text(
        f"""
        SELECT DISTINCT m.login
        FROM public.github_team_members m
        JOIN public.github_teams t
          ON t.organization = m.organization AND t.slug = m.team_slug
        WHERE (t.slug ~* :pat OR COALESCE(t.name, '') ~* :pat)
          {org_clause}
        """
    )
    params = {"pat": pattern}
    if org:
        params["org"] = org
    with engine.connect() as c:
        rows = c.execute(sql, params).all()
    return {r[0] for r in rows}


def get_qa_user_profiles(
    engine,
    pattern: str = r"qa|test",
    org: str | None = None,
) -> list[dict]:
    """Return [{login, name, email}] for all members of QA-like teams.

    Suitable for matching against ADO System.CreatedBy:
      - match by lowercased name (displayName)
      - match by lowercased email (uniqueName)
      - fall back to login vs email local-part
    """
    org_clause = "AND m.organization = :org" if org else ""
    sql = text(
        f"""
        SELECT DISTINCT u.login, u.name, u.email
        FROM public.github_team_members m
        JOIN public.github_teams t
          ON t.organization = m.organization AND t.slug = m.team_slug
        LEFT JOIN public.github_users u ON u.login = m.login
        WHERE (t.slug ~* :pat OR COALESCE(t.name, '') ~* :pat)
          {org_clause}
        """
    )
    params = {"pat": pattern}
    if org:
        params["org"] = org
    with engine.connect() as c:
        return [dict(r) for r in c.execute(sql, params).mappings()]


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--org", default=DEFAULT_ORG, help=f"org to sync (default: {DEFAULT_ORG})")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--show-qa",
        metavar="REGEX",
        nargs="?",
        const=r"qa|test",
        help="After syncing, print the QA-login set from teams matching the regex (default: 'qa|test').",
    )
    args = parser.parse_args()

    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print("GITHUB_TOKEN env var is required", file=sys.stderr)
        return 2

    org = args.org
    headers = _gh_headers(token)
    engine = create_engine(PG_CONN)
    ensure_schema(engine)

    print(f"Fetching teams for org={org} …")
    teams = fetch_teams(org, headers, args.verbose)
    print(f"  {len(teams)} teams returned by GitHub")

    members_by_team: dict[str, list[dict]] = {}
    for i, t in enumerate(teams, 1):
        slug = t["slug"]
        print(f"  [{i}/{len(teams)}] members for {slug} …", end=" ", flush=True)
        members = fetch_team_members(org, slug, headers, args.verbose)
        members_by_team[slug] = members
        print(f"{len(members)} members")

    n_teams, n_members = replace_org_data(engine, org, teams, members_by_team)
    print(f"\nWrote {n_teams} teams and {n_members} memberships "
          f"for org '{org}' into Postgres.")

    # Fetch user profiles (name + email) for every unique member we just saw.
    # Used to map ADO System.CreatedBy (displayName / email) -> GitHub user.
    unique_logins: set[str] = set()
    for members in members_by_team.values():
        for m in members:
            login = (m.get("login") or "").lower()
            if login:
                unique_logins.add(login)
    print(f"\nFetching profile (name+email) for {len(unique_logins)} unique users …")
    profiles: list[dict] = []
    for i, login in enumerate(sorted(unique_logins), 1):
        if i % 25 == 0 or i == len(unique_logins):
            print(f"  {i}/{len(unique_logins)} …")
        try:
            profile = fetch_user_profile(login, headers, args.verbose)
            if profile:
                profiles.append(profile)
        except requests.HTTPError as e:
            print(f"  skip {login}: {e}")
    n_users = upsert_users(engine, profiles)
    print(f"Wrote {n_users} user profiles.")

    if args.show_qa is not None:
        qa = get_qa_logins(engine, args.show_qa, org=org)
        print(f"\nQA logins via pattern /{args.show_qa}/ : {len(qa)} people")
        for login in sorted(qa):
            print(f"  - {login}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
