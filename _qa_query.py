#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, text

load_dotenv(dotenv_path=Path(__file__).parent / '.env')
engine = create_engine(
    f"postgresql://{os.getenv('PG_USERNAME')}:{os.getenv('PG_PASSWORD')}@"
    f"{os.getenv('PG_HOST')}:{os.getenv('PG_PORT','5432')}/{os.getenv('PG_DATABASE')}"
)

with engine.connect() as conn:
    # Find QA-related teams
    r = conn.execute(text("""
        SELECT DISTINCT team_slug FROM pr_metrics_daily
        WHERE team_slug ILIKE '%qa%' OR team_slug ILIKE '%automation%'
        ORDER BY team_slug
    """))
    print("QA/automation related teams in DB:")
    for row in r.fetchall():
        print(f"  {row[0]}")

    # Check repos
    print()
    r = conn.execute(text("""
        SELECT DISTINCT repository FROM pr_metrics_daily
        WHERE repository ILIKE '%automation%' OR repository ILIKE '%qa%'
        ORDER BY repository
    """))
    print("QA/automation related repos in DB:")
    for row in r.fetchall():
        print(f"  {row[0]}")

    # Weekly PR counts for QA automation teams
    for team in ['plc-qa-automation', 'qa', 'pathlock_qa_team', 'qa_team']:
        print(f"\n=== {team} - merged PRs by week (last 2 weeks) ===")
        r = conn.execute(text("""
            SELECT
                date_trunc('week', metric_date)::date as week_start,
                SUM(metric_value) as total
            FROM pr_metrics_daily
            WHERE team_slug = :team
              AND metric_name = 'merged_prs_count'
              AND repository = 'all'
              AND metric_date >= CURRENT_DATE - INTERVAL '14 days'
            GROUP BY date_trunc('week', metric_date)
            ORDER BY week_start
        """), {'team': team})
        rows = r.fetchall()
        for row in rows:
            print(f"  Week of {row[0]}: {int(row[1])} PRs")
        if not rows:
            print("  (no data)")

    # Member breakdown for plc-qa-automation
    print("\n=== plc-qa-automation MEMBERS by week (last 2 weeks) ===")
    # First get team members from GitHub
    import requests
    token = os.getenv('GITHUB_TOKEN')
    headers = {"Accept": "application/vnd.github+json", "Authorization": f"Bearer {token}", "X-GitHub-Api-Version": "2022-11-28"}
    resp = requests.get("https://api.github.com/orgs/Pathlock/teams/plc-qa-automation/members?per_page=100", headers=headers)
    if resp.status_code == 200:
        members = [m['login'].lower() for m in resp.json()]
        print(f"Team has {len(members)} members: {sorted(members)}")
        placeholders = ", ".join([f":m{i}" for i in range(len(members))])
        params = {f"m{i}": m for i, m in enumerate(members)}
        r = conn.execute(text(f"""
            SELECT
                date_trunc('week', metric_date)::date as week_start,
                member_login,
                SUM(merged_prs_count) as total
            FROM pr_metrics_member_daily
            WHERE repository = 'all'
              AND member_login IN ({placeholders})
              AND metric_date >= CURRENT_DATE - INTERVAL '14 days'
            GROUP BY date_trunc('week', metric_date), member_login
            ORDER BY week_start, total DESC
        """), params)
        rows = r.fetchall()
        for row in rows:
            print(f"  Week of {row[0]} | {row[1]}: {int(row[2])} PRs")
        if not rows:
            print("  (no member data)")

    # Also check by repo (plc-qa-momentic, thanos-plc-automation, plc-automation)
    print("\n=== PRs by QA automation REPOS (last 2 weeks) ===")
    for repo in ['plc-automation', 'plc-qa-momentic', 'thanos-plc-automation']:
        r = conn.execute(text("""
            SELECT
                date_trunc('week', metric_date)::date as week_start,
                SUM(metric_value) as total
            FROM pr_metrics_daily
            WHERE repository = :repo
              AND metric_name = 'merged_prs_count'
              AND metric_date >= CURRENT_DATE - INTERVAL '14 days'
            GROUP BY date_trunc('week', metric_date)
            ORDER BY week_start
        """), {'repo': repo})
        rows = r.fetchall()
        if rows:
            for row in rows:
                print(f"  {repo} | Week of {row[0]}: {int(row[1])} PRs")
        else:
            print(f"  {repo}: (no data)")
