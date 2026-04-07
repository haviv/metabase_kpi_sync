#!/usr/bin/env python3
"""Analyze the gap between team leader's 55 PRs and our 25 for plc-qa-automation."""
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
    # 1. Check what the team-level data shows per repo for plc-qa-automation, week of Mar 16
    print("=== plc-qa-automation team PRs by REPO, week of Mar 16 ===")
    r = conn.execute(text("""
        SELECT repository, SUM(metric_value) as total
        FROM pr_metrics_daily
        WHERE team_slug = 'plc-qa-automation'
          AND metric_name = 'merged_prs_count'
          AND metric_date >= '2026-03-16' AND metric_date <= '2026-03-22'
        GROUP BY repository
        ORDER BY total DESC
    """))
    for row in r.fetchall():
        print(f"  {row[0]}: {int(row[1])} PRs")

    # 2. Check what repo-level data shows (all teams aggregated) for QA repos
    print("\n=== ALL teams PRs in QA repos, week of Mar 16 ===")
    r = conn.execute(text("""
        SELECT repository, team_slug, SUM(metric_value) as total
        FROM pr_metrics_daily
        WHERE repository IN ('plc-automation', 'thanos-plc-automation', 'plc-qa-momentic')
          AND metric_name = 'merged_prs_count'
          AND metric_date >= '2026-03-16' AND metric_date <= '2026-03-22'
        GROUP BY repository, team_slug
        ORDER BY repository, total DESC
    """))
    for row in r.fetchall():
        print(f"  {row[0]} | {row[1]}: {int(row[2])} PRs")

    # 3. Member-level for plc-qa-automation members in QA repos specifically
    members = ['a-qa-lab', 'aakashthube', 'apoorva-p-eng', 'chhavi771',
               'ishaanvashistpathlock', 'kalpeshgit-art', 'omkarshirke28',
               'pradeepkumar-report', 'rubychawla17', 'rushikeshyengupatla',
               'sanjeevank1', 'tmazire10', 'viditmohil26', 'vivektiwaripl']

    print("\n=== Member PRs by REPO, week of Mar 16 ===")
    placeholders = ", ".join([f":m{i}" for i in range(len(members))])
    params = {f"m{i}": m for i, m in enumerate(members)}
    r = conn.execute(text(f"""
        SELECT member_login, repository, SUM(merged_prs_count) as total
        FROM pr_metrics_member_daily
        WHERE member_login IN ({placeholders})
          AND metric_date >= '2026-03-16' AND metric_date <= '2026-03-22'
        GROUP BY member_login, repository
        ORDER BY repository, total DESC
    """), params)
    for row in r.fetchall():
        print(f"  {row[0]:<30} {row[1]:<30} {int(row[2])}")

    # 4. Check daily detail for these repos
    print("\n=== Daily detail: plc-automation, week of Mar 16 ===")
    r = conn.execute(text("""
        SELECT metric_date, team_slug, metric_value
        FROM pr_metrics_daily
        WHERE repository = 'plc-automation'
          AND metric_name = 'merged_prs_count'
          AND metric_date >= '2026-03-16' AND metric_date <= '2026-03-22'
        ORDER BY metric_date, team_slug
    """))
    for row in r.fetchall():
        print(f"  {row[0]} | {row[1]}: {int(row[2])}")

    print("\n=== Daily detail: thanos-plc-automation, week of Mar 16 ===")
    r = conn.execute(text("""
        SELECT metric_date, team_slug, metric_value
        FROM pr_metrics_daily
        WHERE repository = 'thanos-plc-automation'
          AND metric_name = 'merged_prs_count'
          AND metric_date >= '2026-03-16' AND metric_date <= '2026-03-22'
        ORDER BY metric_date, team_slug
    """))
    for row in r.fetchall():
        print(f"  {row[0]} | {row[1]}: {int(row[2])}")

    # 5. The key question: what does 'all' repo show for plc-qa-automation?
    print("\n=== plc-qa-automation 'all' repo daily, week of Mar 16 ===")
    r = conn.execute(text("""
        SELECT metric_date, metric_value
        FROM pr_metrics_daily
        WHERE team_slug = 'plc-qa-automation'
          AND repository = 'all'
          AND metric_name = 'merged_prs_count'
          AND metric_date >= '2026-03-16' AND metric_date <= '2026-03-22'
        ORDER BY metric_date
    """))
    total = 0
    for row in r.fetchall():
        total += int(row[1])
        print(f"  {row[0]}: {int(row[1])}")
    print(f"  TOTAL: {total}")

    # 6. Check when data was last synced for these repos
    print("\n=== Last sync date per repo ===")
    r = conn.execute(text("""
        SELECT repository, MAX(metric_date) as last_date, MIN(metric_date) as first_date
        FROM pr_metrics_daily
        WHERE repository IN ('plc-automation', 'thanos-plc-automation', 'plc-qa-momentic', 'pathlock-plc')
        GROUP BY repository
        ORDER BY repository
    """))
    for row in r.fetchall():
        print(f"  {row[0]}: first={row[1]}, last={row[2]}")

    # 7. Count the specific PRs from the team leader's list that we're missing
    # Compare team leader's thanos authors with our member data
    print("\n=== Team leader's authors vs our member counts (thanos, Mar 16-22) ===")
    leader_thanos_authors = {
        'tmazire10': 1, 'aakashthube': 2, 'viditmohil26': 4, 'vivektiwaripl': 11,
        'rushikeshyengupatla': 2, 'rubychawla17': 8, 'kalpeshgit-art': 3,
        'a-qa-lab': 1, 'pradeepkumar-report': 3, 'omkarshirke28': 4,
        'ishaanvashistpathlock': 2, 'chhavi771': 1, 'sanjeevank1': 0, 'apoorva-p-eng': 0
    }
    leader_plc_authors = {
        'aakashthube': 3, 'rubychawla17': 2, 'viditmohil26': 1, 'vivektiwaripl': 1,
        'apoorva-p-eng': 2, 'omkarshirke28': 1, 'sanjeevank1': 1, 'a-qa-lab': 1,
        'kalpeshgit-art': 1
    }

    for repo_name, leader_counts in [('thanos-plc-automation', leader_thanos_authors), ('plc-automation', leader_plc_authors)]:
        print(f"\n  {repo_name}:")
        r = conn.execute(text(f"""
            SELECT member_login, SUM(merged_prs_count) as total
            FROM pr_metrics_member_daily
            WHERE repository = :repo
              AND member_login IN ({placeholders})
              AND metric_date >= '2026-03-16' AND metric_date <= '2026-03-22'
            GROUP BY member_login
        """), {**params, 'repo': repo_name})
        db_counts = {row[0]: int(row[1]) for row in r.fetchall()}
        
        print(f"    {'Member':<30} {'Leader':>8} {'DB':>8} {'Gap':>8}")
        total_leader = 0
        total_db = 0
        for member in sorted(set(list(leader_counts.keys()) + list(db_counts.keys()))):
            lc = leader_counts.get(member, 0)
            dc = db_counts.get(member, 0)
            gap = lc - dc
            total_leader += lc
            total_db += dc
            if lc > 0 or dc > 0:
                print(f"    {member:<30} {lc:>8} {dc:>8} {gap:>+8}")
        print(f"    {'TOTAL':<30} {total_leader:>8} {total_db:>8} {total_leader-total_db:>+8}")
