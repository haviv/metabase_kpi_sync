#!/usr/bin/env python3
"""
Test script to sync GitHub Copilot metrics via export_ado flow
and compare with manual calculation
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# Import required modules
from export_ado import get_database_connection
from export_github_copilot import GitHubCopilotExtractor

# Configuration
github_org = os.getenv('GITHUB_ORG', 'Pathlock')
specific_team = 'platform_nexus_team'

print(f"Testing GitHub Copilot sync for team: {specific_team}")
print(f"Organization: {github_org}")
print("=" * 80)

# Create database connection
db = get_database_connection()

# Create extractor for specific team
copilot_extractor = GitHubCopilotExtractor(
    organization=github_org,
    teams=specific_team,  # Just this one team
    team_sizes="7",  # Approximate size
    db_engine=db.engine
)

# Sync to database (this will fetch last 30 days by default and store)
print("\nSyncing to database...")
processed = copilot_extractor.sync_to_database(db)

print(f"\nProcessed {processed} records")

# Now query the database to see what was stored
print("\n" + "=" * 80)
print("Querying database for stored metrics...")
print("=" * 80)

from sqlalchemy import text

# Get date range for last 7 days
until_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
since_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

with db.engine.connect() as connection:
    # Get all metrics for this team in the last 7 days
    result = connection.execute(
        text("""
            SELECT 
                metric_date,
                metric_name,
                metric_value
            FROM copilot_metrics_daily
            WHERE team_slug = :team
                AND metric_date BETWEEN :since AND :until
                AND metric_name IN ('code_acceptances', 'code_suggestions', 
                                   'code_lines_accepted', 'code_lines_suggested')
            ORDER BY metric_date, metric_name
        """),
        {"team": specific_team, "since": since_date, "until": until_date}
    )
    
    rows = result.fetchall()
    
    # Group by date
    by_date = {}
    for row in rows:
        date = str(row[0])
        metric = row[1]
        value = float(row[2]) if row[2] else 0
        
        if date not in by_date:
            by_date[date] = {}
        by_date[date][metric] = value
    
    print(f"\nMetrics for {specific_team} ({since_date} to {until_date}):\n")
    
    total_suggestions = 0
    total_acceptances = 0
    total_lines_suggested = 0
    total_lines_accepted = 0
    
    for date in sorted(by_date.keys()):
        metrics = by_date[date]
        suggestions = metrics.get('code_suggestions', 0)
        acceptances = metrics.get('code_acceptances', 0)
        lines_sugg = metrics.get('code_lines_suggested', 0)
        lines_acc = metrics.get('code_lines_accepted', 0)
        
        print(f"{date} | Acceptances: {acceptances:4.0f} | Suggestions: {suggestions:4.0f} | "
              f"Lines Accepted: {lines_acc:4.0f} | Lines Suggested: {lines_sugg:4.0f}")
        
        total_suggestions += suggestions
        total_acceptances += acceptances
        total_lines_suggested += lines_sugg
        total_lines_accepted += lines_acc
    
    print("-" * 80)
    print(f"\n{'TOTALS FROM DATABASE':^80}\n")
    print(f"Total Code Acceptances:     {total_acceptances:,.0f}")
    print(f"Total Code Suggestions:     {total_suggestions:,.0f}")
    print(f"Total Lines Accepted:       {total_lines_accepted:,.0f}")
    print(f"Total Lines Suggested:      {total_lines_suggested:,.0f}")
    
    print("\n" + "=" * 80)
    print("COMPARISON WITH MANUAL CALCULATION:")
    print("=" * 80)
    print(f"Manual calculation:  440 acceptances, 475 lines accepted")
    print(f"Database values:     {total_acceptances:.0f} acceptances, {total_lines_accepted:.0f} lines accepted")
    
    if total_acceptances == 440 and total_lines_accepted == 475:
        print("\n✓ VALUES MATCH!")
    else:
        print(f"\n✗ DIFFERENCE: {abs(total_acceptances - 440):.0f} acceptances, {abs(total_lines_accepted - 475):.0f} lines")
