#!/usr/bin/env python3
"""
Get Copilot metrics for test-team
"""

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import text

# Load environment variables
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

from export_ado import get_database_connection

# Configuration
team_name = 'test-team'

# Get date range for last 7 days
until_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
since_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

print(f"Fetching Copilot metrics for: {team_name}")
print(f"Date range: {since_date} to {until_date}")
print("=" * 80)

# Create database connection
db = get_database_connection()

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
        {"team": team_name, "since": since_date, "until": until_date}
    )
    
    rows = result.fetchall()
    
    if not rows:
        print(f"\nNo data found for team '{team_name}' in the specified date range.")
        print("\nChecking if team exists in database...")
        
        # Check what teams exist
        teams_result = connection.execute(
            text("""
                SELECT DISTINCT team_slug 
                FROM copilot_metrics_daily 
                ORDER BY team_slug
            """)
        )
        
        teams = [row[0] for row in teams_result.fetchall()]
        print(f"\nAvailable teams in database: {', '.join(teams)}")
        exit(0)
    
    # Group by date
    by_date = {}
    for row in rows:
        date = str(row[0])
        metric = row[1]
        value = float(row[2]) if row[2] else 0
        
        if date not in by_date:
            by_date[date] = {}
        by_date[date][metric] = value
    
    print(f"\nMetrics for {team_name}:\n")
    
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
    print(f"\nTOTALS FOR {team_name.upper()} (7 days):\n")
    print(f"Total Code Acceptances:     {total_acceptances:,.0f}")
    print(f"Total Code Suggestions:     {total_suggestions:,.0f}")
    print(f"Total Lines Accepted:       {total_lines_accepted:,.0f}")
    print(f"Total Lines Suggested:      {total_lines_suggested:,.0f}")
    
    if total_suggestions > 0:
        print(f"\nAcceptance Rate:            {(total_acceptances/total_suggestions*100):.1f}%")
    if total_lines_suggested > 0:
        print(f"Lines Acceptance Rate:      {(total_lines_accepted/total_lines_suggested*100):.1f}%")
