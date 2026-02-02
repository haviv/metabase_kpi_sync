#!/usr/bin/env python3
"""
Calculate total lines from the raw Copilot data
"""

import requests
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# Configuration
organization = "Pathlock"
team_slug = "platform_nexus_team"
token = os.getenv('GITHUB_TOKEN')

# Date range - last 7 days
until_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
since_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

# API setup
base_url = "https://api.github.com"
headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {token}",
    "X-GitHub-Api-Version": "2022-11-28"
}

# Fetch data
endpoint = (
    f"/orgs/{organization}/team/{team_slug}/copilot/metrics"
    f"?since={since_date}T00:00:00Z&until={until_date}T23:59:59Z&per_page=100"
)
url = f"{base_url}{endpoint}"
response = requests.get(url, headers=headers)

if response.status_code != 200:
    print(f"Error: {response.status_code}")
    exit(1)

data = response.json()

# Calculate totals
total_lines_suggested = 0
total_lines_accepted = 0
total_suggestions = 0
total_acceptances = 0

print(f"Calculating totals for {team_slug}")
print(f"Date range: {since_date} to {until_date}")
print(f"Days of data: {len(data)}\n")
print("=" * 80)

for day_data in data:
    date = day_data.get('date')
    
    # Extract code completions
    code_completions = day_data.get('copilot_ide_code_completions', {})
    
    day_lines_suggested = 0
    day_lines_accepted = 0
    day_suggestions = 0
    day_acceptances = 0
    
    # Sum across all editors/models/languages
    for editor in code_completions.get('editors', []):
        for model in editor.get('models', []):
            for lang in model.get('languages', []):
                lang_name = lang.get('name', 'unknown')
                suggestions = lang.get('total_code_suggestions', 0) or 0
                acceptances = lang.get('total_code_acceptances', 0) or 0
                lines_suggested = lang.get('total_code_lines_suggested', 0) or 0
                lines_accepted = lang.get('total_code_lines_accepted', 0) or 0
                
                day_suggestions += suggestions
                day_acceptances += acceptances
                day_lines_suggested += lines_suggested
                day_lines_accepted += lines_accepted
                
                if lines_accepted > 0 or lines_suggested > 0:
                    print(f"{date} | {lang_name:20s} | Accepted: {lines_accepted:4d} | Suggested: {lines_suggested:4d}")
    
    total_suggestions += day_suggestions
    total_acceptances += day_acceptances
    total_lines_suggested += day_lines_suggested
    total_lines_accepted += day_lines_accepted
    
    print(f"{date} | {'DAILY TOTAL':20s} | Accepted: {day_lines_accepted:4d} | Suggested: {day_lines_suggested:4d}")
    print("-" * 80)

print("=" * 80)
print(f"\n{'PERIOD TOTALS (7 days)':^80}\n")
print(f"Total Code Suggestions:     {total_suggestions:,}")
print(f"Total Code Acceptances:     {total_acceptances:,}")
print(f"Total Lines Suggested:      {total_lines_suggested:,}")
print(f"Total Lines Accepted:       {total_lines_accepted:,}")
print(f"\nAcceptance Rate:            {(total_acceptances/total_suggestions*100) if total_suggestions > 0 else 0:.1f}%")
print(f"Lines Acceptance Rate:      {(total_lines_accepted/total_lines_suggested*100) if total_lines_suggested > 0 else 0:.1f}%")
