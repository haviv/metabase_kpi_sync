#!/usr/bin/env python3
"""
Fetch Copilot metrics for test-team directly from GitHub API
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
team_slug = "test-team"
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

# Make request
endpoint = (
    f"/orgs/{organization}/team/{team_slug}/copilot/metrics"
    f"?since={since_date}T00:00:00Z&until={until_date}T23:59:59Z&per_page=100"
)

url = f"{base_url}{endpoint}"

print(f"Fetching Copilot metrics for team: {team_slug}")
print(f"Organization: {organization}")
print(f"Date range: {since_date} to {until_date}")
print(f"\nAPI URL: {url}\n")
print("=" * 80)

response = requests.get(url, headers=headers)

print(f"Status Code: {response.status_code}")
print("=" * 80)

if response.status_code == 200:
    data = response.json()
    
    print("\nRAW API RESPONSE:")
    print("=" * 80)
    import json
    print(json.dumps(data, indent=2))
    print("=" * 80)
    
    if not data or len(data) == 0:
        print("\nNo data returned (team may have < 5 active users or no activity)")
        exit(0)
    
    # Calculate totals
    total_lines_suggested = 0
    total_lines_accepted = 0
    total_suggestions = 0
    total_acceptances = 0
    
    print(f"\nMetrics for {team_slug}:")
    print(f"Days of data: {len(data)}\n")
    
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
                    day_suggestions += lang.get('total_code_suggestions', 0) or 0
                    day_acceptances += lang.get('total_code_acceptances', 0) or 0
                    day_lines_suggested += lang.get('total_code_lines_suggested', 0) or 0
                    day_lines_accepted += lang.get('total_code_lines_accepted', 0) or 0
        
        total_suggestions += day_suggestions
        total_acceptances += day_acceptances
        total_lines_suggested += day_lines_suggested
        total_lines_accepted += day_lines_accepted
        
        print(f"{date} | Acceptances: {day_acceptances:4d} | Suggestions: {day_suggestions:4d} | "
              f"Lines Accepted: {day_lines_accepted:4d} | Lines Suggested: {day_lines_suggested:4d}")
    
    print("-" * 80)
    print(f"\nTOTALS FOR {team_slug.upper()} (7 days):\n")
    print(f"Total Code Suggestions:     {total_suggestions:,}")
    print(f"Total Code Acceptances:     {total_acceptances:,}")
    print(f"Total Lines Suggested:      {total_lines_suggested:,}")
    print(f"Total Lines Accepted:       {total_lines_accepted:,}")
    
    if total_suggestions > 0:
        print(f"\nAcceptance Rate:            {(total_acceptances/total_suggestions*100):.1f}%")
    if total_lines_suggested > 0:
        print(f"Lines Acceptance Rate:      {(total_lines_accepted/total_lines_suggested*100):.1f}%")
    
elif response.status_code == 404:
    print(f"\nError: Team '{team_slug}' not found in organization '{organization}'")
    print("\nTrying to list all teams in the organization...")
    
    teams_url = f"{base_url}/orgs/{organization}/teams?per_page=100"
    teams_response = requests.get(teams_url, headers=headers)
    
    if teams_response.status_code == 200:
        teams = teams_response.json()
        print(f"\nAvailable teams in {organization}:")
        for team in teams:
            print(f"  - {team['slug']}")
    else:
        print(f"Could not fetch teams: {teams_response.status_code}")
else:
    print(f"\nError Response:")
    try:
        print(response.json())
    except:
        print(response.text)
