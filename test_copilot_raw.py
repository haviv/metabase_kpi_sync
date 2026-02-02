#!/usr/bin/env python3
"""
Test script to fetch and print raw Copilot API data for a specific team
"""

import requests
import os
import json
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
api_version = "2022-11-28"
headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {token}",
    "X-GitHub-Api-Version": api_version
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
    print("\nRAW API RESPONSE:\n")
    print(json.dumps(data, indent=2))
    
    # Also print a summary
    print("\n" + "=" * 80)
    print("SUMMARY:")
    print(f"Number of days returned: {len(data) if isinstance(data, list) else 'N/A'}")
    
    if isinstance(data, list) and len(data) > 0:
        print(f"\nFirst day sample:")
        first_day = data[0]
        print(f"  Date: {first_day.get('date')}")
        print(f"  Total active users: {first_day.get('total_active_users')}")
        print(f"  Total engaged users: {first_day.get('total_engaged_users')}")
        
        # Check code completions structure
        code_completions = first_day.get('copilot_ide_code_completions', {})
        print(f"\n  Code Completions Structure:")
        print(f"    Total engaged users: {code_completions.get('total_engaged_users')}")
        
        editors = code_completions.get('editors', [])
        print(f"    Number of editors: {len(editors)}")
        
        if editors:
            for editor in editors:
                print(f"\n    Editor: {editor.get('name')}")
                models = editor.get('models', [])
                print(f"      Models: {len(models)}")
                
                for model in models:
                    print(f"\n      Model: {model.get('name')}")
                    languages = model.get('languages', [])
                    print(f"        Languages: {len(languages)}")
                    
                    for lang in languages:
                        print(f"\n        Language: {lang.get('name')}")
                        print(f"          Suggestions: {lang.get('total_code_suggestions')}")
                        print(f"          Acceptances: {lang.get('total_code_acceptances')}")
                        print(f"          Lines Suggested: {lang.get('total_code_lines_suggested')}")
                        print(f"          Lines Accepted: {lang.get('total_code_lines_accepted')}")
    
else:
    print(f"\nError Response:")
    print(response.text)
