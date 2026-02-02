#!/usr/bin/env python3
"""
GitHub Repository Statistics Script
Analyzes commit activity, code changes, and PR metrics for a team over the last year
"""

import subprocess
import json
from datetime import datetime, timedelta
from collections import defaultdict
import re
import sys

# Configuration
REPO_PATH = "/Users/havivrosh/work/PathlockGRC"
BRANCH = "origin/cloud/dev"
MONTHS_BACK = 12

def run_git_command(cmd, cwd=REPO_PATH):
    """Execute a git command and return the output"""
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {' '.join(cmd)}")
        print(f"Error: {e.stderr}")
        return None

def get_since_date():
    """Calculate the date from MONTHS_BACK ago"""
    date = datetime.now() - timedelta(days=MONTHS_BACK * 30)
    return date.strftime("%Y-%m-%d")

def get_commit_activity():
    """
    Metric 1: Commit Activity & Contribution Distribution
    Returns: commits per month, commits per developer, active contributors per month
    """
    print("📊 Analyzing Commit Activity & Contribution Distribution...")
    
    since_date = get_since_date()
    
    # Get all commits with author and date
    cmd = [
        "git", "log", BRANCH,
        "--since", since_date,
        "--pretty=format:%H|%an|%ae|%ad",
        "--date=format:%Y-%m"
    ]
    
    output = run_git_command(cmd)
    if not output:
        return None
    
    commits_by_month = defaultdict(int)
    commits_by_developer = defaultdict(int)
    monthly_contributors = defaultdict(set)
    
    for line in output.split('\n'):
        if not line:
            continue
        
        parts = line.split('|')
        if len(parts) >= 4:
            commit_hash, author_name, author_email, month = parts[0], parts[1], parts[2], parts[3]
            
            commits_by_month[month] += 1
            commits_by_developer[author_name] += 1
            monthly_contributors[month].add(author_name)
    
    # Sort by month
    sorted_months = sorted(commits_by_month.keys())
    
    results = {
        "total_commits": sum(commits_by_month.values()),
        "commits_per_month": dict(sorted((k, v) for k, v in commits_by_month.items())),
        "commits_per_developer": dict(sorted(commits_by_developer.items(), key=lambda x: x[1], reverse=True)),
        "active_contributors_per_month": {month: len(contributors) for month, contributors in sorted(monthly_contributors.items())},
        "unique_contributors": len(commits_by_developer)
    }
    
    return results

def get_code_volume_changes():
    """
    Metric 2: Code Volume Changes
    Returns: lines added/deleted per month, net change
    """
    print("📈 Analyzing Code Volume Changes...")
    
    since_date = get_since_date()
    
    # Get commit hashes with dates
    cmd = [
        "git", "log", BRANCH,
        "--since", since_date,
        "--pretty=format:%H|%ad",
        "--date=format:%Y-%m"
    ]
    
    output = run_git_command(cmd)
    if not output:
        return None
    
    commits_by_month = defaultdict(list)
    
    for line in output.split('\n'):
        if not line:
            continue
        parts = line.split('|')
        if len(parts) >= 2:
            commit_hash, month = parts[0], parts[1]
            commits_by_month[month].append(commit_hash)
    
    # Get stats for each month
    monthly_stats = {}
    
    for month, commits in sorted(commits_by_month.items()):
        lines_added = 0
        lines_deleted = 0
        
        for commit_hash in commits:
            # Get numstat for this commit
            cmd = ["git", "show", "--numstat", "--format=", commit_hash]
            stat_output = run_git_command(cmd)
            
            if stat_output:
                for line in stat_output.split('\n'):
                    if not line.strip():
                        continue
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        added = parts[0]
                        deleted = parts[1]
                        
                        # Handle binary files (marked as '-')
                        if added != '-' and added.isdigit():
                            lines_added += int(added)
                        if deleted != '-' and deleted.isdigit():
                            lines_deleted += int(deleted)
        
        monthly_stats[month] = {
            "lines_added": lines_added,
            "lines_deleted": lines_deleted,
            "net_change": lines_added - lines_deleted,
            "total_changes": lines_added + lines_deleted
        }
    
    total_added = sum(stats["lines_added"] for stats in monthly_stats.values())
    total_deleted = sum(stats["lines_deleted"] for stats in monthly_stats.values())
    
    results = {
        "monthly_stats": monthly_stats,
        "total_lines_added": total_added,
        "total_lines_deleted": total_deleted,
        "net_change": total_added - total_deleted,
        "total_churn": total_added + total_deleted
    }
    
    return results

def get_pr_metrics():
    """
    Metric 3: Pull Request Metrics
    Note: This requires GitHub API or parsing from git log merge commits
    For now, we'll analyze merge commits as a proxy
    """
    print("🔀 Analyzing Pull Request Metrics...")
    
    since_date = get_since_date()
    
    # Get merge commits (typically indicate PRs)
    cmd = [
        "git", "log", BRANCH,
        "--since", since_date,
        "--merges",
        "--pretty=format:%H|%ad|%s",
        "--date=format:%Y-%m"
    ]
    
    output = run_git_command(cmd)
    if not output:
        return {"note": "No merge commits found or unable to fetch data"}
    
    prs_by_month = defaultdict(int)
    pr_commits = []
    
    for line in output.split('\n'):
        if not line:
            continue
        parts = line.split('|')
        if len(parts) >= 3:
            commit_hash, month, subject = parts[0], parts[1], parts[2]
            prs_by_month[month] += 1
            pr_commits.append({"hash": commit_hash, "month": month, "subject": subject})
    
    # Calculate average PR size
    pr_sizes = []
    for pr in pr_commits[:50]:  # Limit to avoid too many calls
        cmd = ["git", "show", "--shortstat", "--format=", pr["hash"]]
        stat_output = run_git_command(cmd)
        
        if stat_output:
            # Parse: "X files changed, Y insertions(+), Z deletions(-)"
            match = re.search(r'(\d+) insertion.*?(\d+) deletion', stat_output)
            if match:
                insertions = int(match.group(1))
                deletions = int(match.group(2))
                pr_sizes.append(insertions + deletions)
    
    avg_pr_size = sum(pr_sizes) / len(pr_sizes) if pr_sizes else 0
    
    results = {
        "total_merge_commits": len(pr_commits),
        "merges_per_month": dict(sorted(prs_by_month.items())),
        "average_pr_size_lines": int(avg_pr_size),
        "note": "Based on merge commits as proxy for PRs"
    }
    
    return results

def print_results(results):
    """Pretty print the results"""
    print("\n" + "="*80)
    print(f"📊 GITHUB TEAM STATISTICS REPORT")
    print(f"Repository: {REPO_PATH}")
    print(f"Branch: {BRANCH}")
    print(f"Period: Last {MONTHS_BACK} months (since {get_since_date()})")
    print("="*80 + "\n")
    
    # Metric 1: Commit Activity
    if "commit_activity" in results:
        data = results["commit_activity"]
        print("1️⃣  COMMIT ACTIVITY & CONTRIBUTION DISTRIBUTION")
        print("-" * 80)
        print(f"Total Commits: {data['total_commits']}")
        print(f"Unique Contributors: {data['unique_contributors']}")
        
        print("\n📅 Commits per Month:")
        for month, count in data['commits_per_month'].items():
            bar = "█" * (count // 10) if count > 0 else ""
            print(f"  {month}: {count:4d} {bar}")
        
        print("\n👥 Commits per Developer:")
        for dev, count in list(data['commits_per_developer'].items())[:10]:
            bar = "█" * (count // 10) if count > 0 else ""
            print(f"  {dev:30s}: {count:4d} {bar}")
        
        print("\n👤 Active Contributors per Month:")
        for month, count in data['active_contributors_per_month'].items():
            print(f"  {month}: {count} contributors")
        
        print()
    
    # Metric 2: Code Volume
    if "code_volume" in results:
        data = results["code_volume"]
        print("\n2️⃣  CODE VOLUME CHANGES")
        print("-" * 80)
        print(f"Total Lines Added: {data['total_lines_added']:,}")
        print(f"Total Lines Deleted: {data['total_lines_deleted']:,}")
        print(f"Net Change: {data['net_change']:+,}")
        print(f"Total Code Churn: {data['total_churn']:,}")
        
        print("\n📊 Monthly Breakdown:")
        for month, stats in data['monthly_stats'].items():
            print(f"  {month}:")
            print(f"    Added: {stats['lines_added']:8,} | Deleted: {stats['lines_deleted']:8,} | Net: {stats['net_change']:+8,}")
        
        print()
    
    # Metric 3: PR Metrics
    if "pr_metrics" in results:
        data = results["pr_metrics"]
        print("\n3️⃣  PULL REQUEST METRICS")
        print("-" * 80)
        print(f"Total Merge Commits: {data['total_merge_commits']}")
        print(f"Average PR Size: {data['average_pr_size_lines']} lines changed")
        
        print("\n🔀 Merges per Month:")
        for month, count in data['merges_per_month'].items():
            bar = "█" * (count // 5) if count > 0 else ""
            print(f"  {month}: {count:3d} {bar}")
        
        if "note" in data:
            print(f"\nNote: {data['note']}")
        
        print()

def main():
    """Main execution"""
    print(f"🚀 Starting GitHub Team Statistics Analysis\n")
    print(f"Repository: {REPO_PATH}")
    print(f"Branch: {BRANCH}\n")
    
    # Check if repo exists
    if not run_git_command(["git", "rev-parse", "--git-dir"]):
        print(f"❌ Error: Not a git repository or repository not found: {REPO_PATH}")
        sys.exit(1)
    
    # Check if branch exists
    branch_check = run_git_command(["git", "rev-parse", "--verify", BRANCH])
    if not branch_check:
        print(f"❌ Error: Branch '{BRANCH}' not found")
        sys.exit(1)
    
    results = {}
    
    # Run analyses
    commit_activity = get_commit_activity()
    if commit_activity:
        results["commit_activity"] = commit_activity
    
    code_volume = get_code_volume_changes()
    if code_volume:
        results["code_volume"] = code_volume
    
    pr_metrics = get_pr_metrics()
    if pr_metrics:
        results["pr_metrics"] = pr_metrics
    
    # Display results
    print_results(results)
    
    # Optionally save to JSON
    output_file = "github_team_stats.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n💾 Results saved to: {output_file}")

if __name__ == "__main__":
    main()
