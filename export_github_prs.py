#!/usr/bin/env python3
"""
GitHub PR Metrics by Team
============================================================================
Fetches Pull Request metrics for all teams in an organization
and stores daily metrics in the database.

Metrics tracked per team per day:
- avg_pr_cycle_time_hours: Average time from PR opened to merged
- merged_prs_count: Number of PRs merged
- avg_time_to_first_review_hours: Average time from PR opened to first review

Usage (standalone):
    python export_github_prs.py --org Pathlock --repo pathlock-plc --teams "team1,team2"
"""

import requests
import os
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, text, Table, Column, Integer, String, DateTime, MetaData, Float, Date, Numeric
from sqlalchemy.exc import SQLAlchemyError

# Configuration constants
DELTA_SYNC_OVERLAP_DAYS = 2  # Days to overlap when doing delta sync (to catch late-merged PRs)

# Load environment variables
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)


class PRMetricsDatabase:
    """Database connection and operations for PR metrics"""
    
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
        """Create pr_metrics_daily table with flexible key-value format"""
        self.pr_metrics_daily = Table(
            'pr_metrics_daily', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('metric_date', Date, nullable=False),
            Column('organization', String(200), nullable=False),
            Column('repository', String(200), nullable=False),
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
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_pr_daily_unique 
                    ON pr_metrics_daily(metric_date, organization, repository, team_slug, metric_name)
                """))
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_pr_daily_date 
                    ON pr_metrics_daily(metric_date)
                """))
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_pr_daily_team 
                    ON pr_metrics_daily(team_slug)
                """))
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_pr_daily_repo 
                    ON pr_metrics_daily(repository)
                """))
                connection.commit()
            except SQLAlchemyError as e:
                print(f"Error creating indexes: {str(e)}")
                connection.rollback()
    
    def upsert_daily_metrics(self, daily_records):
        """Insert or update daily PR metrics in key-value format"""
        processed_count = 0
        
        with self.engine.connect() as connection:
            for record in daily_records:
                try:
                    connection.execute(
                        text("""
                            INSERT INTO pr_metrics_daily (
                                metric_date, organization, repository, team_slug, metric_name, metric_value
                            ) VALUES (
                                :metric_date, :organization, :repository, :team_slug, :metric_name, :metric_value
                            )
                            ON CONFLICT (metric_date, organization, repository, team_slug, metric_name) 
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


class GitHubPRExtractor:
    """Extract GitHub PR metrics from the API"""
    
    def __init__(self, organization, repositories, token=None, teams=None, db_engine=None):
        self.organization = organization
        # Support comma-separated list of repositories
        if isinstance(repositories, str):
            self.repositories = [r.strip() for r in repositories.split(',') if r.strip()]
        else:
            self.repositories = repositories if repositories else []
        
        self.token = token or os.getenv('GITHUB_TOKEN')
        self.teams = teams.split(',') if teams else []
        
        self.base_url = "https://api.github.com"
        self.api_version = "2022-11-28"
        self.headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "X-GitHub-Api-Version": self.api_version
        }
        
        self.db = PRMetricsDatabase(engine=db_engine) if db_engine else None
        
        # Cache for team membership
        self._team_members = {}
        self._user_to_teams = {}  # user -> [team1, team2] (can be in multiple teams)
        self._team_sizes = {}  # team -> size (for metrics)
    
    def _api_request(self, endpoint, params=None):
        """Make a request to the GitHub API"""
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code != 200:
            print(f"API Error {response.status_code}: {endpoint}")
            return None
        
        return response.json()
    
    def _api_request_paginated(self, endpoint, params=None, max_pages=10):
        """Make paginated requests to GitHub API"""
        if params is None:
            params = {}
        
        params['per_page'] = 100
        all_results = []
        page = 1
        
        while page <= max_pages:
            params['page'] = page
            results = self._api_request(endpoint, params)
            
            if not results or len(results) == 0:
                break
            
            all_results.extend(results)
            
            if len(results) < 100:
                break
            
            page += 1
        
        return all_results
    
    def get_teams(self):
        """Fetch teams from the organization or use provided list"""
        if self.teams:
            return [t.strip() for t in self.teams]
        
        teams_data = self._api_request(f"/orgs/{self.organization}/teams?per_page=100")
        if not teams_data or isinstance(teams_data, dict) and 'message' in teams_data:
            print(f"Error fetching teams")
            return []
        
        return [team['slug'] for team in teams_data]
    
    def get_team_members(self, team_slug):
        """Get members of a team (cached)"""
        if team_slug in self._team_members:
            return self._team_members[team_slug]
        
        members = self._api_request(f"/orgs/{self.organization}/teams/{team_slug}/members?per_page=100")
        if members and 'message' not in members:
            member_logins = [m['login'].lower() for m in members]
            self._team_members[team_slug] = member_logins
            
            # Build reverse mapping - user can be in multiple teams
            for login in member_logins:
                if login not in self._user_to_teams:
                    self._user_to_teams[login] = []
                if team_slug not in self._user_to_teams[login]:
                    self._user_to_teams[login].append(team_slug)
            
            return member_logins
        
        return []
    
    def build_user_team_mapping(self, teams):
        """Build mapping of GitHub usernames to teams (user can be in multiple teams)"""
        print(f"------>Building user-to-team(s) mapping for {len(teams)} teams...")
        
        for team_slug in teams:
            members = self.get_team_members(team_slug)
            team_size = len(members)
            self._team_sizes[team_slug] = team_size  # Store team size
            print(f"        {team_slug}: {team_size} members")
        
        print(f"------>Total users mapped: {len(self._user_to_teams)}")
        
        # Show users in multiple teams
        multi_team_users = {u: teams for u, teams in self._user_to_teams.items() if len(teams) > 1}
        if multi_team_users:
            print(f"------>Users in multiple teams: {len(multi_team_users)}")
        
        return self._user_to_teams
    
    def get_user_teams(self, username):
        """Get all teams for a username (returns list)"""
        return self._user_to_teams.get(username.lower(), [])
    
    def fetch_merged_prs_from_repos(self, since_date, until_date):
        """Fetch all merged PRs from all configured repositories"""
        print(f"------>Fetching merged PRs from {len(self.repositories)} repos ({since_date} to {until_date})...")
        
        since_dt = datetime.strptime(since_date, '%Y-%m-%d').date()
        until_dt = datetime.strptime(until_date, '%Y-%m-%d').date()
        
        all_prs = []  # List of (repo_name, pr_data) tuples
        
        for repo in self.repositories:
            print(f"        Fetching from {repo}...")
            repo_prs = []
            page = 1
            max_pages = 20
            
            while page <= max_pages:
                params = {
                    'state': 'closed',
                    'sort': 'created',  # Sort by creation date for consistent pagination
                    'direction': 'desc',
                    'per_page': 100,
                    'page': page
                }
                
                prs = self._api_request(
                    f"/repos/{self.organization}/{repo}/pulls",
                    params=params
                )
                
                if not prs or len(prs) == 0:
                    break
                
                # Filter for merged PRs in our date range
                # Don't break early - PRs can be created long before they're merged
                for pr in prs:
                    if not pr.get('merged_at'):
                        continue
                    
                    merged_at = datetime.fromisoformat(pr['merged_at'].replace('Z', '+00:00'))
                    merged_date = merged_at.date()
                    
                    # Only include PRs merged within our date range
                    if since_dt <= merged_date <= until_dt:
                        repo_prs.append(pr)
                
                # Continue pagination if we got a full page
                if len(prs) < 100:
                    break
                
                page += 1
            
            print(f"          Found {len(repo_prs)} merged PRs")
            # Store as (repo, pr) tuples
            all_prs.extend([(repo, pr) for pr in repo_prs])
        
        print(f"------>Total merged PRs across all repos: {len(all_prs)}")
        return all_prs
    
    def fetch_pr_reviews(self, repo, pr_number):
        """Fetch reviews for a specific PR"""
        reviews = self._api_request(
            f"/repos/{self.organization}/{repo}/pulls/{pr_number}/reviews"
        )
        return reviews or []
    
    def calculate_pr_metrics(self, pr, reviews):
        """Calculate metrics for a single PR"""
        metrics = {}
        
        # Parse timestamps
        created_at = datetime.fromisoformat(pr['created_at'].replace('Z', '+00:00'))
        merged_at = datetime.fromisoformat(pr['merged_at'].replace('Z', '+00:00'))
        
        # PR Cycle Time (hours)
        cycle_time = (merged_at - created_at).total_seconds() / 3600
        metrics['cycle_time_hours'] = round(cycle_time, 2)
        
        # Time to First Review (hours)
        first_review_time = None
        if reviews:
            # Sort reviews by submitted_at
            sorted_reviews = sorted(
                [r for r in reviews if r.get('submitted_at')],
                key=lambda r: r['submitted_at']
            )
            
            if sorted_reviews:
                first_review_at = datetime.fromisoformat(
                    sorted_reviews[0]['submitted_at'].replace('Z', '+00:00')
                )
                first_review_time = (first_review_at - created_at).total_seconds() / 3600
                metrics['time_to_first_review_hours'] = round(first_review_time, 2)
        
        # PR author
        metrics['author'] = pr['user']['login'].lower()
        metrics['merged_date'] = merged_at.date().isoformat()
        metrics['pr_number'] = pr['number']
        
        return metrics
    
    def aggregate_team_metrics(self, pr_metrics_list):
        """Aggregate PR metrics by team and date
        
        PRs from users in multiple teams are counted for each team.
        Stores both aggregated (all repos) and per-repo breakdowns.
        """
        # Structure: {date: {team: {repo: {metric_name: [values]}}}}
        daily_team_repo_metrics = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
        
        for pm in pr_metrics_list:
            author = pm['author']
            teams = self.get_user_teams(author)
            date = pm['merged_date']
            repo = pm['repository']
            
            # Skip if user not in any tracked team
            if not teams:
                continue
            
            # Attribute PR to all teams the user belongs to
            for team in teams:
                # Store per-repo metrics
                daily_team_repo_metrics[date][team][repo]['cycle_time_hours'].append(pm['cycle_time_hours'])
                daily_team_repo_metrics[date][team][repo]['merged_count'].append(1)
                
                if 'time_to_first_review_hours' in pm:
                    daily_team_repo_metrics[date][team][repo]['time_to_first_review_hours'].append(
                        pm['time_to_first_review_hours']
                    )
                
                # Also store in 'all' bucket for aggregated metrics
                daily_team_repo_metrics[date][team]['all']['cycle_time_hours'].append(pm['cycle_time_hours'])
                daily_team_repo_metrics[date][team]['all']['merged_count'].append(1)
                
                if 'time_to_first_review_hours' in pm:
                    daily_team_repo_metrics[date][team]['all']['time_to_first_review_hours'].append(
                        pm['time_to_first_review_hours']
                    )
        
        # Convert to database records
        records = []
        
        for date, team_data in daily_team_repo_metrics.items():
            for team, repo_data in team_data.items():
                for repo, metrics in repo_data.items():
                    # Average PR Cycle Time
                    if metrics['cycle_time_hours']:
                        avg_cycle = sum(metrics['cycle_time_hours']) / len(metrics['cycle_time_hours'])
                        records.append({
                            'metric_date': date,
                            'organization': self.organization,
                            'repository': repo,
                            'team_slug': team,
                            'metric_name': 'avg_pr_cycle_time_hours',
                            'metric_value': round(avg_cycle, 2)
                        })
                    
                    # Merged PR Count
                    merged_count = len(metrics['merged_count'])
                    records.append({
                        'metric_date': date,
                        'organization': self.organization,
                        'repository': repo,
                        'team_slug': team,
                        'metric_name': 'merged_prs_count',
                        'metric_value': merged_count
                    })
                    
                    # Average Time to First Review
                    if metrics['time_to_first_review_hours']:
                        avg_review_time = sum(metrics['time_to_first_review_hours']) / len(metrics['time_to_first_review_hours'])
                        records.append({
                            'metric_date': date,
                            'organization': self.organization,
                            'repository': repo,
                            'team_slug': team,
                            'metric_name': 'avg_time_to_first_review_hours',
                            'metric_value': round(avg_review_time, 2)
                        })
                    
                    # Team Size (store once per team per day for 'all' repository only)
                    if repo == 'all':
                        team_size = self._team_sizes.get(team, 0)
                        if team_size > 0:
                            records.append({
                                'metric_date': date,
                                'organization': self.organization,
                                'repository': repo,
                                'team_slug': team,
                                'metric_name': 'team_size',
                                'metric_value': team_size
                            })
        
        return records
    
    def fetch_all_metrics(self, since_date=None, until_date=None):
        """Fetch PR metrics for all configured teams and store in database
        
        New efficient approach:
        1. Build user→teams mapping (handles multi-team users)
        2. Fetch ALL PRs from configured repos (minimal API calls)
        3. Loop through PRs once and attribute to teams
        """
        if not since_date:
            since_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not until_date:
            until_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"====== GitHub PR Metrics Extraction ======")
        print(f"------>Organization: {self.organization}")
        print(f"------>Repositories: {', '.join(self.repositories)}")
        print(f"------>Date range: {since_date} to {until_date}")
        
        if not self.repositories:
            print("------>ERROR: No repositories configured!")
            return 0
        
        # Step 1: Get teams and build user→teams mapping
        teams = self.get_teams()
        if not teams:
            print("------>No teams found or configured")
            return 0
        
        print(f"------>Processing {len(teams)} teams")
        self.build_user_team_mapping(teams)
        
        # Step 2: Fetch all merged PRs from all repos (efficient!)
        prs_with_repos = self.fetch_merged_prs_from_repos(since_date, until_date)
        
        if not prs_with_repos:
            print("------>No merged PRs found in date range")
            return 0
        
        # Step 3: Calculate metrics for each PR
        print(f"------>Calculating metrics for {len(prs_with_repos)} PRs...")
        pr_metrics_list = []
        
        for i, (repo, pr) in enumerate(prs_with_repos):
            if (i + 1) % 20 == 0:
                print(f"        Processing PR {i + 1}/{len(prs_with_repos)}...")
            
            # Fetch reviews for this PR
            reviews = self.fetch_pr_reviews(repo, pr['number'])
            
            # Calculate metrics
            metrics = self.calculate_pr_metrics(pr, reviews)
            metrics['repository'] = repo  # Add repo to metrics
            pr_metrics_list.append(metrics)
        
        # Step 4: Aggregate by team and date (PRs attributed to all teams user belongs to)
        print(f"------>Aggregating metrics by team (across all repos)...")
        daily_records = self.aggregate_team_metrics(pr_metrics_list)
        
        # Store in database
        if daily_records and self.db:
            processed = self.db.upsert_daily_metrics(daily_records)
            print(f"------>Stored {processed} daily metric records")
            
            # Show summary
            aggregated = [r for r in daily_records if r['repository'] == 'all']
            teams_with_data = set(r['team_slug'] for r in aggregated)
            print(f"------>Teams with PR activity: {len(teams_with_data)}")
            
            return processed
        
        return 0
    
    def sync_to_database(self, db_connection=None, initial_sync=False):
        """Fetch metrics and store in database
        
        Args:
            db_connection: Database connection object
            initial_sync: If True, fetch last 90 days (3 months)
        """
        if db_connection:
            self.db = PRMetricsDatabase(engine=db_connection.engine)
        elif not self.db:
            self.db = PRMetricsDatabase()
        
        # Determine date range based on initial_sync flag or last sync time
        if initial_sync:
            since_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            until_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            print(f"------>Initial sync: fetching PR metrics from last 90 days")
        else:
            # Smart sync: only fetch PRs since last sync (with 7-day overlap for safety)
            # Get the last date we have data for
            with self.db.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT MAX(metric_date) 
                    FROM pr_metrics_daily
                    WHERE organization = :org
                """), {'org': self.organization})
                last_date = result.scalar()
            
            if last_date:
                # Fetch from N days before last sync to catch any late-merged PRs
                since_date = (last_date - timedelta(days=DELTA_SYNC_OVERLAP_DAYS)).strftime('%Y-%m-%d')
                until_date = datetime.now().strftime('%Y-%m-%d')
                print(f"------>Delta sync: fetching PRs from {since_date} ({DELTA_SYNC_OVERLAP_DAYS}-day overlap)")
            else:
                # No previous data, fetch last 30 days
                since_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
                until_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                print(f"------>No previous data: fetching last 30 days")
        
        processed = self.fetch_all_metrics(since_date=since_date, until_date=until_date)
        
        if processed > 0:
            print(f"------>Processed {processed} PR daily metrics records")
            return processed
        else:
            print("------>No PR metrics to process")
            return 0
    
    def print_report(self, since_date=None, until_date=None):
        """Print a summary report of PR metrics"""
        if not self.db:
            print("No database connection for report")
            return
        
        if not since_date:
            since_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not until_date:
            until_date = datetime.now().strftime('%Y-%m-%d')
        
        print()
        print("=" * 80)
        print(f"📊 PR METRICS REPORT - {self.organization}")
        print(f"   Repositories: {', '.join(self.repositories)}")
        print(f"   Date Range: {since_date} to {until_date}")
        print("=" * 80)
        
        with self.db.engine.connect() as connection:
            # Get aggregated metrics per team (repository='all')
            result = connection.execute(text("""
                SELECT 
                    team_slug,
                    metric_name,
                    AVG(metric_value) as avg_value,
                    SUM(CASE WHEN metric_name = 'merged_prs_count' THEN metric_value ELSE 0 END) as total
                FROM pr_metrics_daily
                WHERE organization = :org
                    AND repository = 'all'
                    AND metric_date BETWEEN :since AND :until
                GROUP BY team_slug, metric_name
                ORDER BY total DESC, team_slug
            """), {
                'org': self.organization,
                'since': since_date,
                'until': until_date
            })
            
            rows = result.fetchall()
            
            # Organize by team
            team_metrics = defaultdict(dict)
            for row in rows:
                team_slug, metric_name, avg_value, total = row
                if metric_name == 'merged_prs_count':
                    team_metrics[team_slug][metric_name] = int(total) if total else 0
                else:
                    team_metrics[team_slug][metric_name] = round(float(avg_value), 1) if avg_value else 0
            
            # Print aggregated table
            print(f"\n{'Team':<30} {'Merged PRs':>12} {'Avg Cycle (hrs)':>16} {'Avg 1st Review (hrs)':>20}")
            print("-" * 85)
            
            for team, metrics in sorted(team_metrics.items(), key=lambda x: x[1].get('merged_prs_count', 0), reverse=True):
                merged = metrics.get('merged_prs_count', 0)
                cycle = metrics.get('avg_pr_cycle_time_hours', 0)
                review = metrics.get('avg_time_to_first_review_hours', 0)
                
                print(f"{team:<30} {merged:>12} {cycle:>16.1f} {review:>20.1f}")
            
            print()
            print("Note: Metrics aggregated across all repositories. Use repository='<repo>' to see per-repo breakdown.")
            print()


def main():
    """Main function for standalone execution"""
    parser = argparse.ArgumentParser(description='GitHub PR Metrics by Team')
    parser.add_argument('--org', required=True, help='GitHub organization name')
    parser.add_argument('--repos', required=True, help='Comma-separated list of repository names (e.g., "pathlock-plc,pathlock-azure-platform")')
    parser.add_argument('--teams', help='Comma-separated list of team slugs (leave empty to fetch all teams)')
    parser.add_argument('--since', help='Start date (YYYY-MM-DD), default: 30 days ago')
    parser.add_argument('--until', help='End date (YYYY-MM-DD), default: yesterday')
    parser.add_argument('--no-db', action='store_true', help='Skip database storage')
    parser.add_argument('--report', action='store_true', help='Print report after sync')
    
    args = parser.parse_args()
    
    # Validate token
    token = os.getenv('GITHUB_TOKEN')
    if not token:
        print("Error: GITHUB_TOKEN environment variable is not set")
        return 1
    
    # Initialize database if needed
    db_engine = None
    if not args.no_db:
        db = PRMetricsDatabase()
        db_engine = db.engine
    
    # Initialize extractor with multiple repositories
    extractor = GitHubPRExtractor(
        organization=args.org,
        repositories=args.repos,  # Now accepts comma-separated list
        token=token,
        teams=args.teams,
        db_engine=db_engine
    )
    
    # Set dates
    since = args.since or (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    until = args.until or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Fetch metrics
    processed = extractor.fetch_all_metrics(since_date=since, until_date=until)
    
    if processed > 0:
        print(f"\n✓ Stored {processed} daily metric records in database")
    
    # Print report if requested
    if args.report and not args.no_db:
        extractor.print_report(since_date=since, until_date=until)
    
    return 0


if __name__ == "__main__":
    exit(main())

