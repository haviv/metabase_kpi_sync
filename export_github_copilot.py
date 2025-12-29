#!/usr/bin/env python3
"""
GitHub Copilot Usage Metrics by Team - Comprehensive Analysis
============================================================================
Fetches Copilot usage statistics for all teams in an organization
and stores daily metrics in the database.

Can be run standalone or integrated with export_ado.py

Usage (standalone):
    python export_github_copilot.py --org Pathlock --teams "team1,team2" --team-sizes "7,12"
"""

import requests
import os
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, text, Table, Column, Integer, String, DateTime, MetaData, Float, Date, Numeric
from sqlalchemy.dialects.postgresql import TEXT as PG_TEXT
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)


class CopilotMetricsDatabase:
    """Database connection and operations for Copilot metrics"""
    
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
        """Create copilot_metrics_daily table with flexible key-value format"""
        # New daily metrics table - one row per metric per day per team
        self.copilot_metrics_daily = Table(
            'copilot_metrics_daily', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('metric_date', Date, nullable=False),
            Column('organization', String(200), nullable=False),
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
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_copilot_daily_unique 
                    ON copilot_metrics_daily(metric_date, organization, team_slug, metric_name)
                """))
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_copilot_daily_date 
                    ON copilot_metrics_daily(metric_date)
                """))
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_copilot_daily_team 
                    ON copilot_metrics_daily(team_slug)
                """))
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_copilot_daily_metric 
                    ON copilot_metrics_daily(metric_name)
                """))
                connection.commit()
            except SQLAlchemyError as e:
                print(f"Error creating indexes: {str(e)}")
                connection.rollback()
    
    def upsert_daily_metrics(self, daily_records):
        """Insert or update daily copilot metrics in key-value format
        
        Args:
            daily_records: List of dicts with keys: metric_date, organization, team_slug, metric_name, metric_value
        """
        processed_count = 0
        
        with self.engine.connect() as connection:
            for record in daily_records:
                try:
                    # Upsert using ON CONFLICT
                    connection.execute(
                        text("""
                            INSERT INTO copilot_metrics_daily (
                                metric_date, organization, team_slug, metric_name, metric_value
                            ) VALUES (
                                :metric_date, :organization, :team_slug, :metric_name, :metric_value
                            )
                            ON CONFLICT (metric_date, organization, team_slug, metric_name) 
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


class GitHubCopilotExtractor:
    """Extract GitHub Copilot metrics from the API"""
    
    def __init__(self, organization, token=None, teams=None, team_sizes=None, db_engine=None):
        self.organization = organization
        self.token = token or os.getenv('GITHUB_TOKEN')
        self.teams = teams.split(',') if teams else []
        self.team_sizes = {}
        
        if team_sizes and teams:
            sizes = team_sizes.split(',')
            for i, team in enumerate(self.teams):
                if i < len(sizes):
                    try:
                        self.team_sizes[team.strip()] = int(sizes[i].strip())
                    except ValueError:
                        pass
        
        self.base_url = "https://api.github.com"
        self.api_version = "2022-11-28"
        self.headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "X-GitHub-Api-Version": self.api_version
        }
        
        self.db = CopilotMetricsDatabase(engine=db_engine) if db_engine else None
    
    def _api_request(self, endpoint):
        """Make a request to the GitHub API"""
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code != 200:
            return None
        
        return response.json()
    
    def _safe_div(self, num, den, default=0):
        """Safe division with default value"""
        if den and den > 0:
            return num / den
        return default
    
    def get_teams(self):
        """Fetch teams from the organization or use provided list"""
        if self.teams:
            return [t.strip() for t in self.teams]
        
        teams_data = self._api_request(f"/orgs/{self.organization}/teams?per_page=100")
        if not teams_data or 'message' in teams_data:
            print(f"Error fetching teams: {teams_data.get('message', 'Unknown error') if teams_data else 'No response'}")
            return []
        
        return [team['slug'] for team in teams_data]
    
    def get_team_size(self, team_slug):
        """Get team size from manual input or API"""
        if team_slug in self.team_sizes:
            return self.team_sizes[team_slug]
        
        members = self._api_request(f"/orgs/{self.organization}/teams/{team_slug}/members?per_page=100")
        if members and 'message' not in members:
            return len(members)
        
        return 0
    
    def fetch_team_metrics(self, team_slug, since_date, until_date):
        """Fetch Copilot metrics for a specific team"""
        endpoint = (
            f"/orgs/{self.organization}/team/{team_slug}/copilot/metrics"
            f"?since={since_date}T00:00:00Z&until={until_date}T23:59:59Z&per_page=100"
        )
        
        return self._api_request(endpoint)
    
    def extract_day_metrics(self, day_data):
        """Extract all metrics from a single day's API response
        
        Returns a dict with metric_name -> metric_value
        """
        metrics = {}
        
        # Top-level user counts (directly from API response)
        metrics['total_active_users'] = day_data.get('total_active_users', 0) or 0
        metrics['total_engaged_users'] = day_data.get('total_engaged_users', 0) or 0
        
        # IDE Code Completions
        code_completions = day_data.get('copilot_ide_code_completions') or {}
        metrics['code_completions_engaged_users'] = code_completions.get('total_engaged_users', 0) or 0
        
        # Sum nested code completion metrics (across all editors/models/languages)
        suggestions = 0
        acceptances = 0
        lines_suggested = 0
        lines_accepted = 0
        
        for editor in code_completions.get('editors', []):
            for model in editor.get('models', []):
                for lang in model.get('languages', []):
                    suggestions += lang.get('total_code_suggestions', 0) or 0
                    acceptances += lang.get('total_code_acceptances', 0) or 0
                    lines_suggested += lang.get('total_code_lines_suggested', 0) or 0
                    lines_accepted += lang.get('total_code_lines_accepted', 0) or 0
        
        metrics['code_suggestions'] = suggestions
        metrics['code_acceptances'] = acceptances
        metrics['code_lines_suggested'] = lines_suggested
        metrics['code_lines_accepted'] = lines_accepted
        
        # IDE Chat
        ide_chat = day_data.get('copilot_ide_chat') or {}
        metrics['ide_chat_engaged_users'] = ide_chat.get('total_engaged_users', 0) or 0
        
        ide_chats = 0
        chat_insertions = 0
        chat_copies = 0
        
        for editor in ide_chat.get('editors', []):
            for model in editor.get('models', []):
                ide_chats += model.get('total_chats', 0) or 0
                chat_insertions += model.get('total_chat_insertion_events', 0) or 0
                chat_copies += model.get('total_chat_copy_events', 0) or 0
        
        metrics['ide_chats'] = ide_chats
        metrics['ide_chat_insertions'] = chat_insertions
        metrics['ide_chat_copies'] = chat_copies
        
        # Dotcom Chat
        dotcom_chat = day_data.get('copilot_dotcom_chat') or {}
        metrics['dotcom_chat_engaged_users'] = dotcom_chat.get('total_engaged_users', 0) or 0
        
        web_chats = 0
        for model in dotcom_chat.get('models', []):
            web_chats += model.get('total_chats', 0) or 0
        metrics['dotcom_chats'] = web_chats
        
        # PR Summaries
        pr_data = day_data.get('copilot_dotcom_pull_requests') or {}
        metrics['pr_engaged_users'] = pr_data.get('total_engaged_users', 0) or 0
        
        pr_summaries = 0
        for repo in pr_data.get('repositories', []):
            for model in repo.get('models', []):
                pr_summaries += model.get('total_pr_summaries_created', 0) or 0
        metrics['pr_summaries'] = pr_summaries
        
        return metrics
    
    def process_daily_metrics(self, raw_data, team_slug, team_size=0):
        """Process raw API data into daily metric records for database storage
        
        Returns list of records in format: {metric_date, organization, team_slug, metric_name, metric_value}
        """
        if not raw_data:
            return []
        
        records = []
        
        for day_data in raw_data:
            metric_date = day_data.get('date')
            if not metric_date:
                continue
            
            day_metrics = self.extract_day_metrics(day_data)
            
            # Add team_size as a metric (for calculating percentages in Metabase)
            day_metrics['team_size'] = team_size
            
            # Calculate derived metrics per day
            if team_size > 0:
                day_metrics['team_active_pct'] = round((day_metrics['total_active_users'] / team_size) * 100, 1)
                day_metrics['team_engaged_pct'] = round((day_metrics['total_engaged_users'] / team_size) * 100, 1)
            else:
                day_metrics['team_active_pct'] = 0
                day_metrics['team_engaged_pct'] = 0
            
            # Acceptance rate
            if day_metrics['code_suggestions'] > 0:
                day_metrics['acceptance_rate_pct'] = round((day_metrics['code_acceptances'] / day_metrics['code_suggestions']) * 100, 1)
            else:
                day_metrics['acceptance_rate_pct'] = 0
            
            # Chat to code ratio
            total_chats = day_metrics['ide_chats'] + day_metrics['dotcom_chats']
            chat_to_code = day_metrics['ide_chat_insertions'] + day_metrics['ide_chat_copies']
            if total_chats > 0:
                day_metrics['chat_to_code_pct'] = round((chat_to_code / total_chats) * 100, 1)
            else:
                day_metrics['chat_to_code_pct'] = 0
            
            for metric_name, metric_value in day_metrics.items():
                records.append({
                    'metric_date': metric_date,
                    'organization': self.organization,
                    'team_slug': team_slug,
                    'metric_name': metric_name,
                    'metric_value': metric_value
                })
        
        return records
    
    def calculate_period_summary(self, raw_data, team_slug, team_size):
        """Calculate summary metrics for display/reporting (averages over period)
        
        This is used for console output and reporting, not for database storage.
        """
        if not raw_data:
            return None
        
        days = len(raw_data)
        if days == 0:
            return None
        
        # Sum all daily values
        totals = {
            'total_active_users': 0,
            'total_engaged_users': 0,
            'code_completions_engaged_users': 0,
            'ide_chat_engaged_users': 0,
            'dotcom_chat_engaged_users': 0,
            'pr_engaged_users': 0,
            'code_suggestions': 0,
            'code_acceptances': 0,
            'code_lines_suggested': 0,
            'code_lines_accepted': 0,
            'ide_chats': 0,
            'ide_chat_insertions': 0,
            'ide_chat_copies': 0,
            'dotcom_chats': 0,
            'pr_summaries': 0,
        }
        
        for day_data in raw_data:
            day_metrics = self.extract_day_metrics(day_data)
            for key in totals:
                totals[key] += day_metrics.get(key, 0)
        
        # Calculate averages
        avg_active = totals['total_active_users'] / days
        avg_engaged = totals['total_engaged_users'] / days
        avg_code_users = totals['code_completions_engaged_users'] / days
        avg_chat_users = totals['ide_chat_engaged_users'] / days
        
        # Team-based percentages (avg usage vs team size)
        team_active_pct = self._safe_div(avg_active * 100, team_size) if team_size > 0 else 0
        team_engaged_pct = self._safe_div(avg_engaged * 100, team_size) if team_size > 0 else 0
        
        # Acceptance rate (total acceptances / total suggestions)
        acceptance_rate = self._safe_div(totals['code_acceptances'] * 100, totals['code_suggestions'])
        
        # Chat to code ratio
        total_chats = totals['ide_chats'] + totals['dotcom_chats']
        chat_code = totals['ide_chat_insertions'] + totals['ide_chat_copies']
        chat_to_code_pct = self._safe_div(chat_code * 100, total_chats)
        
        return {
            'team_slug': team_slug,
            'team_size': team_size,
            'days_in_period': days,
            'avg_active_per_day': round(avg_active, 1),
            'avg_engaged_per_day': round(avg_engaged, 1),
            'team_active_pct': round(team_active_pct, 1),
            'team_engaged_pct': round(team_engaged_pct, 1),
            'acceptance_rate_pct': round(acceptance_rate, 1),
            'chat_to_code_pct': round(chat_to_code_pct, 0),
            'total_suggestions': totals['code_suggestions'],
            'total_acceptances': totals['code_acceptances'],
            'total_lines_accepted': totals['code_lines_accepted'],
            'total_chats': total_chats,
        }
    
    def fetch_all_metrics(self, since_date=None, until_date=None):
        """Fetch metrics for all configured teams and store in database
        
        Returns the number of records processed
        """
        if not since_date:
            since_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not until_date:
            until_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"------>Fetching GitHub Copilot metrics for org: {self.organization}")
        print(f"------>Date range: {since_date} to {until_date}")
        
        teams = self.get_teams()
        if not teams:
            print("------>No teams found or configured")
            return 0
        
        print(f"------>Processing {len(teams)} teams")
        
        total_records = 0
        all_summaries = []
        
        for team_slug in teams:
            print(f"------>Analyzing: {team_slug}...")
            
            team_size = self.get_team_size(team_slug)
            print(f"        Team size: {team_size}")
            
            raw_data = self.fetch_team_metrics(team_slug, since_date, until_date)
            
            if raw_data is None:
                print(f"        Skipped: API error or no access")
                continue
            
            if isinstance(raw_data, dict) and 'message' in raw_data:
                print(f"        Skipped: {raw_data['message']}")
                continue
            
            if raw_data == [] or not raw_data:
                print(f"        No data (< 5 active users required)")
                continue
            
            # Process daily metrics for database storage
            daily_records = self.process_daily_metrics(raw_data, team_slug, team_size)
            
            if daily_records and self.db:
                processed = self.db.upsert_daily_metrics(daily_records)
                total_records += processed
                print(f"        ✓ Stored {processed} daily metric records ({len(raw_data)} days)")
            
            # Calculate summary for display
            summary = self.calculate_period_summary(raw_data, team_slug, team_size)
            if summary:
                all_summaries.append(summary)
                print(f"        Avg Engaged: {summary['avg_engaged_per_day']}/day, Accept Rate: {summary['acceptance_rate_pct']}%")
        
        # Store summaries for reporting
        self._summaries = all_summaries
        
        return total_records
    
    def sync_to_database(self, db_connection=None):
        """Fetch metrics and store in database"""
        if db_connection:
            self.db = CopilotMetricsDatabase(engine=db_connection.engine)
        elif not self.db:
            self.db = CopilotMetricsDatabase()
        
        # fetch_all_metrics now handles storage internally and returns record count
        processed = self.fetch_all_metrics()
        
        if processed > 0:
            print(f"------>Processed {processed} Copilot daily metrics records")
            return processed
        else:
            print("------>No Copilot metrics to process")
            return 0
    
    def print_report(self, summaries=None):
        """Print a formatted report of the metrics"""
        # Use stored summaries if not provided
        if summaries is None:
            summaries = getattr(self, '_summaries', [])
        
        if not summaries:
            print("No metrics to display")
            return
        
        # Colors for terminal
        RED = '\033[0;31m'
        GREEN = '\033[0;32m'
        YELLOW = '\033[1;33m'
        BLUE = '\033[0;34m'
        CYAN = '\033[0;36m'
        BOLD = '\033[1m'
        NC = '\033[0m'  # No Color
        
        print()
        print(f"{BOLD}{CYAN}╔════════════════════════════════════════════════════════════════════════════════╗{NC}")
        print(f"{BOLD}{CYAN}║                    🤖 GITHUB COPILOT USAGE REPORT                              ║{NC}")
        print(f"{BOLD}{CYAN}║                    Organization: {self.organization:<20}                      ║{NC}")
        print(f"{BOLD}{CYAN}╚════════════════════════════════════════════════════════════════════════════════╝{NC}")
        print()
        
        # Summary table
        print(f"{BOLD}{'Team':<25} {'Size':>6} {'Days':>5} {'Avg Active':>10} {'Avg Engaged':>11} {'Engaged %':>10} {'Accept %':>10}{NC}")
        print("-" * 85)
        
        for m in summaries:
            # Color code engagement
            eng_pct = m.get('team_engaged_pct', 0)
            if eng_pct >= 70:
                eng_color = GREEN
            elif eng_pct >= 40:
                eng_color = YELLOW
            else:
                eng_color = RED
            
            # Color code acceptance rate
            acc_pct = m.get('acceptance_rate_pct', 0)
            if acc_pct >= 35:
                acc_color = GREEN
            elif acc_pct >= 25:
                acc_color = YELLOW
            else:
                acc_color = RED
            
            print(f"{m['team_slug']:<25} {m.get('team_size', 0):>6} {m.get('days_in_period', 0):>5} "
                  f"{m.get('avg_active_per_day', 0):>10.1f} {m.get('avg_engaged_per_day', 0):>11.1f} "
                  f"{eng_color}{eng_pct:>9.1f}%{NC} {acc_color}{acc_pct:>9.1f}%{NC}")
        
        print()
        print(f"{BOLD}Totals:{NC}")
        print("-" * 85)
        
        for m in summaries:
            print(f"{m['team_slug']:<25} Suggestions: {m.get('total_suggestions', 0):>8,}  "
                  f"Acceptances: {m.get('total_acceptances', 0):>8,}  "
                  f"Lines Accepted: {m.get('total_lines_accepted', 0):>8,}  "
                  f"Chats: {m.get('total_chats', 0):>6,}")
        
        print()


def main():
    """Main function for standalone execution"""
    parser = argparse.ArgumentParser(description='GitHub Copilot Usage Metrics')
    parser.add_argument('--org', required=True, help='GitHub organization name')
    parser.add_argument('--teams', help='Comma-separated list of team slugs')
    parser.add_argument('--team-sizes', help='Comma-separated list of team sizes (same order as --teams)')
    parser.add_argument('--since', help='Start date (YYYY-MM-DD), default: 30 days ago')
    parser.add_argument('--until', help='End date (YYYY-MM-DD), default: yesterday')
    parser.add_argument('--no-db', action='store_true', help='Skip database storage')
    
    args = parser.parse_args()
    
    # Validate token
    token = os.getenv('GITHUB_TOKEN')
    if not token:
        print("Error: GITHUB_TOKEN environment variable is not set")
        return 1
    
    # Initialize database if needed
    db_engine = None
    if not args.no_db:
        db = CopilotMetricsDatabase()
        db_engine = db.engine
    
    # Initialize extractor
    extractor = GitHubCopilotExtractor(
        organization=args.org,
        token=token,
        teams=args.teams,
        team_sizes=args.team_sizes,
        db_engine=db_engine
    )
    
    # Set dates
    since = args.since or (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    until = args.until or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Fetch metrics (stores to DB internally if db_engine provided)
    processed = extractor.fetch_all_metrics(since_date=since, until_date=until)
    
    # Print report from stored summaries
    extractor.print_report()
    
    if processed > 0:
        print(f"Stored {processed} daily metric records in database")
    
    return 0


if __name__ == "__main__":
    exit(main())

