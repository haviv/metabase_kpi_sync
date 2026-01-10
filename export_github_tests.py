#!/usr/bin/env python3
"""
GitHub Actions Test Results Extractor

Fetches test results from GitHub Actions workflow runs, parses TRX files,
and stores results in PostgreSQL for Metabase reporting.

Configuration (via .env):
    GITHUB_TOKEN: GitHub Personal Access Token with 'actions:read' permission
    GITHUB_TEST_OWNER: Repository owner (default: Pathlock)
    GITHUB_TEST_REPO: Repository name (default: pathlock-plc)
    GITHUB_TEST_BRANCH: Branch to filter runs (default: cloud/dev)
    GITHUB_TEST_WORKFLOWS: Comma-separated workflow names to monitor
    GITHUB_TEST_ARTIFACT_PATTERNS: Comma-separated artifact name patterns
"""

import os
import io
import re
import zipfile
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Table, Column, Integer, BigInteger, String, DateTime, Float, MetaData, inspect
from sqlalchemy.dialects.postgresql import TEXT as PG_TEXT
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# TRX XML namespace
TRX_NAMESPACE = {'trx': 'http://microsoft.com/schemas/VisualStudio/TeamTest/2010'}


class TRXParser:
    """Parser for Visual Studio Test Results XML (TRX) files."""
    
    @staticmethod
    def parse(trx_content: str) -> dict:
        """
        Parse TRX XML content and extract test results.
        
        Args:
            trx_content: String content of the TRX file
            
        Returns:
            Dictionary containing:
            - run_info: Test run metadata
            - summary: Pass/fail counts
            - tests: List of individual test results
        """
        try:
            # Remove BOM if present
            if trx_content.startswith('\ufeff'):
                trx_content = trx_content[1:]
            
            root = ET.fromstring(trx_content)
            
            # Extract run info
            run_info = {
                'id': root.get('id', ''),
                'name': root.get('name', ''),
                'run_user': root.get('runUser', ''),
            }
            
            # Extract times
            times_elem = root.find('trx:Times', TRX_NAMESPACE)
            if times_elem is not None:
                run_info['creation_time'] = TRXParser._parse_datetime(times_elem.get('creation'))
                run_info['start_time'] = TRXParser._parse_datetime(times_elem.get('start'))
                run_info['finish_time'] = TRXParser._parse_datetime(times_elem.get('finish'))
            
            # Extract test settings for computer name
            test_settings = root.find('trx:TestSettings', TRX_NAMESPACE)
            if test_settings is not None:
                deployment = test_settings.find('trx:Deployment', TRX_NAMESPACE)
                if deployment is not None:
                    run_info['deployment_root'] = deployment.get('runDeploymentRoot', '')
            
            # Build a map of test ID -> class name from TestDefinitions
            test_class_map = TRXParser._build_test_class_map(root)
            
            # Extract individual test results
            tests = []
            results_elem = root.find('trx:Results', TRX_NAMESPACE)
            
            if results_elem is not None:
                for result in results_elem.findall('trx:UnitTestResult', TRX_NAMESPACE):
                    test_data = TRXParser._parse_test_result(result)
                    # Look up the class name from test definitions
                    test_id = test_data.get('test_id', '')
                    if test_id and test_id in test_class_map:
                        test_data['test_class'] = test_class_map[test_id]
                    tests.append(test_data)
            
            # Calculate summary
            summary = {
                'total': len(tests),
                'passed': sum(1 for t in tests if t['outcome'] == 'Passed'),
                'failed': sum(1 for t in tests if t['outcome'] == 'Failed'),
                'not_executed': sum(1 for t in tests if t['outcome'] == 'NotExecuted'),
            }
            
            # Calculate total duration
            total_duration_ms = sum(t['duration_ms'] or 0 for t in tests)
            run_info['total_duration_seconds'] = total_duration_ms / 1000
            
            return {
                'run_info': run_info,
                'summary': summary,
                'tests': tests
            }
            
        except ET.ParseError as e:
            print(f"Error parsing TRX XML: {e}")
            return None
    
    @staticmethod
    def _build_test_class_map(root) -> dict:
        """
        Build a map of test ID -> class name from TestDefinitions section.
        
        The TRX structure has:
        <TestDefinitions>
          <UnitTest id="...">
            <TestMethod className="Namespace.ClassName" name="MethodName" />
          </UnitTest>
        </TestDefinitions>
        """
        test_class_map = {}
        
        test_definitions = root.find('trx:TestDefinitions', TRX_NAMESPACE)
        if test_definitions is not None:
            for unit_test in test_definitions.findall('trx:UnitTest', TRX_NAMESPACE):
                test_id = unit_test.get('id', '')
                if not test_id:
                    continue
                
                # Find TestMethod element which contains the className
                test_method = unit_test.find('trx:TestMethod', TRX_NAMESPACE)
                if test_method is not None:
                    class_name = test_method.get('className', '')
                    if class_name:
                        test_class_map[test_id] = class_name
        
        return test_class_map
    
    @staticmethod
    def _parse_test_result(result_elem) -> dict:
        """Parse a single UnitTestResult element."""
        test_data = {
            'execution_id': result_elem.get('executionId', ''),
            'test_id': result_elem.get('testId', ''),
            'test_name': result_elem.get('testName', ''),
            'computer_name': result_elem.get('computerName', ''),
            'outcome': result_elem.get('outcome', ''),
            'start_time': TRXParser._parse_datetime(result_elem.get('startTime')),
            'end_time': TRXParser._parse_datetime(result_elem.get('endTime')),
            'duration_ms': TRXParser._parse_duration(result_elem.get('duration')),
            'error_message': None,
            'stack_trace': None,
            'stdout': None,
            'test_class': None,  # Will be populated from TestDefinitions
        }
        
        # Extract output information
        output_elem = result_elem.find('trx:Output', TRX_NAMESPACE)
        if output_elem is not None:
            # Standard output
            stdout_elem = output_elem.find('trx:StdOut', TRX_NAMESPACE)
            if stdout_elem is not None and stdout_elem.text:
                test_data['stdout'] = stdout_elem.text[:5000]  # Truncate to 5000 chars
            
            # Error info
            error_info = output_elem.find('trx:ErrorInfo', TRX_NAMESPACE)
            if error_info is not None:
                message_elem = error_info.find('trx:Message', TRX_NAMESPACE)
                if message_elem is not None and message_elem.text:
                    test_data['error_message'] = message_elem.text[:2000]  # Truncate
                
                stack_elem = error_info.find('trx:StackTrace', TRX_NAMESPACE)
                if stack_elem is not None and stack_elem.text:
                    test_data['stack_trace'] = stack_elem.text[:5000]  # Truncate
        
        return test_data
    
    @staticmethod
    def _parse_datetime(dt_str: str) -> datetime:
        """Parse datetime string from TRX format."""
        if not dt_str:
            return None
        
        # Try various formats
        formats = [
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
        ]
        
        for fmt in formats:
            try:
                # Handle timezone offset format (+00:00)
                if '+' in dt_str and ':' in dt_str.split('+')[-1]:
                    # Remove colon from timezone offset for parsing
                    dt_str_clean = dt_str[:-3] + dt_str[-2:]
                    return datetime.strptime(dt_str_clean, fmt)
                return datetime.strptime(dt_str, fmt)
            except ValueError:
                continue
        
        # Try without timezone
        try:
            # Strip timezone info and parse
            dt_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', dt_str)
            return datetime.strptime(dt_clean, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            pass
        
        return None
    
    @staticmethod
    def _parse_duration(duration_str: str) -> float:
        """
        Parse duration string from TRX format (HH:MM:SS.FFFFFFF) to milliseconds.
        """
        if not duration_str:
            return None
        
        try:
            parts = duration_str.split(':')
            if len(parts) == 3:
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = float(parts[2])
                return (hours * 3600 + minutes * 60 + seconds) * 1000
        except (ValueError, IndexError):
            pass
        
        return None


class GitHubTestsExtractor:
    """
    Extracts test results from GitHub Actions workflows.
    
    Downloads artifacts containing TRX files, parses them, and stores
    results in the database.
    """
    
    def __init__(self, db_connection, token=None, owner=None, repo=None, branch=None,
                 workflows=None, artifact_patterns=None):
        """
        Initialize the extractor.
        
        Args:
            db_connection: Database connection object with engine attribute
            token: GitHub token (defaults to GITHUB_TOKEN env var)
            owner: Repository owner (defaults to GITHUB_TEST_OWNER or 'Pathlock')
            repo: Repository name (defaults to GITHUB_TEST_REPO or 'pathlock-plc')
            branch: Branch to filter (defaults to GITHUB_TEST_BRANCH or 'cloud/dev')
            workflows: List of workflow names (defaults to GITHUB_TEST_WORKFLOWS)
            artifact_patterns: List of artifact name patterns (defaults to GITHUB_TEST_ARTIFACT_PATTERNS)
        """
        self.db = db_connection
        self.token = token or os.getenv('GITHUB_TOKEN')
        self.owner = owner or os.getenv('GITHUB_TEST_OWNER', 'Pathlock')
        self.repo = repo or os.getenv('GITHUB_TEST_REPO', 'pathlock-plc')
        self.branch = branch or os.getenv('GITHUB_TEST_BRANCH', 'cloud/dev')
        
        # Parse workflows from env or use defaults
        workflows_env = os.getenv('GITHUB_TEST_WORKFLOWS', '')
        if workflows:
            self.workflows = workflows if isinstance(workflows, list) else [w.strip() for w in workflows.split(',')]
        elif workflows_env:
            self.workflows = [w.strip() for w in workflows_env.split(',') if w.strip()]
        else:
            self.workflows = ['Nightly Full Unit Test Run', 'Nightly Full Integration Test Run']
        
        # Parse artifact patterns
        patterns_env = os.getenv('GITHUB_TEST_ARTIFACT_PATTERNS', '')
        if artifact_patterns:
            self.artifact_patterns = artifact_patterns if isinstance(artifact_patterns, list) else [p.strip() for p in artifact_patterns.split(',')]
        elif patterns_env:
            self.artifact_patterns = [p.strip() for p in patterns_env.split(',') if p.strip()]
        else:
            self.artifact_patterns = ['test-results', 'nightly-test-results']
        
        self.headers = {
            'Accept': 'application/vnd.github+json',
            'Authorization': f'Bearer {self.token}',
            'X-GitHub-Api-Version': '2022-11-28'
        }
        
        self.base_url = 'https://api.github.com'
    
    def setup_tables(self):
        """Create the database tables if they don't exist."""
        try:
            with self.db.engine.connect() as connection:
                # Create github_test_runs table
                connection.execute(text("""
                    CREATE TABLE IF NOT EXISTS github_test_runs (
                        id SERIAL PRIMARY KEY,
                        run_id BIGINT NOT NULL UNIQUE,
                        workflow_name VARCHAR(500),
                        repository VARCHAR(200),
                        branch VARCHAR(200),
                        commit_sha VARCHAR(40),
                        run_number INTEGER,
                        run_started_at TIMESTAMP,
                        run_completed_at TIMESTAMP,
                        conclusion VARCHAR(50),
                        test_run_id VARCHAR(100),
                        test_run_name VARCHAR(500),
                        computer_name VARCHAR(200),
                        total_tests INTEGER DEFAULT 0,
                        passed_tests INTEGER DEFAULT 0,
                        failed_tests INTEGER DEFAULT 0,
                        skipped_tests INTEGER DEFAULT 0,
                        total_duration_seconds FLOAT DEFAULT 0,
                        artifact_name VARCHAR(200),
                        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                # Create github_test_results table
                connection.execute(text("""
                    CREATE TABLE IF NOT EXISTS github_test_results (
                        id SERIAL PRIMARY KEY,
                        run_id BIGINT NOT NULL,
                        test_id VARCHAR(100),
                        execution_id VARCHAR(100),
                        test_name VARCHAR(1000) NOT NULL,
                        test_class VARCHAR(1000),
                        outcome VARCHAR(50) NOT NULL,
                        duration_ms FLOAT,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        error_message TEXT,
                        stack_trace TEXT,
                        stdout TEXT,
                        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                # Create indexes
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_test_runs_workflow 
                    ON github_test_runs(workflow_name, run_started_at)
                """))
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_test_runs_branch 
                    ON github_test_runs(branch, run_started_at)
                """))
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_test_results_run_id 
                    ON github_test_results(run_id)
                """))
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_test_results_outcome 
                    ON github_test_results(outcome)
                """))
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_test_results_name 
                    ON github_test_results(test_name)
                """))
                
                connection.commit()
                print("------>GitHub test tables created/verified")
                
        except SQLAlchemyError as e:
            print(f"Error setting up GitHub test tables: {e}")
            raise
    
    def get_last_sync_run_id(self) -> int:
        """Get the most recent run_id that was synced."""
        try:
            with self.db.engine.connect() as connection:
                result = connection.execute(text("""
                    SELECT MAX(run_id) FROM github_test_runs
                """)).scalar()
                return result or 0
        except SQLAlchemyError:
            return 0
    
    def is_run_synced(self, run_id: int) -> bool:
        """Check if a workflow run has already been synced."""
        try:
            with self.db.engine.connect() as connection:
                result = connection.execute(
                    text("SELECT 1 FROM github_test_runs WHERE run_id = :run_id"),
                    {"run_id": run_id}
                ).first()
                return result is not None
        except SQLAlchemyError:
            return False
    
    def fetch_workflow_runs(self, since_date: datetime = None, limit: int = 100) -> list:
        """
        Fetch workflow runs from GitHub API.
        
        Fetches runs per workflow to ensure we get all relevant runs,
        with pagination support.
        
        Args:
            since_date: Only fetch runs after this date
            limit: Maximum number of runs to fetch per workflow
            
        Returns:
            List of workflow run dictionaries
        """
        all_runs = []
        
        # First, get workflow IDs for our target workflows
        workflows_url = f'{self.base_url}/repos/{self.owner}/{self.repo}/actions/workflows'
        response = requests.get(workflows_url, headers=self.headers)
        
        if response.status_code != 200:
            print(f"Error fetching workflows: {response.status_code} - {response.text}")
            return []
        
        workflow_map = {}
        for wf in response.json().get('workflows', []):
            if wf['name'] in self.workflows:
                workflow_map[wf['name']] = wf['id']
        
        # Fetch runs for each workflow separately
        for workflow_name, workflow_id in workflow_map.items():
            workflow_runs = []
            page = 1
            
            while len(workflow_runs) < limit:
                params = {
                    'branch': self.branch,
                    'status': 'completed',
                    'per_page': min(limit - len(workflow_runs), 100),
                    'page': page
                }
                
                url = f'{self.base_url}/repos/{self.owner}/{self.repo}/actions/workflows/{workflow_id}/runs'
                response = requests.get(url, headers=self.headers, params=params)
                
                if response.status_code != 200:
                    print(f"Error fetching runs for {workflow_name}: {response.status_code}")
                    break
                
                runs = response.json().get('workflow_runs', [])
                if not runs:
                    break  # No more runs
                
                for run in runs:
                    # Filter by date if specified
                    if since_date:
                        run_date = datetime.fromisoformat(run['created_at'].replace('Z', '+00:00'))
                        if run_date.replace(tzinfo=None) < since_date:
                            # Runs are returned in reverse chronological order
                            # If we hit a run before since_date, stop pagination
                            break
                    
                    workflow_runs.append(run)
                    
                    if len(workflow_runs) >= limit:
                        break
                else:
                    # Continue pagination if we didn't break
                    page += 1
                    continue
                break  # Break outer loop if inner loop broke
            
            all_runs.extend(workflow_runs)
        
        # Sort all runs by date (newest first)
        all_runs.sort(key=lambda r: r['created_at'], reverse=True)
        
        return all_runs
    
    def fetch_artifacts(self, run_id: int, workflow_name: str = None) -> list:
        """
        Fetch artifacts for a specific workflow run.
        
        Prioritizes specific artifact names to avoid duplicates:
        - For Unit Test runs: prefers 'nightly-test-results-all-tests'
        - For Integration Test runs: prefers 'test-results'
        """
        url = f'{self.base_url}/repos/{self.owner}/{self.repo}/actions/runs/{run_id}/artifacts'
        response = requests.get(url, headers=self.headers)
        
        if response.status_code != 200:
            print(f"Error fetching artifacts for run {run_id}: {response.status_code}")
            return []
        
        artifacts = response.json().get('artifacts', [])
        
        # Filter out expired artifacts
        valid_artifacts = [a for a in artifacts if not a['expired']]
        
        # Define preferred artifact names per workflow type
        preferred_artifacts = {
            'unit': ['nightly-test-results-all-tests', 'nightly-test-results'],
            'integration': ['test-results', 'nightly-integration-test-results-summary'],
        }
        
        # Determine workflow type
        workflow_type = None
        if workflow_name:
            workflow_lower = workflow_name.lower()
            if 'integration' in workflow_lower:
                workflow_type = 'integration'
            elif 'unit' in workflow_lower:
                workflow_type = 'unit'
        
        # Try to find preferred artifact first
        if workflow_type and workflow_type in preferred_artifacts:
            for preferred_name in preferred_artifacts[workflow_type]:
                for artifact in valid_artifacts:
                    if artifact['name'].lower() == preferred_name.lower():
                        return [artifact]  # Return only the preferred one
        
        # Fallback: filter by patterns
        matching_artifacts = []
        for artifact in valid_artifacts:
            artifact_name = artifact['name'].lower()
            for pattern in self.artifact_patterns:
                if pattern.lower() in artifact_name:
                    matching_artifacts.append(artifact)
                    break
        
        return matching_artifacts
    
    def download_artifact(self, artifact_id: int) -> bytes:
        """Download an artifact and return its content."""
        url = f'{self.base_url}/repos/{self.owner}/{self.repo}/actions/artifacts/{artifact_id}/zip'
        response = requests.get(url, headers=self.headers, allow_redirects=True)
        
        if response.status_code != 200:
            print(f"Error downloading artifact {artifact_id}: {response.status_code}")
            return None
        
        return response.content
    
    def extract_trx_from_zip(self, zip_content: bytes) -> list:
        """
        Extract TRX files from a ZIP archive.
        
        Returns:
            List of tuples (filename, content)
        """
        trx_files = []
        
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content), 'r') as zf:
                for filename in zf.namelist():
                    if filename.endswith('.trx'):
                        content = zf.read(filename).decode('utf-8')
                        trx_files.append((filename, content))
        except zipfile.BadZipFile as e:
            print(f"Error extracting ZIP: {e}")
        
        return trx_files
    
    def store_test_run(self, run_data: dict, parsed_trx: dict, artifact_name: str) -> bool:
        """Store a test run and its results in the database."""
        try:
            with self.db.engine.connect() as connection:
                run_info = parsed_trx['run_info']
                summary = parsed_trx['summary']
                
                # Get computer name from first test result if available
                computer_name = None
                if parsed_trx['tests']:
                    computer_name = parsed_trx['tests'][0].get('computer_name')
                
                # Insert test run
                connection.execute(text("""
                    INSERT INTO github_test_runs (
                        run_id, workflow_name, repository, branch, commit_sha,
                        run_number, run_started_at, run_completed_at, conclusion,
                        test_run_id, test_run_name, computer_name,
                        total_tests, passed_tests, failed_tests, skipped_tests,
                        total_duration_seconds, artifact_name
                    ) VALUES (
                        :run_id, :workflow_name, :repository, :branch, :commit_sha,
                        :run_number, :run_started_at, :run_completed_at, :conclusion,
                        :test_run_id, :test_run_name, :computer_name,
                        :total_tests, :passed_tests, :failed_tests, :skipped_tests,
                        :total_duration_seconds, :artifact_name
                    )
                    ON CONFLICT (run_id) DO UPDATE SET
                        total_tests = EXCLUDED.total_tests,
                        passed_tests = EXCLUDED.passed_tests,
                        failed_tests = EXCLUDED.failed_tests,
                        skipped_tests = EXCLUDED.skipped_tests,
                        total_duration_seconds = EXCLUDED.total_duration_seconds
                """), {
                    'run_id': run_data['id'],
                    'workflow_name': run_data.get('name', ''),
                    'repository': f"{self.owner}/{self.repo}",
                    'branch': run_data.get('head_branch', self.branch),
                    'commit_sha': run_data.get('head_sha', '')[:40],
                    'run_number': run_data.get('run_number'),
                    'run_started_at': datetime.fromisoformat(run_data['created_at'].replace('Z', '+00:00')).replace(tzinfo=None) if run_data.get('created_at') else None,
                    'run_completed_at': datetime.fromisoformat(run_data['updated_at'].replace('Z', '+00:00')).replace(tzinfo=None) if run_data.get('updated_at') else None,
                    'conclusion': run_data.get('conclusion', ''),
                    'test_run_id': run_info.get('id', ''),
                    'test_run_name': run_info.get('name', '')[:500],
                    'computer_name': computer_name,
                    'total_tests': summary['total'],
                    'passed_tests': summary['passed'],
                    'failed_tests': summary['failed'],
                    'skipped_tests': summary['not_executed'],
                    'total_duration_seconds': run_info.get('total_duration_seconds', 0),
                    'artifact_name': artifact_name
                })
                
                # Delete existing test results for this run (in case of re-sync)
                connection.execute(text("""
                    DELETE FROM github_test_results WHERE run_id = :run_id
                """), {'run_id': run_data['id']})
                
                # Insert individual test results
                for test in parsed_trx['tests']:
                    connection.execute(text("""
                        INSERT INTO github_test_results (
                            run_id, test_id, execution_id, test_name, test_class,
                            outcome, duration_ms, start_time, end_time,
                            error_message, stack_trace, stdout
                        ) VALUES (
                            :run_id, :test_id, :execution_id, :test_name, :test_class,
                            :outcome, :duration_ms, :start_time, :end_time,
                            :error_message, :stack_trace, :stdout
                        )
                    """), {
                        'run_id': run_data['id'],
                        'test_id': test.get('test_id', '')[:100],
                        'execution_id': test.get('execution_id', '')[:100],
                        'test_name': test.get('test_name', '')[:1000],
                        'test_class': test.get('test_class', '')[:1000] if test.get('test_class') else None,
                        'outcome': test.get('outcome', ''),
                        'duration_ms': test.get('duration_ms'),
                        'start_time': test.get('start_time'),
                        'end_time': test.get('end_time'),
                        'error_message': test.get('error_message'),
                        'stack_trace': test.get('stack_trace'),
                        'stdout': test.get('stdout')
                    })
                
                connection.commit()
                return True
                
        except SQLAlchemyError as e:
            print(f"Error storing test run {run_data['id']}: {e}")
            return False
    
    def sync_test_runs(self, days_back: int = None, initial_sync: bool = False) -> int:
        """
        Sync test runs from GitHub Actions.
        
        Args:
            days_back: Number of days to look back (default: 1 for daily, 60 for initial)
            initial_sync: If True, fetch last 2 months of data
            
        Returns:
            Number of runs processed
        """
        # Ensure tables exist
        self.setup_tables()
        
        if not self.token:
            print("------>Skipping GitHub tests sync (GITHUB_TOKEN not configured)")
            return 0
        
        # Determine how far back to sync
        if initial_sync:
            since_date = datetime.now() - timedelta(days=60)  # 2 months
            print(f"------>Initial sync: fetching test runs from last 60 days")
        elif days_back:
            since_date = datetime.now() - timedelta(days=days_back)
            print(f"------>Fetching test runs from last {days_back} day(s)")
        else:
            # Default: 1 day for daily sync
            since_date = datetime.now() - timedelta(days=1)
            print(f"------>Fetching test runs from last 1 day")
        
        print(f"------>Monitoring workflows: {', '.join(self.workflows)}")
        print(f"------>Branch: {self.branch}")
        
        # Fetch workflow runs
        runs = self.fetch_workflow_runs(since_date=since_date, limit=200 if initial_sync else 50)
        print(f"------>Found {len(runs)} workflow runs to process")
        
        processed_count = 0
        
        for run in runs:
            run_id = run['id']
            
            # Skip if already synced
            if self.is_run_synced(run_id):
                continue
            
            workflow_name = run['name']
            print(f"------>Processing run #{run['run_number']}: {workflow_name} (ID: {run_id})")
            
            # Fetch artifacts (pass workflow name for smart artifact selection)
            artifacts = self.fetch_artifacts(run_id, workflow_name=workflow_name)
            
            if not artifacts:
                print(f"        No matching test artifacts found")
                continue
            
            # Process only the FIRST matching artifact per run (avoid duplicates)
            artifact = artifacts[0]
            artifact_name = artifact['name']
            print(f"        Downloading artifact: {artifact_name}")
            
            # Download artifact
            zip_content = self.download_artifact(artifact['id'])
            if not zip_content:
                continue
            
            # Extract TRX files
            trx_files = self.extract_trx_from_zip(zip_content)
            
            if not trx_files:
                print(f"        No TRX files found in artifact")
                continue
            
            # Parse and store the first TRX file
            filename, content = trx_files[0]
            print(f"        Parsing: {filename}")
            
            parsed = TRXParser.parse(content)
            if not parsed:
                print(f"        Failed to parse TRX file")
                continue
            
            summary = parsed['summary']
            print(f"        Tests: {summary['total']} total, {summary['passed']} passed, {summary['failed']} failed, {summary['not_executed']} skipped")
            
            # Store results
            if self.store_test_run(run, parsed, artifact_name):
                processed_count += 1
                print(f"        ✓ Stored test results")
        
        print(f"------>Processed {processed_count} test runs")
        return processed_count
    
    def get_sync_summary(self) -> dict:
        """Get a summary of synced test data."""
        try:
            with self.db.engine.connect() as connection:
                total_runs = connection.execute(text(
                    "SELECT COUNT(*) FROM github_test_runs"
                )).scalar()
                
                total_tests = connection.execute(text(
                    "SELECT COUNT(*) FROM github_test_results"
                )).scalar()
                
                latest_run = connection.execute(text(
                    "SELECT MAX(run_started_at) FROM github_test_runs"
                )).scalar()
                
                return {
                    'total_runs': total_runs,
                    'total_tests': total_tests,
                    'latest_run': latest_run
                }
        except SQLAlchemyError:
            return {'total_runs': 0, 'total_tests': 0, 'latest_run': None}


def main():
    """Standalone execution for testing."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Sync GitHub Actions test results')
    parser.add_argument('--days', type=int, default=1, help='Number of days to sync (default: 1)')
    parser.add_argument('--initial', action='store_true', help='Run initial sync (2 months)')
    parser.add_argument('--workflows', type=str, help='Comma-separated workflow names')
    parser.add_argument('--branch', type=str, help='Branch to filter')
    args = parser.parse_args()
    
    # Create database connection
    from export_ado import get_database_connection
    db = get_database_connection()
    
    # Create extractor
    extractor = GitHubTestsExtractor(
        db_connection=db,
        branch=args.branch,
        workflows=args.workflows.split(',') if args.workflows else None
    )
    
    # Run sync
    if args.initial:
        processed = extractor.sync_test_runs(initial_sync=True)
    else:
        processed = extractor.sync_test_runs(days_back=args.days)
    
    # Print summary
    summary = extractor.get_sync_summary()
    print(f"\n=== Sync Complete ===")
    print(f"Total runs in DB: {summary['total_runs']}")
    print(f"Total tests in DB: {summary['total_tests']}")
    print(f"Latest run: {summary['latest_run']}")


if __name__ == '__main__':
    main()

