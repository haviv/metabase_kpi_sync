import requests
import csv
import base64
import time
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, text, Table, Column, Integer, String, DateTime, MetaData, ForeignKey, inspect, Float
from sqlalchemy.dialects.postgresql import TEXT as PG_TEXT
from sqlalchemy.exc import SQLAlchemyError
import json
import re
import sys
from abc import ABC, abstractmethod

# Import GitHub Copilot metrics extractor
try:
    from export_github_copilot import GitHubCopilotExtractor, CopilotMetricsDatabase
    COPILOT_AVAILABLE = True
except ImportError:
    COPILOT_AVAILABLE = False

# Import GitHub Tests extractor
try:
    from export_github_tests import GitHubTestsExtractor
    GITHUB_TESTS_AVAILABLE = True
except ImportError:
    GITHUB_TESTS_AVAILABLE = False

# Import GitHub PR metrics extractor
try:
    from export_github_prs import GitHubPRExtractor, PRMetricsDatabase
    PR_METRICS_AVAILABLE = True
except ImportError:
    PR_METRICS_AVAILABLE = False

# Import Azure cost metrics extractor
try:
    from export_azure_cost import AzureCostExtractor, should_sync_azure_cost
    AZURE_COST_AVAILABLE = True
except ImportError:
    AZURE_COST_AVAILABLE = False

# Load environment variables from .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

class DatabaseConnection:
    def __init__(self):
        self.connection_string = (
            f"postgresql://{os.getenv('PG_USERNAME')}:{os.getenv('PG_PASSWORD')}@"
            f"{os.getenv('PG_HOST')}:{os.getenv('PG_PORT', '5432')}/"
            f"{os.getenv('PG_DATABASE')}"
        )
        self.engine = self._create_engine()
        self.metadata = MetaData()
        self.setup_tables()

    def _create_engine(self):
        return create_engine(self.connection_string)

    def _get_text_type(self):
        return PG_TEXT

    def _create_index(self, connection, index_name, table_name, columns):
        connection.execute(text(f"""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({columns})
        """))

    def _drop_index(self, connection, index_name, table_name):
        connection.execute(text(f"""
            DROP INDEX IF EXISTS {index_name}
        """))

    def _get_merge_query(self, snapshot_date, metrics):
        # Build the source CTE for all metrics
        source_queries = []
        for name, query in metrics:
            source_queries.append(f"SELECT CURRENT_DATE AS snapshot_date, '{name}' AS name, ({query}) AS number")
        
        source_cte = " UNION ALL ".join(source_queries)
        
        # Build the upsert query using ON CONFLICT
        merge_query = f"""
        WITH source_data AS ({source_cte})
        INSERT INTO history_snapshots (snapshot_date, name, number)
        SELECT snapshot_date, name, number FROM source_data
        ON CONFLICT (snapshot_date, name) 
        DO UPDATE SET number = EXCLUDED.number;
        """
        
        return merge_query

    def _handle_identity_insert(self, connection, table_name, enable=True):
        # PostgreSQL doesn't need identity insert handling
        pass

    def _get_last_sync_query(self):
        return """
            SELECT last_sync_time 
            FROM sync_status 
            WHERE entity_type = :entity_type 
            ORDER BY last_sync_time DESC
            LIMIT 1
        """

    def update_parent_issue(self):
        """Update parent_issue references in bugs table"""
        try:
            with self.engine.connect() as connection:
                update_query = text("""
                    WITH ValidParents AS (
                        SELECT 
                            b.id as bug_id,
                            COALESCE(
                                CASE WHEN i.id IS NOT NULL THEN b.parent_issue END,
                                (
                                    SELECT issue_id
                                    FROM bugs_relations br
                                    JOIN issues i2 ON br.issue_id = i2.id
                                    WHERE br.bug_id = b.id 
                                    LIMIT 1
                                )
                            ) as new_parent_issue
                        FROM bugs b
                        LEFT JOIN issues i ON b.parent_issue = i.id
                    )
                    UPDATE bugs
                    SET parent_issue = vp.new_parent_issue
                    FROM ValidParents vp
                    WHERE bugs.id = vp.bug_id
                    AND COALESCE(bugs.parent_issue, -1) != COALESCE(vp.new_parent_issue, -1)
                """)
                
                result = connection.execute(update_query)
                connection.commit()
                
                rows_affected = result.rowcount
                print(f"Updated {rows_affected} bugs with new parent issue references")
                
        except SQLAlchemyError as e:
            print(f"Error updating parent issues: {str(e)}")
            connection.rollback()
            raise

    def update_jira_parent_links(self, items):
        """Resolve Jira parent keys to numeric parent IDs after all items are upserted."""
        if not items:
            return 0

        updated = 0
        with self.engine.connect() as connection:
            try:
                for item in items:
                    parent_key = item.get('ParentJiraKey')
                    if not parent_key:
                        continue

                    parent_id = None
                    for table_name in ('bugs', 'work_items', 'issues'):
                        parent_row = connection.execute(
                            text(f"SELECT id FROM {table_name} WHERE jira_id = :jira_id"),
                            {"jira_id": parent_key},
                        ).first()
                        if parent_row:
                            parent_id = parent_row[0]
                            break
                    if not parent_id:
                        continue

                    work_item_type = item.get('WorkItemType', '')
                    if work_item_type == 'Bug':
                        result = connection.execute(
                            text("""
                                UPDATE bugs
                                SET parent_issue = :parent_id
                                WHERE id = :id
                                  AND COALESCE(parent_issue, -1) <> :parent_id
                            """),
                            {"id": item['ID'], "parent_id": parent_id},
                        )
                    else:
                        result = connection.execute(
                            text("""
                                UPDATE work_items
                                SET parent_work_item = :parent_id
                                WHERE id = :id
                                  AND COALESCE(parent_work_item, -1) <> :parent_id
                            """),
                            {"id": item['ID'], "parent_id": parent_id},
                        )
                    updated += result.rowcount
                connection.commit()
            except SQLAlchemyError as e:
                print(f"Error updating Jira parent links: {str(e)}")
                connection.rollback()
                raise
        if updated:
            print(f"Updated {updated} Jira records with resolved parent links")
        return updated

    def purge_invalid_jira_change_history(self):
        """Remove Jira migration replay and pre-cutover Jira history rows."""
        cutover = get_jira_cutover_date()
        migration_authors = get_jira_migration_authors()
        with self.engine.connect() as connection:
            result = connection.execute(
                text("""
                    DELETE FROM change_history
                    WHERE source = 'JIRA'
                      AND (
                        changed_date < :cutover
                        OR LOWER(COALESCE(changed_by, '')) LIKE ANY(:author_patterns)
                      )
                """),
                {
                    "cutover": cutover,
                    "author_patterns": [f"%{author.lower()}%" for author in migration_authors] or ['%tfs4jira%'],
                },
            )
            connection.commit()
            deleted = result.rowcount
        print(f"------>Purged {deleted} invalid Jira change_history rows (pre-cutover or migration replay)")
        return deleted

    def _add_new_columns(self, connection, table_name):
        """Add new columns to existing tables if they don't exist"""
        inspector = inspect(self.engine)
        existing_columns = [col['name'] for col in inspector.get_columns(table_name)]
        
        new_columns = {
            'jira_id': 'VARCHAR(50)',
            'source': "VARCHAR(20) NOT NULL DEFAULT 'ADO'",
            'iteration_path': 'VARCHAR(500)',
            'hotfix_delivered_version': 'VARCHAR(200)',

            'target_date': 'TIMESTAMP',
            'hf_status': 'VARCHAR(200)',
            'hf_requested_versions': 'VARCHAR(200)'
        }
        
        # Add work_item_type column for work_items table if it doesn't exist
        if table_name == 'work_items' and 'work_item_type' not in existing_columns:
            new_columns['work_item_type'] = 'VARCHAR(50)'
        
        # Add effort-related columns for work_items table
        if table_name == 'work_items':
            work_item_columns = {
                'effort': 'FLOAT',
                'effort_dev_estimate': 'FLOAT',
                'effort_dev_actual': 'FLOAT',
                'qa_effort_estimation': 'FLOAT',
                'qa_effort_actual': 'FLOAT',
                'tshirt_estimation': 'VARCHAR(50)',
                'parent_work_item': 'INTEGER',
                'ticket_type': 'VARCHAR(200)',
                'freshdesk_ticket': 'VARCHAR(200)',
                'target_version': 'VARCHAR(200)',
                'tags': 'VARCHAR(500)',
                'connector': 'VARCHAR(1000)',
                'created_by': 'VARCHAR(200)',
                'blocker': 'VARCHAR(200)',
                'business_value': 'INTEGER',
                'business_outcome': 'VARCHAR(200)'
            }
            for col_name, col_type in work_item_columns.items():
                if col_name not in existing_columns:
                    new_columns[col_name] = col_type
        
        for col_name, col_type in new_columns.items():
            if col_name not in existing_columns:
                connection.execute(text(f"""
                    DO $$ 
                    BEGIN 
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name = '{table_name}' 
                            AND column_name = '{col_name}'
                        ) THEN
                            ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type};
                        END IF;
                    END $$;
                """))

        # Drop and recreate indexes for work_items if needed
        if table_name == 'work_items':
            self._drop_index(connection, "idx_work_items_changed_date", "work_items")
            self._drop_index(connection, "idx_work_items_type", "work_items")
            self._create_index(connection, "idx_work_items_changed_date", "work_items", "changed_date")
            self._create_index(connection, "idx_work_items_type", "work_items", "work_item_type")

    def _ensure_jira_metadata_columns(self, connection, table_name):
        """Ensure public tables can store Jira identity without changing numeric ids."""
        connection.execute(text(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = '{table_name}' AND column_name = 'jira_id'
                ) THEN
                    ALTER TABLE {table_name} ADD COLUMN jira_id VARCHAR(50);
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = '{table_name}' AND column_name = 'source'
                ) THEN
                    ALTER TABLE {table_name} ADD COLUMN source VARCHAR(20) NOT NULL DEFAULT 'ADO';
                END IF;
            END $$;
        """))

    def setup_tables(self):
        """Create tables if they don't exist and add new columns if needed"""
        try:
            text_type = self._get_text_type()

            # Issues table
            self.issues = Table(
                'issues', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('jira_id', String(50), nullable=True),
                Column('source', String(20), nullable=False, server_default=text("'ADO'")),
                Column('title', String(500), nullable=False),
                Column('description', text_type, nullable=True),
                Column('assigned_to', String(200), nullable=True),
                Column('severity', String(50), nullable=True),
                Column('state', String(50), nullable=False),
                Column('customer_name', String(200), nullable=True),
                Column('area_path', String(500), nullable=True),
                Column('created_date', DateTime, nullable=False),
                Column('changed_date', DateTime, nullable=False),
                Column('iteration_path', String(500), nullable=True),
                Column('hotfix_delivered_version', String(200), nullable=True),
                Column('target_date', DateTime, nullable=True),
                Column('hf_status', String(200), nullable=True),
                Column('hf_requested_versions', String(200), nullable=True)
            )

            # Bugs relations table
            self.bugs_relations = Table(
                'bugs_relations', self.metadata,
                Column('bug_id', Integer, nullable=False),
                Column('issue_id', Integer, nullable=False),
                Column('type', String(50), nullable=False)
            )

            # Bugs table with new columns
            self.bugs = Table(
                'bugs', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('jira_id', String(50), nullable=True),
                Column('source', String(20), nullable=False, server_default=text("'ADO'")),
                Column('parent_issue', Integer, nullable=True),
                Column('title', String(500), nullable=False),
                Column('description', text_type, nullable=True),
                Column('assigned_to', String(200), nullable=True),
                Column('severity', String(50), nullable=True),
                Column('state', String(50), nullable=False),
                Column('customer_name', String(200), nullable=True),
                Column('area_path', String(500), nullable=True),
                Column('created_date', DateTime, nullable=False),
                Column('changed_date', DateTime, nullable=False),
                Column('iteration_path', String(500), nullable=True),
                Column('hotfix_delivered_version', String(200), nullable=True),
                Column('target_date', DateTime, nullable=True),
                Column('hf_status', String(200), nullable=True),
                Column('hf_requested_versions', String(200), nullable=True)
            )

            # Sync status table
            self.sync_status = Table(
                'sync_status', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('entity_type', String(50), nullable=False),
                Column('last_sync_time', DateTime, nullable=False),
                Column('status', String(50), nullable=False),
                Column('records_processed', Integer, nullable=False, default=0)
            )

            # Work items table
            self.work_items = Table(
                'work_items', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('jira_id', String(50), nullable=True),
                Column('source', String(20), nullable=False, server_default=text("'ADO'")),
                Column('title', String(500), nullable=False),
                Column('description', text_type, nullable=True),
                Column('assigned_to', String(200), nullable=True),
                Column('severity', String(50), nullable=True),
                Column('state', String(50), nullable=False),
                Column('customer_name', String(200), nullable=True),
                Column('area_path', String(500), nullable=True),
                Column('created_date', DateTime, nullable=False),
                Column('changed_date', DateTime, nullable=False),
                Column('iteration_path', String(500), nullable=True),
                Column('hotfix_delivered_version', String(200), nullable=True),
                Column('work_item_type', String(50), nullable=False),
                Column('target_date', DateTime, nullable=True),
                Column('hf_status', String(200), nullable=True),
                Column('hf_requested_versions', String(200), nullable=True),
                Column('effort', Float, nullable=True),
                Column('effort_dev_estimate', Float, nullable=True),
                Column('effort_dev_actual', Float, nullable=True),
                Column('qa_effort_estimation', Float, nullable=True),
                Column('qa_effort_actual', Float, nullable=True),
                Column('tshirt_estimation', String(50), nullable=True),
                Column('parent_work_item', Integer, nullable=True),
                Column('ticket_type', String(200), nullable=True),
                Column('freshdesk_ticket', String(200), nullable=True),
                Column('target_version', String(200), nullable=True),
                Column('tags', String(500), nullable=True),
                Column('connector', String(1000), nullable=True),
                Column('created_by', String(200), nullable=True),
                Column('blocker', String(200), nullable=True),
                Column('business_value', Integer, nullable=True),
                Column('business_outcome', String(200), nullable=True)
            )

            # History snapshots table
            self.history_snapshots = Table(
                'history_snapshots', self.metadata,
                Column('snapshot_date', DateTime, primary_key=True),
                Column('name', String(255), primary_key=True),
                Column('number', Float, nullable=False)
            )

            # Change history table
            self.change_history = Table(
                'change_history', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('record_id', Integer, nullable=False),
                Column('jira_id', String(50), nullable=True),
                Column('source', String(20), nullable=False, server_default=text("'ADO'")),
                Column('table_name', String(50), nullable=False),
                Column('field_changed', String(100), nullable=False),
                Column('old_value', String(500), nullable=True),
                Column('new_value', String(500), nullable=True),
                Column('changed_by', String(200), nullable=True),
                Column('changed_date', DateTime, nullable=True, server_default=text('CURRENT_TIMESTAMP')),
            )

            # Sprints table
            self.sprints = Table(
                'sprints', self.metadata,
                Column('id', String(255), primary_key=True),
                Column('name', String(500), nullable=False),
                Column('path', String(1000), nullable=True),
                Column('start_date', DateTime, nullable=True),
                Column('finish_date', DateTime, nullable=True),
                Column('state', String(50), nullable=True),
                Column('created_date', DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP')),
                Column('updated_date', DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP')),
            )

            # Sprint capacity table - stores capacity per developer per sprint
            self.sprint_capacity = Table(
                'sprint_capacity', self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('sprint_id', String(255), nullable=False),
                Column('sprint_name', String(500), nullable=False),
                Column('team_member_id', String(255), nullable=False),
                Column('team_member_name', String(200), nullable=False),
                Column('activity', String(100), nullable=True),
                Column('capacity_per_day', Float, nullable=True),
                Column('days_off_count', Integer, nullable=True, default=0),
                Column('days_off_start', DateTime, nullable=True),
                Column('days_off_end', DateTime, nullable=True),
                Column('project_name', String(200), nullable=True),
                Column('team_name', String(200), nullable=True),
                Column('created_date', DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP')),
                Column('updated_date', DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP')),
            )

            # Create tables if they don't exist
            self.metadata.create_all(self.engine, checkfirst=True)

            # Add new columns to existing tables after table creation
            with self.engine.connect() as connection:
                inspector = inspect(self.engine)
                existing_tables = inspector.get_table_names()
                
                # Add new columns if tables exist
                if 'issues' in existing_tables:
                    self._add_new_columns(connection, 'issues')
                    self._ensure_jira_metadata_columns(connection, 'issues')
                
                if 'bugs' in existing_tables:
                    self._add_new_columns(connection, 'bugs')
                    self._ensure_jira_metadata_columns(connection, 'bugs')
                
                if 'work_items' in existing_tables:
                    self._add_new_columns(connection, 'work_items')
                    self._ensure_jira_metadata_columns(connection, 'work_items')

                if 'change_history' in existing_tables:
                    self._ensure_jira_metadata_columns(connection, 'change_history')
                
                # Drop existing indexes if tables exist
                if 'issues' in existing_tables:
                    self._drop_index(connection, "idx_issues_changed_date", "issues")
                
                if 'bugs' in existing_tables:
                    self._drop_index(connection, "idx_bugs_changed_date", "bugs")
                    self._drop_index(connection, "idx_bugs_parent_issue", "bugs")
                
                if 'sync_status' in existing_tables:
                    self._drop_index(connection, "idx_sync_status_entity_type", "sync_status")
                
                if 'sprints' in existing_tables:
                    self._drop_index(connection, "idx_sprints_name", "sprints")
                    self._drop_index(connection, "idx_sprints_start_date", "sprints")
                    self._drop_index(connection, "idx_sprints_finish_date", "sprints")
                
                if 'sprint_capacity' in existing_tables:
                    self._drop_index(connection, "idx_sprint_capacity_sprint_id", "sprint_capacity")
                    self._drop_index(connection, "idx_sprint_capacity_team_member", "sprint_capacity")
                    self._drop_index(connection, "idx_sprint_capacity_composite", "sprint_capacity")

                # Drop existing change_history indexes if table exists (will be recreated below)
                if 'change_history' in existing_tables:
                    self._drop_index(connection, "idx_change_history_query_opt", "change_history")
                    self._drop_index(connection, "idx_change_history_changed_date", "change_history")
                    self._drop_index(connection, "idx_change_history_record_id", "change_history")
                    self._drop_index(connection, "idx_change_history_field_changed", "change_history")

                # Alter history_snapshots.number column to FLOAT if the table exists
                if 'history_snapshots' in existing_tables:
                    connection.execute(text("""
                        ALTER TABLE history_snapshots 
                        ALTER COLUMN number TYPE FLOAT 
                        USING number::FLOAT
                    """))
                
                # Alter work_items.connector column to VARCHAR(1000) if the table exists
                if 'work_items' in existing_tables:
                    connection.execute(text("""
                        ALTER TABLE work_items 
                        ALTER COLUMN connector TYPE VARCHAR(1000);
                    """))
                
                connection.commit()

            # Create indexes
            with self.engine.connect() as connection:
                try:
                    # Create indexes with database-specific syntax
                    self._create_index(connection, "idx_issues_changed_date", "issues", "changed_date")
                    self._create_index(connection, "idx_issues_jira_id", "issues", "jira_id")
                    
                    self._create_index(connection, "idx_bugs_changed_date", "bugs", "changed_date")
                    self._create_index(connection, "idx_bugs_parent_issue", "bugs", "parent_issue")
                    self._create_index(connection, "idx_bugs_jira_id", "bugs", "jira_id")
                    
                    self._create_index(connection, "idx_sync_status_entity_type", "sync_status", "entity_type, last_sync_time DESC")
                    
                    self._create_index(connection, "idx_bugs_relations_bug_id", "bugs_relations", "bug_id")
                    
                    # Add indexes for work_items table
                    self._create_index(connection, "idx_work_items_changed_date", "work_items", "changed_date")
                    self._create_index(connection, "idx_work_items_type", "work_items", "work_item_type")
                    self._create_index(connection, "idx_work_items_jira_id", "work_items", "jira_id")
                    
                    # Add indexes for sprints table
                    self._create_index(connection, "idx_sprints_name", "sprints", "name")
                    self._create_index(connection, "idx_sprints_start_date", "sprints", "start_date")
                    self._create_index(connection, "idx_sprints_finish_date", "sprints", "finish_date")
                    
                    # Add indexes for sprint_capacity table
                    self._create_index(connection, "idx_sprint_capacity_sprint_id", "sprint_capacity", "sprint_id")
                    self._create_index(connection, "idx_sprint_capacity_team_member", "sprint_capacity", "team_member_id")
                    self._create_index(connection, "idx_sprint_capacity_composite", "sprint_capacity", "sprint_id, team_member_id, activity")

                    # Add indexes for change_history table to optimize Metabase report queries (#1)
                    self._create_index(connection, "idx_change_history_query_opt", "change_history", "table_name, field_changed, new_value, changed_date")
                    self._create_index(connection, "idx_change_history_changed_date", "change_history", "changed_date")
                    self._create_index(connection, "idx_change_history_record_id", "change_history", "record_id, table_name")
                    self._create_index(connection, "idx_change_history_field_changed", "change_history", "field_changed, table_name")

                    connection.commit()
                except SQLAlchemyError as e:
                    print(f"Error creating indexes: {str(e)}")
                    connection.rollback()
                    raise
                
        except SQLAlchemyError as e:
            print(f"Error setting up database: {str(e)}")
            raise

    def get_last_sync_time(self, entity_type):
        """Get the last successful sync time for the entity type"""
        with self.engine.connect() as connection:
            result = connection.execute(
                text(self._get_last_sync_query()),
                {"entity_type": entity_type}
            ).first()

            if result:
                return result[0]
            
            # If no sync history, default to March 1st, 2025
            return datetime(2025, 3, 1)

    def has_entity_sync_history(self, entity_type):
        """Return True if at least one successful sync exists for the entity type."""
        with self.engine.connect() as connection:
            result = connection.execute(
                text("SELECT 1 FROM sync_status WHERE entity_type = :entity_type LIMIT 1"),
                {"entity_type": entity_type}
            ).first()
            return result is not None

    def get_last_full_sync_time(self):
        """Get the last full sync time (weekly full sync)"""
        with self.engine.connect() as connection:
            result = connection.execute(
                text("""
                    SELECT last_sync_time 
                    FROM sync_status 
                    WHERE entity_type = 'full_sync' 
                    ORDER BY last_sync_time DESC
                    LIMIT 1
                """)
            ).first()

            if result:
                return result[0]
            
            # If no full sync history, return None to trigger first full sync
            return None

    def update_full_sync_status(self):
        """Update the full sync status timestamp"""
        with self.engine.connect() as connection:
            try:
                connection.execute(
                    text("""
                        INSERT INTO sync_status (entity_type, last_sync_time, status, records_processed)
                        VALUES ('full_sync', CURRENT_TIMESTAMP, 'completed', 0)
                    """)
                )
                connection.commit()
            except SQLAlchemyError as e:
                print(f"Error updating full sync status: {str(e)}")
                connection.rollback()

    def update_sync_status(self, entity_type, records_processed, status='completed'):
        """Update the sync status for the entity type"""
        with self.engine.connect() as connection:
            try:
                connection.execute(
                    text("""
                        INSERT INTO sync_status (entity_type, last_sync_time, status, records_processed)
                        VALUES (:entity_type, CURRENT_TIMESTAMP, :status, :records_processed)
                    """),
                    {
                        "entity_type": entity_type,
                        "status": status,
                        "records_processed": records_processed
                    }
                )
                connection.commit()
            except SQLAlchemyError as e:
                print(f"Error updating sync status: {str(e)}")
                connection.rollback()

    def update_history_snapshots(self):
        """Update the history snapshots table with current metrics"""
        metrics = [
            ('total_bugs', 'SELECT COUNT(*) FROM bugs'),
            ('avg_days_to_close_p1_p2', "SELECT ROUND(AVG(days_to_resolution), 2) AS avg_days_new_to_resolution FROM (SELECT f.bug_id, EXTRACT(DAY FROM r.end_date - f.new_date) AS days_to_resolution FROM (SELECT bug_id, MIN(changed_date) AS new_date FROM (SELECT ch.record_id AS bug_id, ch.new_value, ch.changed_date FROM public.change_history ch JOIN public.bugs b ON ch.record_id = b.id WHERE ch.field_changed = 'System.State' AND ch.new_value IN ('New', 'QA Completed', 'Removed', 'Done', 'Not reproduced') AND ch.table_name = 'bugs' AND ch.changed_date >= DATE_TRUNC('month', NOW() - INTERVAL '6 months') AND ch.changed_date < DATE_TRUNC('month', NOW()) AND b.parent_issue IS NOT NULL AND b.severity IN ('1', '2')) AS state_changes WHERE new_value = 'New' GROUP BY bug_id) f JOIN (SELECT bug_id, MIN(changed_date) AS end_date FROM (SELECT ch.record_id AS bug_id, ch.new_value, ch.changed_date FROM public.change_history ch JOIN public.bugs b ON ch.record_id = b.id WHERE ch.field_changed = 'System.State' AND ch.new_value IN ('New', 'QA Completed', 'Removed') AND ch.table_name = 'bugs' AND ch.changed_date >= DATE_TRUNC('month', NOW() - INTERVAL '6 months') AND ch.changed_date < DATE_TRUNC('month', NOW()) AND b.parent_issue IS NOT NULL AND b.severity IN ('1', '2')) AS state_changes WHERE new_value IN ('QA Completed', 'Removed') GROUP BY bug_id) r ON r.bug_id = f.bug_id WHERE r.end_date > f.new_date) AS durations"),
            ('open_bugs', "SELECT COUNT(*) FROM bugs WHERE state NOT IN ('Done', 'Not reproduced', 'QA Completed', 'Removed')"),
            ('in_qa_bugs', "SELECT COUNT(*) FROM bugs WHERE state = 'In QA'"),
            ('new_bugs', "SELECT COUNT(*) FROM bugs WHERE state IN ('New', 'Approved')"),
            ('closed_bugs', "SELECT COUNT(*) FROM bugs WHERE state IN ('Done', 'Not reproduced', 'QA Completed', 'Removed')"),
            ('reopened_bugs', "SELECT COUNT(*) FROM bugs WHERE state = 'Reopened' AND changed_date >= CURRENT_DATE - INTERVAL '1 day'"),
            ('p1_bugs', "SELECT COUNT(*) FROM bugs WHERE severity = '1' AND state NOT IN ('Done', 'Not reproduced', 'QA Completed', 'Removed')"),
            ('p2_bugs', "SELECT COUNT(*) FROM bugs WHERE severity = '2' AND state NOT IN ('Done', 'Not reproduced', 'QA Completed', 'Removed')"),
            ('p1_p2_bugs_with_customer_issues', "SELECT COUNT(*) FROM bugs WHERE state NOT IN ('Done', 'Not reproduced', 'QA Completed', 'Removed') AND severity IN ('1','2') AND parent_issue IS NOT NULL"),
            ('p1_p2_bugs_in_dev', "SELECT COUNT(*) FROM bugs WHERE state IN ('Approved', 'Issues Found', 'New', 'In Progress', 'Waiting for PR') AND severity IN ('1','2')"),
            ('open_bugs_QA_p1_p2', "SELECT COUNT(*) FROM bugs WHERE state IN ('Ready for QA', 'In QA') AND severity IN ('1','2')"),
            ('total_customers', " SELECT COUNT(DISTINCT i.customer_name) AS total_customers FROM issues i"),
            ('redline_bugs', "SELECT  COUNT(*) * 1.0 / (SELECT COUNT(DISTINCT customer_name) FROM bugs) AS bugs_with_parent_per_customer FROM bugs WHERE parent_issue IS NOT NULL AND DATE_TRUNC('month', created_date) = DATE_TRUNC('month', CURRENT_DATE)"),
            ('open_p1_bugs_with_customer_issues', "SELECT COUNT(*) FROM bugs WHERE state NOT IN ('Done', 'Not reproduced', 'QA Completed', 'Removed') AND severity = '1' AND parent_issue IS NOT NULL"),
            ('open_p1_bugs_with_customer_issues_cn', "SELECT COUNT(*) FROM work_items WHERE state NOT IN ('Done', 'Not reproduced', 'QA Completed', 'Removed') AND severity = '1' AND customer_name IS NOT NULL and created_date>= '2025-12-01' and customer_name <> '' and work_item_type = 'Bug' AND hf_status <> 'Hotfix Parent'"),
            ('p1_p2_bugs_with_customer_issues_cn', "SELECT COUNT(*) FROM work_items WHERE state NOT IN ('Done', 'Not reproduced', 'QA Completed', 'Removed') AND severity IN ('1','2') AND customer_name IS NOT NULL and created_date>= '2025-12-01' and customer_name <> '' and work_item_type = 'Bug' AND hf_status <> 'Hotfix Parent'")
        ]

        snapshot_date = datetime.now().date()
        merge_query = self._get_merge_query(snapshot_date, metrics)

        try:
            with self.engine.connect() as connection:
                connection.execute(text(merge_query))
                connection.commit()
        except SQLAlchemyError as e:
            print(f"Error updating history snapshots: {str(e)}")
            raise

    def update_bug_relations(self, bug_id, relations):
        """Update relations for a bug in the bugs_relations table"""
        try:
            with self.engine.connect() as connection:
                # First delete existing relations for this bug
                connection.execute(
                    text("DELETE FROM bugs_relations WHERE bug_id = :bug_id"),
                    {"bug_id": bug_id}
                )

                # Process each relation
                for relation in relations:
                    rel_type = relation.get('rel')
                    # Only process parent and related relations
                    if rel_type in ['System.LinkTypes.Hierarchy-Forward', 'System.LinkTypes.Related']:
                        # Extract the issue ID from the URL
                        url = relation.get('url', '')
                        try:
                            issue_id = int(url.split('/')[-1])
                            
                            # Map the relation type to a simpler form
                            relation_type = 'parent' if rel_type == 'System.LinkTypes.Hierarchy-Forward' else 'related'
                            
                            # Insert the new relation
                            connection.execute(
                                text("""
                                    INSERT INTO bugs_relations (bug_id, issue_id, type)
                                    VALUES (:bug_id, :issue_id, :type)
                                """),
                                {
                                    "bug_id": bug_id,
                                    "issue_id": issue_id,
                                    "type": relation_type
                                }
                            )
                        except (ValueError, IndexError) as e:
                            print(f"Error processing relation URL for bug {bug_id}: {str(e)}")
                            continue

                connection.commit()
                
        except SQLAlchemyError as e:
            print(f"Error updating relations for bug {bug_id}: {str(e)}")
            connection.rollback()
            raise

    def upsert_items(self, items, table, item_type='bug'):
        """Upsert items into the specified table"""
        processed_count = 0
        with self.engine.connect() as connection:
            for item in items:
                try:
                    # Convert dates from string to datetime
                    for date_field in ['CreatedDate', 'ChangedDate']:
                        if item.get(date_field) is None:
                            continue
                        if item[date_field]:
                            if isinstance(item[date_field], datetime):
                                item[date_field] = normalize_utc_naive(item[date_field])
                                continue
                            parsed_date = None
                            for date_format in (
                                "%Y-%m-%d",
                                "%Y-%m-%dT%H:%M:%S.%fZ",
                                "%Y-%m-%dT%H:%M:%SZ",
                                "%Y-%m-%dT%H:%M:%S.%f%z",
                                "%Y-%m-%dT%H:%M:%S%z",
                                "%a, %d %b %Y %H:%M:%S %z",
                            ):
                                try:
                                    parsed_date = datetime.strptime(item[date_field], date_format)
                                    break
                                except ValueError:
                                    continue
                            if parsed_date is None:
                                raise ValueError(f"Unsupported date format for {date_field}: {item[date_field]}")
                            item[date_field] = normalize_utc_naive(parsed_date)

                    # Check if item exists
                    existing_columns = "jira_id, source"
                    if item.get('Source') == 'JIRA':
                        existing_columns += ", created_date"
                        if item_type == 'work_item':
                            existing_columns += ", area_path, iteration_path, parent_work_item"
                        elif item_type == 'bug':
                            existing_columns += ", area_path, iteration_path, parent_issue"
                        else:
                            existing_columns += ", area_path, iteration_path"
                    result = connection.execute(
                        text(f"SELECT {existing_columns} FROM {table.name} WHERE id = :id"),
                        {"id": item['ID']}
                    ).first()

                    if result and item.get('Source') == 'JIRA':
                        existing_created = result[2] if len(result) > 2 else None
                        path_offset = 3
                        if item.get('MigratedFromADO') and item.get('CreatedDate') is None and existing_created:
                            item['CreatedDate'] = existing_created
                        existing_area = result[path_offset] if len(result) > path_offset else None
                        existing_iteration = result[path_offset + 1] if len(result) > path_offset + 1 else None
                        if not item.get('AreaPath') and existing_area:
                            item['AreaPath'] = existing_area
                        elif (
                            existing_area
                            and item.get('AreaPath')
                            and '\\' not in item['AreaPath']
                            and (
                                existing_area.endswith(f"\\{item['AreaPath']}")
                                or existing_area == item['AreaPath']
                            )
                        ):
                            item['AreaPath'] = existing_area
                        if not item.get('IterationPath') and existing_iteration:
                            item['IterationPath'] = existing_iteration
                        elif (
                            existing_iteration
                            and item.get('IterationPath')
                            and '\\' not in item['IterationPath']
                            and existing_iteration.endswith(item['IterationPath'])
                        ):
                            item['IterationPath'] = existing_iteration

                    if result:
                        if item.get('Source', 'ADO') != 'JIRA' and result[0]:
                            print(f"Skipping ADO update for {table.name} {item['ID']} because it is owned by Jira ({result[0]})")
                            processed_count += 1
                            continue

                        # Update existing item
                        update_stmt = f"""
                        UPDATE {table.name}
                        SET jira_id = :jira_id,
                            source = :source,
                            title = :title,
                            description = :description,
                            assigned_to = :assigned_to,
                            severity = :severity,
                            state = :state,
                            customer_name = :customer_name,
                            area_path = :area_path,
                            created_date = :created_date,
                            changed_date = :changed_date,
                            iteration_path = :iteration_path,
                            hotfix_delivered_version = :hotfix_delivered_version,
                            target_date = :target_date,
                            hf_status = :hf_status,
                            hf_requested_versions = :hf_requested_versions
                        """
                        
                        if item_type == 'bug':
                            update_stmt += ", parent_issue = :parent_issue"
                        elif item_type == 'work_item':
                            update_stmt += ", work_item_type = :work_item_type, effort = :effort, effort_dev_estimate = :effort_dev_estimate, effort_dev_actual = :effort_dev_actual, qa_effort_estimation = :qa_effort_estimation, qa_effort_actual = :qa_effort_actual, tshirt_estimation = :tshirt_estimation, parent_work_item = :parent_work_item, ticket_type = :ticket_type, freshdesk_ticket = :freshdesk_ticket, target_version = :target_version, tags = :tags, connector = :connector, created_by = :created_by, blocker = :blocker, business_value = :business_value, business_outcome = :business_outcome"
                        
                        update_stmt += " WHERE id = :id"

                        params = {
                            "id": item['ID'],
                            "jira_id": item.get('JiraID'),
                            "source": item.get('Source', 'ADO'),
                            "title": item['Title'],
                            "description": item['Description'],
                            "assigned_to": item['AssignedTo'],
                            "severity": item['Severity'],
                            "state": item['State'],
                            "customer_name": item['CustomerName'],
                            "area_path": item['AreaPath'],
                            "created_date": item['CreatedDate'],
                            "changed_date": item['ChangedDate'],
                            "iteration_path": item['IterationPath'],
                            "hotfix_delivered_version": item['HotfixDeliveredVersion'],
                            "target_date": item.get('TargetDate'),
                            "hf_status": item.get('HFStatus'),
                            "hf_requested_versions": item.get('HFRequestedVersions')
                        }
                        
                        if item_type == 'bug':
                            params["parent_issue"] = item.get('ParentID')
                        elif item_type == 'work_item':
                            params["work_item_type"] = item.get('WorkItemType', '')
                            params["effort"] = item.get('Effort')
                            params["effort_dev_estimate"] = item.get('EffortDevEstimate')
                            params["effort_dev_actual"] = item.get('EffortDevActual')
                            params["qa_effort_estimation"] = item.get('QAEffortEstimation')
                            params["qa_effort_actual"] = item.get('QAEffortActual')
                            params["tshirt_estimation"] = item.get('TShirtEstimation', '')
                            params["parent_work_item"] = item.get('ParentWorkItem')
                            params["ticket_type"] = item.get('TicketType', '')
                            params["freshdesk_ticket"] = item.get('FreshdeskTicket', '')
                            params["target_version"] = item.get('TargetVersion', '')
                            params["tags"] = item.get('Tags', '')
                            params["connector"] = item.get('Connector', '')
                            params["created_by"] = item.get('CreatedBy', '')
                            params["blocker"] = item.get('Blocker', '')
                            params["business_value"] = item.get('BusinessValue')
                            params["business_outcome"] = item.get('BusinessOutcome', '')

                        connection.execute(text(update_stmt), params)
                    else:
                        # Handle identity insert for the specific database
                        self._handle_identity_insert(connection, table.name, True)
                        
                        # Insert new item
                        insert_stmt = f"""
                        INSERT INTO {table.name} (
                            id, jira_id, source, title, description, assigned_to, severity,
                            state, customer_name, area_path, created_date, changed_date,
                            iteration_path, hotfix_delivered_version, target_date, hf_status, hf_requested_versions
                        """
                        
                        if item_type == 'bug':
                            insert_stmt += ", parent_issue"
                        elif item_type == 'work_item':
                            insert_stmt += ", work_item_type, effort, effort_dev_estimate, effort_dev_actual, qa_effort_estimation, qa_effort_actual, tshirt_estimation, parent_work_item, ticket_type, freshdesk_ticket, target_version, tags, connector, created_by, blocker, business_value, business_outcome"
                        
                        insert_stmt += """
                        )
                        VALUES (
                            :id, :jira_id, :source, :title, :description, :assigned_to, :severity,
                            :state, :customer_name, :area_path, :created_date, :changed_date,
                            :iteration_path, :hotfix_delivered_version, :target_date, :hf_status, :hf_requested_versions
                        """
                        
                        if item_type == 'bug':
                            insert_stmt += ", :parent_issue"
                        elif item_type == 'work_item':
                            insert_stmt += ", :work_item_type, :effort, :effort_dev_estimate, :effort_dev_actual, :qa_effort_estimation, :qa_effort_actual, :tshirt_estimation, :parent_work_item, :ticket_type, :freshdesk_ticket, :target_version, :tags, :connector, :created_by, :blocker, :business_value, :business_outcome"
                        
                        insert_stmt += ")"

                        params = {
                            "id": item['ID'],
                            "jira_id": item.get('JiraID'),
                            "source": item.get('Source', 'ADO'),
                            "title": item['Title'],
                            "description": item['Description'],
                            "assigned_to": item['AssignedTo'],
                            "severity": item['Severity'],
                            "state": item['State'],
                            "customer_name": item['CustomerName'],
                            "area_path": item['AreaPath'],
                            "created_date": item['CreatedDate'],
                            "changed_date": item['ChangedDate'],
                            "iteration_path": item['IterationPath'],
                            "hotfix_delivered_version": item['HotfixDeliveredVersion'],
                            "target_date": item.get('TargetDate'),
                            "hf_status": item.get('HFStatus'),
                            "hf_requested_versions": item.get('HFRequestedVersions')
                        }
                        
                        if item_type == 'bug':
                            params["parent_issue"] = item.get('ParentID')
                        elif item_type == 'work_item':
                            params["work_item_type"] = item.get('WorkItemType', '')
                            params["effort"] = item.get('Effort')
                            params["effort_dev_estimate"] = item.get('EffortDevEstimate')
                            params["effort_dev_actual"] = item.get('EffortDevActual')
                            params["qa_effort_estimation"] = item.get('QAEffortEstimation')
                            params["qa_effort_actual"] = item.get('QAEffortActual')
                            params["tshirt_estimation"] = item.get('TShirtEstimation', '')
                            params["parent_work_item"] = item.get('ParentWorkItem')
                            params["ticket_type"] = item.get('TicketType', '')
                            params["freshdesk_ticket"] = item.get('FreshdeskTicket', '')
                            params["target_version"] = item.get('TargetVersion', '')
                            params["tags"] = item.get('Tags', '')
                            params["connector"] = item.get('Connector', '')
                            params["created_by"] = item.get('CreatedBy', '')
                            params["blocker"] = item.get('Blocker', '')
                            params["business_value"] = item.get('BusinessValue')
                            params["business_outcome"] = item.get('BusinessOutcome', '')

                        connection.execute(text(insert_stmt), params)
                        
                        # Disable identity insert
                        self._handle_identity_insert(connection, table.name, False)

                    connection.commit()
                    processed_count += 1
                except SQLAlchemyError as e:
                    print(f"Error upserting item {item['ID']}: {str(e)}")
                    connection.rollback()
                    # Ensure identity insert is turned OFF in case of error
                    try:
                        self._handle_identity_insert(connection, table.name, False)
                    except:
                        pass

        return processed_count

    def upsert_sprints(self, sprints):
        """Upsert sprints into the sprints table"""
        processed_count = 0
        with self.engine.connect() as connection:
            for sprint in sprints:
                try:
                    sprint_id = sprint['id']
                    result = connection.execute(
                        text("""
                            SELECT id FROM sprints
                            WHERE path = :path
                               OR (name = :name AND path NOT LIKE 'JIRA/%')
                            ORDER BY CASE WHEN path = :path THEN 0 ELSE 1 END
                            LIMIT 1
                        """),
                        {"path": sprint['path'], "name": sprint['name']},
                    ).first()
                    if result:
                        sprint_id = result[0]

                    existing = connection.execute(
                        text("SELECT id FROM sprints WHERE id = :id"),
                        {"id": sprint_id}
                    ).first()

                    if existing:
                        # Update existing sprint
                        connection.execute(
                            text("""
                                UPDATE sprints 
                                SET name = :name, 
                                    path = :path,
                                    start_date = :start_date, 
                                    finish_date = :finish_date,
                                    state = :state,
                                    updated_date = CURRENT_TIMESTAMP
                                WHERE id = :id
                            """),
                            {
                                "id": sprint_id,
                                "name": sprint['name'],
                                "path": sprint['path'],
                                "start_date": sprint['start_date'],
                                "finish_date": sprint['finish_date'],
                                "state": sprint['state']
                            }
                        )
                    else:
                        # Insert new sprint
                        connection.execute(
                            text("""
                                INSERT INTO sprints (id, name, path, start_date, finish_date, state)
                                VALUES (:id, :name, :path, :start_date, :finish_date, :state)
                            """),
                            {
                                "id": sprint_id,
                                "name": sprint['name'],
                                "path": sprint['path'],
                                "start_date": sprint['start_date'],
                                "finish_date": sprint['finish_date'],
                                "state": sprint['state']
                            }
                        )

                    connection.commit()
                    processed_count += 1
                except SQLAlchemyError as e:
                    print(f"Error upserting sprint {sprint['id']}: {str(e)}")
                    connection.rollback()

            try:
                cleanup = connection.execute(
                    text("""
                        DELETE FROM sprints AS jira_sprint
                        WHERE jira_sprint.path LIKE 'JIRA/%'
                          AND EXISTS (
                            SELECT 1
                            FROM sprints AS ado_sprint
                            WHERE ado_sprint.name = jira_sprint.name
                              AND ado_sprint.path NOT LIKE 'JIRA/%'
                          )
                    """)
                )
                connection.commit()
                if cleanup.rowcount:
                    print(f"------>Removed {cleanup.rowcount} duplicate Jira sprint rows with ADO path equivalents")
            except SQLAlchemyError as e:
                print(f"Error cleaning duplicate Jira sprints: {str(e)}")
                connection.rollback()

        return processed_count

    def upsert_sprint_capacities(self, capacities):
        """Upsert sprint capacities into the sprint_capacity table"""
        processed_count = 0
        with self.engine.connect() as connection:
            for capacity in capacities:
                try:
                    # Check if capacity record exists (based on sprint_id, team_member_id, and activity)
                    result = connection.execute(
                        text("""
                            SELECT id FROM sprint_capacity 
                            WHERE sprint_id = :sprint_id 
                            AND team_member_id = :team_member_id 
                            AND COALESCE(activity, '') = COALESCE(:activity, '')
                        """),
                        {
                            "sprint_id": capacity['sprint_id'],
                            "team_member_id": capacity['team_member_id'],
                            "activity": capacity.get('activity')
                        }
                    ).first()

                    if result:
                        # Update existing capacity record
                        connection.execute(
                            text("""
                                UPDATE sprint_capacity 
                                SET sprint_name = :sprint_name,
                                    team_member_name = :team_member_name,
                                    capacity_per_day = :capacity_per_day,
                                    days_off_count = :days_off_count,
                                    days_off_start = :days_off_start,
                                    days_off_end = :days_off_end,
                                    project_name = :project_name,
                                    team_name = :team_name,
                                    updated_date = CURRENT_TIMESTAMP
                                WHERE id = :id
                            """),
                            {
                                "id": result[0],
                                "sprint_name": capacity['sprint_name'],
                                "team_member_name": capacity['team_member_name'],
                                "capacity_per_day": capacity.get('capacity_per_day'),
                                "days_off_count": capacity.get('days_off_count', 0),
                                "days_off_start": capacity.get('days_off_start'),
                                "days_off_end": capacity.get('days_off_end'),
                                "project_name": capacity.get('project_name'),
                                "team_name": capacity.get('team_name')
                            }
                        )
                    else:
                        # Insert new capacity record
                        connection.execute(
                            text("""
                                INSERT INTO sprint_capacity 
                                (sprint_id, sprint_name, team_member_id, team_member_name, 
                                 activity, capacity_per_day, days_off_count, days_off_start, 
                                 days_off_end, project_name, team_name)
                                VALUES (:sprint_id, :sprint_name, :team_member_id, :team_member_name,
                                        :activity, :capacity_per_day, :days_off_count, :days_off_start,
                                        :days_off_end, :project_name, :team_name)
                            """),
                            {
                                "sprint_id": capacity['sprint_id'],
                                "sprint_name": capacity['sprint_name'],
                                "team_member_id": capacity['team_member_id'],
                                "team_member_name": capacity['team_member_name'],
                                "activity": capacity.get('activity'),
                                "capacity_per_day": capacity.get('capacity_per_day'),
                                "days_off_count": capacity.get('days_off_count', 0),
                                "days_off_start": capacity.get('days_off_start'),
                                "days_off_end": capacity.get('days_off_end'),
                                "project_name": capacity.get('project_name'),
                                "team_name": capacity.get('team_name')
                            }
                        )

                    connection.commit()
                    processed_count += 1
                except SQLAlchemyError as e:
                    print(f"Error upserting sprint capacity for {capacity.get('team_member_name', 'unknown')} in sprint {capacity.get('sprint_id', 'unknown')}: {str(e)}")
                    connection.rollback()

        return processed_count

    def get_last_sprint_capacity_sync(self):
        """Get the last sync time for sprint capacity"""
        with self.engine.connect() as connection:
            result = connection.execute(
                text(self._get_last_sync_query()),
                {"entity_type": "sprint_capacity"}
            ).first()

            if result:
                return result[0]
            
            # If no sync history, return None to indicate first run
            return None

def get_database_connection():
    """Factory function to create the appropriate database connection"""
    try:
        return DatabaseConnection()
    except ModuleNotFoundError as e:
        if "psycopg2" in str(e):
            raise ModuleNotFoundError(
                "PostgreSQL driver (psycopg2) is not installed. "
                "Please install it using: pip install psycopg2-binary"
            ) from e
        raise

class ADOExtractor:
    def __init__(self, organization, project, personal_access_token, scrum_project=None):
        self.organization = organization
        self.project = requests.utils.quote(project)  # URL encode project name
        
        # Handle multiple scrum projects in format "project1:team1,project2:team2"
        if scrum_project:
            self.scrum_projects = []
            for project_team in scrum_project.split(','):
                if ':' in project_team:
                    project_name, team_name = project_team.strip().split(':', 1)
                    self.scrum_projects.append({
                        'project': requests.utils.quote(project_name.strip()),
                        'team': team_name.strip()
                    })
                else:
                    # Fallback to old format - treat as project only
                    self.scrum_projects.append({
                        'project': requests.utils.quote(scrum_project.strip()),
                        'team': None
                    })
        else:
            self.scrum_projects = [{
                'project': self.project,
                'team': None
            }]
        
        self.personal_access_token = personal_access_token
        
        # Setup authentication header
        auth_token = base64.b64encode(f":{personal_access_token}".encode()).decode()
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {auth_token}"
        }

    def get_work_items(self, work_item_type, last_update):
        """Query work items based on type and last update time"""
        query_url = f"https://dev.azure.com/{self.organization}/{self.project}/_apis/wit/wiql?api-version=7.1-preview.2"
        
        # Handle last_update whether it's a string or datetime
        if isinstance(last_update, datetime):
            last_update_str = last_update.strftime('%Y-%m-%d')
        else:
            last_update_str = last_update
        
        print(f"------>Fetching {work_item_type} items with ChangedDate >= {last_update_str}")
        
        # Modified WIQL query to include all required fields
        query_payload = {
            "query": f"""
                SELECT [System.Id],
                       [System.Title],
                       [System.State],
                       [System.CreatedDate],
                       [System.ChangedDate],
                       [System.AssignedTo],
                       [System.Description],
                       [Microsoft.VSTS.Common.Priority],
                       [Microsoft.VSTS.Common.Severity],
                       [Custom.CustomernameGRC],
                       [System.AreaPath],
                       [System.Parent],
                       [System.IterationPath],
                       [Custom.HotfixDeliveredVersions],
                       [Microsoft.VSTS.Scheduling.TargetDate],
                       [Custom.HFstatus],
                       [Custom.HFrequestedversions]
                FROM WorkItems
                WHERE [System.WorkItemType] = '{work_item_type}'
                AND [System.ChangedDate] >= '{last_update_str}'
                ORDER BY [System.ChangedDate] DESC
            """
        }

        # Get work item IDs
        response = requests.post(query_url, json=query_payload, headers=self.headers)
        if response.status_code != 200:
            print(f"Error fetching WIQL query: {response.status_code} - {response.text}")
            return []

        work_items = response.json().get("workItems", [])
        if not work_items:
            return []

        # Get work item details in batches
        items_data = []
        batch_size = 200  # Azure DevOps recommended batch size
        
        for i in range(0, len(work_items), batch_size):
            batch = work_items[i:i + batch_size]
            item_ids = ",".join(str(item["id"]) for item in batch)
            details_url = f"https://dev.azure.com/{self.organization}/_apis/wit/workitems?ids={item_ids}&$expand=relations&api-version=7.0"
            
            details_response = requests.get(details_url, headers=self.headers)
            
            if details_response.status_code != 200:
                print(f"Error fetching work item details: {details_response.status_code}")
                print(f"Response text: {details_response.text}")
                continue  # Skip this batch but continue with others
            
            batch_data = []
            for item in details_response.json()["value"]:
                fields = item["fields"]
                
                # Handle AssignedTo field properly
                assigned_to = fields.get('System.AssignedTo')
                if isinstance(assigned_to, dict):
                    assigned_to = assigned_to.get('displayName', '')
                else:
                    assigned_to = str(assigned_to) if assigned_to is not None else ''
                
                # Modified to extract first character from Severity for Issue Report type
                severity_value = fields.get('Microsoft.VSTS.Common.Severity', '')
                if work_item_type == 'Issue Report' and severity_value and len(severity_value) > 0:
                    severity_value = severity_value[0]  # Extract just the first character (e.g. "1" from "1 Critical...")
                else:
                    severity_value = fields.get('Microsoft.VSTS.Common.Priority', '')
                
                # Parse TargetDate if it exists
                target_date = fields.get('Microsoft.VSTS.Scheduling.TargetDate', '')
                if target_date:
                    try:
                        # Try parsing with milliseconds
                        target_date = datetime.strptime(target_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                    except ValueError:
                        try:
                            # Try parsing without milliseconds
                            target_date = datetime.strptime(target_date, "%Y-%m-%dT%H:%M:%SZ")
                        except ValueError:
                            try:
                                # Try parsing date only
                                target_date = datetime.strptime(target_date, "%Y-%m-%d")
                            except ValueError:
                                target_date = None
                else:
                    target_date = None

                item_data = {
                    'ID': item["id"],
                    'Title': fields.get('System.Title', ''),
                    'Description': fields.get('System.Description', ''),
                    'AssignedTo': assigned_to,
                    'Severity': severity_value,
                    'State': fields.get('System.State', ''),
                    'CustomerName': fields.get('Custom.CustomernameGRC', ''),
                    'AreaPath': fields.get('System.AreaPath', ''),
                    'CreatedDate': fields.get('System.CreatedDate', ''),
                    'ChangedDate': fields.get('System.ChangedDate', ''),
                    'IterationPath': fields.get('System.IterationPath', ''),
                    'HotfixDeliveredVersion': fields.get('Custom.HotfixDeliveredVersions', ''),
                    'TargetDate': target_date,
                    'HFStatus': fields.get('Custom.HFstatus', ''),
                    'HFRequestedVersions': fields.get('Custom.HFrequestedversions', '')
                }

                if work_item_type == 'Bug':
                    item_data['ParentID'] = fields.get('System.Parent', None)
                    # Update relations if we have a database connection
                    if hasattr(self, 'db_connection'):
                        relations = item.get("relations", [])
                        self.db_connection.update_bug_relations(item["id"], relations)

                batch_data.append(item_data)
            
            items_data.extend(batch_data)
            if len(batch) == batch_size:
                time.sleep(1)  # Rate limiting between large batches
            
        return items_data

    def fetch_comments(self, work_item_id):
        """Fetch comments for a work item"""
        comments_url = f"https://dev.azure.com/{self.organization}/{self.project}/_apis/wit/workItems/{work_item_id}/comments?api-version=7.1-preview.3"
        comments_response = requests.get(comments_url, headers=self.headers)
        
        if comments_response.status_code != 200:
            return "Error fetching comments"
            
        comments_data = comments_response.json().get("comments", [])
        return "\n".join(comment["text"] for comment in comments_data if "text" in comment) if comments_data else ""

    def export_to_csv(self, work_items, output_file):
        """Export work items to CSV file"""
        if not work_items:
            return

        with open(output_file, mode="w", newline="", encoding="utf-8") as file:
            # Get all fields from the first item
            fieldnames = list(work_items[0].keys())
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(work_items)

    def _is_jira_owned_record(self, db_connection, table_name, record_id):
        """Return True when a record has moved to Jira and ADO should stop owning history."""
        if not db_connection:
            return False
        try:
            with db_connection.engine.connect() as connection:
                result = connection.execute(
                    text(f"SELECT jira_id FROM {table_name} WHERE id = :id"),
                    {"id": record_id}
                ).first()
                return bool(result and result[0])
        except SQLAlchemyError as e:
            print(f"Warning: could not check Jira ownership for {table_name} {record_id}: {str(e)}")
            return False

    def _parse_ado_changed_date(self, changed_date):
        if not changed_date:
            return None
        for date_format in (
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
        ):
            try:
                return normalize_utc_naive(datetime.strptime(changed_date, date_format))
            except ValueError:
                continue
        return None

    def store_pre_cutover_history(self, record_id, table_name, tracked_fields, jira_id=None, db_connection=None):
        """Restore ADO change history for migrated items before the Jira cutover."""
        if not db_connection:
            return 0

        cutover = get_jira_cutover_date()
        updates_url = f"https://dev.azure.com/{self.organization}/{self.project}/_apis/wit/workitems/{record_id}/updates?api-version=7.0"
        response = requests.get(updates_url, headers=self.headers)
        if response.status_code != 200:
            print(f"Error fetching ADO history for {record_id}: {response.status_code}")
            return 0

        inserted = 0
        updates = response.json().get("value", [])
        with db_connection.engine.connect() as connection:
            try:
                connection.execute(
                    text("""
                        DELETE FROM change_history
                        WHERE record_id = :record_id
                          AND table_name = :table_name
                          AND source = 'ADO'
                          AND field_changed = ANY(:tracked_fields)
                          AND changed_date < :cutover
                    """),
                    {
                        "record_id": record_id,
                        "table_name": table_name,
                        "tracked_fields": list(tracked_fields),
                        "cutover": cutover,
                    },
                )

                for update in updates:
                    changed_date = update.get("fields", {}).get("System.ChangedDate", {}).get("newValue", "")
                    changed_date_obj = self._parse_ado_changed_date(changed_date)
                    if not changed_date_obj or changed_date_obj >= cutover:
                        continue
                    changed_by = update.get("revisedBy", {}).get("displayName", "")

                    for field_name in tracked_fields:
                        if field_name not in update.get("fields", {}):
                            continue
                        field_change = update["fields"][field_name]
                        old_value = field_change.get("oldValue", "")
                        new_value = field_change.get("newValue", "")

                        if field_name == 'Microsoft.VSTS.Scheduling.TargetDate':
                            old_value = str(old_value) if old_value else ""
                            new_value = str(new_value) if new_value else ""

                        connection.execute(
                            text("""
                                INSERT INTO change_history
                                (record_id, jira_id, source, table_name, field_changed, old_value, new_value, changed_by, changed_date)
                                VALUES
                                (:record_id, :jira_id, 'ADO', :table_name, :field_changed, :old_value, :new_value, :changed_by, :changed_date)
                            """),
                            {
                                "record_id": record_id,
                                "jira_id": jira_id,
                                "table_name": table_name,
                                "field_changed": field_name,
                                "old_value": str(old_value)[:500] if old_value else "",
                                "new_value": str(new_value)[:500] if new_value else "",
                                "changed_by": changed_by,
                                "changed_date": changed_date_obj,
                            },
                        )
                        inserted += 1

                connection.commit()
            except SQLAlchemyError as e:
                print(f"Error storing pre-cutover ADO history for {record_id}: {str(e)}")
                connection.rollback()
        return inserted

    def handle_bug_changes(self, bugs, db_connection=None):
        """
        Get and store the change history for a list of bugs (State, HF Status, HF Target Date)
        Args:
            bugs: List of bug dictionaries or bug IDs
            db_connection: Optional database connection for storing changes
        Returns:
            Dictionary mapping bug IDs to their state changes: {bug_id: [[date, old_value, new_value], ...]}
        """
        if not bugs:
            return {}

        # Convert list of bug dictionaries to list of bug IDs if necessary
        bug_ids = [bug['ID'] if isinstance(bug, dict) else bug for bug in bugs]
        all_state_changes = {}

        for bug_id in bug_ids:
            if self._is_jira_owned_record(db_connection, 'bugs', bug_id):
                jira_id = None
                if db_connection:
                    with db_connection.engine.connect() as connection:
                        row = connection.execute(
                            text("SELECT jira_id FROM bugs WHERE id = :id"),
                            {"id": bug_id},
                        ).first()
                        jira_id = row[0] if row else None
                self.store_pre_cutover_history(
                    bug_id,
                    'bugs',
                    BUG_CHANGE_HISTORY_FIELDS,
                    jira_id=jira_id,
                    db_connection=db_connection,
                )
                continue

            updates_url = f"https://dev.azure.com/{self.organization}/{self.project}/_apis/wit/workitems/{bug_id}/updates?api-version=7.0"
            response = requests.get(updates_url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"Error fetching changes for bug {bug_id}: {response.status_code}")
                continue
                
            updates = response.json().get("value", [])
            state_changes = []

            # If we have a database connection, handle the database operations
            if db_connection:
                with db_connection.engine.connect() as connection:
                    try:
                        # First, delete all existing entries for this bug
                        connection.execute(
                            text("""
                                DELETE FROM change_history 
                                WHERE record_id = :record_id 
                                AND table_name = 'bugs'
                                AND field_changed IN ('System.State', 'Custom.HFstatus', 'Microsoft.VSTS.Scheduling.TargetDate', 'Custom.HFrequestedversions')
                            """),
                            {"record_id": bug_id}
                        )
                        
                        # Define fields to track with their display names
                        fields_to_track = {
                            'System.State': 'State',
                            'Custom.HFstatus': 'HF Status',
                            'Microsoft.VSTS.Scheduling.TargetDate': 'HF Target Date',
                            'Custom.HFrequestedversions': 'HF Requested Versions'
                        }
                        
                        # Now process and insert all tracked field changes
                        for update in updates:
                            for field_name, display_name in fields_to_track.items():
                                if field_name in update.get("fields", {}):
                                    field_change = update["fields"][field_name]
                                    # Get the changed date from the update object
                                    changed_date = update.get("fields", {}).get("System.ChangedDate", {}).get("newValue", "")
                                    changed_by = update.get("revisedBy", {}).get("displayName", "")
                                    old_value = field_change.get("oldValue", "")
                                    new_value = field_change.get("newValue", "")
                                    
                                    # Convert datetime objects to strings for target date field
                                    if field_name == 'Microsoft.VSTS.Scheduling.TargetDate':
                                        if isinstance(old_value, str) and old_value:
                                            # Keep as string, will be truncated to 500 chars in DB
                                            pass
                                        elif old_value:
                                            old_value = str(old_value)
                                        else:
                                            old_value = ""
                                            
                                        if isinstance(new_value, str) and new_value:
                                            # Keep as string, will be truncated to 500 chars in DB
                                            pass
                                        elif new_value:
                                            new_value = str(new_value)
                                        else:
                                            new_value = ""

                                    # Convert date string to datetime object
                                    try:
                                        changed_date_obj = datetime.strptime(changed_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                                    except ValueError:
                                        try:
                                            changed_date_obj = datetime.strptime(changed_date, "%Y-%m-%dT%H:%M:%SZ")
                                        except ValueError:
                                            changed_date_obj = None

                                    if changed_date_obj:  # Only insert if we have a valid date
                                        # Insert the change record
                                        connection.execute(
                                            text("""
                                                INSERT INTO change_history 
                                                (record_id, table_name, field_changed, old_value, new_value, changed_by, changed_date)
                                                VALUES 
                                                (:record_id, :table_name, :field_changed, :old_value, :new_value, :changed_by, :changed_date)
                                            """),
                                            {
                                                "record_id": bug_id,
                                                "table_name": "bugs",
                                                "field_changed": field_name,
                                                "old_value": str(old_value) if old_value else "",
                                                "new_value": str(new_value) if new_value else "",
                                                "changed_by": changed_by,
                                                "changed_date": changed_date_obj
                                            }
                                        )

                                    # For backward compatibility, store state changes in the return value
                                    if field_name == 'System.State':
                                        state_changes.append([
                                            changed_date,
                                            old_value,
                                            new_value
                                        ])
                        
                        connection.commit()
                        
                    except SQLAlchemyError as e:
                        print(f"Error processing changes for bug {bug_id}: {str(e)}")
                        connection.rollback()
            else:
                # If no database connection, just collect the state changes
                for update in updates:
                    if "System.State" in update.get("fields", {}):
                        state_change = update["fields"]["System.State"]
                        changed_date = update.get("fields", {}).get("System.ChangedDate", {}).get("newValue", "")
                        old_value = state_change.get("oldValue", "")
                        new_value = state_change.get("newValue", "")

                        state_changes.append([
                            changed_date,
                            old_value,
                            new_value
                        ])
            
            all_state_changes[bug_id] = state_changes
        
        return all_state_changes

    def update_bugs_history_from_csv(self, csv_file_path, db_connection=None):
        """
        Read bug IDs from a CSV file and update their state change history
        Args:
            csv_file_path: Path to the CSV file containing bug IDs
            db_connection: Optional database connection for storing changes
        """
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                bug_ids = []
                for row in reader:
                    try:
                        bug_id = int(row['id'])
                        bug_ids.append(bug_id)
                    except (KeyError, ValueError) as e:
                        print(f"Error processing row: {row}. Error: {str(e)}")
                        continue

            if not bug_ids:
                print("No valid bug IDs found in the CSV file")
                return

            print(f"Found {len(bug_ids)} bug IDs in the CSV file")
            print("Processing state changes...")
            
            # Process state changes in batches of 50 to avoid overwhelming the API
            batch_size = 50
            for i in range(0, len(bug_ids), batch_size):
                batch = bug_ids[i:i + batch_size]
                print(f"Processing batch {i//batch_size + 1} ({len(batch)} bugs)...")
                self.handle_bug_changes(batch, db_connection)
                time.sleep(1)  # Add a small delay between batches
                
            print("Completed processing bug history updates")
            
        except FileNotFoundError:
            print(f"CSV file not found: {csv_file_path}")
        except Exception as e:
            print(f"Error processing CSV file: {str(e)}")

    def update_bugs_relations_from_csv(self, csv_file_path):
        """
        Read bug IDs from a CSV file and update their relations
        Args:
            csv_file_path: Path to the CSV file containing bug IDs
        """
        if not hasattr(self, 'db_connection'):
            print("No database connection available")
            return

        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                bug_ids = []
                for row in reader:
                    try:
                        bug_id = int(row['id'])
                        bug_ids.append(bug_id)
                    except (KeyError, ValueError) as e:
                        print(f"Error processing row: {row}. Error: {str(e)}")
                        continue

            if not bug_ids:
                print("No valid bug IDs found in the CSV file")
                return

            print(f"Found {len(bug_ids)} bug IDs in the CSV file")
            print("Processing bug relations...")
            
            # Process bugs in batches of 200 to match Azure DevOps API recommendations
            batch_size = 200
            for i in range(0, len(bug_ids), batch_size):
                batch = bug_ids[i:i + batch_size]
                print(f"Processing batch {i//batch_size + 1} ({len(batch)} bugs)...")
                
                # Get work item details with relations
                item_ids = ",".join(str(bug_id) for bug_id in batch)
                details_url = f"https://dev.azure.com/{self.organization}/_apis/wit/workitems?ids={item_ids}&$expand=relations&api-version=7.0"
                
                details_response = requests.get(details_url, headers=self.headers)
                
                if details_response.status_code != 200:
                    print(f"Error fetching work item details: {details_response.status_code}")
                    print(f"Response text: {details_response.text}")
                    continue

                # Process each work item's relations
                for item in details_response.json()["value"]:
                    bug_id = item["id"]
                    relations = item.get("relations", [])
                    self.db_connection.update_bug_relations(bug_id, relations)
                
                print(f"Completed processing relations for batch {i//batch_size + 1}")
                if len(batch) == batch_size:
                    time.sleep(1)  # Rate limiting between large batches
                
            print("Completed processing bug relations")
            
        except FileNotFoundError:
            print(f"CSV file not found: {csv_file_path}")
        except Exception as e:
            print(f"Error processing CSV file: {str(e)}")

    def update_bugs_customer_names_from_csv(self, csv_file_path):
        """
        Read bug IDs from a CSV file and update their customer names
        Args:
            csv_file_path: Path to the CSV file containing bug IDs
        """
        if not hasattr(self, 'db_connection'):
            print("No database connection available")
            return

        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                bug_ids = []
                for row in reader:
                    try:
                        bug_id = int(row['id'])
                        bug_ids.append(bug_id)
                    except (KeyError, ValueError) as e:
                        print(f"Error processing row: {row}. Error: {str(e)}")
                        continue

            if not bug_ids:
                print("No valid bug IDs found in the CSV file")
                return

            print(f"Found {len(bug_ids)} bug IDs in the CSV file")
            print("Processing bug customer names...")
            
            # Process bugs in batches of 200 to match Azure DevOps API recommendations
            batch_size = 200
            for i in range(0, len(bug_ids), batch_size):
                batch = bug_ids[i:i + batch_size]
                print(f"Processing batch {i//batch_size + 1} ({len(batch)} bugs)...")
                
                # Get work item details
                item_ids = ",".join(str(bug_id) for bug_id in batch)
                details_url = f"https://dev.azure.com/{self.organization}/_apis/wit/workitems?ids={item_ids}&api-version=7.0"
                
                details_response = requests.get(details_url, headers=self.headers)
                
                if details_response.status_code != 200:
                    print(f"Error fetching work item details: {details_response.status_code}")
                    print(f"Response text: {details_response.text}")
                    continue

                # Process each work item's customer name
                with self.db_connection.engine.connect() as connection:
                    for item in details_response.json()["value"]:
                        bug_id = item["id"]
                        fields = item["fields"]
                        customer_name = fields.get('Custom.CustomernameGRC', '')
                        
                        print(f"Bug {bug_id} - Customer Name: {customer_name}")
                        
                        try:
                            # Update the customer name in the database
                            connection.execute(
                                text("""
                                    UPDATE bugs 
                                    SET customer_name = :customer_name 
                                    WHERE id = :bug_id
                                """),
                                {
                                    "bug_id": bug_id,
                                    "customer_name": customer_name
                                }
                            )
                            connection.commit()
                        except SQLAlchemyError as e:
                            print(f"Error updating customer name for bug {bug_id}: {str(e)}")
                            connection.rollback()
                
                print(f"Completed processing customer names for batch {i//batch_size + 1}")
                if len(batch) == batch_size:
                    time.sleep(1)  # Rate limiting between large batches
                
            print("Completed processing bug customer names")
            
        except FileNotFoundError:
            print(f"CSV file not found: {csv_file_path}")
        except Exception as e:
            print(f"Error processing CSV file: {str(e)}")

    def update_issues_customer_names_from_csv(self, csv_file_path):
        """
        Read issue IDs from a CSV file and update their customer names
        Args:
            csv_file_path: Path to the CSV file containing issue IDs
        """
        if not hasattr(self, 'db_connection'):
            print("No database connection available")
            return

        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                issue_ids = []
                for row in reader:
                    try:
                        issue_id = int(row['id'])
                        issue_ids.append(issue_id)
                    except (KeyError, ValueError) as e:
                        print(f"Error processing row: {row}. Error: {str(e)}")
                        continue

            if not issue_ids:
                print("No valid issue IDs found in the CSV file")
                return

            print(f"Found {len(issue_ids)} issue IDs in the CSV file")
            print("Processing issue customer names...")
            
            # Process issues in batches of 200 to match Azure DevOps API recommendations
            batch_size = 200
            for i in range(0, len(issue_ids), batch_size):
                batch = issue_ids[i:i + batch_size]
                print(f"Processing batch {i//batch_size + 1} ({len(batch)} issues)...")
                
                # Get work item details
                item_ids = ",".join(str(issue_id) for issue_id in batch)
                details_url = f"https://dev.azure.com/{self.organization}/_apis/wit/workitems?ids={item_ids}&api-version=7.0"
                
                details_response = requests.get(details_url, headers=self.headers)
                
                if details_response.status_code != 200:
                    print(f"Error fetching work item details: {details_response.status_code}")
                    print(f"Response text: {details_response.text}")
                    continue

                # Process each work item's customer name
                with self.db_connection.engine.connect() as connection:
                    for item in details_response.json()["value"]:
                        issue_id = item["id"]
                        fields = item["fields"]
                        customer_name = fields.get('Custom.CustomernameGRC', '')
                        
                        print(f"Issue {issue_id} - Customer Name: {customer_name}")
                        
                        try:
                            # Update the customer name in the database
                            connection.execute(
                                text("""
                                    UPDATE issues 
                                    SET customer_name = :customer_name 
                                    WHERE id = :issue_id
                                """),
                                {
                                    "issue_id": issue_id,
                                    "customer_name": customer_name
                                }
                            )
                            connection.commit()
                        except SQLAlchemyError as e:
                            print(f"Error updating customer name for issue {issue_id}: {str(e)}")
                            connection.rollback()
                
                print(f"Completed processing customer names for batch {i//batch_size + 1}")
                if len(batch) == batch_size:
                    time.sleep(1)  # Rate limiting between large batches
                
            print("Completed processing issue customer names")
            
        except FileNotFoundError:
            print(f"CSV file not found: {csv_file_path}")
        except Exception as e:
            print(f"Error processing CSV file: {str(e)}")

    def get_all_work_items(self, last_update):
        """Query all work items regardless of type that were changed since last update"""
        query_url = f"https://dev.azure.com/{self.organization}/{self.project}/_apis/wit/wiql?api-version=7.1-preview.2"
        
        # Handle last_update whether it's a string or datetime
        if isinstance(last_update, datetime):
            last_update_str = last_update.strftime('%Y-%m-%d')
        else:
            last_update_str = last_update
        
        print(f"------>Fetching all work items with ChangedDate >= {last_update_str}")
        
        # Modified WIQL query to include all required fields without type filter
        query_payload = {
            "query": f"""
                SELECT [System.Id],
                       [System.Title],
                       [System.State],
                       [System.CreatedDate],
                       [System.ChangedDate],
                       [System.AssignedTo],
                       [System.Description],
                       [Microsoft.VSTS.Common.Priority],
                       [Microsoft.VSTS.Common.Severity],
                       [Custom.CustomernameGRC],
                       [System.AreaPath],
                       [System.Parent],
                       [System.IterationPath],
                       [Custom.HotfixDeliveredVersions],
                       [System.WorkItemType],
                       [Microsoft.VSTS.Scheduling.TargetDate],
                       [Custom.HFstatus],
                       [Custom.HFrequestedversions],
                       [Microsoft.VSTS.Scheduling.Effort],
                       [Custom.EffortDevestimate],
                       [Custom.EffortDevactual],
                       [Custom.QAeffortestimation],
                       [Custom.QAeffortactual],
                       [Custom.TShirtestimation],
                       [Custom.Tickettype],
                       [Custom.FreshdeskTicket],
                       [Custom.Targetversion],
                       [System.Tags],
                       [Custom.Connector],
                       [System.CreatedBy],
                       [Custom.Blocker],
                       [Custom.BusinessOutcome],
                       [Microsoft.VSTS.Common.BusinessValue]
                FROM WorkItems
                WHERE [System.ChangedDate] >= '{last_update_str}'
                ORDER BY [System.ChangedDate] DESC
            """
        }

        # Get work item IDs
        response = requests.post(query_url, json=query_payload, headers=self.headers)
        if response.status_code != 200:
            print(f"Error fetching WIQL query: {response.status_code} - {response.text}")
            return []

        work_items = response.json().get("workItems", [])
        if not work_items:
            return []

        # Get work item details in batches
        items_data = []
        batch_size = 200  # Azure DevOps recommended batch size
        
        for i in range(0, len(work_items), batch_size):
            batch = work_items[i:i + batch_size]
            item_ids = ",".join(str(item["id"]) for item in batch)
            details_url = f"https://dev.azure.com/{self.organization}/_apis/wit/workitems?ids={item_ids}&$expand=relations&api-version=7.0"
            
            details_response = requests.get(details_url, headers=self.headers)
            
            if details_response.status_code != 200:
                print(f"Error fetching work item details: {details_response.status_code}")
                print(f"Response text: {details_response.text}")
                continue
            
            batch_data = []
            for item in details_response.json()["value"]:
                fields = item["fields"]
                
                # Handle AssignedTo field properly
                assigned_to = fields.get('System.AssignedTo')
                if isinstance(assigned_to, dict):
                    assigned_to = assigned_to.get('displayName', '')
                else:
                    assigned_to = str(assigned_to) if assigned_to is not None else ''
                
                # Modified to extract first character from Severity for Issue Report type
                work_item_type = fields.get('System.WorkItemType', '')
                severity_value = fields.get('Microsoft.VSTS.Common.Severity', '')
                if work_item_type == 'Issue Report' and severity_value and len(severity_value) > 0:
                    severity_value = severity_value[0]  # Extract just the first character (e.g. "1" from "1 Critical...")
                else:
                    severity_value = fields.get('Microsoft.VSTS.Common.Priority', '')
                
                # Parse TargetDate if it exists
                target_date = fields.get('Microsoft.VSTS.Scheduling.TargetDate', '')
                if target_date:
                    try:
                        # Try parsing with milliseconds
                        target_date = datetime.strptime(target_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                    except ValueError:
                        try:
                            # Try parsing without milliseconds
                            target_date = datetime.strptime(target_date, "%Y-%m-%dT%H:%M:%SZ")
                        except ValueError:
                            try:
                                # Try parsing date only
                                target_date = datetime.strptime(target_date, "%Y-%m-%d")
                            except ValueError:
                                target_date = None
                else:
                    target_date = None

                # Parse effort fields (they might be numbers or None)
                effort = fields.get('Microsoft.VSTS.Scheduling.Effort')
                try:
                    effort = float(effort) if effort is not None else None
                except (ValueError, TypeError):
                    effort = None
                
                effort_dev_estimate = fields.get('Custom.EffortDevestimate')
                try:
                    effort_dev_estimate = float(effort_dev_estimate) if effort_dev_estimate is not None else None
                except (ValueError, TypeError):
                    effort_dev_estimate = None
                
                effort_dev_actual = fields.get('Custom.EffortDevactual')
                try:
                    effort_dev_actual = float(effort_dev_actual) if effort_dev_actual is not None else None
                except (ValueError, TypeError):
                    effort_dev_actual = None
                
                qa_effort_estimation = fields.get('Custom.QAeffortestimation')
                try:
                    qa_effort_estimation = float(qa_effort_estimation) if qa_effort_estimation is not None else None
                except (ValueError, TypeError):
                    qa_effort_estimation = None
                
                qa_effort_actual = fields.get('Custom.QAeffortactual')
                try:
                    qa_effort_actual = float(qa_effort_actual) if qa_effort_actual is not None else None
                except (ValueError, TypeError):
                    qa_effort_actual = None

                # Handle CreatedBy field properly
                created_by = fields.get('System.CreatedBy')
                if isinstance(created_by, dict):
                    created_by = created_by.get('displayName', '')
                else:
                    created_by = str(created_by) if created_by is not None else ''
                
                # Parse BusinessValue as integer
                business_value = fields.get('Microsoft.VSTS.Common.BusinessValue')
                try:
                    business_value = int(business_value) if business_value is not None else None
                except (ValueError, TypeError):
                    business_value = None
                
                item_data = {
                    'ID': item["id"],
                    'Title': fields.get('System.Title', ''),
                    'Description': fields.get('System.Description', ''),
                    'AssignedTo': assigned_to,
                    'Severity': severity_value,
                    'State': fields.get('System.State', ''),
                    'CustomerName': fields.get('Custom.CustomernameGRC', ''),
                    'AreaPath': fields.get('System.AreaPath', ''),
                    'CreatedDate': fields.get('System.CreatedDate', ''),
                    'ChangedDate': fields.get('System.ChangedDate', ''),
                    'IterationPath': fields.get('System.IterationPath', ''),
                    'HotfixDeliveredVersion': fields.get('Custom.HotfixDeliveredVersions', ''),
                    'WorkItemType': work_item_type,
                    'TargetDate': target_date,
                    'HFStatus': fields.get('Custom.HFstatus', ''),
                    'HFRequestedVersions': fields.get('Custom.HFrequestedversions', ''),
                    'Effort': effort,
                    'EffortDevEstimate': effort_dev_estimate,
                    'EffortDevActual': effort_dev_actual,
                    'QAEffortEstimation': qa_effort_estimation,
                    'QAEffortActual': qa_effort_actual,
                    'TShirtEstimation': fields.get('Custom.TShirtestimation', ''),
                    'ParentWorkItem': fields.get('System.Parent', None),
                    'TicketType': fields.get('Custom.Tickettype', ''),
                    'FreshdeskTicket': fields.get('Custom.FreshdeskTicket', ''),
                    'TargetVersion': fields.get('Custom.Targetversion', ''),
                    'Tags': (fields.get('System.Tags', '') or '')[:500],
                    'Connector': (fields.get('Custom.Connector', '') or '')[:1000],
                    'CreatedBy': created_by,
                    'Blocker': fields.get('Custom.Blocker', ''),
                    'BusinessOutcome': fields.get('Custom.BusinessOutcome', ''),
                    'BusinessValue': business_value
                }

                batch_data.append(item_data)
            
            items_data.extend(batch_data)
            if len(batch) == batch_size:
                time.sleep(1)  # Rate limiting between large batches
            
        return items_data

    def handle_work_item_changes(self, work_items, db_connection=None):
        """
        Get and store the state change history for a list of work items
        Args:
            work_items: List of work item dictionaries or work item IDs
            db_connection: Optional database connection for storing changes
        Returns:
            Dictionary mapping work item IDs to their state changes: {work_item_id: [[date, old_value, new_value], ...]}
        """
        if not work_items:
            return {}

        # Convert list of work item dictionaries to list of work item IDs if necessary
        work_item_ids = [item['ID'] if isinstance(item, dict) else item for item in work_items]
        all_state_changes = {}

        for work_item_id in work_item_ids:
            if self._is_jira_owned_record(db_connection, 'work_items', work_item_id):
                jira_id = None
                work_item_type = None
                if db_connection:
                    with db_connection.engine.connect() as connection:
                        row = connection.execute(
                            text("SELECT jira_id, work_item_type FROM work_items WHERE id = :id"),
                            {"id": work_item_id},
                        ).first()
                        if row:
                            jira_id, work_item_type = row[0], row[1]
                if work_item_type and work_item_type != 'Bug':
                    self.store_pre_cutover_history(
                        work_item_id,
                        work_item_type,
                        WORK_ITEM_CHANGE_HISTORY_FIELDS,
                        jira_id=jira_id,
                        db_connection=db_connection,
                    )
                continue

            # First get the work item details to ensure we have the type
            details_url = f"https://dev.azure.com/{self.organization}/_apis/wit/workitems/{work_item_id}?api-version=7.0"
            details_response = requests.get(details_url, headers=self.headers)
            
            if details_response.status_code != 200:
                print(f"Error fetching work item details for {work_item_id}: {details_response.status_code}")
                continue

            work_item_type = details_response.json()["fields"].get("System.WorkItemType")
            if not work_item_type:
                print(f"Could not determine work item type for {work_item_id}")
                continue

            updates_url = f"https://dev.azure.com/{self.organization}/{self.project}/_apis/wit/workitems/{work_item_id}/updates?api-version=7.0"
            response = requests.get(updates_url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"Error fetching state changes for work item {work_item_id}: {response.status_code}")
                continue
                
            updates = response.json().get("value", [])
            state_changes = []

            # If we have a database connection, handle the database operations
            if db_connection:
                with db_connection.engine.connect() as connection:
                    try:
                        # First, check if the work item exists and get its type from the database
                        result = connection.execute(
                            text("SELECT work_item_type FROM work_items WHERE id = :id"),
                            {"id": work_item_id}
                        ).first()
                        
                        if not result:
                            # Work item not in database, let's insert it
                            fields = details_response.json()["fields"]
                            
                            # Handle AssignedTo field properly
                            assigned_to = fields.get('System.AssignedTo')
                            if isinstance(assigned_to, dict):
                                assigned_to = assigned_to.get('displayName', '')
                            else:
                                assigned_to = str(assigned_to) if assigned_to is not None else ''

                            # Handle the severity field appropriately
                            severity_value = fields.get('Microsoft.VSTS.Common.Severity', '')
                            if work_item_type == 'Issue Report' and severity_value and len(severity_value) > 0:
                                severity_value = severity_value[0]  # Extract just the first character
                            else:
                                severity_value = fields.get('Microsoft.VSTS.Common.Priority', '')

                            # Parse TargetDate if it exists
                            target_date = fields.get('Microsoft.VSTS.Scheduling.TargetDate', '')
                            if target_date:
                                try:
                                    # Try parsing with milliseconds
                                    target_date = datetime.strptime(target_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                                except ValueError:
                                    try:
                                        # Try parsing without milliseconds
                                        target_date = datetime.strptime(target_date, "%Y-%m-%dT%H:%M:%SZ")
                                    except ValueError:
                                        try:
                                            # Try parsing date only
                                            target_date = datetime.strptime(target_date, "%Y-%m-%d")
                                        except ValueError:
                                            target_date = None
                            else:
                                target_date = None

                            # Parse effort fields (they might be numbers or None)
                            effort = fields.get('Microsoft.VSTS.Scheduling.Effort')
                            try:
                                effort = float(effort) if effort is not None else None
                            except (ValueError, TypeError):
                                effort = None
                            
                            effort_dev_estimate = fields.get('Custom.EffortDevestimate')
                            try:
                                effort_dev_estimate = float(effort_dev_estimate) if effort_dev_estimate is not None else None
                            except (ValueError, TypeError):
                                effort_dev_estimate = None
                            
                            effort_dev_actual = fields.get('Custom.EffortDevactual')
                            try:
                                effort_dev_actual = float(effort_dev_actual) if effort_dev_actual is not None else None
                            except (ValueError, TypeError):
                                effort_dev_actual = None
                            
                            qa_effort_estimation = fields.get('Custom.QAeffortestimation')
                            try:
                                qa_effort_estimation = float(qa_effort_estimation) if qa_effort_estimation is not None else None
                            except (ValueError, TypeError):
                                qa_effort_estimation = None
                            
                            qa_effort_actual = fields.get('Custom.QAeffortactual')
                            try:
                                qa_effort_actual = float(qa_effort_actual) if qa_effort_actual is not None else None
                            except (ValueError, TypeError):
                                qa_effort_actual = None

                            # Parse BusinessValue as integer
                            business_value = fields.get('Microsoft.VSTS.Common.BusinessValue')
                            try:
                                business_value = int(business_value) if business_value is not None else None
                            except (ValueError, TypeError):
                                business_value = None
                            
                            # Handle CreatedBy field properly
                            created_by_field = fields.get('System.CreatedBy')
                            if isinstance(created_by_field, dict):
                                created_by_field = created_by_field.get('displayName', '')
                            else:
                                created_by_field = str(created_by_field) if created_by_field is not None else ''
                            
                            # Insert the work item
                            connection.execute(
                                text("""
                                    INSERT INTO work_items (
                                        id, title, description, assigned_to, severity,
                                        state, customer_name, area_path, created_date, changed_date,
                                        iteration_path, hotfix_delivered_version, work_item_type, target_date, hf_status, hf_requested_versions,
                                        effort, effort_dev_estimate, effort_dev_actual, qa_effort_estimation, qa_effort_actual, tshirt_estimation, parent_work_item, ticket_type,
                                        freshdesk_ticket, target_version, tags, connector, created_by, blocker, business_value, business_outcome
                                    ) VALUES (
                                        :id, :title, :description, :assigned_to, :severity,
                                        :state, :customer_name, :area_path, :created_date, :changed_date,
                                        :iteration_path, :hotfix_delivered_version, :work_item_type, :target_date, :hf_status, :hf_requested_versions,
                                        :effort, :effort_dev_estimate, :effort_dev_actual, :qa_effort_estimation, :qa_effort_actual, :tshirt_estimation, :parent_work_item, :ticket_type,
                                        :freshdesk_ticket, :target_version, :tags, :connector, :created_by, :blocker, :business_value, :business_outcome
                                    )
                                """),
                                {
                                    "id": work_item_id,
                                    "title": fields.get('System.Title', ''),
                                    "description": fields.get('System.Description', ''),
                                    "assigned_to": assigned_to,
                                    "severity": severity_value,
                                    "state": fields.get('System.State', ''),
                                    "customer_name": fields.get('Custom.CustomernameGRC', ''),
                                    "area_path": fields.get('System.AreaPath', ''),
                                    "created_date": fields.get('System.CreatedDate', ''),
                                    "changed_date": fields.get('System.ChangedDate', ''),
                                    "iteration_path": fields.get('System.IterationPath', ''),
                                    "hotfix_delivered_version": fields.get('Custom.HotfixDeliveredVersions', ''),
                                    "work_item_type": work_item_type,
                                    "target_date": target_date,
                                    "hf_status": fields.get('Custom.HFstatus', ''),
                                    "hf_requested_versions": fields.get('Custom.HFrequestedversions', ''),
                                    "effort": effort,
                                    "effort_dev_estimate": effort_dev_estimate,
                                    "effort_dev_actual": effort_dev_actual,
                                    "qa_effort_estimation": qa_effort_estimation,
                                    "qa_effort_actual": qa_effort_actual,
                                    "tshirt_estimation": fields.get('Custom.TShirtestimation', ''),
                                    "parent_work_item": fields.get('System.Parent', None),
                                    "ticket_type": fields.get('Custom.Tickettype', ''),
                                    "freshdesk_ticket": fields.get('Custom.FreshdeskTicket', ''),
                                    "target_version": fields.get('Custom.Targetversion', ''),
                                    "tags": (fields.get('System.Tags', '') or '')[:500],
                                    "connector": (fields.get('Custom.Connector', '') or '')[:1000],
                                    "created_by": created_by_field,
                                    "blocker": fields.get('Custom.Blocker', ''),
                                    "business_value": business_value,
                                    "business_outcome": fields.get('Custom.BusinessOutcome', '')
                                }
                            )
                            print(f"Inserted missing work item {work_item_id} of type {work_item_type}")

                        # Delete all existing entries for this work item
                        connection.execute(
                            text("""
                                DELETE FROM change_history 
                                WHERE record_id = :record_id 
                                AND table_name = :table_name
                                AND field_changed IN ('System.State', 'System.IterationPath')
                            """),
                            {
                                "record_id": work_item_id,
                                "table_name": work_item_type
                            }
                        )
                        
                        # Now process and insert all state and iteration path changes
                        for update in updates:
                            fields_to_track = {'System.State': 'State', 'System.IterationPath': 'Iteration Path'}
                            
                            for field_name, display_name in fields_to_track.items():
                                if field_name in update.get("fields", {}):
                                    field_change = update["fields"][field_name]
                                    # Get the changed date from the update object
                                    changed_date = update.get("fields", {}).get("System.ChangedDate", {}).get("newValue", "")
                                    changed_by = update.get("revisedBy", {}).get("displayName", "")
                                    old_value = field_change.get("oldValue", "")
                                    new_value = field_change.get("newValue", "")

                                    # Convert date string to datetime object
                                    try:
                                        changed_date_obj = datetime.strptime(changed_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                                    except ValueError:
                                        try:
                                            changed_date_obj = datetime.strptime(changed_date, "%Y-%m-%dT%H:%M:%SZ")
                                        except ValueError:
                                            changed_date_obj = None

                                    if changed_date_obj:  # Only insert if we have a valid date
                                        # Insert the change record
                                        connection.execute(
                                            text("""
                                                INSERT INTO change_history 
                                                (record_id, table_name, field_changed, old_value, new_value, changed_by, changed_date)
                                                VALUES 
                                                (:record_id, :table_name, :field_changed, :old_value, :new_value, :changed_by, :changed_date)
                                            """),
                                            {
                                                "record_id": work_item_id,
                                                "table_name": work_item_type,
                                                "field_changed": field_name,
                                                "old_value": old_value,
                                                "new_value": new_value,
                                                "changed_by": changed_by,
                                                "changed_date": changed_date_obj
                                            }
                                        )

                                    # For backward compatibility, store state changes in the return value
                                    if field_name == 'System.State':
                                        state_changes.append([
                                            changed_date,
                                            old_value,
                                            new_value
                                        ])
                        
                        connection.commit()
                        
                    except SQLAlchemyError as e:
                        print(f"Error processing state changes for work item {work_item_id}: {str(e)}")
                        connection.rollback()
            else:
                # If no database connection, just collect the state changes
                for update in updates:
                    if "System.State" in update.get("fields", {}):
                        state_change = update["fields"]["System.State"]
                        changed_date = update.get("fields", {}).get("System.ChangedDate", {}).get("newValue", "")
                        old_value = state_change.get("oldValue", "")
                        new_value = state_change.get("newValue", "")

                        state_changes.append([
                            changed_date,
                            old_value,
                            new_value
                        ])
            
            all_state_changes[work_item_id] = state_changes
        
        return all_state_changes

    def update_sprints(self):
        """Fetch all sprints from Azure DevOps and update the database"""
        print("------>Fetching sprints from Azure DevOps")
        
        all_sprints = []
        total_processed = 0
        
        # Iterate through all scrum project:team combinations
        for scrum_config in self.scrum_projects:
            project_name = scrum_config['project']
            team_name = scrum_config['team']
            
            print(f"------>Processing sprints for project: {project_name}, team: {team_name}")
            
            # Get teams for this project
            teams_url = f"https://dev.azure.com/{self.organization}/_apis/projects/{project_name}/teams?api-version=7.0"
            teams_response = requests.get(teams_url, headers=self.headers)
            
            if teams_response.status_code != 200:
                print(f"Error fetching teams for project {project_name}: {teams_response.status_code} - {teams_response.text}")
                continue
            
            teams = teams_response.json().get("value", [])
            if not teams:
                print(f"No teams found in project {project_name}")
                continue
            
            # Find the specific team if specified, otherwise use the first team
            target_team = None
            if team_name:
                # Look for the specific team
                for team in teams:
                    if team["name"].lower() == team_name.lower():
                        target_team = team
                        break
                
                if not target_team:
                    print(f"Team '{team_name}' not found in project {project_name}. Available teams: {[t['name'] for t in teams]}")
                    continue
            else:
                # Use the first team if no specific team is specified
                target_team = teams[0]
                print(f"Using first team: {target_team['name']}")
            
            team_id = target_team["id"]
            actual_team_name = target_team["name"]
            print(f"------>Fetching sprints for team: {actual_team_name} in project: {project_name}")
            
            # Get iterations (sprints) for this team
            iterations_url = f"https://dev.azure.com/{self.organization}/{project_name}/{team_id}/_apis/work/teamsettings/iterations?api-version=7.0"
            iterations_response = requests.get(iterations_url, headers=self.headers)
            
            if iterations_response.status_code != 200:
                print(f"Error fetching iterations for team {actual_team_name} in project {project_name}: {iterations_response.status_code}")
                continue
            
            iterations = iterations_response.json().get("value", [])
            print(f"------>Found {len(iterations)} iterations for team {actual_team_name}")
            
            for iteration in iterations:
                # Get detailed iteration information
                iteration_id = iteration["id"]
                iteration_detail_url = f"https://dev.azure.com/{self.organization}/{project_name}/{team_id}/_apis/work/teamsettings/iterations/{iteration_id}?api-version=7.0"
                detail_response = requests.get(iteration_detail_url, headers=self.headers)
                
                if detail_response.status_code != 200:
                    print(f"Error fetching iteration details for {iteration['name']}: {detail_response.status_code}")
                    continue
                
                iteration_detail = detail_response.json()
                attributes = iteration_detail.get("attributes", {})
                
                # Parse dates
                start_date = None
                finish_date = None
                
                if "startDate" in attributes and attributes["startDate"]:
                    try:
                        start_date = datetime.strptime(attributes["startDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    except ValueError:
                        try:
                            start_date = datetime.strptime(attributes["startDate"], "%Y-%m-%dT%H:%M:%SZ")
                        except ValueError:
                            print(f"Could not parse start date: {attributes['startDate']}")
                
                if "finishDate" in attributes and attributes["finishDate"]:
                    try:
                        finish_date = datetime.strptime(attributes["finishDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    except ValueError:
                        try:
                            finish_date = datetime.strptime(attributes["finishDate"], "%Y-%m-%dT%H:%M:%SZ")
                        except ValueError:
                            print(f"Could not parse finish date: {attributes['finishDate']}")
                
                sprint_data = {
                    'id': iteration_detail["id"],
                    'name': iteration_detail["name"],
                    'path': iteration_detail["path"],
                    'start_date': start_date,
                    'finish_date': finish_date,
                    'state': attributes.get("timeFrame", "")
                }
                
                all_sprints.append(sprint_data)
        
        # Update database with sprints
        if hasattr(self, 'db_connection') and self.db_connection:
            processed_count = self.db_connection.upsert_sprints(all_sprints)
            print(f"------>Processed {processed_count} sprints from {len(self.scrum_projects)} project:team combinations")
            return processed_count
        else:
            print("------>No database connection available for sprints update")
            return 0

    def update_sprint_capacities(self, current_sprint_only=True):
        """
        Fetch and store sprint capacity data from Azure DevOps.
        
        Args:
            current_sprint_only: If True, only fetch capacity for current sprints.
                               If False, fetch for all sprints (used for initial sync).
        """
        all_capacities = []
        
        for scrum_config in self.scrum_projects:
            project_name = scrum_config['project']
            team_name = scrum_config['team']
            
            print(f"------>Processing sprint capacities for project: {project_name}, team: {team_name}")
            
            # Get teams for this project
            teams_url = f"https://dev.azure.com/{self.organization}/_apis/projects/{project_name}/teams?api-version=7.0"
            teams_response = requests.get(teams_url, headers=self.headers)
            
            if teams_response.status_code != 200:
                print(f"Error fetching teams for project {project_name}: {teams_response.status_code} - {teams_response.text}")
                continue
            
            teams = teams_response.json().get("value", [])
            if not teams:
                print(f"No teams found in project {project_name}")
                continue
            
            # Find the specific team if specified, otherwise use the first team
            target_team = None
            if team_name:
                for team in teams:
                    if team["name"].lower() == team_name.lower():
                        target_team = team
                        break
                
                if not target_team:
                    print(f"Team '{team_name}' not found in project {project_name}. Available teams: {[t['name'] for t in teams]}")
                    continue
            else:
                target_team = teams[0]
                print(f"Using first team: {target_team['name']}")
            
            team_id = target_team["id"]
            actual_team_name = target_team["name"]
            print(f"------>Fetching sprint capacities for team: {actual_team_name} in project: {project_name}")
            
            # Get iterations (sprints) for this team
            iterations_url = f"https://dev.azure.com/{self.organization}/{project_name}/{team_id}/_apis/work/teamsettings/iterations?api-version=7.0"
            iterations_response = requests.get(iterations_url, headers=self.headers)
            
            if iterations_response.status_code != 200:
                print(f"Error fetching iterations for team {actual_team_name} in project {project_name}: {iterations_response.status_code}")
                continue
            
            iterations = iterations_response.json().get("value", [])
            print(f"------>Found {len(iterations)} iterations for team {actual_team_name}")
            
            # Filter iterations based on current_sprint_only flag
            iterations_to_process = []
            current_date = datetime.now()
            
            for iteration in iterations:
                # Get detailed iteration information to check dates
                iteration_id = iteration["id"]
                iteration_detail_url = f"https://dev.azure.com/{self.organization}/{project_name}/{team_id}/_apis/work/teamsettings/iterations/{iteration_id}?api-version=7.0"
                detail_response = requests.get(iteration_detail_url, headers=self.headers)
                
                if detail_response.status_code != 200:
                    continue
                
                iteration_detail = detail_response.json()
                attributes = iteration_detail.get("attributes", {})
                time_frame = attributes.get("timeFrame", "")
                
                # Parse dates for filtering
                start_date = None
                finish_date = None
                
                if "startDate" in attributes and attributes["startDate"]:
                    try:
                        start_date = datetime.strptime(attributes["startDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    except ValueError:
                        try:
                            start_date = datetime.strptime(attributes["startDate"], "%Y-%m-%dT%H:%M:%SZ")
                        except ValueError:
                            pass
                
                if "finishDate" in attributes and attributes["finishDate"]:
                    try:
                        finish_date = datetime.strptime(attributes["finishDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    except ValueError:
                        try:
                            finish_date = datetime.strptime(attributes["finishDate"], "%Y-%m-%dT%H:%M:%SZ")
                        except ValueError:
                            pass
                
                # Decide whether to include this iteration
                if current_sprint_only:
                    # Only include current sprint (timeFrame == "current" or dates encompass today)
                    is_current = time_frame == "current"
                    is_date_current = (start_date and finish_date and 
                                      start_date <= current_date <= finish_date)
                    if is_current or is_date_current:
                        iterations_to_process.append({
                            'iteration': iteration,
                            'detail': iteration_detail
                        })
                else:
                    # Include all iterations for initial full sync
                    iterations_to_process.append({
                        'iteration': iteration,
                        'detail': iteration_detail
                    })
            
            print(f"------>Processing capacities for {len(iterations_to_process)} iteration(s)")
            
            for iter_data in iterations_to_process:
                iteration = iter_data['iteration']
                iteration_detail = iter_data['detail']
                iteration_id = iteration["id"]
                iteration_name = iteration["name"]
                
                # Fetch capacity for this iteration - use team name (URL encoded) instead of team_id
                team_name_encoded = requests.utils.quote(actual_team_name)
                capacity_url = f"https://dev.azure.com/{self.organization}/{project_name}/{team_name_encoded}/_apis/work/teamsettings/iterations/{iteration_id}/capacities?api-version=7.0"
                capacity_response = requests.get(capacity_url, headers=self.headers)
                
                if capacity_response.status_code != 200:
                    print(f"Error fetching capacity for iteration {iteration_name}: {capacity_response.status_code} - {capacity_response.text}")
                    continue
                
                response_json = capacity_response.json()
                # API returns data in 'teamMembers' key, not 'value'
                capacity_data = response_json.get("teamMembers", [])
                print(f"------>Found {len(capacity_data)} team member capacity records for sprint: {iteration_name}")
                
                for member_capacity in capacity_data:
                    team_member = member_capacity.get("teamMember", {})
                    team_member_id = team_member.get("id", "")
                    team_member_name = team_member.get("displayName", "Unknown")
                    
                    activities = member_capacity.get("activities", [])
                    days_off = member_capacity.get("daysOff", [])
                    
                    # Calculate days off count and date range
                    days_off_count = 0
                    days_off_start = None
                    days_off_end = None
                    
                    if days_off:
                        for day_off in days_off:
                            start_str = day_off.get("start", "")
                            end_str = day_off.get("end", "")
                            
                            if start_str:
                                try:
                                    start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                                except ValueError:
                                    try:
                                        start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%SZ")
                                    except ValueError:
                                        start_dt = None
                                
                                if start_dt and (days_off_start is None or start_dt < days_off_start):
                                    days_off_start = start_dt
                            
                            if end_str:
                                try:
                                    end_dt = datetime.strptime(end_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                                except ValueError:
                                    try:
                                        end_dt = datetime.strptime(end_str, "%Y-%m-%dT%H:%M:%SZ")
                                    except ValueError:
                                        end_dt = None
                                
                                if end_dt and (days_off_end is None or end_dt > days_off_end):
                                    days_off_end = end_dt
                        
                        # Calculate total days off
                        if days_off_start and days_off_end:
                            days_off_count = (days_off_end - days_off_start).days + 1
                    
                    # Process activities - each activity gets its own record
                    if activities:
                        for activity in activities:
                            activity_name = activity.get("name", "")
                            capacity_per_day = activity.get("capacityPerDay", 0)
                            
                            capacity_record = {
                                'sprint_id': iteration_id,
                                'sprint_name': iteration_name,
                                'team_member_id': team_member_id,
                                'team_member_name': team_member_name,
                                'activity': activity_name if activity_name else None,
                                'capacity_per_day': float(capacity_per_day) if capacity_per_day else 0,
                                'days_off_count': days_off_count,
                                'days_off_start': days_off_start,
                                'days_off_end': days_off_end,
                                'project_name': project_name,
                                'team_name': actual_team_name
                            }
                            all_capacities.append(capacity_record)
                    else:
                        # No activities defined, still record the team member with null activity
                        capacity_record = {
                            'sprint_id': iteration_id,
                            'sprint_name': iteration_name,
                            'team_member_id': team_member_id,
                            'team_member_name': team_member_name,
                            'activity': None,
                            'capacity_per_day': 0,
                            'days_off_count': days_off_count,
                            'days_off_start': days_off_start,
                            'days_off_end': days_off_end,
                            'project_name': project_name,
                            'team_name': actual_team_name
                        }
                        all_capacities.append(capacity_record)
        
        # Update database with capacities
        if hasattr(self, 'db_connection') and self.db_connection:
            processed_count = self.db_connection.upsert_sprint_capacities(all_capacities)
            print(f"------>Processed {processed_count} sprint capacity records from {len(self.scrum_projects)} project:team combinations")
            return processed_count
        else:
            print("------>No database connection available for sprint capacities update")
            return 0

class JIRAExtractor:
    """Jira implementation that emits ADO-compatible item dictionaries."""

    def __init__(self, jira_url, project_key, email, api_token, board_id=None, board_ids=None):
        self.jira_url = jira_url.rstrip('/')
        self.project_key = project_key
        self.board_id = board_id
        self.board_ids = board_ids
        self._boards_cache = None
        auth_string = f"{email}:{api_token}"
        auth_b64 = base64.b64encode(auth_string.encode('ascii')).decode('ascii')
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Basic {auth_b64}"
        }

        self.sprint_field = os.getenv('JIRA_SPRINT_FIELD_ID', 'customfield_10020')
        self.area_path_field = os.getenv('JIRA_AREA_PATH_FIELD_ID', 'customfield_12494')
        self.story_points_field = os.getenv('JIRA_STORY_POINTS_FIELD_ID', 'customfield_10037')
        self._area_path_cache = {}
        self.ado_work_item_id_field = os.getenv('JIRA_ADO_WORK_ITEM_ID_FIELD_ID', 'customfield_12495')
        self.ado_created_date_field = os.getenv('JIRA_ADO_CREATED_DATE_FIELD_ID', 'customfield_12499')
        self._done_statuses = {'done', 'closed'}
        self.customer_name_fields = [
            os.getenv('JIRA_NEXUS_CUSTOMER_NAME_FIELD_ID', 'customfield_12470'),
            os.getenv('JIRA_CUSTOMER_NAME_FIELD_ID', 'customfield_10360'),
        ]
        self.hotfix_delivered_version_field = os.getenv('JIRA_HOTFIX_DELIVERED_VERSION_FIELD_ID', 'customfield_12480')
        self.target_date_field = os.getenv('JIRA_TARGET_DATE_FIELD_ID', 'customfield_12496')
        self.hf_status_field = os.getenv('JIRA_HF_STATUS_FIELD_ID', 'customfield_12479')
        self.hf_requested_versions_field = os.getenv('JIRA_HF_REQUESTED_VERSIONS_FIELD_ID', 'customfield_12478')
        self.effort_field = os.getenv('JIRA_EFFORT_FIELD_ID', 'customfield_10619')
        self.effort_dev_estimate_field = os.getenv('JIRA_EFFORT_DEV_ESTIMATE_FIELD_ID', 'customfield_10623')
        self.effort_dev_actual_field = os.getenv('JIRA_EFFORT_DEV_ACTUAL_FIELD_ID', 'customfield_10624')
        self.qa_effort_estimation_field = os.getenv('JIRA_QA_EFFORT_ESTIMATION_FIELD_ID', 'customfield_12484')
        self.qa_effort_actual_field = os.getenv('JIRA_QA_EFFORT_ACTUAL_FIELD_ID', 'customfield_12483')
        self.tshirt_estimation_field = os.getenv('JIRA_TSHIRT_ESTIMATION_FIELD_ID', 'customfield_12489')
        self.ticket_type_field = os.getenv('JIRA_TICKET_TYPE_FIELD_ID', 'customfield_12467')
        self.freshdesk_ticket_field = os.getenv('JIRA_FRESHDESK_TICKET_FIELD_ID', 'customfield_12476')
        self.target_version_fields = [
            os.getenv('JIRA_TARGET_VERSION_FIELD_ID', 'customfield_12490'),
            os.getenv('JIRA_VERSION_FIELD_ID', 'customfield_12493'),
        ]
        self.connector_fields = [
            os.getenv('JIRA_NEXUS_CONNECTOR_FIELD_ID', 'customfield_12509'),
            os.getenv('JIRA_CONNECTOR_FIELD_ID', 'customfield_11384'),
        ]
        self.blocker_field = os.getenv('JIRA_BLOCKER_FIELD_ID', 'customfield_12466')
        self.business_value_field = os.getenv('JIRA_BUSINESS_VALUE_FIELD_ID', 'customfield_10620')
        self.business_outcome_field = os.getenv('JIRA_BUSINESS_OUTCOME_FIELD_ID', 'customfield_12468')

    def _request_json(self, path, params=None):
        response = requests.get(f"{self.jira_url}{path}", headers=self.headers, params=params, timeout=60)
        if response.status_code != 200:
            print(f"Error fetching Jira data from {path}: {response.status_code} - {response.text}")
            return None
        return response.json()

    def _adf_to_text(self, adf_value):
        if not adf_value:
            return ""
        if isinstance(adf_value, str):
            return adf_value

        parts = []

        def walk(node):
            if isinstance(node, list):
                for child in node:
                    walk(child)
                return
            if not isinstance(node, dict):
                return
            if node.get('type') == 'text':
                parts.append(node.get('text', ''))
                return
            for child in node.get('content', []) or []:
                walk(child)
            if node.get('type') in ['paragraph', 'heading', 'blockquote', 'listItem']:
                parts.append('\n')
            if node.get('type') == 'hardBreak':
                parts.append('\n')

        walk(adf_value)
        return '\n'.join([line.rstrip() for line in ''.join(parts).splitlines()]).strip()

    def _extract_value(self, fields, field_id, field_type='string'):
        raw_value = fields.get(field_id)
        if raw_value in (None, '', [], {}):
            return None
        if field_type == 'number':
            try:
                return float(raw_value) if raw_value is not None else None
            except (ValueError, TypeError):
                return None
        if field_type == 'user':
            if isinstance(raw_value, dict):
                return raw_value.get('displayName', '')
            if isinstance(raw_value, list) and raw_value and isinstance(raw_value[0], dict):
                return raw_value[0].get('displayName', '')
            return str(raw_value)
        if isinstance(raw_value, list):
            values = []
            for item in raw_value:
                if isinstance(item, dict):
                    values.append(item.get('value') or item.get('name') or item.get('displayName') or item.get('key') or str(item))
                else:
                    values.append(str(item))
            return ', '.join([value for value in values if value])
        if isinstance(raw_value, dict):
            return raw_value.get('value') or raw_value.get('name') or raw_value.get('displayName') or raw_value.get('key') or str(raw_value)
        return str(raw_value)

    def _first_value(self, fields, field_ids, field_type='string'):
        for field_id in field_ids:
            value = self._extract_value(fields, field_id, field_type)
            if value not in (None, ''):
                return value
        return ''

    def _parse_jira_date(self, value):
        if not value:
            return None
        for date_format in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, date_format)
            except ValueError:
                continue
        print(f"Warning: unsupported Jira date format: {value}")
        return None

    def _normalize_priority_to_ado_severity(self, priority_name):
        """Translate Jira priorities into the numeric severity values existing dashboards use."""
        if not priority_name:
            return ''

        priority_text = str(priority_name).strip()
        leading_number = re.match(r'^\s*([1-5])\b', priority_text)
        if leading_number:
            return leading_number.group(1)

        normalized = priority_text.lower()
        if any(token in normalized for token in ('blocker', 'critical', 'highest')):
            return '1'
        if any(token in normalized for token in ('high', 'major')):
            return '2'
        if any(token in normalized for token in ('medium', 'normal')):
            return '3'
        if any(token in normalized for token in ('lowest', 'low', 'minor', 'trivial')):
            return '4'

        print(f"Warning: unknown Jira priority '{priority_name}', keeping original value")
        return priority_text

    def _normalize_jira_state_to_ado_state(self, state_name):
        """Translate Jira workflow statuses into the closest existing ADO state values."""
        if not state_name:
            return ''

        state_text = str(state_name).strip()
        normalized = re.sub(r'\s+', ' ', state_text).lower()
        jira_to_ado_states = {
            'open': 'New',
            'to do': 'New',
            'blocked': 'Blocked',
            'in progress': 'In Progress',
            'code review': 'Waiting for PR',
            'ready for qa': 'Ready for QA',
            'in qa': 'In QA',
            'done': 'Done',
            'closed': 'Done',
        }

        mapped_state = jira_to_ado_states.get(normalized)
        if mapped_state:
            return mapped_state

        print(f"Warning: unknown Jira state '{state_name}', keeping original value")
        return state_text

    def _extract_migrated_ado_id(self, fields):
        raw_value = fields.get(self.ado_work_item_id_field)
        if raw_value in (None, '', [], {}):
            return None
        try:
            if isinstance(raw_value, str):
                raw_value = raw_value.replace(',', '').strip()
            return int(float(raw_value))
        except (TypeError, ValueError):
            print(f"Warning: could not parse ADO work item id from {self.ado_work_item_id_field}: {raw_value}")
            return None

    def _compatibility_id(self, issue, table_name):
        jira_key = issue.get('key')
        fields = issue.get('fields', {})
        migrated_ado_id = self._extract_migrated_ado_id(fields)
        if migrated_ado_id:
            return migrated_ado_id

        jira_internal_id = int(issue['id']) if str(issue.get('id', '')).isdigit() else None
        if jira_internal_id is None:
            raise ValueError(f"Jira issue {jira_key} does not have a numeric issue id")

        # Avoid overwriting an unrelated ADO row if Jira's internal id collides.
        if hasattr(self, 'db_connection') and self.db_connection:
            with self.db_connection.engine.connect() as connection:
                existing = connection.execute(
                    text(f"SELECT jira_id, source FROM {table_name} WHERE id = :id"),
                    {"id": jira_internal_id}
                ).first()
                if existing and not (existing[0] == jira_key or existing[1] == 'JIRA'):
                    fallback_id = 1_000_000_000 + jira_internal_id
                    print(
                        f"Warning: Jira issue {jira_key} internal id {jira_internal_id} collides with "
                        f"existing {table_name} row. Using compatibility id {fallback_id}."
                    )
                    return fallback_id
        return jira_internal_id

    def _board_name_map(self):
        return {board['id']: board['name'] for board in self._resolve_boards()}

    def _parse_ado_project_from_goal(self, goal):
        if not goal:
            return None
        match = re.search(r'Migrated from ADO project:\s*(.+)', goal, re.IGNORECASE)
        return match.group(1).strip() if match else None

    def _ado_iteration_path(self, sprint_name, ado_project):
        if not sprint_name:
            return ''
        if ado_project == 'PathlockGRC':
            return f"PathlockGRC\\PLC Engineering Sprints\\{sprint_name}"
        if ado_project == 'Flex Connectors':
            return f"Flex Connectors\\{sprint_name}"
        if ado_project:
            return f"{ado_project}\\{sprint_name}"
        return sprint_name

    def _parse_sprint_entries(self, fields):
        sprint_val = fields.get(self.sprint_field) or fields.get('customfield_10020') or fields.get('sprint')
        if isinstance(sprint_val, dict):
            return [sprint_val]
        if isinstance(sprint_val, list):
            entries = []
            for sprint in sprint_val:
                if isinstance(sprint, dict):
                    entries.append(sprint)
                elif isinstance(sprint, str):
                    match_id = re.search(r"id=(\d+)", sprint)
                    match_name = re.search(r"name=([^,\]]+)", sprint)
                    match_board = re.search(r"boardId=(\d+)", sprint)
                    match_goal = re.search(r"goal=([^,\]]+)", sprint)
                    if match_name:
                        entries.append({
                            'id': int(match_id.group(1)) if match_id else None,
                            'name': match_name.group(1),
                            'boardId': int(match_board.group(1)) if match_board else None,
                            'goal': match_goal.group(1) if match_goal else '',
                        })
            return entries
        if isinstance(sprint_val, str):
            return [{'name': sprint_val}]
        return []

    def _iteration_path(self, fields):
        entries = self._parse_sprint_entries(fields)
        if not entries:
            return ''
        paths = []
        for sprint in entries:
            sprint_name = sprint.get('name', '')
            if not sprint_name:
                continue
            ado_project = self._parse_ado_project_from_goal(sprint.get('goal', ''))
            paths.append(self._ado_iteration_path(sprint_name, ado_project))
        return ', '.join([path for path in paths if path])

    def _lookup_ado_area_path(self, leaf):
        if not leaf or '\\' in leaf:
            return leaf
        if leaf in self._area_path_cache:
            return self._area_path_cache[leaf]
        resolved = ''
        if hasattr(self, 'db_connection') and self.db_connection:
            with self.db_connection.engine.connect() as connection:
                result = connection.execute(
                    text("""
                        SELECT area_path
                        FROM work_items
                        WHERE source = 'ADO'
                          AND area_path LIKE :pattern
                        GROUP BY area_path
                        ORDER BY COUNT(*) DESC
                        LIMIT 1
                    """),
                    {"pattern": f"%\\{leaf}"},
                ).first()
                if result and result[0]:
                    resolved = result[0]
        if not resolved:
            if leaf.startswith('Connectors'):
                resolved = f"Flex Connectors\\{leaf}"
            else:
                resolved = leaf
        self._area_path_cache[leaf] = resolved
        return resolved

    def _area_path(self, fields, ado_project=None):
        components = fields.get('components', [])
        component_path = ", ".join([component.get("name", "") for component in components if component.get("name")])
        jira_area = self._extract_value(fields, self.area_path_field) or ''
        if jira_area:
            if '\\' in jira_area:
                return jira_area
            return self._lookup_ado_area_path(jira_area)
        if component_path:
            return component_path
        if ado_project == 'Flex Connectors':
            return ''
        return ''

    def _extract_parent_jira_key(self, fields):
        parent = fields.get('parent')
        if isinstance(parent, dict) and parent.get('key'):
            return parent['key']
        for link in fields.get('issuelinks', []) or []:
            link_type = (link.get('type') or {}).get('name', '')
            if link_type == 'Parent / Child' and link.get('inwardIssue'):
                return link['inwardIssue'].get('key')
        return None

    def _resolve_parent_id(self, parent_jira_key):
        if not parent_jira_key or not hasattr(self, 'db_connection') or not self.db_connection:
            return None
        with self.db_connection.engine.connect() as connection:
            for table_name in ('bugs', 'work_items', 'issues'):
                result = connection.execute(
                    text(f"SELECT id FROM {table_name} WHERE jira_id = :jira_id"),
                    {"jira_id": parent_jira_key},
                ).first()
                if result:
                    return result[0]
        return None

    def _parse_ado_created_date(self, fields):
        raw_value = fields.get(self.ado_created_date_field)
        if raw_value in (None, '', [], {}):
            return None
        if isinstance(raw_value, datetime):
            return normalize_utc_naive(raw_value)
        raw_text = str(raw_value).strip()
        for date_format in (
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%a, %d %b %Y %H:%M:%S %z",
        ):
            try:
                return normalize_utc_naive(datetime.strptime(raw_text, date_format))
            except ValueError:
                continue
        parsed = self._parse_jira_date(raw_text)
        return normalize_utc_naive(parsed) if parsed else None

    def _created_date(self, fields):
        """Use Azure DevOps Created Date for migrated items instead of Jira import timestamp."""
        migrated = self._extract_migrated_ado_id(fields) is not None
        ado_created = self._parse_ado_created_date(fields)
        if migrated:
            if ado_created:
                return ado_created
            return None
        if ado_created:
            return ado_created
        return fields.get("created", "")

    def _base_item_data(self, issue, table_name):
        fields = issue["fields"]
        assignee = fields.get("assignee")
        creator = fields.get("creator") or fields.get("reporter")
        priority = fields.get("priority")
        status = fields.get("status")
        components = fields.get("components", [])
        labels = fields.get("labels", [])
        fix_versions = fields.get("fixVersions", [])
        target_date = self._parse_jira_date(self._extract_value(fields, self.target_date_field) or fields.get('duedate'))
        sprint_entries = self._parse_sprint_entries(fields)
        ado_project = None
        if sprint_entries:
            ado_project = self._parse_ado_project_from_goal(sprint_entries[0].get('goal', ''))
        parent_jira_key = self._extract_parent_jira_key(fields)
        parent_id = self._resolve_parent_id(parent_jira_key)

        return {
            'ID': self._compatibility_id(issue, table_name),
            'JiraID': issue["key"],
            'Source': 'JIRA',
            'Title': fields.get("summary", ""),
            'Description': self._adf_to_text(fields.get("description")),
            'AssignedTo': assignee.get("displayName", "") if assignee else "",
            'Severity': self._normalize_priority_to_ado_severity(priority.get("name", "") if priority else ""),
            'State': self._normalize_jira_state_to_ado_state(status.get("name", "") if status else ""),
            'CustomerName': self._first_value(fields, self.customer_name_fields),
            'AreaPath': self._area_path(fields, ado_project),
            'CreatedDate': self._created_date(fields),
            'MigratedFromADO': self._extract_migrated_ado_id(fields) is not None,
            'ChangedDate': fields.get("updated", ""),
            'IterationPath': self._iteration_path(fields),
            'HotfixDeliveredVersion': self._extract_value(fields, self.hotfix_delivered_version_field) or '',
            'TargetDate': target_date,
            'HFStatus': self._extract_value(fields, self.hf_status_field) or '',
            'HFRequestedVersions': self._extract_value(fields, self.hf_requested_versions_field) or '',
            'Effort': self._extract_value(fields, self.effort_field, 'number'),
            'EffortDevEstimate': self._extract_value(fields, self.effort_dev_estimate_field, 'number'),
            'EffortDevActual': self._extract_value(fields, self.effort_dev_actual_field, 'number'),
            'QAEffortEstimation': self._extract_value(fields, self.qa_effort_estimation_field, 'number'),
            'QAEffortActual': self._extract_value(fields, self.qa_effort_actual_field, 'number'),
            'TShirtEstimation': self._extract_value(fields, self.tshirt_estimation_field) or '',
            'ParentWorkItem': parent_id,
            'ParentID': parent_id,
            'ParentJiraKey': parent_jira_key,
            'TicketType': self._extract_value(fields, self.ticket_type_field) or '',
            'FreshdeskTicket': self._extract_value(fields, self.freshdesk_ticket_field) or '',
            'TargetVersion': self._first_value(fields, self.target_version_fields) or ", ".join(
                [version.get("name", "") for version in fix_versions if version.get("name")]
            ),
            'Tags': ", ".join(labels)[:500] if labels else '',
            'Connector': self._first_value(fields, self.connector_fields)[:1000],
            'CreatedBy': creator.get("displayName", "") if creator else '',
            'Blocker': self._extract_value(fields, self.blocker_field) or '',
            'BusinessValue': self._extract_value(fields, self.business_value_field, 'number'),
            'BusinessOutcome': self._extract_value(fields, self.business_outcome_field) or '',
        }

    def _search_issues(self, jql_query, fields_param):
        issues = []
        next_page_token = None
        while True:
            params = {
                'jql': jql_query,
                'maxResults': 100,
                'fields': fields_param,
                'expand': 'renderedFields'
            }
            if next_page_token:
                params['nextPageToken'] = next_page_token
            response = requests.get(f"{self.jira_url}/rest/api/3/search/jql", headers=self.headers, params=params, timeout=60)
            if response.status_code != 200:
                print(f"Error fetching Jira issues: {response.status_code} - {response.text}")
                break
            data = response.json()
            issues.extend(data.get("issues", []))
            if data.get("isLast", True) or not data.get("nextPageToken"):
                break
            next_page_token = data.get("nextPageToken")
            time.sleep(0.5)
        return issues

    def _get_worklog_details(self, jira_key, worklog_id):
        worklog_data = self._request_json(f"/rest/api/3/issue/{jira_key}/worklog") or {}
        for worklog in worklog_data.get('worklogs', []):
            if str(worklog.get('id')) == str(worklog_id):
                return worklog
        return None

    def _jira_change_field_name(self, item):
        field_name = item.get("field")
        field_id = item.get("fieldId")
        if field_name == "status":
            return "System.State"
        if field_name == "Sprint" or field_id in (self.sprint_field, "customfield_10020"):
            return "System.IterationPath"
        if field_name == "WorklogId":
            return "Work Log Entry"
        if field_id == self.hf_status_field:
            return "Custom.HFstatus"
        if field_id == self.target_date_field:
            return "Microsoft.VSTS.Scheduling.TargetDate"
        if field_id == self.hf_requested_versions_field:
            return "Custom.HFrequestedversions"
        return None

    def _handle_jira_changes(self, items, table_name_getter, db_connection=None):
        if not items:
            return {}

        all_state_changes = {}
        total_items = len(items)
        for index, item_data in enumerate(items, start=1):
            if index == 1 or index % 500 == 0 or index == total_items:
                print(f"------>Processing Jira changelog {index}/{total_items}")
            if not isinstance(item_data, dict):
                print(f"Skipping Jira change history for unsupported item reference: {item_data}")
                continue

            record_id = item_data['ID']
            jira_key = item_data.get('JiraID')
            if not jira_key:
                print(f"Skipping Jira change history for {record_id}; missing jira_id")
                continue

            issue_data = self._request_json(f"/rest/api/3/issue/{jira_key}", {"expand": "changelog"})
            if not issue_data:
                continue

            changelog = issue_data.get("changelog", {}).get("histories", [])
            state_changes = []
            table_name = table_name_getter(item_data)
            tracked_fields = (
                'System.State',
                'System.IterationPath',
                'Work Log Entry',
                'Custom.HFstatus',
                'Microsoft.VSTS.Scheduling.TargetDate',
                'Custom.HFrequestedversions',
            )

            if db_connection:
                with db_connection.engine.connect() as connection:
                    try:
                        cutover = get_jira_cutover_date()
                        connection.execute(
                            text("""
                                DELETE FROM change_history
                                WHERE record_id = :record_id
                                AND source = 'JIRA'
                                AND field_changed = ANY(:tracked_fields)
                            """),
                            {
                                "record_id": record_id,
                                "tracked_fields": list(tracked_fields),
                            }
                        )

                        for history in changelog:
                            changed_date_obj = normalize_utc_naive(self._parse_jira_date(history.get("created")))
                            if not changed_date_obj:
                                continue
                            changed_by = history.get("author", {}).get("displayName", "")
                            if is_jira_migration_changelog_entry(changed_by, changed_date_obj, cutover):
                                continue
                            for history_item in history.get("items", []):
                                field_changed = self._jira_change_field_name(history_item)
                                if not field_changed:
                                    continue
                                old_value = history_item.get("fromString", "") or ""
                                new_value = history_item.get("toString", "") or ""
                                if field_changed == "System.State":
                                    old_value = self._normalize_jira_state_to_ado_state(old_value)
                                    new_value = self._normalize_jira_state_to_ado_state(new_value)

                                if field_changed == "Work Log Entry" and new_value:
                                    old_value = ""
                                    worklog_details = self._get_worklog_details(jira_key, new_value)
                                    if worklog_details:
                                        new_value = str(
                                            worklog_details.get('timeSpentSeconds')
                                            or worklog_details.get('timeSpent')
                                            or new_value
                                        )

                                connection.execute(
                                    text("""
                                        INSERT INTO change_history
                                        (record_id, jira_id, source, table_name, field_changed, old_value, new_value, changed_by, changed_date)
                                        VALUES
                                        (:record_id, :jira_id, 'JIRA', :table_name, :field_changed, :old_value, :new_value, :changed_by, :changed_date)
                                    """),
                                    {
                                        "record_id": record_id,
                                        "jira_id": jira_key,
                                        "table_name": table_name,
                                        "field_changed": field_changed,
                                        "old_value": str(old_value)[:500] if old_value else "",
                                        "new_value": str(new_value)[:500] if new_value else "",
                                        "changed_by": changed_by,
                                        "changed_date": changed_date_obj,
                                    }
                                )

                                if field_changed == "System.State":
                                    state_changes.append([history.get("created", ""), old_value, new_value])

                        connection.commit()
                    except SQLAlchemyError as e:
                        print(f"Error processing Jira changes for {jira_key}: {str(e)}")
                        connection.rollback()
            else:
                for history in changelog:
                    for history_item in history.get("items", []):
                        if self._jira_change_field_name(history_item) == "System.State":
                            state_changes.append([
                                history.get("created", ""),
                                history_item.get("fromString", "") or "",
                                history_item.get("toString", "") or "",
                            ])

            all_state_changes[record_id] = state_changes

        return all_state_changes

    def _fields_param(self):
        fields = {
            "summary", "description", "assignee", "priority", "status", "components",
            "created", "updated", "parent", "creator", "reporter", "issuetype", "labels",
            "fixVersions", "duedate", "timetracking", "worklog", "sprint", "customfield_10020",
            "issuelinks",
            self.area_path_field, self.sprint_field,
            *self.customer_name_fields, self.hotfix_delivered_version_field, self.target_date_field,
            self.hf_status_field, self.hf_requested_versions_field, self.effort_field,
            self.effort_dev_estimate_field, self.effort_dev_actual_field,
            self.qa_effort_estimation_field, self.qa_effort_actual_field,
            self.tshirt_estimation_field, self.ticket_type_field, self.freshdesk_ticket_field,
            self.ado_work_item_id_field, self.ado_created_date_field,
            *self.target_version_fields, *self.connector_fields, self.blocker_field,
            self.business_value_field, self.business_outcome_field
        }
        return ",".join(sorted([field for field in fields if field]))

    def get_all_work_items(self, last_update, full_sync=False):
        if full_sync:
            print(f"------>Fetching all Jira work items for project {self.project_key}")
            jql_query = (
                f'project = {self.project_key} '
                f'AND issuetype in (Story, Bug, Task, Epic, "Sub-task") '
                f'ORDER BY updated DESC'
            )
        else:
            print(f"------>Fetching Jira work items with updated >= {last_update}")
            jql_query = (
                f'project = {self.project_key} '
                f'AND issuetype in (Story, Bug, Task, Epic, "Sub-task") '
                f'AND updated >= "{last_update}" ORDER BY updated DESC'
            )
        issues = self._search_issues(jql_query, self._fields_param())
        work_items = []
        for issue in issues:
            item_data = self._base_item_data(issue, 'work_items')
            item_data['WorkItemType'] = self._extract_value(issue['fields'], 'issuetype') or 'Unknown'
            work_items.append(item_data)
        print(f"------>Total Jira work items fetched: {len(work_items)}")
        return work_items

    def get_work_items(self, work_item_type, last_update, full_sync=False):
        if work_item_type == 'Issue Report':
            print("------>Skipping Jira Issue Report sync; NEXUS has no Issue Report issue type")
            return []

        if full_sync:
            print(f"------>Fetching all Jira {work_item_type} items for project {self.project_key}")
            jql_query = (
                f'project = {self.project_key} '
                f'AND issuetype = "{work_item_type}" '
                f'ORDER BY updated DESC'
            )
        else:
            print(f"------>Fetching Jira {work_item_type} items with updated >= {last_update}")
            jql_query = (
                f'project = {self.project_key} '
                f'AND issuetype = "{work_item_type}" '
                f'AND updated >= "{last_update}" ORDER BY updated DESC'
            )
        issues = self._search_issues(jql_query, self._fields_param())
        items = []
        for issue in issues:
            item_data = self._base_item_data(issue, 'bugs' if work_item_type == 'Bug' else 'work_items')
            item_data['WorkItemType'] = work_item_type
            items.append(item_data)
        print(f"------>Total Jira {work_item_type} items fetched: {len(items)}")
        return items

    def handle_work_item_changes(self, work_items, db_connection=None):
        return self._handle_jira_changes(
            work_items,
            lambda item: item.get('WorkItemType', 'work_items'),
            db_connection
        )

    def handle_bug_changes(self, bugs, db_connection=None):
        return self._handle_jira_changes(
            bugs,
            lambda item: 'bugs',
            db_connection
        )

    def normalize_existing_jira_states(self, db_connection):
        """Backfill existing Jira-owned rows that were synced before state normalization existed."""
        if not db_connection:
            return

        state_mappings = {
            'OPEN': 'New',
            'TO DO': 'New',
            'BLOCKED': 'Blocked',
            'IN PROGRESS': 'In Progress',
            'CODE REVIEW': 'Waiting for PR',
            'READY FOR QA': 'Ready for QA',
            'IN QA': 'In QA',
            'DONE': 'Done',
            'CLOSED': 'Done',
        }

        with db_connection.engine.connect() as connection:
            try:
                for raw_state, ado_state in state_mappings.items():
                    for table_name in ('work_items', 'bugs'):
                        connection.execute(
                            text(f"""
                                UPDATE {table_name}
                                SET state = :ado_state
                                WHERE source = 'JIRA'
                                  AND UPPER(state) = :raw_state
                                  AND state <> :ado_state
                            """),
                            {"raw_state": raw_state, "ado_state": ado_state}
                        )

                    connection.execute(
                        text("""
                            UPDATE change_history
                            SET old_value = :ado_state
                            WHERE source = 'JIRA'
                              AND field_changed = 'System.State'
                              AND UPPER(old_value) = :raw_state
                              AND old_value <> :ado_state
                        """),
                        {"raw_state": raw_state, "ado_state": ado_state}
                    )
                    connection.execute(
                        text("""
                            UPDATE change_history
                            SET new_value = :ado_state
                            WHERE source = 'JIRA'
                              AND field_changed = 'System.State'
                              AND UPPER(new_value) = :raw_state
                              AND new_value <> :ado_state
                        """),
                        {"raw_state": raw_state, "ado_state": ado_state}
                    )
                connection.commit()
            except SQLAlchemyError as e:
                print(f"Error normalizing existing Jira states: {str(e)}")
                connection.rollback()

    def _resolve_boards(self):
        """Resolve Jira boards to sync (explicit IDs, env list, or project auto-discovery)."""
        if self._boards_cache is not None:
            return self._boards_cache

        boards = []
        explicit_ids = self.board_ids
        if not explicit_ids:
            env_ids = os.getenv('JIRA_NEXUS_BOARD_IDS', '').strip()
            if env_ids:
                explicit_ids = [board_id.strip() for board_id in env_ids.split(',') if board_id.strip()]

        if explicit_ids:
            for board_id in explicit_ids:
                board_data = self._request_json(f"/rest/agile/1.0/board/{board_id}") or {}
                boards.append({
                    'id': str(board_id),
                    'name': board_data.get('name', f"Board {board_id}"),
                })
        else:
            start_at = 0
            while True:
                data = self._request_json(
                    "/rest/agile/1.0/board",
                    {
                        'projectKeyOrId': self.project_key,
                        'startAt': start_at,
                        'maxResults': 50,
                    },
                ) or {}
                for board in data.get('values', []):
                    boards.append({
                        'id': str(board.get('id')),
                        'name': board.get('name', f"Board {board.get('id')}"),
                    })
                if data.get('isLast', True):
                    break
                start_at += data.get('maxResults', 50)

        if not boards and self.board_id:
            board_data = self._request_json(f"/rest/agile/1.0/board/{self.board_id}") or {}
            boards.append({
                'id': str(self.board_id),
                'name': board_data.get('name', f"Board {self.board_id}"),
            })

        self._boards_cache = boards
        return boards

    def _fetch_sprints_for_board(self, board_id, board_name):
        """Fetch all sprints for a board via GET /rest/agile/1.0/board/{boardId}/sprint."""
        sprints = []
        start_at = 0
        while True:
            data = self._request_json(
                f"/rest/agile/1.0/board/{board_id}/sprint",
                {
                    'startAt': start_at,
                    'maxResults': 50,
                    'state': 'active,future,closed',
                },
            ) or {}
            for sprint in data.get('values', []):
                sprint_name = sprint.get('name', '')
                ado_project = self._parse_ado_project_from_goal(sprint.get('goal', ''))
                if ado_project:
                    sprint_path = self._ado_iteration_path(sprint_name, ado_project)
                else:
                    sprint_path = f"JIRA/{self.project_key}/{board_name}/{sprint_name}"
                sprints.append({
                    'id': str(sprint.get('id')),
                    'name': sprint_name,
                    'path': sprint_path,
                    'start_date': self._parse_jira_date(sprint.get('startDate')) if sprint.get('startDate') else None,
                    'finish_date': self._parse_jira_date(sprint.get('endDate')) if sprint.get('endDate') else None,
                    'state': sprint.get('state', ''),
                    'board_id': str(board_id),
                    'board_name': board_name,
                })
            if data.get('isLast', True):
                break
            start_at += data.get('maxResults', 50)
        return sprints

    def _story_points(self, fields):
        raw_value = fields.get(self.story_points_field)
        if raw_value in (None, ''):
            return 0.0
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            return 0.0

    def _fetch_sprint_issue_capacities(self, board, sprint_meta):
        """Derive per-member sprint load from issues in the sprint (Jira has no ADO-style capacity API)."""
        sprint_id = sprint_meta['id']
        sprint_name = sprint_meta['name']
        fields_param = f"assignee,status,{self.story_points_field}"
        member_totals = {}
        start_at = 0

        while True:
            data = self._request_json(
                f"/rest/agile/1.0/sprint/{sprint_id}/issue",
                {
                    'startAt': start_at,
                    'maxResults': 50,
                    'fields': fields_param,
                },
            ) or {}
            for issue in data.get('issues', []):
                fields = issue.get('fields', {})
                assignee = fields.get('assignee') or {}
                member_id = assignee.get('accountId') or 'unassigned'
                member_name = assignee.get('displayName') or 'Unassigned'
                points = self._story_points(fields)
                status_name = (fields.get('status') or {}).get('name', '')
                is_done = status_name.strip().lower() in self._done_statuses

                if member_id not in member_totals:
                    member_totals[member_id] = {
                        'name': member_name,
                        'committed': 0.0,
                        'completed': 0.0,
                    }
                member_totals[member_id]['committed'] += points
                if is_done:
                    member_totals[member_id]['completed'] += points

            if data.get('isLast', True):
                break
            start_at += data.get('maxResults', 50)
            time.sleep(0.2)

        capacities = []
        for member_id, totals in member_totals.items():
            capacities.append({
                'sprint_id': sprint_id,
                'sprint_name': sprint_name,
                'team_member_id': member_id,
                'team_member_name': totals['name'],
                'activity': 'Story Points (committed)',
                'capacity_per_day': totals['committed'],
                'days_off_count': 0,
                'days_off_start': None,
                'days_off_end': None,
                'project_name': self.project_key,
                'team_name': board['name'],
            })
            capacities.append({
                'sprint_id': sprint_id,
                'sprint_name': sprint_name,
                'team_member_id': member_id,
                'team_member_name': totals['name'],
                'activity': 'Story Points (completed)',
                'capacity_per_day': totals['completed'],
                'days_off_count': 0,
                'days_off_start': None,
                'days_off_end': None,
                'project_name': self.project_key,
                'team_name': board['name'],
            })
        return capacities

    def update_sprints(self):
        boards = self._resolve_boards()
        if not boards:
            print("------>Skipping Jira sprints sync; no boards configured or discovered")
            return 0

        all_sprints = []
        seen_ids = set()
        for board in boards:
            print(f"------>Fetching Jira sprints for board {board['name']} ({board['id']})")
            board_sprints = self._fetch_sprints_for_board(board['id'], board['name'])
            print(f"------>Found {len(board_sprints)} sprints for board {board['name']}")
            for sprint in board_sprints:
                if sprint['id'] in seen_ids:
                    continue
                seen_ids.add(sprint['id'])
                all_sprints.append({
                    'id': sprint['id'],
                    'name': sprint['name'],
                    'path': sprint['path'],
                    'start_date': sprint['start_date'],
                    'finish_date': sprint['finish_date'],
                    'state': sprint['state'],
                })

        if hasattr(self, 'db_connection') and self.db_connection:
            processed_count = self.db_connection.upsert_sprints(all_sprints)
            print(f"------>Processed {processed_count} Jira sprints from {len(boards)} board(s)")
            return processed_count
        return 0

    def update_sprint_capacities(self, current_sprint_only=True):
        boards = self._resolve_boards()
        if not boards:
            print("------>Skipping Jira sprint capacity sync; no boards configured or discovered")
            return 0

        print(f"------>Fetching Jira sprint capacities from {len(boards)} board(s)")
        all_capacities = []
        for board in boards:
            board_sprints = self._fetch_sprints_for_board(board['id'], board['name'])
            sprints_to_process = board_sprints
            if current_sprint_only:
                sprints_to_process = [sprint for sprint in board_sprints if sprint.get('state') == 'active']

            print(
                f"------>Processing capacities for {len(sprints_to_process)} sprint(s) "
                f"on board {board['name']}"
            )
            for sprint in sprints_to_process:
                sprint_capacities = self._fetch_sprint_issue_capacities(board, sprint)
                all_capacities.extend(sprint_capacities)

        if hasattr(self, 'db_connection') and self.db_connection:
            processed_count = self.db_connection.upsert_sprint_capacities(all_capacities)
            print(f"------>Processed {processed_count} Jira sprint capacity records from {len(boards)} board(s)")
            return processed_count
        return 0

def get_jira_initial_sync_date():
  """First Jira sync window (default: 3 months back)."""
  months = int(os.getenv('JIRA_INITIAL_SYNC_MONTHS', '3'))
  return datetime.now() - timedelta(days=months * 30)


def get_jira_cutover_date():
    """UTC cutover when Jira became source of truth for live changes."""
    raw = os.getenv('JIRA_CUTOVER_DATE', '2026-07-08T00:00:00')
    for date_format in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            parsed = datetime.strptime(raw, date_format)
            return parsed.replace(tzinfo=None)
        except ValueError:
            continue
    raise ValueError(f"Unsupported JIRA_CUTOVER_DATE format: {raw}")


def get_jira_migration_authors():
    raw = os.getenv('JIRA_MIGRATION_AUTHORS', 'TFS4JIRA Azure DevOps integration')
    return [author.strip() for author in raw.split(',') if author.strip()]


def normalize_utc_naive(value):
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def is_jira_migration_changelog_entry(changed_by, changed_date, cutover=None):
    cutover = cutover or get_jira_cutover_date()
    changed_date = normalize_utc_naive(changed_date)
    if changed_date and changed_date < cutover:
        return True
    changed_by_text = (changed_by or '').lower()
    for author in get_jira_migration_authors():
        if author.lower() in changed_by_text:
            return True
    return False


BUG_CHANGE_HISTORY_FIELDS = (
    'System.State',
    'Custom.HFstatus',
    'Microsoft.VSTS.Scheduling.TargetDate',
    'Custom.HFrequestedversions',
)

WORK_ITEM_CHANGE_HISTORY_FIELDS = (
    'System.State',
    'System.IterationPath',
)


def backfill_migrated_created_dates(db, jira_extractor):
    """Refresh created_date from Azure DevOps Created Date (customfield_12499) for migrated items."""
    with db.engine.connect() as connection:
        keys = [
            row[0] for row in connection.execute(
                text("""
                    SELECT DISTINCT jira_id FROM (
                        SELECT jira_id FROM bugs WHERE jira_id IS NOT NULL
                        UNION
                        SELECT jira_id FROM work_items WHERE jira_id IS NOT NULL
                    ) items
                    ORDER BY jira_id
                """)
            ).fetchall()
        ]

    print(f"------>Backfilling created_date for {len(keys)} Jira keys from ADO Created Date field")
    fields_param = jira_extractor._fields_param()
    updated = 0
    batch_size = 50
    for index in range(0, len(keys), batch_size):
        batch = keys[index:index + batch_size]
        issues = jira_extractor._search_issues(f"key in ({','.join(batch)})", fields_param)
        bug_items = []
        work_items = []
        for issue in issues:
            fields = issue.get('fields', {})
            if not jira_extractor._extract_migrated_ado_id(fields):
                continue
            item_data = jira_extractor._base_item_data(issue, 'work_items')
            item_data['WorkItemType'] = jira_extractor._extract_value(fields, 'issuetype') or 'Unknown'
            work_items.append(item_data)
            if item_data['WorkItemType'] == 'Bug':
                bug_items.append(item_data)

        if bug_items:
            db.upsert_items(bug_items, db.bugs, 'bug')
        if work_items:
            db.upsert_items(work_items, db.work_items, 'work_item')
        updated += len(work_items)

        if index == 0 or (index // batch_size) % 20 == 0 or index + batch_size >= len(keys):
            print(f"------>Created date backfill {min(index + batch_size, len(keys))}/{len(keys)}")
        time.sleep(0.15)

    print(f"------>Updated created_date for {updated} migrated items")
    return updated


def repair_migrated_change_history(db, ado_extractor, jira_extractor=None):
    """One-time repair: ADO history before cutover, Jira history after cutover only."""
    cutover = get_jira_cutover_date()
    print(f"------>Repairing migrated change history (cutover UTC: {cutover.isoformat(sep=' ')})")

    db.purge_invalid_jira_change_history()

    ado_restored = 0
    with db.engine.connect() as connection:
        bug_rows = connection.execute(
            text("SELECT id, jira_id FROM bugs WHERE jira_id IS NOT NULL ORDER BY id")
        ).fetchall()
        work_item_rows = connection.execute(
            text("""
                SELECT id, jira_id, work_item_type
                FROM work_items
                WHERE jira_id IS NOT NULL
                  AND work_item_type <> 'Bug'
                ORDER BY id
            """)
        ).fetchall()

    total_ado = len(bug_rows) + len(work_item_rows)
    print(f"------>Restoring pre-cutover ADO history for {total_ado} migrated records")

    for index, row in enumerate(bug_rows, start=1):
        if index == 1 or index % 250 == 0 or index == len(bug_rows):
            print(f"------>ADO bug history restore {index}/{len(bug_rows)}")
        ado_restored += ado_extractor.store_pre_cutover_history(
            row[0], 'bugs', BUG_CHANGE_HISTORY_FIELDS, jira_id=row[1], db_connection=db
        )
        if index % 50 == 0:
            time.sleep(0.2)

    for index, row in enumerate(work_item_rows, start=1):
        if index == 1 or index % 250 == 0 or index == len(work_item_rows):
            print(f"------>ADO work item history restore {index}/{len(work_item_rows)}")
        ado_restored += ado_extractor.store_pre_cutover_history(
            row[0], row[2], WORK_ITEM_CHANGE_HISTORY_FIELDS, jira_id=row[1], db_connection=db
        )
        if index % 50 == 0:
            time.sleep(0.2)

    print(f"------>Inserted {ado_restored} pre-cutover ADO change_history rows")

    if jira_extractor:
        backfill_migrated_created_dates(db, jira_extractor)

    if not jira_extractor:
        print("------>Skipping post-cutover Jira history refresh (no Jira extractor configured)")
        return ado_restored

    jira_bug_items = [{'ID': row[0], 'JiraID': row[1], 'WorkItemType': 'Bug'} for row in bug_rows]
    jira_work_items = [
        {'ID': row[0], 'JiraID': row[1], 'WorkItemType': row[2]}
        for row in work_item_rows
    ]

    print(f"------>Refreshing post-cutover Jira history for {len(jira_bug_items)} bugs")
    jira_extractor.handle_bug_changes(jira_bug_items, db)
    print(f"------>Refreshing post-cutover Jira history for {len(jira_work_items)} work items")
    jira_extractor.handle_work_item_changes(jira_work_items, db)
    return ado_restored


def main():
    # Load configuration from .env file
    organization = os.getenv('ADO_ORGANIZATION')
    project = os.getenv('ADO_PROJECT')
    scrum_project = os.getenv('ADO_SCRUM_PROJECT')
    pat = os.getenv('ADO_PERSONAL_ACCESS_TOKEN')

    include_ado = os.getenv('INCLUDE_ADO', 'true').lower() in ('1', 'true', 'yes')
    if '--skip-ado' in sys.argv:
        include_ado = False
    elif '--include-ado' in sys.argv:
        include_ado = True

    include_jira = os.getenv('INCLUDE_JIRA', 'false').lower() in ('1', 'true', 'yes') or '--include-jira' in sys.argv
    repair_jira_history = os.getenv('JIRA_REPAIR_HISTORY', 'false').lower() in ('1', 'true', 'yes') or '--repair-jira-history' in sys.argv
    repair_only = os.getenv('JIRA_REPAIR_ONLY', 'false').lower() in ('1', 'true', 'yes') or '--repair-jira-history-only' in sys.argv

    if not include_ado and not include_jira and not repair_jira_history:
        raise ValueError("At least one of INCLUDE_ADO or INCLUDE_JIRA must be enabled")

    db = get_database_connection()

    extractor = None
    if include_ado:
        if not all([organization, project, pat]):
            raise ValueError("Please check your .env file and ensure ADO_ORGANIZATION, ADO_PROJECT, and ADO_PERSONAL_ACCESS_TOKEN are set")

        if pat == "your-pat-token-here":
            raise ValueError("Please update the ADO_PERSONAL_ACCESS_TOKEN in your .env file with your actual PAT")

        extractor = ADOExtractor(organization, project, pat, scrum_project)
        extractor.db_connection = db

    run_once = os.getenv('RUN_ONCE', 'false').lower() in ('1', 'true', 'yes') or '--once' in sys.argv
    skip_ado_history = os.getenv('SKIP_ADO_HISTORY', 'false').lower() in ('1', 'true', 'yes') or '--skip-ado-history' in sys.argv
    skip_jira_history = os.getenv('SKIP_JIRA_HISTORY', 'false').lower() in ('1', 'true', 'yes') or '--skip-jira-history' in sys.argv
    skip_github_metrics = os.getenv('SKIP_GITHUB_METRICS', 'false').lower() in ('1', 'true', 'yes') or '--skip-github-metrics' in sys.argv
    include_azure_cost = os.getenv('INCLUDE_AZURE_COST', 'false').lower() in ('1', 'true', 'yes') or '--include-azure-cost' in sys.argv
    jira_extractor = None
    if include_jira:
        jira_url = os.getenv('JIRA_CLOUD_URL')
        jira_project = os.getenv('JIRA_NEXUS_PROJECT_KEY') or os.getenv('JIRA_PROJECT_KEY')
        jira_email = os.getenv('JIRA_USER_EMAIL')
        jira_token = os.getenv('JIRA_API_TOKEN')
        jira_board_id = os.getenv('JIRA_NEXUS_BOARD_ID', '1766')
        board_ids_env = os.getenv('JIRA_NEXUS_BOARD_IDS', '').strip()
        board_ids = [board_id.strip() for board_id in board_ids_env.split(',') if board_id.strip()] if board_ids_env else None
        if not all([jira_url, jira_project, jira_email, jira_token]):
            raise ValueError("INCLUDE_JIRA is enabled, but JIRA_CLOUD_URL, JIRA_NEXUS_PROJECT_KEY/JIRA_PROJECT_KEY, JIRA_USER_EMAIL, or JIRA_API_TOKEN is missing")
        jira_extractor = JIRAExtractor(jira_url, jira_project, jira_email, jira_token, jira_board_id, board_ids=board_ids)
        jira_extractor.db_connection = db
        if board_ids:
            print(f"------>Jira sync enabled for project {jira_project}, boards {board_ids}")
        else:
            print(f"------>Jira sync enabled for project {jira_project}, auto-discovering boards (fallback board {jira_board_id})")

    if repair_jira_history:
        if not all([organization, project, pat]):
            raise ValueError("Jira history repair requires ADO_ORGANIZATION, ADO_PROJECT, and ADO_PERSONAL_ACCESS_TOKEN")
        if not jira_extractor:
            raise ValueError("Jira history repair requires INCLUDE_JIRA=true for post-cutover Jira history")
        ado_for_repair = extractor or ADOExtractor(organization, project, pat, scrum_project)
        ado_for_repair.db_connection = db
        repair_migrated_change_history(db, ado_for_repair, jira_extractor)
        if repair_only:
            print("Jira history repair completed; exiting")
            return

    # Process bugs from CSV file
    # bugs_csv_file_path = '/Users/haviv_rosh/work/scripts_python/metabase/bugs_202503011913.csv'
    # print("\nProcessing bug customer names from CSV file...")
    # extractor.update_bugs_customer_names_from_csv(bugs_csv_file_path)
    # print("Bugs CSV processing completed\n")

    # Process issues from CSV file
    # issues_csv_file_path = '/Users/haviv_rosh/work/scripts_python/metabase/issues.csv'
    # print("\nProcessing issue customer names from CSV file...")
    # extractor.update_issues_customer_names_from_csv(issues_csv_file_path)
    # print("Issues CSV processing completed\n")

    SLEEP_INTERVAL = int(os.getenv('SYNC_INTERVAL_SECONDS', 60 * 60))  # 1 hour by default
    
    print(f"Starting continuous sync with {SLEEP_INTERVAL/60} minute intervals")
    if run_once:
        print("Running a single sync cycle (--once/RUN_ONCE enabled)")
    if include_ado:
        print("ADO sync enabled (INCLUDE_ADO=true)")
    else:
        print("ADO sync disabled (INCLUDE_ADO=false)")
    if include_jira:
        print("Jira sync enabled (INCLUDE_JIRA=true)")
    if skip_ado_history:
        print("Skipping ADO change-history refresh (--skip-ado-history/SKIP_ADO_HISTORY enabled)")
    if skip_jira_history:
        print("Skipping Jira change-history refresh (--skip-jira-history/SKIP_JIRA_HISTORY enabled)")
    if skip_github_metrics:
        print("Skipping GitHub metric refresh (--skip-github-metrics/SKIP_GITHUB_METRICS enabled)")
    if include_azure_cost:
        print("Azure cost sync enabled (--include-azure-cost/INCLUDE_AZURE_COST)")
    print("Press Ctrl+C to stop the program")
    
    while True:
        try:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n=== Starting sync cycle at {current_time} ===")
            
            # Check if full sync is disabled (for local development)
            disable_full_sync = os.getenv('DISABLE_FULL_SYNC', 'false').lower() == 'true'
            
            # Check if we need to run a full sync (weekly, going back 1 month)
            last_full_sync = db.get_last_full_sync_time()
            should_run_full_sync = False
            sync_date_override = None
            
            if disable_full_sync:
                print(f"------>Full sync disabled (DISABLE_FULL_SYNC=true). Running delta sync only")
                should_run_full_sync = False
            elif last_full_sync is None:
                # First time running, do a full sync
                should_run_full_sync = True
                sync_date_override = datetime.now() - timedelta(days=30)
                print(f"------>No previous full sync found. Running initial full sync (1 month back)")
            else:
                # Check if a week has passed since last full sync
                days_since_full_sync = (datetime.now() - last_full_sync).days
                if days_since_full_sync >= 7:
                    should_run_full_sync = True
                    sync_date_override = datetime.now() - timedelta(days=30)
                    print(f"------>Last full sync was {days_since_full_sync} days ago. Running weekly full sync (1 month back)")
                else:
                    print(f"------>Last full sync was {days_since_full_sync} days ago. Running delta sync")
            
            processed_issues = 0
            processed_bugs = 0
            processed_work_items = 0
            processed_sprints = 0
            processed_capacities = 0

            if include_ado:
                # Get last sync times from database
                issues_last_sync = db.get_last_sync_time('issue')
                bugs_last_sync = db.get_last_sync_time('bug')
                work_items_last_sync = db.get_last_sync_time('work_item')

                # Override with full sync date if needed
                if should_run_full_sync:
                    issues_last_sync = sync_date_override
                    bugs_last_sync = sync_date_override
                    work_items_last_sync = sync_date_override
                    print(f"------>Using ADO sync date: {sync_date_override.strftime('%Y-%m-%d')} (1 month back)")

                # Extract and store all work items first
                work_items = extractor.get_all_work_items(work_items_last_sync.strftime('%Y-%m-%d'))
                processed_work_items = db.upsert_items(work_items, db.work_items, 'work_item')
                print(f"------>Processed {processed_work_items} work items")

                # Process state changes for all work items
                if skip_ado_history:
                    print("------>Skipping ADO work item state changes")
                else:
                    print("------>Processing work item state changes")
                    extractor.handle_work_item_changes(work_items, db)
                    print(f"------>Processed state changes for {len(work_items)} work items")

                # Extract and store issues
                issues = extractor.get_work_items('Issue Report', issues_last_sync.strftime('%Y-%m-%d'))
                processed_issues = db.upsert_items(issues, db.issues, 'issue')
                print(f"------>Processed {processed_issues} issues")

                # Extract and store bugs with parent relationships
                bugs = extractor.get_work_items('Bug', bugs_last_sync.strftime('%Y-%m-%d'))
                processed_bugs = db.upsert_items(bugs, db.bugs, 'bug')
                print(f"------>Processed {processed_bugs} bugs")

                # Process state changes for all bugs at once
                if skip_ado_history:
                    print("------>Skipping ADO bug state changes")
                else:
                    print("------>Processing bug state changes")
                    extractor.handle_bug_changes(bugs, db)
                    print(f"------>Processed state changes for {len(bugs)} bugs")

                # Clean up invalid parent issue references
                print("------>Cleaning up invalid parent issue references")
                db.update_parent_issue()

                # Update sprints
                processed_sprints = extractor.update_sprints()

                # Update sprint capacities
                last_capacity_sync = db.get_last_sprint_capacity_sync()
                if last_capacity_sync is None:
                    print("------>First sprint capacity sync - fetching all historical data")
                    processed_capacities = extractor.update_sprint_capacities(current_sprint_only=False)
                else:
                    print("------>Daily sprint capacity sync - fetching current sprint only")
                    processed_capacities = extractor.update_sprint_capacities(current_sprint_only=True)
            else:
                print("------>Skipping ADO sync (INCLUDE_ADO=false)")

            processed_jira_work_items = 0
            processed_jira_issues = 0
            processed_jira_bugs = 0
            processed_jira_sprints = 0
            processed_jira_capacities = 0

            if include_jira and jira_extractor:
                jira_full_project_sync = os.getenv('JIRA_FULL_PROJECT_SYNC', 'false').lower() in ('1', 'true', 'yes')
                if jira_full_project_sync:
                    print("------>JIRA_FULL_PROJECT_SYNC enabled — fetching entire NEXUS project")
                    jira_sync_from = None
                elif not db.has_entity_sync_history('jira_work_item'):
                    jira_sync_from = get_jira_initial_sync_date()
                    print(
                        f"------>First Jira sync — fetching all issues updated since "
                        f"{jira_sync_from.strftime('%Y-%m-%d')}"
                    )
                elif should_run_full_sync:
                    jira_sync_from = sync_date_override
                    print(f"------>Using Jira full sync date: {jira_sync_from.strftime('%Y-%m-%d')}")
                else:
                    jira_sync_from = db.get_last_sync_time('jira_work_item')

                jira_work_items_last_sync = jira_sync_from
                jira_issues_last_sync = jira_sync_from
                jira_bugs_last_sync = jira_sync_from

                jira_work_items = jira_extractor.get_all_work_items(
                    jira_work_items_last_sync.strftime('%Y-%m-%d') if jira_work_items_last_sync else '2000-01-01',
                    full_sync=jira_full_project_sync,
                )
                processed_jira_work_items = db.upsert_items(jira_work_items, db.work_items, 'work_item')
                print(f"------>Processed {processed_jira_work_items} Jira work items")

                print("------>Processing Jira work item state changes")
                if skip_jira_history:
                    print("------>Skipping Jira work item state changes")
                else:
                    jira_extractor.handle_work_item_changes(jira_work_items, db)
                    print(f"------>Processed Jira state changes for {len(jira_work_items)} work items")
                db.update_jira_parent_links(jira_work_items)

                jira_issues = jira_extractor.get_work_items(
                    'Issue Report',
                    jira_issues_last_sync.strftime('%Y-%m-%d') if jira_issues_last_sync else '2000-01-01',
                    full_sync=jira_full_project_sync,
                )
                processed_jira_issues = db.upsert_items(jira_issues, db.issues, 'issue')
                print(f"------>Processed {processed_jira_issues} Jira issues")

                jira_bugs = jira_extractor.get_work_items(
                    'Bug',
                    jira_bugs_last_sync.strftime('%Y-%m-%d') if jira_bugs_last_sync else '2000-01-01',
                    full_sync=jira_full_project_sync,
                )
                processed_jira_bugs = db.upsert_items(jira_bugs, db.bugs, 'bug')
                print(f"------>Processed {processed_jira_bugs} Jira bugs")
                processed_jira_bug_work_items = db.upsert_items(jira_bugs, db.work_items, 'work_item')
                print(f"------>Refreshed {processed_jira_bug_work_items} Jira bugs in work_items")

                print("------>Processing Jira bug state changes")
                if skip_jira_history:
                    print("------>Skipping Jira bug state changes")
                else:
                    jira_extractor.handle_bug_changes(jira_bugs, db)
                    print(f"------>Processed Jira state changes for {len(jira_bugs)} bugs")
                db.update_jira_parent_links(jira_bugs)

                jira_extractor.normalize_existing_jira_states(db)
                print("------>Normalized existing Jira states to ADO values")

                processed_jira_sprints = jira_extractor.update_sprints()
                if not db.has_entity_sync_history('jira_sprint_capacity'):
                    print("------>First Jira sprint capacity sync - fetching all boards/sprints")
                    processed_jira_capacities = jira_extractor.update_sprint_capacities(current_sprint_only=False)
                else:
                    processed_jira_capacities = jira_extractor.update_sprint_capacities(current_sprint_only=True)

            # Update GitHub Copilot metrics (if configured)
            # Uses org users-1-day usage reports + team membership (export_github_copilot).
            # Runs at most once per calendar day; daily window is a few days ending yesterday.
            processed_copilot = 0
            if skip_github_metrics:
                print("------>Skipping Copilot metrics")
            elif COPILOT_AVAILABLE:
                github_org = os.getenv('GITHUB_ORG')
                github_teams = os.getenv('GITHUB_TEAMS')
                github_team_sizes = os.getenv('GITHUB_TEAM_SIZES')
                
                if github_org and os.getenv('GITHUB_TOKEN'):
                    last_copilot_sync = db.get_last_sync_time('copilot_metrics')
                    today = datetime.now().date()
                    last_sync_date = last_copilot_sync.date() if last_copilot_sync and last_copilot_sync != datetime(2025, 3, 1) else None
                    
                    if last_sync_date == today:
                        print("------>Skipping Copilot metrics (already synced today)")
                    else:
                        print("------>Syncing GitHub Copilot metrics")
                        try:
                            copilot_extractor = GitHubCopilotExtractor(
                                organization=github_org,
                                teams=github_teams,
                                team_sizes=github_team_sizes
                            )
                            if last_copilot_sync == datetime(2025, 3, 1):
                                print("------>First Copilot sync — backfill (GITHUB_COPILOT_INITIAL_SYNC_DAYS)")
                                processed_copilot = copilot_extractor.sync_to_database(db, initial_sync=True)
                            else:
                                processed_copilot = copilot_extractor.sync_to_database(db)
                        except Exception as e:
                            print(f"------>Error syncing Copilot metrics: {str(e)}")
                else:
                    print("------>Skipping Copilot metrics (GITHUB_ORG or GITHUB_TOKEN not configured)")

            # Update GitHub PR metrics (if configured)
            # PR metrics sync runs DAILY to avoid excessive API calls
            processed_prs = 0
            if skip_github_metrics:
                print("------>Skipping PR metrics")
            elif PR_METRICS_AVAILABLE:
                github_org = os.getenv('GITHUB_ORG')
                github_pr_repos = os.getenv('GITHUB_PR_REPOS')  # Comma-separated list of repos
                github_teams = os.getenv('GITHUB_TEAMS')
                
                if github_org and github_pr_repos and os.getenv('GITHUB_TOKEN'):
                    # Check if PR metrics already synced today
                    last_pr_sync = db.get_last_sync_time('pr_metrics')
                    today = datetime.now().date()
                    last_sync_date = last_pr_sync.date() if last_pr_sync and last_pr_sync != datetime(2025, 3, 1) else None
                    
                    if last_sync_date == today:
                        print("------>Skipping PR metrics (already synced today)")
                    else:
                        print("------>Syncing GitHub PR metrics (daily sync)")
                        try:
                            pr_extractor = GitHubPRExtractor(
                                organization=github_org,
                                repositories=github_pr_repos,  # Now supports multiple repos
                                teams=github_teams
                            )
                            
                            # Check if this is the first PR metrics sync
                            if last_pr_sync == datetime(2025, 3, 1):
                                print("------>First PR metrics sync - fetching last 90 days")
                                processed_prs = pr_extractor.sync_to_database(db, initial_sync=True)
                            else:
                                processed_prs = pr_extractor.sync_to_database(db)
                        except Exception as e:
                            print(f"------>Error syncing PR metrics: {str(e)}")
                else:
                    print("------>Skipping PR metrics (GITHUB_ORG, GITHUB_PR_REPOS or GITHUB_TOKEN not configured)")

            # Update GitHub Actions test results (if configured)
            processed_tests = 0
            if skip_github_metrics:
                print("------>Skipping GitHub tests")
            elif GITHUB_TESTS_AVAILABLE:
                # Support both new GITHUB_TEST_CONFIGS and legacy GITHUB_TEST_WORKFLOWS
                github_test_configs = os.getenv('GITHUB_TEST_CONFIGS')
                github_test_workflows = os.getenv('GITHUB_TEST_WORKFLOWS')
                
                if os.getenv('GITHUB_TOKEN') and (github_test_configs or github_test_workflows):
                    print("------>Syncing GitHub Actions test results")
                    try:
                        tests_extractor = GitHubTestsExtractor(db_connection=db)
                        
                        # Check if this is the first test sync (use sync_status, not table data)
                        last_tests_sync = db.get_last_sync_time('github_tests')
                        # get_last_sync_time returns default date (2025-03-01) if no sync found
                        # If it's the default, this is first sync
                        if last_tests_sync == datetime(2025, 3, 1):
                            print("------>First GitHub tests sync - fetching last 60 days")
                            processed_tests = tests_extractor.sync_test_runs(initial_sync=True)
                        else:
                            # Watermark from MAX(run_started_at), not a 1-day window (misses weekly CI)
                            processed_tests = tests_extractor.sync_test_runs()
                    except Exception as e:
                        print(f"------>Error syncing GitHub tests: {str(e)}")
                else:
                    print("------>Skipping GitHub tests (GITHUB_TOKEN or GITHUB_TEST_CONFIGS not configured)")

            # Azure subscription cost + tenant metrics (daily, at most once per 24h)
            processed_azure_cost = 0
            if include_azure_cost and AZURE_COST_AVAILABLE:
                last_azure_cost_sync = db.get_last_sync_time('azure_cost')
                min_sync_hours = int(os.getenv('AZURE_COST_MIN_SYNC_HOURS', '24'))
                if should_sync_azure_cost(last_azure_cost_sync, min_sync_hours):
                    print("------>Syncing Azure subscription cost metrics")
                    try:
                        azure_cost_extractor = AzureCostExtractor()
                        processed_azure_cost = azure_cost_extractor.sync_to_database(db)
                        print(f"------>Processed {processed_azure_cost} Azure cost records")
                    except Exception as e:
                        print(f"------>Error syncing Azure cost metrics: {str(e)}")
                else:
                    hours_since = (datetime.now() - last_azure_cost_sync).total_seconds() / 3600
                    print(
                        f"------>Skipping Azure cost metrics "
                        f"(last sync {hours_since:.1f}h ago; min interval {min_sync_hours}h)"
                    )
            elif include_azure_cost and not AZURE_COST_AVAILABLE:
                print("------>Skipping Azure cost metrics (export_azure_cost not available)")

            # Update history snapshots
            db.update_history_snapshots()
            print("------>Updated history snapshots")

            # Only update sync status if all operations completed successfully
            if processed_issues > 0:
                db.update_sync_status('issue', processed_issues)
            
            if processed_bugs > 0:
                db.update_sync_status('bug', processed_bugs)

            if processed_work_items > 0:
                db.update_sync_status('work_item', processed_work_items)

            if processed_sprints > 0:
                db.update_sync_status('sprint', processed_sprints)

            if processed_capacities > 0:
                db.update_sync_status('sprint_capacity', processed_capacities)

            if processed_jira_issues > 0:
                db.update_sync_status('jira_issue', processed_jira_issues)

            if processed_jira_bugs > 0:
                db.update_sync_status('jira_bug', processed_jira_bugs)

            if processed_jira_work_items > 0:
                db.update_sync_status('jira_work_item', processed_jira_work_items)

            if processed_jira_sprints > 0:
                db.update_sync_status('jira_sprint', processed_jira_sprints)

            if processed_jira_capacities > 0:
                db.update_sync_status('jira_sprint_capacity', processed_jira_capacities)

            if processed_copilot > 0:
                db.update_sync_status('copilot_metrics', processed_copilot)

            if processed_prs > 0:
                db.update_sync_status('pr_metrics', processed_prs)

            if processed_tests > 0:
                db.update_sync_status('github_tests', processed_tests)

            if processed_azure_cost > 0:
                db.update_sync_status('azure_cost', processed_azure_cost)

            # Update full sync status if we ran a full sync
            if should_run_full_sync:
                db.update_full_sync_status()
                print("------>Updated full sync timestamp")

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"=== Sync cycle completed at {current_time} ===")
            if run_once:
                print("Single sync cycle completed; exiting")
                break
            print(f"Sleeping for {SLEEP_INTERVAL/60} minutes...\n")
            
            time.sleep(SLEEP_INTERVAL)
            
        except KeyboardInterrupt:
            print("\nReceived interrupt signal. Shutting down gracefully...")
            break
        except Exception as e:
            print(f"\nError during processing: {str(e)}")
            print(f"\nError during processing: {e}")
            print("Will retry in the next cycle")
            print(f"Sleeping for {SLEEP_INTERVAL/60} minutes...")
            time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    main()