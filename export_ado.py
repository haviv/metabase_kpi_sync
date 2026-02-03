import requests
import csv
import base64
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, text, Table, Column, Integer, String, DateTime, MetaData, ForeignKey, inspect, Float
from sqlalchemy.dialects.postgresql import TEXT as PG_TEXT
from sqlalchemy.exc import SQLAlchemyError
import json
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

    def _add_new_columns(self, connection, table_name):
        """Add new columns to existing tables if they don't exist"""
        inspector = inspect(self.engine)
        existing_columns = [col['name'] for col in inspector.get_columns(table_name)]
        
        new_columns = {
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
                'business_value': 'INTEGER'
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

    def setup_tables(self):
        """Create tables if they don't exist and add new columns if needed"""
        try:
            text_type = self._get_text_type()

            # Issues table
            self.issues = Table(
                'issues', self.metadata,
                Column('id', Integer, primary_key=True),
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
                Column('business_value', Integer, nullable=True)
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
                
                if 'bugs' in existing_tables:
                    self._add_new_columns(connection, 'bugs')
                
                if 'work_items' in existing_tables:
                    self._add_new_columns(connection, 'work_items')
                
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
                    
                    self._create_index(connection, "idx_bugs_changed_date", "bugs", "changed_date")
                    self._create_index(connection, "idx_bugs_parent_issue", "bugs", "parent_issue")
                    
                    self._create_index(connection, "idx_sync_status_entity_type", "sync_status", "entity_type, last_sync_time DESC")
                    
                    self._create_index(connection, "idx_bugs_relations_bug_id", "bugs_relations", "bug_id")
                    
                    # Add indexes for work_items table
                    self._create_index(connection, "idx_work_items_changed_date", "work_items", "changed_date")
                    self._create_index(connection, "idx_work_items_type", "work_items", "work_item_type")
                    
                    # Add indexes for sprints table
                    self._create_index(connection, "idx_sprints_name", "sprints", "name")
                    self._create_index(connection, "idx_sprints_start_date", "sprints", "start_date")
                    self._create_index(connection, "idx_sprints_finish_date", "sprints", "finish_date")
                    
                    # Add indexes for sprint_capacity table
                    self._create_index(connection, "idx_sprint_capacity_sprint_id", "sprint_capacity", "sprint_id")
                    self._create_index(connection, "idx_sprint_capacity_team_member", "sprint_capacity", "team_member_id")
                    self._create_index(connection, "idx_sprint_capacity_composite", "sprint_capacity", "sprint_id, team_member_id, activity")
                    
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
            ('open_p1_bugs_with_customer_issues_cn', "SELECT COUNT(*) FROM bugs WHERE state NOT IN ('Done', 'Not reproduced', 'QA Completed', 'Removed') AND severity = '1' AND customer_name IS NOT NULL AND created_date >= '2025-12-01' AND customer_name <> ''"),
            ('p1_p2_bugs_with_customer_issues_cn', "SELECT COUNT(*) FROM bugs WHERE state NOT IN ('Done', 'Not reproduced', 'QA Completed', 'Removed') AND severity IN ('1','2') AND customer_name IS NOT NULL AND created_date >= '2025-12-01' AND customer_name <> ''")
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
                        if item[date_field]:
                            try:
                                # Try parsing with milliseconds
                                item[date_field] = datetime.strptime(item[date_field], "%Y-%m-%dT%H:%M:%S.%fZ")
                            except ValueError:
                                try:
                                    # Try parsing without milliseconds
                                    item[date_field] = datetime.strptime(item[date_field], "%Y-%m-%dT%H:%M:%SZ")
                                except ValueError as e:
                                    print(f"Error parsing date {item[date_field]}: {str(e)}")
                                    raise

                    # Check if item exists
                    result = connection.execute(
                        text(f"SELECT 1 FROM {table.name} WHERE id = :id"),
                        {"id": item['ID']}
                    ).first()

                    if result:
                        # Update existing item
                        update_stmt = f"""
                        UPDATE {table.name}
                        SET title = :title,
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
                            update_stmt += ", work_item_type = :work_item_type, effort = :effort, effort_dev_estimate = :effort_dev_estimate, effort_dev_actual = :effort_dev_actual, qa_effort_estimation = :qa_effort_estimation, qa_effort_actual = :qa_effort_actual, tshirt_estimation = :tshirt_estimation, parent_work_item = :parent_work_item, ticket_type = :ticket_type, freshdesk_ticket = :freshdesk_ticket, target_version = :target_version, tags = :tags, connector = :connector, created_by = :created_by, blocker = :blocker, business_value = :business_value"
                        
                        update_stmt += " WHERE id = :id"

                        params = {
                            "id": item['ID'],
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

                        connection.execute(text(update_stmt), params)
                    else:
                        # Handle identity insert for the specific database
                        self._handle_identity_insert(connection, table.name, True)
                        
                        # Insert new item
                        insert_stmt = f"""
                        INSERT INTO {table.name} (
                            id, title, description, assigned_to, severity,
                            state, customer_name, area_path, created_date, changed_date,
                            iteration_path, hotfix_delivered_version, target_date, hf_status, hf_requested_versions
                        """
                        
                        if item_type == 'bug':
                            insert_stmt += ", parent_issue"
                        elif item_type == 'work_item':
                            insert_stmt += ", work_item_type, effort, effort_dev_estimate, effort_dev_actual, qa_effort_estimation, qa_effort_actual, tshirt_estimation, parent_work_item, ticket_type, freshdesk_ticket, target_version, tags, connector, created_by, blocker, business_value"
                        
                        insert_stmt += """
                        )
                        VALUES (
                            :id, :title, :description, :assigned_to, :severity,
                            :state, :customer_name, :area_path, :created_date, :changed_date,
                            :iteration_path, :hotfix_delivered_version, :target_date, :hf_status, :hf_requested_versions
                        """
                        
                        if item_type == 'bug':
                            insert_stmt += ", :parent_issue"
                        elif item_type == 'work_item':
                            insert_stmt += ", :work_item_type, :effort, :effort_dev_estimate, :effort_dev_actual, :qa_effort_estimation, :qa_effort_actual, :tshirt_estimation, :parent_work_item, :ticket_type, :freshdesk_ticket, :target_version, :tags, :connector, :created_by, :blocker, :business_value"
                        
                        insert_stmt += ")"

                        params = {
                            "id": item['ID'],
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
                    # Check if sprint exists
                    result = connection.execute(
                        text("SELECT id FROM sprints WHERE id = :id"),
                        {"id": sprint['id']}
                    ).first()

                    if result:
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
                                "id": sprint['id'],
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
                                "id": sprint['id'],
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
                                        freshdesk_ticket, target_version, tags, connector, created_by, blocker, business_value
                                    ) VALUES (
                                        :id, :title, :description, :assigned_to, :severity,
                                        :state, :customer_name, :area_path, :created_date, :changed_date,
                                        :iteration_path, :hotfix_delivered_version, :work_item_type, :target_date, :hf_status, :hf_requested_versions,
                                        :effort, :effort_dev_estimate, :effort_dev_actual, :qa_effort_estimation, :qa_effort_actual, :tshirt_estimation, :parent_work_item, :ticket_type,
                                        :freshdesk_ticket, :target_version, :tags, :connector, :created_by, :blocker, :business_value
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
                                    "business_value": business_value
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

def main():
    # Load configuration from .env file
    organization = os.getenv('ADO_ORGANIZATION')
    project = os.getenv('ADO_PROJECT')
    scrum_project = os.getenv('ADO_SCRUM_PROJECT')
    pat = os.getenv('ADO_PERSONAL_ACCESS_TOKEN')
    
    if not all([organization, project, pat]):
        raise ValueError("Please check your .env file and ensure ADO_ORGANIZATION, ADO_PROJECT, and ADO_PERSONAL_ACCESS_TOKEN are set")

    if pat == "your-pat-token-here":
        raise ValueError("Please update the ADO_PERSONAL_ACCESS_TOKEN in your .env file with your actual PAT")

    # Initialize extractor and database connection
    extractor = ADOExtractor(organization, project, pat, scrum_project)
    db = get_database_connection()
    
    # Set the database connection for the extractor
    extractor.db_connection = db

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

    SLEEP_INTERVAL = 60 * 60  # 1 hour in seconds
    
    print(f"Starting continuous sync with {SLEEP_INTERVAL/60} minute intervals")
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
            
            # Get last sync times from database
            issues_last_sync = db.get_last_sync_time('issue')
            bugs_last_sync = db.get_last_sync_time('bug')
            work_items_last_sync = db.get_last_sync_time('work_item')
            
            # Override with full sync date if needed
            if should_run_full_sync:
                issues_last_sync = sync_date_override
                bugs_last_sync = sync_date_override
                work_items_last_sync = sync_date_override
                print(f"------>Using sync date: {sync_date_override.strftime('%Y-%m-%d')} (1 month back)")
            
            # Extract and store all work items first
            work_items = extractor.get_all_work_items(work_items_last_sync.strftime('%Y-%m-%d'))
            processed_work_items = db.upsert_items(work_items, db.work_items, 'work_item')
            print(f"------>Processed {processed_work_items} work items")

            # Process state changes for all work items
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
            print("------>Processing bug state changes")
            extractor.handle_bug_changes(bugs, db)
            print(f"------>Processed state changes for {len(bugs)} bugs")

            # Clean up invalid parent issue references
            print("------>Cleaning up invalid parent issue references")
            db.update_parent_issue()

            # Update sprints
            processed_sprints = extractor.update_sprints()

            # Update sprint capacities
            # Check if this is the first sprint capacity sync
            last_capacity_sync = db.get_last_sprint_capacity_sync()
            if last_capacity_sync is None:
                # First run - sync all historical sprint capacities
                print("------>First sprint capacity sync - fetching all historical data")
                processed_capacities = extractor.update_sprint_capacities(current_sprint_only=False)
            else:
                # Daily run - only sync current sprint capacity
                print("------>Daily sprint capacity sync - fetching current sprint only")
                processed_capacities = extractor.update_sprint_capacities(current_sprint_only=True)

            # Update GitHub Copilot metrics (if configured)
            # Copilot metrics sync runs DAILY (API provides daily data)
            processed_copilot = 0
            if COPILOT_AVAILABLE:
                github_org = os.getenv('GITHUB_ORG')
                github_teams = os.getenv('GITHUB_TEAMS')
                github_team_sizes = os.getenv('GITHUB_TEAM_SIZES')
                
                if github_org and os.getenv('GITHUB_TOKEN'):
                    # Check if Copilot metrics already synced today
                    last_copilot_sync = db.get_last_sync_time('copilot_metrics')
                    today = datetime.now().date()
                    last_sync_date = last_copilot_sync.date() if last_copilot_sync and last_copilot_sync != datetime(2025, 3, 1) else None
                    
                    if last_sync_date == today:
                        print("------>Skipping Copilot metrics (already synced today)")
                    else:
                        print("------>Syncing GitHub Copilot metrics (daily sync)")
                        try:
                            copilot_extractor = GitHubCopilotExtractor(
                                organization=github_org,
                                teams=github_teams,
                                team_sizes=github_team_sizes
                            )
                            processed_copilot = copilot_extractor.sync_to_database(db)
                        except Exception as e:
                            print(f"------>Error syncing Copilot metrics: {str(e)}")
                else:
                    print("------>Skipping Copilot metrics (GITHUB_ORG or GITHUB_TOKEN not configured)")

            # Update GitHub PR metrics (if configured)
            # PR metrics sync runs DAILY to avoid excessive API calls
            processed_prs = 0
            if PR_METRICS_AVAILABLE:
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
            if GITHUB_TESTS_AVAILABLE:
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
                            processed_tests = tests_extractor.sync_test_runs(days_back=1)
                    except Exception as e:
                        print(f"------>Error syncing GitHub tests: {str(e)}")
                else:
                    print("------>Skipping GitHub tests (GITHUB_TOKEN or GITHUB_TEST_CONFIGS not configured)")

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

            if processed_copilot > 0:
                db.update_sync_status('copilot_metrics', processed_copilot)

            if processed_prs > 0:
                db.update_sync_status('pr_metrics', processed_prs)

            if processed_tests > 0:
                db.update_sync_status('github_tests', processed_tests)

            # Update full sync status if we ran a full sync
            if should_run_full_sync:
                db.update_full_sync_status()
                print("------>Updated full sync timestamp")

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"=== Sync cycle completed at {current_time} ===")
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