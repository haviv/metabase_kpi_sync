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

# Load environment variables from .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

class JIRADatabaseConnection:
    def __init__(self, schema_name='jira_sync'):
        self.schema_name = schema_name
        self.connection_string = (
            f"postgresql://{os.getenv('PG_USERNAME')}:{os.getenv('PG_PASSWORD')}@"
            f"{os.getenv('PG_HOST')}:{os.getenv('PG_PORT', '5432')}/"
            f"{os.getenv('PG_DATABASE')}"
        )
        self.engine = self._create_engine()
        self.metadata = MetaData(schema=self.schema_name)
        self.setup_schema()
        self.setup_tables()

    def _create_engine(self):
        return create_engine(self.connection_string)

    def _get_text_type(self):
        return PG_TEXT

    def _get_merge_query(self, snapshot_date, metrics):
        # Build the source CTE for all metrics
        source_queries = []
        for name, query in metrics:
            source_queries.append(f"SELECT CURRENT_DATE AS snapshot_date, '{name}' AS name, ({query}) AS number")
        
        source_cte = " UNION ALL ".join(source_queries)
        
        # Build the upsert query using ON CONFLICT
        merge_query = f"""
        WITH source_data AS ({source_cte})
        INSERT INTO {self.schema_name}.history_snapshots (snapshot_date, name, number)
        SELECT snapshot_date, name, number FROM source_data
        ON CONFLICT (snapshot_date, name) 
        DO UPDATE SET number = EXCLUDED.number;
        """
        
        return merge_query

    def _create_index(self, connection, index_name, table_name, columns):
        connection.execute(text(f"""
            CREATE INDEX IF NOT EXISTS {index_name} ON {self.schema_name}.{table_name}({columns})
        """))

    def _drop_index(self, connection, index_name, table_name):
        connection.execute(text(f"""
            DROP INDEX IF EXISTS {index_name}
        """))

    def _migrate_tables(self):
        """Migrate existing tables to add new columns for JIRA integration"""
        try:
            with self.engine.connect() as connection:
                inspector = inspect(self.engine)
                existing_tables = inspector.get_table_names(schema=self.schema_name)
                
                # Define new columns to add to bugs table
                bugs_new_columns = [
                    ('test_iterations', 'VARCHAR(500)'),
                    ('bugs_found', 'INTEGER'),
                    ('regression', 'VARCHAR(100)'),
                    ('time_spent', 'INTEGER'),
                    ('fix_versions', 'VARCHAR(1000)'),
                    ('tester', 'VARCHAR(200)'),
                    ('story_points', 'FLOAT'),
                    ('labels', 'VARCHAR(1000)'),
                    ('worklog_total_time_spent', 'VARCHAR(50)'),
                    ('worklog_entries_count', 'INTEGER'),
                    ('developer', 'VARCHAR(200)')
                ]
                
                # Define new columns to add to work_items table
                work_items_new_columns = [
                    ('test_iterations', 'VARCHAR(500)'),
                    ('bugs_found', 'INTEGER'),
                    ('regression', 'VARCHAR(100)'),
                    ('time_spent', 'INTEGER'),
                    ('fix_versions', 'VARCHAR(1000)'),
                    ('tester', 'VARCHAR(200)'),
                    ('story_points', 'FLOAT'),
                    ('labels', 'VARCHAR(1000)'),
                    ('worklog_total_time_spent', 'VARCHAR(50)'),
                    ('worklog_entries_count', 'INTEGER'),
                    ('developer', 'VARCHAR(200)')
                ]
                
                # Migrate bugs table
                if 'bugs' in existing_tables:
                    print("------>Migrating bugs table...")
                    for column_name, column_type in bugs_new_columns:
                        try:
                            # Check if column already exists
                            existing_columns = [col['name'] for col in inspector.get_columns('bugs', schema=self.schema_name)]
                            if column_name not in existing_columns:
                                connection.execute(text(f"""
                                    ALTER TABLE {self.schema_name}.bugs 
                                    ADD COLUMN {column_name} {column_type}
                                """))
                                print(f"  Added column: {column_name}")
                        except Exception as e:
                            print(f"  Warning: Could not add column {column_name}: {str(e)}")
                
                # Migrate work_items table
                if 'work_items' in existing_tables:
                    print("------>Migrating work_items table...")
                    for column_name, column_type in work_items_new_columns:
                        try:
                            # Check if column already exists
                            existing_columns = [col['name'] for col in inspector.get_columns('work_items', schema=self.schema_name)]
                            if column_name not in existing_columns:
                                connection.execute(text(f"""
                                    ALTER TABLE {self.schema_name}.work_items 
                                    ADD COLUMN {column_name} {column_type}
                                """))
                                print(f"  Added column: {column_name}")
                        except Exception as e:
                            print(f"  Warning: Could not add column {column_name}: {str(e)}")
                
                # Migrate time_spent column from VARCHAR to INTEGER if it exists
                for table_name in ['bugs', 'work_items']:
                    if table_name in existing_tables:
                        try:
                            # Check if time_spent column exists and is VARCHAR
                            existing_columns = [col['name'] for col in inspector.get_columns(table_name, schema=self.schema_name)]
                            if 'time_spent' in existing_columns:
                                # Check current data type
                                column_info = inspector.get_columns(table_name, schema=self.schema_name)
                                time_spent_col = next((col for col in column_info if col['name'] == 'time_spent'), None)
                                if time_spent_col and time_spent_col['type'].__class__.__name__ == 'VARCHAR':
                                    print(f"  Converting time_spent column in {table_name} from VARCHAR to INTEGER...")
                                    # First, try to convert existing data
                                    connection.execute(text(f"""
                                        UPDATE {self.schema_name}.{table_name} 
                                        SET time_spent = CASE 
                                            WHEN time_spent ~ '^[0-9]+$' THEN time_spent::INTEGER
                                            ELSE NULL
                                        END
                                        WHERE time_spent IS NOT NULL
                                    """))
                                    # Then alter the column type
                                    connection.execute(text(f"""
                                        ALTER TABLE {self.schema_name}.{table_name} 
                                        ALTER COLUMN time_spent TYPE INTEGER USING time_spent::INTEGER
                                    """))
                                    print(f"  Converted time_spent column in {table_name} to INTEGER")
                        except Exception as e:
                            print(f"  Warning: Could not convert time_spent column in {table_name}: {str(e)}")
                
                connection.commit()
                print("------>Schema migration completed")
                
        except Exception as e:
            print(f"Error during schema migration: {str(e)}")
            # Don't raise the exception as this shouldn't stop the sync process

    def setup_schema(self):
        """Create the JIRA schema if it doesn't exist"""
        try:
            with self.engine.connect() as connection:
                connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}"))
                connection.commit()
        except SQLAlchemyError as e:
            print(f"Error creating schema: {str(e)}")
            raise

    def setup_tables(self):
        """Create tables in the JIRA schema if they don't exist and migrate types"""
        try:
            text_type = self._get_text_type()

            # Attempt to migrate existing tables/columns to desired types
            with self.engine.connect() as connection:
                inspector = inspect(self.engine)
                existing_tables = inspector.get_table_names(schema=self.schema_name)

                if 'bugs' in existing_tables:
                    # Ensure id is VARCHAR and add numeric_id if missing; ensure parent_issue is VARCHAR
                    connection.execute(text(f"""
                        DO $$
                        BEGIN
                            IF EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_schema = '{self.schema_name}' AND table_name = 'bugs' AND column_name = 'id' AND data_type <> 'character varying'
                            ) THEN
                                ALTER TABLE {self.schema_name}.bugs ALTER COLUMN id TYPE VARCHAR(50) USING id::text;
                            END IF;

                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_schema = '{self.schema_name}' AND table_name = 'bugs' AND column_name = 'numeric_id'
                            ) THEN
                                ALTER TABLE {self.schema_name}.bugs ADD COLUMN numeric_id INTEGER;
                            END IF;

                            IF EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_schema = '{self.schema_name}' AND table_name = 'bugs' AND column_name = 'parent_issue' AND data_type <> 'character varying'
                            ) THEN
                                ALTER TABLE {self.schema_name}.bugs ALTER COLUMN parent_issue TYPE VARCHAR(50) USING parent_issue::text;
                            END IF;
                        END $$;
                    """))

                if 'bugs_relations' in existing_tables:
                    # Ensure relation ids are VARCHAR
                    connection.execute(text(f"""
                        DO $$
                        BEGIN
                            IF EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_schema = '{self.schema_name}' AND table_name = 'bugs_relations' AND column_name = 'bug_id' AND data_type <> 'character varying'
                            ) THEN
                                ALTER TABLE {self.schema_name}.bugs_relations ALTER COLUMN bug_id TYPE VARCHAR(50) USING bug_id::text;
                            END IF;
                            IF EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_schema = '{self.schema_name}' AND table_name = 'bugs_relations' AND column_name = 'issue_id' AND data_type <> 'character varying'
                            ) THEN
                                ALTER TABLE {self.schema_name}.bugs_relations ALTER COLUMN issue_id TYPE VARCHAR(50) USING issue_id::text;
                            END IF;
                        END $$;
                    """))

                if 'change_history' in existing_tables:
                    # Ensure record_id is VARCHAR
                    connection.execute(text(f"""
                        DO $$
                        BEGIN
                            IF EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_schema = '{self.schema_name}' AND table_name = 'change_history' AND column_name = 'record_id' AND data_type <> 'character varying'
                            ) THEN
                                ALTER TABLE {self.schema_name}.change_history ALTER COLUMN record_id TYPE VARCHAR(50) USING record_id::text;
                            END IF;
                        END $$;
                    """))

                connection.commit()

            # Bugs table with updated structure for JIRA (string ids + numeric_id)
            self.bugs = Table(
                'bugs', self.metadata,
                Column('id', String(50), primary_key=True),
                Column('numeric_id', Integer, nullable=True),
                Column('parent_issue', String(50), nullable=True),
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
                # New fields for JIRA integration
                Column('test_iterations', String(500), nullable=True),
                Column('bugs_found', Integer, nullable=True),
                Column('regression', String(100), nullable=True),
                Column('time_spent', Integer, nullable=True),
                Column('fix_versions', String(1000), nullable=True),
                Column('tester', String(200), nullable=True),
                Column('story_points', Float, nullable=True),
                Column('labels', String(1000), nullable=True),
                Column('worklog_total_time_spent', String(50), nullable=True),
                Column('worklog_entries_count', Integer, nullable=True),
                Column('developer', String(200), nullable=True),
                schema=self.schema_name
            )

            # Bugs relations table
            self.bugs_relations = Table(
                'bugs_relations', self.metadata,
                Column('bug_id', String(50), nullable=False),
                Column('issue_id', String(50), nullable=False),
                Column('type', String(50), nullable=False),
                schema=self.schema_name
            )

            # Sync status table
            self.sync_status = Table(
                'sync_status', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('entity_type', String(50), nullable=False),
                Column('last_sync_time', DateTime, nullable=False),
                Column('status', String(50), nullable=False),
                Column('records_processed', Integer, nullable=False, default=0),
                schema=self.schema_name
            )

            # Change history table
            self.change_history = Table(
                'change_history', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('record_id', String(50), nullable=False),
                Column('table_name', String(50), nullable=False),
                Column('field_changed', String(100), nullable=False),
                Column('old_value', String(500), nullable=True),
                Column('new_value', String(500), nullable=True),
                Column('changed_by', String(200), nullable=True),
                Column('changed_date', DateTime, nullable=True, server_default=text('CURRENT_TIMESTAMP')),
                schema=self.schema_name
            )

            # History snapshots table
            self.history_snapshots = Table(
                'history_snapshots', self.metadata,
                Column('snapshot_date', DateTime, primary_key=True),
                Column('name', String(255), primary_key=True),
                Column('number', Float, nullable=False),
                schema=self.schema_name
            )

            # Sprints table
            self.sprints = Table(
                'sprints', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String(500), nullable=False),
                Column('state', String(50), nullable=True),
                Column('start_date', DateTime, nullable=True),
                Column('end_date', DateTime, nullable=True),
                Column('complete_date', DateTime, nullable=True),
                Column('created_date', DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP')),
                Column('updated_date', DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP')),
                schema=self.schema_name
            )

            # Work items table for stories and other issue types
            self.work_items = Table(
                'work_items', self.metadata,
                Column('id', String(50), primary_key=True),
                Column('numeric_id', Integer, nullable=True),
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
                # New fields for JIRA integration
                Column('test_iterations', String(500), nullable=True),
                Column('bugs_found', Integer, nullable=True),
                Column('regression', String(100), nullable=True),
                Column('time_spent', Integer, nullable=True),
                Column('fix_versions', String(1000), nullable=True),
                Column('tester', String(200), nullable=True),
                Column('story_points', Float, nullable=True),
                Column('labels', String(1000), nullable=True),
                Column('worklog_total_time_spent', String(50), nullable=True),
                Column('worklog_entries_count', Integer, nullable=True),
                Column('developer', String(200), nullable=True),
                schema=self.schema_name
            )

            # Create tables if they don't exist
            self.metadata.create_all(self.engine, checkfirst=True)
            
            # Migrate existing tables to add new columns
            self._migrate_tables()

            # Create indexes
            with self.engine.connect() as connection:
                try:
                    self._create_index(connection, "idx_bugs_changed_date", "bugs", "changed_date")
                    self._create_index(connection, "idx_bugs_parent_issue", "bugs", "parent_issue")
                    self._create_index(connection, "idx_sync_status_entity_type", "sync_status", "entity_type, last_sync_time DESC")
                    self._create_index(connection, "idx_bugs_relations_bug_id", "bugs_relations", "bug_id")
                    self._create_index(connection, "idx_work_items_changed_date", "work_items", "changed_date")
                    self._create_index(connection, "idx_work_items_type", "work_items", "work_item_type")
                    self._create_index(connection, "idx_sprints_name", "sprints", "name")
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
                text(f"""
                    SELECT last_sync_time 
                    FROM {self.schema_name}.sync_status 
                    WHERE entity_type = :entity_type 
                    ORDER BY last_sync_time DESC
                    LIMIT 1
                """),
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
                text(f"""
                    SELECT last_sync_time 
                    FROM {self.schema_name}.sync_status 
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
                    text(f"""
                        INSERT INTO {self.schema_name}.sync_status (entity_type, last_sync_time, status, records_processed)
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
                    text(f"""
                        INSERT INTO {self.schema_name}.sync_status (entity_type, last_sync_time, status, records_processed)
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

    def upsert_sprints(self, sprints):
        """Upsert sprints into the sprints table"""
        processed_count = 0
        with self.engine.connect() as connection:
            for sprint in sprints:
                try:
                    # Check if sprint exists
                    result = connection.execute(
                        text(f"SELECT id FROM {self.schema_name}.sprints WHERE id = :id"),
                        {"id": sprint['id']}
                    ).first()

                    if result:
                        # Update existing sprint
                        connection.execute(
                            text(f"""
                                UPDATE {self.schema_name}.sprints 
                                SET name = :name, 
                                    state = :state,
                                    start_date = :start_date, 
                                    end_date = :end_date,
                                    complete_date = :complete_date,
                                    updated_date = CURRENT_TIMESTAMP
                                WHERE id = :id
                            """),
                            {
                                "id": sprint['id'],
                                "name": sprint['name'],
                                "state": sprint['state'],
                                "start_date": sprint['start_date'],
                                "end_date": sprint['end_date'],
                                "complete_date": sprint['complete_date']
                            }
                        )
                    else:
                        # Insert new sprint
                        connection.execute(
                            text(f"""
                                INSERT INTO {self.schema_name}.sprints (id, name, state, start_date, end_date, complete_date)
                                VALUES (:id, :name, :state, :start_date, :end_date, :complete_date)
                            """),
                            {
                                "id": sprint['id'],
                                "name": sprint['name'],
                                "state": sprint['state'],
                                "start_date": sprint['start_date'],
                                "end_date": sprint['end_date'],
                                "complete_date": sprint['complete_date']
                            }
                        )

                    connection.commit()
                    processed_count += 1
                except SQLAlchemyError as e:
                    print(f"Error upserting sprint {sprint.get('id', 'unknown')}: {str(e)}")
                    connection.rollback()

        return processed_count

    def update_history_snapshots(self):
        """Update the history snapshots table with current metrics"""
        metrics = [
            ('total_bugs', f'SELECT COUNT(*) FROM {self.schema_name}.bugs'),
            ('open_bugs', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state NOT IN ('DONE')"),
            ('new_bugs', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state IN ('New Request')"),
            ('closed_bugs', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state IN ('DONE')"),
            ('reopened_bugs', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state = 'New Request' AND changed_date >= CURRENT_DATE - INTERVAL '1 day'"),
            ('p1_bugs', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE severity = '1 - Critical' AND state NOT IN ('DONE')"),
            ('p2_bugs', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE severity = '2 - High' AND state NOT IN ('DONE')"),
            ('p1_p2_bugs_with_customer_issues', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state NOT IN ('DONE') AND severity IN ('1 - Critical','2 - High') AND parent_issue IS NOT NULL"),
            ('p1_p2_bugs_in_dev', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state IN ('New Request', 'IN-PROGRESS', 'In Review') AND severity IN ('1 - Critical','2 - High')"),
            ('open_bugs_QA_p1_p2', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state IN ('READY FOR QA', 'IN QA') AND severity IN ('1 - Critical','2 - High')"),
            ('total_customers', f"SELECT COUNT(DISTINCT customer_name) AS total_customers FROM {self.schema_name}.bugs WHERE customer_name IS NOT NULL AND customer_name != ''"),
            ('redline_bugs', f"SELECT COALESCE(COUNT(*) * 1.0 / NULLIF((SELECT COUNT(DISTINCT customer_name) FROM {self.schema_name}.bugs WHERE customer_name IS NOT NULL AND customer_name != ''), 0), 0) AS bugs_with_parent_per_customer FROM {self.schema_name}.bugs WHERE parent_issue IS NOT NULL AND DATE_TRUNC('month', created_date) = DATE_TRUNC('month', CURRENT_DATE)"),
            ('open_p1_bugs_with_customer_issues', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state NOT IN ('DONE') AND severity = '1 - Critical' AND parent_issue IS NOT NULL"),
            ('avg_days_to_close_p1_p2', f"WITH first_inprog AS (SELECT record_id, MIN(changed_date) AS first_inprog_at FROM {self.schema_name}.change_history WHERE table_name = 'bugs' AND field_changed = 'System.State' AND REPLACE(UPPER(new_value), '-', ' ') = 'IN PROGRESS' AND changed_date >= DATE_TRUNC('month', NOW() - INTERVAL '6 months') AND changed_date < DATE_TRUNC('month', NOW()) GROUP BY record_id) SELECT AVG(EXTRACT(EPOCH FROM (COALESCE(f.first_inprog_at, now()) - b.created_date)) / 86400.0) AS avg_days_create_to_inprog_including_open FROM {self.schema_name}.bugs b INNER JOIN first_inprog f ON f.record_id = b.id WHERE b.customer_name IS NOT NULL AND b.severity IN ('1 - Critical','2 - High')")
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
                    text(f"DELETE FROM {self.schema_name}.bugs_relations WHERE bug_id = :bug_id"),
                    {"bug_id": bug_id}
                )

                # Process each relation
                for relation in relations:
                    rel_type = relation.get('type')
                    # Only process parent and related relations
                    if rel_type in ['parent', 'related']:
                        issue_id = relation.get('issue_id')
                        if issue_id:
                            # Insert the new relation
                            connection.execute(
                                text(f"""
                                    INSERT INTO {self.schema_name}.bugs_relations (bug_id, issue_id, type)
                                    VALUES (:bug_id, :issue_id, :type)
                                """),
                                {
                                    "bug_id": bug_id,
                                    "issue_id": issue_id,
                                    "type": rel_type
                                }
                            )

                connection.commit()
                
        except SQLAlchemyError as e:
            print(f"Error updating relations for bug {bug_id}: {str(e)}")
            connection.rollback()
            raise

    def upsert_bugs(self, bugs):
        """Upsert bugs into the bugs table"""
        processed_count = 0
        with self.engine.connect() as connection:
            for bug in bugs:
                try:
                    # Convert dates from string to datetime
                    for date_field in ['created_date', 'changed_date']:
                        if bug[date_field]:
                            try:
                                # Try parsing with milliseconds
                                bug[date_field] = datetime.strptime(bug[date_field], "%Y-%m-%dT%H:%M:%S.%f%z")
                            except ValueError:
                                try:
                                    # Try parsing without milliseconds
                                    bug[date_field] = datetime.strptime(bug[date_field], "%Y-%m-%dT%H:%M:%S%z")
                                except ValueError:
                                    try:
                                        # Try parsing without timezone
                                        bug[date_field] = datetime.strptime(bug[date_field], "%Y-%m-%dT%H:%M:%S")
                                    except ValueError as e:
                                        print(f"Error parsing date {bug[date_field]}: {str(e)}")
                                        raise

                    # Check if bug exists
                    result = connection.execute(
                        text(f"SELECT 1 FROM {self.schema_name}.bugs WHERE id = :id"),
                        {"id": bug['id']}
                    ).first()

                    if result:
                        # Update existing bug
                        connection.execute(
                            text(f"""
                            UPDATE {self.schema_name}.bugs
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
                                parent_issue = :parent_issue,
                                numeric_id = :numeric_id,
                                test_iterations = :test_iterations,
                                bugs_found = :bugs_found,
                                regression = :regression,
                                time_spent = :time_spent,
                                fix_versions = :fix_versions,
                                tester = :tester,
                                story_points = :story_points,
                                labels = :labels,
                                worklog_total_time_spent = :worklog_total_time_spent,
                                worklog_entries_count = :worklog_entries_count,
                                developer = :developer
                            WHERE id = :id
                            """),
                            {
                                "id": bug['id'],
                                "title": bug['title'],
                                "description": bug['description'],
                                "assigned_to": bug['assigned_to'],
                                "severity": bug['severity'],
                                "state": bug['state'],
                                "customer_name": bug['customer_name'],
                                "area_path": bug['area_path'],
                                "created_date": bug['created_date'],
                                "changed_date": bug['changed_date'],
                                "iteration_path": bug['iteration_path'],
                                "hotfix_delivered_version": bug['hotfix_delivered_version'],
                                "parent_issue": bug.get('parent_issue'),
                                "numeric_id": bug.get('numeric_id'),
                                "test_iterations": bug.get('test_iterations'),
                                "bugs_found": bug.get('bugs_found'),
                                "regression": bug.get('regression'),
                                "time_spent": bug.get('time_spent'),
                                "fix_versions": bug.get('fix_versions'),
                                "tester": bug.get('tester'),
                                "story_points": bug.get('story_points'),
                                "labels": bug.get('labels'),
                                "worklog_total_time_spent": bug.get('worklog_total_time_spent'),
                                "worklog_entries_count": bug.get('worklog_entries_count'),
                                "developer": bug.get('developer')
                            }
                        )
                    else:
                        # Insert new bug
                        connection.execute(
                            text(f"""
                            INSERT INTO {self.schema_name}.bugs (
                                id, numeric_id, title, description, assigned_to, severity,
                                state, customer_name, area_path, created_date, changed_date,
                                iteration_path, hotfix_delivered_version, parent_issue,
                                test_iterations, bugs_found, regression,
                                time_spent,
                                fix_versions, tester, story_points, labels,
                                worklog_total_time_spent, worklog_entries_count, developer
                            ) VALUES (
                                :id, :numeric_id, :title, :description, :assigned_to, :severity,
                                :state, :customer_name, :area_path, :created_date, :changed_date,
                                :iteration_path, :hotfix_delivered_version, :parent_issue,
                                :test_iterations, :bugs_found, :regression,
                                :time_spent,
                                :fix_versions, :tester, :story_points, :labels,
                                :worklog_total_time_spent, :worklog_entries_count, :developer
                            )
                            """),
                            {
                                "id": bug['id'],
                                "numeric_id": bug.get('numeric_id'),
                                "title": bug['title'],
                                "description": bug['description'],
                                "assigned_to": bug['assigned_to'],
                                "severity": bug['severity'],
                                "state": bug['state'],
                                "customer_name": bug['customer_name'],
                                "area_path": bug['area_path'],
                                "created_date": bug['created_date'],
                                "changed_date": bug['changed_date'],
                                "iteration_path": bug['iteration_path'],
                                "hotfix_delivered_version": bug['hotfix_delivered_version'],
                                "parent_issue": bug.get('parent_issue'),
                                "test_iterations": bug.get('test_iterations'),
                                "bugs_found": bug.get('bugs_found'),
                                "regression": bug.get('regression'),
                                "time_spent": bug.get('time_spent'),
                                "fix_versions": bug.get('fix_versions'),
                                "tester": bug.get('tester'),
                                "story_points": bug.get('story_points'),
                                "labels": bug.get('labels'),
                                "worklog_total_time_spent": bug.get('worklog_total_time_spent'),
                                "worklog_entries_count": bug.get('worklog_entries_count'),
                                "developer": bug.get('developer')
                            }
                        )

                    connection.commit()
                    processed_count += 1
                except SQLAlchemyError as e:
                    print(f"Error upserting bug {bug['id']}: {str(e)}")
                    connection.rollback()

        return processed_count

    def upsert_work_items(self, work_items):
        """Upsert work items into the work_items table"""
        processed_count = 0
        with self.engine.connect() as connection:
            for item in work_items:
                try:
                    # Convert dates from string to datetime
                    for date_field in ['created_date', 'changed_date']:
                        if item[date_field]:
                            try:
                                # Try parsing with milliseconds
                                item[date_field] = datetime.strptime(item[date_field], "%Y-%m-%dT%H:%M:%S.%f%z")
                            except ValueError:
                                try:
                                    # Try parsing without milliseconds
                                    item[date_field] = datetime.strptime(item[date_field], "%Y-%m-%dT%H:%M:%S%z")
                                except ValueError:
                                    try:
                                        # Try parsing without timezone
                                        item[date_field] = datetime.strptime(item[date_field], "%Y-%m-%dT%H:%M:%S")
                                    except ValueError as e:
                                        print(f"Error parsing date {item[date_field]}: {str(e)}")
                                        raise

                    # Check if work item exists
                    result = connection.execute(
                        text(f"SELECT 1 FROM {self.schema_name}.work_items WHERE id = :id"),
                        {"id": item['id']}
                    ).first()

                    if result:
                        # Update existing work item
                        connection.execute(
                            text(f"""
                            UPDATE {self.schema_name}.work_items
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
                                work_item_type = :work_item_type,
                                numeric_id = :numeric_id,
                                test_iterations = :test_iterations,
                                bugs_found = :bugs_found,
                                regression = :regression,
                                time_spent = :time_spent,
                                fix_versions = :fix_versions,
                                tester = :tester,
                                story_points = :story_points,
                                labels = :labels,
                                worklog_total_time_spent = :worklog_total_time_spent,
                                worklog_entries_count = :worklog_entries_count,
                                developer = :developer
                            WHERE id = :id
                            """),
                            {
                                "id": item['id'],
                                "title": item['title'],
                                "description": item['description'],
                                "assigned_to": item['assigned_to'],
                                "severity": item['severity'],
                                "state": item['state'],
                                "customer_name": item['customer_name'],
                                "area_path": item['area_path'],
                                "created_date": item['created_date'],
                                "changed_date": item['changed_date'],
                                "iteration_path": item['iteration_path'],
                                "hotfix_delivered_version": item['hotfix_delivered_version'],
                                "work_item_type": item['work_item_type'],
                                "numeric_id": item.get('numeric_id'),
                                "test_iterations": item.get('test_iterations'),
                                "bugs_found": item.get('bugs_found'),
                                "regression": item.get('regression'),
                                "time_spent": item.get('time_spent'),
                                "fix_versions": item.get('fix_versions'),
                                "tester": item.get('tester'),
                                "story_points": item.get('story_points'),
                                "labels": item.get('labels'),
                                "worklog_total_time_spent": item.get('worklog_total_time_spent'),
                                "worklog_entries_count": item.get('worklog_entries_count'),
                                "developer": item.get('developer')
                            }
                        )
                    else:
                        # Insert new work item
                        connection.execute(
                            text(f"""
                            INSERT INTO {self.schema_name}.work_items (
                                id, numeric_id, title, description, assigned_to, severity,
                                state, customer_name, area_path, created_date, changed_date,
                                iteration_path, hotfix_delivered_version, work_item_type,
                                test_iterations, bugs_found, regression,
                                time_spent,
                                fix_versions, tester, story_points, labels,
                                worklog_total_time_spent, worklog_entries_count, developer
                            ) VALUES (
                                :id, :numeric_id, :title, :description, :assigned_to, :severity,
                                :state, :customer_name, :area_path, :created_date, :changed_date,
                                :iteration_path, :hotfix_delivered_version, :work_item_type,
                                :test_iterations, :bugs_found, :regression,
                                :time_spent,
                                :fix_versions, :tester, :story_points, :labels,
                                :worklog_total_time_spent, :worklog_entries_count, :developer
                            )
                            """),
                            {
                                "id": item['id'],
                                "numeric_id": item.get('numeric_id'),
                                "title": item['title'],
                                "description": item['description'],
                                "assigned_to": item['assigned_to'],
                                "severity": item['severity'],
                                "state": item['state'],
                                "customer_name": item['customer_name'],
                                "area_path": item['area_path'],
                                "created_date": item['created_date'],
                                "changed_date": item['changed_date'],
                                "iteration_path": item['iteration_path'],
                                "hotfix_delivered_version": item['hotfix_delivered_version'],
                                "work_item_type": item['work_item_type'],
                                "test_iterations": item.get('test_iterations'),
                                "bugs_found": item.get('bugs_found'),
                                "regression": item.get('regression'),
                                "time_spent": item.get('time_spent'),
                                "fix_versions": item.get('fix_versions'),
                                "tester": item.get('tester'),
                                "story_points": item.get('story_points'),
                                "labels": item.get('labels'),
                                "worklog_total_time_spent": item.get('worklog_total_time_spent'),
                                "worklog_entries_count": item.get('worklog_entries_count'),
                                "developer": item.get('developer')
                            }
                        )

                    connection.commit()
                    processed_count += 1
                except SQLAlchemyError as e:
                    print(f"Error upserting work item {item['id']}: {str(e)}")
                    connection.rollback()

        return processed_count

def get_database_connection(schema_name='jira_sync'):
    """Factory function to create the appropriate database connection"""
    try:
        return JIRADatabaseConnection(schema_name)
    except ModuleNotFoundError as e:
        if "psycopg2" in str(e):
            raise ModuleNotFoundError(
                "PostgreSQL driver (psycopg2) is not installed. "
                "Please install it using: pip install psycopg2-binary"
            ) from e
        raise

class JIRAExtractor:
    def __init__(self, jira_url, project_key, email, api_token):
        self.jira_url = jira_url.rstrip('/')
        self.project_key = project_key
        self.email = email
        self.api_token = api_token
        # Configurable field IDs
        self.customer_name_field = os.getenv('JIRA_CUSTOMER_NAME_FIELD_ID', 'customfield_10001')
        # Default JIRA Sprint field is commonly customfield_10020
        self.sprint_field = os.getenv('JIRA_SPRINT_FIELD_ID', 'customfield_10020')
        # New custom field IDs for additional fields
        # These can be configured via environment variables in .env file
        # Updated with actual field IDs from Pathlock JIRA instance
        self.test_iterations_field = os.getenv('JIRA_TEST_ITERATIONS_FIELD_ID', 'customfield_10856')
        self.bugs_found_field = os.getenv('JIRA_BUGS_FOUND_FIELD_ID', 'customfield_10955')
        self.regression_field = os.getenv('JIRA_REGRESSION_FIELD_ID', 'customfield_10814')
        self.tester_field = os.getenv('JIRA_TESTER_FIELD_ID', 'customfield_10454')
        self.story_points_field = os.getenv('JIRA_STORY_POINTS_FIELD_ID', 'customfield_10037')  # Updated for Pathlock JIRA
        self.developer_field = os.getenv('JIRA_DEVELOPER_FIELD_ID', 'customfield_10399')  # Developer field
        

        
        # Setup authentication header
        auth_string = f"{email}:{api_token}"
        auth_bytes = auth_string.encode('ascii')
        auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Basic {auth_b64}"
        }

    def _adf_to_text(self, adf_value):
        """Convert JIRA ADF (Atlassian Document Format) to plain text"""
        if not adf_value:
            return ""
        if isinstance(adf_value, str):
            return adf_value

        parts = []

        def walk(node):
            node_type = node.get('type') if isinstance(node, dict) else None
            if not node_type:
                return
            if node_type == 'text':
                text_val = node.get('text', '')
                parts.append(text_val)
                return
            # Recurse into content
            for child in node.get('content', []) or []:
                walk(child)
            # Add line breaks for block-level nodes
            if node_type in ['paragraph', 'heading', 'blockquote', 'listItem']:
                parts.append('\n')
            if node_type == 'hardBreak':
                parts.append('\n')

        # Root document
        if isinstance(adf_value, dict):
            walk(adf_value)
        elif isinstance(adf_value, list):
            for item in adf_value:
                if isinstance(item, dict):
                    walk(item)

        # Collapse multiple newlines and strip
        text = ''.join(parts)
        # Normalize newlines
        text = '\n'.join([line.rstrip() for line in text.splitlines()]).strip()
        return text

    def _extract_custom_field_value(self, fields, field_id, field_type='string'):
        """Extract value from a custom field with proper type handling"""
        raw_value = fields.get(field_id)
        if not raw_value:
            return None
            
        if field_type == 'string':
            if isinstance(raw_value, list) and raw_value:
                if isinstance(raw_value[0], dict) and 'value' in raw_value[0]:
                    return raw_value[0]['value']
                else:
                    return str(raw_value[0])
            elif isinstance(raw_value, dict) and 'value' in raw_value:
                return raw_value['value']
            else:
                return str(raw_value)
        elif field_type == 'number':
            if isinstance(raw_value, (int, float)):
                return raw_value
            elif isinstance(raw_value, str) and raw_value.isdigit():
                return int(raw_value)
            elif isinstance(raw_value, str) and raw_value.replace('.', '').isdigit():
                return float(raw_value)
            return None
        elif field_type == 'user':
            if isinstance(raw_value, dict):
                return raw_value.get('displayName', '')
            elif isinstance(raw_value, list) and raw_value and isinstance(raw_value[0], dict):
                return raw_value[0].get('displayName', '')
            return str(raw_value) if raw_value else ''
        
        return str(raw_value) if raw_value else ''

    def _extract_time_tracking(self, fields):
        """Extract time tracking information - focus on aggregatetimespent in seconds"""
        timetracking = fields.get('timetracking', {})
        # Extract the aggregatetimespent field which contains the total time spent in seconds
        time_spent_seconds = timetracking.get('timeSpentSeconds')
        # Return as integer for database storage, or None if no time tracking data
        time_spent = int(time_spent_seconds) if time_spent_seconds else None
        return {
            'time_spent': time_spent
        }

    def _extract_fix_versions(self, fields):
        """Extract fix versions as comma-separated string"""
        fix_versions = fields.get('fixVersions', [])
        if not fix_versions:
            return ''
        version_names = []
        for version in fix_versions:
            if isinstance(version, dict) and 'name' in version:
                version_names.append(version['name'])
            elif isinstance(version, str):
                version_names.append(version)
        return ', '.join(version_names)

    def _extract_labels(self, fields):
        """Extract labels as comma-separated string"""
        labels = fields.get('labels', [])
        if not labels:
            return ''
        return ', '.join(labels)

    def _extract_worklog_summary(self, fields):
        """Extract worklog summary information"""
        worklog = fields.get('worklog', {})
        if not worklog:
            return {'total_time_spent': '', 'entries_count': 0}
        
        worklogs = worklog.get('worklogs', [])
        total_seconds = 0
        for entry in worklogs:
            time_spent = entry.get('timeSpentSeconds', 0)
            if isinstance(time_spent, (int, float)):
                total_seconds += time_spent
        
        # Convert seconds to readable format
        if total_seconds > 0:
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            if hours > 0:
                time_str = f"{hours}h {minutes}m" if minutes > 0 else f"{hours}h"
            else:
                time_str = f"{minutes}m"
        else:
            time_str = ''
            
        return {
            'total_time_spent': time_str,
            'entries_count': len(worklogs)
        }

    def get_bugs_since_last_sync(self, last_sync_time):
        """Query bugs from JIRA since last sync time with pagination using new JQL API"""
        # Convert last_sync_time to JIRA date format
        if isinstance(last_sync_time, datetime):
            last_sync_str = last_sync_time.strftime('%Y-%m-%d')
        else:
            last_sync_str = last_sync_time
        
        print(f"------>Fetching bugs with updated >= {last_sync_str}")
        
        # JQL query to get bugs updated since last sync
        jql_query = f'project = {self.project_key} AND issuetype = Bug AND updated >= "{last_sync_str}" ORDER BY updated DESC'
        
        # Prepare fields parameter
        fields_param = "summary,description,assignee,priority,status,components,created,updated,parent,timetracking,fixVersions,labels,worklog"
        if self.customer_name_field not in fields_param:
            fields_param += f",{self.customer_name_field}"
        if self.sprint_field not in fields_param:
            fields_param += f",{self.sprint_field}"
        # Add common sprint fallbacks
        if 'customfield_10020' not in fields_param and self.sprint_field != 'customfield_10020':
            fields_param += ",customfield_10020"
        if 'sprint' not in fields_param and self.sprint_field != 'sprint':
            fields_param += ",sprint"
        # Add new custom fields
        if self.test_iterations_field not in fields_param:
            fields_param += f",{self.test_iterations_field}"
        if self.bugs_found_field not in fields_param:
            fields_param += f",{self.bugs_found_field}"
        if self.regression_field not in fields_param:
            fields_param += f",{self.regression_field}"
        if self.tester_field not in fields_param:
            fields_param += f",{self.tester_field}"
        if self.story_points_field not in fields_param:
            fields_param += f",{self.story_points_field}"
        if self.developer_field not in fields_param:
            fields_param += f",{self.developer_field}"
        
        bugs = []
        next_page_token = None
        max_results = 100  # JIRA Cloud recommended page size
        total_fetched = 0
        
        while True:
            # Use the new JQL search endpoint
            search_url = f"{self.jira_url}/rest/api/3/search/jql"
            
            # Prepare query parameters
            query_params = {
                'jql': jql_query,
                'maxResults': max_results,
                'fields': fields_param,
                'expand': 'renderedFields'
            }
            
            # Add nextPageToken if we have one
            if next_page_token:
                query_params['nextPageToken'] = next_page_token
            
            response = requests.get(search_url, headers=self.headers, params=query_params)
            if response.status_code != 200:
                print(f"Error fetching bugs from JIRA: {response.status_code} - {response.text}")
                break

            issues_data = response.json()
            issues = issues_data.get("issues", [])
            
            if not issues:
                break
                
            print(f"------>Fetched {len(issues)} bugs (total so far: {total_fetched + len(issues)})")
            total_fetched += len(issues)

            # Process this batch of issues
            for issue in issues:
                fields = issue["fields"]
            
                # Extract assignee
                assignee = fields.get("assignee")
                assigned_to = assignee.get("displayName") if assignee else ""
                
                # Extract priority as severity
                priority = fields.get("priority")
                severity = priority.get("name") if priority else ""
                
                # Extract status as state
                status = fields.get("status")
                state = status.get("name") if status else ""
                
                # Extract customer name from configurable custom field
                customer_name_raw = fields.get(self.customer_name_field, "")
                # Handle different customer name field formats
                if isinstance(customer_name_raw, list) and customer_name_raw:
                    # If it's a list, extract the 'value' from the first item
                    if isinstance(customer_name_raw[0], dict) and 'value' in customer_name_raw[0]:
                        customer_name = customer_name_raw[0]['value']
                    else:
                        customer_name = str(customer_name_raw[0])
                elif isinstance(customer_name_raw, dict) and 'value' in customer_name_raw:
                    # If it's a dict, extract the 'value'
                    customer_name = customer_name_raw['value']
                else:
                    # Otherwise, convert to string
                    customer_name = str(customer_name_raw) if customer_name_raw else ""
                

                
                # Extract components as area path
                components = fields.get("components", [])
                area_path = ", ".join([comp["name"] for comp in components]) if components else ""
                
                # Extract sprint information (supports multiple data shapes and field fallbacks)
                iteration_path = ""
                sprint_val = fields.get(self.sprint_field)
                if sprint_val is None:
                    sprint_val = fields.get('customfield_10020') or fields.get('sprint')
                if isinstance(sprint_val, dict):
                    iteration_path = sprint_val.get("name", "")
                elif isinstance(sprint_val, list):
                    # Could be list of dicts or list of strings
                    names = []
                    for s in sprint_val:
                        if isinstance(s, dict) and 'name' in s:
                            names.append(s['name'])
                        elif isinstance(s, str):
                            # Parse string like "com.atlassian.greenhopper.service.sprint.Sprint@... [id=1,rapidViewId=2,state=...,name=Sprint 1,...]"
                            import re
                            m = re.search(r"name=([^,\]]+)", s)
                            if m:
                                names.append(m.group(1))
                    iteration_path = ", ".join(names)
                elif isinstance(sprint_val, str):
                    iteration_path = sprint_val
                
                # Extract parent issue (store key)
                parent = fields.get("parent")
                parent_issue = parent.get("key") if parent else None
                
                # Description: prefer ADF to text; fallback to rendered HTML
                description_text = self._adf_to_text(fields.get("description"))
                if not description_text:
                    rendered_desc = issue.get("renderedFields", {}).get("description")
                    if rendered_desc:
                        import re
                        from html import unescape
                        description_text = unescape(re.sub(r"<[^>]+>", "", rendered_desc)).strip()

                # Extract new fields
                test_iterations = self._extract_custom_field_value(fields, self.test_iterations_field, 'string')
                bugs_found = self._extract_custom_field_value(fields, self.bugs_found_field, 'number')
                regression = self._extract_custom_field_value(fields, self.regression_field, 'string')
                tester = self._extract_custom_field_value(fields, self.tester_field, 'user')
                story_points = self._extract_custom_field_value(fields, self.story_points_field, 'number')
                developer = self._extract_custom_field_value(fields, self.developer_field, 'user')
                
                # Extract time tracking
                time_tracking = self._extract_time_tracking(fields)
                
                # Extract fix versions
                fix_versions = self._extract_fix_versions(fields)
                
                # Extract labels
                labels = self._extract_labels(fields)
                
                # Extract worklog summary
                worklog_summary = self._extract_worklog_summary(fields)

                bug_data = {
                    'id': issue["key"],  # use human-readable key as primary id (e.g., PN-15092)
                    'numeric_id': int(issue["id"]) if str(issue.get("id", "")).isdigit() else None,
                    'title': fields.get("summary", ""),
                    'description': description_text,
                    'assigned_to': assigned_to,
                    'severity': severity,
                    'state': state,
                    'customer_name': customer_name,
                    'area_path': area_path,
                    'created_date': fields.get("created", ""),
                    'changed_date': fields.get("updated", ""),
                    'iteration_path': iteration_path,
                    'hotfix_delivered_version': "",  # JIRA doesn't have this field
                    'parent_issue': parent_issue,
                    # New fields
                    'test_iterations': test_iterations,
                    'bugs_found': bugs_found,
                    'regression': regression,
                    'time_spent': time_tracking['time_spent'],
                    'fix_versions': fix_versions,
                    'tester': tester,
                    'story_points': story_points,
                    'labels': labels,
                    'worklog_total_time_spent': worklog_summary['total_time_spent'],
                    'worklog_entries_count': worklog_summary['entries_count'],
                    'developer': developer
                }

                
                bugs.append(bug_data)
                
                # Update relations if we have a database connection
                if hasattr(self, 'db_connection') and parent_issue:
                    relations = [{'type': 'parent', 'issue_id': parent_issue}]
                    self.db_connection.update_bug_relations(bug_data["id"], relations)
            
            # Check if we need to fetch more pages using the new pagination method
            is_last = issues_data.get("isLast", True)
            if is_last:
                break
                
            # Get the next page token for the next iteration
            next_page_token = issues_data.get("nextPageToken")
            if not next_page_token:
                break
                
            time.sleep(0.5)  # Rate limiting between pages
        
        print(f"------>Total bugs fetched: {total_fetched}")
        return bugs

    def get_stories_since_last_sync(self, last_sync_time):
        """Query all work items from JIRA since last sync time with pagination using new JQL API"""
        # Convert last_sync_time to JIRA date format
        if isinstance(last_sync_time, datetime):
            last_sync_str = last_sync_time.strftime('%Y-%m-%d')
        else:
            last_sync_str = last_sync_time
        
        print(f"------>Fetching work items with updated >= {last_sync_str}")
        
        # JQL query to get all issue types updated since last sync
        jql_query = f'project = {self.project_key} AND issuetype in (Story, Bug, Task, Epic, "New Feature", Improvement, Enhancement, "Sub-task") AND updated >= "{last_sync_str}" ORDER BY updated DESC'
        
        # Prepare fields parameter
        fields_param = "summary,description,assignee,priority,status,components,created,updated,sprint,timetracking,fixVersions,labels,worklog,issuetype"
        if self.customer_name_field not in fields_param:
            fields_param += f",{self.customer_name_field}"
        if self.sprint_field not in fields_param:
            fields_param += f",{self.sprint_field}"
        # Add common sprint fallbacks
        if 'customfield_10020' not in fields_param and self.sprint_field != 'customfield_10020':
            fields_param += ",customfield_10020"
        if 'sprint' not in fields_param and self.sprint_field != 'sprint':
            fields_param += ",sprint"
        # Add new custom fields
        if self.test_iterations_field not in fields_param:
            fields_param += f",{self.test_iterations_field}"
        if self.bugs_found_field not in fields_param:
            fields_param += f",{self.bugs_found_field}"
        if self.regression_field not in fields_param:
            fields_param += f",{self.regression_field}"
        if self.tester_field not in fields_param:
            fields_param += f",{self.tester_field}"
        if self.story_points_field not in fields_param:
            fields_param += f",{self.story_points_field}"
        if self.developer_field not in fields_param:
            fields_param += f",{self.developer_field}"
        
        work_items = []
        next_page_token = None
        max_results = 100  # JIRA Cloud recommended page size
        total_fetched = 0
        
        while True:
            # Use the new JQL search endpoint
            search_url = f"{self.jira_url}/rest/api/3/search/jql"
            
            # Prepare query parameters
            query_params = {
                'jql': jql_query,
                'maxResults': max_results,
                'fields': fields_param,
                'expand': 'renderedFields'
            }
            
            # Add nextPageToken if we have one
            if next_page_token:
                query_params['nextPageToken'] = next_page_token
            
            response = requests.get(search_url, headers=self.headers, params=query_params)
            if response.status_code != 200:
                print(f"Error fetching work items from JIRA: {response.status_code} - {response.text}")
                break

            issues_data = response.json()
            issues = issues_data.get("issues", [])
            
            if not issues:
                break
                
            print(f"------>Fetched {len(issues)} work items (total so far: {total_fetched + len(issues)})")
            total_fetched += len(issues)

            # Process this batch of issues
            for issue in issues:
                fields = issue["fields"]
                
                # Extract issue type
                issue_type_obj = fields.get("issuetype")
                work_item_type = issue_type_obj.get("name") if issue_type_obj else "Unknown"
                
                # Extract assignee
                assignee = fields.get("assignee")
                assigned_to = assignee.get("displayName") if assignee else ""
                
                # Extract priority as severity
                priority = fields.get("priority")
                severity = priority.get("name") if priority else ""
                
                # Extract status as state
                status = fields.get("status")
                state = status.get("name") if status else ""
                
                # Extract customer name from configurable custom field
                customer_name_raw = fields.get(self.customer_name_field, "")
                # Handle different customer name field formats
                if isinstance(customer_name_raw, list) and customer_name_raw:
                    # If it's a list, extract the 'value' from the first item
                    if isinstance(customer_name_raw[0], dict) and 'value' in customer_name_raw[0]:
                        customer_name = customer_name_raw[0]['value']
                    else:
                        customer_name = str(customer_name_raw[0])
                elif isinstance(customer_name_raw, dict) and 'value' in customer_name_raw:
                    # If it's a dict, extract the 'value'
                    customer_name = customer_name_raw['value']
                else:
                    # Otherwise, convert to string
                    customer_name = str(customer_name_raw) if customer_name_raw else ""
                
                # Extract components as area path
                components = fields.get("components", [])
                area_path = ", ".join([comp["name"] for comp in components]) if components else ""
                
                # Extract sprint information (supports multiple data shapes and field fallbacks)
                iteration_path = ""
                sprint_val = fields.get(self.sprint_field)
                if sprint_val is None:
                    sprint_val = fields.get('customfield_10020') or fields.get('sprint')
                if isinstance(sprint_val, dict):
                    iteration_path = sprint_val.get("name", "")
                elif isinstance(sprint_val, list):
                    # Could be list of dicts or list of strings
                    names = []
                    for s in sprint_val:
                        if isinstance(s, dict) and 'name' in s:
                            names.append(s['name'])
                        elif isinstance(s, str):
                            # Parse string like "com.atlassian.greenhopper.service.sprint.Sprint@... [id=1,rapidViewId=2,state=...,name=Sprint 1,...]"
                            import re
                            m = re.search(r"name=([^,\]]+)", s)
                            if m:
                                names.append(m.group(1))
                    iteration_path = ", ".join(names)
                elif isinstance(sprint_val, str):
                    iteration_path = sprint_val
                
                # Description: prefer ADF to text; fallback to rendered HTML
                description_text = self._adf_to_text(fields.get("description"))
                if not description_text:
                    rendered_desc = issue.get("renderedFields", {}).get("description")
                    if rendered_desc:
                        import re
                        from html import unescape
                        description_text = unescape(re.sub(r"<[^>]+>", "", rendered_desc)).strip()

                # Extract new fields
                test_iterations = self._extract_custom_field_value(fields, self.test_iterations_field, 'string')
                bugs_found = self._extract_custom_field_value(fields, self.bugs_found_field, 'number')
                regression = self._extract_custom_field_value(fields, self.regression_field, 'string')
                tester = self._extract_custom_field_value(fields, self.tester_field, 'user')
                story_points = self._extract_custom_field_value(fields, self.story_points_field, 'number')
                developer = self._extract_custom_field_value(fields, self.developer_field, 'user')
                
                # Extract time tracking
                time_tracking = self._extract_time_tracking(fields)
                
                # Extract fix versions
                fix_versions = self._extract_fix_versions(fields)
                
                # Extract labels
                labels = self._extract_labels(fields)
                
                # Extract worklog summary
                worklog_summary = self._extract_worklog_summary(fields)

                work_item_data = {
                    'id': issue["key"],  # use human-readable key as primary id (e.g., PN-15092)
                    'numeric_id': int(issue["id"]) if str(issue.get("id", "")).isdigit() else None,
                    'title': fields.get("summary", ""),
                    'description': description_text,
                    'assigned_to': assigned_to,
                    'severity': severity,
                    'state': state,
                    'customer_name': customer_name,
                    'area_path': area_path,
                    'created_date': fields.get("created", ""),
                    'changed_date': fields.get("updated", ""),
                    'iteration_path': iteration_path,
                    'hotfix_delivered_version': "",  # JIRA doesn't have this field
                    'work_item_type': work_item_type,  # Use the actual issue type from JIRA
                    # New fields
                    'test_iterations': test_iterations,
                    'bugs_found': bugs_found,
                    'regression': regression,
                    'time_spent': time_tracking['time_spent'],
                    'fix_versions': fix_versions,
                    'tester': tester,
                    'story_points': story_points,
                    'labels': labels,
                    'worklog_total_time_spent': worklog_summary['total_time_spent'],
                    'worklog_entries_count': worklog_summary['entries_count'],
                    'developer': developer
                }

                
                work_items.append(work_item_data)
            
            # Check if we need to fetch more pages using the new pagination method
            is_last = issues_data.get("isLast", True)
            if is_last:
                break
                
            # Get the next page token for the next iteration
            next_page_token = issues_data.get("nextPageToken")
            if not next_page_token:
                break
                
            time.sleep(0.5)  # Rate limiting between pages
        
        print(f"------>Total work items fetched: {total_fetched}")
        return work_items

    def handle_bug_changes(self, bugs, db_connection=None):
        """Get and store the state change history for bugs using JIRA changelog"""
        if not bugs:
            return {}

        bug_ids = [bug['id'] if isinstance(bug, dict) else bug for bug in bugs]
        all_state_changes = {}

        for bug_id in bug_ids:
            # Get changelog for the bug
            changelog_url = f"{self.jira_url}/rest/api/3/issue/{bug_id}?expand=changelog"
            response = requests.get(changelog_url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"Error fetching changelog for bug {bug_id}: {response.status_code}")
                continue
                
            issue_data = response.json()
            changelog = issue_data.get("changelog", {}).get("histories", [])
            state_changes = []

            # If we have a database connection, handle the database operations
            if db_connection:
                with db_connection.engine.connect() as connection:
                    try:
                        # First, delete all existing entries for this bug
                        connection.execute(
                            text(f"""
                                DELETE FROM {db_connection.schema_name}.change_history 
                                WHERE record_id = :record_id 
                                AND table_name = 'bugs'
                                AND field_changed IN ('System.State', 'Work Log Entry')
                            """),
                            {"record_id": bug_id}
                        )
                        
                        # Now process and insert all state changes
                        for history in changelog:
                            for item in history.get("items", []):
                                field_name = item.get("field")
                                if field_name in ["status", "WorklogId"]:
                                    changed_date = history.get("created", "")
                                    changed_by = history.get("author", {}).get("displayName", "")
                                    old_value = item.get("fromString", "")
                                    new_value = item.get("toString", "")

                                    # Convert date string to datetime object
                                    try:
                                        changed_date_obj = datetime.strptime(changed_date, "%Y-%m-%dT%H:%M:%S.%f%z")
                                    except ValueError:
                                        try:
                                            changed_date_obj = datetime.strptime(changed_date, "%Y-%m-%dT%H:%M:%S%z")
                                        except ValueError:
                                            try:
                                                changed_date_obj = datetime.strptime(changed_date, "%Y-%m-%dT%H:%M:%S")
                                            except ValueError:
                                                changed_date_obj = None

                                    if changed_date_obj:  # Only insert if we have a valid date
                                        # Map field names to display names
                                        field_display_name = "System.State" if field_name == "status" else "Work Log Entry"
                                        
                                        # For work log entries, get the time spent value and ensure old_value is empty
                                        if field_name == "WorklogId" and new_value:
                                            # Work logs are additions, so old_value should always be empty
                                            old_value = ""
                                            worklog_details = self._get_worklog_details(bug_id, new_value)
                                            if worklog_details:
                                                # Use time spent in seconds if available, otherwise use the timeSpent string
                                                time_spent_seconds = worklog_details.get('timeSpentSeconds')
                                                if time_spent_seconds:
                                                    new_value = str(time_spent_seconds)
                                                else:
                                                    new_value = worklog_details.get('timeSpent', new_value)
                                        
                                        # Insert the change record
                                        connection.execute(
                                            text(f"""
                                                INSERT INTO {db_connection.schema_name}.change_history 
                                                (record_id, table_name, field_changed, old_value, new_value, changed_by, changed_date)
                                                VALUES 
                                                (:record_id, :table_name, :field_changed, :old_value, :new_value, :changed_by, :changed_date)
                                            """),
                                            {
                                                "record_id": bug_id,
                                                "table_name": "bugs",
                                                "field_changed": field_display_name,
                                                "old_value": old_value,
                                                "new_value": new_value,
                                                "changed_by": changed_by,
                                                "changed_date": changed_date_obj
                                            }
                                        )

                                    state_changes.append([
                                        changed_date,
                                        old_value,
                                        new_value
                                    ])
                        
                        connection.commit()
                        
                    except SQLAlchemyError as e:
                        print(f"Error processing state changes for bug {bug_id}: {str(e)}")
                        connection.rollback()
            else:
                # If no database connection, just collect the state changes
                for history in changelog:
                    for item in history.get("items", []):
                        if item.get("field") == "status":
                            changed_date = history.get("created", "")
                            old_value = item.get("fromString", "")
                            new_value = item.get("toString", "")

                            state_changes.append([
                                changed_date,
                                old_value,
                                new_value
                            ])
            
            all_state_changes[bug_id] = state_changes
        
        return all_state_changes

    def _get_worklog_details(self, issue_id, worklog_id):
        """Get work log details by ID for enhanced tracking"""
        try:
            worklog_url = f"{self.jira_url}/rest/api/3/issue/{issue_id}/worklog"
            response = requests.get(worklog_url, headers=self.headers)
            
            if response.status_code != 200:
                return None
                
            worklog_data = response.json()
            worklogs = worklog_data.get('worklogs', [])
            
            # Find the work log with matching ID
            for worklog in worklogs:
                if str(worklog.get('id')) == str(worklog_id):
                    return worklog
                    
            return None
        except Exception as e:
            print(f"Error fetching work log details: {str(e)}")
            return None

    def _get_sprint_details(self, sprint_id):
        """Get sprint details from JIRA Agile API"""
        try:
            sprint_url = f"{self.jira_url}/rest/agile/1.0/sprint/{sprint_id}"
            response = requests.get(sprint_url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"Error fetching sprint {sprint_id}: {response.status_code} - {response.text}")
                return None
            
            sprint_data = response.json()
            
            # Parse dates
            start_date = None
            end_date = None
            complete_date = None
            
            if sprint_data.get('startDate'):
                try:
                    start_date = datetime.fromisoformat(sprint_data['startDate'].replace('Z', '+00:00'))
                except ValueError:
                    try:
                        start_date = datetime.strptime(sprint_data['startDate'], "%Y-%m-%dT%H:%M:%S.%f%z")
                    except ValueError:
                        start_date = None
            
            if sprint_data.get('endDate'):
                try:
                    end_date = datetime.fromisoformat(sprint_data['endDate'].replace('Z', '+00:00'))
                except ValueError:
                    try:
                        end_date = datetime.strptime(sprint_data['endDate'], "%Y-%m-%dT%H:%M:%S.%f%z")
                    except ValueError:
                        end_date = None
            
            if sprint_data.get('completeDate'):
                try:
                    complete_date = datetime.fromisoformat(sprint_data['completeDate'].replace('Z', '+00:00'))
                except ValueError:
                    try:
                        complete_date = datetime.strptime(sprint_data['completeDate'], "%Y-%m-%dT%H:%M:%S.%f%z")
                    except ValueError:
                        complete_date = None
            
            return {
                'id': sprint_data.get('id'),
                'name': sprint_data.get('name', ''),
                'state': sprint_data.get('state', ''),
                'start_date': start_date,
                'end_date': end_date,
                'complete_date': complete_date
            }
        except Exception as e:
            print(f"Error fetching sprint details for {sprint_id}: {str(e)}")
            return None

    def update_sprints(self):
        """Fetch all sprints from JIRA boards and update database"""
        print("------>Fetching sprints from JIRA")
        
        try:
            # Get all boards for the project
            boards_url = f"{self.jira_url}/rest/agile/1.0/board"
            boards_params = {'projectKeyOrId': self.project_key}
            boards_response = requests.get(boards_url, headers=self.headers, params=boards_params)
            
            if boards_response.status_code != 200:
                print(f"Error fetching boards: {boards_response.status_code} - {boards_response.text}")
                return 0
            
            boards_data = boards_response.json()
            boards = boards_data.get('values', [])
            
            if not boards:
                print("------>No boards found for project")
                return 0
            
            all_sprints = []
            
            # Fetch sprints from all boards
            for board in boards:
                board_id = board.get('id')
                board_name = board.get('name', 'Unknown')
                print(f"------>Fetching sprints from board: {board_name} (ID: {board_id})")
                
                sprints_url = f"{self.jira_url}/rest/agile/1.0/board/{board_id}/sprint"
                sprints_params = {'maxResults': 100}
                
                while True:
                    sprints_response = requests.get(sprints_url, headers=self.headers, params=sprints_params)
                    
                    if sprints_response.status_code != 200:
                        print(f"Error fetching sprints from board {board_id}: {sprints_response.status_code}")
                        break
                    
                    sprints_data = sprints_response.json()
                    sprints = sprints_data.get('values', [])
                    
                    if not sprints:
                        break
                    
                    for sprint in sprints:
                        sprint_id = sprint.get('id')
                        if sprint_id and sprint_id not in [s['id'] for s in all_sprints]:
                            # Parse dates
                            start_date = None
                            end_date = None
                            complete_date = None
                            
                            if sprint.get('startDate'):
                                try:
                                    start_date = datetime.fromisoformat(sprint['startDate'].replace('Z', '+00:00'))
                                except ValueError:
                                    try:
                                        start_date = datetime.strptime(sprint['startDate'], "%Y-%m-%dT%H:%M:%S.%f%z")
                                    except ValueError:
                                        start_date = None
                            
                            if sprint.get('endDate'):
                                try:
                                    end_date = datetime.fromisoformat(sprint['endDate'].replace('Z', '+00:00'))
                                except ValueError:
                                    try:
                                        end_date = datetime.strptime(sprint['endDate'], "%Y-%m-%dT%H:%M:%S.%f%z")
                                    except ValueError:
                                        end_date = None
                            
                            if sprint.get('completeDate'):
                                try:
                                    complete_date = datetime.fromisoformat(sprint['completeDate'].replace('Z', '+00:00'))
                                except ValueError:
                                    try:
                                        complete_date = datetime.strptime(sprint['completeDate'], "%Y-%m-%dT%H:%M:%S.%f%z")
                                    except ValueError:
                                        complete_date = None
                            
                            sprint_info = {
                                'id': sprint_id,
                                'name': sprint.get('name', ''),
                                'state': sprint.get('state', ''),
                                'start_date': start_date,
                                'end_date': end_date,
                                'complete_date': complete_date
                            }
                            all_sprints.append(sprint_info)
                    
                    # Check for more pages
                    if sprints_data.get('isLast', True):
                        break
                    sprints_params['startAt'] = sprints_data.get('startAt', 0) + len(sprints)
            
            # Update database with sprints
            if hasattr(self, 'db_connection') and self.db_connection:
                processed_count = self.db_connection.upsert_sprints(all_sprints)
                print(f"------>Processed {processed_count} sprints from {len(boards)} board(s)")
                return processed_count
            else:
                print("------>No database connection available for sprints update")
                return 0
                
        except Exception as e:
            print(f"Error updating sprints: {str(e)}")
            return 0

    def handle_story_changes(self, stories, db_connection=None):
        """Get and store the state change history for stories using JIRA changelog"""
        if not stories:
            return {}

        story_ids = [story['id'] if isinstance(story, dict) else story for story in stories]
        all_state_changes = {}

        for story_id in story_ids:
            # Get changelog for the story
            changelog_url = f"{self.jira_url}/rest/api/3/issue/{story_id}?expand=changelog"
            response = requests.get(changelog_url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"Error fetching changelog for story {story_id}: {response.status_code}")
                continue
                
            issue_data = response.json()
            changelog = issue_data.get("changelog", {}).get("histories", [])
            state_changes = []

            # If we have a database connection, handle the database operations
            if db_connection:
                with db_connection.engine.connect() as connection:
                    try:
                        # First, delete all existing entries for this story
                        connection.execute(text(f"""
                            DELETE FROM {db_connection.schema_name}.change_history 
                            WHERE record_id = :record_id 
                            AND field_changed IN ('state', 'Work Log Entry')
                        """), 
                        {"record_id": story_id})
                        
                        # Insert new state changes
                        for history in changelog:
                            for item in history.get("items", []):
                                field_name = item.get("field")
                                if field_name in ["status", "WorklogId"]:
                                    changed_date = history.get("created", "")
                                    old_value = item.get("fromString", "")
                                    new_value = item.get("toString", "")
                                    
                                    # Parse the changed date
                                    try:
                                        changed_date_obj = datetime.fromisoformat(changed_date.replace('Z', '+00:00'))
                                    except ValueError:
                                        try:
                                            # Try parsing without timezone info
                                            changed_date_obj = datetime.fromisoformat(changed_date.split('.')[0])
                                        except ValueError:
                                            changed_date_obj = None

                                    if changed_date_obj:  # Only insert if we have a valid date
                                        # Get the user who made the change
                                        changed_by = history.get("author", {}).get("displayName", "")
                                        
                                        # Map field names to display names
                                        field_display_name = "state" if field_name == "status" else "Work Log Entry"
                                        
                                        # For work log entries, get the time spent value and ensure old_value is empty
                                        if field_name == "WorklogId" and new_value:
                                            # Work logs are additions, so old_value should always be empty
                                            old_value = ""
                                            worklog_details = self._get_worklog_details(story_id, new_value)
                                            if worklog_details:
                                                # Use time spent in seconds if available, otherwise use the timeSpent string
                                                time_spent_seconds = worklog_details.get('timeSpentSeconds')
                                                if time_spent_seconds:
                                                    new_value = str(time_spent_seconds)
                                                else:
                                                    new_value = worklog_details.get('timeSpent', new_value)
                                        
                                        connection.execute(text(f"""
                                            INSERT INTO {db_connection.schema_name}.change_history 
                                            (record_id, table_name, field_changed, old_value, new_value, changed_by, changed_date)
                                            VALUES (:record_id, :table_name, :field_changed, :old_value, :new_value, :changed_by, :changed_date)
                                        """), {
                                            "record_id": story_id,
                                            "table_name": "work_items",
                                            "field_changed": field_display_name,
                                            "old_value": old_value,
                                            "new_value": new_value,
                                            "changed_by": changed_by,
                                            "changed_date": changed_date_obj
                                        })
                                        
                                        state_changes.append([
                                            changed_date,
                                            old_value,
                                            new_value
                                        ])
                        
                        connection.commit()
                        print(f"------>Processed {len(state_changes)} state changes for story {story_id}")
                        
                    except Exception as e:
                        print(f"Error processing state changes for story {story_id}: {str(e)}")
                        connection.rollback()
            else:
                # If no database connection, just collect the state changes
                for history in changelog:
                    for item in history.get("items", []):
                        if item.get("field") == "status":
                            changed_date = history.get("created", "")
                            old_value = item.get("fromString", "")
                            new_value = item.get("toString", "")

                            state_changes.append([
                                changed_date,
                                old_value,
                                new_value
                            ])
            
            all_state_changes[story_id] = state_changes
        
        return all_state_changes

def main():
    # Load configuration from .env file
    jira_url = os.getenv('JIRA_CLOUD_URL')
    project_key = os.getenv('JIRA_PROJECT_KEY')
    email = os.getenv('JIRA_USER_EMAIL')
    api_token = os.getenv('JIRA_API_TOKEN')
    schema_name = os.getenv('JIRA_SCHEMA_NAME', 'jira_sync')
    
    if not all([jira_url, project_key, email, api_token]):
        raise ValueError("Please check your .env file and ensure JIRA_CLOUD_URL, JIRA_PROJECT_KEY, JIRA_USER_EMAIL, and JIRA_API_TOKEN are set")

    if api_token == "your-jira-api-token-here":
        raise ValueError("Please update the JIRA_API_TOKEN in your .env file with your actual API token")

    # Initialize extractor and database connection
    extractor = JIRAExtractor(jira_url, project_key, email, api_token)
    db = get_database_connection(schema_name)
    
    # Set the database connection for the extractor
    extractor.db_connection = db

    SLEEP_INTERVAL = 80 * 60 
    
    print(f"Starting continuous JIRA sync with {SLEEP_INTERVAL/60} minute intervals")
    print(f"Using schema: {schema_name}")
    print("Press Ctrl+C to stop the program")
    
    while True:
        try:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n=== Starting JIRA sync cycle at {current_time} ===")
            
            # Check if we need to run a full sync (weekly, going back 1 month)
            last_full_sync = db.get_last_full_sync_time()
            should_run_full_sync = False
            sync_date_override = None
            
            if last_full_sync is None:
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
            bugs_last_sync = db.get_last_sync_time('bug')
            stories_last_sync = db.get_last_sync_time('story')
            
            # Override with full sync date if needed
            if should_run_full_sync:
                bugs_last_sync = sync_date_override
                stories_last_sync = sync_date_override
                print(f"------>Using sync date: {sync_date_override.strftime('%Y-%m-%d')} (1 month back)")

            print(f"------>Last bug sync: {bugs_last_sync}")
            print(f"------>Last story sync: {stories_last_sync}")
            
            # Extract and store bugs
            bugs = extractor.get_bugs_since_last_sync(bugs_last_sync)
            processed_bugs = db.upsert_bugs(bugs)
            print(f"------>Processed {processed_bugs} bugs")

            # Extract and store all work items
            work_items = extractor.get_stories_since_last_sync(stories_last_sync)
            processed_work_items = db.upsert_work_items(work_items)
            print(f"------>Processed {processed_work_items} work items")

            # Process state changes for all bugs
            print("------>Processing bug state changes")
            extractor.handle_bug_changes(bugs, db)
            print(f"------>Processed state changes for {len(bugs)} bugs")

            # Process state changes for all work items
            print("------>Processing work item state changes")
            extractor.handle_story_changes(work_items, db)
            print(f"------>Processed state changes for {len(work_items)} work items")

            # Update sprints
            processed_sprints = extractor.update_sprints()

            # Update sync status for both bugs and work items
            if processed_bugs > 0:
                db.update_sync_status('bug', processed_bugs)
            if processed_work_items > 0:
                db.update_sync_status('story', processed_work_items)
            if processed_sprints > 0:
                db.update_sync_status('sprint', processed_sprints)

            # Update history snapshots
            db.update_history_snapshots()
            print("------>Updated history snapshots")

            # Update full sync status if we ran a full sync
            if should_run_full_sync:
                db.update_full_sync_status()
                print("------>Updated full sync timestamp")

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"=== JIRA sync cycle completed at {current_time} ===")
            print(f"Sleeping for {SLEEP_INTERVAL/60} minutes...\n")
            
            time.sleep(SLEEP_INTERVAL)
            
        except KeyboardInterrupt:
            print("\nReceived interrupt signal. Shutting down gracefully...")
            break
        except Exception as e:
            print(f"\nError during processing: {str(e)}")
            print("Will retry in the next cycle")
            print(f"Sleeping for {SLEEP_INTERVAL/60} minutes...")
            time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    main()
