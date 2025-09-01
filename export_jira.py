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
                schema=self.schema_name
            )

            # Create tables if they don't exist
            self.metadata.create_all(self.engine, checkfirst=True)

            # Create indexes
            with self.engine.connect() as connection:
                try:
                    self._create_index(connection, "idx_bugs_changed_date", "bugs", "changed_date")
                    self._create_index(connection, "idx_bugs_parent_issue", "bugs", "parent_issue")
                    self._create_index(connection, "idx_sync_status_entity_type", "sync_status", "entity_type, last_sync_time DESC")
                    self._create_index(connection, "idx_bugs_relations_bug_id", "bugs_relations", "bug_id")
                    self._create_index(connection, "idx_work_items_changed_date", "work_items", "changed_date")
                    self._create_index(connection, "idx_work_items_type", "work_items", "work_item_type")
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
            
            # If no sync history, default to 6 months ago
            return datetime.now() - timedelta(days=180)

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

    def update_history_snapshots(self):
        """Update the history snapshots table with current metrics"""
        metrics = [
            ('total_bugs', f'SELECT COUNT(*) FROM {self.schema_name}.bugs'),
            ('open_bugs', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state NOT IN ('DONE')"),
            ('new_bugs', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state IN ('New Request')"),
            ('closed_bugs', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state IN ('DONE')"),
            ('reopened_bugs', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state = 'New Request' AND changed_date >= CURRENT_DATE - INTERVAL '1 day'"),
            ('p1_bugs', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE severity = 'Critical' AND state NOT IN ('DONE')"),
            ('p2_bugs', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE severity = 'High' AND state NOT IN ('DONE')"),
            ('p1_p2_bugs_with_customer_issues', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state NOT IN ('DONE') AND severity IN ('Critical','High') AND parent_issue IS NOT NULL"),
            ('p1_p2_bugs_in_dev', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state IN ('New Request', 'IN-PROGRESS', 'In Review') AND severity IN ('Critical','High')"),
            ('open_bugs_QA_p1_p2', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state IN ('READY FOR QA', 'IN QA') AND severity IN ('Critical','High')"),
            ('total_customers', f"SELECT COUNT(DISTINCT customer_name) AS total_customers FROM {self.schema_name}.bugs WHERE customer_name IS NOT NULL AND customer_name != ''"),
            ('redline_bugs', f"SELECT COALESCE(COUNT(*) * 1.0 / NULLIF((SELECT COUNT(DISTINCT customer_name) FROM {self.schema_name}.bugs WHERE customer_name IS NOT NULL AND customer_name != ''), 0), 0) AS bugs_with_parent_per_customer FROM {self.schema_name}.bugs WHERE parent_issue IS NOT NULL AND DATE_TRUNC('month', created_date) = DATE_TRUNC('month', CURRENT_DATE)"),
            ('open_p1_bugs_with_customer_issues', f"SELECT COUNT(*) FROM {self.schema_name}.bugs WHERE state NOT IN ('DONE') AND severity = 'Critical' AND parent_issue IS NOT NULL")
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
                                numeric_id = :numeric_id
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
                                "numeric_id": bug.get('numeric_id')
                            }
                        )
                    else:
                        # Insert new bug
                        connection.execute(
                            text(f"""
                            INSERT INTO {self.schema_name}.bugs (
                                id, numeric_id, title, description, assigned_to, severity,
                                state, customer_name, area_path, created_date, changed_date,
                                iteration_path, hotfix_delivered_version, parent_issue
                            ) VALUES (
                                :id, :numeric_id, :title, :description, :assigned_to, :severity,
                                :state, :customer_name, :area_path, :created_date, :changed_date,
                                :iteration_path, :hotfix_delivered_version, :parent_issue
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
                                "parent_issue": bug.get('parent_issue')
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
                                numeric_id = :numeric_id
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
                                "numeric_id": item.get('numeric_id')
                            }
                        )
                    else:
                        # Insert new work item
                        connection.execute(
                            text(f"""
                            INSERT INTO {self.schema_name}.work_items (
                                id, numeric_id, title, description, assigned_to, severity,
                                state, customer_name, area_path, created_date, changed_date,
                                iteration_path, hotfix_delivered_version, work_item_type
                            ) VALUES (
                                :id, :numeric_id, :title, :description, :assigned_to, :severity,
                                :state, :customer_name, :area_path, :created_date, :changed_date,
                                :iteration_path, :hotfix_delivered_version, :work_item_type
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
                                "work_item_type": item['work_item_type']
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

    def get_bugs_since_last_sync(self, last_sync_time):
        """Query bugs from JIRA since last sync time with pagination"""
        # Convert last_sync_time to JIRA date format
        last_sync_str = last_sync_time.strftime('%Y-%m-%d')
        
        # JQL query to get bugs updated since last sync
        jql_query = f'project = {self.project_key} AND issuetype = Bug AND updated >= "{last_sync_str}" ORDER BY updated DESC'
        
        # URL encode the JQL query
        import urllib.parse
        encoded_jql = urllib.parse.quote(jql_query)
        
        # Prepare fields parameter
        fields_param = "summary,description,assignee,priority,status,components,created,updated,parent"
        if self.customer_name_field not in fields_param:
            fields_param += f",{self.customer_name_field}"
        if self.sprint_field not in fields_param:
            fields_param += f",{self.sprint_field}"
        # Add common sprint fallbacks
        if 'customfield_10020' not in fields_param and self.sprint_field != 'customfield_10020':
            fields_param += ",customfield_10020"
        if 'sprint' not in fields_param and self.sprint_field != 'sprint':
            fields_param += ",sprint"
        
        bugs = []
        start_at = 0
        max_results = 100  # JIRA Cloud recommended page size
        total_fetched = 0
        
        while True:
            search_url = f"{self.jira_url}/rest/api/3/search?jql={encoded_jql}&startAt={start_at}&maxResults={max_results}&fields={fields_param}&expand=renderedFields"
            
            response = requests.get(search_url, headers=self.headers)
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
                    'parent_issue': parent_issue
                }

                
                bugs.append(bug_data)
                
                # Update relations if we have a database connection
                if hasattr(self, 'db_connection') and parent_issue:
                    relations = [{'type': 'parent', 'issue_id': parent_issue}]
                    self.db_connection.update_bug_relations(bug_data["id"], relations)
            
            # Check if we need to fetch more pages
            total_issues = issues_data.get("total", 0)
            if start_at + len(issues) >= total_issues:
                break
                
            start_at += len(issues)
            time.sleep(0.5)  # Rate limiting between pages
        
        print(f"------>Total bugs fetched: {total_fetched}")
        return bugs

    def get_stories_since_last_sync(self, last_sync_time):
        """Query stories from JIRA since last sync time with pagination"""
        # Convert last_sync_time to JIRA date format
        last_sync_str = last_sync_time.strftime('%Y-%m-%d')
        
        # JQL query to get stories updated since last sync
        jql_query = f'project = {self.project_key} AND issuetype = Story AND updated >= "{last_sync_str}" ORDER BY updated DESC'
        
        # URL encode the JQL query
        import urllib.parse
        encoded_jql = urllib.parse.quote(jql_query)
        
        # Prepare fields parameter
        fields_param = "summary,description,assignee,priority,status,components,created,updated,sprint"
        if self.customer_name_field not in fields_param:
            fields_param += f",{self.customer_name_field}"
        if self.sprint_field not in fields_param:
            fields_param += f",{self.sprint_field}"
        # Add common sprint fallbacks
        if 'customfield_10020' not in fields_param and self.sprint_field != 'customfield_10020':
            fields_param += ",customfield_10020"
        if 'sprint' not in fields_param and self.sprint_field != 'sprint':
            fields_param += ",sprint"
        
        stories = []
        start_at = 0
        max_results = 100  # JIRA Cloud recommended page size
        total_fetched = 0
        
        while True:
            search_url = f"{self.jira_url}/rest/api/3/search?jql={encoded_jql}&startAt={start_at}&maxResults={max_results}&fields={fields_param}&expand=renderedFields"
            
            response = requests.get(search_url, headers=self.headers)
            if response.status_code != 200:
                print(f"Error fetching stories from JIRA: {response.status_code} - {response.text}")
                break

            issues_data = response.json()
            issues = issues_data.get("issues", [])
            
            if not issues:
                break
                
            print(f"------>Fetched {len(issues)} stories (total so far: {total_fetched + len(issues)})")
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
                
                # Description: prefer ADF to text; fallback to rendered HTML
                description_text = self._adf_to_text(fields.get("description"))
                if not description_text:
                    rendered_desc = issue.get("renderedFields", {}).get("description")
                    if rendered_desc:
                        import re
                        from html import unescape
                        description_text = unescape(re.sub(r"<[^>]+>", "", rendered_desc)).strip()

                story_data = {
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
                    'work_item_type': 'Story'
                }

                
                stories.append(story_data)
            
            # Check if we need to fetch more pages
            total_issues = issues_data.get("total", 0)
            if start_at + len(issues) >= total_issues:
                break
                
            start_at += len(issues)
            time.sleep(0.5)  # Rate limiting between pages
        
        print(f"------>Total stories fetched: {total_fetched}")
        return stories

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
                                AND field_changed = 'System.State'
                            """),
                            {"record_id": bug_id}
                        )
                        
                        # Now process and insert all state changes
                        for history in changelog:
                            for item in history.get("items", []):
                                if item.get("field") == "status":
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
                                                "field_changed": "System.State",
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
                        connection.execute(text(f"DELETE FROM {db_connection.schema_name}.change_history WHERE record_id = :record_id"), 
                                        {"record_id": story_id})
                        
                        # Insert new state changes
                        for history in changelog:
                            for item in history.get("items", []):
                                if item.get("field") == "status":
                                    changed_date = history.get("created", "")
                                    old_value = item.get("fromString", "")
                                    new_value = item.get("toString", "")
                                    
                                    if old_value and new_value:  # Only insert if both values exist
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
                                            
                                            connection.execute(text(f"""
                                                INSERT INTO {db_connection.schema_name}.change_history 
                                                (record_id, table_name, field_changed, old_value, new_value, changed_by, changed_date)
                                                VALUES (:record_id, :table_name, :field_changed, :old_value, :new_value, :changed_by, :changed_date)
                                            """), {
                                                "record_id": story_id,
                                                "table_name": "work_items",
                                                "field_changed": "state",
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
            
            # Get last sync times from database
            bugs_last_sync = db.get_last_sync_time('bug')
            stories_last_sync = db.get_last_sync_time('story')
            
            # Extract and store bugs
            bugs = extractor.get_bugs_since_last_sync(bugs_last_sync)
            processed_bugs = db.upsert_bugs(bugs)
            print(f"------>Processed {processed_bugs} bugs")

            # Extract and store stories
            stories = extractor.get_stories_since_last_sync(stories_last_sync)
            processed_stories = db.upsert_work_items(stories)
            print(f"------>Processed {processed_stories} stories")

            # Process state changes for all bugs
            print("------>Processing bug state changes")
            extractor.handle_bug_changes(bugs, db)
            print(f"------>Processed state changes for {len(bugs)} bugs")

            # Process state changes for all stories
            print("------>Processing story state changes")
            extractor.handle_story_changes(stories, db)
            print(f"------>Processed state changes for {len(stories)} stories")

            # Update sync status for both bugs and stories
            if processed_bugs > 0:
                db.update_sync_status('bug', processed_bugs)
            if processed_stories > 0:
                db.update_sync_status('story', processed_stories)

            # Update history snapshots
            db.update_history_snapshots()
            print("------>Updated history snapshots")

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
