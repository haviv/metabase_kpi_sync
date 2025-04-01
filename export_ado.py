import requests
import csv
import base64
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, text, Table, Column, Integer, String, DateTime, MetaData, ForeignKey, inspect
from sqlalchemy.dialects.mssql import NVARCHAR, TEXT as MSSQL_TEXT
from sqlalchemy.dialects.postgresql import TEXT as PG_TEXT
from sqlalchemy.exc import SQLAlchemyError
import json
from abc import ABC, abstractmethod

# Load environment variables from .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

class DatabaseConnection(ABC):
    def __init__(self):
        self.engine = self._create_engine()
        self.metadata = MetaData()
        self.setup_tables()

    @abstractmethod
    def _create_engine(self):
        """Create and return a SQLAlchemy engine"""
        pass

    @abstractmethod
    def _get_text_type(self):
        """Return the appropriate TEXT type for the database"""
        pass

    @abstractmethod
    def _get_merge_query(self, snapshot_date, metrics):
        """Return the appropriate MERGE/UPSERT query for history snapshots"""
        pass

    @abstractmethod
    def _handle_identity_insert(self, connection, table_name, enable=True):
        """Handle identity insert for the specific database"""
        pass

    @abstractmethod
    def _create_index(self, connection, index_name, table_name, columns):
        """Create an index with database-specific syntax"""
        pass

    @abstractmethod
    def _drop_index(self, connection, index_name, table_name):
        """Drop an index with database-specific syntax"""
        pass

    @abstractmethod
    def _get_last_sync_query(self):
        """Return database-specific query for getting last sync time"""
        pass

    @abstractmethod
    def update_parent_issue(self):
        """Update parent_issue references in bugs table with database-specific syntax"""
        pass

    @abstractmethod
    def _add_new_columns(self, connection, table_name):
        """Add new columns to existing tables if they don't exist with database-specific syntax"""
        pass

    def setup_tables(self):
        """Create tables if they don't exist and add new columns if needed"""
        try:
            # Check if tables exist
            with self.engine.connect() as connection:
                inspector = inspect(self.engine)
                existing_tables = inspector.get_table_names()
                
                # Add new columns if tables exist
                if 'issues' in existing_tables:
                    self._add_new_columns(connection, 'issues')
                
                if 'bugs' in existing_tables:
                    self._add_new_columns(connection, 'bugs')
                
                # Drop existing indexes if tables exist
                if 'issues' in existing_tables:
                    self._drop_index(connection, "idx_issues_changed_date", "issues")
                
                if 'bugs' in existing_tables:
                    self._drop_index(connection, "idx_bugs_changed_date", "bugs")
                    self._drop_index(connection, "idx_bugs_parent_issue", "bugs")
                
                if 'sync_status' in existing_tables:
                    self._drop_index(connection, "idx_sync_status_entity_type", "sync_status")
                
                connection.commit()

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
                Column('hotfix_delivered_version', String(200), nullable=True)
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
                Column('hotfix_delivered_version', String(200), nullable=True)
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

            # History snapshots table
            self.history_snapshots = Table(
                'history_snapshots', self.metadata,
                Column('snapshot_date', DateTime, primary_key=True),
                Column('name', String(255), primary_key=True),
                Column('number', Integer, nullable=False)
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

            # Create tables if they don't exist
            self.metadata.create_all(self.engine, checkfirst=True)

            # Create indexes
            with self.engine.connect() as connection:
                try:
                    # Create indexes with database-specific syntax
                    self._create_index(connection, "idx_issues_changed_date", "issues", "changed_date")
                    
                    self._create_index(connection, "idx_bugs_changed_date", "bugs", "changed_date")
                    self._create_index(connection, "idx_bugs_parent_issue", "bugs", "parent_issue")
                    
                    self._create_index(connection, "idx_sync_status_entity_type", "sync_status", "entity_type, last_sync_time DESC")
                    
                    self._create_index(connection, "idx_bugs_relations_bug_id", "bugs_relations", "bug_id")
                    
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
            
            # If no sync history, default to 6 months ago
            return datetime.now() - timedelta(days=180)

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
            ('open_bugs', "SELECT COUNT(*) FROM bugs WHERE state NOT IN ('Done', 'Not Reproduced', 'QA Completed', 'Removed')"),
            ('in_qa_bugs', "SELECT COUNT(*) FROM bugs WHERE state = 'In QA'"),
            ('new_bugs', "SELECT COUNT(*) FROM bugs WHERE state IN ('New', 'Approved')"),
            ('closed_bugs', "SELECT COUNT(*) FROM bugs WHERE state IN ('Done', 'Not Reproduced', 'QA Completed', 'Removed')"),
            ('reopened_bugs', "SELECT COUNT(*) FROM bugs WHERE state = 'Reopened' AND changed_date >= CURRENT_DATE - INTERVAL '1 day'"),
            ('p1_bugs', "SELECT COUNT(*) FROM bugs WHERE severity = '1' AND state NOT IN ('Done', 'Not Reproduced', 'QA Completed', 'Removed')"),
            ('p2_bugs', "SELECT COUNT(*) FROM bugs WHERE severity = '2' AND state NOT IN ('Done', 'Not Reproduced', 'QA Completed', 'Removed')"),
            ('p1_p2_bugs_with_customer_issues', "SELECT COUNT(*) FROM bugs WHERE state NOT IN ('Done', 'Not Reproduced', 'QA Completed', 'Removed') AND severity IN ('1','2') AND parent_issue IS NOT NULL"),
            ('p1_p2_bugs_in_dev', "SELECT COUNT(*) FROM bugs WHERE state IN ('Approved', 'Issues Found', 'New', 'In Progress', 'Waiting for PR') AND severity IN ('1','2')"),
            ('open_bugs_QA_p1_p2', "SELECT COUNT(*) FROM bugs WHERE state IN ('Ready for QA', 'In QA') AND severity IN ('1','2')"),
            ('open_p1_bugs_with_customer_issues', "SELECT COUNT(*) FROM bugs WHERE state NOT IN ('Done', 'Not Reproduced', 'QA Completed', 'Removed') AND severity = '1' AND parent_issue IS NOT NULL")
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
                            hotfix_delivered_version = :hotfix_delivered_version
                        """
                        
                        if item_type == 'bug':
                            update_stmt += ", parent_issue = :parent_issue"
                        
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
                            "hotfix_delivered_version": item['HotfixDeliveredVersion']
                        }
                        
                        if item_type == 'bug':
                            params["parent_issue"] = item.get('ParentID')

                        connection.execute(text(update_stmt), params)
                    else:
                        # Handle identity insert for the specific database
                        self._handle_identity_insert(connection, table.name, True)
                        
                        # Insert new item
                        insert_stmt = f"""
                        INSERT INTO {table.name} (
                            id, title, description, assigned_to, severity,
                            state, customer_name, area_path, created_date, changed_date,
                            iteration_path, hotfix_delivered_version
                        """
                        
                        if item_type == 'bug':
                            insert_stmt += ", parent_issue"
                        
                        insert_stmt += """)
                        VALUES (
                            :id, :title, :description, :assigned_to, :severity,
                            :state, :customer_name, :area_path, :created_date, :changed_date,
                            :iteration_path, :hotfix_delivered_version
                        """
                        
                        if item_type == 'bug':
                            insert_stmt += ", :parent_issue"
                        
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
                            "hotfix_delivered_version": item['HotfixDeliveredVersion']
                        }
                        
                        if item_type == 'bug':
                            params["parent_issue"] = item.get('ParentID')

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

class SQLServerConnection(DatabaseConnection):
    def __init__(self):
        self.connection_string = (
            f"DRIVER={{{os.getenv('SQL_DRIVER')}}};"
            f"SERVER={os.getenv('SQL_SERVER')};"
            f"DATABASE={os.getenv('SQL_DATABASE')};"
            f"UID={os.getenv('SQL_USERNAME')};"
            f"PWD={os.getenv('SQL_PASSWORD')};"
            "TrustServerCertificate=yes;"
        )
        super().__init__()

    def _create_engine(self):
        return create_engine(f"mssql+pyodbc:///?odbc_connect={requests.utils.quote(self.connection_string)}")

    def _get_text_type(self):
        return MSSQL_TEXT

    def _create_index(self, connection, index_name, table_name, columns):
        connection.execute(text(f"""
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '{index_name}' AND object_id = OBJECT_ID('{table_name}'))
            CREATE INDEX {index_name} ON {table_name}({columns})
        """))

    def _drop_index(self, connection, index_name, table_name):
        connection.execute(text(f"""
            IF EXISTS (SELECT * FROM sys.indexes WHERE name = '{index_name}' AND object_id = OBJECT_ID('{table_name}'))
            DROP INDEX {index_name} ON {table_name}
        """))

    def _get_merge_query(self, snapshot_date, metrics):
        # Build the source CTE for all metrics
        source_queries = []
        for name, query in metrics:
            # Replace PostgreSQL date syntax with SQL Server syntax
            if 'CURRENT_DATE - INTERVAL' in query:
                query = query.replace("CURRENT_DATE - INTERVAL '1 day'", "DATEADD(day, -1, CAST(GETUTCDATE() AS DATE))")
            source_queries.append(f"SELECT CAST(GETUTCDATE() AS DATE) AS snapshot_date, '{name}' AS name, ({query}) AS number")
        
        source_cte = " UNION ALL ".join(source_queries)
        
        # Build the MERGE statement
        merge_query = f"""
        MERGE INTO history_snapshots AS target
        USING ({source_cte}) AS source
        ON target.snapshot_date = source.snapshot_date 
        AND target.name = source.name
        WHEN MATCHED THEN 
            UPDATE SET number = source.number
        WHEN NOT MATCHED THEN 
            INSERT (snapshot_date, name, number) 
            VALUES (source.snapshot_date, source.name, source.number);
        """
        
        return merge_query

    def _handle_identity_insert(self, connection, table_name, enable=True):
        connection.execute(text(f"SET IDENTITY_INSERT {table_name} {'ON' if enable else 'OFF'}"))

    def _get_last_sync_query(self):
        return """
            SELECT TOP 1 last_sync_time 
            FROM sync_status 
            WHERE entity_type = :entity_type 
            ORDER BY last_sync_time DESC
        """

    def update_parent_issue(self):
        """Update parent_issue references in bugs table with SQL Server syntax"""
        try:
            with self.engine.connect() as connection:
                update_query = text("""
                    WITH ValidParents AS (
                        SELECT 
                            b.id as bug_id,
                            COALESCE(
                                CASE WHEN i.id IS NOT NULL THEN b.parent_issue END,
                                (
                                    SELECT TOP 1 issue_id
                                    FROM bugs_relations br
                                    JOIN issues i2 ON br.issue_id = i2.id
                                    WHERE br.bug_id = b.id 
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
        """Add new columns to existing tables if they don't exist using SQL Server syntax"""
        inspector = inspect(self.engine)
        existing_columns = [col['name'] for col in inspector.get_columns(table_name)]
        
        new_columns = {
            'iteration_path': 'VARCHAR(500)',
            'hotfix_delivered_version': 'VARCHAR(200)'
        }
        
        for col_name, col_type in new_columns.items():
            if col_name not in existing_columns:
                connection.execute(text(f"""
                    IF NOT EXISTS (
                        SELECT * FROM sys.columns 
                        WHERE object_id = OBJECT_ID('{table_name}')
                        AND name = '{col_name}'
                    )
                    BEGIN
                        ALTER TABLE {table_name} ADD {col_name} {col_type}
                    END
                """))

class PostgreSQLConnection(DatabaseConnection):
    def __init__(self):
        self.connection_string = (
            f"postgresql://{os.getenv('PG_USERNAME')}:{os.getenv('PG_PASSWORD')}@"
            f"{os.getenv('PG_HOST')}:{os.getenv('PG_PORT', '5432')}/"
            f"{os.getenv('PG_DATABASE')}"
        )
        super().__init__()

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
            # Replace SQL Server date syntax with PostgreSQL syntax if needed
            if 'DATEADD' in query:
                query = query.replace("DATEADD(day, -1, CAST(GETUTCDATE() AS DATE))", "CURRENT_DATE - INTERVAL '1 day'")
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
        # PostgreSQL doesn't need identity insert handling as it uses OVERRIDING SYSTEM VALUE
        # when inserting explicit values into identity columns
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
        """Update parent_issue references in bugs table with PostgreSQL syntax"""
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
        """Add new columns to existing tables if they don't exist using PostgreSQL syntax"""
        inspector = inspect(self.engine)
        existing_columns = [col['name'] for col in inspector.get_columns(table_name)]
        
        new_columns = {
            'iteration_path': 'VARCHAR(500)',
            'hotfix_delivered_version': 'VARCHAR(200)'
        }
        
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

def get_database_connection():
    """Factory function to create the appropriate database connection based on environment variables"""
    db_type = os.getenv('DB_TYPE', 'sqlserver').lower()
    if db_type == 'postgresql':
        try:
            return PostgreSQLConnection()
        except ModuleNotFoundError as e:
            if "psycopg2" in str(e):
                raise ModuleNotFoundError(
                    "PostgreSQL driver (psycopg2) is not installed. "
                    "Please install it using: pip install psycopg2-binary"
                ) from e
            raise
    elif db_type == 'sqlserver':
        try:
            return SQLServerConnection()
        except ModuleNotFoundError as e:
            if "pyodbc" in str(e):
                raise ModuleNotFoundError(
                    "SQL Server driver (pyodbc) is not installed. "
                    "Please install it using: pip install pyodbc"
                ) from e
            raise
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

class ADOExtractor:
    def __init__(self, organization, project, personal_access_token):
        self.organization = organization
        self.project = requests.utils.quote(project)  # URL encode project name
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
        last_update_str = last_update if isinstance(last_update, str) else last_update.strftime('%Y-%m-%d')
        
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
                       [Custom.CustomernameGRC],
                       [System.AreaPath],
                       [System.Parent],
                       [System.IterationPath],
                       [Custom.HotfixDeliveredVersions]
                FROM WorkItems
                WHERE [System.WorkItemType] = '{work_item_type}'
                AND [System.ChangedDate] > @Today - 1
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
            
            # Print raw response data for debugging
            # print("\nRaw API Response for Work Items:")
            # print("--------------------------------")
            # for item in details_response.json()["value"]:
            #     print(f"\nWork Item {item['id']}:")
            #     print("Fields:")
            #     print(json.dumps(item["fields"], indent=2))
            #     print("--------------------------------")
            
            batch_data = []
            for item in details_response.json()["value"]:
                fields = item["fields"]
                
                # Handle AssignedTo field properly
                assigned_to = fields.get('System.AssignedTo')
                if isinstance(assigned_to, dict):
                    assigned_to = assigned_to.get('displayName', '')
                else:
                    assigned_to = str(assigned_to) if assigned_to is not None else ''
                
                item_data = {
                    'ID': item["id"],
                    'Title': fields.get('System.Title', ''),
                    'Description': fields.get('System.Description', ''),
                    'AssignedTo': assigned_to,
                    'Severity': fields.get('Microsoft.VSTS.Common.Priority', ''),
                    'State': fields.get('System.State', ''),
                    'CustomerName': fields.get('Custom.CustomernameGRC', ''),
                    'AreaPath': fields.get('System.AreaPath', ''),
                    'CreatedDate': fields.get('System.CreatedDate', ''),
                    'ChangedDate': fields.get('System.ChangedDate', ''),
                    'IterationPath': fields.get('System.IterationPath', ''),
                    'HotfixDeliveredVersion': fields.get('Custom.HotfixDeliveredVersions', '')
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
        Get and store the state change history for a list of bugs
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
                print(f"Error fetching state changes for bug {bug_id}: {response.status_code}")
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
                                AND field_changed = 'System.State'
                            """),
                            {"record_id": bug_id}
                        )
                        
                        # Now process and insert all state changes
                        for update in updates:
                            if "System.State" in update.get("fields", {}):
                                state_change = update["fields"]["System.State"]
                                # Get the changed date from the update object
                                changed_date = update.get("fields", {}).get("System.ChangedDate", {}).get("newValue", "")
                                changed_by = update.get("revisedBy", {}).get("displayName", "")
                                old_value = state_change.get("oldValue", "")
                                new_value = state_change.get("newValue", "")

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

def main():
    # Load configuration from .env file
    organization = os.getenv('ADO_ORGANIZATION')
    project = os.getenv('ADO_PROJECT')
    pat = os.getenv('ADO_PERSONAL_ACCESS_TOKEN')
    
    if not all([organization, project, pat]):
        raise ValueError("Please check your .env file and ensure ADO_ORGANIZATION, ADO_PROJECT, and ADO_PERSONAL_ACCESS_TOKEN are set")

    if pat == "your-pat-token-here":
        raise ValueError("Please update the ADO_PERSONAL_ACCESS_TOKEN in your .env file with your actual PAT")

    # Initialize extractor and database connection
    extractor = ADOExtractor(organization, project, pat)
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

    SLEEP_INTERVAL = 30 * 60  # 30 minutes in seconds
    
    print(f"Starting continuous sync with {SLEEP_INTERVAL/60} minute intervals")
    print("Press Ctrl+C to stop the program")
    
    while True:
        try:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n=== Starting sync cycle at {current_time} ===")
            
            # Get last sync times from database
            issues_last_sync = db.get_last_sync_time('issue')
            bugs_last_sync = db.get_last_sync_time('bug')
            
            # Extract and store issues first
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

            # Update history snapshots
            db.update_history_snapshots()
            print("------>Updated history snapshots")

            # Only update sync status if all operations completed successfully
            if processed_issues > 0:
                db.update_sync_status('issue', processed_issues)
            
            if processed_bugs > 0:
                db.update_sync_status('bug', processed_bugs)

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"=== Sync cycle completed at {current_time} ===")
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