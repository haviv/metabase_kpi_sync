#!/usr/bin/env python3
"""
Test that change_history indexes are properly created during setup.
Verifies fix for GitHub issue #1: slow Metabase reports on historical data.
"""

import unittest
from unittest.mock import MagicMock, patch, call
from sqlalchemy import text


EXPECTED_CHANGE_HISTORY_INDEXES = {
    "idx_change_history_query_opt": "table_name, field_changed, new_value, changed_date",
    "idx_change_history_changed_date": "changed_date",
    "idx_change_history_record_id": "record_id, table_name",
    "idx_change_history_field_changed": "field_changed, table_name",
}


class TestChangeHistoryIndexes(unittest.TestCase):
    """Verify that change_history table indexes are created in setup_tables."""

    @patch.dict(
        "os.environ",
        {
            "PG_USERNAME": "test",
            "PG_PASSWORD": "test",
            "PG_HOST": "localhost",
            "PG_PORT": "5432",
            "PG_DATABASE": "test_db",
        },
    )
    @patch("export_ado.create_engine")
    @patch("export_ado.inspect")
    @patch("export_ado.MetaData")
    def test_ado_setup_creates_change_history_indexes(
        self,
        mock_metadata_cls: MagicMock,
        mock_inspect: MagicMock,
        mock_create_engine: MagicMock,
    ) -> None:
        """ADO DatabaseConnection.setup_tables creates change_history indexes."""
        from export_ado import DatabaseConnection

        # Mock the engine and connection
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_connection
        )
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        # Mock inspector to report all tables as existing
        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.get_table_names.return_value = [
            "issues",
            "bugs",
            "work_items",
            "sync_status",
            "sprints",
            "sprint_capacity",
            "change_history",
            "history_snapshots",
            "bugs_relations",
        ]
        mock_inspector.get_columns.return_value = []

        # Mock metadata
        mock_metadata = MagicMock()
        mock_metadata_cls.return_value = mock_metadata

        db = DatabaseConnection()

        # Collect all SQL statements executed on the connection
        executed_sql = []
        for c in mock_connection.execute.call_args_list:
            args = c[0]
            if args and hasattr(args[0], "text"):
                executed_sql.append(args[0].text.strip())

        # Verify each expected change_history index was created
        for index_name, columns in EXPECTED_CHANGE_HISTORY_INDEXES.items():
            create_stmt = (
                f"CREATE INDEX IF NOT EXISTS {index_name} ON change_history({columns})"
            )
            found = any(create_stmt in sql for sql in executed_sql)
            self.assertTrue(
                found,
                f"Missing index creation: {index_name} on change_history({columns})",
            )

        # Verify each expected change_history index was dropped (for recreation)
        for index_name in EXPECTED_CHANGE_HISTORY_INDEXES:
            drop_stmt = f"DROP INDEX IF EXISTS {index_name}"
            found = any(drop_stmt in sql for sql in executed_sql)
            self.assertTrue(
                found,
                f"Missing index drop: {index_name}",
            )

    @patch.dict(
        "os.environ",
        {
            "PG_USERNAME": "test",
            "PG_PASSWORD": "test",
            "PG_HOST": "localhost",
            "PG_PORT": "5432",
            "PG_DATABASE": "test_db",
        },
    )
    @patch("export_jira.create_engine")
    @patch("export_jira.inspect")
    @patch("export_jira.MetaData")
    def test_jira_setup_creates_change_history_indexes(
        self,
        mock_metadata_cls: MagicMock,
        mock_inspect: MagicMock,
        mock_create_engine: MagicMock,
    ) -> None:
        """JIRA JIRADatabaseConnection.setup_tables creates change_history indexes."""
        from export_jira import JIRADatabaseConnection

        # Mock the engine and connection
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_connection
        )
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        # Mock inspector
        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.get_table_names.return_value = [
            "bugs",
            "sync_status",
            "change_history",
            "history_snapshots",
            "bugs_relations",
            "sprints",
            "work_items",
        ]
        mock_inspector.get_columns.return_value = []

        # Mock metadata
        mock_metadata = MagicMock()
        mock_metadata_cls.return_value = mock_metadata

        db = JIRADatabaseConnection(schema_name="test_schema")

        # Collect all SQL statements executed on the connection
        executed_sql = []
        for c in mock_connection.execute.call_args_list:
            args = c[0]
            if args and hasattr(args[0], "text"):
                executed_sql.append(args[0].text.strip())

        # Verify each expected change_history index was created (schema-qualified)
        for index_name, columns in EXPECTED_CHANGE_HISTORY_INDEXES.items():
            create_stmt = f"CREATE INDEX IF NOT EXISTS {index_name} ON test_schema.change_history({columns})"
            found = any(create_stmt in sql for sql in executed_sql)
            self.assertTrue(
                found,
                f"Missing JIRA index creation: {index_name} on test_schema.change_history({columns})",
            )


if __name__ == "__main__":
    unittest.main()
