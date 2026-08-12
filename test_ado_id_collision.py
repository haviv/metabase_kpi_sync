"""Sync specific Jira keys and verify ADO-id collision handling."""
import os
from dotenv import load_dotenv
from sqlalchemy import text

from export_ado import get_database_connection, JIRAExtractor

load_dotenv()

TEST_KEYS = [
    "NEXUS-20829", "NEXUS-19091",
    "NEXUS-20733", "NEXUS-21229",
    "NEXUS-6127", "NEXUS-21004",
    "NEXUS-20529", "NEXUS-20277",
    "NEXUS-20268", "NEXUS-20266",
]

ADO_PAIRS = [
    ("NEXUS-20829", "NEXUS-19091", 68677),
    ("NEXUS-20733", "NEXUS-21229", 68609),
    ("NEXUS-6127", "NEXUS-21004", 65914),
    ("NEXUS-20529", "NEXUS-20277", 69211),
    ("NEXUS-20268", "NEXUS-20266", 69788),
]


def main():
    db = get_database_connection()
    jira_extractor = JIRAExtractor(
        os.getenv("JIRA_CLOUD_URL"),
        os.getenv("JIRA_NEXUS_PROJECT_KEY") or os.getenv("JIRA_PROJECT_KEY"),
        os.getenv("JIRA_USER_EMAIL"),
        os.getenv("JIRA_API_TOKEN"),
        os.getenv("JIRA_NEXUS_BOARD_ID", "1766"),
    )
    jira_extractor.db_connection = db

    jql = f"key in ({','.join(TEST_KEYS)})"
    issues = jira_extractor._search_issues(jql, jira_extractor._fields_param())
    print(f"Fetched {len(issues)} issues from Jira")

    work_items = []
    bugs = []
    for issue in issues:
        fields = issue.get("fields", {})
        item_data = jira_extractor._base_item_data(issue, "work_items")
        item_data["WorkItemType"] = jira_extractor._extract_value(fields, "issuetype") or "Unknown"
        work_items.append(item_data)
        if item_data["WorkItemType"] == "Bug":
            bugs.append(item_data)
        print(
            f"  {issue['key']}: assigned id={item_data['ID']}, "
            f"ado_field={jira_extractor._extract_migrated_ado_id(fields)}"
        )

    if bugs:
        db.upsert_items(bugs, db.bugs, "bug")
    if work_items:
        db.upsert_items(work_items, db.work_items, "work_item")

    print("\n--- DB state after upsert ---")
    with db.engine.connect() as conn:
        for table in ("work_items", "bugs"):
            rows = conn.execute(
                text(
                    f"""
                    SELECT jira_id, id, title
                    FROM {table}
                    WHERE jira_id = ANY(:keys)
                    ORDER BY jira_id
                    """
                ),
                {"keys": TEST_KEYS},
            ).fetchall()
            if rows:
                print(f"\n{table}:")
                for row in rows:
                    print(f"  {row[0]} | id={row[1]} | {row[2][:60]}")

    print("\n--- Collision pair checks ---")
    with db.engine.connect() as conn:
        for clone, original, ado_id in ADO_PAIRS:
            rows = conn.execute(
                text(
                    """
                    SELECT jira_id, id FROM work_items WHERE jira_id IN (:a, :b)
                    UNION ALL
                    SELECT jira_id, id FROM bugs WHERE jira_id IN (:a, :b)
                    """
                ),
                {"a": clone, "b": original},
            ).fetchall()
            by_key = {r[0]: r[1] for r in rows}
            clone_id = by_key.get(clone)
            orig_id = by_key.get(original)
            ok = clone in by_key and original in by_key and clone_id != orig_id
            owner_of_ado = conn.execute(
                text(
                    """
                    SELECT jira_id, id FROM work_items WHERE id = :ado_id
                    UNION ALL
                    SELECT jira_id, id FROM bugs WHERE id = :ado_id
                    """
                ),
                {"ado_id": ado_id},
            ).fetchall()
            status = "PASS" if ok else "FAIL"
            print(
                f"{status} {clone} (id={clone_id}) + {original} (id={orig_id}) | "
                f"ADO {ado_id} owned by: {owner_of_ado}"
            )


if __name__ == "__main__":
    main()
