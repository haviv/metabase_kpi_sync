#!/usr/bin/env python3
"""
Test script to verify the developer field implementation
"""
import os
from dotenv import load_dotenv
from pathlib import Path
from export_jira import JIRAExtractor, get_database_connection

# Load environment variables from .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

def test_developer_field():
    """Test the developer field extraction and database operations"""
    
    # Get JIRA configuration
    jira_url = os.getenv('JIRA_CLOUD_URL')
    project_key = os.getenv('JIRA_PROJECT_KEY')
    email = os.getenv('JIRA_USER_EMAIL')
    api_token = os.getenv('JIRA_API_TOKEN')
    schema_name = os.getenv('JIRA_SCHEMA_NAME', 'jira_sync')
    
    if not all([jira_url, project_key, email, api_token]):
        print("Error: Please set JIRA_CLOUD_URL, JIRA_PROJECT_KEY, JIRA_USER_EMAIL, and JIRA_API_TOKEN environment variables")
        return
    
    if api_token == "your-jira-api-token-here":
        print("Error: Please update the JIRA_API_TOKEN in your .env file with your actual API token")
        return
    
    print("Testing Developer Field Implementation")
    print("=" * 50)
    
    try:
        # Initialize extractor and database connection
        extractor = JIRAExtractor(jira_url, project_key, email, api_token)
        db = get_database_connection(schema_name)
        
        print(f"✓ JIRA Extractor initialized")
        print(f"✓ Database connection established")
        print(f"✓ Developer field ID: {extractor.developer_field}")
        
        # Test field extraction with a sample issue
        from datetime import datetime, timedelta
        last_sync_time = datetime.now() - timedelta(days=1)
        
        print(f"\nFetching bugs since: {last_sync_time}")
        bugs = extractor.get_bugs_since_last_sync(last_sync_time)
        
        if bugs:
            print(f"✓ Fetched {len(bugs)} bugs")
            
            # Check if developer field is present in the first bug
            first_bug = bugs[0]
            if 'developer' in first_bug:
                print(f"✓ Developer field found in bug data: {first_bug['developer']}")
            else:
                print("⚠ Developer field not found in bug data")
            
            # Test database upsert
            print(f"\nTesting database upsert...")
            processed_bugs = db.upsert_bugs(bugs[:1])  # Test with just one bug
            print(f"✓ Processed {processed_bugs} bugs in database")
            
        else:
            print("⚠ No bugs found for testing")
        
        # Test work items
        print(f"\nFetching work items since: {last_sync_time}")
        work_items = extractor.get_stories_since_last_sync(last_sync_time)
        
        if work_items:
            print(f"✓ Fetched {len(work_items)} work items")
            
            # Check if developer field is present in the first work item
            first_item = work_items[0]
            if 'developer' in first_item:
                print(f"✓ Developer field found in work item data: {first_item['developer']}")
            else:
                print("⚠ Developer field not found in work item data")
            
            # Test database upsert
            print(f"\nTesting database upsert...")
            processed_items = db.upsert_work_items(work_items[:1])  # Test with just one item
            print(f"✓ Processed {processed_items} work items in database")
            
        else:
            print("⚠ No work items found for testing")
        
        print(f"\n✅ Developer field implementation test completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during testing: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_developer_field()
