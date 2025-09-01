# Multi-Platform Work Items Sync

This project synchronizes work items from **Azure DevOps** and **JIRA Cloud** to a local PostgreSQL database. It tracks work item details, state changes, relationships between items, and generates comprehensive metrics for both platforms.

## Features

- **Azure DevOps Integration**: Syncs bugs, issues, and all work item types
- **JIRA Cloud Integration**: Syncs bugs and stories with full pagination support
- **Unified Data Model**: Same database schema for both platforms
- **State Change Tracking**: Complete history of all status changes
- **Relationship Management**: Tracks parent-child relationships between items
- **Historical Metrics**: Generates KPI snapshots for both platforms
- **Customer Data**: Handles customer name mapping from both systems
- **Continuous Sync**: Configurable sync intervals (default: hourly)
- **Schema Isolation**: Separate schemas for ADO and JIRA data
- **Comprehensive Logging**: Real-time monitoring with log rotation

## Prerequisites

- Python 3.9 or higher
- PostgreSQL database
- Azure DevOps Personal Access Token (PAT) with read access to work items
- JIRA Cloud API token with read access to issues
- Required Python packages (see requirements.txt)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-directory>
```

2. Create and activate a virtual environment:
```bash
# Create virtual environment
python3 -m venv venv

# Activate on Unix/macOS
source venv/bin/activate

# Activate on Windows
.\venv\Scripts\activate

# Activate on Fish shell
source venv/bin/activate.fish
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file based on the example below.

## Configuration

Create a `.env` file in the project root with the following configuration:

```ini
# Database Configuration
PG_USERNAME=your_username
PG_PASSWORD=your_password
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=your_database

# Azure DevOps Configuration (for export_ado.py)
ADO_ORGANIZATION=your-organization
ADO_PROJECT=your-project
ADO_SCRUM_PROJECT=project1:team1,project2:team2
ADO_PERSONAL_ACCESS_TOKEN=your-pat-token-here

# JIRA Cloud Configuration (for export_jira.py)
JIRA_CLOUD_URL=https://your-domain.atlassian.net
JIRA_PROJECT_KEY=PROJ
JIRA_USER_EMAIL=your-email@domain.com
JIRA_API_TOKEN=your-jira-api-token-here
JIRA_SCHEMA_NAME=jira_sync

# Optional: Custom JIRA Field IDs (if different from defaults)
JIRA_CUSTOMER_NAME_FIELD_ID=customfield_10001
JIRA_SPRINT_FIELD_ID=customfield_10020
```

### Database Setup

The application automatically creates the necessary tables and indexes when it starts. Two separate schemas are created:

#### **Azure DevOps Schema (default)**
- `work_items`: Stores all work item types
- `bugs`: Stores bug-specific information  
- `issues`: Stores issue-specific information
- `bugs_relations`: Tracks relationships between bugs and issues
- `change_history`: Tracks state changes for all items
- `sync_status`: Tracks last sync times
- `history_snapshots`: Stores historical metrics

#### **JIRA Schema (configurable, default: `jira_sync`)**
- `bugs`: Stores JIRA bugs with JIRA keys as primary IDs
- `work_items`: Stores JIRA stories and other issue types
- `bugs_relations`: Tracks relationships between bugs and parent issues
- `change_history`: Tracks state changes using JIRA changelog
- `sync_status`: Tracks last sync times
- `history_snapshots`: Stores historical metrics

## Usage

### **Unified Sync (Recommended)**

Run both Azure DevOps and JIRA sync services simultaneously:
```bash
./run_exports.sh
```

This script will:
1. Start both ADO and JIRA sync services in parallel
2. Create separate log files for each service
3. Monitor both processes and show real-time status
4. Handle graceful shutdown for both services

### **Individual Services**

#### Azure DevOps Sync
```bash
python3 export_ado.py
```

#### JIRA Cloud Sync
```bash
python3 export_jira.py
```

### **What Happens During Sync**

Both services will:
1. Connect to their respective platforms and your PostgreSQL database
2. Sync work items that have changed since the last sync
3. Track state changes and relationships
4. Generate historical metrics and snapshots
5. Sleep for the configured interval (default: 1 hour)
6. Continue monitoring for changes

## Logging and Monitoring

### **Real-Time Log Monitoring**
```bash
# Follow ADO logs in real-time
tail -f logs/ado_export.log

# Follow JIRA logs in real-time
tail -f logs/jira_export.log

# Follow both simultaneously
tail -f logs/ado_export.log logs/jira_export.log
```

### **Log File Management**
- **Fixed log files**: `logs/ado_export.log` and `logs/jira_export.log`
- **Automatic rotation**: Logs rotate when they exceed 10MB
- **Clean startup**: Logs are cleared at the start of each run
- **No file clutter**: Reuses same files instead of creating timestamped versions

### **Process Monitoring**
```bash
# Check if services are running
ps aux | grep -E "(export_ado|export_jira)"

# View recent logs
tail -50 logs/ado_export.log
tail -50 logs/jira_export.log
```

## JIRA Integration Details

The JIRA integration follows the same data model as the Azure DevOps sync but uses JIRA Cloud REST API v3:

- **Authentication**: Uses email + API token with Basic Auth
- **Data Extraction**: Uses JQL queries to fetch bugs and their changes
- **Change Tracking**: Leverages JIRA's built-in changelog for state change history
- **Relationships**: Maps JIRA subtasks and issue links to the existing relationship model
- **Schema Isolation**: Creates a separate PostgreSQL schema to avoid conflicts with ADO data
- **Full Pagination**: Fetches all matching items regardless of count

### JIRA Field Mapping

- JIRA Summary → Database title
- JIRA Description → Database description (ADF to text with HTML fallback)
- JIRA Assignee → Database assigned_to
- JIRA Priority → Database severity (Critical = P1, High = P2)
- JIRA Status → Database state (New Request, IN-PROGRESS, READY FOR QA, IN QA, DONE, etc.)
- JIRA Components → Database area_path
- JIRA Sprint → Database iteration_path (configurable field ID)
- JIRA Custom Field (Customer Name) → Database customer_name (configurable field ID)
- JIRA Issue Key (e.g., PN-15092) → Database primary ID
- JIRA Numeric ID → Database numeric_id field

## Docker Support

### **Build and Run**
```bash
# Build the Docker image
docker build -t metabase-sync .

# Run the container
docker run -it metabase-sync
```

### **Docker Configuration**
The Dockerfile automatically:
- Uses the unified sync script (`./run_exports.sh`)
- Runs both ADO and JIRA services simultaneously
- Creates log files accessible from the container
- Handles graceful shutdown signals

### Custom Field Configuration

You'll need to identify the correct custom field IDs in your JIRA instance. The current code uses these defaults:

- **Customer Name**: `customfield_10001`
- **Sprint**: `customfield_10020`

To find the correct field IDs:

1. Go to any issue in your JIRA project
2. Right-click on the field and inspect the HTML
3. Look for the `customfield_XXXXX` ID in the field name
4. Update the environment variables in your `.env` file:
   ```bash
   JIRA_CUSTOMER_NAME_FIELD_ID=your_actual_field_id
   JIRA_SPRINT_FIELD_ID=your_actual_field_id
   ```

## Troubleshooting

### **Common Issues**

#### **Logs Not Appearing**
- Ensure you're using `python3` (not `python`)
- Check if processes are running: `ps aux | grep export_`
- Verify log files exist: `ls -la logs/`

#### **JIRA Authentication Issues**
- Verify your API token is correct
- Check your JIRA Cloud URL format
- Ensure your user has read access to the project

#### **Database Connection Issues**
- Verify PostgreSQL is running
- Check your database credentials in `.env`
- Ensure the database exists

#### **Custom Field Mapping Issues**
- Use the log viewer to see what fields are being fetched
- Verify custom field IDs in your JIRA instance
- Check if fields have different names in your JIRA setup

### **Getting Help**
- Check the logs for detailed error messages
- Verify all environment variables are set correctly
- Ensure both Python scripts can run individually before using the unified script 