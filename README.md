# Azure DevOps Work Items Sync

This project synchronizes work items (bugs, issues, and other work item types) from Azure DevOps to a local database (PostgreSQL or SQL Server). It tracks work item details, state changes, and relationships between items.

## Features

- Syncs multiple types of work items (bugs, issues, and all other work item types)
- Tracks state changes history
- Maintains relationships between bugs and issues
- Supports both PostgreSQL and SQL Server databases
- Creates historical snapshots for metrics
- Handles customer name updates
- Continuous sync with configurable intervals

## Prerequisites

- Python 3.9 or higher
- PostgreSQL or SQL Server database
- Azure DevOps Personal Access Token (PAT) with read access to work items
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
# Azure DevOps Configuration
ADO_ORGANIZATION=your-organization
ADO_PROJECT=your-project
ADO_PERSONAL_ACCESS_TOKEN=your-pat-token-here

# Database Type (postgresql)
DB_TYPE=postgresql

# PostgreSQL Configuration (if using PostgreSQL)
PG_USERNAME=your_username
PG_PASSWORD=your_password
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=your_database


```

### Database Setup

The application will automatically create the necessary tables and indexes when it starts. The following tables will be created:

- `work_items`: Stores all work item types
- `bugs`: Stores bug-specific information
- `issues`: Stores issue-specific information
- `bugs_relations`: Tracks relationships between bugs and issues
- `change_history`: Tracks state changes for all items
- `sync_status`: Tracks last sync times
- `history_snapshots`: Stores historical metrics

## Usage

Run the script:
```bash
python export_ado.py
```

The script will:
1. Connect to Azure DevOps and your configured database
2. Sync all work items that have changed in the last day
3. Track state changes for all items
4. Update relationships between bugs and issues
5. Create historical snapshots
6. Sleep for the configured interval (default: 30 minutes)
7. Repeat the process

To stop the script, press Ctrl+C. The script will shut down gracefully.

## Database Support

### PostgreSQL
- Requires `psycopg2-binary` package
- Supports all features including automatic index creation
- Uses native upsert functionality

## Monitoring

The script provides detailed logging of its operations:
- Number of items processed in each sync cycle
- State changes processed
- Any errors encountered
- Sync completion status

## Error Handling

The script includes robust error handling:
- Graceful handling of API rate limits
- Database connection retry logic
- Transaction management for data consistency
- Detailed error logging

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 