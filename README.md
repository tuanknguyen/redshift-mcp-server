# Redshift MCP Server

A Model Context Protocol (MCP) server for interacting with Amazon Redshift databases.

## Features

- Run SQL queries on Redshift
- Explain SQL query execution plans
- List tables and views within a schema
- List all schemas within the database
- Test connection to Redshift

## Prerequisites

- Python 3.10+
- Amazon Redshift database credentials

## Installation

### Installation Requirements

1. Install `uv` from [Astral](https://docs.astral.sh/uv/getting-started/installation/) or the [GitHub README](https://github.com/astral-sh/uv#installation)
2. Install Python 3.10 or newer using `uv python install 3.10` (or a more recent version)

### Configure for Amazon Q or Claude

To add this MCP server to your Amazon Q or Claude, add the following to your MCP config file. With Amazon Q, create (if does not yet exist) a file named `.amazonq/mcp.json` under the same directory that is running `q chat`. Then add the following config:

```json
{
  "mcpServers": {
    "redshift": {
      "type": "stdio",
      "command": "uvx",
      "args": [
        "--with", "<path-to-redshift-mcp-server>", "redshift-mcp-server"
      ],
      "env": {
        "FASTMCP_LOG_LEVEL": "ERROR"
      },
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

### Setup Your Credentials

⚠️ **SECURITY WARNING** ⚠️

**NEVER commit credential files to version control or include them in Docker images!**

Set up your Redshift credentials using environment variables:

```bash
# Linux/macOS
export REDSHIFT_HOST=<your-redshift-host>
export REDSHIFT_PORT=5439  # Default Redshift port
export REDSHIFT_DATABASE=<your-database-name>
export REDSHIFT_USER=<your-username>
export REDSHIFT_PASSWORD=<your-password>

# Windows (Command Prompt)
set REDSHIFT_HOST=<your-redshift-host>
set REDSHIFT_PORT=5439
set REDSHIFT_DATABASE=<your-database-name>
set REDSHIFT_USER=<your-username>
set REDSHIFT_PASSWORD=<your-password>

# Windows (PowerShell)
$env:REDSHIFT_HOST = "<your-redshift-host>"
$env:REDSHIFT_PORT = "5439"
$env:REDSHIFT_DATABASE = "<your-database-name>"
$env:REDSHIFT_USER = "<your-username>"
$env:REDSHIFT_PASSWORD = "<your-password>"
```

## Development

1. Clone this repository:
   ```bash
   git clone https://github.com/tuanknguyen/redshift-mcp-server.git
   cd redshift-mcp-server
   ```

2. Create and activate a virtual environment:
   ```bash
   # Create the venv
   uv venv
   
   # Activate it
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install the project:
   ```bash
   # Install the project and its dependencies
   uv pip install -e .
   
   # Or for development
   uv pip install -e ".[dev]"
   ```

## Configuration

Set the following environment variables with your Redshift connection details:

```bash
export REDSHIFT_HOST=<your-redshift-host>
export REDSHIFT_PORT=5439  # Default Redshift port
export REDSHIFT_DATABASE=<your-database-name>
export REDSHIFT_USER=<your-username>
export REDSHIFT_PASSWORD=<your-password>
```

Optional environment variables:
```bash
export FASTMCP_LOG_LEVEL=INFO  # Set log level (DEBUG, INFO, WARNING, ERROR)
```

### Development Mode

For development and testing with the MCP Inspector:

```bash
mcp dev redshift-mcp-server
```
## Available Tools

- `run_query(query: str)` - Execute a SQL query against Redshift
- `explain_query(query: str)` - Get the execution plan for a SQL query
- `list_tables_in_schema(schema_name: str)` - List all tables and views in a specific schema
- `list_schemas()` - List all schemas in the database
- `test_redshift_connection()` - Test connectivity to the Redshift database

## Connection Testing

The MCP server automatically tests the connection when starting up. If the connection test fails, the server will not start and will raise an error.

You can also run a standalone connection test using:

```bash
python test_connection.py
```

## Best Practices

- Use `list_schemas` and `list_tables_in_schema` to explore the database structure
- For large queries, consider using `explain_query` first to understand execution plans
- Always include appropriate WHERE clauses to limit result sets
- Use column names explicitly instead of SELECT * for better performance
- Consider adding LIMIT clauses to prevent returning too many rows
