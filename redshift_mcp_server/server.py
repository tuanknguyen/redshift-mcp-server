#!/usr/bin/env python3
"""Redshift MCP Server implementation."""

import os
import sys
import argparse
import logging
import time
from typing import Dict, List, Optional, Any

import psycopg2
import psycopg2.extensions
from psycopg2 import sql

# Import models
from redshift_mcp_server.models import (
    RedshiftConnectionConfig,
    QueryResult,
    ConnectionTestResult,
    SchemaInfo,
    TableInfo
)

# Import utility functions
from redshift_mcp_server.util import (
    test_connection,
    create_connection,
    get_db_connection,
    format_query_results,
    get_error_detail
)

from mcp.server.fastmcp import FastMCP, Context
from pydantic import Field

# Setup logging
logger = logging.getLogger("redshift_mcp_server")
log_level = os.environ.get('FASTMCP_LOG_LEVEL', 'DEBUG')
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)


# Global connection configuration
redshift_config = None

def initialize_connection_config():
    """Initialize Redshift connection configuration."""
    global redshift_config
    
    # Get connection parameters from environment variables
    host = os.environ.get("REDSHIFT_HOST")
    port = os.environ.get("REDSHIFT_PORT", 5439)  # Default Redshift port
    database = os.environ.get("REDSHIFT_DATABASE")
    user = os.environ.get("REDSHIFT_USER")
    password = os.environ.get("REDSHIFT_PASSWORD")
    
    # Validate connection parameters
    if not all([host, database, user, password]):
        error_msg = "Missing Redshift connection parameters. Set the following environment variables: REDSHIFT_HOST, REDSHIFT_DATABASE, REDSHIFT_USER, REDSHIFT_PASSWORD"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Test connection before proceeding
    success, message = test_connection(host, port, database, user, password)
    if not success:
        error_msg = f"Redshift connection test failed: {message}"
        logger.error(error_msg)
        raise ConnectionError(error_msg)
    else:
        logger.info(f"Connection test successful: {message}")
    
    # Create connection configuration
    redshift_config = RedshiftConnectionConfig(
        host=host,
        port=int(port),
        database=database,
        user=user,
        password=password
    )
    
    logger.info(f"Redshift connection configuration created for {host}:{port}/{database}")
    return redshift_config

# Initialize the connection configuration
initialize_connection_config()

# Create the FastMCP server
mcp = FastMCP(
    'redshift-mcp-server',
    instructions="""
    # Redshift MCP Server
    
    This server provides tools to interact with Amazon Redshift databases, execute SQL queries, 
    analyze query performance, and explore database schema information.
    
    ## Best Practices
    
    - Use `list_schemas` and `list_tables_in_schema` to explore the database structure
    - For large queries, consider using `explain_query` first to understand execution plans
    - Always include appropriate WHERE clauses to limit result sets
    - Use column names explicitly instead of SELECT * for better performance
    - Consider adding LIMIT clauses to prevent returning too many rows
    
    ## Tool Selection Guide
    
    - Use `run_query` when: You need to execute a SQL query against the Redshift database
    - Use `explain_query` when: You want to understand a query's execution plan without running it
    - Use `list_schemas` when: You need to explore available schemas in the database
    - Use `list_tables_in_schema` when: You need to explore tables within a specific schema
    - Use `test_redshift_connection` when: You need to verify connectivity to the database
    """,
    dependencies=[
        'psycopg2',
        'pydantic',
    ],
)


# Helper function to execute queries safely
def execute_query(ctx: Context, query: str) -> QueryResult:
    """Execute a SQL query and return results as a QueryResult object."""
    if not query or not query.strip():
        error_msg = "Empty query provided"
        ctx.error(error_msg)
        raise ValueError(error_msg)
        
    try:
        # Use the global connection configuration
        config = redshift_config
        
        # Measure execution time
        start_time = time.time()
        
        # Create a new connection for this query
        conn = create_connection(config)
        logger.debug("Created new connection for query execution")
        
        try:
            # Use cursor with dictionary result
            with conn.cursor() as cursor:
                try:
                    logger.debug(f"Executing query: {query[:100]}...")
                    cursor.execute(query)
                    
                    # For SELECT queries, return results
                    if cursor.description:
                        # Format the results using utility function
                        results, column_names = format_query_results(cursor)
                        
                        logger.debug(f"Query returned {len(results)} rows")
                        
                        # Calculate execution time
                        execution_time = time.time() - start_time
                        
                        return QueryResult(
                            rows=results,
                            column_names=column_names,
                            execution_time=execution_time
                        )
                    # For non-SELECT queries, return affected row count
                    else:
                        row_count = cursor.rowcount
                        execution_time = time.time() - start_time
                        
                        return QueryResult(
                            rows=[{"affected_rows": row_count}],
                            affected_rows=row_count,
                            execution_time=execution_time
                        )
                        
                except psycopg2.Error as e:
                    # Get the most detailed error message possible from Redshift
                    error_detail = get_error_detail(e)
                    
                    error_msg = f"Query execution failed: {error_detail}"
                    logger.error(f"Database error: {error_detail}")
                    ctx.error(error_msg)
                    
                    # Preserve the original error type in the exception message
                    raise ValueError(f"{e.__class__.__name__}: {error_detail}")
        finally:
            # Always close the connection
            conn.close()
            logger.debug("Connection closed after query execution")
                
    except Exception as e:
        if not isinstance(e, ValueError):  # Don't double-wrap ValueError exceptions
            error_msg = f"Error during query execution: {str(e)}"
            ctx.error(error_msg)
            raise ValueError(error_msg)
        raise


@mcp.tool()
def run_query(
    ctx: Context,
    query: str = Field(description="The SQL query to execute"),
) -> List[Dict[str, Any]]:
    """
    Run a SQL query on Redshift and return the results.
    
    ## Usage
    
    This tool executes a SQL query against the Redshift database and returns the results.
    For SELECT queries, it returns the matching rows as a list of dictionaries.
    For non-SELECT queries (INSERT, UPDATE, DELETE, etc.), it returns the number of affected rows.
    
    ## Best Practices
    
    - Include appropriate WHERE clauses to limit result sets
    - Use column names explicitly instead of SELECT * for better performance 
    - Consider adding LIMIT clauses to prevent returning too many rows
    - For complex queries, test with LIMIT first before running on full dataset
    - For very large result sets, consider using aggregate functions
    
    ## Example Queries
    
    - SELECT * FROM schema_name.table_name LIMIT 10
    - SELECT column1, column2 FROM schema_name.table_name WHERE condition = 'value'
    - SELECT COUNT(*) FROM schema_name.table_name WHERE date_column > '2024-01-01'
    
    Args:
        ctx: MCP context for logging and error handling
        query: The SQL query to execute
        
    Returns:
        Results of the query as a list of dictionaries
    """
    result = execute_query(ctx, query)
    return result.rows


@mcp.tool()
def explain_query(
    ctx: Context,
    query: str = Field(description="The SQL query to explain"),
) -> List[Dict[str, Any]]:
    """
    Get the execution plan for a SQL query.
    
    ## Usage
    
    This tool returns the execution plan for a SQL query without actually executing the query.
    It's useful for understanding how Redshift will process a query and identifying potential
    performance issues before running it.
    
    ## When to Use
    
    - Before running complex queries to understand their execution plan
    - To identify potential performance bottlenecks
    - When troubleshooting slow queries
    - To understand join operations and table access patterns
    
    ## Understanding the Results
    
    The execution plan shows:
    - The order of operations (bottom to top)
    - Table scans and access methods
    - Join types and conditions
    - Sort operations
    - Estimated costs
    
    Args:
        ctx: MCP context for logging and error handling
        query: The SQL query to explain
        
    Returns:
        Query execution plan
    """
    explain_query = f"EXPLAIN {query}"
    result = execute_query(ctx, explain_query)
    return result.rows


@mcp.tool()
def list_tables_in_schema(
    ctx: Context,
    schema_name: str = Field(description="The name of the schema"),
) -> List[Dict[str, Any]]:
    """
    List all tables and views in a specific schema.
    
    ## Usage
    
    This tool returns a list of all tables and views in the specified schema,
    along with their types and other metadata.
    
    ## When to Use
    
    - To explore the structure of a database schema
    - When you need to find tables or views in a specific schema
    - Before writing queries to understand available tables
    - To verify table existence or type
    
    ## Understanding the Results
    
    Each result includes:
    - table_name: The name of the table or view
    - table_type: The type (TABLE, VIEW, etc.)
    - table_schema: The schema name
    
    ## Common Schema Names
    
    - public: The default schema for user-created objects
    - information_schema: System schema with metadata about the database
    - pg_catalog: System schema with Postgres/Redshift system tables
    - Custom schemas created for your specific workload
    
    Args:
        ctx: MCP context for logging and error handling
        schema_name: The name of the schema
        
    Returns:
        List of tables and views in the schema
    """
    if not schema_name or not schema_name.strip():
        error_msg = "Empty schema name provided"
        ctx.error(error_msg)
        raise ValueError(error_msg)
        
    # Use parameterized query with proper escaping to prevent SQL injection
    query = sql.SQL("""
    SELECT table_name, table_type, table_schema
    FROM information_schema.tables
    WHERE table_schema = {}
    ORDER BY table_name
    """).format(sql.Literal(schema_name))
    
    try:
        # Use the global connection configuration
        config = redshift_config
        
        # Create a new connection just to format the SQL query
        conn = create_connection(config)
        formatted_query = query.as_string(conn)
        conn.close()
        
        result = execute_query(ctx, formatted_query)
        return result.rows
    except ValueError as e:
        # Re-raise with more specific error message
        error_msg = f"Failed to list tables in schema '{schema_name}': {str(e)}"
        ctx.error(error_msg)
        raise ValueError(error_msg)


@mcp.tool()
def list_schemas(ctx: Context) -> List[Dict[str, Any]]:
    """
    List all schemas in the database.
    
    ## Usage
    
    This tool returns a list of all schemas in the database along with their owners.
    Use it to explore the high-level structure of the Redshift database.
    
    ## When to Use
    
    - When first connecting to a database to explore its organization
    - To discover available schemas before querying specific tables
    - To check schema ownership or permissions
    
    ## Common Schemas
    
    Most Redshift databases include these standard schemas:
    - public: Default schema for user-created objects
    - information_schema: Contains metadata views about the database structure
    - pg_catalog: Contains system tables and views
    
    Additional schemas may be created for different applications, teams, or data sources.
    
    ## Next Steps
    
    After identifying a schema of interest, use list_tables_in_schema to explore
    the tables and views available within that schema.
    
    Args:
        ctx: MCP context for logging and error handling
        
    Returns:
        List of schemas in the database
    """
    query = """
    SELECT schema_name, schema_owner
    FROM information_schema.schemata
    ORDER BY schema_name
    """
    result = execute_query(ctx, query)
    return result.rows


@mcp.tool()
def test_redshift_connection(ctx: Context) -> Dict[str, Any]:
    """
    Test connection to the Redshift database.
    
    ## Usage
    
    This tool verifies that the connection to the Redshift database is working correctly.
    It establishes a connection and runs a simple query to confirm database availability.
    
    ## When to Use
    
    - When first setting up the connection to verify credentials
    - When troubleshooting connectivity issues
    - To check if the database is available
    - To view the current Redshift version
    
    ## Result Interpretation
    
    If successful, the result includes:
    - status: "success"
    - connected: true
    - version: The Redshift version string
    - timestamp: When the test was performed
    
    If unsuccessful, the result includes:
    - status: "error"
    - connected: false
    - message: Details about the error
    
    Args:
        ctx: MCP context for logging and error handling
        
    Returns:
        Dictionary with connection status and version information
    """
    try:
        # Use the global connection configuration
        config = redshift_config
        
        # Create a new connection for testing
        conn = create_connection(config)
        
        try:
            # Test connection with a simple query
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                
            return {
                "status": "success",
                "version": version,
                "connected": True,
                "timestamp": time.time()
            }
        finally:
            # Always close the connection
            conn.close()
    
    except Exception as e:
        error_msg = f"Connection test failed: {str(e)}"
        ctx.error(error_msg)
        return {"status": "error", "message": error_msg, "connected": False}


def main():
    """Run the MCP server with CLI argument support."""
    parser = argparse.ArgumentParser(
        description='Redshift Model Context Protocol (MCP) server'
    )
    parser.add_argument('--sse', action='store_true', help='Use SSE transport')
    parser.add_argument('--port', type=int, default=8888, help='Port to run the server on')

    args = parser.parse_args()

    # Log startup information
    logger.info('Starting Redshift MCP Server')
    
    # Ensure connection config is initialized
    if redshift_config is None:
        initialize_connection_config()

    # Run server with appropriate transport
    if args.sse:
        logger.info(f'Using SSE transport on port {args.port}')
        mcp.settings.port = args.port
        mcp.run(transport='sse')
    else:
        logger.info('Using standard stdio transport')
        mcp.run()


if __name__ == '__main__':
    main()