#!/usr/bin/env python3
"""Utility functions for Redshift MCP Server."""

import logging
import psycopg2
import psycopg2.extensions
from contextlib import contextmanager
from typing import Dict, List, Tuple, Optional, Any

from redshift_mcp_server.models import RedshiftConnectionConfig


logger = logging.getLogger("redshift_mcp_server.util")


def test_connection(host: str, port: int, database: str, user: str, password: str) -> Tuple[bool, str]:
    """Test connection to Redshift and return version if successful.
    
    Args:
        host: Redshift host
        port: Redshift port
        database: Database name
        user: Username
        password: Password
        
    Returns:
        Tuple of (success_boolean, message)
    """
    try:
        conn = psycopg2.connect(
            host=host,
            port=int(port),
            database=database,
            user=user,
            password=password,
            connect_timeout=10
        )
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            
        conn.close()
        return True, version
    except Exception as e:
        return False, str(e)


def create_connection(config: RedshiftConnectionConfig) -> psycopg2.extensions.connection:
    """Create a new connection to Redshift.
    
    Args:
        config: Redshift connection configuration
        
    Returns:
        psycopg2 connection object
    """
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password,
        connect_timeout=10
    )


@contextmanager
def get_db_connection(config: RedshiftConnectionConfig):
    """Context manager for database connections.
    
    Args:
        config: Redshift connection configuration
        
    Yields:
        psycopg2 connection object
    """
    conn = None
    try:
        conn = create_connection(config)
        yield conn
    finally:
        if conn is not None:
            conn.close()


def format_query_results(cursor) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Format query results from cursor to list of dictionaries.
    
    Args:
        cursor: psycopg2 cursor with executed query
        
    Returns:
        Tuple of (results list, column names list)
    """
    # Fetch column names
    column_names = [desc[0] for desc in cursor.description]
    
    # Convert results to list of dictionaries
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(column_names, row)))
    
    return results, column_names


def get_error_detail(e: Exception) -> str:
    """Extract the most detailed error message from a psycopg2 exception.
    
    Args:
        e: psycopg2 exception
        
    Returns:
        Detailed error message
    """
    if hasattr(e, 'pgerror') and e.pgerror:
        return e.pgerror
    elif hasattr(e, 'diag') and e.diag.message_detail:
        return e.diag.message_detail
    else:
        return str(e)