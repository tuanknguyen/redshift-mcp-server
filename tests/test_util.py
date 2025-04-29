#!/usr/bin/env python3
"""Unit tests for the util module."""

import pytest
from unittest.mock import patch, MagicMock, call
from psycopg2 import OperationalError, ProgrammingError
import psycopg2.extensions
from redshift_mcp_server.models import RedshiftConnectionConfig
from redshift_mcp_server.util import (
    test_connection,
    create_connection,
    get_db_connection,
    format_query_results,
    get_error_detail
)


class TestTestConnection:
    """Tests for test_connection function."""
    
    @patch('redshift_mcp_server.util.psycopg2.connect')
    def test_successful_connection(self, mock_connect):
        """Test successful connection test with valid parameters."""
        # Set up mock
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ["PostgreSQL 13.2 on x86_64-apple-darwin"]
        
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_cursor
        
        mock_connect.return_value.cursor.return_value = mock_conn
        
        # Execute test
        success, message = test_connection(
            "redshift-host.example.com", 
            5439, 
            "test_db", 
            "test_user", 
            "test_password"
        )
        
        # Verify
        assert success is True
        assert "PostgreSQL 13.2" in message
        mock_connect.assert_called_once_with(
            host="redshift-host.example.com", 
            port=5439, 
            database="test_db", 
            user="test_user", 
            password="test_password",
            connect_timeout=10
        )
    
    @patch('redshift_mcp_server.util.psycopg2.connect')
    def test_connection_failure(self, mock_connect):
        """Test failed connection test."""
        # Set up mock
        mock_connect.side_effect = OperationalError("Connection refused")
        
        # Execute test
        success, message = test_connection(
            "invalid-host", 
            5439, 
            "test_db", 
            "test_user", 
            "test_password"
        )
        
        # Verify
        assert success is False
        assert "Connection refused" in message
        mock_connect.assert_called_once()


class TestCreateConnection:
    """Tests for create_connection function."""
    
    @patch('redshift_mcp_server.util.psycopg2.connect')
    def test_create_connection(self, mock_connect):
        """Test creating a connection with a config object."""
        # Set up test data
        config = RedshiftConnectionConfig(
            host="redshift-host.example.com",
            port=5439,
            database="test_db",
            user="test_user",
            password="test_password"
        )
        
        # Execute test
        create_connection(config)
        
        # Verify
        mock_connect.assert_called_once_with(
            host="redshift-host.example.com", 
            port=5439, 
            database="test_db", 
            user="test_user", 
            password="test_password",
            connect_timeout=10
        )


class TestGetDbConnection:
    """Tests for get_db_connection context manager."""
    
    @patch('redshift_mcp_server.util.create_connection')
    def test_context_manager_normal_exit(self, mock_create_connection):
        """Test that connection is properly closed on normal exit."""
        # Set up mock
        mock_conn = MagicMock()
        mock_create_connection.return_value = mock_conn
        
        # Set up test data
        config = RedshiftConnectionConfig(
            host="redshift-host.example.com",
            port=5439,
            database="test_db",
            user="test_user",
            password="test_password"
        )
        
        # Execute test
        with get_db_connection(config) as conn:
            assert conn is mock_conn
        
        # Verify
        mock_create_connection.assert_called_once_with(config)
        mock_conn.close.assert_called_once()
    
    @patch('redshift_mcp_server.util.create_connection')
    def test_context_manager_exception(self, mock_create_connection):
        """Test that connection is closed even when exception occurs."""
        # Set up mock
        mock_conn = MagicMock()
        mock_create_connection.return_value = mock_conn
        
        # Set up test data
        config = RedshiftConnectionConfig(
            host="redshift-host.example.com",
            port=5439,
            database="test_db",
            user="test_user",
            password="test_password"
        )
        
        # Execute test
        try:
            with get_db_connection(config) as conn:
                assert conn is mock_conn
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # Verify
        mock_create_connection.assert_called_once_with(config)
        mock_conn.close.assert_called_once()


class TestFormatQueryResults:
    """Tests for format_query_results function."""
    
    def test_format_query_results(self):
        """Test formatting query results from cursor."""
        # Set up mock cursor
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "John"), (2, "Jane")]
        
        # Execute test
        results, column_names = format_query_results(mock_cursor)
        
        # Verify
        assert column_names == ["id", "name"]
        assert results == [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
        mock_cursor.fetchall.assert_called_once()


class TestGetErrorDetail:
    """Tests for get_error_detail function."""
    
    def test_get_error_detail_pgerror(self):
        """Test getting error detail from pgerror attribute."""
        e = ProgrammingError()
        e.pgerror = "Syntax error near 'FROM'"
        
        detail = get_error_detail(e)
        assert detail == "Syntax error near 'FROM'"
    
    def test_get_error_detail_diag(self):
        """Test getting error detail from diag.message_detail."""
        e = ProgrammingError()
        e.pgerror = None
        e.diag = MagicMock()
        e.diag.message_detail = "Table does not exist"
        
        detail = get_error_detail(e)
        assert detail == "Table does not exist"
    
    def test_get_error_detail_fallback(self):
        """Test fallback to str(e) when no specific error info available."""
        e = Exception("Generic error")
        
        detail = get_error_detail(e)
        assert detail == "Generic error"