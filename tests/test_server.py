#!/usr/bin/env python3
"""Unit tests for the server module."""

import os
import pytest
import time
from unittest.mock import patch, MagicMock, call
from contextlib import contextmanager
from psycopg2 import OperationalError, ProgrammingError
import psycopg2.extensions

from redshift_mcp_server.models import RedshiftConnectionConfig, QueryResult
from redshift_mcp_server.server import (
    app_lifespan,
    execute_query,
    run_query,
    explain_query,
    list_tables_in_schema,
    list_schemas,
    test_redshift_connection,
    main
)


class MockContext:
    """Mock for the MCP Context object."""
    
    def __init__(self):
        self.error_messages = []
        self.request_context = MagicMock()
        self.request_context.lifespan_context = {
            "redshift_config": RedshiftConnectionConfig(
                host="mock-host",
                port=5439,
                database="mock_db",
                user="mock_user",
                password="mock_password"
            )
        }
    
    def error(self, message):
        """Record error messages."""
        self.error_messages.append(message)


class TestAppLifespan:
    """Tests for app_lifespan context manager."""
    
    @patch('redshift_mcp_server.server.test_connection')
    def test_app_lifespan_success(self, mock_test_connection, monkeypatch):
        """Test successful app lifespan with valid config."""
        # Set environment variables
        monkeypatch.setenv('REDSHIFT_HOST', 'test-host')
        monkeypatch.setenv('REDSHIFT_PORT', '5439')
        monkeypatch.setenv('REDSHIFT_DATABASE', 'test_db')
        monkeypatch.setenv('REDSHIFT_USER', 'test_user')
        monkeypatch.setenv('REDSHIFT_PASSWORD', 'test_password')
        
        # Mock connection test
        mock_test_connection.return_value = (True, "PostgreSQL 13.2")
        
        # Execute test
        mock_server = MagicMock()
        with app_lifespan(mock_server) as context:
            config = context["redshift_config"]
            assert config.host == 'test-host'
            assert config.port == 5439
            assert config.database == 'test_db'
            assert config.user == 'test_user'
            assert config.password == 'test_password'
        
        mock_test_connection.assert_called_once_with(
            'test-host', '5439', 'test_db', 'test_user', 'test_password'
        )
    
    @patch('redshift_mcp_server.server.test_connection')
    def test_app_lifespan_missing_env_vars(self, mock_test_connection, monkeypatch):
        """Test app lifespan with missing environment variables."""
        # Clear environment variables
        for var in ['REDSHIFT_HOST', 'REDSHIFT_DATABASE', 'REDSHIFT_USER', 'REDSHIFT_PASSWORD']:
            monkeypatch.delenv(var, raising=False)
        
        # Execute test
        mock_server = MagicMock()
        with pytest.raises(ValueError) as excinfo:
            with app_lifespan(mock_server):
                pass
        
        assert "Missing Redshift connection parameters" in str(excinfo.value)
        mock_test_connection.assert_not_called()
    
    @patch('redshift_mcp_server.server.test_connection')
    def test_app_lifespan_connection_failure(self, mock_test_connection, monkeypatch):
        """Test app lifespan with connection failure."""
        # Set environment variables
        monkeypatch.setenv('REDSHIFT_HOST', 'test-host')
        monkeypatch.setenv('REDSHIFT_PORT', '5439')
        monkeypatch.setenv('REDSHIFT_DATABASE', 'test_db')
        monkeypatch.setenv('REDSHIFT_USER', 'test_user')
        monkeypatch.setenv('REDSHIFT_PASSWORD', 'test_password')
        
        # Mock connection test failure
        mock_test_connection.return_value = (False, "Connection refused")
        
        # Execute test
        mock_server = MagicMock()
        with pytest.raises(ConnectionError) as excinfo:
            with app_lifespan(mock_server):
                pass
        
        assert "Redshift connection test failed" in str(excinfo.value)
        mock_test_connection.assert_called_once()


class TestExecuteQuery:
    """Tests for execute_query function."""
    
    @patch('redshift_mcp_server.server.create_connection')
    def test_execute_select_query_success(self, mock_create_connection):
        """Test successful execution of a SELECT query."""
        # Set up mocks
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "test")]
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        mock_create_connection.return_value = mock_conn
        
        # Execute test
        ctx = MockContext()
        result = execute_query(ctx, "SELECT id, name FROM test_table")
        
        # Verify
        assert len(ctx.error_messages) == 0
        assert isinstance(result, QueryResult)
        assert result.rows == [{"id": 1, "name": "test"}]
        assert result.column_names == ["id", "name"]
        assert result.execution_time is not None
        assert result.affected_rows is None
        mock_create_connection.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('redshift_mcp_server.server.create_connection')
    def test_execute_update_query_success(self, mock_create_connection):
        """Test successful execution of an UPDATE query."""
        # Set up mocks
        mock_cursor = MagicMock()
        mock_cursor.description = None
        mock_cursor.rowcount = 5
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        mock_create_connection.return_value = mock_conn
        
        # Execute test
        ctx = MockContext()
        result = execute_query(ctx, "UPDATE test_table SET name = 'new_name' WHERE id = 1")
        
        # Verify
        assert len(ctx.error_messages) == 0
        assert isinstance(result, QueryResult)
        assert result.rows == [{"affected_rows": 5}]
        assert result.affected_rows == 5
        assert result.execution_time is not None
        assert result.column_names is None
        mock_create_connection.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('redshift_mcp_server.server.create_connection')
    def test_execute_query_empty_query(self, mock_create_connection):
        """Test execution with empty query."""
        # Execute test
        ctx = MockContext()
        with pytest.raises(ValueError) as excinfo:
            execute_query(ctx, "   ")
        
        # Verify
        assert "Empty query provided" in str(excinfo.value)
        assert len(ctx.error_messages) == 1
        assert "Empty query provided" in ctx.error_messages[0]
        mock_create_connection.assert_not_called()
    
    @patch('redshift_mcp_server.server.create_connection')
    def test_execute_query_db_error(self, mock_create_connection):
        """Test handling of database error during query execution."""
        # Set up mocks
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = ProgrammingError("Syntax error")
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        mock_create_connection.return_value = mock_conn
        
        # Execute test
        ctx = MockContext()
        with pytest.raises(ValueError) as excinfo:
            execute_query(ctx, "SELECT * FORM test_table")  # Intentional typo
        
        # Verify
        assert "ProgrammingError" in str(excinfo.value)
        assert len(ctx.error_messages) == 1
        assert "Query execution failed" in ctx.error_messages[0]
        mock_create_connection.assert_called_once()
        mock_conn.close.assert_called_once()


class TestToolFunctions:
    """Tests for MCP tool functions."""
    
    @patch('redshift_mcp_server.server.execute_query')
    def test_run_query(self, mock_execute_query):
        """Test run_query tool function."""
        # Set up mock
        mock_result = QueryResult(
            rows=[{"id": 1, "name": "test"}],
            column_names=["id", "name"],
            execution_time=0.123
        )
        mock_execute_query.return_value = mock_result
        
        # Execute test
        ctx = MockContext()
        result = run_query(ctx, "SELECT * FROM test_table")
        
        # Verify
        assert result == [{"id": 1, "name": "test"}]
        mock_execute_query.assert_called_once_with(ctx, "SELECT * FROM test_table")
    
    @patch('redshift_mcp_server.server.execute_query')
    def test_explain_query(self, mock_execute_query):
        """Test explain_query tool function."""
        # Set up mock
        mock_result = QueryResult(
            rows=[{"QUERY PLAN": "Seq Scan on test_table"}],
            column_names=["QUERY PLAN"],
            execution_time=0.123
        )
        mock_execute_query.return_value = mock_result
        
        # Execute test
        ctx = MockContext()
        result = explain_query(ctx, "SELECT * FROM test_table")
        
        # Verify
        assert result == [{"QUERY PLAN": "Seq Scan on test_table"}]
        mock_execute_query.assert_called_once_with(ctx, "EXPLAIN SELECT * FROM test_table")
    
    @patch('redshift_mcp_server.server.execute_query')
    def test_list_schemas(self, mock_execute_query):
        """Test list_schemas tool function."""
        # Set up mock
        mock_result = QueryResult(
            rows=[
                {"schema_name": "public", "schema_owner": "admin"},
                {"schema_name": "information_schema", "schema_owner": "admin"}
            ],
            column_names=["schema_name", "schema_owner"],
            execution_time=0.123
        )
        mock_execute_query.return_value = mock_result
        
        # Execute test
        ctx = MockContext()
        result = list_schemas(ctx)
        
        # Verify
        assert result == [
            {"schema_name": "public", "schema_owner": "admin"},
            {"schema_name": "information_schema", "schema_owner": "admin"}
        ]
        mock_execute_query.assert_called_once()
        assert "SELECT schema_name, schema_owner" in mock_execute_query.call_args[0][1]
    
    @patch('redshift_mcp_server.server.create_connection')
    @patch('redshift_mcp_server.server.execute_query')
    def test_list_tables_in_schema(self, mock_execute_query, mock_create_connection):
        """Test list_tables_in_schema tool function."""
        # Set up mocks
        mock_result = QueryResult(
            rows=[
                {"table_name": "users", "table_type": "TABLE", "table_schema": "public"},
                {"table_name": "orders", "table_type": "TABLE", "table_schema": "public"}
            ],
            column_names=["table_name", "table_type", "table_schema"],
            execution_time=0.123
        )
        mock_execute_query.return_value = mock_result
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = MagicMock()
        mock_create_connection.return_value = mock_conn
        
        # Execute test
        ctx = MockContext()
        result = list_tables_in_schema(ctx, "public")
        
        # Verify
        assert result == [
            {"table_name": "users", "table_type": "TABLE", "table_schema": "public"},
            {"table_name": "orders", "table_type": "TABLE", "table_schema": "public"}
        ]
        mock_create_connection.assert_called_once()
        mock_conn.close.assert_called_once()
        mock_execute_query.assert_called_once()
    
    def test_list_tables_in_schema_empty_schema(self):
        """Test list_tables_in_schema with empty schema name."""
        ctx = MockContext()
        with pytest.raises(ValueError) as excinfo:
            list_tables_in_schema(ctx, "")
        
        assert "Empty schema name provided" in str(excinfo.value)
        assert len(ctx.error_messages) == 1
        assert "Empty schema name provided" in ctx.error_messages[0]
    
    @patch('redshift_mcp_server.server.create_connection')
    def test_test_redshift_connection_success(self, mock_create_connection):
        """Test test_redshift_connection tool function success case."""
        # Set up mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ["PostgreSQL 13.2"]
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        mock_create_connection.return_value = mock_conn
        
        # Execute test
        ctx = MockContext()
        start_time = time.time()
        result = test_redshift_connection(ctx)
        
        # Verify
        assert result["status"] == "success"
        assert result["connected"] is True
        assert "PostgreSQL 13.2" in result["version"]
        assert start_time <= result["timestamp"] <= time.time()
        assert len(ctx.error_messages) == 0
        mock_create_connection.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('redshift_mcp_server.server.create_connection')
    def test_test_redshift_connection_failure(self, mock_create_connection):
        """Test test_redshift_connection tool function failure case."""
        # Set up mocks
        mock_create_connection.side_effect = OperationalError("Connection refused")
        
        # Execute test
        ctx = MockContext()
        result = test_redshift_connection(ctx)
        
        # Verify
        assert result["status"] == "error"
        assert result["connected"] is False
        assert "Connection refused" in result["message"]
        assert len(ctx.error_messages) == 1
        assert "Connection test failed" in ctx.error_messages[0]
        mock_create_connection.assert_called_once()


class TestMain:
    """Tests for main function."""
    
    @patch('redshift_mcp_server.server.mcp.run')
    @patch('redshift_mcp_server.server.argparse.ArgumentParser.parse_args')
    def test_main_stdio(self, mock_parse_args, mock_run):
        """Test main function with standard stdio transport."""
        # Set up mock
        mock_args = MagicMock()
        mock_args.sse = False
        mock_parse_args.return_value = mock_args
        
        # Execute test
        main()
        
        # Verify
        mock_run.assert_called_once_with()
    
    @patch('redshift_mcp_server.server.mcp.run')
    @patch('redshift_mcp_server.server.argparse.ArgumentParser.parse_args')
    def test_main_sse(self, mock_parse_args, mock_run):
        """Test main function with SSE transport."""
        # Set up mock
        mock_args = MagicMock()
        mock_args.sse = True
        mock_args.port = 8888
        mock_parse_args.return_value = mock_args
        
        # Execute test
        main()
        
        # Verify
        mock_run.assert_called_once_with(transport='sse')