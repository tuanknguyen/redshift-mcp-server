#!/usr/bin/env python3
"""Unit tests for the models module."""

import pytest
from pydantic import ValidationError
from redshift_mcp_server.models import (
    RedshiftConnectionConfig,
    QueryResult,
    ConnectionTestResult,
    SchemaInfo,
    TableInfo
)


class TestRedshiftConnectionConfig:
    """Tests for RedshiftConnectionConfig dataclass."""
    
    def test_valid_config(self):
        """Test creating a valid connection configuration."""
        config = RedshiftConnectionConfig(
            host="redshift-host.example.com",
            port=5439,
            database="test_db",
            user="test_user",
            password="test_password"
        )
        assert config.host == "redshift-host.example.com"
        assert config.port == 5439
        assert config.database == "test_db"
        assert config.user == "test_user"
        assert config.password == "test_password"


class TestQueryResult:
    """Tests for QueryResult model."""
    
    def test_minimal_query_result(self):
        """Test creating a QueryResult with only required fields."""
        result = QueryResult(rows=[{"id": 1, "name": "test"}])
        assert result.rows == [{"id": 1, "name": "test"}]
        assert result.column_names is None
        assert result.affected_rows is None
        assert result.execution_time is None
    
    def test_full_query_result(self):
        """Test creating a QueryResult with all fields."""
        result = QueryResult(
            rows=[{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}],
            column_names=["id", "name"],
            affected_rows=2,
            execution_time=0.123
        )
        assert result.rows == [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
        assert result.column_names == ["id", "name"]
        assert result.affected_rows == 2
        assert result.execution_time == 0.123


class TestConnectionTestResult:
    """Tests for ConnectionTestResult model."""
    
    def test_successful_connection_result(self):
        """Test creating a successful connection test result."""
        result = ConnectionTestResult(
            status="success",
            connected=True,
            version="PostgreSQL 13.2 on x86_64-apple-darwin",
            timestamp=1612345678.9
        )
        assert result.status == "success"
        assert result.connected is True
        assert "PostgreSQL" in result.version
        assert result.timestamp == 1612345678.9
        assert result.message is None
    
    def test_failed_connection_result(self):
        """Test creating a failed connection test result."""
        result = ConnectionTestResult(
            status="error",
            connected=False,
            message="Connection refused",
            timestamp=1612345678.9
        )
        assert result.status == "error"
        assert result.connected is False
        assert result.message == "Connection refused"
        assert result.timestamp == 1612345678.9
        assert result.version is None


class TestSchemaInfo:
    """Tests for SchemaInfo model."""
    
    def test_schema_info(self):
        """Test creating a SchemaInfo."""
        info = SchemaInfo(schema_name="public", schema_owner="admin")
        assert info.schema_name == "public"
        assert info.schema_owner == "admin"


class TestTableInfo:
    """Tests for TableInfo model."""
    
    def test_table_info(self):
        """Test creating a TableInfo."""
        info = TableInfo(
            table_name="users",
            table_type="TABLE",
            table_schema="public"
        )
        assert info.table_name == "users"
        assert info.table_type == "TABLE"
        assert info.table_schema == "public"