#!/usr/bin/env python3
"""Data models for Redshift MCP Server."""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from pydantic import BaseModel


@dataclass
class RedshiftConnectionConfig:
    """Configuration for connecting to a Redshift database."""
    host: str
    port: int
    database: str
    user: str
    password: str


class QueryResult(BaseModel):
    """Represents the result of a SQL query execution."""
    rows: List[Dict[str, Any]]
    column_names: Optional[List[str]] = None
    affected_rows: Optional[int] = None
    execution_time: Optional[float] = None


class ConnectionTestResult(BaseModel):
    """Result of a Redshift connection test."""
    status: str
    connected: bool
    version: Optional[str] = None
    message: Optional[str] = None
    timestamp: Optional[float] = None


class SchemaInfo(BaseModel):
    """Information about a database schema."""
    schema_name: str
    schema_owner: str


class TableInfo(BaseModel):
    """Information about a database table."""
    table_name: str
    table_type: str
    table_schema: str