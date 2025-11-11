#!/usr/bin/env python3
"""
ETCD Analyzer Storage Utility ELT Module
Common database operations and utilities for DuckDB ELT patterns
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Union
import duckdb

logger = logging.getLogger(__name__)


class BaseStoreELT:
    """Base class for DuckDB ELT storage operations"""
    
    def __init__(self, db_path: str = "etcd_analyzer.duckdb"):
        self.db_path = db_path
        self.conn = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize DuckDB connection and create tables"""
        if self._initialized:
            return
            
        try:
            self.conn = duckdb.connect(self.db_path)
            await self._create_tables()
            self._initialized = True
            logger.info(f"Initialized DuckDB connection to {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB: {str(e)}")
            raise
    
    async def _create_tables(self):
        """Override this method to create specific tables"""
        pass
    
    def _execute_with_transaction(self, operations: List[tuple], rollback_on_error: bool = True):
        """Execute multiple operations in a transaction
        
        Args:
            operations: List of (sql, params) tuples
            rollback_on_error: Whether to rollback on error
        """
        try:
            self.conn.begin()
            
            for sql, params in operations:
                if params:
                    self.conn.execute(sql, params)
                else:
                    self.conn.execute(sql)
            
            self.conn.commit()
            return True
            
        except Exception as e:
            if rollback_on_error and self.conn:
                self.conn.rollback()
            logger.error(f"Transaction failed: {str(e)}")
            raise
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self._initialized = False
    
    def __del__(self):
        """Cleanup on object destruction"""
        self.close()

class StorageUtilityELT:
    """Utility functions for DuckDB ELT operations"""
    
    @staticmethod
    def parse_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
        """Parse timestamp string to datetime object
        
        Args:
            timestamp_str: ISO format timestamp string
            
        Returns:
            Parsed datetime object or None
        """
        if not timestamp_str:
            return None
            
        try:
            # Handle ISO format with 'Z' suffix
            if timestamp_str.endswith('Z'):
                timestamp_str = timestamp_str.replace('Z', '+00:00')
            
            return datetime.fromisoformat(timestamp_str)
        except Exception as e:
            logger.warning(f"Failed to parse timestamp '{timestamp_str}': {str(e)}")
            return None
    
    @staticmethod
    def generate_uuid() -> str:
        """Generate a UUID string"""
        return str(uuid.uuid4())
    
    @staticmethod
    def current_timestamp() -> datetime:
        """Get current UTC timestamp"""
        return datetime.now(timezone.utc)
    
    @staticmethod
    def serialize_json(data: Dict[str, Any], indent: int = 2) -> str:
        """Serialize data to JSON string with datetime handling
        
        Args:
            data: Data to serialize
            indent: JSON indentation level
            
        Returns:
            JSON string
        """
        return json.dumps(data, indent=indent, default=str)
    
    @staticmethod
    def safe_get(data: Dict[str, Any], key: str, default: Any = None) -> Any:
        """Safely get value from dictionary with default
        
        Args:
            data: Dictionary to get value from
            key: Key to retrieve
            default: Default value if key not found
            
        Returns:
            Value or default
        """
        return data.get(key, default)
    
    @staticmethod
    def validate_testing_id(testing_id: str) -> bool:
        """Validate testing ID format
        
        Args:
            testing_id: Testing ID to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not testing_id or not isinstance(testing_id, str):
            return False
        
        # Check if it's a valid UUID format (basic check)
        try:
            uuid.UUID(testing_id)
            return True
        except ValueError:
            # Allow other string formats but ensure it's not empty
            return len(testing_id.strip()) > 0
    
    @staticmethod
    def create_composite_id(testing_id: str, identifier: str) -> str:
        """Create a composite ID for related records
        
        Args:
            testing_id: Main testing session ID
            identifier: Additional identifier
            
        Returns:
            Composite ID string
        """
        return f"{testing_id}_{identifier}"
    
    @staticmethod
    def extract_dict_values(data: Dict[str, Any], keys: List[str], defaults: List[Any] = None) -> tuple:
        """Extract multiple values from dictionary in order
        
        Args:
            data: Source dictionary
            keys: List of keys to extract
            defaults: List of default values (same length as keys)
            
        Returns:
            Tuple of extracted values
        """
        if defaults is None:
            defaults = [None] * len(keys)
        elif len(defaults) != len(keys):
            raise ValueError("defaults list must be same length as keys list")
        
        return tuple(data.get(key, default) for key, default in zip(keys, defaults))
    
    @staticmethod
    def batch_insert_data(conn: duckdb.DuckDBPyConnection, 
                         table_name: str, 
                         columns: List[str], 
                         data_rows: List[tuple],
                         conflict_resolution: str = "REPLACE") -> None:
        """Batch insert data into table
        
        Args:
            conn: DuckDB connection
            table_name: Target table name
            columns: Column names
            data_rows: List of data tuples
            conflict_resolution: REPLACE, IGNORE, or empty string
        """
        if not data_rows:
            return
        
        placeholders = ", ".join(["?"] * len(columns))
        columns_str = ", ".join(columns)
        
        conflict_clause = ""
        if conflict_resolution.upper() == "REPLACE":
            conflict_clause = "OR REPLACE"
        elif conflict_resolution.upper() == "IGNORE":
            conflict_clause = "OR IGNORE"
        
        sql = f"""
        INSERT {conflict_clause} INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        """
        
        for row_data in data_rows:
            conn.execute(sql, row_data)
    
    @staticmethod
    def get_table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> List[str]:
        """Get column names for a table
        
        Args:
            conn: DuckDB connection
            table_name: Table name
            
        Returns:
            List of column names
        """
        try:
            column_query = f"PRAGMA table_info({table_name})"
            columns_result = conn.execute(column_query).fetchall()
            return [col[1] for col in columns_result]  # col[1] is column name
        except Exception as e:
            logger.warning(f"Could not get columns for table {table_name}: {str(e)}")
            return []
    
    @staticmethod
    def row_to_dict(row: tuple, columns: List[str]) -> Dict[str, Any]:
        """Convert database row to dictionary
        
        Args:
            row: Database row tuple
            columns: Column names
            
        Returns:
            Dictionary representation
        """
        return dict(zip(columns, row))
    
    @staticmethod
    def rows_to_dicts(rows: List[tuple], columns: List[str]) -> List[Dict[str, Any]]:
        """Convert multiple database rows to list of dictionaries
        
        Args:
            rows: List of database row tuples
            columns: Column names
            
        Returns:
            List of dictionary representations
        """
        return [StorageUtilityELT.row_to_dict(row, columns) for row in rows]

class TimeRangeUtilityELT:
    """Utility functions for time range handling in UTC timezone"""
    
    @staticmethod
    def parse_utc_time_range(start_time: str, end_time: str) -> tuple[Optional[datetime], Optional[datetime]]:
        """Parse UTC time range strings to datetime objects
        
        Args:
            start_time: ISO format start time string (UTC)
            end_time: ISO format end time string (UTC)
            
        Returns:
            Tuple of (start_datetime, end_datetime)
        """
        start_dt = StorageUtilityELT.parse_timestamp(start_time)
        end_dt = StorageUtilityELT.parse_timestamp(end_time)
        
        # Ensure timestamps are in UTC
        if start_dt and start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)
        if end_dt and end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
            
        return start_dt, end_dt
    
    @staticmethod
    def validate_time_range(start_time: str, end_time: str) -> Dict[str, Any]:
        """Validate time range parameters
        
        Args:
            start_time: ISO format start time string 
            end_time: ISO format end time string
            
        Returns:
            Validation result dictionary
        """
        result = {
            "valid": False,
            "error": None,
            "start_datetime": None,
            "end_datetime": None
        }
        
        try:
            start_dt, end_dt = TimeRangeUtilityELT.parse_utc_time_range(start_time, end_time)
            
            if not start_dt:
                result["error"] = f"Invalid start_time format: {start_time}"
                return result
            
            if not end_dt:
                result["error"] = f"Invalid end_time format: {end_time}"
                return result
            
            if start_dt >= end_dt:
                result["error"] = "start_time must be before end_time"
                return result
            
            result.update({
                "valid": True,
                "start_datetime": start_dt,
                "end_datetime": end_dt,
                "duration_seconds": (end_dt - start_dt).total_seconds(),
                "duration_hours": (end_dt - start_dt).total_seconds() / 3600
            })
            
        except Exception as e:
            result["error"] = f"Time range validation error: {str(e)}"
        
        return result
    
    @staticmethod
    def format_duration_string(start_time: str, end_time: str) -> Optional[str]:
        """Convert time range to duration string format
        
        Args:
            start_time: ISO format start time string
            end_time: ISO format end time string
            
        Returns:
            Duration string (e.g., "2h30m") or None if invalid
        """
        validation = TimeRangeUtilityELT.validate_time_range(start_time, end_time)
        
        if not validation["valid"]:
            return None
        
        duration_seconds = validation["duration_seconds"]
        hours = int(duration_seconds // 3600)
        minutes = int((duration_seconds % 3600) // 60)
        
        if hours > 0 and minutes > 0:
            return f"{hours}h{minutes}m"
        elif hours > 0:
            return f"{hours}h"
        elif minutes > 0:
            return f"{minutes}m"
        else:
            return "1m"  # Minimum duration


