#!/usr/bin/env python3
"""
ETCD Analyzer Disk I/O Storage ELT Module
Handles storage and retrieval of etcd disk I/O metrics in DuckDB
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Union
import duckdb

from .etcd_analyzer_stor_utility import BaseStoreELT, StorageUtilityELT, TimeRangeUtilityELT

logger = logging.getLogger(__name__)


class DiskIOStorELT(BaseStoreELT):
    """Disk I/O metrics storage using DuckDB ELT patterns"""
    
    def __init__(self, db_path: str = "etcd_analyzer.duckdb"):
        super().__init__(db_path)
        self.table_names = {
            'disk_io_container_disk_writes': 'disk_io_container_writes',
            'disk_io_node_disk_throughput_read': 'disk_io_node_throughput_read', 
            'disk_io_node_disk_throughput_write': 'disk_io_node_throughput_write',
            'disk_io_node_disk_iops_read': 'disk_io_node_iops_read',
            'disk_io_node_disk_iops_write': 'disk_io_node_iops_write',
            'disk_io_node_disk_read_time_seconds': 'disk_io_node_read_time_seconds',
            'disk_io_node_disk_writes_time_seconds': 'disk_io_node_writes_time_seconds',
            'disk_io_node_disk_io_time_seconds': 'disk_io_node_io_time_seconds',
            'disk_io_summary': 'disk_io_summary'
        }
    
    async def _create_tables(self):
        """Create disk I/O metrics tables"""
        table_sqls = [
            # Container disk writes table
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_names['disk_io_container_disk_writes']} (
                id VARCHAR PRIMARY KEY,
                testing_id VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                metric_name VARCHAR NOT NULL,
                node_name VARCHAR NOT NULL,
                avg_value DOUBLE,
                max_value DOUBLE,
                unit VARCHAR,
                devices VARCHAR[],
                status VARCHAR
            )
            """,
            
            # Node disk throughput read table
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_names['disk_io_node_disk_throughput_read']} (
                id VARCHAR PRIMARY KEY,
                testing_id VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                metric_name VARCHAR NOT NULL,
                node_name VARCHAR NOT NULL,
                avg_value DOUBLE,
                max_value DOUBLE,
                unit VARCHAR,
                devices VARCHAR[],
                status VARCHAR
            )
            """,
            
            # Node disk throughput write table
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_names['disk_io_node_disk_throughput_write']} (
                id VARCHAR PRIMARY KEY,
                testing_id VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                metric_name VARCHAR NOT NULL,
                node_name VARCHAR NOT NULL,
                avg_value DOUBLE,
                max_value DOUBLE,
                unit VARCHAR,
                devices VARCHAR[],
                status VARCHAR
            )
            """,
            
            # Node disk IOPS read table
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_names['disk_io_node_disk_iops_read']} (
                id VARCHAR PRIMARY KEY,
                testing_id VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                metric_name VARCHAR NOT NULL,
                node_name VARCHAR NOT NULL,
                avg_value DOUBLE,
                max_value DOUBLE,
                unit VARCHAR,
                devices VARCHAR[],
                status VARCHAR
            )
            """,
            
            # Node disk IOPS write table
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_names['disk_io_node_disk_iops_write']} (
                id VARCHAR PRIMARY KEY,
                testing_id VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                metric_name VARCHAR NOT NULL,
                node_name VARCHAR NOT NULL,
                avg_value DOUBLE,
                max_value DOUBLE,
                unit VARCHAR,
                devices VARCHAR[],
                status VARCHAR
            )
            """,

            # Node disk read time seconds table
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_names['disk_io_node_disk_read_time_seconds']} (
                id VARCHAR PRIMARY KEY,
                testing_id VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                metric_name VARCHAR NOT NULL,
                node_name VARCHAR NOT NULL,
                avg_value DOUBLE,
                max_value DOUBLE,
                unit VARCHAR,
                devices VARCHAR[],
                status VARCHAR
            )
            """,

            # Node disk writes time seconds table
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_names['disk_io_node_disk_writes_time_seconds']} (
                id VARCHAR PRIMARY KEY,
                testing_id VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                metric_name VARCHAR NOT NULL,
                node_name VARCHAR NOT NULL,
                avg_value DOUBLE,
                max_value DOUBLE,
                unit VARCHAR,
                devices VARCHAR[],
                status VARCHAR
            )
            """,

            # Node disk io time seconds table
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_names['disk_io_node_disk_io_time_seconds']} (
                id VARCHAR PRIMARY KEY,
                testing_id VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                metric_name VARCHAR NOT NULL,
                node_name VARCHAR NOT NULL,
                avg_value DOUBLE,
                max_value DOUBLE,
                unit VARCHAR,
                devices VARCHAR[],
                status VARCHAR
            )
            """,
            
            # Summary table
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_names['disk_io_summary']} (
                id VARCHAR PRIMARY KEY,
                testing_id VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                metric_name VARCHAR NOT NULL,
                category VARCHAR NOT NULL,
                unit VARCHAR,
                avg_value DOUBLE,
                max_value DOUBLE,
                data_points INTEGER,
                node_count INTEGER,
                device_count INTEGER
            )
            """
        ]
        
        for sql in table_sqls:
            self.conn.execute(sql)
        
        # Create indexes separately (DuckDB-style)
        index_statements = [
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_container_disk_writes']}_testing_id ON {self.table_names['disk_io_container_disk_writes']}(testing_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_container_disk_writes']}_timestamp ON {self.table_names['disk_io_container_disk_writes']}(timestamp)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_container_disk_writes']}_node_name ON {self.table_names['disk_io_container_disk_writes']}(node_name)",
            
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_throughput_read']}_testing_id ON {self.table_names['disk_io_node_disk_throughput_read']}(testing_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_throughput_read']}_timestamp ON {self.table_names['disk_io_node_disk_throughput_read']}(timestamp)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_throughput_read']}_node_name ON {self.table_names['disk_io_node_disk_throughput_read']}(node_name)",
            
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_throughput_write']}_testing_id ON {self.table_names['disk_io_node_disk_throughput_write']}(testing_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_throughput_write']}_timestamp ON {self.table_names['disk_io_node_disk_throughput_write']}(timestamp)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_throughput_write']}_node_name ON {self.table_names['disk_io_node_disk_throughput_write']}(node_name)",
            
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_iops_read']}_testing_id ON {self.table_names['disk_io_node_disk_iops_read']}(testing_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_iops_read']}_timestamp ON {self.table_names['disk_io_node_disk_iops_read']}(timestamp)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_iops_read']}_node_name ON {self.table_names['disk_io_node_disk_iops_read']}(node_name)",
            
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_iops_write']}_testing_id ON {self.table_names['disk_io_node_disk_iops_write']}(testing_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_iops_write']}_timestamp ON {self.table_names['disk_io_node_disk_iops_write']}(timestamp)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_iops_write']}_node_name ON {self.table_names['disk_io_node_disk_iops_write']}(node_name)",

            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_read_time_seconds']}_testing_id ON {self.table_names['disk_io_node_disk_read_time_seconds']}(testing_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_read_time_seconds']}_timestamp ON {self.table_names['disk_io_node_disk_read_time_seconds']}(timestamp)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_read_time_seconds']}_node_name ON {self.table_names['disk_io_node_disk_read_time_seconds']}(node_name)",

            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_writes_time_seconds']}_testing_id ON {self.table_names['disk_io_node_disk_writes_time_seconds']}(testing_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_writes_time_seconds']}_timestamp ON {self.table_names['disk_io_node_disk_writes_time_seconds']}(timestamp)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_writes_time_seconds']}_node_name ON {self.table_names['disk_io_node_disk_writes_time_seconds']}(node_name)",

            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_io_time_seconds']}_testing_id ON {self.table_names['disk_io_node_disk_io_time_seconds']}(testing_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_io_time_seconds']}_timestamp ON {self.table_names['disk_io_node_disk_io_time_seconds']}(timestamp)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_node_disk_io_time_seconds']}_node_name ON {self.table_names['disk_io_node_disk_io_time_seconds']}(node_name)",
            
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_summary']}_testing_id ON {self.table_names['disk_io_summary']}(testing_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_summary']}_timestamp ON {self.table_names['disk_io_summary']}(timestamp)",
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['disk_io_summary']}_metric_name ON {self.table_names['disk_io_summary']}(metric_name)",
        ]
        
        for idx_sql in index_statements:
            self.conn.execute(idx_sql)
    
    async def store_disk_io_metrics(self, testing_id: str, disk_io_data: Dict[str, Any]) -> Dict[str, Any]:
        """Store disk I/O metrics data in DuckDB"""
        if not self._initialized:
            await self.initialize()
        
        if not StorageUtilityELT.validate_testing_id(testing_id):
            return {
                "status": "error",
                "error": "Invalid testing_id format"
            }
        
        try:
            # Extract metrics from the nested structure
            if disk_io_data.get("status") != "success":
                return {
                    "status": "error", 
                    "error": f"Disk I/O data collection failed: {disk_io_data.get('error')}"
                }
            
            # Get the data section
            data_section = disk_io_data.get("data", {})
            if not data_section or data_section.get("status") != "success":
                return {
                    "status": "error",
                    "error": "No valid disk I/O metrics data found"
                }
            
            metrics = data_section.get("metrics", {})
            if not metrics:
                return {
                    "status": "error",
                    "error": "No disk I/O metrics found in data"
                }
            
            storage_results = {}
            
            # Store each metric type
            for metric_name, metric_data in metrics.items():
                if metric_data.get("status") != "success":
                    storage_results[metric_name] = f"skipped: {metric_data.get('error', 'no data')}"
                    continue
                
                try:
                    result = await self._store_metric_data(testing_id, metric_name, metric_data, data_section)
                    storage_results[metric_name] = result
                except Exception as e:
                    logger.error(f"Error storing {metric_name}: {str(e)}")
                    storage_results[metric_name] = f"error: {str(e)}"
            
            # Generate and store summary
            try:
                summary_result = await self._generate_and_store_summary(testing_id, metrics, data_section)
                storage_results["summary"] = summary_result
            except Exception as e:
                logger.error(f"Error generating summary: {str(e)}")
                storage_results["summary"] = f"error: {str(e)}"
            
            return {
                "status": "success",
                "testing_id": testing_id,
                "storage_results": storage_results,
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error storing disk I/O metrics: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _store_metric_data(self, testing_id: str, metric_name: str, metric_data: Dict[str, Any], data_section: Dict[str, Any]) -> str:
        """Store individual metric data"""
        table_name = self.table_names.get(metric_name)
        if not table_name:
            return f"error: Unknown metric {metric_name}"
        
        timestamp = StorageUtilityELT.parse_timestamp(data_section.get("timestamp")) or StorageUtilityELT.current_timestamp()
        nodes = metric_data.get("nodes", {})
        unit = metric_data.get("unit", "")
        
        if not nodes:
            return "error: No node data found"
        
        # Prepare batch insert data
        rows = []
        for node_name, node_data in nodes.items():
            if node_name == "unknown" and node_data.get("avg", 0) == 0:
                continue  # Skip unknown nodes with zero values
            
            row_id = StorageUtilityELT.generate_uuid()
            devices = node_data.get("devices", [])
            
            row_data = (
                row_id,
                testing_id,
                timestamp,
                metric_name,
                node_name,
                node_data.get("avg"),
                node_data.get("max"),
                unit,
                devices,
                "success"
            )
            rows.append(row_data)
        
        if not rows:
            return "error: No valid node data to store"
        
        # Batch insert
        columns = ["id", "testing_id", "timestamp", "metric_name", "node_name", 
                  "avg_value", "max_value", "unit", "devices", "status"]
        
        StorageUtilityELT.batch_insert_data(
            self.conn, table_name, columns, rows, "REPLACE"
        )
        
        return f"success: {len(rows)} records stored"
    
    async def _generate_and_store_summary(self, testing_id: str, metrics: Dict[str, Any], data_section: Dict[str, Any]) -> str:
        """Generate and store disk I/O summary"""
        timestamp = StorageUtilityELT.parse_timestamp(data_section.get("timestamp")) or StorageUtilityELT.current_timestamp()
        
        rows = []
        for metric_name, metric_data in metrics.items():
            if metric_data.get("status") != "success":
                continue
            
            overall_stats = metric_data.get("overall_stats", {})
            nodes = metric_data.get("nodes", {})
            
            # Count non-unknown nodes and devices
            valid_nodes = [node for node in nodes.keys() if node != "unknown"]
            device_count = 0
            for node_data in nodes.values():
                devices = node_data.get("devices", [])
                if devices:
                    device_count += len(devices)
            
            # Determine category
            if "container" in metric_name:
                category = "Container I/O"
            elif "throughput" in metric_name:
                category = "Disk Throughput" 
            elif "iops" in metric_name:
                category = "Disk IOPS"
            else:
                category = "Disk I/O"
            
            row_id = StorageUtilityELT.generate_uuid()
            row_data = (
                row_id,
                testing_id,
                timestamp,
                metric_name,
                category,
                metric_data.get("unit", ""),
                overall_stats.get("avg"),
                overall_stats.get("max"),
                overall_stats.get("count", 0),
                len(valid_nodes),
                device_count
            )
            rows.append(row_data)
        
        if not rows:
            return "error: No summary data to store"
        
        columns = ["id", "testing_id", "timestamp", "metric_name", "category", "unit",
                  "avg_value", "max_value", "data_points", "node_count", "device_count"]
        
        StorageUtilityELT.batch_insert_data(
            self.conn, self.table_names['disk_io_summary'], columns, rows, "REPLACE"
        )
        
        return f"success: {len(rows)} summary records stored"
    
    async def get_disk_io_summary(self, testing_id: str) -> Dict[str, Any]:
        """Get disk I/O summary for a testing session"""
        if not self._initialized:
            await self.initialize()
        
        try:
            # Get summary data
            summary_query = f"""
            SELECT metric_name, category, unit, avg_value, max_value,
                   data_points, node_count, device_count
            FROM {self.table_names['disk_io_summary']}
            WHERE testing_id = ?
            ORDER BY metric_name
            """
            
            summary_results = self.conn.execute(summary_query, [testing_id]).fetchall()
            summary_columns = ["metric_name", "category", "unit", "avg_value", "max_value",
                             "data_points", "node_count", "device_count"]
            
            summary_data = StorageUtilityELT.rows_to_dicts(summary_results, summary_columns)
            
            # Get detailed node data for each metric
            detailed_data = {}
            for metric_name in [row["metric_name"] for row in summary_data]:
                table_name = self.table_names.get(metric_name)
                if table_name:
                    node_query = f"""
                    SELECT node_name, avg_value, max_value, unit, devices
                    FROM {table_name}
                    WHERE testing_id = ?
                    ORDER BY node_name
                    """
                    
                    node_results = self.conn.execute(node_query, [testing_id]).fetchall()
                    node_columns = ["node_name", "avg_value", "max_value", "unit", "devices"]
                    detailed_data[metric_name] = StorageUtilityELT.rows_to_dicts(node_results, node_columns)
            
            return {
                "status": "success",
                "testing_id": testing_id,
                "summary": summary_data,
                "detailed_data": detailed_data,
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting disk I/O summary: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def query_disk_io_data_by_duration(self, duration: str) -> Dict[str, Any]:
        """Query disk I/O data by duration from current time"""
        if not self._initialized:
            await self.initialize()
        
        try:
            # Calculate time range based on duration
            current_time = StorageUtilityELT.current_timestamp()
            
            # Convert duration to hours for SQL interval
            duration_hours = self._parse_duration_to_hours(duration)
            
            query = f"""
            SELECT DISTINCT testing_id, timestamp
            FROM {self.table_names['disk_io_summary']}
            WHERE timestamp >= (CURRENT_TIMESTAMP - INTERVAL '{duration_hours} hours')
            ORDER BY timestamp DESC
            """
            
            results = self.conn.execute(query).fetchall()
            
            if not results:
                return {
                    "status": "success",
                    "message": f"No disk I/O data found for duration: {duration}",
                    "data": []
                }
            
            # Get summary data for each testing session
            sessions_data = []
            for testing_id, timestamp in results:
                session_data = await self.get_disk_io_summary(testing_id)
                if session_data.get("status") == "success":
                    sessions_data.append(session_data)
            
            return {
                "status": "success",
                "duration": duration,
                "sessions_found": len(sessions_data),
                "data": sessions_data,
                "query_timestamp": current_time.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error querying disk I/O data by duration: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def query_disk_io_data_by_time_range(self, start_time: str, end_time: str) -> Dict[str, Any]:
        """Query disk I/O data by time range"""
        if not self._initialized:
            await self.initialize()
        
        # Validate time range
        validation = TimeRangeUtilityELT.validate_time_range(start_time, end_time)
        if not validation["valid"]:
            return {
                "status": "error",
                "error": validation["error"]
            }
        
        try:
            start_dt, end_dt = TimeRangeUtilityELT.parse_utc_time_range(start_time, end_time)
            
            query = f"""
            SELECT DISTINCT testing_id, timestamp
            FROM {self.table_names['disk_io_summary']}
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp DESC
            """
            
            results = self.conn.execute(query, [start_dt, end_dt]).fetchall()
            
            if not results:
                return {
                    "status": "success",
                    "message": f"No disk I/O data found for time range: {start_time} to {end_time}",
                    "data": []
                }
            
            # Get summary data for each testing session
            sessions_data = []
            for testing_id, timestamp in results:
                session_data = await self.get_disk_io_summary(testing_id)
                if session_data.get("status") == "success":
                    sessions_data.append(session_data)
            
            return {
                "status": "success",
                "start_time": start_time,
                "end_time": end_time,
                "sessions_found": len(sessions_data),
                "data": sessions_data,
                "query_timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error querying disk I/O data by time range: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _parse_duration_to_hours(self, duration: str) -> float:
        """Parse duration string to hours"""
        duration = duration.lower().strip()
        
        if duration.endswith('h'):
            return float(duration[:-1])
        elif duration.endswith('m'):
            return float(duration[:-1]) / 60
        elif duration.endswith('d'):
            return float(duration[:-1]) * 24
        else:
            # Default to 1 hour if format not recognized
            return 1.0
    
    async def get_table_info(self) -> Dict[str, Any]:
        """Get information about disk I/O tables"""
        if not self._initialized:
            await self.initialize()
        
        table_info = {}
        for logical_name, table_name in self.table_names.items():
            try:
                columns = StorageUtilityELT.get_table_columns(self.conn, table_name)
                table_info[logical_name] = {
                    "table_name": table_name,
                    "columns": columns
                }
            except Exception as e:
                table_info[logical_name] = {"error": str(e)}
        
        return table_info


def print_disk_io_summary_tables(summary_data: Dict[str, Any]):
    """Print disk I/O summary in tabular format to terminal"""
    if summary_data.get("status") != "success":
        print(f"Error getting disk I/O summary: {summary_data.get('error')}")
        return
    
    print("\n" + "="*120)
    print("DISK I/O PERFORMANCE ANALYSIS SUMMARY")
    print("="*120)
    
    # Comprehensive Metrics Overview Table
    print("\nComprehensive Metrics Overview")
    header = f"{'Metric Name':<40} {'Category':<15} {'Unit':<8} {'Avg':<10} {'Max':<10} {'Data Points':<12}"
    print("-" * 120)
    print(header)
    print("-" * 120)
    
    for metric in summary_data.get("summary", []):
        metric_name = metric.get("metric_name", "")
        category = metric.get("category", "")
        unit = metric.get("unit", "")
        avg_val = metric.get("avg_value", 0)
        max_val = metric.get("max_value", 0)
        data_points = metric.get("data_points", 0)
        
        # Format values based on unit
        if "bytes" in unit.lower():
            if avg_val >= 1024*1024:
                avg_str = f"{avg_val/(1024*1024):.1f} MB/s"
                max_str = f"{max_val/(1024*1024):.1f} MB/s"
            elif avg_val >= 1024:
                avg_str = f"{avg_val/1024:.1f} KB/s"
                max_str = f"{max_val/1024:.1f} KB/s" 
            else:
                avg_str = f"{avg_val:.1f} B/s"
                max_str = f"{max_val:.1f} B/s"
        elif "operations" in unit.lower():
            avg_str = f"{avg_val:.3f} IOPS"
            max_str = f"{max_val:.1f} IOPS"
        else:
            avg_str = f"{avg_val:.1f}"
            max_str = f"{max_val:.1f}"
        
        print(f"{metric_name:<40} {category:<15} {unit:<8} {avg_str:<10} {max_str:<10} {data_points:<12}")
    
    # Detailed Node Performance Table  
    print(f"\nDetailed Node Performance")
    node_header = f"{'Metric':<40} {'Node':<35} {'Category':<15} {'Avg Value':<12} {'Max Value':<12} {'Avg (Numeric)':<15} {'Max (Numeric)':<15} {'Unit':<8} {'Devices'}"
    print("-" * 160)
    print(node_header)
    print("-" * 160)
    
    detailed_data = summary_data.get("detailed_data", {})
    for metric_name in sorted(detailed_data.keys()):
        metric_nodes = detailed_data[metric_name]
        
        # Get category from summary
        category = ""
        for summary_metric in summary_data.get("summary", []):
            if summary_metric.get("metric_name") == metric_name:
                category = summary_metric.get("category", "")
                break
        
        for node_data in metric_nodes:
            node_name = node_data.get("node_name", "")
            avg_val = node_data.get("avg_value", 0)
            max_val = node_data.get("max_value", 0)
            unit = node_data.get("unit", "")
            devices = node_data.get("devices", [])
            device_str = ", ".join(devices) if devices else "All"
            
            # Format display values
            if "bytes" in unit.lower():
                if avg_val >= 1024*1024:
                    avg_display = f"{avg_val/(1024*1024):.1f} MB/s"
                    max_display = f"{max_val/(1024*1024):.1f} MB/s"
                elif avg_val >= 1024:
                    avg_display = f"{avg_val/1024:.1f} KB/s"
                    max_display = f"{max_val/1024:.1f} KB/s"
                else:
                    avg_display = f"{avg_val:.1f} B/s"
                    max_display = f"{max_val:.1f} B/s"
            elif "operations" in unit.lower():
                avg_display = f"{avg_val:.3f} IOPS"
                max_display = f"{max_val:.1f} IOPS"
            else:
                avg_display = f"{avg_val:.1f}"
                max_display = f"{max_val:.1f}"
            
            print(f"{metric_name:<40} {node_name:<35} {category:<15} {avg_display:<12} {max_display:<12} {avg_val:<15.6e} {max_val:<15.6e} {unit:<8} {device_str}")
    
    print("-" * 160)
    print(f"Testing ID: {summary_data.get('testing_id', 'N/A')}")
    print(f"Generated: {summary_data.get('timestamp', 'N/A')}")
    print("="*120)