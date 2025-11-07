#!/usr/bin/env python3
"""
ETCD Analyzer General Info Storage ELT Module
Handles storage and retrieval of general etcd metrics in DuckDB
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
import re
from tabulate import tabulate

from .etcd_analyzer_stor_utility import BaseStoreELT, StorageUtilityELT, TimeRangeUtilityELT

logger = logging.getLogger(__name__)


class GeneralInfoStorELT(BaseStoreELT):
    """Storage class for ETCD general info metrics using DuckDB ELT pattern"""
    
    def __init__(self, db_path: str = "etcd_analyzer.duckdb"):
        super().__init__(db_path)
        self.schema_name = "pods_metrics"
        
        # Define metrics that should be tracked
        self.tracked_metrics = [
            'cpu_usage', 'memory_usage', 'proposal_failure_rate', 'proposal_pending_total',
            'proposal_commit_rate', 'proposal_apply_rate', 'total_proposals_committed',
            'leader_changes_rate', 'slow_applies', 'slow_read_indexes', 'put_operations_rate',
            'delete_operations_rate', 'heartbeat_send_failures', 'health_failures',
            'vmstat_pgmajfault_total', 'vmstat_pgmajfault_rate'
        ]
    
    async def _create_tables(self):
        """Create schema and tables for general info metrics"""
        if not self.conn:
            await self.initialize()
        
        try:
            # Create schema
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
            
            # Create individual metric tables
            for metric_name in self.tracked_metrics:
                table_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.{metric_name} (
                    id VARCHAR PRIMARY KEY,
                    testing_id VARCHAR NOT NULL,
                    pod_name VARCHAR NOT NULL,
                    node_name VARCHAR,
                    metric_name VARCHAR NOT NULL,
                    metric_title VARCHAR,
                    unit VARCHAR,
                    avg_value DOUBLE,
                    max_value DOUBLE,
                    min_value DOUBLE,
                    data_count INTEGER,
                    query_string TEXT,
                    collection_timestamp TIMESTAMP,
                    duration VARCHAR,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                self.conn.execute(table_sql)
            
            # Create summary table
            summary_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.schema_name}.summary_general_info (
                id VARCHAR PRIMARY KEY,
                testing_id VARCHAR NOT NULL,
                metric_name VARCHAR NOT NULL,
                unit VARCHAR,
                cluster_avg DOUBLE,
                cluster_max DOUBLE,
                pods_count INTEGER,
                collection_timestamp TIMESTAMP,
                duration VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            self.conn.execute(summary_sql)
            
            # Create indexes for better query performance
            for metric_name in self.tracked_metrics:
                try:
                    self.conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{metric_name}_testing_id 
                    ON {self.schema_name}.{metric_name}(testing_id)
                    """)
                    self.conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{metric_name}_timestamp 
                    ON {self.schema_name}.{metric_name}(collection_timestamp)
                    """)
                except Exception as e:
                    logger.warning(f"Could not create index for {metric_name}: {str(e)}")
            
            # Create summary table indexes
            self.conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_summary_testing_id 
            ON {self.schema_name}.summary_general_info(testing_id)
            """)
            
            logger.info(f"Created tables and indexes for general info metrics in schema {self.schema_name}")
            
        except Exception as e:
            logger.error(f"Failed to create general info tables: {str(e)}")
            raise
    
    async def store_general_info_metrics(self, testing_id: str, general_info_data: Dict[str, Any]) -> Dict[str, Any]:
        """Store general info metrics data in DuckDB
        
        Args:
            testing_id: Unique testing session identifier
            general_info_data: General info data from MCP tool
            
        Returns:
            Storage result dictionary
        """
        if not self.conn:
            await self.initialize()
        
        try:
            if not StorageUtilityELT.validate_testing_id(testing_id):
                raise ValueError(f"Invalid testing_id: {testing_id}")
            
            # Extract data from general_info_data
            if general_info_data.get("status") != "success":
                raise ValueError(f"General info data status is not success: {general_info_data.get('status')}")
            
            data = general_info_data.get("data", {})
            pod_metrics = data.get("pod_metrics", {})
            
            if not pod_metrics:
                raise ValueError("No pod_metrics found in general info data")
            
            # Extract metadata
            timestamp_str = data.get("timestamp")
            duration = data.get("duration", "1h")
            
            collection_timestamp = StorageUtilityELT.parse_timestamp(timestamp_str)
            if not collection_timestamp:
                collection_timestamp = StorageUtilityELT.current_timestamp()
            
            storage_results = {}
            summary_data = {}
            
            # Process all metrics present (robust to config/name changes)
            for metric_name, metric_data in pod_metrics.items():
                metric_results = await self._store_metric_data(
                    testing_id, metric_name, metric_data, 
                    collection_timestamp, duration
                )
                storage_results[metric_name] = metric_results
                
                # Collect summary data
                if metric_results.get("status") == "success":
                    summary_data[metric_name] = metric_results.get("summary", {})
            
            # Store summary data
            summary_result = await self._store_summary_data(
                testing_id, summary_data, collection_timestamp, duration
            )
            storage_results["summary"] = summary_result
            
            # Generate and print summary tables
            summary_report = await self._generate_summary_report(testing_id)
            
            return {
                "status": "success",
                "testing_id": testing_id,
                "storage_results": storage_results,
                "summary_report": summary_report,
                "metrics_stored": len([k for k, v in storage_results.items() 
                                     if k != "summary" and v.get("status") == "success"]),
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to store general info metrics: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "testing_id": testing_id,
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
    
    async def _store_metric_data(self, testing_id: str, metric_name: str, 
                               metric_data: Dict[str, Any], collection_timestamp: datetime,
                               duration: str) -> Dict[str, Any]:
        """Store individual metric data"""
        try:
            table_name = f"{self.schema_name}.{metric_name}"
            # Ensure table exists for dynamic metrics
            try:
                table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id VARCHAR PRIMARY KEY,
                    testing_id VARCHAR NOT NULL,
                    pod_name VARCHAR NOT NULL,
                    node_name VARCHAR,
                    metric_name VARCHAR NOT NULL,
                    metric_title VARCHAR,
                    unit VARCHAR,
                    avg_value DOUBLE,
                    max_value DOUBLE,
                    min_value DOUBLE,
                    data_count INTEGER,
                    query_string TEXT,
                    collection_timestamp TIMESTAMP,
                    duration VARCHAR,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                self.conn.execute(table_sql)
                # Basic indexes
                self.conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{metric_name}_testing_id ON {table_name}(testing_id)")
                self.conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{metric_name}_timestamp ON {table_name}(collection_timestamp)")
            except Exception:
                pass
            pods_data = metric_data.get("pods", {})
            
            if not pods_data:
                return {"status": "warning", "message": f"No pods data for metric {metric_name}"}
            
            # Calculate cluster-level statistics
            all_avg_values = [pod_data.get("avg", 0) for pod_data in pods_data.values() if pod_data.get("avg") is not None]
            all_max_values = [pod_data.get("max", 0) for pod_data in pods_data.values() if pod_data.get("max") is not None]
            
            cluster_avg = sum(all_avg_values) / len(all_avg_values) if all_avg_values else 0
            cluster_max = max(all_max_values) if all_max_values else 0
            
            # Prepare data for batch insert
            insert_rows = []
            for pod_name, pod_data in pods_data.items():
                row_id = StorageUtilityELT.generate_uuid()
                
                insert_rows.append((
                    row_id,
                    testing_id,
                    pod_name,
                    pod_data.get("node"),
                    metric_name,
                    metric_data.get("title"),
                    metric_data.get("unit"),
                    pod_data.get("avg"),
                    pod_data.get("max"),
                    pod_data.get("min"),
                    pod_data.get("count"),
                    metric_data.get("query"),
                    collection_timestamp,
                    duration,
                    None,  # start_time
                    None,  # end_time
                ))
            
            # Batch insert
            columns = [
                "id", "testing_id", "pod_name", "node_name", "metric_name", 
                "metric_title", "unit", "avg_value", "max_value", "min_value",
                "data_count", "query_string", "collection_timestamp", "duration",
                "start_time", "end_time"
            ]
            
            StorageUtilityELT.batch_insert_data(
                self.conn, table_name, columns, insert_rows, "REPLACE"
            )
            
            return {
                "status": "success",
                "records_inserted": len(insert_rows),
                "summary": {
                    "metric_name": metric_name,
                    "unit": metric_data.get("unit"),
                    "cluster_avg": cluster_avg,
                    "cluster_max": cluster_max,
                    "pods_count": len(pods_data)
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to store metric {metric_name}: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _store_summary_data(self, testing_id: str, summary_data: Dict[str, Dict[str, Any]],
                                collection_timestamp: datetime, duration: str) -> Dict[str, Any]:
        """Store summary data"""
        try:
            table_name = f"{self.schema_name}.summary_general_info"
            
            insert_rows = []
            for metric_name, summary in summary_data.items():
                row_id = StorageUtilityELT.generate_uuid()
                
                insert_rows.append((
                    row_id,
                    testing_id,
                    metric_name,
                    summary.get("unit"),
                    summary.get("cluster_avg"),
                    summary.get("cluster_max"),
                    summary.get("pods_count"),
                    collection_timestamp,
                    duration,
                    None,  # start_time
                    None,  # end_time
                ))
            
            columns = [
                "id", "testing_id", "metric_name", "unit", "cluster_avg",
                "cluster_max", "pods_count", "collection_timestamp", 
                "duration", "start_time", "end_time"
            ]
            
            StorageUtilityELT.batch_insert_data(
                self.conn, table_name, columns, insert_rows, "REPLACE"
            )
            
            return {
                "status": "success",
                "records_inserted": len(insert_rows)
            }
            
        except Exception as e:
            logger.error(f"Failed to store summary data: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _generate_summary_report(self, testing_id: str) -> Dict[str, Any]:
        """Generate summary report for the testing session"""
        try:
            query = f"""
            SELECT 
                metric_name,
                unit,
                cluster_avg,
                cluster_max,
                pods_count
            FROM {self.schema_name}.summary_general_info
            WHERE testing_id = ?
            ORDER BY metric_name
            """
            
            results = self.conn.execute(query, [testing_id]).fetchall()
            
            summary_report = {
                "testing_id": testing_id,
                "metrics": []
            }
            
            for row in results:
                summary_report["metrics"].append({
                    "metric_name": row[0],
                    "unit": row[1],
                    "cluster_avg": row[2],
                    "cluster_max": row[3],
                    "pods_count": row[4]
                })
            
            return summary_report
            
        except Exception as e:
            logger.error(f"Failed to generate summary report: {str(e)}")
            return {"error": str(e)}
    
    async def get_general_info_summary(self, testing_id: str) -> Dict[str, Any]:
        """Get general info summary for a testing session"""
        try:
            if not self.conn:
                await self.initialize()
            
            summary_report = await self._generate_summary_report(testing_id)
            
            return {
                "status": "success",
                "summary_report": summary_report,
                "testing_id": testing_id,
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get general info summary: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "testing_id": testing_id,
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
    
    async def query_general_info_data_by_duration(self, duration: str) -> Dict[str, Any]:
        """Query general info data by duration"""
        try:
            if not self.conn:
                await self.initialize()
            
            # Convert duration like '2h', '30m', '1d' to DuckDB INTERVAL expression
            interval_expr = self._to_duckdb_interval(duration)
            # Query summary data using DuckDB time arithmetic
            query = f"""
            SELECT DISTINCT testing_id, collection_timestamp, duration
            FROM {self.schema_name}.summary_general_info
            WHERE collection_timestamp >= (now() - {interval_expr})
            ORDER BY collection_timestamp DESC
            """
            
            sessions = self.conn.execute(query).fetchall()
            
            results = []
            for session in sessions:
                testing_id = session[0]
                summary_data = await self._generate_summary_report(testing_id)
                results.append({
                    "testing_id": testing_id,
                    "collection_timestamp": session[1],
                    "duration": session[2],
                    "summary": summary_data
                })
            
            return {
                "status": "success",
                "query_type": "duration",
                "duration": duration,
                "sessions_found": len(results),
                "results": results,
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to query by duration: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "query_type": "duration",
                "duration": duration,
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }

    def _to_duckdb_interval(self, duration: str) -> str:
        """Convert simple duration strings (e.g., '2h', '30m', '1d', '45s') to DuckDB INTERVAL expression.
        Falls back to minutes when unrecognized.
        """
        try:
            if not duration:
                return "INTERVAL '60 MINUTES'"
            duration = duration.strip().lower()
            m = re.match(r"^(\d+)\s*([smhd])$", duration)
            if not m:
                # Try to handle like '2h30m' by taking the leading number and unit
                m2 = re.match(r"^(\d+)", duration)
                if m2:
                    n = int(m2.group(1))
                    return f"INTERVAL '{n} MINUTES'"
                return "INTERVAL '60 MINUTES'"
            n = int(m.group(1))
            unit = m.group(2)
            unit_map = {
                's': 'SECONDS',
                'm': 'MINUTES',
                'h': 'HOURS',
                'd': 'DAYS'
            }
            duck_unit = unit_map.get(unit, 'MINUTES')
            return f"INTERVAL '{n} {duck_unit}'"
        except Exception:
            return "INTERVAL '60 MINUTES'"
    
    async def query_general_info_data_by_time_range(self, start_time: str, end_time: str) -> Dict[str, Any]:
        """Query general info data by time range (UTC)"""
        try:
            if not self.conn:
                await self.initialize()
            
            # Validate time range
            validation = TimeRangeUtilityELT.validate_time_range(start_time, end_time)
            if not validation["valid"]:
                return {
                    "status": "error",
                    "error": validation["error"],
                    "query_type": "time_range",
                    "timestamp": StorageUtilityELT.current_timestamp().isoformat()
                }
            
            start_dt = validation["start_datetime"]
            end_dt = validation["end_datetime"]
            
            # Query summary data
            query = f"""
            SELECT DISTINCT testing_id, collection_timestamp, duration
            FROM {self.schema_name}.summary_general_info
            WHERE collection_timestamp BETWEEN ? AND ?
            ORDER BY collection_timestamp DESC
            """
            
            sessions = self.conn.execute(query, [start_dt, end_dt]).fetchall()
            
            results = []
            for session in sessions:
                testing_id = session[0]
                summary_data = await self._generate_summary_report(testing_id)
                results.append({
                    "testing_id": testing_id,
                    "collection_timestamp": session[1],
                    "duration": session[2],
                    "summary": summary_data
                })
            
            return {
                "status": "success",
                "query_type": "time_range",
                "start_time": start_time,
                "end_time": end_time,
                "sessions_found": len(results),
                "results": results,
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to query by time range: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "query_type": "time_range",
                "start_time": start_time,
                "end_time": end_time,
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
    
    async def get_table_info(self) -> Dict[str, List[str]]:
        """Get table information for all general info tables"""
        try:
            if not self.conn:
                await self.initialize()
            
            table_info = {}
            
            # Get info for each metric table
            for metric_name in self.tracked_metrics:
                table_name = f"{self.schema_name}.{metric_name}"
                columns = StorageUtilityELT.get_table_columns(self.conn, table_name)
                if columns:
                    table_info[table_name] = columns
            
            # Get summary table info
            summary_table = f"{self.schema_name}.summary_general_info"
            summary_columns = StorageUtilityELT.get_table_columns(self.conn, summary_table)
            if summary_columns:
                table_info[summary_table] = summary_columns
            
            return table_info
            
        except Exception as e:
            logger.error(f"Failed to get table info: {str(e)}")
            return {}


def print_general_info_summary_tables(summary_report: Dict[str, Any]):
    """Print general info summary in a readable table format"""
    try:
        if summary_report.get("error"):
            print(f"Error in summary report: {summary_report['error']}")
            return
        
        metrics = summary_report.get("metrics", [])
        if not metrics:
            print("No metrics data found in summary report")
            return
        
        print("\n" + "="*80)
        print("**ETCD GENERAL INFO METRICS OVERVIEW**")
        print("="*80)
        
        # Prepare data for tabulation
        table_data = []
        for metric in metrics:
            metric_name = metric.get("metric_name", "").replace("_", " ").title()
            unit = metric.get("unit", "")
            cluster_avg = metric.get("cluster_avg", 0)
            cluster_max = metric.get("cluster_max", 0)
            pods_count = metric.get("pods_count", 0)
            
            # Format values based on unit
            if unit == "MB":
                avg_formatted = f"{cluster_avg:.1f} MB"
                max_formatted = f"{cluster_max:.1f} MB"
                if cluster_avg > 300:  # Warning threshold for memory
                    avg_formatted = f"⚠ {avg_formatted}"
                    max_formatted = f"⚠ {max_formatted}"
            elif unit == "percent":
                avg_formatted = f"{cluster_avg:.2f}%"
                max_formatted = f"{cluster_max:.2f}%"
            elif unit == "per_second":
                avg_formatted = f"{cluster_avg:.2f}/s"
                max_formatted = f"{cluster_max:.2f}/s"
            elif unit == "count":
                if cluster_avg > 1000:
                    avg_formatted = f"{cluster_avg/1000:.1f}K"
                    max_formatted = f"{cluster_max/1000:.1f}K"
                else:
                    avg_formatted = f"{cluster_avg:.0f}"
                    max_formatted = f"{cluster_max:.0f}"
            elif unit == "boolean":
                avg_formatted = "Yes" if cluster_avg == 1 else "No"
                max_formatted = "Yes" if cluster_max == 1 else "No"
            elif unit == "per_day":
                avg_formatted = f"{cluster_avg:.2f}/day"
                max_formatted = f"{cluster_max:.2f}/day"
            else:
                avg_formatted = f"{cluster_avg:.2f}"
                max_formatted = f"{cluster_max:.2f}"
            
            table_data.append([
                metric_name,
                unit,
                avg_formatted,
                max_formatted,
                pods_count
            ])
        
        # Print the table
        headers = ["Metric", "Unit", "Cluster Avg", "Cluster Max", "Pods Count"]
        print(tabulate(table_data, headers=headers, tablefmt="grid", stralign="left"))
        print("="*80)
        print(f"Testing ID: {summary_report.get('testing_id', 'N/A')}")
        print("="*80 + "\n")
        
    except Exception as e:
        logger.error(f"Failed to print summary tables: {str(e)}")
        print(f"Error printing summary tables: {str(e)}")