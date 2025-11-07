#!/usr/bin/env python3
"""
ETCD Analyzer Backend Commit Storage Module
Handles storage and retrieval of etcd backend commit metrics data in DuckDB
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
import duckdb
from tabulate import tabulate

from .etcd_analyzer_stor_utility import BaseStoreELT, StorageUtilityELT, TimeRangeUtilityELT

logger = logging.getLogger(__name__)


class BackendCommitStorELT(BaseStoreELT):
    """Storage class for etcd backend commit metrics"""
    
    def __init__(self, db_path: str = "etcd_analyzer.duckdb"):
        super().__init__(db_path)
        self.table_names = {
            "p99_duration": "disk_backend_commit_duration_seconds_p99",
            "sum_rate": "disk_backend_commit_duration_sum_rate", 
            "sum": "disk_backend_commit_duration_sum",
            "count_rate": "disk_backend_commit_duration_count_rate",
            "count": "disk_backend_commit_duration_count",
            "summary": "disk_backend_commit_summary"
        }
    
    async def _create_tables(self):
        """Create tables for backend commit metrics storage"""
        try:
            # Create individual metric tables
            for metric_key, table_name in self.table_names.items():
                if metric_key != "summary":
                    await self._create_metric_table(table_name)
            
            # Create summary table
            await self._create_summary_table()
            
            logger.info("Backend commit tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating backend commit tables: {str(e)}")
            raise
    
    async def _create_metric_table(self, table_name: str):
        """Create table for individual metrics"""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id VARCHAR PRIMARY KEY,
            testing_id VARCHAR NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            duration VARCHAR,
            metric_name VARCHAR NOT NULL,
            pod_name VARCHAR NOT NULL,
            avg_value DOUBLE,
            max_value DOUBLE,
            count_value INTEGER,
            unit VARCHAR,
            status VARCHAR DEFAULT 'success',
            total_data_points INTEGER DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.conn.execute(sql)
        
        # Create index for faster queries
        index_sql = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_testing_id ON {table_name}(testing_id)"
        self.conn.execute(index_sql)
        
        index_sql2 = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp)"
        self.conn.execute(index_sql2)
    
    async def _create_summary_table(self):
        """Create summary table for backend commit metrics"""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_names['summary']} (
            id VARCHAR PRIMARY KEY,
            testing_id VARCHAR NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            duration VARCHAR,
            total_metrics INTEGER,
            successful_metrics INTEGER,
            failed_metrics INTEGER,
            total_pods INTEGER,
            overall_avg_p99_duration DOUBLE,
            overall_max_p99_duration DOUBLE,
            overall_avg_sum_rate DOUBLE,
            overall_max_sum_rate DOUBLE,
            overall_avg_sum DOUBLE,
            overall_max_sum DOUBLE,
            overall_avg_count_rate DOUBLE,
            overall_max_count_rate DOUBLE,
            overall_avg_count DOUBLE,
            overall_max_count DOUBLE,
            status VARCHAR DEFAULT 'success',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.conn.execute(sql)
        
        # Create indexes
        index_sql = f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['summary']}_testing_id ON {self.table_names['summary']}(testing_id)"
        self.conn.execute(index_sql)
        
        index_sql2 = f"CREATE INDEX IF NOT EXISTS idx_{self.table_names['summary']}_timestamp ON {self.table_names['summary']}(timestamp)"
        self.conn.execute(index_sql2)
    
    async def store_backend_commit_metrics(self, testing_id: str, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Store backend commit metrics data in DuckDB"""
        try:
            if not StorageUtilityELT.validate_testing_id(testing_id):
                return {"status": "error", "error": "Invalid testing_id"}
            
            await self.initialize()
            
            # Extract data from metrics_data
            data = metrics_data.get("data", {})
            pods_metrics = data.get("pods_metrics", {})
            summary_data = data.get("summary", {})
            duration = metrics_data.get("duration", "1h")
            timestamp = StorageUtilityELT.parse_timestamp(metrics_data.get("timestamp"))
            
            if not timestamp:
                timestamp = StorageUtilityELT.current_timestamp()
            
            storage_results = {}
            
            # Store individual metrics
            for metric_name, metric_data in pods_metrics.items():
                if metric_data.get("status") == "success":
                    result = await self._store_metric_data(
                        testing_id, metric_name, metric_data, duration, timestamp
                    )
                    storage_results[metric_name] = result
                else:
                    storage_results[metric_name] = {"status": "skipped", "reason": "metric failed"}
            
            # Store summary
            summary_result = await self._store_summary_data(
                testing_id, summary_data, duration, timestamp
            )
            storage_results["summary"] = summary_result
            
            return {
                "status": "success",
                "testing_id": testing_id,
                "timestamp": timestamp.isoformat(),
                "storage_results": storage_results
            }
            
        except Exception as e:
            logger.error(f"Error storing backend commit metrics: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _store_metric_data(self, testing_id: str, metric_name: str, 
                                metric_data: Dict[str, Any], duration: str, 
                                timestamp: datetime) -> Dict[str, Any]:
        """Store individual metric data"""
        try:
            table_name = self.table_names.get(self._get_metric_key(metric_name))
            if not table_name:
                return {"status": "error", "error": f"Unknown metric: {metric_name}"}
            
            pods_data = metric_data.get("pods", {})
            unit = metric_data.get("unit", "")
            total_data_points = metric_data.get("total_data_points", 0)
            
            data_rows = []
            for pod_name, pod_metrics in pods_data.items():
                record_id = StorageUtilityELT.generate_uuid()
                row_data = (
                    record_id,
                    testing_id,
                    timestamp,
                    duration,
                    metric_name,
                    pod_name,
                    pod_metrics.get("avg"),
                    pod_metrics.get("max"),
                    pod_metrics.get("count", 0),
                    unit,
                    "success",
                    total_data_points,
                    StorageUtilityELT.current_timestamp()
                )
                data_rows.append(row_data)
            
            if data_rows:
                columns = [
                    "id", "testing_id", "timestamp", "duration", "metric_name",
                    "pod_name", "avg_value", "max_value",
                    "count_value", "unit", "status", "total_data_points",
                    "created_at"
                ]
                
                StorageUtilityELT.batch_insert_data(
                    self.conn, table_name, columns, data_rows, "REPLACE"
                )
                
                return {"status": "success", "records_stored": len(data_rows)}
            else:
                return {"status": "warning", "message": "No pod data to store"}
                
        except Exception as e:
            logger.error(f"Error storing metric {metric_name}: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _store_summary_data(self, testing_id: str, summary_data: Dict[str, Any], 
                                 duration: str, timestamp: datetime) -> Dict[str, Any]:
        """Store summary data"""
        try:
            metrics_overview = summary_data.get("metrics_overview", {})
            
            # Extract values for each metric type
            p99_data = metrics_overview.get("disk_backend_commit_duration_seconds_p99", {})
            sum_rate_data = metrics_overview.get("disk_backend_commit_duration_sum_rate", {})
            sum_data = metrics_overview.get("disk_backend_commit_duration_sum", {})
            count_rate_data = metrics_overview.get("disk_backend_commit_duration_count_rate", {})
            count_data = metrics_overview.get("disk_backend_commit_duration_count", {})
            
            record_id = StorageUtilityELT.generate_uuid()
            
            row_data = (
                record_id,
                testing_id,
                timestamp,
                duration,
                summary_data.get("total_metrics", 0),
                summary_data.get("successful_metrics", 0),
                summary_data.get("failed_metrics", 0),
                summary_data.get("total_pods", 0),
                p99_data.get("avg"),
                p99_data.get("max"),
                sum_rate_data.get("avg"),
                sum_rate_data.get("max"),
                sum_data.get("avg"),
                sum_data.get("max"),
                count_rate_data.get("avg"),
                count_rate_data.get("max"),
                count_data.get("avg"),
                count_data.get("max"),
                "success",
                StorageUtilityELT.current_timestamp()
            )
            
            columns = [
                "id", "testing_id", "timestamp", "duration", "total_metrics",
                "successful_metrics", "failed_metrics", "total_pods",
                "overall_avg_p99_duration", "overall_max_p99_duration",
                "overall_avg_sum_rate", "overall_max_sum_rate",
                "overall_avg_sum", "overall_max_sum",
                "overall_avg_count_rate", "overall_max_count_rate",
                "overall_avg_count", "overall_max_count",
                "status", "created_at"
            ]
            
            StorageUtilityELT.batch_insert_data(
                self.conn, self.table_names["summary"], columns, [row_data], "REPLACE"
            )
            
            return {"status": "success", "summary_stored": True}
            
        except Exception as e:
            logger.error(f"Error storing summary data: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    def _get_metric_key(self, metric_name: str) -> str:
        """Get metric key from metric name"""
        metric_map = {
            "disk_backend_commit_duration_seconds_p99": "p99_duration",
            "disk_backend_commit_duration_sum_rate": "sum_rate",
            "disk_backend_commit_duration_sum": "sum",
            "disk_backend_commit_duration_count_rate": "count_rate",
            "disk_backend_commit_duration_count": "count"
        }
        return metric_map.get(metric_name, metric_name)
    
    async def get_backend_commit_summary(self, testing_id: str) -> Dict[str, Any]:
        """Get backend commit analysis summary for a testing session"""
        try:
            await self.initialize()
            
            sql = f"""
            SELECT * FROM {self.table_names['summary']} 
            WHERE testing_id = ? 
            ORDER BY timestamp DESC 
            LIMIT 1
            """
            
            result = self.conn.execute(sql, [testing_id]).fetchone()
            
            if result:
                columns = StorageUtilityELT.get_table_columns(self.conn, self.table_names['summary'])
                summary_dict = StorageUtilityELT.row_to_dict(result, columns)
                
                return {
                    "status": "success",
                    "testing_id": testing_id,
                    "summary": summary_dict
                }
            else:
                return {
                    "status": "warning",
                    "message": f"No summary found for testing_id: {testing_id}"
                }
                
        except Exception as e:
            logger.error(f"Error getting summary for {testing_id}: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def query_backend_commit_data_by_duration(self, duration: str) -> Dict[str, Any]:
        """Query backend commit data by duration"""
        try:
            await self.initialize()
            
            # Calculate time threshold
            now = StorageUtilityELT.current_timestamp()
            duration_hours = self._parse_duration_to_hours(duration)
            time_threshold = now - timedelta(hours=duration_hours)
            
            results = {}
            
            # Query each metric table
            for metric_key, table_name in self.table_names.items():
                if metric_key != "summary":
                    sql = f"""
                    SELECT * FROM {table_name}
                    WHERE timestamp >= ?
                    ORDER BY timestamp DESC
                    """
                    
                    rows = self.conn.execute(sql, [time_threshold]).fetchall()
                    columns = StorageUtilityELT.get_table_columns(self.conn, table_name)
                    results[metric_key] = StorageUtilityELT.rows_to_dicts(rows, columns)
            
            # Query summary
            sql = f"""
            SELECT * FROM {self.table_names['summary']}
            WHERE timestamp >= ?
            ORDER BY timestamp DESC
            """
            
            summary_rows = self.conn.execute(sql, [time_threshold]).fetchall()
            summary_columns = StorageUtilityELT.get_table_columns(self.conn, self.table_names['summary'])
            results['summary'] = StorageUtilityELT.rows_to_dicts(summary_rows, summary_columns)
            
            return {
                "status": "success",
                "duration": duration,
                "time_threshold": time_threshold.isoformat(),
                "results": results
            }
            
        except Exception as e:
            logger.error(f"Error querying by duration {duration}: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def query_backend_commit_data_by_time_range(self, start_time: str, end_time: str) -> Dict[str, Any]:
        """Query backend commit data by time range"""
        try:
            await self.initialize()
            
            # Validate and parse time range
            validation = TimeRangeUtilityELT.validate_time_range(start_time, end_time)
            if not validation["valid"]:
                return {"status": "error", "error": validation["error"]}
            
            start_dt = validation["start_datetime"]
            end_dt = validation["end_datetime"]
            
            results = {}
            
            # Query each metric table
            for metric_key, table_name in self.table_names.items():
                if metric_key != "summary":
                    sql = f"""
                    SELECT * FROM {table_name}
                    WHERE timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp DESC
                    """
                    
                    rows = self.conn.execute(sql, [start_dt, end_dt]).fetchall()
                    columns = StorageUtilityELT.get_table_columns(self.conn, table_name)
                    results[metric_key] = StorageUtilityELT.rows_to_dicts(rows, columns)
            
            # Query summary
            sql = f"""
            SELECT * FROM {self.table_names['summary']}
            WHERE timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp DESC
            """
            
            summary_rows = self.conn.execute(sql, [start_dt, end_dt]).fetchall()
            summary_columns = StorageUtilityELT.get_table_columns(self.conn, self.table_names['summary'])
            results['summary'] = StorageUtilityELT.rows_to_dicts(summary_rows, summary_columns)
            
            return {
                "status": "success",
                "start_time": start_time,
                "end_time": end_time,
                "results": results
            }
            
        except Exception as e:
            logger.error(f"Error querying by time range: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    def _parse_duration_to_hours(self, duration: str) -> float:
        """Parse duration string to hours"""
        try:
            if duration.endswith('h'):
                return float(duration[:-1])
            elif duration.endswith('m'):
                return float(duration[:-1]) / 60
            elif duration.endswith('s'):
                return float(duration[:-1]) / 3600
            else:
                return float(duration)
        except:
            return 1.0
    
    async def get_table_info(self) -> Dict[str, List[str]]:
        """Get information about all backend commit tables"""
        try:
            await self.initialize()
            table_info = {}
            
            for table_name in self.table_names.values():
                columns = StorageUtilityELT.get_table_columns(self.conn, table_name)
                table_info[table_name] = columns
            
            return table_info
            
        except Exception as e:
            logger.error(f"Error getting table info: {str(e)}")
            return {}


def print_backend_commit_summary_tables(summary_data: Dict[str, Any]) -> None:
    """Print backend commit summary tables in terminal"""
    try:
        if summary_data.get("status") != "success":
            print(f"Error in summary data: {summary_data.get('error', 'Unknown error')}")
            return
        
        summary = summary_data.get("summary", {})
        if not summary:
            print("No summary data available")
            return
        
        print("\n" + "="*80)
        print("ETCD BACKEND COMMIT METRICS SUMMARY")
        print("="*80)
        
        # Overview table
        overview_data = [
            ["Testing ID", summary.get("testing_id", "N/A")],
            ["Timestamp", summary.get("timestamp", "N/A")],
            ["Duration", summary.get("duration", "N/A")],
            ["Total Metrics", summary.get("total_metrics", 0)],
            ["Successful Metrics", summary.get("successful_metrics", 0)],
            ["Failed Metrics", summary.get("failed_metrics", 0)],
            ["Total Pods", summary.get("total_pods", 0)],
        ]
        
        print("\nOverview:")
        print(tabulate(overview_data, headers=["Metric", "Value"], tablefmt="grid"))
        
        # Metrics performance table
        metrics_data = [
            ["P99 Duration (seconds)", 
             f"{summary.get('overall_avg_p99_duration', 0):.6f}", 
             f"{summary.get('overall_max_p99_duration', 0):.6f}"],
            ["Sum Rate (seconds/sec)", 
             f"{summary.get('overall_avg_sum_rate', 0):.6f}", 
             f"{summary.get('overall_max_sum_rate', 0):.6f}"],
            ["Duration Sum (seconds)", 
             f"{summary.get('overall_avg_sum', 0):.2f}", 
             f"{summary.get('overall_max_sum', 0):.2f}"],
            ["Count Rate (ops/sec)", 
             f"{summary.get('overall_avg_count_rate', 0):.2f}", 
             f"{summary.get('overall_max_count_rate', 0):.2f}"],
            ["Total Count (ops)", 
             f"{summary.get('overall_avg_count', 0):.0f}", 
             f"{summary.get('overall_max_count', 0):.0f}"],
        ]
        
        print("\nBackend Commit Performance Metrics:")
        print(tabulate(metrics_data, headers=["Metric", "Average", "Maximum"], tablefmt="grid"))
        
        print("="*80)
        
    except Exception as e:
        logger.error(f"Error printing summary tables: {str(e)}")
        print(f"Error displaying summary: {str(e)}")
