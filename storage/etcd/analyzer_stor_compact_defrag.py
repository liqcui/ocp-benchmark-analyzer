#!/usr/bin/env python3
"""
ETCD Analyzer Storage - Compact Defrag ELT Module
Storage operations for compact defragmentation metrics in DuckDB
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
import duckdb

from .etcd_analyzer_stor_utility import (
    BaseStoreELT, 
    StorageUtilityELT, 
    TimeRangeUtilityELT
)

logger = logging.getLogger(__name__)


class CompactDefragStorELT(BaseStoreELT):
    """Storage class for ETCD compact defrag metrics using DuckDB ELT patterns"""
    
    def __init__(self, db_path: str = "etcd_analyzer.duckdb"):
        super().__init__(db_path)
        self.schema_name = "pods_metrics"
        self.summary_table = "compact_defrag_summaries"
    
    async def _create_tables(self):
        """Create compact defrag related tables"""
        if not self.conn:
            await self.initialize()
        
        try:
            # Create schema
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
            
            # Create individual metric tables
            metric_tables = [
                "debugging_mvcc_db_compacted_keys",
                "debugging_mvcc_db_compaction_duration_sum_delta", 
                "debugging_mvcc_db_compaction_duration_sum",
                "debugging_snapshot_duration",
                "disk_backend_defrag_duration_sum_rate",
                "disk_backend_defrag_duration_sum"
            ]
            
            for table_name in metric_tables:
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.{table_name} (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    testing_id UUID NOT NULL,
                    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    metric_name VARCHAR NOT NULL,
                    pod_name VARCHAR,
                    node_name VARCHAR,
                    avg_value DOUBLE,
                    max_value DOUBLE,
                    count_value INTEGER,
                    unit VARCHAR,
                    status VARCHAR DEFAULT 'success',
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
                """
                self.conn.execute(create_sql)
                
                # Create index on testing_id and timestamp
                self.conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_testing_id 
                ON {self.schema_name}.{table_name} (testing_id, timestamp)
                """)
            
            # Create summary table
            summary_create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.summary_table} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                testing_id UUID NOT NULL,
                timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                metric_name VARCHAR NOT NULL,
                unit VARCHAR,
                status VARCHAR DEFAULT 'success',
                avg_raw DOUBLE,
                avg_formatted VARCHAR,
                max_raw DOUBLE,
                max_formatted VARCHAR,
                count_value INTEGER,
                data_points INTEGER,
                interpretation JSON,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
            """
            self.conn.execute(summary_create_sql)
            
            # Create index on summary table
            self.conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.summary_table}_testing_id 
            ON {self.schema_name}.{self.summary_table} (testing_id, timestamp)
            """)
            
            logger.info(f"Created compact defrag tables in schema: {self.schema_name}")
            
        except Exception as e:
            logger.error(f"Failed to create compact defrag tables: {str(e)}")
            raise
    
    async def store_compact_defrag_metrics(self, testing_id: str, compact_defrag_data: Dict[str, Any]) -> Dict[str, Any]:
        """Store compact defrag metrics data in DuckDB
        
        Args:
            testing_id: Unique testing session identifier
            compact_defrag_data: Complete compact defrag data from MCP tool
            
        Returns:
            Storage operation results
        """
        if not self.conn:
            await self.initialize()
        
        if not StorageUtilityELT.validate_testing_id(testing_id):
            return {
                "status": "error",
                "error": "Invalid testing_id provided",
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
        
        try:
            storage_results = {}
            timestamp = StorageUtilityELT.current_timestamp()
            
            # Extract pods_metrics data
            pods_metrics = compact_defrag_data.get("data", {}).get("pods_metrics", {})
            if not pods_metrics:
                return {
                    "status": "error", 
                    "error": "No pods_metrics data found",
                    "timestamp": timestamp.isoformat()
                }
            
            # Store each metric's data
            for metric_name, metric_data in pods_metrics.items():
                if metric_data.get("status") == "success":
                    result = await self._store_single_metric_data(
                        testing_id, metric_name, metric_data, timestamp
                    )
                    storage_results[metric_name] = result
                else:
                    storage_results[metric_name] = {
                        "status": "skipped",
                        "reason": f"Metric status: {metric_data.get('status')}"
                    }
            
            # Store metric summaries
            metric_summaries = compact_defrag_data.get("data", {}).get("metric_summaries", {})
            if metric_summaries:
                summary_result = await self._store_metric_summaries(
                    testing_id, metric_summaries, timestamp
                )
                storage_results["summaries"] = summary_result
            
            return {
                "status": "success",
                "storage_results": storage_results,
                "testing_id": testing_id,
                "timestamp": timestamp.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to store compact defrag metrics: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
    
    async def _store_single_metric_data(self, testing_id: str, metric_name: str, 
                                      metric_data: Dict[str, Any], timestamp: datetime) -> Dict[str, Any]:
        """Store single metric data into its dedicated table"""
        try:
            table_name = f"{self.schema_name}.{metric_name}"
            unit = metric_data.get("unit", "")
            
            # Get pods or instances data
            data_section = metric_data.get("data", {})
            pods_data = data_section.get("pods", data_section.get("instances", {}))
            
            if not pods_data:
                return {"status": "error", "error": "No pods/instances data found"}
            
            # Prepare data rows for batch insert
            data_rows = []
            for pod_name, pod_metrics in pods_data.items():
                node_name = pod_metrics.get("node", "unknown")
                
                row_data = (
                    testing_id,
                    timestamp,
                    metric_name,
                    pod_name,
                    node_name,
                    pod_metrics.get("avg"),
                    pod_metrics.get("max"),
                    pod_metrics.get("count", 0),
                    unit,
                    "success"
                )
                data_rows.append(row_data)
            
            # Batch insert
            columns = [
                "testing_id", "timestamp", "metric_name", "pod_name", "node_name",
                "avg_value", "max_value", "count_value", "unit", "status"
            ]
            
            StorageUtilityELT.batch_insert_data(
                self.conn, table_name, columns, data_rows, "REPLACE"
            )
            
            return {
                "status": "success",
                "records_inserted": len(data_rows),
                "table": table_name
            }
            
        except Exception as e:
            logger.error(f"Failed to store metric {metric_name}: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _store_metric_summaries(self, testing_id: str, metric_summaries: Dict[str, Any], 
                                    timestamp: datetime) -> Dict[str, Any]:
        """Store metric summaries in summary table"""
        try:
            table_name = f"{self.schema_name}.{self.summary_table}"
            data_rows = []
            
            for metric_name, summary_data in metric_summaries.items():
                if summary_data.get("status") == "success":
                    stats = summary_data.get("statistics", {})
                    interpretation = summary_data.get("interpretation", {})
                    
                    row_data = (
                        testing_id,
                        timestamp,
                        metric_name,
                        summary_data.get("unit", ""),
                        "success",
                        stats.get("avg", {}).get("raw"),
                        stats.get("avg", {}).get("formatted", ""),
                        stats.get("max", {}).get("raw"),
                        stats.get("max", {}).get("formatted", ""),
                        stats.get("count", 0),
                        summary_data.get("data_points", 0),
                        json.dumps(interpretation, default=str)
                    )
                    data_rows.append(row_data)
            
            if data_rows:
                columns = [
                    "testing_id", "timestamp", "metric_name", "unit", "status",
                    "avg_raw", "avg_formatted", "max_raw", "max_formatted", 
                    "count_value", "data_points", "interpretation"
                ]
                
                StorageUtilityELT.batch_insert_data(
                    self.conn, table_name, columns, data_rows, "REPLACE"
                )
                
                return {
                    "status": "success",
                    "records_inserted": len(data_rows),
                    "table": table_name
                }
            else:
                return {"status": "warning", "message": "No valid summary data to store"}
                
        except Exception as e:
            logger.error(f"Failed to store metric summaries: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def get_compact_defrag_summary(self, testing_id: str) -> Dict[str, Any]:
        """Get compact defrag summary for a testing session"""
        if not self.conn:
            await self.initialize()
        
        try:
            # Get summary data
            summary_sql = f"""
            SELECT metric_name, unit, status, avg_raw, avg_formatted,
                   max_raw, max_formatted, count_value, data_points, 
                   interpretation, timestamp
            FROM {self.schema_name}.{self.summary_table}
            WHERE testing_id = ?
            ORDER BY metric_name, timestamp DESC
            """
            
            summary_rows = self.conn.execute(summary_sql, [testing_id]).fetchall()
            summary_columns = [
                "metric_name", "unit", "status", "avg_raw", "avg_formatted",
                "max_raw", "max_formatted", "count_value", "data_points",
                "interpretation", "timestamp"
            ]
            
            summaries = {}
            for row in summary_rows:
                row_dict = StorageUtilityELT.row_to_dict(row, summary_columns)
                metric_name = row_dict["metric_name"]
                
                # Parse interpretation JSON
                try:
                    interpretation = json.loads(row_dict["interpretation"]) if row_dict["interpretation"] else {}
                except:
                    interpretation = {}
                
                summaries[metric_name] = {
                    "unit": row_dict["unit"],
                    "status": row_dict["status"],
                    "statistics": {
                        "avg": {"raw": row_dict["avg_raw"], "formatted": row_dict["avg_formatted"]},
                        "max": {"raw": row_dict["max_raw"], "formatted": row_dict["max_formatted"]},
                        "count": row_dict["count_value"]
                    },
                    "data_points": row_dict["data_points"],
                    "interpretation": interpretation,
                    "timestamp": row_dict["timestamp"]
                }
            
            return {
                "status": "success",
                "testing_id": testing_id,
                "summaries": summaries,
                "total_metrics": len(summaries),
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get compact defrag summary: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
    
    async def query_compact_defrag_data_by_duration(self, duration: str) -> Dict[str, Any]:
        """Query compact defrag data by duration (e.g., '1h', '30m', '2d')"""
        if not self.conn:
            await self.initialize()
        
        try:
            # Convert duration to SQL interval
            duration_mapping = {
                'm': 'MINUTE', 'h': 'HOUR', 'd': 'DAY', 
                'min': 'MINUTE', 'hour': 'HOUR', 'day': 'DAY'
            }
            
            # Parse duration (e.g., '1h' -> 1, 'h')
            import re
            match = re.match(r'^(\d+)([mhd]|min|hour|day)$', duration.lower())
            if not match:
                return {"status": "error", "error": f"Invalid duration format: {duration}"}
            
            amount, unit = match.groups()
            sql_unit = duration_mapping.get(unit)
            if not sql_unit:
                return {"status": "error", "error": f"Unsupported duration unit: {unit}"}
            
            results = {}
            
            # Query each metric table
            metric_tables = [
                "debugging_mvcc_db_compacted_keys",
                "debugging_mvcc_db_compaction_duration_sum_delta",
                "debugging_mvcc_db_compaction_duration_sum", 
                "debugging_snapshot_duration",
                "disk_backend_defrag_duration_sum_rate",
                "disk_backend_defrag_duration_sum"
            ]
            
            for table_name in metric_tables:
                query_sql = f"""
                SELECT testing_id, timestamp, metric_name, pod_name, node_name,
                       avg_value, max_value, count_value,
                       unit, status
                FROM {self.schema_name}.{table_name}
                WHERE timestamp >= NOW() - INTERVAL '{amount} {sql_unit}'
                ORDER BY timestamp DESC, pod_name
                """
                
                rows = self.conn.execute(query_sql).fetchall()
                columns = [
                    "testing_id", "timestamp", "metric_name", "pod_name", "node_name",
                    "avg_value", "max_value", "count_value",
                    "unit", "status"
                ]
                
                results[table_name] = StorageUtilityELT.rows_to_dicts(rows, columns)
            
            return {
                "status": "success",
                "duration": duration,
                "results": results,
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to query compact defrag data by duration: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
    
    async def query_compact_defrag_data_by_time_range(self, start_time: str, end_time: str) -> Dict[str, Any]:
        """Query compact defrag data by time range (UTC)"""
        if not self.conn:
            await self.initialize()
        
        # Validate time range
        validation = TimeRangeUtilityELT.validate_time_range(start_time, end_time)
        if not validation["valid"]:
            return {
                "status": "error",
                "error": validation["error"],
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
        
        try:
            start_dt = validation["start_datetime"]
            end_dt = validation["end_datetime"]
            results = {}
            
            # Query each metric table
            metric_tables = [
                "debugging_mvcc_db_compacted_keys",
                "debugging_mvcc_db_compaction_duration_sum_delta",
                "debugging_mvcc_db_compaction_duration_sum",
                "debugging_snapshot_duration", 
                "disk_backend_defrag_duration_sum_rate",
                "disk_backend_defrag_duration_sum"
            ]
            
            for table_name in metric_tables:
                query_sql = f"""
                SELECT testing_id, timestamp, metric_name, pod_name, node_name,
                       avg_value, max_value, count_value,
                       unit, status
                FROM {self.schema_name}.{table_name}
                WHERE timestamp BETWEEN ? AND ?
                ORDER BY timestamp DESC, pod_name
                """
                
                rows = self.conn.execute(query_sql, [start_dt, end_dt]).fetchall()
                columns = [
                    "testing_id", "timestamp", "metric_name", "pod_name", "node_name",
                    "avg_value", "max_value", "count_value",
                    "unit", "status"
                ]
                
                results[table_name] = StorageUtilityELT.rows_to_dicts(rows, columns)
            
            return {
                "status": "success", 
                "start_time": start_time,
                "end_time": end_time,
                "results": results,
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to query compact defrag data by time range: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": StorageUtilityELT.current_timestamp().isoformat()
            }
    
    async def get_table_info(self) -> Dict[str, List[str]]:
        """Get table information for compact defrag tables"""
        if not self.conn:
            await self.initialize()
        
        table_info = {}
        
        # Get metric tables info
        metric_tables = [
            "debugging_mvcc_db_compacted_keys",
            "debugging_mvcc_db_compaction_duration_sum_delta",
            "debugging_mvcc_db_compaction_duration_sum",
            "debugging_snapshot_duration",
            "disk_backend_defrag_duration_sum_rate", 
            "disk_backend_defrag_duration_sum"
        ]
        
        for table_name in metric_tables:
            full_table_name = f"{self.schema_name}.{table_name}"
            columns = StorageUtilityELT.get_table_columns(self.conn, full_table_name)
            table_info[full_table_name] = columns
        
        # Get summary table info
        summary_table_name = f"{self.schema_name}.{self.summary_table}"
        summary_columns = StorageUtilityELT.get_table_columns(self.conn, summary_table_name)
        table_info[summary_table_name] = summary_columns
        
        return table_info


def print_compact_defrag_summary_tables(summary_data: Dict[str, Any]) -> None:
    """Print compact defrag summary tables in readable format in terminal"""
    if summary_data.get("status") != "success":
        print(f"❌ Compact Defrag Summary Error: {summary_data.get('error')}")
        return
    
    summaries = summary_data.get("summaries", {})
    if not summaries:
        print("⚠️  No compact defrag summary data available")
        return
    
    print("\n" + "="*100)
    print("🔧 ETCD COMPACT DEFRAG METRICS SUMMARY")
    print("="*100)
    
    # Table headers
    headers = [
        "Metric Name", "Avg Value", "Max Value", "Unit", 
        "Count", "Status", "Interpretation"
    ]
    
    # Calculate column widths
    col_widths = [len(h) for h in headers]
    
    for metric_name, summary in summaries.items():
        stats = summary.get("statistics", {})
        interpretation = summary.get("interpretation", {})
        
        row_data = [
            metric_name,
            stats.get("avg", {}).get("formatted", "N/A"),
            stats.get("max", {}).get("formatted", "N/A"), 
            summary.get("unit", ""),
            str(stats.get("count", 0)),
            summary.get("status", "unknown"),
            interpretation.get("avg", "No interpretation")
        ]
        
        # Update column widths
        for i, data in enumerate(row_data):
            col_widths[i] = max(col_widths[i], len(str(data)))
    
    # Print table header
    header_row = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    print(header_row)
    print("-" * len(header_row))
    
    # Print data rows
    for metric_name, summary in summaries.items():
        stats = summary.get("statistics", {})
        interpretation = summary.get("interpretation", {})
        
        row_data = [
            metric_name,
            stats.get("avg", {}).get("formatted", "N/A"),
            stats.get("max", {}).get("formatted", "N/A"),
            summary.get("unit", ""),
            str(stats.get("count", 0)),
            summary.get("status", "unknown"),
            interpretation.get("avg", "No interpretation")
        ]
        
        data_row = " | ".join(str(data).ljust(col_widths[i]) for i, data in enumerate(row_data))
        print(data_row)
    
    print("\n" + "="*100)
    print(f"📊 Total Metrics: {len(summaries)}")
    print(f"🕒 Query Time: {summary_data.get('timestamp', 'Unknown')}")
    print("="*100 + "\n")
