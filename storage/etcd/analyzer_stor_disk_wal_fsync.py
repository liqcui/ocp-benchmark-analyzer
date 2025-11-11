#!/usr/bin/env python3
"""
ETCD Analyzer Disk WAL Fsync Storage Module
Handles DuckDB storage operations for WAL fsync metrics data
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Union
import duckdb

from .etcd_analyzer_stor_utility import BaseStoreELT, StorageUtilityELT

logger = logging.getLogger(__name__)


class DiskWalFsyncStorELT(BaseStoreELT):
    """Storage class for WAL fsync metrics data"""
    
    def __init__(self, db_path: str = "etcd_analyzer.duckdb"):
        super().__init__(db_path)
        self.metric_tables = [
            'disk_wal_fsync_seconds_duration_p99',
            'disk_wal_fsync_duration_seconds_sum_rate', 
            'disk_wal_fsync_duration_sum',
            'disk_wal_fsync_duration_seconds_count_rate',
            'disk_wal_fsync_duration_seconds_count'
        ]
    
    async def _create_tables(self):
        """Create WAL fsync metrics tables"""
        
        # Create WAL Fsync Metrics Overview table
        create_metrics_overview_sql = """
        CREATE TABLE IF NOT EXISTS disk_wal_fsync_metrics_overview (
            id VARCHAR PRIMARY KEY,
            testing_id VARCHAR NOT NULL,
            metric VARCHAR NOT NULL,
            unit VARCHAR,
            pods INTEGER,
            avg_value VARCHAR,
            max_value VARCHAR,
            data_points INTEGER,
            collection_timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Create Pod Performance Summary table
        create_pod_summary_sql = """
        CREATE TABLE IF NOT EXISTS disk_wal_fsync_pod_performance_summary (
            id VARCHAR PRIMARY KEY,
            testing_id VARCHAR NOT NULL,
            pod_name VARCHAR NOT NULL,
            node VARCHAR,
            p99_latency_avg_ms DOUBLE,
            p99_latency_max_ms DOUBLE,
            ops_rate_avg DOUBLE,
            total_operations BIGINT,
            cumulative_duration_s DOUBLE,
            collection_timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Create main summary table for overall statistics
        create_summary_sql = """
        CREATE TABLE IF NOT EXISTS disk_wal_fsync_summary (
            testing_id VARCHAR PRIMARY KEY,
            collection_timestamp TIMESTAMP,
            duration VARCHAR,
            total_metrics INTEGER,
            successful_metrics INTEGER,
            failed_metrics INTEGER,
            total_pods INTEGER,
            performance_rating VARCHAR,
            max_p99_latency_ms DOUBLE,
            avg_p99_latency_ms DOUBLE,
            max_ops_per_sec DOUBLE,
            health_status VARCHAR,
            recommendations TEXT,
            raw_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Create individual metric tables
        metric_table_template = """
        CREATE TABLE IF NOT EXISTS {table_name} (
            id VARCHAR PRIMARY KEY,
            testing_id VARCHAR NOT NULL,
            pod_name VARCHAR NOT NULL,
            node_name VARCHAR,
            metric_name VARCHAR NOT NULL,
            unit VARCHAR,
            promql_query TEXT,
            avg_value DOUBLE,
            max_value DOUBLE,            
            data_points INTEGER,
            collection_timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Execute table creation
        self.conn.execute(create_metrics_overview_sql)
        self.conn.execute(create_pod_summary_sql)
        self.conn.execute(create_summary_sql)
        
        for table_name in self.metric_tables:
            sql = metric_table_template.format(table_name=table_name)
            self.conn.execute(sql)
        
        # Create indexes
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_disk_wal_fsync_metrics_overview_testing_id ON disk_wal_fsync_metrics_overview(testing_id)",
            "CREATE INDEX IF NOT EXISTS idx_disk_wal_fsync_pod_summary_testing_id ON disk_wal_fsync_pod_performance_summary(testing_id)",
            "CREATE INDEX IF NOT EXISTS idx_disk_wal_fsync_summary_testing_id ON disk_wal_fsync_summary(testing_id)",
            "CREATE INDEX IF NOT EXISTS idx_disk_wal_fsync_summary_timestamp ON disk_wal_fsync_summary(collection_timestamp)"
        ]
        
        for table_name in self.metric_tables:
            indexes.extend([
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_testing_id ON {table_name}(testing_id)",
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_pod ON {table_name}(pod_name)",
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(collection_timestamp)"
            ])
        
        for idx_sql in indexes:
            self.conn.execute(idx_sql)
        
        logger.info("Created WAL fsync metrics tables and indexes")
    
    async def store_wal_fsync_metrics(self, testing_id: str, wal_fsync_data: Dict[str, Any]) -> Dict[str, Any]:
        """Store WAL fsync metrics data
        
        Args:
            testing_id: Unique testing session ID
            wal_fsync_data: WAL fsync metrics data dictionary
            
        Returns:
            Storage result dictionary
        """
        if not self._initialized:
            await self.initialize()
        
        try:
            operations = []
            storage_results = {}
            
            # Validate input data
            if not wal_fsync_data or wal_fsync_data.get("status") != "success":
                return {"status": "error", "error": "Invalid or failed WAL fsync data"}
            
            data = wal_fsync_data.get("data", {})
            metrics = data.get("metrics", {})
            collection_timestamp = StorageUtilityELT.parse_timestamp(data.get("timestamp"))
            
            # Store individual metrics data
            for metric_name, metric_data in metrics.items():
                if metric_name in self.metric_tables and metric_data.get("status") == "success":
                    operations.extend(
                        self._prepare_metric_table_operations(
                            testing_id, metric_name, metric_data, collection_timestamp
                        )
                    )
            
            # Generate overview and pod summary data
            overview_data = self._generate_metrics_overview(testing_id, metrics, collection_timestamp)
            pod_summary_data = self._generate_pod_performance_summary(testing_id, metrics, collection_timestamp)
            
            # Store overview data
            operations.extend(self._prepare_overview_operations(overview_data))
            
            # Store pod summary data
            operations.extend(self._prepare_pod_summary_operations(pod_summary_data))
            
            # Store main summary
            summary_data = self._generate_summary_data(testing_id, data, overview_data, pod_summary_data)
            operations.append(self._prepare_summary_operation(summary_data))
            
            # Execute all operations in transaction
            self._execute_with_transaction(operations)
            
            storage_results = {
                "metrics_stored": len([op for op in operations if "INSERT" in op[0]]),
                "overview_records": len(overview_data),
                "pod_summary_records": len(pod_summary_data),
                "summary_record": 1 if summary_data else 0
            }
            
            # Generate and print the summary report
            summary_report = await self.generate_summary_report(testing_id)
            
            return {
                "status": "success",
                "testing_id": testing_id,
                "storage_results": storage_results,
                "summary_report": summary_report
            }
            
        except Exception as e:
            logger.error(f"Failed to store WAL fsync metrics: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    def _generate_metrics_overview(self, testing_id: str, metrics: Dict[str, Any], timestamp: datetime) -> List[Dict[str, Any]]:
        """Generate metrics overview data"""
        overview_records = []
        
        for metric_name, metric_data in metrics.items():
            if metric_data.get("status") != "success":
                continue
            
            pod_metrics = metric_data.get("pod_metrics", {})
            total_pods = len(pod_metrics)
            
            # Calculate aggregate values
            all_avg_values = []
            all_max_values = []
            total_data_points = 0
            
            for pod_stats in pod_metrics.values():
                # Get the appropriate value keys based on metric type
                if "p99" in metric_name or "percentile" in metric_name.lower():
                    avg_val = pod_stats.get("avg_seconds", 0)
                    max_val = pod_stats.get("max_seconds", 0)
                    all_avg_values.append(avg_val * 1000)  # Convert to ms
                    all_max_values.append(max_val * 1000)
                elif "rate" in metric_name:
                    if "count" in metric_name:
                        avg_val = pod_stats.get("avg_ops_per_sec", 0) or pod_stats.get("avg_rate_seconds", 0)
                        max_val = pod_stats.get("max_ops_per_sec", 0) or pod_stats.get("max_rate_seconds", 0)
                    else:
                        avg_val = pod_stats.get("avg_rate_seconds", 0)
                        max_val = pod_stats.get("max_rate_seconds", 0)
                        avg_val *= 1000  # Convert to ms
                        max_val *= 1000
                    all_avg_values.append(avg_val)
                    all_max_values.append(max_val)
                elif "sum" in metric_name:
                    avg_val = pod_stats.get("avg_sum_seconds", 0)
                    max_val = pod_stats.get("max_sum_seconds", 0)
                    all_avg_values.append(avg_val)
                    all_max_values.append(max_val)
                elif "count" in metric_name and "rate" not in metric_name:
                    avg_val = pod_stats.get("avg_count", 0)
                    max_val = pod_stats.get("max_count", 0)
                    all_avg_values.append(avg_val / 1000)  # Convert to K
                    all_max_values.append(max_val / 1000)
                
                total_data_points += pod_stats.get("data_points", 0)
            
            # Format values appropriately
            if "p99" in metric_name:
                avg_display = f"{sum(all_avg_values)/len(all_avg_values):.3f} ms" if all_avg_values else "0 ms"
                max_display = f"{max(all_max_values):.3f} ms" if all_max_values else "0 ms"
            elif "rate" in metric_name and "count" in metric_name:
                avg_display = f"{sum(all_avg_values)/len(all_avg_values):.0f}" if all_avg_values else "0"
                max_display = f"{max(all_max_values):.0f}" if all_max_values else "0"
            elif "rate" in metric_name:
                avg_display = f"{sum(all_avg_values)/len(all_avg_values):.3f} ms" if all_avg_values else "0 ms"
                max_display = f"{max(all_max_values):.3f} ms" if all_max_values else "0 ms"
            elif "sum" in metric_name:
                avg_display = f"{sum(all_avg_values)/len(all_avg_values):.6f} s" if all_avg_values else "0 s"
                max_display = f"{max(all_max_values):.6f} s" if all_max_values else "0 s"
            elif "count" in metric_name:
                avg_display = f"{sum(all_avg_values)/len(all_avg_values):.1f}K" if all_avg_values else "0K"
                max_display = f"{max(all_max_values):.1f}K" if all_max_values else "0K"
            else:
                avg_display = f"{sum(all_avg_values)/len(all_avg_values):.3f}" if all_avg_values else "0"
                max_display = f"{max(all_max_values):.3f}" if all_max_values else "0"
            
            overview_records.append({
                "id": StorageUtilityELT.create_composite_id(testing_id, metric_name),
                "testing_id": testing_id,
                "metric": metric_name,
                "unit": metric_data.get("unit", "unknown"),
                "pods": total_pods,
                "avg_value": avg_display,
                "max_value": max_display,
                "data_points": total_data_points,
                "collection_timestamp": timestamp
            })
        
        return overview_records
    
    def _generate_pod_performance_summary(self, testing_id: str, metrics: Dict[str, Any], timestamp: datetime) -> List[Dict[str, Any]]:
        """Generate pod performance summary data"""
        pod_summaries = {}
        
        # Collect data from different metrics for each pod
        for metric_name, metric_data in metrics.items():
            if metric_data.get("status") != "success":
                continue
            
            pod_metrics = metric_data.get("pod_metrics", {})
            node_mapping = metric_data.get("node_mapping", {})
            
            for pod_name, pod_stats in pod_metrics.items():
                if pod_name not in pod_summaries:
                    pod_summaries[pod_name] = {
                        "id": StorageUtilityELT.create_composite_id(testing_id, pod_name),
                        "testing_id": testing_id,
                        "pod_name": pod_name,
                        "node": node_mapping.get(pod_name, "unknown"),
                        "p99_latency_avg_ms": 0.0,
                        "p99_latency_max_ms": 0.0,
                        "ops_rate_avg": 0.0,
                        "total_operations": 0,
                        "cumulative_duration_s": 0.0,
                        "collection_timestamp": timestamp
                    }
                
                # Extract specific metrics
                if "p99" in metric_name:
                    pod_summaries[pod_name]["p99_latency_avg_ms"] = (pod_stats.get("avg_seconds", 0) or 0) * 1000
                    pod_summaries[pod_name]["p99_latency_max_ms"] = (pod_stats.get("max_seconds", 0) or 0) * 1000
                elif metric_name == "disk_wal_fsync_duration_seconds_count_rate":
                    pod_summaries[pod_name]["ops_rate_avg"] = pod_stats.get("avg_ops_per_sec", 0) or pod_stats.get("avg_rate_seconds", 0) or 0
                elif metric_name == "disk_wal_fsync_duration_seconds_count":
                    pod_summaries[pod_name]["total_operations"] = int(pod_stats.get("latest_count", 0) or 0)
                elif metric_name == "disk_wal_fsync_duration_sum":
                    pod_summaries[pod_name]["cumulative_duration_s"] = pod_stats.get("latest_sum_seconds", 0) or 0
        
        return list(pod_summaries.values())
    
    def _prepare_overview_operations(self, overview_data: List[Dict[str, Any]]) -> List[tuple]:
        """Prepare database operations for overview data"""
        operations = []
        
        for record in overview_data:
            insert_sql = """
            INSERT OR REPLACE INTO disk_wal_fsync_metrics_overview 
            (id, testing_id, metric, unit, pods, avg_value, max_value, data_points, collection_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            operations.append((insert_sql, (
                record["id"], record["testing_id"], record["metric"], record["unit"],
                record["pods"], record["avg_value"], record["max_value"], 
                record["data_points"], record["collection_timestamp"]
            )))
        
        return operations
    
    def _prepare_pod_summary_operations(self, pod_summary_data: List[Dict[str, Any]]) -> List[tuple]:
        """Prepare database operations for pod summary data"""
        operations = []
        
        for record in pod_summary_data:
            insert_sql = """
            INSERT OR REPLACE INTO disk_wal_fsync_pod_performance_summary 
            (id, testing_id, pod_name, node, p99_latency_avg_ms, p99_latency_max_ms, 
             ops_rate_avg, total_operations, cumulative_duration_s, collection_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            operations.append((insert_sql, (
                record["id"], record["testing_id"], record["pod_name"], record["node"],
                record["p99_latency_avg_ms"], record["p99_latency_max_ms"],
                record["ops_rate_avg"], record["total_operations"], 
                record["cumulative_duration_s"], record["collection_timestamp"]
            )))
        
        return operations
    
    def _prepare_metric_table_operations(self, testing_id: str, metric_name: str, 
                                       metric_data: Dict[str, Any], timestamp: datetime) -> List[tuple]:
        """Prepare database operations for individual metric tables"""
        operations = []
        pod_metrics = metric_data.get("pod_metrics", {})
        node_mapping = metric_data.get("node_mapping", {})
        
        for pod_name, pod_stats in pod_metrics.items():
            record_id = StorageUtilityELT.create_composite_id(testing_id, f"{metric_name}_{pod_name}")
            
            # Get appropriate values based on metric type
            if "p99" in metric_name:
                avg_val = pod_stats.get("avg_seconds", 0)
                max_val = pod_stats.get("max_seconds", 0) 
            elif "rate" in metric_name:
                if "count" in metric_name:
                    avg_val = pod_stats.get("avg_ops_per_sec", 0) or pod_stats.get("avg_rate_seconds", 0)
                    max_val = pod_stats.get("max_ops_per_sec", 0) or pod_stats.get("max_rate_seconds", 0)
                else:
                    avg_val = pod_stats.get("avg_rate_seconds", 0)
                    max_val = pod_stats.get("max_rate_seconds", 0)
            elif "sum" in metric_name:
                avg_val = pod_stats.get("avg_sum_seconds", 0)
                max_val = pod_stats.get("max_sum_seconds", 0)
            elif "count" in metric_name:
                avg_val = pod_stats.get("avg_count", 0)
                max_val = pod_stats.get("max_count", 0)
            else:
                avg_val = pod_stats.get("avg_value", 0)
                max_val = pod_stats.get("max_value", 0)
            
            insert_sql = f"""
            INSERT OR REPLACE INTO {metric_name} 
            (id, testing_id, pod_name, node_name, metric_name, unit, avg_value, 
             max_value, data_points, collection_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            operations.append((insert_sql, (
                record_id, testing_id, pod_name, node_mapping.get(pod_name, "unknown"),
                metric_name, metric_data.get("unit", "unknown"), avg_val, max_val, 
                pod_stats.get("data_points", 0), timestamp
            )))
        
        return operations
    
    def _generate_summary_data(self, testing_id: str, data: Dict[str, Any], 
                             overview_data: List[Dict[str, Any]], 
                             pod_summary_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate main summary data"""
        summary = data.get("summary", {})
        metrics = data.get("metrics", {})
        
        # Calculate performance indicators
        p99_latencies = [pod["p99_latency_max_ms"] for pod in pod_summary_data]
        max_p99_latency = max(p99_latencies) if p99_latencies else 0
        avg_p99_latency = sum(p99_latencies) / len(p99_latencies) if p99_latencies else 0
        
        ops_rates = [pod["ops_rate_avg"] for pod in pod_summary_data]
        max_ops_per_sec = max(ops_rates) if ops_rates else 0
        
        # Determine performance rating
        if max_p99_latency <= 10:
            performance_rating = "excellent"
            health_status = "healthy"
        elif max_p99_latency <= 25:
            performance_rating = "good"
            health_status = "healthy"
        elif max_p99_latency <= 100:
            performance_rating = "warning"
            health_status = "warning"
        else:
            performance_rating = "critical"
            health_status = "critical"
        
        # Generate recommendations
        recommendations = []
        if max_p99_latency > 100:
            recommendations.append("High WAL fsync latency detected (>100ms). Check disk I/O performance.")
        if max_ops_per_sec > 1000:
            recommendations.append("High WAL fsync operation rate. Monitor for potential performance impact.")
        if not recommendations:
            recommendations.append("WAL fsync performance is within acceptable ranges.")
        
        return {
            "testing_id": testing_id,
            "collection_timestamp": StorageUtilityELT.parse_timestamp(data.get("timestamp")),
            "duration": data.get("duration", "unknown"),
            "total_metrics": summary.get("total_metrics", 0),
            "successful_metrics": summary.get("successful_metrics", 0),
            "failed_metrics": summary.get("failed_metrics", 0),
            "total_pods": len(pod_summary_data),
            "performance_rating": performance_rating,
            "max_p99_latency_ms": max_p99_latency,
            "avg_p99_latency_ms": avg_p99_latency,
            "max_ops_per_sec": max_ops_per_sec,
            "health_status": health_status,
            "recommendations": json.dumps(recommendations),
            "raw_data": StorageUtilityELT.serialize_json(data)
        }
    
    def _prepare_summary_operation(self, summary_data: Dict[str, Any]) -> tuple:
        """Prepare database operation for summary data"""
        insert_sql = """
        INSERT OR REPLACE INTO disk_wal_fsync_summary 
        (testing_id, collection_timestamp, duration, total_metrics, successful_metrics, 
         failed_metrics, total_pods, performance_rating, max_p99_latency_ms, 
         avg_p99_latency_ms, max_ops_per_sec, health_status, recommendations, raw_data)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        return (insert_sql, (
            summary_data["testing_id"], summary_data["collection_timestamp"],
            summary_data["duration"], summary_data["total_metrics"],
            summary_data["successful_metrics"], summary_data["failed_metrics"],
            summary_data["total_pods"], summary_data["performance_rating"],
            summary_data["max_p99_latency_ms"], summary_data["avg_p99_latency_ms"],
            summary_data["max_ops_per_sec"], summary_data["health_status"],
            summary_data["recommendations"], summary_data["raw_data"]
        ))
    
    async def generate_summary_report(self, testing_id: str) -> Dict[str, Any]:
        """Generate and return summary report from stored data"""
        try:
            # Get overview data
            overview_query = """
            SELECT metric, unit, pods, avg_value, max_value, data_points
            FROM disk_wal_fsync_metrics_overview 
            WHERE testing_id = ?
            ORDER BY metric
            """
            overview_results = self.conn.execute(overview_query, (testing_id,)).fetchall()
            
            # Get pod summary data
            pod_summary_query = """
            SELECT pod_name, node, p99_latency_avg_ms, p99_latency_max_ms, 
                   ops_rate_avg, total_operations, cumulative_duration_s
            FROM disk_wal_fsync_pod_performance_summary 
            WHERE testing_id = ?
            ORDER BY pod_name
            """
            pod_results = self.conn.execute(pod_summary_query, (testing_id,)).fetchall()
            
            # Get main summary
            summary_query = """
            SELECT performance_rating, health_status, max_p99_latency_ms, 
                   avg_p99_latency_ms, max_ops_per_sec, recommendations
            FROM disk_wal_fsync_summary 
            WHERE testing_id = ?
            """
            summary_result = self.conn.execute(summary_query, (testing_id,)).fetchone()
            
            # Format overview data
            metrics_overview = []
            for row in overview_results:
                metrics_overview.append({
                    "metric": row[0],
                    "unit": row[1],
                    "pods": row[2],
                    "avg_value": row[3],
                    "max_value": row[4],
                    "data_points": row[5]
                })
            
            # Format pod summary data
            pod_performance_summary = []
            for row in pod_results:
                pod_performance_summary.append({
                    "pod_name": row[0],
                    "node": row[1],
                    "p99_latency_avg": f"{row[2]:.3f} ms" if row[2] else "0.000 ms",
                    "p99_latency_max": f"{row[3]:.3f} ms" if row[3] else "0.000 ms", 
                    "ops_rate_avg": f"{row[4]:.2f} ops/s" if row[4] else "0.00 ops/s",
                    "total_operations": f"{row[5]:,}" if row[5] else "0",
                    "cumulative_duration": f"{row[6]:.3f} s" if row[6] else "0.000 s"
                })
            
            report = {
                "testing_id": testing_id,
                "metrics_overview": metrics_overview,
                "pod_performance_summary": pod_performance_summary
            }
            
            if summary_result:
                report["overall_summary"] = {
                    "performance_rating": summary_result[0],
                    "health_status": summary_result[1],
                    "max_p99_latency_ms": summary_result[2],
                    "avg_p99_latency_ms": summary_result[3],
                    "max_ops_per_sec": summary_result[4],
                    "recommendations": json.loads(summary_result[5]) if summary_result[5] else []
                }
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating summary report: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def query_wal_fsync_data_by_duration(self, duration: str = "1h") -> Dict[str, Any]:
        """Query WAL fsync data by duration from current time"""
        try:
            current_time = StorageUtilityELT.current_timestamp()
            
            # Convert duration to seconds for calculation
            duration_seconds = self._parse_duration_to_seconds(duration)
            start_time = current_time - timedelta(seconds=duration_seconds)
            
            return await self._query_wal_fsync_data_by_time_range(start_time, current_time)
            
        except Exception as e:
            logger.error(f"Error querying WAL fsync data by duration: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def query_wal_fsync_data_by_time_range(self, start_time: str, end_time: str) -> Dict[str, Any]:
        """Query WAL fsync data by time range"""
        try:
            start_dt = StorageUtilityELT.parse_timestamp(start_time)
            end_dt = StorageUtilityELT.parse_timestamp(end_time)
            
            if not start_dt or not end_dt:
                return {"status": "error", "error": "Invalid timestamp format"}
            
            return await self._query_wal_fsync_data_by_time_range(start_dt, end_dt)
            
        except Exception as e:
            logger.error(f"Error querying WAL fsync data by time range: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _query_wal_fsync_data_by_time_range(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Internal method to query WAL fsync data by time range"""
        try:
            # Query summary data
            summary_query = """
            SELECT testing_id, collection_timestamp, duration, performance_rating, 
                   health_status, max_p99_latency_ms, avg_p99_latency_ms, max_ops_per_sec
            FROM disk_wal_fsync_summary 
            WHERE collection_timestamp BETWEEN ? AND ?
            ORDER BY collection_timestamp DESC
            """
            
            summary_results = self.conn.execute(summary_query, (start_time, end_time)).fetchall()
            
            query_results = []
            for row in summary_results:
                testing_id = row[0]
                
                # Get detailed report for each testing session
                report = await self.generate_summary_report(testing_id)
                
                query_results.append({
                    "testing_id": testing_id,
                    "collection_timestamp": row[1].isoformat() if row[1] else None,
                    "duration": row[2],
                    "performance_rating": row[3],
                    "health_status": row[4],
                    "max_p99_latency_ms": row[5],
                    "avg_p99_latency_ms": row[6],
                    "max_ops_per_sec": row[7],
                    "detailed_report": report
                })
            
            return {
                "status": "success",
                "query_range": {
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat()
                },
                "total_records": len(query_results),
                "results": query_results
            }
            
        except Exception as e:
            logger.error(f"Error in _query_wal_fsync_data_by_time_range: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    def _parse_duration_to_seconds(self, duration: str) -> int:
        """Parse duration string to seconds"""
        import re
        
        # Handle formats like '1h', '30m', '2h30m', etc.
        duration = duration.lower().strip()
        
        # Extract hours and minutes
        hours = 0
        minutes = 0
        
        hour_match = re.search(r'(\d+)h', duration)
        if hour_match:
            hours = int(hour_match.group(1))
        
        minute_match = re.search(r'(\d+)m', duration)
        if minute_match:
            minutes = int(minute_match.group(1))
        
        # If no pattern matches, assume it's minutes
        if hours == 0 and minutes == 0:
            try:
                minutes = int(duration.replace('m', ''))
            except:
                minutes = 60  # Default to 1 hour
        
        return hours * 3600 + minutes * 60

    async def get_table_info(self) -> Dict[str, List[str]]:
        """Get information about WAL fsync tables and their columns"""
        if not self._initialized:
            await self.initialize()
        try:
            tables = [
                "disk_wal_fsync_metrics_overview",
                "disk_wal_fsync_pod_performance_summary",
                "disk_wal_fsync_summary",
            ] + self.metric_tables

            table_info: Dict[str, List[str]] = {}
            for table in tables:
                columns = StorageUtilityELT.get_table_columns(self.conn, table)
                table_info[table] = columns

            return table_info
        except Exception as e:
            logger.error(f"Failed to get WAL fsync table info: {str(e)}")
            return {}


# Convenience functions for external usage
async def store_wal_fsync_metrics(testing_id: str, wal_fsync_data: Dict[str, Any], 
                                db_path: str = "etcd_analyzer.duckdb") -> Dict[str, Any]:
    """Convenience function to store WAL fsync metrics"""
    storage = DiskWalFsyncStorELT(db_path)
    try:
        return await storage.store_wal_fsync_metrics(testing_id, wal_fsync_data)
    finally:
        storage.close()


async def query_wal_fsync_by_duration(duration: str = "1h", 
                                    db_path: str = "etcd_analyzer.duckdb") -> Dict[str, Any]:
    """Convenience function to query WAL fsync data by duration"""
    storage = DiskWalFsyncStorELT(db_path)
    try:
        await storage.initialize()
        return await storage.query_wal_fsync_data_by_duration(duration)
    finally:
        storage.close()


async def query_wal_fsync_by_time_range(start_time: str, end_time: str,
                                      db_path: str = "etcd_analyzer.duckdb") -> Dict[str, Any]:
    """Convenience function to query WAL fsync data by time range"""
    storage = DiskWalFsyncStorELT(db_path)
    try:
        await storage.initialize()
        return await storage.query_wal_fsync_data_by_time_range(start_time, end_time)
    finally:
        storage.close()


def print_wal_fsync_summary_tables(summary_report: Dict[str, Any]):
    """Print WAL Fsync summary tables in formatted output"""
    
    print("\n" + "="*80)
    print("WAL Fsync Metrics Overview")
    print("="*80)
    print(f"{'Metric':<50} {'Unit':<12} {'Pods':<6} {'Avg Value':<15} {'Max Value':<15} {'Data Points':<12}")
    print("-" * 80)
    
    for metric in summary_report.get("metrics_overview", []):
        print(f"{metric['metric']:<50} {metric['unit']:<12} {metric['pods']:<6} "
              f"{metric['avg_value']:<15} {metric['max_value']:<15} {metric['data_points']:<12}")
    
    print("\n" + "="*100)
    print("Pod Performance Summary")
    print("="*100)
    print(f"{'Pod Name':<30} {'Node':<25} {'P99 Latency (avg)':<16} {'P99 Latency (max)':<16} "
          f"{'Ops Rate (avg)':<14} {'Total Operations':<16} {'Cumulative Duration':<18}")
    print("-" * 100)
    
    for pod in summary_report.get("pod_performance_summary", []):
        node_display = pod['node'][:24] + "..." if len(pod['node']) > 24 else pod['node']
        print(f"{pod['pod_name']:<30} {node_display:<25} {pod['p99_latency_avg']:<16} "
              f"{pod['p99_latency_max']:<16} {pod['ops_rate_avg']:<14} "
              f"{pod['total_operations']:<16} {pod['cumulative_duration']:<18}")
    
    # Print overall summary if available
    if "overall_summary" in summary_report:
        summary = summary_report["overall_summary"]
        print("\n" + "="*60)
        print("Overall Performance Summary")
        print("="*60)
        print(f"Performance Rating: {summary['performance_rating']}")
        print(f"Health Status: {summary['health_status']}")
        print(f"Max P99 Latency: {summary['max_p99_latency_ms']:.3f} ms")
        print(f"Avg P99 Latency: {summary['avg_p99_latency_ms']:.3f} ms")
        print(f"Max Ops/Sec: {summary['max_ops_per_sec']:.2f}")
        
        if summary.get("recommendations"):
            print("\nRecommendations:")
            for i, rec in enumerate(summary["recommendations"], 1):
                print(f"  {i}. {rec}")
    
    print("="*80)
    