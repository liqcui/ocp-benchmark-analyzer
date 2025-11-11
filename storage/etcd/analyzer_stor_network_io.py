#!/usr/bin/env python3
"""
ETCD Analyzer Storage Network I/O ELT Module
Handles network I/O metrics storage and retrieval in DuckDB using ELT patterns
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
import duckdb

from .etcd_analyzer_stor_utility import BaseStoreELT, StorageUtilityELT, TimeRangeUtilityELT

logger = logging.getLogger(__name__)


class NetworkIOStorELT(BaseStoreELT):
    """Network I/O metrics storage using DuckDB ELT patterns"""
    
    def __init__(self, db_path: str = "etcd_analyzer.duckdb"):
        super().__init__(db_path)
        
        # Pods metrics (formerly container metrics) - individual tables for each metric
        self.container_metrics = {
            'network_io_container_network_rx': 'pods_metrics.network_io_container_network_rx',
            'network_io_container_network_tx': 'pods_metrics.network_io_container_network_tx',
            'network_io_peer2peer_latency_p99': 'pods_metrics.network_io_peer2peer_latency_p99',
            'network_io_network_peer_received_bytes': 'pods_metrics.network_io_network_peer_received_bytes',
            'network_io_network_peer_sent_bytes': 'pods_metrics.network_io_network_peer_sent_bytes',
            'network_io_network_client_grpc_received_bytes': 'pods_metrics.network_io_network_client_grpc_received_bytes',
            'network_io_network_client_grpc_sent_bytes': 'pods_metrics.network_io_network_client_grpc_sent_bytes'
        }
        
        # Node metrics - individual tables for each metric
        self.node_metrics = {
            'network_io_node_network_rx_utilization': 'node_metrics.network_io_node_network_rx_utilization',
            'network_io_node_network_tx_utilization': 'node_metrics.network_io_node_network_tx_utilization',
            'network_io_node_network_rx_package': 'node_metrics.network_io_node_network_rx_package',
            'network_io_node_network_tx_package': 'node_metrics.network_io_node_network_tx_package',
            'network_io_node_network_rx_drop': 'node_metrics.network_io_node_network_rx_drop',
            'network_io_node_network_tx_drop': 'node_metrics.network_io_node_network_tx_drop'
        }
        
        # Cluster metrics - individual tables for each metric
        self.cluster_metrics = {
            'network_io_grpc_active_watch_streams': 'cluster_metrics.network_io_grpc_active_watch_streams',
            'network_io_grpc_active_lease_streams': 'cluster_metrics.network_io_grpc_active_lease_streams'
        }
        
        # Summary table
        self.summary_table = 'network_io_summary'
    
    async def _create_tables(self):
        """Create network I/O related tables - individual tables for each metric"""
        if not self.conn:
            await self.initialize()
        
        # Create schemas
        schema_operations = [
            ("CREATE SCHEMA IF NOT EXISTS pods_metrics", None),
            ("CREATE SCHEMA IF NOT EXISTS node_metrics", None),
            ("CREATE SCHEMA IF NOT EXISTS cluster_metrics", None)
        ]
        
        # Container metrics tables - one table per metric
        container_table_operations = []
        index_operations = []
        for metric_name, table_name in self.container_metrics.items():
            sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                testing_id TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                pod_name TEXT NOT NULL,
                node_name TEXT,
                avg_value DOUBLE,
                max_value DOUBLE,
                unit TEXT,
                timestamp TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            container_table_operations.append((sql, None))
            # Indexes for container tables
            index_operations.append((f"CREATE INDEX IF NOT EXISTS idx_{metric_name}_testing_id ON {table_name}(testing_id)", None))
            index_operations.append((f"CREATE INDEX IF NOT EXISTS idx_{metric_name}_pod ON {table_name}(pod_name)", None))
        
        # Node metrics tables - one table per metric
        node_table_operations = []
        for metric_name, table_name in self.node_metrics.items():
            sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                testing_id TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                node_name TEXT NOT NULL,
                avg_value DOUBLE,
                max_value DOUBLE,
                device_count INTEGER,
                unit TEXT,
                timestamp TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            node_table_operations.append((sql, None))
            # Indexes for node tables
            index_operations.append((f"CREATE INDEX IF NOT EXISTS idx_{metric_name}_testing_id ON {table_name}(testing_id)", None))
            index_operations.append((f"CREATE INDEX IF NOT EXISTS idx_{metric_name}_node ON {table_name}(node_name)", None))
        
        # Cluster metrics tables - one table per metric
        cluster_table_operations = []
        for metric_name, table_name in self.cluster_metrics.items():
            sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                testing_id TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                avg_value DOUBLE,
                max_value DOUBLE,
                unit TEXT,
                timestamp TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cluster_table_operations.append((sql, None))
            # Index for cluster tables
            index_operations.append((f"CREATE INDEX IF NOT EXISTS idx_{metric_name}_testing_id ON {table_name}(testing_id)", None))
        
        # Summary table
        summary_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.summary_table} (
            id TEXT PRIMARY KEY,
            testing_id TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            status TEXT,
            max_value DOUBLE,
            avg_value DOUBLE,
            count_value INTEGER,
            unit TEXT,
            level TEXT,
            summary_data JSON,
            timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        # Summary table indexes
        index_operations.append((f"CREATE INDEX IF NOT EXISTS idx_summary_testing_id ON {self.summary_table}(testing_id)", None))
        index_operations.append((f"CREATE INDEX IF NOT EXISTS idx_summary_metric ON {self.summary_table}(metric_name)", None))
        
        # Combine all operations
        all_operations = (
            schema_operations + 
            container_table_operations + 
            node_table_operations + 
            cluster_table_operations + 
            [(summary_table_sql, None)] +
            index_operations
        )
        
        try:
            self._execute_with_transaction(all_operations)
            logger.info("Network I/O tables and schemas created successfully")
        except Exception as e:
            logger.error(f"Failed to create network I/O tables: {str(e)}")
            raise
    
    async def store_network_io_metrics(self, testing_id: str, network_io_data: Dict[str, Any]) -> Dict[str, Any]:
        """Store network I/O metrics data from MCP response"""
        try:
            if not StorageUtilityELT.validate_testing_id(testing_id):
                return {"status": "error", "error": "Invalid testing_id"}
            
            if not self._initialized:
                await self.initialize()
            
            data_section = network_io_data.get("data", {}).get("data", {})
            timestamp = StorageUtilityELT.parse_timestamp(
                network_io_data.get("data", {}).get("timestamp")
            ) or StorageUtilityELT.current_timestamp()
            
            storage_results = {}
            
            # Store pods metrics (backward compatible with container_metrics key)
            container_result = await self._store_container_metrics(
                testing_id, data_section.get("pods_metrics", data_section.get("container_metrics", {})), timestamp
            )
            storage_results["pods_metrics"] = container_result
            
            # Store node metrics
            node_result = await self._store_node_metrics(
                testing_id, data_section.get("node_metrics", {}), timestamp
            )
            storage_results["node_metrics"] = node_result
            
            # Store cluster metrics
            cluster_result = await self._store_cluster_metrics(
                testing_id, data_section.get("cluster_metrics", {}), timestamp
            )
            storage_results["cluster_metrics"] = cluster_result
            
            # Generate and store summary
            summary_result = await self._generate_and_store_summary(testing_id, timestamp)
            storage_results["summary"] = summary_result
            
            return {
                "status": "success",
                "testing_id": testing_id,
                "timestamp": timestamp.isoformat(),
                "storage_results": storage_results
            }
            
        except Exception as e:
            logger.error(f"Error storing network I/O metrics: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _store_container_metrics(self, testing_id: str, container_data: Dict[str, Any], timestamp: datetime) -> Dict[str, Any]:
        """Store container/pod level metrics in individual tables"""
        try:
            operations = []
            stored_count = 0
            
            for metric_name, metric_info in container_data.items():
                if metric_info.get("status") != "success" or metric_name not in self.container_metrics:
                    continue
                
                table_name = self.container_metrics[metric_name]
                pods = metric_info.get("pods", {})
                unit = metric_info.get("unit", "")
                # removed query_expression handling
                
                for pod_name, pod_stats in pods.items():
                    record_id = StorageUtilityELT.generate_uuid()
                    
                    insert_sql = f"""
                    INSERT OR REPLACE INTO {table_name}
                    (id, testing_id, metric_name, pod_name, node_name, avg_value, max_value, unit, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    params = (
                        record_id,
                        testing_id,
                        metric_name,
                        pod_name,
                        pod_stats.get("node", "unknown"),
                        pod_stats.get("avg"),
                        pod_stats.get("max"),
                        unit,
                        timestamp
                    )
                    
                    operations.append((insert_sql, params))
                    stored_count += 1
            
            if operations:
                self._execute_with_transaction(operations)
            
            return {"status": "success", "records_stored": stored_count}
            
        except Exception as e:
            logger.error(f"Error storing container metrics: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _store_node_metrics(self, testing_id: str, node_data: Dict[str, Any], timestamp: datetime) -> Dict[str, Any]:
        """Store node level metrics in individual tables"""
        try:
            operations = []
            stored_count = 0
            
            for metric_name, metric_info in node_data.items():
                if metric_info.get("status") != "success" or metric_name not in self.node_metrics:
                    continue
                
                table_name = self.node_metrics[metric_name]
                nodes = metric_info.get("nodes", {})
                unit = metric_info.get("unit", "")
                # removed query_expression handling
                
                for node_name, node_stats in nodes.items():
                    record_id = StorageUtilityELT.generate_uuid()
                    
                    insert_sql = f"""
                    INSERT OR REPLACE INTO {table_name}
                    (id, testing_id, metric_name, node_name, avg_value, max_value, device_count, unit, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    params = (
                        record_id,
                        testing_id,
                        metric_name,
                        node_name,
                        node_stats.get("avg"),
                        node_stats.get("max"),
                        node_stats.get("device_count"),
                        unit,
                        timestamp
                    )
                    
                    operations.append((insert_sql, params))
                    stored_count += 1
            
            if operations:
                self._execute_with_transaction(operations)
            
            return {"status": "success", "records_stored": stored_count}
            
        except Exception as e:
            logger.error(f"Error storing node metrics: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _store_cluster_metrics(self, testing_id: str, cluster_data: Dict[str, Any], timestamp: datetime) -> Dict[str, Any]:
        """Store cluster level metrics in individual tables"""
        try:
            operations = []
            stored_count = 0
            
            for metric_name, metric_info in cluster_data.items():
                if metric_info.get("status") != "success" or metric_name not in self.cluster_metrics:
                    continue
                
                table_name = self.cluster_metrics[metric_name]
                record_id = StorageUtilityELT.generate_uuid()
                unit = metric_info.get("unit", "")
                # removed query_expression handling
                
                insert_sql = f"""
                INSERT OR REPLACE INTO {table_name}
                (id, testing_id, metric_name, avg_value, max_value, unit, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                
                params = (
                    record_id,
                    testing_id,
                    metric_name,
                    metric_info.get("avg"),
                    metric_info.get("max"),
                    unit,
                    timestamp
                )
                
                operations.append((insert_sql, params))
                stored_count += 1
            
            if operations:
                self._execute_with_transaction(operations)
            
            return {"status": "success", "records_stored": stored_count}
            
        except Exception as e:
            logger.error(f"Error storing cluster metrics: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _generate_and_store_summary(self, testing_id: str, timestamp: datetime) -> Dict[str, Any]:
        """Generate summary from individual metric tables and save to summary table"""
        try:
            operations = []
            
            # removed metric titles mapping
            
            # Generate pods metrics summary from individual tables
            for metric_name, table_name in self.container_metrics.items():
                try:
                    summary = self._generate_individual_metric_summary(table_name, testing_id, metric_name)
                    if summary:
                        summary_id = StorageUtilityELT.generate_uuid()
                        
                        insert_sql = f"""
                        INSERT OR REPLACE INTO {self.summary_table}
                        (id, testing_id, metric_name, status, max_value, avg_value, count_value, unit, level, summary_data, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        params = (
                            summary_id,
                            testing_id,
                            metric_name,
                            summary['status'],
                            summary.get('max_value'),
                            summary.get('avg_value'),
                            summary.get('count'),
                            summary.get('unit'),
                            'container',
                            StorageUtilityELT.serialize_json(summary),
                            timestamp
                        )
                        
                        operations.append((insert_sql, params))
                except Exception as e:
                    logger.warning(f"Error generating summary for {metric_name}: {str(e)}")
            
            # Generate node metrics summary from individual tables
            for metric_name, table_name in self.node_metrics.items():
                try:
                    summary = self._generate_individual_metric_summary(table_name, testing_id, metric_name)
                    if summary:
                        summary_id = StorageUtilityELT.generate_uuid()
                        
                        insert_sql = f"""
                        INSERT OR REPLACE INTO {self.summary_table}
                        (id, testing_id, metric_name, status, max_value, avg_value, count_value, unit, level, summary_data, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        params = (
                            summary_id,
                            testing_id,
                            metric_name,
                            summary['status'],
                            summary.get('max_value'),
                            summary.get('avg_value'),
                            summary.get('count'),
                            summary.get('unit'),
                            'node',
                            StorageUtilityELT.serialize_json(summary),
                            timestamp
                        )
                        
                        operations.append((insert_sql, params))
                except Exception as e:
                    logger.warning(f"Error generating summary for {metric_name}: {str(e)}")
            
            # Generate cluster metrics summary from individual tables
            for metric_name, table_name in self.cluster_metrics.items():
                try:
                    summary = self._generate_individual_metric_summary(table_name, testing_id, metric_name)
                    if summary:
                        summary_id = StorageUtilityELT.generate_uuid()
                        
                        insert_sql = f"""
                        INSERT OR REPLACE INTO {self.summary_table}
                        (id, testing_id, metric_name, status, max_value, avg_value, count_value, unit, level, summary_data, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        params = (
                            summary_id,
                            testing_id,
                            metric_name,
                            summary['status'],
                            summary.get('max_value'),
                            summary.get('avg_value'),
                            summary.get('count'),
                            summary.get('unit'),
                            'cluster',
                            StorageUtilityELT.serialize_json(summary),
                            timestamp
                        )
                        
                        operations.append((insert_sql, params))
                except Exception as e:
                    logger.warning(f"Error generating summary for {metric_name}: {str(e)}")
            
            if operations:
                self._execute_with_transaction(operations)
            
            return {"status": "success", "summary_records": len(operations)}
            
        except Exception as e:
            logger.error(f"Error generating summary: {str(e)}")
            return {"status": "error", "error": str(e)}

    def _generate_individual_metric_summary(self, table_name: str, testing_id: str, metric_name: str) -> Optional[Dict[str, Any]]:
        """Generate summary statistics for an individual metric table"""
        try:
            query = f"""
            SELECT 
                unit,
                COUNT(*) as count,
                MAX(max_value) as max_value,
                AVG(avg_value) as avg_value
            FROM {table_name}
            WHERE testing_id = ? AND avg_value IS NOT NULL
            GROUP BY unit
            """
            
            result = self.conn.execute(query, (testing_id,)).fetchone()
            
            if result:
                return {
                    'status': 'success',
                    'unit': result[0],
                    'count': result[1],
                    'max_value': result[2],
                    'avg_value': result[3]
                }
            return None
            
        except Exception as e:
            logger.warning(f"Error generating individual summary for {metric_name}: {str(e)}")
            return None             
            
    async def get_network_io_summary(self, testing_id: str) -> Dict[str, Any]:
        """Get network I/O summary for a testing session"""
        try:
            if not self._initialized:
                await self.initialize()
            
            query = f"""
            SELECT metric_name, status, max_value, avg_value, count_value, unit, level
            FROM {self.summary_table}
            WHERE testing_id = ?
            ORDER BY level, metric_name
            """
            
            results = self.conn.execute(query, (testing_id,)).fetchall()
            
            if not results:
                return {
                    "status": "not_found",
                    "message": f"No network I/O summary found for testing_id: {testing_id}"
                }
            
            summary_data = {
                "status": "success",
                "testing_id": testing_id,
                "timestamp": StorageUtilityELT.current_timestamp().isoformat(),
                "network_metrics_overview": []
            }
            
            for row in results:
                metric_info = {
                    "metric_name": row[0],
                    "status": row[1] or "success",
                    "max_value": row[2] if row[2] is not None else "NaN", 
                    "avg_value": row[3] if row[3] is not None else "NaN",
                    "count": row[4] if row[4] is not None else "NaN"
                }
                summary_data["network_metrics_overview"].append(metric_info)
            
            return summary_data
            
        except Exception as e:
            logger.error(f"Error getting network I/O summary: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def query_network_io_data_by_duration(self, duration: str) -> Dict[str, Any]:
        """Query network I/O data from individual tables by duration"""
        try:
            if not self._initialized:
                await self.initialize()
            
            # Convert duration to hours and compute cutoff timestamp
            hours = self._parse_duration_to_hours(duration)
            cutoff_time = StorageUtilityELT.current_timestamp() - timedelta(hours=hours)
            
            results = {
                "status": "success",
                "duration": duration,
                "query_timestamp": StorageUtilityELT.current_timestamp().isoformat(),
                "pods_metrics": {},
                "container_metrics": {},
                "node_metrics": {},
                "cluster_metrics": {}
            }
            
            # Query pods metrics from individual tables
            for metric_name, table_name in self.container_metrics.items():
                try:
                    query = f"""
                    SELECT testing_id, pod_name, node_name, avg_value, max_value, unit, timestamp
                    FROM {table_name}
                    WHERE timestamp >= ?
                    ORDER BY timestamp DESC, pod_name
                    """
                    rows = self.conn.execute(query, (cutoff_time,)).fetchall()
                    results["pods_metrics"][metric_name] = [
                        {
                            "testing_id": row[0],
                            "pod_name": row[1],
                            "node_name": row[2],
                            "avg_value": row[3],
                            "max_value": row[4],
                            "unit": row[5],
                            "timestamp": row[6].isoformat() if row[6] else None
                        }
                        for row in rows
                    ]
                except Exception as e:
                    logger.warning(f"Error querying {metric_name}: {str(e)}")
                    results["pods_metrics"][metric_name] = []
            
            # Query node metrics from individual tables
            for metric_name, table_name in self.node_metrics.items():
                try:
                    query = f"""
                    SELECT testing_id, node_name, avg_value, max_value, device_count, unit, timestamp
                    FROM {table_name}
                    WHERE timestamp >= ?
                    ORDER BY timestamp DESC, node_name
                    """
                    rows = self.conn.execute(query, (cutoff_time,)).fetchall()
                    results["node_metrics"][metric_name] = [
                        {
                            "testing_id": row[0],
                            "node_name": row[1],
                            "avg_value": row[2],
                            "max_value": row[3],
                            "device_count": row[4],
                            "unit": row[5],
                            "timestamp": row[6].isoformat() if row[6] else None
                        }
                        for row in rows
                    ]
                except Exception as e:
                    logger.warning(f"Error querying {metric_name}: {str(e)}")
                    results["node_metrics"][metric_name] = []
            
            # Query cluster metrics from individual tables
            for metric_name, table_name in self.cluster_metrics.items():
                try:
                    query = f"""
                    SELECT testing_id, avg_value, max_value, unit, timestamp
                    FROM {table_name}
                    WHERE timestamp >= ?
                    ORDER BY timestamp DESC
                    """
                    rows = self.conn.execute(query, (cutoff_time,)).fetchall()
                    results["cluster_metrics"][metric_name] = [
                        {
                            "testing_id": row[0],
                            "avg_value": row[1],
                            "max_value": row[2],
                            "unit": row[3],
                            "timestamp": row[4].isoformat() if row[4] else None
                        }
                        for row in rows
                    ]
                except Exception as e:
                    logger.warning(f"Error querying {metric_name}: {str(e)}")
                    results["cluster_metrics"][metric_name] = []
            
            return results
            
        except Exception as e:
            logger.error(f"Error querying network I/O data by duration: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def query_network_io_data_by_time_range(self, start_time: str, end_time: str) -> Dict[str, Any]:
        """Query network I/O data from individual tables by time range (UTC)"""
        try:
            if not self._initialized:
                await self.initialize()
            
            start_dt, end_dt = TimeRangeUtilityELT.parse_utc_time_range(start_time, end_time)
            
            if not start_dt or not end_dt:
                return {"status": "error", "error": "Invalid time range format"}
            
            results = {
                "status": "success",
                "start_time": start_time,
                "end_time": end_time,
                "query_timestamp": StorageUtilityELT.current_timestamp().isoformat(),
                "pods_metrics": {},
                "container_metrics": {},
                "node_metrics": {},
                "cluster_metrics": {}
            }
            
            # Query pods metrics from individual tables
            for metric_name, table_name in self.container_metrics.items():
                try:
                    query = f"""
                    SELECT testing_id, pod_name, node_name, avg_value, max_value, unit, timestamp
                    FROM {table_name}
                    WHERE timestamp BETWEEN ? AND ?
                    ORDER BY timestamp DESC, pod_name
                    """
                    rows = self.conn.execute(query, (start_dt, end_dt)).fetchall()
                    results["pods_metrics"][metric_name] = [
                        {
                            "testing_id": row[0],
                            "pod_name": row[1],
                            "node_name": row[2],
                            "avg_value": row[3],
                            "max_value": row[4],
                            "unit": row[5],
                            "timestamp": row[6].isoformat() if row[6] else None
                        }
                        for row in rows
                    ]
                except Exception as e:
                    logger.warning(f"Error querying {metric_name}: {str(e)}")
                    results["pods_metrics"][metric_name] = []
            
            # Query node metrics from individual tables
            for metric_name, table_name in self.node_metrics.items():
                try:
                    query = f"""
                    SELECT testing_id, node_name, avg_value, max_value, device_count, unit, timestamp
                    FROM {table_name}
                    WHERE timestamp BETWEEN ? AND ?
                    ORDER BY timestamp DESC, node_name
                    """
                    rows = self.conn.execute(query, (start_dt, end_dt)).fetchall()
                    results["node_metrics"][metric_name] = [
                        {
                            "testing_id": row[0],
                            "node_name": row[1],
                            "avg_value": row[2],
                            "max_value": row[3],
                            "device_count": row[4],
                            "unit": row[5],
                            "timestamp": row[6].isoformat() if row[6] else None
                        }
                        for row in rows
                    ]
                except Exception as e:
                    logger.warning(f"Error querying {metric_name}: {str(e)}")
                    results["node_metrics"][metric_name] = []
            
            # Query cluster metrics from individual tables
            for metric_name, table_name in self.cluster_metrics.items():
                try:
                    query = f"""
                    SELECT testing_id, avg_value, max_value, unit, timestamp
                    FROM {table_name}
                    WHERE timestamp BETWEEN ? AND ?
                    ORDER BY timestamp DESC
                    """
                    rows = self.conn.execute(query, (start_dt, end_dt)).fetchall()
                    results["cluster_metrics"][metric_name] = [
                        {
                            "testing_id": row[0],
                            "avg_value": row[1],
                            "max_value": row[2],
                            "unit": row[3],
                            "timestamp": row[4].isoformat() if row[4] else None
                        }
                        for row in rows
                    ]
                except Exception as e:
                    logger.warning(f"Error querying {metric_name}: {str(e)}")
                    results["cluster_metrics"][metric_name] = []
            
            return results
            
        except Exception as e:
            logger.error(f"Error querying network I/O data by time range: {str(e)}")
    
    def _format_container_results(self, results: List[Tuple]) -> List[Dict[str, Any]]:
        """Format container query results - kept for compatibility"""
        return [
            {
                "testing_id": row[0],
                "metric_name": row[1],
                "pod_name": row[2],
                "node_name": row[3],
                "avg_value": row[4],
                "max_value": row[5],
                "unit": row[6],
                "timestamp": row[7].isoformat() if row[7] else None
            }
            for row in results
        ]
    
    def _format_node_results(self, results: List[Tuple]) -> List[Dict[str, Any]]:
        """Format node query results - kept for compatibility"""
        return [
            {
                "testing_id": row[0],
                "metric_name": row[1],
                "node_name": row[2],
                "avg_value": row[3],
                "max_value": row[4],
                "device_count": row[5],
                "unit": row[6],
                "timestamp": row[7].isoformat() if row[7] else None
            }
            for row in results
        ]
    
    def _format_cluster_results(self, results: List[Tuple]) -> List[Dict[str, Any]]:
        """Format cluster query results - kept for compatibility"""
        return [
            {
                "testing_id": row[0],
                "metric_name": row[1],
                "avg_value": row[2],
                "max_value": row[3],
                "unit": row[4],
                "timestamp": row[5].isoformat() if row[5] else None
            }
            for row in results
        ]
    
    def _parse_duration_to_hours(self, duration: str) -> float:
        """Parse duration string to hours"""
        try:
            duration = duration.lower().strip()
            if duration.endswith('h'):
                return float(duration[:-1])
            elif duration.endswith('m'):
                return float(duration[:-1]) / 60.0
            elif duration.endswith('d'):
                return float(duration[:-1]) * 24.0
            else:
                # Default to 1 hour
                return 1.0
        except:
            return 1.0
    
    async def get_table_info(self) -> Dict[str, List[str]]:
        """Get table schema information for all individual metric tables"""
        try:
            if not self._initialized:
                await self.initialize()
            
            table_info = {}
            
            # Get container metric table info
            for metric_name, table_name in self.container_metrics.items():
                columns = StorageUtilityELT.get_table_columns(self.conn, table_name)
                table_info[table_name] = columns
            
            # Get node metric table info
            for metric_name, table_name in self.node_metrics.items():
                columns = StorageUtilityELT.get_table_columns(self.conn, table_name)
                table_info[table_name] = columns
            
            # Get cluster metric table info
            for metric_name, table_name in self.cluster_metrics.items():
                columns = StorageUtilityELT.get_table_columns(self.conn, table_name)
                table_info[table_name] = columns
            
            # Get summary table info
            columns = StorageUtilityELT.get_table_columns(self.conn, self.summary_table)
            table_info[self.summary_table] = columns
            
            return table_info
            
        except Exception as e:
            logger.error(f"Error getting table info: {str(e)}")
            return {}

def print_network_io_summary_tables(summary_data: Dict[str, Any]):
    """Print network I/O summary tables in terminal with readable formatting"""
    try:
        if summary_data.get("status") != "success":
            print(f"Network I/O Summary Error: {summary_data.get('error', 'Unknown error')}")
            return
        
        print("\n" + "="*80)
        print("ETCD Network I/O Metrics Overview")
        print("="*80)
        
        metrics = summary_data.get("network_metrics_overview", [])
        if not metrics:
            print("No network metrics data available")
            return
        
        # Table header
        print(f"{'metric_name':<35} {'status':<10} {'max_value':<15} {'avg_value':<15} {'count':<10}")
        print("-" * 100)
        
        # Format numeric values function with human-readable units
        def format_readable_value(val, metric_name=""):
            if val == 'NaN' or val is None:
                return 'NaN'
            
            try:
                # Convert to float if it's a string
                if isinstance(val, str) and val != 'NaN':
                    val = float(val)
                
                if not isinstance(val, (int, float)):
                    return str(val)
                
                # Handle different metric types with appropriate formatting
                if 'bytes' in metric_name.lower():
                    # Format bytes values
                    if val >= 1024**3:  # GB
                        return f"{val/1024**3:.1f}GB"
                    elif val >= 1024**2:  # MB
                        return f"{val/1024**2:.1f}MB"
                    elif val >= 1024:  # KB
                        return f"{val/1024:.1f}KB"
                    else:
                        return f"{val:.0f}B"
                        
                elif 'utilization' in metric_name.lower():
                    # Format utilization values (assume bits/sec)
                    if val >= 1000**3:  # Gbps
                        return f"{val/1000**3:.1f}Gbps"
                    elif val >= 1000**2:  # Mbps
                        return f"{val/1000**2:.1f}Mbps"
                    elif val >= 1000:  # Kbps
                        return f"{val/1000:.1f}Kbps"
                    else:
                        return f"{val:.0f}bps"
                        
                elif 'latency' in metric_name.lower():
                    # Format latency values (assume seconds)
                    if val >= 1:
                        return f"{val:.2f}s"
                    elif val >= 0.001:
                        return f"{val*1000:.1f}ms"
                    else:
                        return f"{val*1000000:.0f}μs"
                        
                elif 'package' in metric_name.lower() or 'streams' in metric_name.lower():
                    # Format packet/stream counts
                    if val >= 1000000:
                        return f"{val/1000000:.1f}M"
                    elif val >= 1000:
                        return f"{val/1000:.1f}K"
                    else:
                        return f"{val:.0f}"
                        
                elif 'drop' in metric_name.lower():
                    # Format drop rates
                    if val < 1:
                        return f"{val*100:.2f}%"
                    else:
                        return f"{val:.0f}"
                        
                elif 'network_rx' in metric_name.lower() or 'network_tx' in metric_name.lower():
                    # Format network rx/tx (likely bytes/sec)
                    if val >= 1024**2:  # MB/s
                        return f"{val/1024**2:.1f}MB/s"
                    elif val >= 1024:  # KB/s
                        return f"{val/1024:.1f}KB/s"
                    else:
                        return f"{val:.0f}B/s"
                        
                else:
                    # Generic formatting for other metrics
                    if val >= 1000000:
                        return f"{val/1000000:.1f}M"
                    elif val >= 1000:
                        return f"{val/1000:.1f}K"
                    else:
                        return f"{val:.2f}"
                        
            except (ValueError, TypeError):
                return str(val)
        
        # Table rows with formatted values
        for metric in metrics:
            metric_name = metric.get('metric_name', 'unknown')
            status = metric.get('status', 'unknown')
            max_val = metric.get('max_value', 'NaN')
            avg_val = metric.get('avg_value', 'NaN')
            count = metric.get('count', 'NaN')
            
            # Format count as simple integer
            try:
                count_formatted = f"{int(float(count))}" if count != 'NaN' and count is not None else 'NaN'
            except:
                count_formatted = str(count)
            
            # Format all values using the readable format function
            max_formatted = format_readable_value(max_val, metric_name)
            avg_formatted = format_readable_value(avg_val, metric_name)
            
            print(f"{metric_name:<35} {status:<10} {max_formatted:<15} {avg_formatted:<15} {count_formatted:<10}")
        
        print("\n" + "="*80)
        print(f"Total Network Metrics Collected: {len(metrics)}")
        print(f"Analysis Timestamp: {summary_data.get('timestamp', 'N/A')}")
        print("="*80)
        
    except Exception as e:
        print(f"Error printing network I/O summary: {str(e)}")
