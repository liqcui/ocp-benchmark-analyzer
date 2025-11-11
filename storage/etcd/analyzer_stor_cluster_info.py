#!/usr/bin/env python3
"""
ETCD Analyzer Cluster Info Storage - DuckDB ELT Module (Optimized)
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import uuid
import duckdb

from .etcd_analyzer_stor_utility import BaseStoreELT, StorageUtilityELT, TimeRangeUtilityELT

logger = logging.getLogger(__name__)


class ClusterInfoStoreELT(BaseStoreELT):
    """Cluster Information Storage using DuckDB with ELT patterns"""
    
    async def _create_tables(self):
        """Create all necessary tables for cluster information"""
        
        table_definitions = {
            "cluster_info": """
                CREATE TABLE IF NOT EXISTS cluster_info (
                    testing_id VARCHAR PRIMARY KEY,
                    cluster_name VARCHAR,
                    cluster_version VARCHAR,
                    platform VARCHAR,
                    api_server_url VARCHAR,
                    total_nodes INTEGER,
                    namespaces_count INTEGER,
                    pods_count INTEGER,
                    services_count INTEGER,
                    secrets_count INTEGER,
                    configmaps_count INTEGER,
                    networkpolicies_count INTEGER,
                    adminnetworkpolicies_count INTEGER,
                    baselineadminnetworkpolicies_count INTEGER,
                    egressfirewalls_count INTEGER,
                    egressips_count INTEGER,
                    clusteruserdefinednetworks_count INTEGER,
                    userdefinednetworks_count INTEGER,
                    collection_timestamp TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            "cluster_nodes": """
                CREATE TABLE IF NOT EXISTS cluster_nodes (
                    id VARCHAR PRIMARY KEY,
                    testing_id VARCHAR,
                    node_name VARCHAR,
                    node_type VARCHAR,
                    cpu_capacity VARCHAR,
                    memory_capacity VARCHAR,
                    architecture VARCHAR,
                    kernel_version VARCHAR,
                    container_runtime VARCHAR,
                    kubelet_version VARCHAR,
                    os_image VARCHAR,
                    ready_status VARCHAR,
                    schedulable BOOLEAN,
                    creation_timestamp TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (testing_id) REFERENCES cluster_info(testing_id)
                )
            """,
            
            "cluster_operators": """
                CREATE TABLE IF NOT EXISTS cluster_operators (
                    id VARCHAR PRIMARY KEY,
                    testing_id VARCHAR,
                    operator_name VARCHAR,
                    status VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (testing_id) REFERENCES cluster_info(testing_id)
                )
            """,
            
            "machine_config_pools": """
                CREATE TABLE IF NOT EXISTS machine_config_pools (
                    id VARCHAR PRIMARY KEY,
                    testing_id VARCHAR,
                    pool_name VARCHAR,
                    status VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (testing_id) REFERENCES cluster_info(testing_id)
                )
            """,
            
            "cluster_raw_data": """
                CREATE TABLE IF NOT EXISTS cluster_raw_data (
                    testing_id VARCHAR PRIMARY KEY,
                    raw_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (testing_id) REFERENCES cluster_info(testing_id)
                )
            """
        }
        
        # Execute all CREATE TABLE statements
        try:
            for table_name, sql in table_definitions.items():
                self.conn.execute(sql)
                logger.debug(f"Created/verified table: {table_name}")
            
            # Create indexes for better query performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_cluster_info_timestamp ON cluster_info(collection_timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_cluster_nodes_testing_id ON cluster_nodes(testing_id)",
                "CREATE INDEX IF NOT EXISTS idx_cluster_operators_testing_id ON cluster_operators(testing_id)",
                "CREATE INDEX IF NOT EXISTS idx_machine_config_pools_testing_id ON machine_config_pools(testing_id)",
                "CREATE INDEX IF NOT EXISTS idx_cluster_raw_data_testing_id ON cluster_raw_data(testing_id)"
            ]
            
            for index_sql in indexes:
                self.conn.execute(index_sql)
            
            logger.info("Created all cluster information tables and indexes")
            
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise

    async def initialize(self):
        """Initialize DuckDB connection and create tables automatically"""
        if self._initialized:
            return
            
        try:
            self.conn = duckdb.connect(self.db_path)
            await self._create_tables()
            self._initialized = True
            logger.info(f"Initialized DuckDB connection to {self.db_path} with cluster info tables")
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB: {str(e)}")
            raise

    async def ensure_tables_exist(self):
        """Ensure all required tables exist, create them if they don't"""
        if not self._initialized:
            await self.initialize()
            return
        
        try:
            # Check if main cluster_info table exists
            check_query = """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='cluster_info'
            """
            
            result = self.conn.execute(check_query).fetchone()
            
            if not result:
                logger.warning("Tables not found, recreating...")
                await self._create_tables()
            else:
                logger.debug("All required tables exist")
                
        except Exception as e:
            logger.warning(f"Error checking tables, recreating: {str(e)}")
            await self._create_tables()

    async def store_cluster_info(self, cluster_info: Dict[str, Any], testing_id: str) -> bool:
        """Store cluster information in DuckDB tables"""
        if not self._initialized:
            await self.initialize()
        
        # Ensure tables exist before storing data
        await self.ensure_tables_exist()
        
        if not StorageUtilityELT.validate_testing_id(testing_id):
            raise ValueError(f"Invalid testing_id: {testing_id}")
        
        try:
            operations = []
            
            # Prepare all store operations
            operations.extend(self._prepare_main_cluster_operations(cluster_info, testing_id))
            operations.extend(self._prepare_nodes_operations(cluster_info, testing_id))
            operations.extend(self._prepare_operators_operations(cluster_info, testing_id))
            operations.extend(self._prepare_mcp_operations(cluster_info, testing_id))
            operations.extend(self._prepare_raw_data_operations(cluster_info, testing_id))
            
            # Execute all operations in transaction
            self._execute_with_transaction(operations)
            
            # Print cluster_info table data in terminal
            await self._print_cluster_info_data(testing_id)
            
            logger.info(f"Successfully stored cluster info for testing_id: {testing_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store cluster info: {str(e)}")
            raise

    async def _print_cluster_info_data(self, testing_id: str):
        """Print cluster_info table data in the terminal"""
        try:
            query = """
            SELECT testing_id, cluster_name, cluster_version, platform, 
                   total_nodes, collection_timestamp, created_at
            FROM cluster_info 
            WHERE testing_id = ?
            """
            result = self.conn.execute(query, (testing_id,)).fetchone()
            
            if result:
                print("=== Cluster Info Data ===")
                print(f"Testing ID: {result[0]}")
                print(f"Cluster Name: {result[1]}")
                print(f"Cluster Version: {result[2]}")
                print(f"Platform: {result[3]}")
                print(f"Total Nodes: {result[4]}")
                print(f"Collection Timestamp: {result[5]}")
                print(f"Created At: {result[6]}")
                print("-" * 50)
            
        except Exception as e:
            logger.warning(f"Could not print cluster info data: {str(e)}")
    
    def _prepare_main_cluster_operations(self, cluster_info: Dict[str, Any], testing_id: str) -> List[tuple]:
        """Prepare operations for main cluster information"""
        insert_sql = """
        INSERT OR REPLACE INTO cluster_info (
            testing_id, cluster_name, cluster_version, platform, api_server_url,
            total_nodes, namespaces_count, pods_count, services_count, secrets_count,
            configmaps_count, networkpolicies_count, adminnetworkpolicies_count,
            baselineadminnetworkpolicies_count, egressfirewalls_count, egressips_count,
            clusteruserdefinednetworks_count, userdefinednetworks_count, collection_timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Extract values using utility function
        keys = [
            "cluster_name", "cluster_version", "platform", "api_server_url",
            "total_nodes", "namespaces_count", "pods_count", "services_count", 
            "secrets_count", "configmaps_count", "networkpolicies_count",
            "adminnetworkpolicies_count", "baselineadminnetworkpolicies_count",
            "egressfirewalls_count", "egressips_count", "clusteruserdefinednetworks_count",
            "userdefinednetworks_count"
        ]
        
        defaults = [
            "unknown", "unknown", "unknown", "",
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        ]
        
        extracted_values = StorageUtilityELT.extract_dict_values(cluster_info, keys, defaults)
        
        # Parse collection timestamp
        collection_ts = StorageUtilityELT.parse_timestamp(
            cluster_info.get("collection_timestamp")
        )
        
        values = (testing_id,) + extracted_values + (collection_ts,)
        
        return [(insert_sql, values)]
    
    def _prepare_nodes_operations(self, cluster_info: Dict[str, Any], testing_id: str) -> List[tuple]:
        """Prepare operations for nodes information"""
        operations = []
        
        insert_sql = """
        INSERT OR REPLACE INTO cluster_nodes (
            id, testing_id, node_name, node_type, cpu_capacity, memory_capacity,
            architecture, kernel_version, container_runtime, kubelet_version,
            os_image, ready_status, schedulable, creation_timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Process all node types
        for node_type in ["master_nodes", "infra_nodes", "worker_nodes"]:
            nodes = cluster_info.get(node_type, [])
            
            for node in nodes:
                node_name = StorageUtilityELT.safe_get(node, "name", f"unknown_{uuid.uuid4()}")
                node_id = StorageUtilityELT.create_composite_id(testing_id, node_name)
                
                # Extract node values
                node_keys = [
                    "node_type", "cpu_capacity", "memory_capacity", "architecture",
                    "kernel_version", "container_runtime", "kubelet_version",
                    "os_image", "ready_status", "schedulable"
                ]
                
                node_defaults = [
                    "unknown", "unknown", "unknown", "unknown",
                    "unknown", "unknown", "unknown", "unknown", "unknown", False
                ]
                
                node_values = StorageUtilityELT.extract_dict_values(node, node_keys, node_defaults)
                
                # Parse creation timestamp
                creation_ts = StorageUtilityELT.parse_timestamp(
                    node.get("creation_timestamp")
                )
                
                values = (node_id, testing_id, node_name) + node_values + (creation_ts,)
                operations.append((insert_sql, values))
        
        return operations
    
    def _prepare_operators_operations(self, cluster_info: Dict[str, Any], testing_id: str) -> List[tuple]:
        """Prepare operations for cluster operators information"""
        operations = []
        unavailable_operators = cluster_info.get("unavailable_cluster_operators", [])
        
        if unavailable_operators:
            insert_sql = """
            INSERT OR REPLACE INTO cluster_operators (id, testing_id, operator_name, status)
            VALUES (?, ?, ?, ?)
            """
            
            for operator in unavailable_operators:
                operator_id = StorageUtilityELT.create_composite_id(testing_id, operator)
                values = (operator_id, testing_id, operator, "unavailable")
                operations.append((insert_sql, values))
        
        return operations
    
    def _prepare_mcp_operations(self, cluster_info: Dict[str, Any], testing_id: str) -> List[tuple]:
        """Prepare operations for Machine Config Pool information"""
        operations = []
        mcp_status = cluster_info.get("mcp_status", {})
        
        if mcp_status:
            insert_sql = """
            INSERT OR REPLACE INTO machine_config_pools (id, testing_id, pool_name, status)
            VALUES (?, ?, ?, ?)
            """
            
            for pool_name, status in mcp_status.items():
                pool_id = StorageUtilityELT.create_composite_id(testing_id, pool_name)
                values = (pool_id, testing_id, pool_name, status)
                operations.append((insert_sql, values))
        
        return operations
    
    def _prepare_raw_data_operations(self, cluster_info: Dict[str, Any], testing_id: str) -> List[tuple]:
        """Prepare operations for raw JSON data storage"""
        insert_sql = """
        INSERT OR REPLACE INTO cluster_raw_data (testing_id, raw_json)
        VALUES (?, ?)
        """
        
        raw_json = StorageUtilityELT.serialize_json(cluster_info)
        return [(insert_sql, (testing_id, raw_json))]
    
    async def query_cluster_info_by_duration(self, duration: str = "1h") -> List[Dict[str, Any]]:
        """Query cluster info by duration from current time (UTC)"""
        if not self._initialized:
            await self.initialize()
        
        try:
            # Calculate time range based on duration
            current_time = StorageUtilityELT.current_timestamp()
            
            # Parse duration (simple implementation for common patterns)
            duration_mapping = {
                "1h": 3600, "2h": 7200, "6h": 21600, "12h": 43200,
                "24h": 86400, "1d": 86400, "2d": 172800, "7d": 604800
            }
            
            seconds = duration_mapping.get(duration.lower(), 3600)  # Default to 1 hour
            start_time = datetime.fromtimestamp(
                current_time.timestamp() - seconds, tz=timezone.utc
            )
            
            query = """
            SELECT testing_id, cluster_name, cluster_version, platform, 
                   total_nodes, collection_timestamp, created_at
            FROM cluster_info 
            WHERE collection_timestamp >= ?
            ORDER BY collection_timestamp DESC
            """
            
            results = self.conn.execute(query, (start_time,)).fetchall()
            columns = ['testing_id', 'cluster_name', 'cluster_version', 'platform', 
                      'total_nodes', 'collection_timestamp', 'created_at']
            
            return StorageUtilityELT.rows_to_dicts(results, columns)
            
        except Exception as e:
            logger.error(f"Failed to query cluster info by duration: {str(e)}")
            return []

    async def query_cluster_info_by_time_range(self, start_time: str, end_time: str) -> List[Dict[str, Any]]:
        """Query cluster info by time range (UTC timezone)"""
        if not self._initialized:
            await self.initialize()
        
        try:
            # Validate and parse time range
            validation = TimeRangeUtilityELT.validate_time_range(start_time, end_time)
            if not validation["valid"]:
                logger.error(f"Invalid time range: {validation['error']}")
                return []
            
            start_dt = validation["start_datetime"]
            end_dt = validation["end_datetime"]
            
            query = """
            SELECT testing_id, cluster_name, cluster_version, platform, 
                   total_nodes, collection_timestamp, created_at
            FROM cluster_info 
            WHERE collection_timestamp >= ? AND collection_timestamp <= ?
            ORDER BY collection_timestamp DESC
            """
            
            results = self.conn.execute(query, (start_dt, end_dt)).fetchall()
            columns = ['testing_id', 'cluster_name', 'cluster_version', 'platform', 
                      'total_nodes', 'collection_timestamp', 'created_at']
            
            return StorageUtilityELT.rows_to_dicts(results, columns)
            
        except Exception as e:
            logger.error(f"Failed to query cluster info by time range: {str(e)}")
            return []

    async def get_cluster_info(self, testing_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve cluster information by testing_id"""
        if not self._initialized:
            await self.initialize()
        
        if not StorageUtilityELT.validate_testing_id(testing_id):
            return None
        
        try:
            # Get main cluster info
            cluster_result = self._get_cluster_basic_info(testing_id)
            if not cluster_result:
                return None
            
            # Get related information
            cluster_data = cluster_result
            cluster_data["nodes"] = self._get_cluster_nodes(testing_id)
            cluster_data["operators"] = self._get_cluster_operators(testing_id)
            cluster_data["mcp_status"] = self._get_mcp_status(testing_id)
            
            return cluster_data
            
        except Exception as e:
            logger.error(f"Failed to get cluster info: {str(e)}")
            raise
    
    def _get_cluster_basic_info(self, testing_id: str) -> Optional[Dict[str, Any]]:
        """Get basic cluster information"""
        cluster_query = "SELECT * FROM cluster_info WHERE testing_id = ?"
        cluster_result = self.conn.execute(cluster_query, (testing_id,)).fetchone()
        
        if not cluster_result:
            return None
        
        columns = StorageUtilityELT.get_table_columns(self.conn, "cluster_info")
        return StorageUtilityELT.row_to_dict(cluster_result, columns)
    
    def _get_cluster_nodes(self, testing_id: str) -> List[Dict[str, Any]]:
        """Get cluster nodes information"""
        nodes_query = "SELECT * FROM cluster_nodes WHERE testing_id = ?"
        nodes_results = self.conn.execute(nodes_query, (testing_id,)).fetchall()
        
        if not nodes_results:
            return []
        
        columns = StorageUtilityELT.get_table_columns(self.conn, "cluster_nodes")
        return StorageUtilityELT.rows_to_dicts(nodes_results, columns)
    
    def _get_cluster_operators(self, testing_id: str) -> List[Dict[str, str]]:
        """Get cluster operators information"""
        operators_query = """
        SELECT operator_name, status FROM cluster_operators WHERE testing_id = ?
        """
        operators_results = self.conn.execute(operators_query, (testing_id,)).fetchall()
        
        return [{"name": op[0], "status": op[1]} for op in operators_results]
    
    def _get_mcp_status(self, testing_id: str) -> Dict[str, str]:
        """Get Machine Config Pool status"""
        mcp_query = """
        SELECT pool_name, status FROM machine_config_pools WHERE testing_id = ?
        """
        mcp_results = self.conn.execute(mcp_query, (testing_id,)).fetchall()
        
        return {mcp[0]: mcp[1] for mcp in mcp_results}

    async def get_cluster_analysis_summary(self, testing_id: str) -> Optional[Dict[str, Any]]:
        """Get cluster analysis summary with flattened structure (max 5 levels)"""
        if not self._initialized:
            await self.initialize()
        
        if not StorageUtilityELT.validate_testing_id(testing_id):
            return None
        
        try:
            # Get basic cluster info
            basic_query = """
            SELECT cluster_name, cluster_version, platform, total_nodes,
                   namespaces_count, pods_count, services_count, collection_timestamp
            FROM cluster_info 
            WHERE testing_id = ?
            """
            
            basic_result = self.conn.execute(basic_query, (testing_id,)).fetchone()
            if not basic_result:
                return None
            
            # Get node counts by type
            node_counts = self._get_node_type_counts(testing_id)
            
            # Get operators count
            operators_count = self._get_unavailable_operators_count(testing_id)
            
            # Get MCP status summary
            mcp_summary = self._get_mcp_status_summary(testing_id)
            
            # Flatten structure to avoid deep nesting
            summary = {
                "testing_id": testing_id,
                "cluster_name": basic_result[0],
                "cluster_version": basic_result[1], 
                "platform": basic_result[2],
                "total_nodes": basic_result[3],
                "namespaces_count": basic_result[4],
                "pods_count": basic_result[5],
                "services_count": basic_result[6],
                "collection_timestamp": basic_result[7],
                "master_nodes_count": node_counts.get("master", 0),
                "worker_nodes_count": node_counts.get("worker", 0),
                "infra_nodes_count": node_counts.get("infra", 0),
                "unavailable_operators_count": operators_count,
                "mcp_pools_count": mcp_summary["total_pools"],
                "mcp_updated_count": mcp_summary["updated_pools"],
                "analysis_status": "success"
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get cluster analysis summary: {str(e)}")
            return {
                "testing_id": testing_id,
                "analysis_status": "error",
                "error_message": str(e)
            }
    
    def _get_mcp_status_summary(self, testing_id: str) -> Dict[str, int]:
        """Get MCP status summary counts"""
        try:
            mcp_query = """
            SELECT status, COUNT(*) as count
            FROM machine_config_pools 
            WHERE testing_id = ? 
            GROUP BY status
            """
            
            mcp_results = self.conn.execute(mcp_query, (testing_id,)).fetchall()
            status_counts = {result[0]: result[1] for result in mcp_results}
            
            return {
                "total_pools": sum(status_counts.values()),
                "updated_pools": status_counts.get("Updated", 0),
                "updating_pools": status_counts.get("Updating", 0),
                "degraded_pools": status_counts.get("Degraded", 0)
            }
        except Exception as e:
            logger.warning(f"Error getting MCP status summary: {str(e)}")
            return {"total_pools": 0, "updated_pools": 0, "updating_pools": 0, "degraded_pools": 0}

    async def get_table_info(self) -> Dict[str, List[str]]:
        """Get information about all tables and their columns"""
        if not self._initialized:
            await self.initialize()
        
        try:
            tables = [
                "cluster_info", "cluster_nodes", "cluster_operators", 
                "machine_config_pools", "cluster_raw_data"
            ]
            
            table_info = {}
            for table in tables:
                columns = StorageUtilityELT.get_table_columns(self.conn, table)
                table_info[table] = columns
            
            return table_info
            
        except Exception as e:
            logger.error(f"Failed to get table info: {str(e)}")
            return {}

    async def get_table_schema_info(self) -> Dict[str, Dict[str, str]]:
        """Get detailed schema information for all cluster tables"""
        if not self._initialized:
            await self.initialize()
        
        try:
            tables = [
                "cluster_info", "cluster_nodes", "cluster_operators", 
                "machine_config_pools", "cluster_raw_data"
            ]
            
            schema_info = {}
            for table in tables:
                try:
                    schema_query = f"PRAGMA table_info({table})"
                    columns_result = self.conn.execute(schema_query).fetchall()
                    
                    columns_info = {}
                    for col_info in columns_result:
                        # col_info format: (cid, name, type, notnull, dflt_value, pk)
                        columns_info[col_info[1]] = {
                            "type": col_info[2],
                            "not_null": bool(col_info[3]),
                            "default": col_info[4],
                            "primary_key": bool(col_info[5])
                        }
                    
                    schema_info[table] = columns_info
                    
                except Exception as e:
                    logger.warning(f"Could not get schema for table {table}: {str(e)}")
                    schema_info[table] = {}
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get table schema info: {str(e)}")
            return {}

    async def verify_table_integrity(self) -> Dict[str, Any]:
        """Verify table integrity and return status"""
        if not self._initialized:
            await self.initialize()
        
        try:
            integrity_status = {
                "status": "success",
                "tables_checked": 0,
                "tables_valid": 0,
                "issues": []
            }
            
            tables = ["cluster_info", "cluster_nodes", "cluster_operators", 
                     "machine_config_pools", "cluster_raw_data"]
            
            for table in tables:
                try:
                    # Check if table exists and get row count
                    count_query = f"SELECT COUNT(*) FROM {table}"
                    count_result = self.conn.execute(count_query).fetchone()
                    
                    integrity_status["tables_checked"] += 1
                    integrity_status["tables_valid"] += 1
                    
                    logger.debug(f"Table {table}: {count_result[0]} rows")
                    
                except Exception as e:
                    integrity_status["issues"].append(f"Table {table}: {str(e)}")
                    logger.warning(f"Issue with table {table}: {str(e)}")
            
            if integrity_status["issues"]:
                integrity_status["status"] = "warning"
            
            return integrity_status
            
        except Exception as e:
            logger.error(f"Failed to verify table integrity: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "tables_checked": 0,
                "tables_valid": 0
            }
    
    async def list_testing_sessions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """List recent testing sessions"""
        if not self._initialized:
            await self.initialize()
        
        try:
            query = """
            SELECT testing_id, cluster_name, cluster_version, platform, 
                   total_nodes, collection_timestamp, created_at
            FROM cluster_info 
            ORDER BY created_at DESC 
            LIMIT ?
            """
            
            results = self.conn.execute(query, (limit,)).fetchall()
            columns = [desc[0] for desc in self.conn.description]
            
            return StorageUtilityELT.rows_to_dicts(results, columns)
            
        except Exception as e:
            logger.error(f"Failed to list testing sessions: {str(e)}")
            return []
    
    async def delete_testing_session(self, testing_id: str) -> bool:
        """Delete all data for a testing session"""
        if not self._initialized:
            await self.initialize()
        
        if not StorageUtilityELT.validate_testing_id(testing_id):
            return False
        
        try:
            # Prepare delete operations in reverse dependency order
            tables = [
                "cluster_raw_data",
                "machine_config_pools", 
                "cluster_operators",
                "cluster_nodes",
                "cluster_info"
            ]
            
            operations = []
            for table in tables:
                delete_sql = f"DELETE FROM {table} WHERE testing_id = ?"
                operations.append((delete_sql, (testing_id,)))
            
            # Execute all deletions in transaction
            self._execute_with_transaction(operations)
            
            logger.info(f"Deleted testing session: {testing_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete testing session {testing_id}: {str(e)}")
            return False

    def _get_node_type_counts(self, testing_id: str) -> Dict[str, int]:
        """Get node type counts for testing session"""
        nodes_query = """
        SELECT node_type, COUNT(*) as count
        FROM cluster_nodes 
        WHERE testing_id = ? 
        GROUP BY node_type
        """
        
        nodes_results = self.conn.execute(nodes_query, (testing_id,)).fetchall()
        return {result[0]: result[1] for result in nodes_results}
    
    def _get_unavailable_operators_count(self, testing_id: str) -> int:
        """Get count of unavailable operators"""
        operators_query = """
        SELECT COUNT(*) as unavailable_operators
        FROM cluster_operators 
        WHERE testing_id = ? AND status = 'unavailable'
        """
        
        operators_result = self.conn.execute(operators_query, (testing_id,)).fetchone()
        return operators_result[0] if operators_result else 0


# Example usage and testing functions
async def test_cluster_store():
    """Test the ClusterInfoStoreELT functionality"""
    
    # Test the storage functionality
    store = ClusterInfoStoreELT("test_etcd_analyzer_cluster.duckdb")
    
    try:
        # Initialize and store data
        await store.initialize()
        testing_id = StorageUtilityELT.generate_uuid()
        
        # Test duration-based query
        print("Testing duration-based query (1h)...")
        duration_results = await store.query_cluster_info_by_duration("1h")
        print(f"Duration query results: {len(duration_results)} records")
        
        # Test time range query
        print("Testing time range query...")
        start_time = "2025-09-14T00:00:00Z"
        end_time = "2025-09-14T23:59:59Z"
        range_results = await store.query_cluster_info_by_time_range(start_time, end_time)
        print(f"Time range query results: {len(range_results)} records")
        
        # Retrieve and verify data
        print("Retrieving stored data...")
        retrieved_data = await store.get_cluster_info(testing_id)
        print(f"Retrieved data keys: {retrieved_data.keys() if retrieved_data else 'None'}")
        
        # Get analysis summary
        print("Getting cluster analysis summary...")
        summary = await store.get_cluster_analysis_summary(testing_id)
        print(f"Analysis summary: {summary}")
        
        # Get table information
        print("Table information:")
        table_info = await store.get_table_info()
        for table_name, columns in table_info.items():
            print(f"  {table_name}: {len(columns)} columns")
        
        # Verify table integrity
        print("Verifying table integrity...")
        integrity = await store.verify_table_integrity()
        print(f"Integrity check: {integrity}")
        
        # List recent sessions
        print("Recent sessions:")
        sessions = await store.list_testing_sessions(5)
        for session in sessions:
            print(f"  Session: {session.get('testing_id', 'unknown')[:8]}... - {session.get('cluster_name', 'unknown')}")
        
    except Exception as e:
        print(f"Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        store.close()


if __name__ == "__main__":
    asyncio.run(test_cluster_store())