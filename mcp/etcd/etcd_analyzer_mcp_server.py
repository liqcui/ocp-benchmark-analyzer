#!/usr/bin/env python3
"""
OpenShift etcd Analyzer MCP Server
Main server implementation using FastMCP
"""

import os
import sys
import asyncio
import logging
import warnings
import subprocess
import shutil
from typing import Any, Dict, Optional, List
from datetime import datetime
import pytz

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set timezone to UTC
os.environ['TZ'] = 'UTC'

# Suppress deprecation warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"^websockets(\..*)?$")
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"^uvicorn\.protocols\.websockets(\..*)?$")
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message=r"HTTPResponse\.getheaders\(\) is deprecated"
)

# Ensure project root is on sys.path
try:
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
    if PROJECT_ROOT not in sys.path:
        sys.path.append(PROJECT_ROOT)
except Exception:
    pass

try:
    from fastmcp import FastMCP
    from pydantic import BaseModel, Field, ConfigDict
    import uvicorn
except ImportError as e:
    logger.error(f"Required dependencies not installed: {e}")
    logger.error("Please install: pip install fastmcp>=1.12.4 pydantic uvicorn")
    sys.exit(1)

# Import our modules
try:
    from ocauth.openshift_auth import OpenShiftAuth
    from tools.etcd.etcd_cluster_status import ClusterStatCollector
    from tools.etcd.etcd_general_info import GeneralInfoCollector
    from tools.etcd.etcd_disk_compact_defrag import CompactDefragCollector
    from tools.etcd.etcd_disk_wal_fsync import DiskWALFsyncCollector
    from tools.etcd.etcd_disk_backend_commit import DiskBackendCommitCollector
    from tools.etcd.etcd_network_io import NetworkIOCollector
    from tools.disk.disk_io import DiskIOCollector
    from tools.ocp.cluster_info import ClusterInfoCollector
    from tools.node.node_usage import nodeUsageCollector
    from config.metrics_config_reader import Config
except ImportError as e:
    logger.error(f"Failed to import local modules: {e}")
    logger.error("Please ensure all modules are in the correct directory structure")
    sys.exit(1)


# Pydantic models (keeping all existing models)
class MCPBaseModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class DurationInput(MCPBaseModel):
    duration: str = Field(default="1h", description="Time duration for metrics collection")

class ETCDClusterStatusResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str

class ETCDMetricsResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: Optional[str] = None
    duration: Optional[str] = None

class ETCDNodeUsageResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="node_usage")
    duration: str

class ETCDGeneralInfoResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="general_info")
    duration: str

class ETCDCompactDefragResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="disk_compact_defrag")
    duration: str

class ETCDWALFsyncResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="disk_wal_fsync")
    duration: str

class ETCDBackendCommitResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="disk_backend_commit")
    duration: str

class ETCDNetworkIOResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="network_io")
    duration: str

class ETCDDiskIOResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="disk_io")
    duration: str

class OCPClusterInfoResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str

class ServerHealthResponse(MCPBaseModel):
    status: str
    timestamp: str
    collectors_initialized: bool
    details: Dict[str, bool]

class ETCDPerformanceDeepDriveResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    analysis: Optional[Dict[str, Any]] = None
    summary: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="performance_deep_drive")
    duration: str
    test_id: Optional[str] = None

class DeepDriveInput(MCPBaseModel):
    duration: Optional[str] = Field(default="1h", description="Time range for metrics collection")

class ETCDBottleneckAnalysisResponse(MCPBaseModel):
    status: str
    bottleneck_analysis: Optional[Dict[str, Any]] = None
    root_cause_analysis: Optional[List[Dict[str, Any]]] = None
    performance_recommendations: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    timestamp: str
    duration: str
    test_id: Optional[str] = None

class ETCDPerformanceReportResponse(MCPBaseModel):
    status: str
    analysis_results: Optional[Dict[str, Any]] = None
    performance_report: Optional[str] = None
    error: Optional[str] = None
    timestamp: str
    duration: str
    test_id: Optional[str] = None

class PerformanceReportInput(MCPBaseModel):
    duration: Optional[str] = Field(default="1h", description="Time range for metrics collection")
    test_id: Optional[str] = Field(default=None, description="Optional test identifier")

# Helper function
def _duration_from_time_range(start_time_iso: Optional[str], end_time_iso: Optional[str]) -> Optional[str]:
    """Convert ISO start/end times into a duration string"""
    try:
        if not start_time_iso or not end_time_iso:
            return None
        start_dt = datetime.fromisoformat(start_time_iso.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_time_iso.replace("Z", "+00:00"))
        if end_dt <= start_dt:
            return None
        total_seconds = int((end_dt - start_dt).total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        if hours > 0 and minutes > 0:
            return f"{hours}h{minutes}m"
        if hours > 0:
            return f"{hours}h"
        if minutes > 0:
            return f"{minutes}m"
        return "1m"
    except Exception:
        return None

# Initialize MCP server
mcp = FastMCP("OpenShift etcd Analyzer")

# Global variables for collectors
auth_manager = None
config = None
cluster_collector = None
general_collector = None
compact_defrag_collector = None
wal_fsync_collector = None
backend_commit_collector = None
network_collector = None
disk_io_collector = None
cluster_info_collector = None
node_usage_collector = None

async def initialize_collectors():
    """Initialize all collectors with authentication - each loads its own metrics"""
    global auth_manager, config, cluster_collector, general_collector, compact_defrag_collector
    global wal_fsync_collector, backend_commit_collector, network_collector, disk_io_collector
    global cluster_info_collector, node_usage_collector

    try:
        logger.info("="*70)
        logger.info("Initializing OpenShift etcd Analyzer components...")
        logger.info("="*70)
        
        # Initialize global config for shared access using metrics-etcd.yml only                
        config = Config()
        metrics_etcd_file = os.path.join(PROJECT_ROOT, 'config', 'metrics-etcd.yml')
        metrics_disk_file = os.path.join(PROJECT_ROOT, 'config', 'metrics-disk.yml')
        #        
        # loaded_before = config.get_metrics_count()
        # if loaded_before == 0:
        #     metrics_etcd_file = os.path.join(PROJECT_ROOT, 'config', 'metrics-etcd.yml')
        #     metrics_disk_file = os.path.join(PROJECT_ROOT, 'config', 'metrics-disk.yml')
        #     load_result4disk = config.load_metrics_file(metrics_disk_file)
        #     load_result = config.load_metrics_file(metrics_etcd_file)
        #     total_after = config.get_metrics_count()
        #     if total_after > 0:
        #         file_summary = config.get_file_summary()
        #         files_descr = ", ".join(
        #             f"{v['file_name']}={v['metrics_count']}" for v in file_summary.values()
        #         ) or os.path.basename(metrics_etcd_file)
        #         logger.info(f"Metrics loaded: total={total_after}, files=[{files_descr}]")
        #     else:
        #         logger.warning(f"No metrics loaded: {load_result.get('error', 'no metrics found')}")
        # else:
        #     file_summary = config.get_file_summary()
        #     total = config.get_metrics_count()
        #     files_descr = ", ".join(f"{v['file_name']}={v['metrics_count']}" for v in file_summary.values())
        #     logger.info(f"Metrics preloaded: total={total}, files=[{files_descr}]")

        
        # Initialize OpenShift authentication (use kubeconfig from config if provided)
        logger.info("🔗 Initializing OpenShift authentication...")
        auth_manager = OpenShiftAuth(config.kubeconfig_path)
        await auth_manager.initialize()
        logger.info("✅ OpenShift authentication initialized successfully")
        logger.info("")

        # Initialize collectors - each will load and log their own category metrics
        logger.info("📊 Initializing metric collectors...")
        logger.info("-" * 70)
        
        # Cluster Status Collector
        logger.info("Initializing ClusterStatCollector...")
        cluster_collector = ClusterStatCollector(auth_manager, metrics_file_path=metrics_etcd_file)
        
        # General Info Collector
        logger.info("Initializing GeneralInfoCollector...")
        general_collector = GeneralInfoCollector(auth_manager, metrics_file_path=metrics_etcd_file)
        
        # Compact/Defrag Collector
        logger.info("Initializing CompactDefragCollector...")
        compact_defrag_collector = CompactDefragCollector(auth_manager, metrics_file_path=metrics_etcd_file)
        
        # WAL Fsync Collector
        logger.info("Initializing DiskWALFsyncCollector...")
        wal_fsync_collector = DiskWALFsyncCollector(auth_manager, metrics_file_path=metrics_etcd_file)
        
        # Backend Commit Collector
        logger.info("Initializing DiskBackendCommitCollector...")
        backend_commit_collector = DiskBackendCommitCollector(auth_manager, metrics_file_path=metrics_etcd_file)
        
        # Network IO Collector
        logger.info("Initializing NetworkIOCollector...")
        network_collector = NetworkIOCollector(auth_manager, metrics_file_path=metrics_etcd_file)
        
        # Disk IO Collector - uses metrics-disk.yml
        logger.info("Initializing DiskIOCollector...")
        disk_io_collector = DiskIOCollector(auth_manager, duration="1h", metrics_file_path=metrics_disk_file)
        
        # Node Usage Collector
        logger.info("Initializing nodeUsageCollector...")
        prometheus_config = {
            'url': auth_manager.prometheus_url,
            'token': getattr(auth_manager, 'prometheus_token', None),
            'verify_ssl': False
        }
        node_usage_collector = nodeUsageCollector(auth_manager, prometheus_config)
        # Note: nodeUsageCollector doesn't use etcd metrics, so no logging needed
        logger.info("✅ nodeUsageCollector initialized (uses node metrics)")
        
        # OCP Cluster Info Collector
        logger.info("Initializing ClusterInfoCollector...")
        cluster_info_collector = ClusterInfoCollector()
        await cluster_info_collector.initialize()
        logger.info("✅ ClusterInfoCollector initialized")
        
        logger.info("-" * 70)
        logger.info("✅ All collectors initialized successfully!")
        logger.info("="*70)
        logger.info("")
        
        return True
        
    except Exception as e:
        logger.error("="*70)
        logger.error(f"❌ Failed to initialize collectors: {e}")
        logger.error("="*70)
        return False

# MCP Tool Definitions
@mcp.tool()
async def get_server_health() -> ServerHealthResponse:
    """Get server health status and collector initialization status"""
    collectors_initialized = all([
        auth_manager is not None,
        config is not None,
        cluster_collector is not None,
        general_collector is not None,
        compact_defrag_collector is not None,
        wal_fsync_collector is not None,
        backend_commit_collector is not None,
        network_collector is not None,
        disk_io_collector is not None
    ])
    
    return ServerHealthResponse(
        status="healthy" if collectors_initialized else "unhealthy",
        timestamp=datetime.now(pytz.UTC).isoformat(),
        collectors_initialized=collectors_initialized,
        details={
            "auth_manager": auth_manager is not None,
            "config": config is not None,
            "cluster_collector": cluster_collector is not None,
            "general_collector": general_collector is not None,
            "compact_defrag_collector": compact_defrag_collector is not None,
            "wal_fsync_collector": wal_fsync_collector is not None,
            "backend_commit_collector": backend_commit_collector is not None,
            "network_collector": network_collector is not None,
            "disk_io_collector": disk_io_collector is not None,
            "cluster_info_collector": cluster_info_collector is not None,
            "node_usage_collector": node_usage_collector is not None
        }
    )


@mcp.tool()
async def get_ocp_cluster_info() -> OCPClusterInfoResponse:
    """
    Get comprehensive OpenShift cluster information and infrastructure details.
    
    Collects detailed information about the OpenShift cluster hosting the etcd cluster:
    - Cluster identification (name, version, platform - AWS/Azure/GCP/etc.)
    - Node information (master, infra, worker nodes with specs and status)
    - Resource counts (namespaces, pods, services, secrets, configmaps)
    - Network policy counts (NetworkPolicies, AdminNetworkPolicies, etc.)
    - Network resources (EgressFirewalls, EgressIPs, UserDefinedNetworks)
    - Cluster operator status (unavailable operators)
    - Machine Config Pool (MCP) status
    
    This provides context for etcd performance by showing the cluster environment.
    
    Returns:
        OCPClusterInfoResponse: Comprehensive cluster information including cluster details, node inventory, resource statistics, and operator status
    """
    try:
        global cluster_info_collector
        if cluster_info_collector is None:
            # Lazy initialize the ClusterInfoCollector on first use
            try:
                cluster_info_collector = ClusterInfoCollector()
                await cluster_info_collector.initialize()
            except Exception as init_err:
                return OCPClusterInfoResponse(
                    status="error",
                    error=f"Failed to initialize ClusterInfoCollector: {init_err}",
                    timestamp=datetime.now(pytz.UTC).isoformat()
                )

        info = await cluster_info_collector.collect_cluster_info()
        return OCPClusterInfoResponse(
            status="success",
            data=cluster_info_collector.to_dict(info),
            timestamp=datetime.now(pytz.UTC).isoformat()
        )
    except Exception as e:
        logger.error(f"Error collecting OCP cluster info: {e}")
        return OCPClusterInfoResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat()
        )

@mcp.tool()
async def get_etcd_node_usage(duration: str = "1h") -> ETCDNodeUsageResponse:
    """
    Get comprehensive node usage metrics for master nodes hosting etcd.
    
    Monitors resource utilization metrics at the node and cgroup level for master nodes:
    - Node CPU usage by mode (user, system, idle, iowait, etc.)
    - Node memory used (active memory consumption)
    - Node memory cache and buffer (filesystem cache and buffers)
    - Cgroup CPU usage (CPU consumption per control group)
    - Cgroup RSS usage (Resident Set Size memory per control group)
    
    These metrics provide insights into:
    - Overall master node resource utilization and capacity
    - CPU contention and workload distribution patterns
    - Memory pressure and caching efficiency
    - Container-level resource consumption via cgroups
    - Potential resource bottlenecks affecting etcd performance
    
    Node resource constraints can directly impact etcd cluster stability and performance.
    High CPU usage (>80%) or memory pressure can cause etcd timeouts and degraded performance.
    
    Args:
        duration: Time range for metrics collection. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
    
    Returns:
        ETCDNodeUsageResponse: Node usage metrics including CPU usage by mode, memory consumption, cache/buffer statistics, and cgroup-level resource utilization for all master nodes
    """
    try:
        global auth_manager, node_usage_collector
        if not node_usage_collector:
            # Lazy initialize if startup initialization didn't complete
            if auth_manager is None:
                auth_manager = OpenShiftAuth(config.kubeconfig_path if config else None)
                try:
                    await auth_manager.initialize()
                except Exception:
                    return ETCDNodeUsageResponse(
                        status="error",
                        error="Failed to initialize OpenShift auth for node usage",
                        timestamp=datetime.now(pytz.UTC).isoformat(),
                        duration=duration
                    )
            try:
                prometheus_config = {
                    'url': auth_manager.prometheus_url,
                    'token': getattr(auth_manager, 'prometheus_token', None),
                    'verify_ssl': False
                }
                node_usage_collector = nodeUsageCollector(auth_manager, prometheus_config)
            except Exception as e:
                return ETCDNodeUsageResponse(
                    status="error",
                    error=f"Failed to initialize nodeUsageCollector: {e}",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=duration
                )
        
        result = await node_usage_collector.collect_all_metrics(duration)
        
        return ETCDNodeUsageResponse(
            status=result.get('status', 'unknown'),
            data=result,
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting node usage metrics: {e}")
        return ETCDNodeUsageResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            duration=duration
        )

@mcp.tool()
async def get_etcd_cluster_status() -> ETCDClusterStatusResponse:
    """
    Get comprehensive etcd cluster status including health, member information, and leadership details.
    
    This tool provides real-time etcd cluster status by executing etcdctl commands to check:
    - Cluster health status (healthy/degraded endpoints)
    - Member list with active and learner members
    - Endpoint status including leader information, database sizes, and Raft terms
    - Leadership information and changes
    - Basic cluster metrics
    
    Returns:
        ETCDClusterStatusResponse: Complete cluster status including health, members, endpoints, leadership, and basic metrics
    """
    try:
        if not cluster_collector:
            return ETCDClusterStatusResponse(
                status="error",
                error="Cluster collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat()
            )
        
        result = await cluster_collector.get_cluster_status()
        
        return ETCDClusterStatusResponse(
            status=result.get('status', 'unknown'),
            data=result.get('data'),
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat())
        )
        
    except Exception as e:
        logger.error(f"Error getting cluster status: {e}")
        return ETCDClusterStatusResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat()
        )


@mcp.tool()
async def get_etcd_general_info(duration: str = "1h") -> ETCDGeneralInfoResponse:
    """
    Get general etcd cluster information including resource usage and operational metrics.
    
    Collects comprehensive etcd performance and health metrics including:
    - CPU and memory usage patterns
    - Database size metrics (physical and logical sizes, space utilization)
    - Proposal metrics (commit rates, failures, pending proposals)
    - Leadership metrics (leader changes, elections, has_leader status)
    - Performance metrics (slow applies, read indexes, operation rates)
    - Health metrics (heartbeat failures, total keys, compacted keys)
    
    Args:
        duration: Time range for metrics collection. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
    
    Returns:
        ETCDGeneralInfoResponse: General cluster information including resource usage, operational performance, and health statistics
    """
    try:
        if not general_collector:
            return ETCDGeneralInfoResponse(
                status="error",
                error="General info collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )
        
        result = await general_collector.collect_metrics(duration)
        
        return ETCDGeneralInfoResponse(
            status=result.get('status', 'unknown'),
            data=result.get('data'),
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting general info: {e}")
        return ETCDGeneralInfoResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            duration=duration
        )


@mcp.tool()
async def get_etcd_disk_compact_defrag(duration: str = "1h") -> ETCDCompactDefragResponse:
    """
    Get etcd database compaction and defragmentation performance metrics.
    
    Monitors database maintenance operations that are critical for etcd performance:
    - Compaction duration and rates (time spent compacting old revisions)
    - Defragmentation duration and rates (database defragmentation operations)
    - Page fault metrics (vmstat pgmajfault rates indicating memory pressure)
    - Operation efficiency analysis and performance recommendations
    
    These metrics help identify database maintenance bottlenecks and storage performance issues.
    
    Args:
        duration: Time range for metrics collection. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
    
    Returns:
        ETCDCompactDefragResponse: Compaction and defragmentation metrics with performance analysis and recommendations
    """
    try:
        if not compact_defrag_collector:
            return ETCDCompactDefragResponse(
                status="error",
                error="Compact/defrag collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )
        
        result = await compact_defrag_collector.collect_metrics(duration)
        
        return ETCDCompactDefragResponse(
            status=result.get('status', 'unknown'),
            data=result.get('data'),
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting compact/defrag metrics: {e}")
        return ETCDCompactDefragResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            duration=duration
        )

@mcp.tool()
async def get_etcd_disk_wal_fsync(duration: str = "1h") -> ETCDWALFsyncResponse:
    """
    Get etcd Write-Ahead Log (WAL) fsync performance metrics.
    
    Monitors WAL fsync operations that are critical for etcd data durability and write performance:
    - WAL fsync P99 latency (99th percentile fsync duration - target <10ms for good performance)
    - WAL fsync operation rates and counts (operations per second)
    - WAL fsync duration sum statistics (cumulative fsync time)
    - Cluster-wide WAL fsync performance analysis and health scoring
    
    WAL fsync performance directly impacts write latency. High fsync times (>100ms) indicate storage bottlenecks
    that can cause cluster instability and performance degradation.
    
    Args:
        duration: Time range for metrics collection. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
    
    Returns:
        ETCDWALFsyncResponse: WAL fsync performance metrics including P99 latency, operation rates, and cluster-wide analysis with storage performance recommendations
    """
    try:
        global auth_manager, wal_fsync_collector
        if not wal_fsync_collector:
            # Lazy initialize if startup initialization didn't complete
            if auth_manager is None:
                auth_manager = OpenShiftAuth(config.kubeconfig_path if config else None)
                try:
                    await auth_manager.initialize()
                except Exception:
                    return ETCDWALFsyncResponse(
                        status="error",
                        error="Failed to initialize OpenShift auth for WAL fsync",
                        timestamp=datetime.now(pytz.UTC).isoformat(),
                        duration=duration
                    )
            try:
                wal_fsync_collector = DiskWALFsyncCollector(auth_manager, duration)
            except Exception as e:
                return ETCDWALFsyncResponse(
                    status="error",
                    error=f"Failed to initialize DiskWALFsyncCollector: {e}",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=duration
                )
        
        # Update duration for this collection
        wal_fsync_collector.duration = duration
        result = await wal_fsync_collector.collect_all_metrics()
        
        return ETCDWALFsyncResponse(
            status=result.get('status', 'unknown'),
            data=result,
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting WAL fsync metrics: {e}")
        return ETCDWALFsyncResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            duration=duration
        )


@mcp.tool()
async def get_etcd_disk_backend_commit(duration: str = "1h") -> ETCDBackendCommitResponse:
    """
    Get etcd backend commit operation performance metrics.
    
    Monitors backend database commit operations that handle data persistence:
    - Backend commit duration P99 latency (99th percentile response times)
    - Commit operation rates and counts  
    - Commit duration statistics and efficiency analysis
    - Performance recommendations for write optimization
    
    Backend commit latency affects overall write performance. High latency (>25ms) indicates storage bottlenecks.
    
    Args:
        duration: Time range for metrics collection. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
    
    Returns:
        ETCDBackendCommitResponse: Backend commit performance metrics including P99 latency, operation throughput, and storage optimization recommendations
    """
    try:
        if not backend_commit_collector:
            return ETCDBackendCommitResponse(
                status="error",
                error="Backend commit collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )
        
        result = await backend_commit_collector.collect_metrics(duration)
        
        return ETCDBackendCommitResponse(
            status=result.get('status', 'unknown'),
            data=result.get('data'),
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting backend commit metrics: {e}")
        return ETCDBackendCommitResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            duration=duration
        )

@mcp.tool()
async def get_etcd_network_io(duration: str = "1h") -> ETCDNetworkIOResponse:
    """
    Get etcd network I/O performance and utilization metrics.
    
    Monitors comprehensive network performance metrics for etcd cluster operations:
    - Container network metrics (receive/transmit bytes for etcd pods)
    - Peer network metrics (peer-to-peer communication latency and throughput)
    - Client gRPC network metrics (client communication bandwidth)
    - Node network utilization (network interface utilization and packet rates)
    - Network drops and errors (packet loss and network issues)
    - gRPC stream metrics (active watch and lease streams)
    - Snapshot transfer duration (cluster synchronization network performance)
    
    Network performance directly impacts etcd cluster stability, client response times, and peer synchronization.
    High network latency or packet loss can cause cluster instability and performance degradation.
    
    Args:
        duration: Time range for metrics collection. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
    
    Returns:
        ETCDNetworkIOResponse: Network I/O performance metrics including container/peer/client network statistics, node utilization, error rates, and health assessment
    """
    try:
        global auth_manager, network_collector
        if not network_collector:
            # Lazy initialize if startup initialization didn't complete
            if auth_manager is None:
                auth_manager = OpenShiftAuth(config.kubeconfig_path if config else None)
                try:
                    await auth_manager.initialize()
                except Exception:
                    return ETCDNetworkIOResponse(
                        status="error",
                        error="Failed to initialize OpenShift auth for network I/O",
                        timestamp=datetime.now(pytz.UTC).isoformat(),
                        duration=duration
                    )
            try:
                network_collector = NetworkIOCollector(auth_manager)
            except Exception as e:
                return ETCDNetworkIOResponse(
                    status="error",
                    error=f"Failed to initialize NetworkIOCollector: {e}",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=duration
                )
        
        # Prefer collect_all_metrics; fall back to collect_metrics for compatibility
        if hasattr(network_collector, "collect_all_metrics"):
            result = await network_collector.collect_all_metrics(duration)
        else:
            result = await network_collector.collect_metrics(duration)
        
        return ETCDNetworkIOResponse(
            status=result.get('status', 'unknown'),
            data=result,
            error=result.get('error'),
            timestamp=(result.get('collection_time') or result.get('timestamp') or datetime.now(pytz.UTC).isoformat()),
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting network I/O metrics: {e}")
        return ETCDNetworkIOResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            duration=duration
        )

@mcp.tool()
async def get_etcd_disk_io(duration: str = "1h") -> ETCDDiskIOResponse:
    """
    Get etcd disk I/O performance metrics including throughput and IOPS.
    
    Monitors comprehensive disk I/O performance metrics that directly impact etcd performance:
    - Container disk write metrics (etcd pod disk write throughput and patterns)
    - Node disk read/write throughput (bytes per second for storage devices)  
    - Node disk read/write IOPS (input/output operations per second)
    - Device-level I/O statistics aggregated by master node
    - Storage performance analysis and bottleneck identification
    
    Disk I/O performance is critical for etcd operations including:
    - WAL (Write-Ahead Log) write operations
    - Database snapshot creation and transfers
    - Compaction and defragmentation operations
    - Overall cluster stability and response times
    
    Poor disk I/O performance can cause etcd timeouts, leader elections, and cluster instability.
    
    Args:
        duration: Time range for metrics collection. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
    
    Returns:
        ETCDDiskIOResponse: Disk I/O performance metrics including container write rates, node throughput/IOPS, device statistics, and storage optimization recommendations
    """
    try:
        global auth_manager, disk_io_collector, PROJECT_ROOT
        if not disk_io_collector:
            # Lazy initialize if startup initialization didn't complete
            if auth_manager is None:
                auth_manager = OpenShiftAuth(config.kubeconfig_path if config else None)
                try:
                    await auth_manager.initialize()
                except Exception:
                    return ETCDDiskIOResponse(
                        status="error",
                        error="Failed to initialize OpenShift auth for disk I/O",
                        timestamp=datetime.now(pytz.UTC).isoformat(),
                        duration=duration
                    )
            try:
                # Use metrics-disk.yml for disk I/O metrics
                metrics_disk_file = os.path.join(PROJECT_ROOT, 'config', 'metrics-disk.yml')
                disk_io_collector = DiskIOCollector(auth_manager, duration, metrics_file_path=metrics_disk_file)
            except Exception as e:
                return ETCDDiskIOResponse(
                    status="error",
                    error=f"Failed to initialize DiskIOCollector: {e}",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=duration
                )
        
        # Update duration for this collection
        disk_io_collector.duration = duration
        result = await disk_io_collector.collect_all_metrics()
        
        return ETCDDiskIOResponse(
            status=result.get('status', 'unknown'),
            data=result,
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting disk I/O metrics: {e}")
        return ETCDDiskIOResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            duration=duration
        )

@mcp.tool()
async def get_etcd_performance_deep_drive(
    duration: str = "1h",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    input: DeepDriveInput | None = None,
) -> ETCDPerformanceDeepDriveResponse:
    """
    Perform comprehensive etcd performance deep drive analysis across all critical subsystems.
    
    This tool executes an in-depth performance analysis of the etcd cluster by collecting and analyzing metrics from multiple subsystems:
    
    **Collected Metrics:**
    - General cluster metrics: CPU/memory usage, proposal rates, leadership changes, operation rates
    - WAL fsync performance: P99 latency, operation rates, duration statistics (critical for write performance)
    - Disk I/O metrics: Container and node-level disk throughput, IOPS, device statistics
    - Network I/O performance: Container network, peer communication, client gRPC, node utilization
    - Backend commit operations: Database commit latency, operation rates, efficiency analysis
    - Compact/defrag operations: Database maintenance performance, compaction duration, page faults
    
    **Analysis Features:**
    - Latency pattern analysis across all subsystems
    - Performance correlation analysis between different metrics
    - Health scoring and performance benchmarking
    - Automated performance summary with key findings
    - Cross-subsystem performance impact assessment
    
    **Use Cases:**
    - Comprehensive cluster health assessment
    - Performance baseline establishment
    - Pre/post-change performance comparison
    - Identifying performance trends and patterns
    - Generating detailed performance reports for stakeholders
    
    The analysis provides a holistic view of etcd performance, making it easier to identify performance bottlenecks and optimization opportunities across the entire cluster stack.
    
    Args:
        duration: Time range for metrics collection and analysis. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'. Default: '1h'
    
    Returns:
        ETCDPerformanceDeepDriveResponse: Comprehensive performance analysis including all subsystem metrics, latency analysis, performance summary, and actionable insights with unique test ID for tracking
    """
    try:
        global auth_manager
        if not auth_manager:
            return ETCDPerformanceDeepDriveResponse(
                status="error",
                error="OpenShift authentication not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration if input is None or not getattr(input, "duration", None) else input.duration
            )
        
        # Lazy import of analysis module
        try:
            from analysis.etcd.performance_analysis_deepdrive import etcdDeepDriveAnalyzer
        except ImportError as import_err:
            logger.error(f"Failed to import etcdDeepDriveAnalyzer: {import_err}")
            return ETCDPerformanceDeepDriveResponse(
                status="error",
                error=f"Performance deep drive analysis module not available: {import_err}",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration if input is None or not getattr(input, "duration", None) else input.duration
            )
        
        # Determine effective duration: prefer explicit input.duration, else compute from time range, else use duration arg
        eff_duration = duration if input is None or not getattr(input, "duration", None) else input.duration
        if start_time and end_time:
            computed = _duration_from_time_range(start_time, end_time)
            if computed:
                eff_duration = computed
        deep_drive_analyzer = etcdDeepDriveAnalyzer(auth_manager, eff_duration)
        
        # Perform the comprehensive analysis
        result = await deep_drive_analyzer.analyze_performance_deep_drive()
        
        return ETCDPerformanceDeepDriveResponse(
            status=result.get('status', 'unknown'),
            data=result.get('data'),
            analysis=result.get('analysis'),
            summary=result.get('summary'),
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            duration=eff_duration,
            test_id=result.get('test_id')
        )
        
    except Exception as e:
        logger.error(f"Error performing etcd performance deep drive analysis: {e}")
        return ETCDPerformanceDeepDriveResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            duration=duration if input is None or not getattr(input, "duration", None) else input.duration
        )


@mcp.tool()
async def get_etcd_bottleneck_analysis(duration: str = "1h") -> ETCDBottleneckAnalysisResponse:
    """
    Perform advanced etcd performance bottleneck analysis with root cause identification and optimization recommendations.
    
    This tool performs sophisticated bottleneck analysis by:
    
    **Bottleneck Detection:**
    - Disk I/O bottlenecks: WAL fsync high latency (>100ms P99), backend commit delays (>50ms P99), low disk throughput
    - Network bottlenecks: High peer-to-peer latency (>100ms), network utilization (>80%), packet drops
    - Memory bottlenecks: High memory usage (>80%), memory pressure indicators, potential memory leaks
    - Consensus bottlenecks: Proposal failures, high pending proposals (>10), slow applies, frequent leader changes (>1/hour)
    
    **Analysis Methodology:**
    - Automated threshold-based bottleneck identification with severity classification (high/medium/low)
    - Cross-subsystem correlation analysis to identify cascading performance issues
    - Performance impact assessment for each identified bottleneck
    - Historical pattern analysis to distinguish temporary vs. persistent issues
    
    **Root Cause Analysis:**
    - Evidence-based root cause identification linking symptoms to underlying causes
    - Likelihood assessment for each potential root cause
    - Impact analysis showing how bottlenecks affect cluster performance
    - Categorization by subsystem (disk_io, network, memory, consensus)
    
    **Optimization Recommendations:**
    - Prioritized recommendations based on performance impact and implementation complexity
    - Specific actionable steps for each identified bottleneck
    - Infrastructure optimization suggestions (storage upgrades, network improvements)
    - Configuration tuning recommendations for etcd and OpenShift
    
    **Use Cases:**
    - Performance troubleshooting and problem diagnosis
    - Proactive performance optimization planning
    - Infrastructure capacity planning and upgrades
    - Performance regression analysis after changes
    - Creating performance improvement roadmaps
    
    Args:
        duration: Time range for bottleneck analysis. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'. Default: '1h'
    
    Returns:
        ETCDBottleneckAnalysisResponse: Comprehensive bottleneck analysis including identified performance issues, root cause analysis, and prioritized optimization recommendations with unique test ID
    """
    try:
        global auth_manager
        if not auth_manager:
            return ETCDBottleneckAnalysisResponse(
                status="error",
                error="OpenShift authentication not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )
        
        # Lazy import of analysis module
        try:
            from analysis.etcd.performance_analysis_deepdrive import etcdDeepDriveAnalyzer
        except ImportError as import_err:
            logger.error(f"Failed to import etcdDeepDriveAnalyzer: {import_err}")
            return ETCDBottleneckAnalysisResponse(
                status="error",
                error=f"Bottleneck analysis module not available: {import_err}",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )
        
        # Initialize the deep drive analyzer
        deep_drive_analyzer = etcdDeepDriveAnalyzer(auth_manager, duration)
        
        # Perform bottleneck analysis
        result = await deep_drive_analyzer.analyze_bottlenecks()
        
        return ETCDBottleneckAnalysisResponse(
            status=result.get('status', 'success'),
            bottleneck_analysis=result.get('bottleneck_analysis'),
            root_cause_analysis=result.get('root_cause_analysis'),
            performance_recommendations=result.get('performance_recommendations'),
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            duration=duration,
            test_id=result.get('test_id')
        )
        
    except Exception as e:
        logger.error(f"Error performing etcd bottleneck analysis: {e}")
        return ETCDBottleneckAnalysisResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            duration=duration
        )

@mcp.tool()
async def generate_etcd_performance_report(duration: str = "1h", input: PerformanceReportInput | None = None) -> ETCDPerformanceReportResponse:
    """
    Generate comprehensive etcd performance analysis report with detailed metrics evaluation and recommendations.
    
    This tool provides enterprise-grade performance analysis and reporting for etcd clusters by:
    
    **Comprehensive Data Collection:**
    - Critical performance metrics: WAL fsync P99 latency, backend commit P99 latency
    - Supporting metrics: CPU/memory usage, network I/O, disk I/O performance
    - Cluster health indicators: proposal rates, leadership changes, compaction metrics
    - Infrastructure metrics: node resources, network utilization, storage performance
    
    **Advanced Performance Analysis:**
    - Threshold-based analysis using etcd best practices (WAL fsync <10ms, backend commit <25ms)
    - Baseline comparison against industry benchmarks and performance targets
    - Health status determination with severity classification (excellent/good/warning/critical)
    - Cross-metric correlation analysis to identify performance patterns
    
    **Executive Reporting:**
    - Executive summary with overall cluster health assessment and performance grade
    - Critical alerts section highlighting urgent performance issues requiring immediate attention
    - Detailed metrics analysis with formatted tables showing per-pod performance
    - Baseline comparison showing current vs. target performance with pass/fail status
    - Prioritized recommendations categorized by priority (high/medium/low) with specific actions
    
    **Analysis Methodology:**
    - Industry best practice thresholds and performance benchmarks
    - Root cause analysis linking performance symptoms to underlying infrastructure issues
    - Performance impact assessment and optimization recommendations
    - Detailed methodology explanation for audit and compliance purposes
    
    **Report Features:**
    - Professional formatting suitable for stakeholder presentations
    - Unique test ID for tracking and historical comparison
    - Timestamp and duration information for audit trails
    - Actionable recommendations with implementation guidance
    - Analysis rationale and methodology documentation
    
    **Use Cases:**
    - Regular performance health checks and monitoring reports
    - Pre/post-change performance impact analysis
    - Performance troubleshooting and root cause analysis
    - Capacity planning and infrastructure optimization
    - Executive dashboards and stakeholder reporting
    - Compliance documentation and performance auditing
    
    Args:
        duration: Time range for metrics collection and analysis. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'. Default: '1h'
        input: Optional input parameters including custom duration and test_id
    
    Returns:
        ETCDPerformanceReportResponse: Comprehensive performance analysis results and formatted report including critical metrics analysis, performance summary, baseline comparison, prioritized recommendations, and executive-ready documentation
    """
    try:
        global auth_manager
        if not auth_manager:
            return ETCDPerformanceReportResponse(
                status="error",
                error="OpenShift authentication not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration if input is None or not getattr(input, "duration", None) else input.duration
            )
        
        # Lazy import of analysis modules
        try:
            from analysis.etcd.performance_analysis_deepdrive import etcdDeepDriveAnalyzer
            from analysis.etcd.performance_analysis_report import etcdReportAnalyzer
        except ImportError as import_err:
            logger.error(f"Failed to import performance report modules: {import_err}")
            return ETCDPerformanceReportResponse(
                status="error",
                error=f"Performance report analysis modules not available: {import_err}",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration if input is None or not getattr(input, "duration", None) else input.duration,
                test_id=f"error-{datetime.now(pytz.UTC).strftime('%Y%m%d-%H%M%S')}"
            )
        
        # Get effective parameters
        eff_duration = duration if input is None or not getattr(input, "duration", None) else input.duration
        test_id = None
        if input and hasattr(input, "test_id") and input.test_id:
            test_id = input.test_id
        else:
            # Generate unique test ID
            test_id = f"perf-report-{datetime.now(pytz.UTC).strftime('%Y%m%d-%H%M%S')}"
        
        # Initialize the deep drive analyzer to collect metrics
        deep_drive_analyzer = etcdDeepDriveAnalyzer(auth_manager, eff_duration)
        
        # Collect comprehensive metrics for analysis
        metrics_result = await deep_drive_analyzer.analyze_performance_deep_drive()
        
        if metrics_result.get('status') != 'success':
            return ETCDPerformanceReportResponse(
                status="error",
                error=f"Failed to collect metrics for performance report: {metrics_result.get('error', 'Unknown error')}",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=eff_duration,
                test_id=test_id
            )
        
        # Initialize the performance report analyzer
        report_analyzer = etcdReportAnalyzer()

        # Collect node usage to enrich the analysis (ensure expected schema)
        node_usage_wrapped: Dict[str, Any] | None = None
        try:
            global node_usage_collector
            if not node_usage_collector:
                # Lazy init like get_etcd_node_usage
                prometheus_config_local = {
                    'url': auth_manager.prometheus_url,
                    'token': getattr(auth_manager, 'prometheus_token', None),
                    'verify_ssl': False,
                }
                node_usage_collector = nodeUsageCollector(auth_manager, prometheus_config_local)
            node_usage_result = await node_usage_collector.collect_all_metrics(eff_duration)
            node_usage_wrapped = {
                'status': node_usage_result.get('status', 'unknown'),
                'data': node_usage_result,
                'timestamp': node_usage_result.get('timestamp')
            }
        except Exception as nu_err:
            logger.warning(f"Node usage collection failed for performance report: {nu_err}")
            node_usage_wrapped = None

        # Analyze the collected metrics (including node usage when available)
        analysis_results = report_analyzer.analyze_performance_metrics(metrics_result, test_id, node_usage_wrapped)
        
        # Generate the comprehensive performance report
        performance_report = report_analyzer.generate_performance_report(
            analysis_results, test_id, eff_duration
        )
        
        return ETCDPerformanceReportResponse(
            status=analysis_results.get('status', 'success'),
            analysis_results=analysis_results,
            performance_report=performance_report,
            error=analysis_results.get('error'),
            timestamp=analysis_results.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            duration=eff_duration,
            test_id=test_id
        )
        
    except Exception as e:
        logger.error(f"Error generating etcd performance report: {e}")
        return ETCDPerformanceReportResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            duration=duration if input is None or not getattr(input, "duration", None) else input.duration,
            test_id=f"error-{datetime.now(pytz.UTC).strftime('%Y%m%d-%H%M%S')}"
        )

async def startup_event():
    """Startup event handler"""
    logger.info("")
    logger.info("🚀 Starting OpenShift etcd Analyzer MCP Server...")
    logger.info("")
    
    # Initialize collectors (includes metrics loading with logging)
    init_success = await initialize_collectors()
    if not init_success:
        logger.error("❌ Failed to initialize collectors. Server may not function properly.")
        logger.error("")
    else:
        logger.info("✅ OpenShift etcd Analyzer MCP Server started successfully!")
        logger.info("")


def main():
    """Main function to run the MCP server"""
    async def run_server():
        try:
            # Perform startup initialization
            await startup_event()
            
            # Optional MCP Inspector launch
            enable_inspector = os.environ.get("ENABLE_MCP_INSPECTOR", "").lower() in ("1", "true", "yes", "on")
            host = "0.0.0.0"
            port = 8001

            if enable_inspector:
                def start_mcp_inspector(url: str):
                    try:
                        if shutil.which("npx") is None:
                            logger.warning("MCP Inspector requested but 'npx' not found")
                            return
                        inspector_cmd = ["npx", "--yes", "@modelcontextprotocol/inspector", url]
                        subprocess.Popen(inspector_cmd)
                        logger.info("Launched MCP Inspector for URL: %s", url)
                    except Exception as ie:
                        logger.warning("Failed to launch MCP Inspector: %s", ie)

                inspector_url = os.environ.get("MCP_INSPECTOR_URL", f"http://127.0.0.1:{port}/sse")
                start_mcp_inspector(inspector_url)

            # Run the server
            await mcp.run_async(
                transport="streamable-http",
                port=port,
                host=host,
                uvicorn_config={"ws": "none"}
            )
        except Exception as e:
            logger.error(f"Failed to start server: {e}")
            sys.exit(1)
    
    try:
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            logger.warning("Already running in an event loop. Creating new task.")
            return loop.create_task(run_server())
        else:
            new_loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(new_loop)
                new_loop.run_until_complete(run_server())
            finally:
                new_loop.close()
                asyncio.set_event_loop(None)
            
    except KeyboardInterrupt:
        logger.info("Server shutdown requested")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()