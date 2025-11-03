#!/usr/bin/env python3
"""
OpenShift OVN-Kubernetes Benchmark MCP Server
Main server entry point using FastMCP with streamable-http transport
Fixed SSE stream handling and resource management
"""

import asyncio
import os
import json
import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict
import signal
import sys

"""Ensure project root is on sys.path so namespace packages like `tools` import
when running this module directly (python mcp/net/.../server.py)."""
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from tools.ocp.cluster_info import ClusterInfoCollector, collect_cluster_information, get_cluster_info_json
from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from tools.net.network_io import NetworkIOCollector
from tools.net.network_l1 import NetworkL1Collector
from tools.net.network_socket4tcp import socketStatTCPCollector
from tools.net.network_socket4mem import socketStatMemCollector
from tools.net.network_socket4softnet import socketStatSoftNetCollector
from tools.net.network_socket4ip import socketStatIPCollector
from tools.net.network_netstat4tcp import netStatTCPCollector
from tools.net.network_netstat4udp import netStatUDPCollector
from ocauth.openshift_auth import OpenShiftAuth
from config.metrics_config_reader import Config

import fastmcp
from fastmcp.server import FastMCP

import warnings
# Suppress urllib3 deprecation warning triggered by kubernetes client using HTTPResponse.getheaders()
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message=r"HTTPResponse\.getheaders\(\) is deprecated"
)

# Suppress anyio stream warnings
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    module="anyio.streams.memory"
)

# Configure logging with more granular control
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Allow overriding log level via env
_server_log_level = os.environ.get("OVNK_LOG_LEVEL", "INFO").upper()
try:
    root_level = getattr(logging, _server_log_level, logging.INFO)
except Exception:
    root_level = logging.INFO
logging.getLogger().setLevel(root_level)
logger.setLevel(root_level)

# Ensure submodule logs are visible in server output
_sub_loggers = [
    "tools.ovnk_benchmark_prometheus_basequery",
    "ocauth.openshift_auth",
]
for lname in _sub_loggers:
    l = logging.getLogger(lname)
    l.setLevel(root_level)
    l.propagate = True

# Reduce noise from overly chatty libs
logging.getLogger("mcp.server.streamable_http").setLevel(logging.WARNING)
logging.getLogger("anyio").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
logger.debug(f"Logging configured. Root level={root_level}, OVNK_LOG_LEVEL={_server_log_level}")

# Configure timezone
os.environ['TZ'] = 'UTC'

# Global shutdown event
shutdown_event = asyncio.Event()

class ClusterInfoRequest(BaseModel):
    """Request model for OpenShift cluster information queries"""
    include_node_details: bool = Field(
        default=True,
        description="Whether to include detailed node information including capacity, versions, and status for all nodes grouped by role (master/worker/infra)"
    )
    include_resource_counts: bool = Field(
        default=True,
        description="Whether to include comprehensive resource counts across all namespaces including pods, services, secrets, configmaps, and network policies"
    )
    include_network_policies: bool = Field(
        default=True,
        description="Whether to include detailed network policy information including NetworkPolicy, AdminNetworkPolicy, EgressFirewall, EgressIP, and UserDefinedNetwork counts"
    )
    include_operator_status: bool = Field(
        default=True,
        description="Whether to include cluster operator health status and identify any unavailable or degraded operators"
    )
    include_mcp_status: bool = Field(
        default=True,
        description="Whether to include Machine Config Pool (MCP) status information showing update progress and any degraded pools"
    )
    save_to_file: bool = Field(
        default=False,
        description="Whether to save the collected cluster information to a timestamped JSON file for documentation and audit purposes"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class GeneralClusterStatusRequest(BaseModel):
    """Request model for general cluster status check and health assessment"""
    include_detailed_analysis: bool = Field(
        default=True,
        description="Whether to include detailed component analysis with health scoring, performance metrics breakdown, recommendations, and alerts for each cluster component"
    )
    generate_summary_report: bool = Field(
        default=True, 
        description="Whether to generate a human-readable executive summary report with key findings, risk assessment, and prioritized action items in addition to structured analysis data"
    )
    health_check_scope: Optional[List[str]] = Field(
        default=None,
        description="Optional list to limit health check scope to specific areas: ['operators', 'nodes', 'networking', 'storage', 'mcps']. If not specified, performs comprehensive health assessment across all cluster components"
    )
    performance_baseline_comparison: bool = Field(
        default=False,
        description="Whether to include comparison against performance baselines and historical trends when available for identifying performance degradation or improvement patterns"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class NetworkL1Request(BaseModel):
    """Request model for network Layer 1 metrics queries"""
    duration: str = Field(
        default="5m",
        description="Query duration for metrics collection (e.g., 5m, 15m, 1h)"
    )
    metric_name: Optional[str] = Field(
        default=None,
        description="Optional specific metric name to query (e.g., 'network_l1_node_arp_entries'). If not specified, collects all L1 metrics"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class NetworkSocketTCPRequest(BaseModel):
    """Request model for network socket TCP metrics queries"""
    include_all_workers: bool = Field(
        default=False,
        description="Whether to include all worker nodes (default: false, shows top 3 by max value)"
    )
    metric_filter: Optional[List[str]] = Field(
        default=None,
        description="Optional list to filter specific TCP metrics: ['socket_tcp_allocated', 'socket_tcp_inuse', 'socket_tcp_orphan', 'socket_tcp_tw', 'socket_used', 'socket_frag_inuse', 'socket_raw_inuse']. If not specified, collects all metrics"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class NetworkSocketMemRequest(BaseModel):
    """Request model for network socket memory statistics queries"""
    duration: str = Field(
        default="1h",
        description="Query duration (e.g., 5m, 1h, 1d, 7d)"
    )
    start_time: Optional[str] = Field(
        default=None,
        description="Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)"
    )
    end_time: Optional[str] = Field(
        default=None,
        description="End time in ISO format (YYYY-MM-DDTHH:MM:SSZ)"
    )
    include_all_workers: bool = Field(
        default=False,
        description="Include all worker nodes instead of top 3 only"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class NetworkSocketMemRequest(BaseModel):
    """Request model for network socket memory metrics queries"""
    include_statistics: bool = Field(
        default=True,
        description="Whether to include statistical analysis (avg, max) for each node's socket memory metrics"
    )
    worker_top_n: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Number of top worker nodes to return by maximum socket memory usage (1-10)"
    )
    include_all_roles: bool = Field(
        default=True,
        description="Whether to include all node roles (controlplane/infra/workload) or only workers"
    )
    metric_filter: Optional[List[str]] = Field(
        default=None,
        description="Optional list to filter specific metrics: ['node_sockstat_FRAG_memory', 'TCP_Kernel_Buffer_Memory_Pages', 'UDP_Kernel_Buffer_Memory_Pages', 'node_sockstat_TCP_mem_bytes', 'node_sockstat_UDP_mem_bytes']. If not specified, collects all socket memory metrics"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class NetworkSocketSoftnetRequest(BaseModel):
    """Request model for network socket softnet statistics queries"""
    duration: str = Field(
        default="1h", 
        description="Query duration using Prometheus time format (e.g., '5m', '15m', '1h', '3h', '1d', '7d') for softnet metrics collection"
    )
    start_time: Optional[str] = Field(
        default=None, 
        description="Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ) for historical softnet analysis - automatically calculated from duration if not provided"
    )
    end_time: Optional[str] = Field(
        default=None, 
        description="End time in ISO format (YYYY-MM-DDTHH:MM:SSZ) for historical softnet analysis - defaults to current time if not provided"
    )
    step: str = Field(
        default="15s",
        description="Query step interval for Prometheus range queries controlling data point granularity (e.g., '15s', '30s', '1m')"
    )
    include_summary: bool = Field(
        default=True,
        description="Whether to include summary statistics with total metrics collected, node counts per role, and collection metadata"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class NetworkSocketIPRequest(BaseModel):
    """Request model for network socket IP metrics queries"""
    duration: str = Field(
        default="1h", 
        description="Query duration for range queries (e.g., 5m, 1h, 6h, 1d, 7d)"
    )
    start_time: Optional[str] = Field(
        default=None, 
        description="Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)"
    )
    end_time: Optional[str] = Field(
        default=None, 
        description="End time in ISO format (YYYY-MM-DDTHH:MM:SSZ)"
    )
    step: str = Field(
        default="15s",
        description="Query resolution step width (e.g., 15s, 30s, 1m)"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class NetStatTCPRequest(BaseModel):
    """Request model for network netstat TCP metrics queries"""
    duration: str = Field(
        default="1h", 
        description="Query duration (e.g., 5m, 1h, 1d, 7d)"
    )
    start_time: Optional[str] = Field(
        default=None, 
        description="Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)"
    )
    end_time: Optional[str] = Field(
        default=None, 
        description="End time in ISO format (YYYY-MM-DDTHH:MM:SSZ)"
    )
    step: str = Field(
        default="15s",
        description="Query step interval (e.g., 15s, 30s, 1m)"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class MetricsRequest(BaseModel):
    """Request model for basic metrics queries"""
    duration: str = Field(
        default="1h", 
        description="Query duration (e.g., 5m, 1h, 1d, 7d)"
    )
    start_time: Optional[str] = Field(
        default=None, 
        description="Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)"
    )
    end_time: Optional[str] = Field(
        default=None, 
        description="End time in ISO format (YYYY-MM-DDTHH:MM:SSZ)"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class PODsContainerRequest(BaseModel):
    """Request model for pod-specific metrics queries"""
    pod_pattern: str = Field(
        default="ovnkube.*", 
        description="Regex pattern for pod names (e.g., 'ovnkube.*', 'multus.*')"
    )
    container_pattern: str = Field(
        default="ovnkube-controller", 
        description="Regex pattern for container names (e.g., 'ovnkube-controller', 'kube-rbac-proxy.*')"
    )
    label_selector: str = Field(
        default=".*", 
        description="Regex pattern for pod label selectors"
    )
    namespace_pattern: str = Field(
        default="openshift-ovn-kubernetes", 
        description="Regex pattern for namespace (e.g., 'openshift-ovn-kubernetes', 'openshift-multus')"
    )
    top_n: int = Field(
        default=10, 
        description="Number of top results to return (1-50)"
    )
    duration: str = Field(
        default="1h", 
        description="Query duration for range queries (e.g., 5m, 1h, 1d)"
    )
    start_time: Optional[str] = Field(
        default=None, 
        description="Start time in ISO format"
    )
    end_time: Optional[str] = Field(
        default=None, 
        description="End time in ISO format"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class PODsRequest(BaseModel):
    """Request model for pod-specific metrics queries"""
    pod_pattern: str = Field(
        default="ovnkube.*", 
        description="Regex pattern for pod names (e.g., 'ovnkube.*', 'multus.*')"
    )
    container_pattern: str = Field(
        default=".*", 
        description="Regex pattern for container names (e.g., 'ovnkube-controller', 'kube-rbac-proxy.*')"
    )
    label_selector: str = Field(
        default=".*", 
        description="Regex pattern for pod label selectors"
    )
    namespace_pattern: str = Field(
        default="openshift-ovn-kubernetes", 
        description="Regex pattern for namespace (e.g., 'openshift-ovn-kubernetes', 'openshift-multus')"
    )
    top_n: int = Field(
        default=10, 
        description="Number of top results to return (1-50)"
    )
    duration: str = Field(
        default="1h", 
        description="Query duration for range queries (e.g., 5m, 1h, 1d)"
    )
    start_time: Optional[str] = Field(
        default=None, 
        description="Start time in ISO format"
    )
    end_time: Optional[str] = Field(
        default=None, 
        description="End time in ISO format"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class PODsMultusRequest(BaseModel):
    """Request model for pod-specific metrics queries"""
    pod_pattern: str = Field(
        default="multus-.*|network-metrics-.*", 
        description="Regex pattern for pod names (e.g., 'ovnkube.*', 'multus.*')"
    )
    container_pattern: str = Field(
        default=".*", 
        description="Regex pattern for container names (e.g., 'ovnkube-controller', 'kube-rbac-proxy.*')"
    )
    label_selector: str = Field(
        default=".*", 
        description="Regex pattern for pod label selectors"
    )
    namespace_pattern: str = Field(
        default="openshift-multus", 
        description="Regex pattern for namespace (e.g., 'openshift-ovn-kubernetes', 'openshift-multus')"
    )
    top_n: int = Field(
        default=10, 
        description="Number of top results to return (1-50)"
    )
    duration: str = Field(
        default="1h", 
        description="Query duration for range queries (e.g., 5m, 1h, 1d)"
    )
    start_time: Optional[str] = Field(
        default=None, 
        description="Start time in ISO format"
    )
    end_time: Optional[str] = Field(
        default=None, 
        description="End time in ISO format"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class PrometheusBasicInfoRequest(BaseModel):
    """Request model for Prometheus basic OVN information queries"""
    include_pod_status: bool = Field(
        default=True,
        description="Whether to include cluster-wide pod phase counts and status information"
    )
    include_db_metrics: bool = Field(
        default=True,
        description="Whether to include OVN database size metrics (Northbound and Southbound)"
    )
    custom_metrics: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional dictionary of custom metric_name -> prometheus_query pairs for additional data collection"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class OVNKLatencyMetricsRequest(BaseModel):
    """Request model for OVN-Kubernetes latency metrics collection"""
    duration: Optional[str] = Field(
        default="1h",
        description="Analysis duration using Prometheus time format (e.g., '5m', '30m', '1h', '2h'). If not provided, performs instant query for current latency values"
    )
    end_time: Optional[str] = Field(
        default=None,
        description="End time in ISO format (YYYY-MM-DDTHH:MM:SSZ). If not specified, uses current time"
    )
    include_controller_metrics: bool = Field(
        default=True,
        description="Whether to include OVN controller-related latency metrics (ready duration, sync duration)"
    )
    include_node_metrics: bool = Field(
        default=True,
        description="Whether to include OVN node-related latency metrics (node ready duration)"
    )
    include_extended_metrics: bool = Field(
        default=True,
        description="Whether to include extended latency metrics (CNI latency, pod creation latency, service latency, network configuration latency)"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class NetworkIORequest(BaseModel):
    """Request model for network I/O metrics queries"""
    duration: str = Field(
        default="5m",
        description="Query duration (e.g., 5m, 15m, 1h, 6h, 1d)"
    )
    include_metrics: Optional[List[str]] = Field(
        default=None,
        description="Optional list to include specific metrics: ['rx_utilization', 'tx_utilization', 'rx_packets', 'tx_packets', 'rx_drops', 'tx_drops', 'grpc_streams', 'conntrack', 'saturation', 'errors', 'interface_status', 'speed', 'arp', 'fifo']. If not specified, collects all metrics"
    )
    node_groups: Optional[List[str]] = Field(
        default=None,
        description="Optional list to filter by node groups: ['controlplane', 'worker', 'infra', 'workload']. If not specified, includes all node groups"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class HealthCheckRequest(BaseModel):
    """Empty request model for health check"""
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class OVNBasicInfoRequest(BaseModel):
    """Request model for OVN basic information queries"""
    include_pod_status: bool = Field(
        default=True,
        description="Whether to include cluster-wide pod phase counts and status information"
    )
    include_db_metrics: bool = Field(
        default=True,
        description="Whether to include OVN database size metrics (Northbound and Southbound)"
    )
    custom_metrics: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional dictionary of custom metric_name -> prometheus_query pairs for additional data collection"
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class OVNKDeepDriveAnalysisRequest(BaseModel):
    """Request model for comprehensive OVN-Kubernetes deep drive performance analysis"""
    duration: Optional[str] = Field(
        default="1h",
        description="Analysis duration using Prometheus time format (e.g., '5m', '15m', '30m', '1h', '2h', '24h'). If not provided, performs instant analysis using current metrics. Duration analysis provides historical trend data while instant analysis gives real-time snapshot. Recommended: '5m' for quick performance checks, '30m' for standard analysis, '1h' for comprehensive trend analysis, '24h' for long-term performance patterns."
    )
    include_performance_insights: bool = Field(
        default=True,
        description="Whether to include detailed performance analysis with scoring, key findings, recommendations, and resource hotspot identification. When True, provides comprehensive analysis including performance grading (A-D), component scoring for latency/resources/stability/OVS, actionable recommendations, and identification of high-usage nodes and pods. Set to False for faster execution when only raw metrics are needed."
    )
    focus_components: Optional[List[str]] = Field(
        default=None,
        description="Optional list of specific components to focus analysis on for targeted performance investigation. Available components: ['basic_info', 'ovnkube_pods', 'ovn_containers', 'ovs_metrics', 'latency_metrics', 'nodes_usage']. Examples: ['ovnkube_pods', 'latency_metrics'] for pod and latency focus, ['ovs_metrics'] for OVS-specific analysis, ['nodes_usage'] for node performance focus. If not specified, analyzes all components comprehensively for complete performance picture."
    )
    top_n_results: int = Field(
        default=5,
        description="Number of top results to return for each metric category (1-10). Controls the depth of analysis by limiting results to top N highest usage pods, containers, nodes, and latency metrics. Lower values (1-3) provide focused analysis on critical issues, higher values (5-10) provide broader performance visibility. Affects response size and processing time."
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class OCPOVERALLPerformanceRequest(BaseModel):
    """Request model for overall OCP cluster performance analysis"""
    duration: str = Field(
        default="1h",
        description="Analysis duration for metrics collection using Prometheus time format (e.g., '5m', '30m', '1h', '2h', '24h'). Longer durations provide more comprehensive trend analysis but take more time to process. Recommended values: '5m' for quick checks, '1h' for standard analysis, '24h' for trend analysis."
    )
    include_detailed_analysis: bool = Field(
        default=True,
        description="Whether to include detailed component-level analysis in the response. When True, provides comprehensive breakdown of each component's performance metrics, resource usage, and health status. Set to False for faster execution when only summary metrics are needed."
    )
    focus_areas: Optional[List[str]] = Field(
        default=None,
        description="Optional list of specific focus areas to emphasize in analysis. Available areas: ['cluster', 'api', 'ovnk', 'nodes', 'databases', 'sync']. Use to optimize analysis time by focusing on specific components. Examples: ['cluster', 'api'] for control plane focus, ['ovnk', 'sync'] for networking focus, ['nodes'] for compute focus. If not specified, analyzes all areas comprehensively."
    )
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


# Initialize FastMCP app
app = FastMCP("ovnk-benchmark-mcp")

# Global components
auth_manager: Optional[OpenShiftAuth] = None
config: Optional[Config] = None
prometheus_client: Optional[PrometheusBaseQuery] = None
# Note: PrometheusStorage and ClusterStatAnalyzer classes don't exist in current codebase
# storage: Optional[PrometheusStorage] = None
# cluster_analyzer: Optional[ClusterStatAnalyzer] = None
storage: Optional[Any] = None
cluster_analyzer: Optional[Any] = None


def _sanitize_json_compat(value):
    """Recursively replace NaN/Inf floats with None for JSON compatibility."""
    try:
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        if isinstance(value, dict):
            return {k: _sanitize_json_compat(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_sanitize_json_compat(v) for v in value]
        # Tuples -> lists for JSON
        if isinstance(value, tuple):
            return [_sanitize_json_compat(v) for v in value]
        return value
    except Exception:
        return None

async def initialize_components():
    """Initialize global components with proper error handling"""
    global auth_manager, config, prometheus_client, storage, cluster_analyzer
    
    try:
        config = Config()
        # Ensure metrics are loaded and report which files and how many metrics
        loaded_before = config.get_metrics_count()
        if loaded_before == 0:
            # Resolve metrics file relative to project root to avoid CWD issues
            metrics_file_path = os.path.join(PROJECT_ROOT, 'config', 'metrics-net.yml')
            load_result = config.load_metrics_file(metrics_file_path)
            # Trust the actual config state after attempting to load
            total_after = config.get_metrics_count()
            if total_after > 0:
                file_summary = config.get_file_summary()
                files_descr = ", ".join(
                    f"{v['file_name']}={v['metrics_count']}" for v in file_summary.values()
                ) or os.path.basename(metrics_file_path)
                logger.info(
                    "Metrics loaded on startup: total=%s, files=[%s]",
                    total_after,
                    files_descr
                )
            else:
                logger.warning(
                    "No metrics loaded on startup (%s). You can set METRICS_FILES env or place metrics under %s",
                    load_result.get('error', 'no metrics found'),
                    config.metrics_directory
                )
        else:
            # Already loaded via METRICS_FILES or auto discovery
            file_summary = config.get_file_summary()
            total = config.get_metrics_count()
            files_descr = ", ".join(f"{v['file_name']}={v['metrics_count']}" for v in file_summary.values()) or "(none)"
            logger.info("Metrics preloaded on startup: total=%s, files=[%s]", total, files_descr)
        auth_manager = OpenShiftAuth(config.kubeconfig_path)
        await auth_manager.initialize()        
        prometheus_client = PrometheusBaseQuery(
            auth_manager.prometheus_url,
            auth_manager.prometheus_token
        )
        
        # Note: PrometheusStorage class doesn't exist in current codebase
        # storage = PrometheusStorage()
        # await storage.initialize()
        storage = None  # Placeholder for missing PrometheusStorage
            
        logger.info("All components initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize components: {e}")
        raise

async def cleanup_resources():
    """Clean up global resources on shutdown"""
    global auth_manager, storage
    
    logger.info("Cleaning up resources...")
    
    try:
        if storage:
            await storage.close()
    except Exception as e:
        logger.error(f"Error cleaning up storage: {e}")
    
    try:
        if auth_manager:
            await auth_manager.cleanup()
    except Exception as e:
        logger.error(f"Error cleaning up auth manager: {e}")
    
    except Exception as e:
        logger.error(f"Error cleaning up cluster collector: {e}")
    
    logger.info("Resource cleanup completed")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_event.set()

# Setup signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


@app.tool(
    name="get_mcp_health_status",
    description="""Health check for the MCP server. Verifies MCP server is running, Prometheus connectivity, and Kubernetes API connectivity. Returns component statuses and timestamps."""
)
async def get_mcp_health_status(request: HealthCheckRequest) -> Dict[str, Any]:
    """Return health status for MCP server, Prometheus, and KubeAPI with improved error handling"""
    global auth_manager, prometheus_client
    
    try:
        # Ensure components exist
        if not auth_manager or not prometheus_client:
            try:
                await initialize_components()
            except Exception as init_error:
                return {
                    "status": "error", 
                    "error": f"Component initialization failed: {init_error}",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }

        # Test Prometheus connectivity with timeout
        prometheus_ok = False
        prometheus_error: Optional[str] = None
        try:
            # Add timeout to prevent hanging
            prometheus_ok = await asyncio.wait_for(
                prometheus_client.test_connection(),
                timeout=10.0
            )
          
        except asyncio.TimeoutError:
            prometheus_error = "Connection timeout after 10 seconds"
            prometheus_ok = False
        except Exception as e:
            prometheus_error = str(e)
            prometheus_ok = False
            logger.error(f"Prometheus connection error: {e}")

        # Test Kube API connectivity with timeout
        kubeapi_ok = False
        kubeapi_error: Optional[str] = None
        try:
            if auth_manager:
                kubeapi_ok = await asyncio.wait_for(
                    auth_manager.test_kubeapi_connection(),
                    timeout=10.0
                )
        except asyncio.TimeoutError:
            kubeapi_error = "Connection timeout after 10 seconds"
            kubeapi_ok = False
        except Exception as e:
            kubeapi_error = str(e)
            kubeapi_ok = False
            logger.error(f"KubeAPI connection error: {e}")

        status = "healthy" if prometheus_ok and kubeapi_ok else ("degraded" if prometheus_ok or kubeapi_ok else "unhealthy")

        return {
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "mcp_server": {
                "running": True,
                "transport": "streamable-http",
            },
            "prometheus": {
                "connected": prometheus_ok,
                "url": getattr(auth_manager, "prometheus_url", None) if auth_manager else None,
                "error": prometheus_error,
            },
            "kubeapi": {
                "connected": kubeapi_ok,
                "node_count": (auth_manager.cluster_info.get("node_count") if (auth_manager and auth_manager.cluster_info) else None),
                "error": kubeapi_error,
            },
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "error", "error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="get_openshift_cluster_info",
    description="""Collect comprehensive OpenShift cluster information including detailed node status, resource utilization, network policy configurations, cluster operator health, and infrastructure details. This tool provides a complete cluster inventory and health overview essential for understanding cluster architecture, capacity, and operational status.

This tool leverages the ClusterInfoCollector to gather structured information about your OpenShift cluster including:

CLUSTER ARCHITECTURE:
- Cluster name, version, and infrastructure platform (AWS, Azure, GCP, VMware, etc.)
- API server endpoint and cluster identification
- Total node count with role-based categorization (master/worker/infra nodes)
- Collection timestamp for data freshness verification

NODE INFORMATION (when include_node_details=True):
- Detailed node inventory grouped by role (master, worker, infrastructure nodes)
- Per-node specifications: CPU capacity, memory capacity, architecture, kernel version
- Container runtime versions, kubelet versions, and OS image information  
- Node health status (Ready/NotReady/SchedulingDisabled) and schedulability
- Node creation timestamps and lifecycle information
- Hardware and software configuration details for capacity planning

RESOURCE INVENTORY (when include_resource_counts=True):
- Comprehensive resource counts across all namespaces
- Pod distribution and total pod count for capacity monitoring
- Service, Secret, and ConfigMap counts for resource utilization analysis
- Cross-namespace resource distribution patterns
- Resource density metrics for capacity planning

NETWORK POLICY CONFIGURATION (when include_network_policies=True):
- NetworkPolicy count for standard Kubernetes network segmentation
- AdminNetworkPolicy (ANP) count for cluster-wide network security policies  
- EgressFirewall policy count for egress traffic control and security
- EgressIP configuration count for source IP management
- UserDefinedNetwork (UDN) count for custom network configurations
- Network security posture assessment through policy distribution analysis

CLUSTER OPERATOR HEALTH (when include_operator_status=True):
- Complete cluster operator inventory and availability status
- Identification of unavailable or degraded cluster operators
- Operator health trends and stability indicators
- Critical operator failure detection for immediate attention
- Operator dependency analysis for troubleshooting

MACHINE CONFIG POOL STATUS (when include_mcp_status=True):
- Machine Config Pool health and update status (Updated/Updating/Degraded)
- Configuration synchronization status across node pools
- Update progress tracking for maintenance windows
- Configuration drift detection and remediation status
- Node pool stability and configuration consistency

OPERATIONAL METADATA:
- Data collection timestamp for freshness verification
- Collection success metrics and any partial failure indicators
- Structured JSON format for integration with monitoring and automation systems
- Optional file export for documentation, compliance, and historical analysis

Parameters:
- include_node_details (default: true): Include comprehensive node information with hardware specs, software versions, and health status for all cluster nodes
- include_resource_counts (default: true): Include detailed resource inventory counts across all namespaces for capacity analysis
- include_network_policies (default: true): Include network policy configurations and counts for security posture assessment
- include_operator_status (default: true): Include cluster operator health status and identify any degraded or unavailable operators
- include_mcp_status (default: true): Include Machine Config Pool status and update progress information
- save_to_file (default: false): Save complete cluster information to timestamped JSON file for documentation and audit trails

Use this tool for:
- Pre-deployment cluster readiness verification and infrastructure validation
- Capacity planning and resource allocation analysis based on current utilization
- Security posture assessment through network policy and operator health analysis
- Operational health monitoring and cluster status reporting
- Troubleshooting cluster issues by understanding current configuration and status
- Compliance reporting and infrastructure documentation for audit purposes
- Baseline establishment for cluster monitoring and change tracking over time
- Executive reporting on cluster architecture, capacity, and operational health

The tool provides structured, comprehensive cluster information suitable for both technical analysis and management reporting, enabling informed decisions about cluster operations, capacity planning, and infrastructure optimization."""
)
async def get_openshift_cluster_info(request: ClusterInfoRequest) -> Dict[str, Any]:
    """
    Collect comprehensive OpenShift cluster information including detailed node status,
    resource utilization, network policy configurations, cluster operator health,
    and infrastructure details.
    
    Provides complete cluster inventory and health overview essential for understanding
    cluster architecture, capacity, and operational status.
    """
    try:
        logger.info("Starting comprehensive cluster information collection...")
        
        # Initialize collector
        collector = ClusterInfoCollector()
        
        # Add timeout to prevent hanging during cluster information collection
        cluster_info = await asyncio.wait_for(
            collector.collect_cluster_info(),
            timeout=60.0  # Extended timeout for comprehensive collection
        )
        
        # Convert to dictionary format
        cluster_data = collector.to_dict(cluster_info)
        
        # Apply filtering based on request parameters
        if not request.include_node_details:
            # Remove detailed node information but keep counts
            cluster_data.pop('master_nodes', None)
            cluster_data.pop('infra_nodes', None) 
            cluster_data.pop('worker_nodes', None)
            logger.info("Node details excluded from response")
        
        if not request.include_resource_counts:
            # Remove resource counts
            resource_fields = ['namespaces_count', 'pods_count', 'services_count', 
                             'secrets_count', 'configmaps_count']
            for field in resource_fields:
                cluster_data.pop(field, None)
            logger.info("Resource counts excluded from response")
        
        if not request.include_network_policies:
            # Remove network policy information
            policy_fields = ['networkpolicies_count', 'adminnetworkpolicies_count',
                           'egressfirewalls_count', 'egressips_count', 'udn_count']
            for field in policy_fields:
                cluster_data.pop(field, None)
            logger.info("Network policy information excluded from response")
        
        if not request.include_operator_status:
            cluster_data.pop('unavailable_cluster_operators', None)
            logger.info("Operator status excluded from response")
        
        if not request.include_mcp_status:
            cluster_data.pop('mcp_status', None)
            logger.info("Machine Config Pool status excluded from response")
        
        # Save to file if requested
        # save_to_file handling removed
        
        # Add collection metadata
        cluster_data['collection_metadata'] = {
            'tool_name': 'get_openshift_cluster_info',
            'parameters_applied': {
                'include_node_details': request.include_node_details,
                'include_resource_counts': request.include_resource_counts,
                'include_network_policies': request.include_network_policies,
                'include_operator_status': request.include_operator_status,
                'include_mcp_status': request.include_mcp_status,
                # 'save_to_file': request.save_to_file
            },
            'collection_duration_seconds': 60.0,
            'data_freshness': cluster_data.get('collection_timestamp'),
            'total_fields_collected': len(cluster_data)
        }
        
        # Log collection summary
        node_summary = f"Nodes: {cluster_data.get('total_nodes', 0)} total"
        if request.include_node_details:
            masters = len(cluster_data.get('master_nodes', []))
            workers = len(cluster_data.get('worker_nodes', []))
            infra = len(cluster_data.get('infra_nodes', []))
            node_summary += f" ({masters} master, {workers} worker, {infra} infra)"
        
        unavailable_ops = len(cluster_data.get('unavailable_cluster_operators', []))
        degraded_mcps = len([status for status in cluster_data.get('mcp_status', {}).values() 
                           if status in ['Degraded', 'Updating']])
        
        logger.info(f"Cluster information collection completed - {node_summary}, "
                   f"Unavailable operators: {unavailable_ops}, Degraded MCPs: {degraded_mcps}")
        
        return cluster_data
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting cluster information - cluster may be experiencing issues or have extensive resources",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 60,
            "suggestion": "Try running with fewer details enabled or check cluster responsiveness"
        }
    except Exception as e:
        logger.error(f"Error collecting cluster information: {e}")
        return {
            "error": str(e), 
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "get_openshift_cluster_info"
        }

@app.tool(
    name="query_network_l1_metrics",
    description="""Collect comprehensive Layer 1 network metrics across all cluster nodes including network interface status, carrier detection, speed, MTU configuration, and ARP table entries. This tool provides physical layer network health visibility essential for diagnosing connectivity issues and capacity planning.

METRICS COLLECTED:
- Network Up Status: Operational status (Yes/No) for each network interface per node
- Traffic Carrier Status: Physical carrier detection status for interfaces (link presence detection)
- Network Speed: Maximum network interface speeds in bits per second for bandwidth capacity analysis
- Network MTU: Maximum Transmission Unit sizes in bytes for each interface
- ARP Entries: ARP table entry counts with average and maximum values per node and device

NODE ORGANIZATION:
- Metrics automatically grouped by node role: controlplane, worker, infra, workload
- For worker nodes: returns top 3 nodes by name to manage output size
- For other roles: returns all nodes for complete visibility
- Per-node and per-interface granularity for detailed troubleshooting

INTERFACE-LEVEL DETAILS:
- Individual interface metrics for multi-NIC configurations
- Device-specific status and performance data
- Interface naming and identification for cross-reference with network topology

USE CASES:
- Network connectivity troubleshooting and link status verification
- Physical layer health monitoring and interface availability checks
- Network capacity planning through speed and MTU analysis
- ARP table monitoring for network scaling and address exhaustion detection
- Pre-maintenance validation of network interface configurations
- Performance baseline establishment for network infrastructure
- Identifying misconfigured interfaces or suboptimal network settings

Parameters:
- duration (default: "5m"): Time range for metrics collection using Prometheus format
- metric_name (optional): Query specific metric only (e.g., 'network_l1_node_arp_entries', 'network_l1_node_network_speed_bytes'). If not specified, collects all L1 metrics

Returns comprehensive Layer 1 network visibility across cluster infrastructure."""
)
async def query_network_l1_metrics(request: NetworkL1Request) -> Dict[str, Any]:
    """
    Collect comprehensive Layer 1 network metrics including interface status, speed, 
    MTU, and ARP entries across all cluster nodes grouped by role.
    """
    global prometheus_client, auth_manager, config
    try:
        if not prometheus_client or not auth_manager or not config:
            await initialize_components()
        
        logger.info(f"Collecting network L1 metrics for duration: {request.duration}")
        
        # Initialize utility and collector
        utility = mcpToolsUtility()
        collector = NetworkL1Collector(prometheus_client, config, utility)
        
        # Collect metrics with timeout
        if request.metric_name:
            # Collect specific metric
            results = await asyncio.wait_for(
                collector.get_metric_by_name(request.metric_name, request.duration),
                timeout=30.0
            )
        else:
            # Collect all metrics
            results = await asyncio.wait_for(
                collector.collect_all_metrics(request.duration),
                timeout=45.0
            )
        
        # Add collection metadata
        if 'error' not in results:
            results['collection_metadata'] = {
                'tool_name': 'query_network_l1_metrics',
                'duration': request.duration,
                'metric_name': request.metric_name,
                'collection_timestamp': datetime.now(timezone.utc).isoformat()
            }
        
        logger.info(f"Network L1 metrics collection completed - "
                   f"Metric: {request.metric_name or 'all'}, Duration: {request.duration}")
        
        return results
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting network L1 metrics - cluster may be experiencing issues",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 45 if not request.metric_name else 30,
            "suggestion": "Try reducing the duration parameter or check cluster network connectivity"
        }
    except Exception as e:
        logger.error(f"Error collecting network L1 metrics: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_network_l1_metrics"
        }

@app.tool(
    name="query_network_socket_tcp_stats",
    description="""Collect comprehensive TCP socket statistics across cluster nodes including allocated sockets, in-use connections, orphaned connections, time-wait states, and fragmentation metrics. Provides per-node socket resource consumption with statistical analysis grouped by node role for network capacity planning and connection tracking.

TCP SOCKET METRICS COLLECTED:
- socket_tcp_allocated: Total TCP sockets allocated by the kernel for connection tracking
- socket_tcp_inuse: Currently active TCP connections and listening sockets
- socket_tcp_orphan: Orphaned TCP connections not attached to any process file descriptor
- socket_tcp_tw: TCP connections in TIME_WAIT state awaiting timeout before cleanup
- socket_used: Total sockets in use across all protocols for overall socket pressure
- socket_frag_inuse: IP fragment reassembly buffers currently in use
- socket_raw_inuse: Raw sockets in use for low-level network operations

NODE ORGANIZATION:
- Nodes automatically grouped by role: controlplane, worker, infra categories
- Statistical analysis with average and maximum values per node
- Top 3 worker nodes by maximum value highlighted (configurable to show all workers)
- Instance-to-node name mapping for cross-referencing

STATISTICAL ANALYSIS:
- Average and maximum values per metric per node
- Role-based aggregation for capacity planning
- Top worker identification for load balancing analysis

METADATA AND TIMESTAMPS:
- Collection timestamp in UTC for data correlation
- Metric descriptions and units for interpretation
- Per-metric metadata including category and timezone

Parameters:
- include_all_workers (default: false): Show all worker nodes instead of top 3 by max value
- metric_filter (optional): List of specific metrics to collect. Available: ['socket_tcp_allocated', 'socket_tcp_inuse', 'socket_tcp_orphan', 'socket_tcp_tw', 'socket_used', 'socket_frag_inuse', 'socket_raw_inuse']

Use this tool for:
- Network capacity planning and socket resource monitoring
- Connection tracking and TCP state distribution analysis
- Identifying socket exhaustion and connection leaks
- Monitoring TIME_WAIT accumulation and potential port exhaustion
- Orphaned connection detection indicating application issues
- Network performance troubleshooting and optimization
- Load balancing verification across worker nodes"""
)
async def query_network_socket_tcp_stats(request: NetworkSocketTCPRequest) -> Dict[str, Any]:
    """
    Collect comprehensive TCP socket statistics across cluster nodes including 
    allocated sockets, in-use connections, orphaned connections, time-wait states, 
    and fragmentation metrics.
    """
    global config, auth_manager
    try:
        if not config or not auth_manager:
            await initialize_components()
        
        logger.info("Collecting TCP socket statistics...")
        
        async with socketStatTCPCollector(config, auth_manager) as collector:
            # Collect all metrics with timeout
            socket_data = await asyncio.wait_for(
                collector.collect_all_tcp_metrics(),
                timeout=30.0
            )
            
            # Apply metric filtering if requested
            if request.metric_filter:
                filtered_metrics = {
                    k: v for k, v in socket_data.get('metrics', {}).items()
                    if k in request.metric_filter
                }
                socket_data['metrics'] = filtered_metrics
                logger.info(f"Applied metric filter: {len(filtered_metrics)} metrics selected")
            
            # Add collection metadata
            socket_data['collection_metadata'] = {
                'tool_name': 'query_network_socket_tcp_stats',
                'parameters_applied': {
                    'include_all_workers': request.include_all_workers,
                    'metric_filter': request.metric_filter or 'all'
                },
                'metrics_collected': len(socket_data.get('metrics', {})),
                'worker_limit': 'none' if request.include_all_workers else 'top_3'
            }
            
            # Log summary
            metrics_count = len(socket_data.get('metrics', {}))
            successful_metrics = sum(
                1 for m in socket_data.get('metrics', {}).values()
                if isinstance(m, dict) and 'error' not in m
            )
            
            logger.info(f"TCP socket collection completed - "
                       f"{successful_metrics}/{metrics_count} metrics successful")
            
            return socket_data
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting TCP socket statistics",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 30,
            "suggestion": "Check cluster node responsiveness and Prometheus availability"
        }
    except Exception as e:
        logger.error(f"Error collecting TCP socket statistics: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_network_socket_tcp_stats"
        }

@app.tool(
    name="query_network_socket_memory_stats",
    description="""Get comprehensive network socket memory statistics across all cluster nodes including TCP/UDP kernel buffer memory usage, fragment memory consumption, and memory page allocations grouped by node role (controlplane/worker/infra/workload).

This tool provides detailed socket memory layer metrics essential for diagnosing memory exhaustion issues, buffer bloat problems, and kernel memory pressure related to network operations.

SOCKET MEMORY METRICS COLLECTED:
- FRAG memory usage (node_sockstat_FRAG_memory): Fragment reassembly buffer memory consumption
- TCP kernel buffer memory in pages (node_sockstat_TCP_mem): TCP socket buffer memory allocation in pages
- UDP kernel buffer memory in pages (node_sockstat_UDP_mem): UDP socket buffer memory allocation in pages  
- TCP kernel buffer memory in bytes (node_sockstat_TCP_mem_bytes): TCP socket buffer size in bytes for direct analysis
- UDP kernel buffer memory in bytes (node_sockstat_UDP_mem_bytes): UDP socket buffer size in bytes for capacity planning
- Per-node memory consumption patterns showing average and maximum utilization
- Role-based memory distribution for infrastructure capacity assessment

NODE GROUPING:
- Nodes automatically categorized by role: controlplane, worker, infra, workload
- Top 3 worker nodes by maximum memory usage (configurable to show all workers)
- All controlplane, infra, and workload nodes included for complete visibility
- Node-level granularity with avg/max statistics per node for trend analysis

STATISTICAL ANALYSIS:
- Average socket memory usage over query duration for baseline establishment
- Maximum socket memory usage to identify peak demand periods and potential exhaustion risks
- Per-node and per-role aggregation for capacity planning and memory allocation optimization
- Unit tracking (bytes, pages, count) for all metrics with appropriate conversions

MEMORY PRESSURE INDICATORS:
- Fragment buffer memory tracking for packet reassembly overhead analysis
- TCP buffer memory consumption for connection-heavy workload assessment
- UDP buffer memory usage for datagram-intensive application monitoring
- Kernel page allocation patterns for system-level memory pressure detection
- Buffer memory growth trends indicating potential memory leaks or configuration issues

OPERATIONAL INSIGHTS:
- Identify nodes approaching kernel socket memory limits (net.core.rmem/wmem parameters)
- Detect excessive fragment buffer usage indicating fragmentation problems or attacks
- Monitor TCP/UDP buffer memory for capacity planning and tuning opportunities
- Analyze socket memory distribution for load balancing and workload placement decisions
- Correlate memory usage with network performance degradation and packet drops

Parameters:
- duration (default: "1h"): Query duration (5m, 15m, 1h, 3h, 1d, 7d)
- end_time (optional): End time in ISO format - defaults to current time
- start_time (optional): Start time in ISO format - calculated from duration if not provided
- include_all_workers (default: false): Show all worker nodes instead of top 3

Use this tool for:
- Diagnosing kernel socket memory exhaustion causing connection failures or packet drops
- Monitoring TCP/UDP buffer memory for high-throughput or connection-intensive workloads
- Detecting fragment buffer exhaustion from IP fragmentation (PMTU issues, fragmentation attacks)
- Capacity planning for socket buffer memory (rmem_max, wmem_max tuning)
- Identifying memory leaks in kernel socket subsystem or misconfigured applications
- Performance tuning for OVN-Kubernetes overlay networking (Geneve encapsulation overhead)
- Troubleshooting "no buffer space available" errors and socket allocation failures
- Pre-scaling analysis to ensure adequate kernel memory for increased network load
- Correlating socket memory usage with application performance and connection issues

Returns JSON with category, timestamp, and metrics array containing per-metric data with node-grouped memory statistics in both bytes and pages where applicable."""
)
async def query_network_socket_memory_stats(request: NetworkSocketMemRequest) -> Dict[str, Any]:
    """
    Get comprehensive network socket memory statistics across all cluster nodes grouped by role.
    Provides TCP/UDP buffer memory, fragment memory, and kernel page allocation metrics.
    """
    global prometheus_client, auth_manager, config
    try:
        if not prometheus_client or not auth_manager or not config:
            await initialize_components()
        
        logger.info(f"Collecting socket memory statistics for duration: {request.duration}")
        
        # Initialize utility and collector
        from tools.utils.promql_utility import mcpToolsUtility
        utility = mcpToolsUtility(auth_client=auth_manager)
        
        async with prometheus_client:
            collector = socketStatMemCollector(prometheus_client, config, utility)
            
            # Collect metrics with timeout
            mem_stats = await asyncio.wait_for(
                collector.collect_all_metrics(),
                timeout=30.0
            )
            
            # Apply worker filtering if needed
            if not request.include_all_workers and 'metrics' in mem_stats:
                for metric_data in mem_stats['metrics']:
                    if 'data' in metric_data and 'worker' in metric_data['data']:
                        workers = metric_data['data']['worker']
                        if len(workers) > 3:
                            # Keep top 3 by max value
                            metric_data['data']['worker'] = sorted(
                                workers,
                                key=lambda x: x.get('max', 0),
                                reverse=True
                            )[:3]
            
            # Add query metadata
            mem_stats['query_info'] = {
                'duration': request.duration,
                'start_time': request.start_time,
                'end_time': request.end_time,
                'include_all_workers': request.include_all_workers,
                'timezone': 'UTC'
            }
            
            # Log summary
            total_metrics = len(mem_stats.get('metrics', []))
            total_nodes = sum(
                len(m.get('data', {}).get(role, []))
                for m in mem_stats.get('metrics', [])
                for role in ['controlplane', 'worker', 'infra', 'workload']
            )
            
            logger.info(f"Socket memory stats collection completed - "
                       f"Metrics: {total_metrics}, Total node readings: {total_nodes}")
            
            return mem_stats
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting socket memory statistics",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 30,
            "suggestion": "Try reducing the duration or check Prometheus responsiveness"
        }
    except Exception as e:
        logger.error(f"Error collecting socket memory statistics: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_network_socket_memory_stats"
        }

@app.tool(
    name="query_network_socket_memory",
    description="""Get comprehensive network socket memory statistics including TCP/UDP kernel buffer memory usage, FRAG memory, and socket memory allocation across all cluster nodes grouped by role. This tool provides critical insights into network stack memory consumption for performance optimization and troubleshooting network-related issues.

SOCKET MEMORY METRICS COLLECTED:
- node_sockstat_FRAG_memory: Socket fragment memory usage indicating packet reassembly buffer consumption
- TCP_Kernel_Buffer_Memory_Pages: TCP kernel buffer memory in pages for send/receive buffer analysis
- UDP_Kernel_Buffer_Memory_Pages: UDP kernel buffer memory in pages for datagram processing
- node_sockstat_TCP_mem_bytes: TCP socket memory allocation in bytes for detailed memory tracking
- node_sockstat_UDP_mem_bytes: UDP socket memory allocation in bytes for UDP traffic analysis

NODE ORGANIZATION:
- Nodes automatically grouped by role: controlplane, worker, infra, and workload categories
- All nodes returned for controlplane, infra, and workload roles with complete memory statistics
- Configurable top N worker nodes by maximum memory usage (default: top 3) for focused analysis
- Role-based analysis enables targeted capacity planning and performance tuning

STATISTICAL ANALYSIS:
- Average memory usage: Mean socket memory consumption over query period for baseline analysis
- Maximum memory usage: Peak socket memory allocation for capacity planning and spike detection
- Per-node granular statistics for identifying memory hotspots and resource imbalances
- Cross-node comparison for load distribution analysis and optimization opportunities
- Optional statistics filtering to reduce response payload size when only raw values needed

METADATA AND TIMESTAMPS:
- Query execution timestamp in UTC for data correlation with other monitoring systems
- Timezone information (UTC) for consistent time-based analysis across global deployments
- Per-metric collection metadata including unit (bytes/pages/count) and descriptions
- Category classification (network_socket_mem) for organized metrics management

Parameters:
- include_statistics (default: true): Include avg and max statistical analysis for each node's socket memory metrics
- worker_top_n (default: 3, range: 1-10): Number of top worker nodes to return by maximum socket memory usage
- include_all_roles (default: true): Include controlplane/infra/workload nodes or only workers for focused analysis
- metric_filter (optional): List of specific metrics to collect. If not specified, collects all 5 socket memory metrics

Use this tool for:
- Identifying socket memory exhaustion issues causing network performance degradation or packet drops
- Capacity planning for network-intensive workloads requiring large socket buffers
- Troubleshooting TCP/UDP performance issues related to insufficient kernel buffer memory
- Detecting memory leaks in network stack or socket handling code
- Pre-deployment validation ensuring adequate socket memory for expected traffic patterns
- Load balancing analysis by comparing socket memory usage across worker nodes
- Performance optimization by identifying nodes with excessive socket memory consumption
- Network stack tuning recommendations based on actual memory utilization patterns
- Compliance monitoring for socket memory allocation policies and limits
- Focused troubleshooting by filtering specific metrics or limiting to top consumers

The tool provides instant visibility into network socket memory consumption essential for maintaining optimal network performance and preventing memory-related network issues in OpenShift clusters."""
)
async def query_network_socket_memory(request: NetworkSocketMemRequest) -> Dict[str, Any]:
    """
    Get comprehensive network socket memory statistics including TCP/UDP kernel buffer 
    memory usage, FRAG memory, and socket memory allocation across all cluster nodes 
    grouped by role.
    
    Provides critical insights into network stack memory consumption for performance 
    optimization and troubleshooting.
    """
    global auth_manager, config
    try:
        if not auth_manager or not config:
            await initialize_components()
        
        logger.info(f"Collecting network socket memory metrics - "
                   f"worker_top_n={request.worker_top_n}, "
                   f"include_all_roles={request.include_all_roles}, "
                   f"metric_filter={request.metric_filter}")
        
        # Initialize socket memory collector
        async with socketStatMemCollector(config=config, auth=auth_manager) as collector:
            # Update collector settings based on request
            if request.worker_top_n != 3:
                collector.worker_top_n = request.worker_top_n
            
            # Collect all socket memory metrics with timeout
            socket_mem_data = await asyncio.wait_for(
                collector.collect_all_metrics(),
                timeout=30.0
            )
            
            # Apply filtering based on request parameters
            if request.metric_filter:
                # Filter metrics to only requested ones
                filtered_metrics = []
                for metric in socket_mem_data.get('metrics', []):
                    if metric.get('metric') in request.metric_filter:
                        filtered_metrics.append(metric)
                socket_mem_data['metrics'] = filtered_metrics
                socket_mem_data['metrics_count'] = len(filtered_metrics)
                logger.info(f"Filtered to {len(filtered_metrics)} requested metrics")
            
            if not request.include_all_roles:
                # Remove non-worker roles from results
                for metric in socket_mem_data.get('metrics', []):
                    if 'nodes' in metric:
                        metric['nodes'] = {
                            'worker': metric['nodes'].get('worker', {})
                        }
                logger.info("Filtered to worker nodes only")
            
            if not request.include_statistics:
                # Remove statistical analysis, keep only node names and roles
                for metric in socket_mem_data.get('metrics', []):
                    if 'nodes' in metric:
                        for role, nodes in metric['nodes'].items():
                            for node_name in list(nodes.keys()):
                                # Keep node but remove stats
                                nodes[node_name] = {'included': True}
                logger.info("Statistics removed from response")
            
            # Add request metadata
            socket_mem_data['request_parameters'] = {
                'include_statistics': request.include_statistics,
                'worker_top_n': request.worker_top_n,
                'include_all_roles': request.include_all_roles,
                'metric_filter_applied': request.metric_filter is not None,
                'filtered_metric_count': len(request.metric_filter) if request.metric_filter else None
            }
            
            # Log collection summary
            metrics_count = socket_mem_data.get('metrics_count', 0)
            total_nodes = 0
            for metric in socket_mem_data.get('metrics', []):
                if 'nodes' in metric:
                    for role, nodes in metric['nodes'].items():
                        total_nodes += len(nodes)
            
            logger.info(f"Socket memory collection completed - Metrics: {metrics_count}, "
                       f"Total node measurements: {total_nodes}")
            
            return socket_mem_data
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting socket memory metrics - cluster may be experiencing issues",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 30,
            "suggestion": "Check Prometheus connectivity and cluster responsiveness"
        }
    except Exception as e:
        logger.error(f"Error collecting socket memory metrics: {e}")
        return {
            "error": str(e), 
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_network_socket_memory"
        }

@app.tool(
    name="query_network_socket_softnet_stats",
    description="""Get comprehensive network socket softnet statistics including packet processing rates, drops, CPU RPS, and flow limits across all cluster nodes grouped by role. This tool provides critical insights into network stack performance and identifies potential bottlenecks in packet processing at the softnet layer.

SOFTNET METRICS COLLECTED:
- Softnet processed packets total: Rate of packets successfully processed by softnet per second
- Softnet dropped packets total: Rate of packets dropped by softnet due to backlog/quota exhaustion
- Softnet out of quota events: Rate of times softnet processing exceeded its quota (times_squeezed)
- Softnet CPU RPS (Receive Packet Steering): Rate of packets steered to different CPUs for processing
- Softnet flow limit count: Rate of flow limit events indicating network stack congestion

NODE ORGANIZATION:
- Nodes automatically grouped by role: controlplane, worker, infra, and workload categories
- Role-based statistics providing per-role network stack performance analysis
- Individual node metrics with average and maximum values over the time period
- Top 3 worker nodes by maximum usage for focused performance analysis

STATISTICAL ANALYSIS:
- Average and maximum packet rates for each metric over the specified time period
- Per-node breakdown showing both sustained load (avg) and peak burst behavior (max)
- Time-based trend analysis for identifying network stack pressure patterns
- Cross-node comparison for load distribution and bottleneck identification

PERFORMANCE INDICATORS:
- High dropped packet rates indicate softnet backlog exhaustion or insufficient CPU resources
- Frequent out-of-quota events suggest network processing limits are being hit
- RPS distribution shows packet steering effectiveness across CPU cores
- Flow limit counts reveal network stack congestion and potential tuning opportunities

METADATA AND TIMESTAMPS:
- Query execution timestamp in UTC timezone for data correlation
- Duration coverage with time range details (start, end, step) for analysis context
- Summary statistics including total metrics collected and node counts per role
- Data freshness indicators and collection success metrics

Parameters:
- duration (default: "1h"): Query duration using Prometheus time format (e.g., "5m", "15m", "1h", "3h", "1d") - longer durations provide better trend analysis
- end_time (optional): End time in ISO format (YYYY-MM-DDTHH:MM:SSZ) for historical analysis - defaults to current time
- start_time (optional): Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ) - automatically calculated from duration and end_time if not provided

Use this tool for:
- Network stack performance monitoring and bottleneck identification
- Packet drop analysis to identify nodes experiencing network processing issues
- CPU RPS effectiveness evaluation for packet processing distribution
- Network tuning validation after sysctl or kernel parameter changes
- Capacity planning by understanding network processing limits per node
- Troubleshooting network performance degradation and packet loss issues
- Pre-production performance validation for network-intensive workloads
- Identifying nodes requiring network stack tuning or hardware upgrades

The tool provides detailed softnet-layer visibility essential for diagnosing network performance issues at the kernel networking stack level."""
)
async def query_network_socket_softnet_stats(request: MetricsRequest) -> Dict[str, Any]:
    """
    Get comprehensive network socket softnet statistics including packet processing rates,
    drops, CPU RPS, and flow limits across all cluster nodes grouped by role.
    
    Provides critical insights into network stack performance and identifies potential
    bottlenecks in packet processing at the softnet layer.
    """
    global prometheus_client, auth_manager, config
    try:
        if not prometheus_client or not auth_manager or not config:
            await initialize_components()
        
        logger.info(f"Collecting softnet statistics for duration: {request.duration}")
        
        # Calculate time range
        from datetime import timedelta
        end_time = datetime.now(timezone.utc)
        if request.end_time:
            end_time = datetime.fromisoformat(request.end_time.replace('Z', '+00:00'))
        
        duration_delta = prometheus_client.parse_duration(request.duration)
        start_time = end_time - duration_delta
        
        if request.start_time:
            start_time = datetime.fromisoformat(request.start_time.replace('Z', '+00:00'))
        
        start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # Collect all softnet metrics with timeout
        softnet_data = await asyncio.wait_for(
            collect_softnet_statistics(auth_manager, config, start_str, end_str, step='15s'),
            timeout=45.0
        )
        
        # Sanitize for JSON compatibility
        softnet_data = _sanitize_json_compat(softnet_data)
        
        # Log collection summary
        summary = softnet_data.get('summary', {})
        metrics_collected = summary.get('metrics_with_data', 0)
        total_nodes = summary.get('total_unique_nodes', 0)
        
        logger.info(f"Softnet statistics collection completed - "
                   f"Metrics: {metrics_collected}/5, Total nodes: {total_nodes}")
        
        return softnet_data
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting softnet statistics - cluster may be experiencing issues",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 45,
            "suggestion": "Try reducing the duration parameter or check cluster responsiveness"
        }
    except Exception as e:
        logger.error(f"Error collecting softnet statistics: {e}")
        return {
            "error": str(e), 
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_network_socket_softnet_stats"
        }

@app.tool(
    name="query_network_socket_ip_metrics",
    description="""Get comprehensive network socket IP-level statistics including netstat IP traffic metrics and ICMP message statistics across all cluster nodes grouped by role (controlplane/worker/infra/workload). This tool provides detailed network layer metrics for monitoring IP traffic patterns, ICMP operations, and identifying network communication issues at the IP protocol level.

METRICS COLLECTED:
- IP incoming octets per second (bytes_per_second) - Rate of incoming IP traffic for bandwidth monitoring
- IP outgoing octets per second (bytes_per_second) - Rate of outgoing IP traffic for bandwidth analysis
- ICMP incoming messages per second (packets_per_second) - Rate of incoming ICMP messages including pings and errors
- ICMP outgoing messages per second (packets_per_second) - Rate of outgoing ICMP messages for connectivity testing
- ICMP incoming errors per second (packets_per_second) - Rate of ICMP errors indicating network issues

NODE ORGANIZATION:
- Nodes automatically grouped by role: controlplane, worker, infra, workload
- Worker nodes: Returns top 3 by maximum usage for focused analysis
- Other roles: Returns all nodes for comprehensive monitoring
- Per-node statistics include average and maximum values over time range

STATISTICAL ANALYSIS:
- Average (avg) values showing typical traffic patterns
- Maximum (max) values identifying traffic peaks and spikes
- Per-node granularity for pinpointing specific network issues
- Time-based trend data for capacity planning

METADATA:
- Query execution timestamp in UTC timezone
- Time range coverage (start, end, step) for analysis context
- Node groups summary showing count per role
- Unit information for each metric

Parameters:
- duration (default: "1h"): Query duration in Prometheus format (5m, 1h, 6h, 1d, 7d)
- start_time (optional): Start time in ISO format for historical analysis
- end_time (optional): End time in ISO format, defaults to current time
- step (default: "15s"): Query resolution step width for data granularity

Use this tool for:
- Network traffic pattern analysis and bandwidth monitoring at IP layer
- ICMP connectivity troubleshooting and ping monitoring
- Network error detection through ICMP error rate tracking
- Capacity planning based on IP traffic trends
- Network performance baseline establishment
- Identifying nodes with abnormal network behavior
- Cross-node network load distribution analysis
- Network security monitoring through traffic pattern analysis"""
)
async def query_network_socket_ip_metrics(request: NetworkSocketIPRequest) -> Dict[str, Any]:
    """
    Get comprehensive network socket IP-level statistics including netstat IP traffic 
    metrics and ICMP message statistics across all cluster nodes grouped by role.
    
    Provides detailed network layer metrics for monitoring IP traffic patterns, 
    ICMP operations, and network communication issues.
    """
    global prometheus_client, auth_manager, config
    try:
        if not prometheus_client or not auth_manager or not config:
            await initialize_components()
        
        logger.info(f"Collecting network socket IP metrics for duration: {request.duration}")
        
        # Initialize the socket IP collector
        async with socketStatIPCollector(auth_manager, config) as collector:
            # Calculate time range
            from datetime import timedelta
            end_time = datetime.now(timezone.utc)
            if request.end_time:
                end_time = datetime.fromisoformat(request.end_time.replace('Z', '+00:00'))
            
            duration_delta = prometheus_client.parse_duration(request.duration)
            start_time = end_time - duration_delta
            
            if request.start_time:
                start_time = datetime.fromisoformat(request.start_time.replace('Z', '+00:00'))
            
            start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            
            # Collect all socket IP metrics with timeout
            metrics_data = await asyncio.wait_for(
                collector.collect_all_socket_ip_metrics(start_str, end_str, request.step),
                timeout=45.0
            )
            
            # Log collection summary
            total_nodes = sum(metrics_data.get('node_groups_summary', {}).values())
            metrics_collected = len([m for m in metrics_data.get('metrics', {}).values() 
                                   if not isinstance(m, dict) or 'error' not in m])
            
            logger.info(f"Network socket IP metrics collection completed - "
                       f"Total nodes: {total_nodes}, Metrics collected: {metrics_collected}/5")
            
            return metrics_data
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting network socket IP metrics - cluster may be experiencing issues",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 45,
            "suggestion": "Try reducing the duration parameter or check cluster network responsiveness"
        }
    except Exception as e:
        logger.error(f"Error collecting network socket IP metrics: {e}")
        return {
            "error": str(e), 
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_network_socket_ip_metrics"
        }

@app.tool(
    name="query_network_netstat_tcp_metrics",
    description="""Collect comprehensive network netstat TCP metrics including TCP segments, connection states, error conditions, and SYN cookie statistics across all cluster nodes grouped by role. This tool provides detailed TCP stack performance analysis with statistical insights (avg/max) for network troubleshooting and capacity planning.

TCP SEGMENT METRICS:
- TCP in/out segments per second for throughput analysis
- Segment transmission rates and patterns for traffic analysis
- Direction-specific traffic flow monitoring

TCP ERROR METRICS:
- Listen overflow and drops indicating backlog saturation
- SYN retransmissions for connection establishment issues
- Segment retransmissions for network reliability problems
- Receive errors and RST errors for protocol-level failures
- Receive queue drops indicating buffer pressure
- Out-of-order queue events for packet reordering issues
- TCP timeout events for connection stability monitoring

TCP CONNECTION METRICS:
- Maximum connection limits per node
- Current established connections for load monitoring
- Connection state distribution and trends

TCP SYN COOKIE METRICS:
- SYN cookie failures for SYN flood protection analysis
- SYN cookies validated for attack mitigation verification
- SYN cookies sent for protection activation tracking

NODE ORGANIZATION:
- Nodes grouped by role: controlplane, worker, infra, workload
- Top 3 worker nodes by maximum metric value for focused analysis
- All nodes reported for controlplane, infra, and workload roles
- Role-based resource consumption patterns

STATISTICAL ANALYSIS:
- Average values over time period for baseline understanding
- Maximum values for peak load identification
- Per-node metrics with unit specifications
- Summary statistics across all metrics and node groups

Parameters:
- duration (default: "1h"): Query duration (e.g., "5m", "1h", "3h", "1d")
- start_time (optional): Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
- end_time (optional): End time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
- step (default: "15s"): Query resolution step interval

Use this tool for:
- TCP stack performance analysis and bottleneck identification
- Network error troubleshooting and root cause analysis
- SYN flood attack detection and mitigation verification
- Connection capacity planning and limit management
- TCP retransmission analysis for network quality assessment
- Buffer and queue management optimization
- Protocol-level network health monitoring
- Identifying nodes with TCP stack issues or misconfigurations

Returns JSON format with category, timestamp, time range, detailed metrics per node grouped by role, and summary statistics."""
)
async def query_network_netstat_tcp_metrics(request: NetStatTCPRequest) -> Dict[str, Any]:
    """
    Collect comprehensive network netstat TCP metrics including TCP segments, 
    connection states, error conditions, and SYN cookie statistics across 
    all cluster nodes grouped by role.
    """
    global prometheus_client, auth_manager, config
    try:
        if not prometheus_client or not auth_manager or not config:
            await initialize_components()
        
        logger.info(f"Collecting network netstat TCP metrics for duration: {request.duration}")
        
        # Calculate time range
        end_time = datetime.now(timezone.utc)
        if request.end_time:
            end_time = datetime.fromisoformat(request.end_time.replace('Z', '+00:00'))
        
        duration_delta = prometheus_client.parse_duration(request.duration)
        start_time = end_time - duration_delta
        
        if request.start_time:
            start_time = datetime.fromisoformat(request.start_time.replace('Z', '+00:00'))
        
        start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # Collect all netstat TCP metrics with timeout
        async with netStatTCPCollector(auth_manager, config) as collector:
            tcp_metrics = await asyncio.wait_for(
                collector.collect_all_netstat_tcp_metrics(start_str, end_str, request.step),
                timeout=60.0
            )
        
        # Log collection summary
        total_metrics = tcp_metrics.get('summary', {}).get('total_metrics', 0)
        node_counts = tcp_metrics.get('summary', {}).get('node_counts', {})
        total_nodes = sum(node_counts.values())
        
        logger.info(f"Network netstat TCP metrics collection completed - "
                   f"Metrics: {total_metrics}, Total nodes: {total_nodes}")
        
        # Sanitize JSON for NaN/Inf values
        return _sanitize_json_compat(tcp_metrics)
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting network netstat TCP metrics - cluster may be experiencing issues",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 60,
            "suggestion": "Try reducing the duration parameter or check cluster responsiveness"
        }
    except Exception as e:
        logger.error(f"Error collecting network netstat TCP metrics: {e}")
        return {
            "error": str(e), 
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_network_netstat_tcp_metrics"
        }


@app.tool(
    name="query_cluster_node_usage",
    description="""Get comprehensive cluster node resource usage metrics including CPU utilization, memory consumption, and cgroup-level resource tracking across all worker, control plane, and infrastructure nodes grouped by role. This tool provides both instant snapshots and duration-based node performance metrics with statistical analysis (min/avg/max) for capacity planning and performance monitoring.

RESOURCE METRICS COLLECTED:
- CPU usage percentages per node with mode breakdown (user, system, iowait, etc.) and statistical analysis
- Cgroup CPU usage for system services (kubelet, ovs-vswitchd, crio, ovsdb-server, systemd services, kubepods)
- Memory utilization including total used memory calculated from MemTotal minus (MemFree + Buffers + Cached)
- Memory cache and buffer usage for system performance analysis
- Cgroup RSS memory usage per system service and kubepods slice
- Per-node resource consumption trends and patterns for capacity planning analysis

NODE ORGANIZATION:
- Nodes automatically grouped by role: controlplane, worker, infra, and workload categories
- Role-based summary statistics providing group-level resource utilization overview
- Individual node metrics with instance mapping from Prometheus endpoints to node names
- Node count and distribution analysis across different roles for infrastructure planning

TOP RESOURCE CONSUMERS:
- Top 5 worker nodes by maximum CPU usage with mode breakdown and average utilization metrics
- Top 5 worker nodes by maximum memory usage with both peak and average consumption data
- Top 5 worker nodes by cgroup CPU and RSS usage for service-level resource analysis
- Resource ranking helps identify nodes requiring attention or optimization
- Performance comparison data for load balancing and capacity distribution analysis

STATISTICAL ANALYSIS:
- Average and maximum values for each metric over the specified time period
- Time-based trend analysis showing resource utilization patterns and peaks
- Group-level aggregated statistics for role-based capacity planning
- Per-cgroup resource breakdown for system service optimization

METADATA AND TIMESTAMPS:
- Query execution timestamp and timezone information (UTC) for data correlation
- Duration coverage and time range details (start, end, step) for analysis context
- Data freshness indicators and collection success metrics
- Instance-to-node name mapping for cross-referencing with other monitoring systems
- Node groups summary showing node count per role

Parameters:
- duration (default: "1h"): Query duration using Prometheus time format (e.g., "5m", "15m", "1h", "3h", "1d", "7d") - longer durations provide better trend analysis but require more processing time
- end_time (optional): End time in ISO format (YYYY-MM-DDTHH:MM:SSZ) for historical analysis - defaults to current time for recent data analysis
- start_time (optional): Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ) - automatically calculated from duration and end_time if not provided
- step (default: "15s"): Query step interval for Prometheus range queries - controls data point granularity

Use this tool for:
- Identifying node-level resource bottlenecks and capacity constraints before they impact applications
- System service resource consumption analysis (kubelet, OVS, CRI-O performance monitoring)
- Capacity planning and resource allocation optimization based on historical usage patterns
- Performance monitoring and trend analysis for proactive infrastructure management
- Load balancing analysis to identify unevenly distributed workloads across nodes
- Infrastructure health monitoring and operational dashboard integration
- Pre-maintenance planning by identifying high-resource utilization nodes
- Cost optimization analysis by understanding actual vs. allocated resource usage
- Troubleshooting cluster performance issues through node-level resource correlation

The tool provides comprehensive node-level visibility with cgroup-level granularity essential for effective OpenShift cluster resource management and operational excellence."""
)

async def query_cluster_node_usage(request: MetricsRequest) -> Dict[str, Any]:
    """
    Get comprehensive cluster node resource usage metrics including CPU utilization, 
    memory consumption, and cgroup-level resource tracking across all worker, control plane, 
    and infrastructure nodes grouped by role.
    
    Provides both instant snapshots and duration-based node performance metrics with 
    statistical analysis for capacity planning and performance monitoring.
    """
    global prometheus_client, auth_manager, config
    try:
        if not prometheus_client or not auth_manager or not config:
            await initialize_components()
        
        logger.info(f"Collecting node usage data for duration: {request.duration}")
        
        # Initialize the new node usage collector
        async with nodeUsageCollector(auth_manager, config) as collector:
            # Calculate time range
            from datetime import timedelta
            end_time = datetime.now(timezone.utc)
            if request.end_time:
                end_time = datetime.fromisoformat(request.end_time.replace('Z', '+00:00'))
            
            duration_delta = prometheus_client.parse_duration(request.duration)
            start_time = end_time - duration_delta
            
            if request.start_time:
                start_time = datetime.fromisoformat(request.start_time.replace('Z', '+00:00'))
            
            start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            
            # Collect all node usage metrics with timeout
            usage_data = await asyncio.wait_for(
                collector.collect_all_node_usage_metrics(start_str, end_str, step='15s'),
                timeout=45.0
            )
            
            # Log collection summary for operational visibility
            total_nodes = sum(usage_data.get('node_groups_summary', {}).values())
            metrics_collected = len([m for m in usage_data.get('metrics', {}).values() 
                                   if not isinstance(m, dict) or 'error' not in m])
            
            logger.info(f"Node usage collection completed - Total nodes: {total_nodes}, "
                       f"Metrics collected: {metrics_collected}/5")
            
            return usage_data
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting node usage metrics - cluster may be experiencing issues or have many nodes",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 45,
            "suggestion": "Try reducing the duration parameter or check cluster node responsiveness"
        }
    except Exception as e:
        logger.error(f"Error collecting node usage metrics: {e}")
        return {
            "error": str(e), 
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_cluster_node_usage"
        }

@app.tool(
    name="query_prometheus_basic_info",
    description="""Query comprehensive OVN-Kubernetes basic infrastructure information including OVN database metrics, cluster-wide pod status distribution, top alerts analysis, pod distribution across nodes, and network latency metrics from Prometheus. This tool provides essential baseline metrics and operational insights for OVN-K cluster health monitoring, capacity planning, and performance analysis.

COMPREHENSIVE METRICS COLLECTION:

OVN DATABASE METRICS (when include_db_metrics=True):
- OVN Northbound database size in bytes with maximum values across all instances for storage monitoring
- OVN Southbound database size in bytes with maximum values across all instances for capacity planning  
- Database size trends help monitor OVN control plane storage requirements and growth patterns
- Instance labels and metadata for identifying specific database locations and performance characteristics
- Critical for detecting database bloat, storage capacity planning, and performance optimization initiatives

CLUSTER-WIDE POD STATUS ANALYSIS (when include_pod_status=True):
- Complete pod phase distribution (Running, Pending, Failed, Succeeded, Unknown) across all namespaces
- Total pod count for cluster-wide capacity monitoring and workload density analysis
- Pod health indicators and failure pattern identification for operational monitoring
- Resource utilization insights through pod distribution patterns and scheduling effectiveness
- Essential for capacity planning, troubleshooting deployment issues, and cluster health assessment

TOP ALERTS MONITORING (when include_top_alerts=True):
- Top 6 active alerts ranked by severity and occurrence count for immediate operational attention
- Alert severity classification (critical, warning, info) with impact assessment and priority ranking
- Alert frequency analysis showing trending issues and recurring problems requiring systematic resolution
- Alert metadata including alert names, severity levels, occurrence counts, and timestamps
- Proactive issue identification enabling preventive maintenance and system reliability improvements

POD DISTRIBUTION ANALYSIS (when include_pod_distribution=True):
- Top 6 nodes by pod count showing workload distribution patterns and potential scheduling imbalances
- Node role identification (master, worker, infra) with workload characteristics and capacity utilization
- Per-node pod density analysis for load balancing insights and capacity optimization
- Node metadata including names, roles, pod counts, and timestamps for operational correlation
- Load balancing assessment and capacity planning guidance based on current distribution patterns

NETWORK LATENCY METRICS (when include_latency_metrics=True):
- API server request duration percentiles (99th percentile) for control plane performance monitoring
- etcd request duration percentiles for backend storage performance and responsiveness analysis
- OVN controller latency percentiles (95th percentile) for network control plane performance assessment
- Network RTT (Round Trip Time) percentiles for inter-component communication performance evaluation
- Custom latency metrics from optional metrics-latency.yml configuration file for specialized monitoring

OPERATIONAL METADATA AND INTEGRATION:
- Collection timestamp and timezone information (UTC) for data freshness verification and correlation
- Query execution success metrics and error reporting for individual component collection failures
- Structured JSON output format suitable for monitoring dashboard integration and automation systems
- Component-specific metadata including query types, measurement units, and result counts
- Data quality indicators and collection completeness reporting for operational confidence

FLEXIBLE CONFIGURATION OPTIONS:
- Custom metrics support through metric_name -> prometheus_query dictionary for specialized monitoring requirements
- Optional metrics file integration (metrics-latency.yml) for standardized latency monitoring configurations
- Selective component collection enabling focused analysis and reduced response times when needed
- Error isolation ensuring partial failures don't prevent successful collection of other components
- Extensible architecture supporting additional metric categories and specialized monitoring requirements

COMPREHENSIVE SUMMARY REPORTING:
- Unified JSON summary combining all collected metrics with correlation and cross-component analysis
- Statistical summaries and key performance indicators for executive reporting and dashboard integration
- Collection metadata including execution duration, success rates, and component health indicators
- Structured format enabling programmatic analysis, alerting integration, and automated reporting workflows
- Historical baseline establishment for trend analysis and performance degradation detection over time

Parameters:
- include_pod_status (default: true): Collect cluster-wide pod phase distribution and status information for workload monitoring and capacity planning
- include_db_metrics (default: true): Collect OVN Northbound and Southbound database size metrics for storage monitoring and growth analysis
- include_top_alerts (default: true): Collect top 6 active alerts by severity for immediate operational awareness and proactive issue identification
- include_pod_distribution (default: true): Collect top 6 nodes by pod count for load balancing analysis and capacity distribution assessment
- include_latency_metrics (default: true): Collect network and API latency percentiles for performance monitoring and bottleneck identification
- custom_metrics (optional): Dictionary of additional Prometheus queries in format {"metric_name": "prometheus_query"} for specialized monitoring
- metrics_file (optional): Path to metrics-latency.yml file containing standardized latency metric definitions for consistent monitoring
- comprehensive_collection (default: true): Collect all available metrics in single operation for complete infrastructure overview

SPECIALIZED USE CASES:
- Daily operational health monitoring combining infrastructure status, workload distribution, and performance metrics
- Capacity planning analysis using database growth, pod distribution, and resource utilization patterns
- Performance baseline establishment for SLA monitoring and trend analysis over time
- Alert correlation analysis combining active alerts with infrastructure metrics for root cause identification
- Load balancing optimization using pod distribution and node capacity analysis
- Network performance monitoring through latency metrics and OVN database responsiveness
- Executive reporting with comprehensive infrastructure health and performance summaries

INTEGRATION AND AUTOMATION:
- Monitoring dashboard data source for unified OVN-K infrastructure visibility
- Automated alerting system integration with threshold-based risk assessment capabilities
- Capacity planning automation using growth trends and utilization forecasting
- Performance regression detection through historical baseline comparison and trend analysis
- Operational runbook integration providing context for troubleshooting and incident response

The tool provides comprehensive OVN-K infrastructure baseline metrics essential for operational monitoring, capacity planning, performance analysis, and proactive issue identification, making it ideal for both real-time operations and strategic infrastructure management initiatives."""
)
async def query_prometheus_basic_info(request: PrometheusBasicInfoRequest) -> Dict[str, Any]:
    """
    Query comprehensive OVN-Kubernetes basic infrastructure information including OVN database metrics,
    cluster-wide pod status distribution, top alerts analysis, pod distribution across nodes,
    and network latency metrics from Prometheus.
    
    Provides essential baseline metrics and operational insights for OVN-K cluster health monitoring,
    capacity planning, and performance analysis.
    """
    global prometheus_client, auth_manager
    
    try:
        if not prometheus_client or not auth_manager:
            await initialize_components()
        
        logger.info("Starting comprehensive basic OVN infrastructure information collection...")
        
        # Initialize the enhanced OVN basic info collector
        collector = ovnBasicInfoCollector(
            auth_manager.prometheus_url, 
            auth_manager.prometheus_token
        )
        
        # Collect comprehensive summary with all metrics
        logger.debug("Collecting comprehensive metrics summary...")
        
        # Add timeout to prevent hanging during comprehensive collection
        comprehensive_summary = await asyncio.wait_for(
            collector.collect_comprehensive_summary(),
            timeout=60.0  # Extended timeout for comprehensive collection
        )
        
        # Apply parameter-based filtering
        results = {
            "collection_timestamp": comprehensive_summary.get("collection_timestamp"),
            "prometheus_url": comprehensive_summary.get("prometheus_url"),
            "tool_name": "query_prometheus_basic_info_enhanced"
        }
        
        # Include metrics based on request parameters
        if request.include_db_metrics:
            results["ovn_database_metrics"] = comprehensive_summary.get("metrics", {}).get("ovn_database", {})
            logger.info("OVN database metrics included in response")
        
        if request.include_pod_status:
            results["pod_status_metrics"] = comprehensive_summary.get("metrics", {}).get("pod_status", {})
            logger.info("Pod status metrics included in response")
        
        # Always include additional comprehensive metrics from the enhanced collector
        results["alerts_summary"] = comprehensive_summary.get("metrics", {}).get("alerts", {})
        results["pod_distribution"] = comprehensive_summary.get("metrics", {}).get("pod_distribution", {})
        results["latency_metrics"] = comprehensive_summary.get("metrics", {}).get("latency", {})
        
        # Handle custom metrics if specified
        if request.custom_metrics:
            try:
                logger.debug(f"Collecting {len(request.custom_metrics)} custom metrics...")
                
                custom_results = await asyncio.wait_for(
                    collector.collect_max_values(request.custom_metrics),
                    timeout=20.0
                )
                
                results["custom_metrics"] = custom_results
                logger.info(f"Custom metrics collected: {list(request.custom_metrics.keys())}")
                
            except asyncio.TimeoutError:
                logger.warning("Timeout collecting custom metrics")
                results["custom_metrics"] = {
                    "error": "Timeout collecting custom metrics",
                    "timeout_seconds": 20
                }
            except Exception as e:
                logger.error(f"Error collecting custom metrics: {e}")
                results["custom_metrics"] = {"error": str(e)}
        
        # Add comprehensive collection summary
        original_summary = comprehensive_summary.get("summary", {})
        results["collection_summary"] = {
            "total_metric_categories": original_summary.get("total_metric_categories", 5),
            "successful_collections": original_summary.get("successful_collections", 0),
            "failed_collections": original_summary.get("failed_collections", 0),
            "parameters_applied": {
                "include_pod_status": request.include_pod_status,
                "include_db_metrics": request.include_db_metrics,
                "custom_metrics_count": len(request.custom_metrics) if request.custom_metrics else 0
            },
            "collection_method": "comprehensive_enhanced"
        }
        
        # Log collection summary with enhanced metrics
        alerts_count = len(results.get("alerts_summary", {}).get("top_alerts", []))
        top_nodes_count = len(results.get("pod_distribution", {}).get("top_nodes", []))
        latency_metrics_count = len(results.get("latency_metrics", {}).get("metrics", {}))
        
        logger.info(f"Enhanced basic info collection completed - "
                   f"Alerts: {alerts_count}, Top nodes: {top_nodes_count}, "
                   f"Latency metrics: {latency_metrics_count}")
        
        return results
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting comprehensive basic information - cluster may be experiencing issues",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 60,
            "suggestion": "Try limiting the scope with selective parameters or check cluster responsiveness"
        }
    except Exception as e:
        logger.error(f"Error in enhanced basic info collection: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_prometheus_basic_info_enhanced"
        }

@app.tool(
    name="query_ovnk_pods_metrics",
    description="""Query OVN-Kubernetes pod resource usage metrics including CPU and memory consumption for ovnkube-controller, ovnkube-node, and related pods. This tool provides detailed resource utilization data for OVN-K components with intelligent pod filtering.

Parameters:
- pod_pattern (default: "ovnkube.*"): Regex pattern for pod names (e.g., "ovnkube.*", "multus.*", "ovnkube-node.*")
- container_pattern (default: ".*"): Regex pattern for container names (e.g., "ovnkube-controller", "kube-rbac-proxy.*")
- label_selector (default: ".*"): Regex pattern for pod label selectors
- namespace_pattern (default: "openshift-ovn-kubernetes"): Regex pattern for namespace filtering
- top_n (default: 10): Number of top resource consuming pods to return (1-50)
- duration (default: "1h"): Query duration for analysis (e.g., "5m", "1h", "1d")
- start_time (optional): Start time in ISO format for historical queries
- end_time (optional): End time in ISO format for historical queries

Returns detailed pod metrics including:
- Top resource consuming pods ranked by CPU and memory usage
- Per-pod CPU utilization percentages with min/avg/max statistics
- Memory usage in bytes with readable format (MB/GB)
- Pod metadata including node placement and resource limits
- Container-level resource breakdown within pods
- Performance trends over the specified duration

Use this tool to identify resource-intensive OVN-K pods, troubleshoot performance issues, or monitor resource consumption patterns."""
)
async def query_ovnk_pods_metrics(request: PODsRequest) -> Dict[str, Any]:
    """
    Query OVN-Kubernetes pod resource usage metrics including CPU, memory consumption,
    and performance characteristics for ovnkube-controller, ovnkube-node, and related pods.
    
    Provides detailed resource utilization data for OVN-K components.
    """
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()

        # Add timeout to prevent hanging
        pods_duration_summary = await asyncio.wait_for(
            collect_ovn_duration_usage(prometheus_client, request.duration,auth_manager),
            timeout=45.0
        )
        return pods_duration_summary
    
    except asyncio.TimeoutError:
        return {"error": "Timeout collecting pod metrics", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"Error querying pod metrics: {e}")
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}


@app.tool(
    name="query_ovnk_containers_metrics", 
    description="""Query detailed container-level metrics within OVN-Kubernetes pods including per-container CPU and memory usage, and resource limits utilization. This tool enables fine-grained analysis of individual container performance within OVN-K pods.

Parameters:
- pod_pattern (default: "ovnkube.*"): Regex pattern for pod names to analyze
- container_pattern (default: ".*"): Regex pattern for specific container names (e.g., "ovnkube-controller", "northd", "sbdb")
- label_selector (default: ".*"): Regex pattern for pod label selectors
- namespace_pattern (default: "openshift-ovn-kubernetes"): Target namespace pattern
- top_n (default: 10): Number of top containers to return based on resource usage
- duration (default: "1h"): Analysis time window (e.g., "5m", "1h", "1d") 
- start_time (optional): Historical query start time in ISO format
- end_time (optional): Historical query end time in ISO format

Returns container-level metrics including:
- Individual container CPU and memory usage within pods
- Resource limit utilization percentages
- Container restart counts and health status
- Performance comparison between containers in the same pod
- Resource allocation efficiency analysis
- Container-specific performance bottlenecks

Use this tool for deep-dive container analysis, identifying which specific containers within OVN-K pods are consuming the most resources, or troubleshooting container-level performance issues."""
)
async def query_ovnk_containers_metrics(request: PODsContainerRequest) -> Dict[str, Any]:
    """
    Query detailed container-level metrics within OVN-Kubernetes pods including
    per-container CPU, memory usage, and resource limits utilization.
    
    Enables fine-grained analysis of individual container performance within OVN-K pods.
    """
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
        
        collector = PodsUsageCollector(prometheus_client,auth_manager)
        
        # Add timeout to prevent hanging
        result = await asyncio.wait_for(
            collector.collect_duration_usage(
                request.duration, 
                request.pod_pattern, 
                request.container_pattern, 
                request.namespace_pattern
            ),
            timeout=45.0
        )
        return result
    
    except asyncio.TimeoutError:
        return {"error": "Timeout collecting container metrics", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"Error querying container metrics: {e}")
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}


@app.tool(
    name="query_ovnk_ovs_metrics",
    description="""Query Open vSwitch (OVS) performance metrics including CPU usage, memory consumption, flow table statistics, bridge statistics, and connection metrics. This tool is critical for monitoring OVS dataplane performance and flow processing efficiency.

Parameters:
- pod_pattern (default: "ovnkube.*"): Regex pattern for OVS-related pods
- container_pattern (default: ".*"): Container pattern for OVS components
- label_selector (default: ".*"): Label selector pattern
- namespace_pattern (default: "openshift-ovn-kubernetes"): Target namespace
- top_n (default: 10): Number of top results to return
- duration (default: "1h"): Analysis duration (e.g., "5m", "1h", "1d")
- start_time (optional): Historical query start time
- end_time (optional): Historical query end time

Returns comprehensive OVS metrics including:
- ovs-vswitchd CPU and memory usage per node
- ovsdb-server resource consumption 
- Dataplane flow counts (ovs_vswitchd_dp_flows_total)
- Bridge-specific flow statistics for br-int and br-ex
- OVS connection metrics (stream_open, rconn_overflow, rconn_discarded)
- Flow table efficiency and processing performance
- Per-node OVS component health status

Use this tool to monitor OVS dataplane performance, identify flow processing bottlenecks, troubleshoot network connectivity issues, or analyze OVS resource consumption patterns."""
)
async def query_ovnk_ovs_metrics(request: PODsRequest) -> Dict[str, Any]:
    """
    Query Open vSwitch (OVS) performance metrics including CPU usage, memory consumption,
    flow table statistics, bridge statistics, and connection metrics.
    
    Critical for monitoring OVS dataplane performance and flow processing efficiency.
    """
    global prometheus_client, auth_manager
    try:
        if not prometheus_client or not auth_manager:
            await initialize_components()
        
        collector = OVSUsageCollector(prometheus_client, auth_manager)
        
        # Add timeout to prevent hanging
        range_results = await asyncio.wait_for(
            collector.collect_all_ovs_metrics(request.duration),
            timeout=45.0
        )
        return range_results
    except asyncio.TimeoutError:
        return {"error": "Timeout collecting OVS metrics", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"Error querying OVS metrics: {e}")
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}


@app.tool(
    name="query_multus_metrics",
    description="""Query Multus CNI metrics including network attachment processing times, resource usage, and network interface management performance. This tool is essential for monitoring secondary network interface provisioning and management in multi-network environments.

Parameters:
- pod_pattern (default: "multus.*"): Regex pattern for Multus-related pods (e.g., "multus.*", "network-metrics.*")
- container_pattern (default: ".*"): Container pattern within Multus pods  
- label_selector (default: ".*"): Label selector for filtering pods
- namespace_pattern (default: "openshift-multus"): Target namespace for Multus components
- top_n (default: 10): Number of top resource consuming pods to return
- duration (default: "1h"): Analysis time window (e.g., "5m", "1h", "1d")
- start_time (optional): Historical analysis start time in ISO format
- end_time (optional): Historical analysis end time in ISO format

Returns Multus-specific metrics including:
- Multus daemon CPU and memory usage per node
- Network attachment definition processing performance
- Secondary interface provisioning latency and success rates
- Multi-network pod resource consumption
- CNI plugin invocation metrics and error rates
- Network attachment controller performance

Use this tool to monitor multi-network performance, troubleshoot secondary network interface issues, analyze Multus resource consumption, or validate multi-network configuration efficiency."""
)
async def query_multus_metrics(request: PODsMultusRequest) -> Dict[str, Any]:
    """
    Query Multus CNI metrics including network attachment processing times,
    resource usage, and network interface management performance.
    
    Essential for monitoring secondary network interface provisioning and management.
    """
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
                
        collector = PodsUsageCollector(prometheus_client,auth_manager)
        
        # Add timeout to prevent hanging
        result = await asyncio.wait_for(
            collector.collect_duration_usage(
                request.duration, 
                request.pod_pattern, 
                request.container_pattern, 
                request.namespace_pattern
            ),
            timeout=45.0
        )
        return result

    except asyncio.TimeoutError:
        return {"error": "Timeout collecting Multus metrics", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"Error querying Multus metrics: {e}")
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="get_ovnk_latency_metrics",
    description="""Collect comprehensive OVN-Kubernetes latency metrics including controller sync duration, node ready duration, CNI request latency, pod creation latency, and service configuration latency. This tool provides detailed timing analysis of OVN-Kubernetes components to identify performance bottlenecks and latency issues in network operations.

LATENCY METRICS COLLECTED:

CONTROLLER LATENCY METRICS (when include_controller_metrics=True):
- Controller Ready Duration: Time taken for OVN controller pods to become ready and operational
- Controller Sync Duration: Time taken for controller to sync with OVN database and apply network configurations (includes top 20 resource watchers with highest sync times)

NODE LATENCY METRICS (when include_node_metrics=True):
- Node Ready Duration: Time taken for OVN node pods to become ready on worker nodes

EXTENDED LATENCY METRICS (when include_extended_metrics=True):
- CNI Request Latency: 99th percentile latency for CNI ADD/DEL operations during pod creation/deletion
- Pod Creation Latency: End-to-end pod networking setup timing including LSP creation, port binding, and network programming
- Pod Annotation Latency: Time from pod creation to network annotation completion (99th percentile)
- Service Latency: Service synchronization and load balancer configuration timing (average and 99th percentile)
- Network Configuration Application: Time to apply network policies and routing rules for pods and services (99th percentile)

METRICS ANALYSIS FEATURES:
- Statistical Analysis: Maximum, average, and percentile values for each metric type
- Pod-to-Node Mapping: Associates latency measurements with specific nodes for infrastructure correlation
- Resource Context: Links sync duration metrics to specific Kubernetes resource types (pods, services, endpoints, etc.)
- Top Performance Analysis: Identifies highest latency pods, nodes, and operations for targeted optimization
- Time-based Analysis: Supports both instant queries and duration-based trend analysis

QUERY MODES:
- Instant Query (duration=None): Current latency values and immediate performance snapshot
- Duration Query (duration specified): Historical latency trends over time periods (5m to 24h recommended)
- Range Analysis: Custom time window analysis with start/end time specification

PERFORMANCE THRESHOLDS & ALERTS:
- Controller sync duration >5s indicates database performance issues
- CNI request latency >10s suggests node resource constraints
- Pod creation latency >30s indicates networking bottlenecks
- Service sync latency >2s suggests load balancer configuration issues

USE CASES:
- Network Performance Troubleshooting: Identify slow network operations and bottlenecks
- Capacity Planning: Understand latency trends under different load conditions
- SLA Monitoring: Track network operation timing against service level agreements
- Infrastructure Optimization: Correlate latency with node performance and resource allocation
- Incident Investigation: Analyze timing during network-related outages or performance degradation
- Baseline Establishment: Create performance baselines for ongoing monitoring

The tool returns structured latency data organized by component type with statistical summaries, top performers/laggards, and actionable insights for network performance optimization."""
)
async def get_ovnk_latency_metrics(request: OVNKLatencyMetricsRequest) -> Dict[str, Any]:
    """
    Collect comprehensive OVN-Kubernetes latency metrics including controller sync,
    node ready, CNI request, pod creation, and service latency analysis.
    
    Provides detailed timing analysis to identify network performance bottlenecks
    and latency issues in OVN-Kubernetes operations.
    """
    try:
        # Ensure components are initialized
        if not auth_manager or not prometheus_client:
            try:
                await initialize_components()
            except Exception as init_error:
                return {
                    "error": f"Component initialization failed: {init_error}",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "tool_name": "get_ovnk_latency_metrics"
                }

        logger.info(f"Starting OVN-K latency metrics collection - Duration: {request.duration}, "
                   f"Controller: {request.include_controller_metrics}, "
                   f"Node: {request.include_node_metrics}, "
                   f"Extended: {request.include_extended_metrics}")

        # Initialize latency collector
        latency_collector = OVNLatencyCollector(prometheus_client)
        
        # Set timeout based on query type and scope
        timeout_seconds = 300.0  # 5 minutes for instant queries
        if request.duration:
            # Longer timeout for duration queries
            timeout_seconds = 600.0  # 10 minutes
        
        # Collect latency metrics with timeout
        latency_data = await asyncio.wait_for(
            latency_collector.collect_comprehensive_latency_metrics(
                time=None,
                duration=request.duration,
                end_time=request.end_time,
                include_controller_metrics=request.include_controller_metrics,
                include_node_metrics=request.include_node_metrics,
                include_extended_metrics=request.include_extended_metrics
            ),
            timeout=timeout_seconds
        )
        
        # Add collection metadata
        latency_data['collection_metadata'] = {
            'tool_name': 'get_ovnk_latency_metrics',
            'parameters_applied': {
                'duration': request.duration,
                'end_time': request.end_time,
                'include_controller_metrics': request.include_controller_metrics,
                'include_node_metrics': request.include_node_metrics,
                'include_extended_metrics': request.include_extended_metrics
            },
            'timeout_seconds': timeout_seconds,
            'query_mode': 'duration_analysis' if request.duration else 'instant_snapshot'
        }
        
        # Log collection summary
        summary = latency_data.get('summary', {})
        total_metrics = summary.get('total_metrics', 0)
        successful_metrics = summary.get('successful_metrics', 0)
        failed_metrics = summary.get('failed_metrics', 0)
        
        logger.info(f"OVN-K latency collection completed - "
                   f"Total: {total_metrics}, Successful: {successful_metrics}, Failed: {failed_metrics}")
        
        # Add performance insights if we have data
        if summary.get('top_latencies'):
            top_latency = summary['top_latencies'][0]
            logger.info(f"Highest latency detected: {top_latency['metric_name']} = "
                       f"{top_latency['readable_max']['value']}{top_latency['readable_max']['unit']}")
        
        return _sanitize_json_compat(latency_data)
        
    except asyncio.TimeoutError:
        return {
            "error": f"Timeout collecting OVN-K latency metrics after {timeout_seconds}s - cluster may be experiencing high load",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": timeout_seconds,
            "suggestion": "Try reducing scope by disabling extended metrics or using shorter duration",
            "tool_name": "get_ovnk_latency_metrics"
        }
    except Exception as e:
        logger.error(f"Error collecting OVN-K latency metrics: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "get_ovnk_latency_metrics"
        }

@app.tool(
    name="query_kube_api_metrics",
    description="""Query Kubernetes API server performance metrics including request rates, response times, error rates, and resource consumption. This tool is essential for monitoring cluster control plane health and performance, providing insights into API server latency and throughput.

Parameters:  
- duration (default: "5m"): Query duration using Prometheus time format (e.g., "5m", "1h", "1d")
- start_time (optional): Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ) for historical analysis
- end_time (optional): End time in ISO format (YYYY-MM-DDTHH:MM:SSZ) for historical analysis

Returns comprehensive API server metrics including:
- Read-only API call latency (LIST/GET operations) with p99 percentiles
- Mutating API call latency (POST/PUT/DELETE/PATCH operations) with p99 percentiles  
- Request rates per verb and resource type
- Error rates by HTTP status code and operation type
- Current inflight requests and queue depths
- etcd request duration metrics for backend storage performance
- Overall health scoring and performance alerts

Use this tool to diagnose API server performance issues, identify slow operations, monitor control plane health, or troubleshoot cluster responsiveness problems."""
)
async def query_kube_api_metrics(request: MetricsRequest) -> Dict[str, Any]:
    """
    Query Kubernetes API server performance metrics including request rates, 
    response times, error rates, and resource consumption.
    
    Essential for monitoring cluster control plane health and performance.
    """
    global prometheus_client
    try:
        if not prometheus_client:
            await initialize_components()
        
        kube_api_metrics = kubeAPICollector(prometheus_client)
        
        # Add timeout to prevent hanging
        result = await asyncio.wait_for(
            kube_api_metrics.get_metrics(request.duration, request.start_time, request.end_time),
            timeout=30.0
        )
        return result
    except asyncio.TimeoutError:
        return {"error": "Timeout querying Kube API metrics", "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"Error querying Kube API metrics: {e}")
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

@app.tool(
    name="perform_general_cluster_status_check",
    description="""Perform comprehensive general cluster status check and health assessment including cluster operator status, node health, Machine Config Pool (MCP) status, networking components health, and overall cluster operational readiness. This tool provides essential cluster health monitoring and operational status verification distinct from performance analysis tools.

CORE HEALTH ASSESSMENTS:

CLUSTER OPERATOR STATUS:
- Complete inventory of all cluster operators with availability and health status
- Identification of unavailable, degraded, or progressing operators requiring attention
- Operator dependency analysis and impact assessment for failed or degraded operators
- Version consistency checks and upgrade status monitoring across all operators
- Critical operator failure detection with immediate remediation recommendations

NODE HEALTH EVALUATION:
- Node readiness status (Ready/NotReady/SchedulingDisabled) across all cluster nodes
- Node role distribution analysis (master/worker/infra) with capacity assessment
- Node resource capacity and allocatable resources for capacity planning
- Hardware health indicators including CPU, memory, and disk capacity status
- Node condition analysis (DiskPressure, MemoryPressure, PIDPressure, NetworkUnavailable)
- Node age analysis and lifecycle management recommendations

MACHINE CONFIG POOL (MCP) STATUS:
- MCP health status (Updated/Updating/Degraded) for all node pools
- Configuration synchronization progress and update completion status
- Failed update identification with rollback recommendations when applicable
- Node configuration consistency verification across pools
- Update queue analysis and maintenance window optimization recommendations

NETWORKING COMPONENT HEALTH:
- OVN-Kubernetes component operational status and readiness verification
- Network policy enforcement health and configuration validation
- DNS resolution functionality and service discovery health checks
- Ingress controller status and traffic routing operational verification
- CNI plugin health and network interface provisioning capability assessment

STORAGE SYSTEM HEALTH:
- Persistent volume provisioning capability and storage class availability
- Storage operator health and CSI driver operational status verification
- Volume attachment health and mounting success rate analysis
- Storage capacity monitoring and expansion capability assessment

OVERALL CLUSTER READINESS:
- Cluster API responsiveness and control plane health verification
- etcd cluster health and data consistency validation
- Authentication and authorization system operational status
- Resource quota utilization and namespace-level health assessment
- Certificate validity and expiration monitoring for security components

AUTOMATED HEALTH SCORING:
- Component-level health scores (0-100) with weighted importance for overall impact
- Overall cluster health score combining all component assessments
- Risk categorization (Critical/High/Medium/Low) with severity impact analysis
- Health trend analysis when baseline data is available for comparison
- Predictive health alerts for components showing degradation patterns

ACTIONABLE RECOMMENDATIONS:
- Immediate action items for critical health issues requiring urgent attention
- Preventive maintenance recommendations for warning-level health issues
- Capacity planning guidance based on current utilization and growth trends
- Configuration optimization suggestions for improved stability and performance
- Maintenance window planning with priority-based remediation schedules

EXECUTIVE REPORTING:
- Executive summary suitable for management stakeholders and operational reporting
- Key performance indicators (KPIs) for cluster operational health and readiness
- Risk assessment summary with business impact analysis and mitigation strategies
- Compliance status reporting for operational SLAs and availability requirements
- Historical health trend summary when baseline comparison data is available

OPERATIONAL METADATA:
- Health check execution timestamp and data freshness verification
- Assessment scope and coverage details for audit and compliance purposes
- Component response times and data collection success rates
- Integration compatibility for monitoring dashboards and alerting systems
- Structured JSON output format for automation and programmatic consumption

Parameters:
- include_detailed_analysis (default: true): Include comprehensive component-level health analysis with detailed metrics, scoring, and specific recommendations for each cluster component
- generate_summary_report (default: true): Generate executive summary report with key findings, prioritized action items, and business impact assessment for stakeholder communication
- health_check_scope (optional): Limit assessment to specific areas (['operators', 'nodes', 'networking', 'storage', 'mcps']) for focused health checks or faster execution
- performance_baseline_comparison (default: false): Include historical baseline comparison when available to identify health trends, degradation patterns, or improvement verification

DISTINCTION FROM PERFORMANCE TOOLS:
- Focus on operational health and readiness rather than detailed performance metrics
- Emphasizes cluster component availability and functional status over resource utilization
- Provides immediate operational actionability rather than deep performance analysis
- Suitable for daily health monitoring and operational readiness verification
- Complements performance analysis tools with foundational health assessment

Use this tool for:
- Daily operational health monitoring and cluster readiness verification
- Pre-maintenance health assessment and go/no-go decision support
- Post-deployment health validation and operational readiness confirmation
- Incident response initial assessment and scope determination for cluster issues
- Compliance reporting and operational SLA monitoring for availability requirements
- Change management health verification before and after configuration changes
- Executive reporting on infrastructure health and operational status
- Automated health monitoring integration with alerting and dashboard systems

This tool provides essential cluster health visibility focusing on operational readiness and component availability, making it ideal for operations teams and daily health monitoring workflows."""
)
async def perform_general_cluster_status_check(request: GeneralClusterStatusRequest) -> Dict[str, Any]:
    """
    Perform comprehensive general cluster status check and health assessment including
    cluster operator status, node health, MCP status, networking components health,
    and overall cluster operational readiness.
    
    This tool provides essential cluster health monitoring and operational status verification
    distinct from performance analysis tools.
    """
    global auth_manager
    try:
        if not auth_manager:
            await initialize_components()
        
        logger.info("Starting general cluster status check and health assessment...")
        
        # Collect cluster information
        cluster_data = await collect_cluster_information()
        
        # Note: ClusterStatAnalyzer class doesn't exist in current codebase
        # analyzer = ClusterStatAnalyzer()
        # analysis_result = analyzer.analyze_metrics_data(cluster_data)
        # report = analyzer.generate_report(analysis_result)
        # return _sanitize_json_compat(analysis_result)
        
        # Return cluster data directly since analyzer is not available
        return _sanitize_json_compat({
            'cluster_info': cluster_data,
            'note': 'ClusterStatAnalyzer not available - returning raw cluster data',
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
    except asyncio.TimeoutError:
        return {
            'error': 'Timeout during cluster status check - cluster may be experiencing significant issues',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'timeout_seconds': 90,
            'suggestion': 'Check cluster API responsiveness and component availability',
            'tool_name': 'perform_general_cluster_status_check'
        }
    except Exception as e:
        logger.error(f"Error in general cluster status check: {e}")
        return {
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'tool_name': 'perform_general_cluster_status_check'
        }

@app.tool(
    name="query_cluster_network_io",
    description="""Get comprehensive cluster network I/O metrics including bandwidth utilization, packet rates, drops, errors, and connection tracking across all nodes grouped by role. This tool provides detailed network performance analysis essential for identifying network bottlenecks, saturation issues, and connectivity problems.

NETWORK METRICS COLLECTED:
- RX/TX bandwidth utilization in bits per second for throughput analysis
- RX/TX packet rates per second for traffic pattern analysis
- RX/TX packet drop rates indicating buffer overflows or network congestion
- RX/TX error rates for detecting network hardware or driver issues
- Network interface saturation metrics (percentage of capacity used)
- Interface operational status (up/down) and carrier detection
- Network speed configuration in bits per second
- GRPC active watch streams count for API server network load
- NF conntrack entries and limits for connection tracking analysis
- ARP table entry counts for network discovery monitoring
- FIFO queue depths for receive and transmit buffer analysis

NODE ORGANIZATION:
- Nodes automatically grouped by role: controlplane, worker, infra, and workload
- Worker nodes show top 3 by maximum metric value plus complete list
- Per-node average and maximum values over the query duration
- Role-based network performance comparison and analysis

NETWORK HEALTH INDICATORS:
- High drop rates indicating network congestion or buffer exhaustion
- Interface saturation approaching 100% suggesting bandwidth constraints
- Error rates indicating hardware issues or misconfiguration
- Conntrack entries approaching limit warning of connection exhaustion
- Interface status changes detecting connectivity problems

STATISTICAL ANALYSIS:
- Average and maximum values for each metric per node over time period
- Top resource consumers identification for targeted optimization
- Trend analysis for capacity planning and performance monitoring
- Per-interface and per-node granularity for precise troubleshooting

Parameters:
- duration (default: "5m"): Query duration using Prometheus format (e.g., "5m", "15m", "1h", "6h", "1d")
- include_metrics (optional): Filter specific metric categories to reduce response size
- node_groups (optional): Filter by specific node groups (controlplane/worker/infra/workload)

Use this tool for:
- Identifying network bottlenecks and saturation issues before they impact applications
- Detecting packet drops and errors indicating network problems
- Monitoring connection tracking table utilization and potential exhaustion
- Analyzing network traffic patterns and bandwidth consumption
- Capacity planning for network infrastructure upgrades
- Troubleshooting connectivity and performance issues
- Validating network configuration and interface status
- Monitoring GRPC API server network load and watch streams
- Pre-maintenance network health assessment
- Performance baseline establishment and trend analysis

The tool provides comprehensive network-level visibility essential for maintaining optimal OpenShift cluster network performance and reliability."""
)
async def query_cluster_network_io(request: NetworkIORequest) -> Dict[str, Any]:
    """
    Get comprehensive cluster network I/O metrics including bandwidth utilization,
    packet rates, drops, errors, and connection tracking across all nodes grouped by role.
    
    Provides detailed network performance analysis for identifying bottlenecks and issues.
    """
    global prometheus_client, auth_manager, config
    try:
        if not prometheus_client or not auth_manager or not config:
            await initialize_components()
        
        logger.info(f"Collecting network I/O metrics for duration: {request.duration}")
        
        # Initialize network I/O collector
        collector = NetworkIOCollector(
            prometheus_url=auth_manager.prometheus_url,
            token=auth_manager.prometheus_token,
            config=config
        )
        
        try:
            await collector.initialize()
            
            # Collect all network I/O metrics with timeout
            network_data = await asyncio.wait_for(
                collector.collect_all_metrics(duration=request.duration),
                timeout=60.0
            )
            
            # Apply metric filtering if requested
            if request.include_metrics:
                filtered_metrics = {}
                metric_map = {
                    'rx_utilization': 'network_io_node_network_rx_utilization',
                    'tx_utilization': 'network_io_node_network_tx_utilization',
                    'rx_packets': 'network_io_node_network_rx_package',
                    'tx_packets': 'network_io_node_network_tx_package',
                    'rx_drops': 'network_io_node_network_rx_drop',
                    'tx_drops': 'network_io_node_network_tx_drop',
                    'grpc_streams': 'network_io_grpc_active_watch_streams',
                    'conntrack': ['network_io_node_nf_conntrack_entries', 'network_io_node_nf_conntrack_entries_limit'],
                    'saturation': ['network_io_node_saturation_rx', 'network_io_node_saturation_tx'],
                    'errors': ['network_io_node_error_rx', 'network_io_node_error_tx'],
                    'interface_status': ['network_io_node_network_up', 'network_io_node_traffic_carrier'],
                    'speed': 'network_io_node_speed_bytes',
                    'arp': 'network_io_node_arp_entries',
                    'fifo': ['network_io_nodec_receive_fifo_total', 'network_io_node_transit_fifo_total']
                }
                
                for filter_key in request.include_metrics:
                    metric_keys = metric_map.get(filter_key, [])
                    if isinstance(metric_keys, str):
                        metric_keys = [metric_keys]
                    for key in metric_keys:
                        if key in network_data.get('metrics', {}):
                            filtered_metrics[key] = network_data['metrics'][key]
                
                network_data['metrics'] = filtered_metrics
                logger.info(f"Applied metric filtering - {len(filtered_metrics)} metrics included")
            
            # Apply node group filtering if requested
            if request.node_groups:
                for metric_name, metric_data in network_data.get('metrics', {}).items():
                    if isinstance(metric_data, dict):
                        for group in ['controlplane', 'worker', 'infra', 'workload']:
                            if group not in request.node_groups:
                                metric_data.pop(group, None)
                logger.info(f"Applied node group filtering - groups: {request.node_groups}")
            
            # Add collection metadata
            network_data['collection_metadata'] = {
                'tool_name': 'query_cluster_network_io',
                'parameters_applied': {
                    'duration': request.duration,
                    'include_metrics': request.include_metrics,
                    'node_groups': request.node_groups
                },
                'total_metrics_collected': len(network_data.get('metrics', {}))
            }
            
            # Log collection summary
            metrics_with_data = sum(1 for m in network_data.get('metrics', {}).values() 
                                  if isinstance(m, dict) and not m.get('error'))
            logger.info(f"Network I/O collection completed - {metrics_with_data} metrics with data")
            
            return network_data
            
        finally:
            await collector.close()
        
    except asyncio.TimeoutError:
        return {
            "error": "Timeout collecting network I/O metrics - cluster may be experiencing issues",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 60,
            "suggestion": "Try reducing the duration parameter or check cluster network responsiveness"
        }
    except Exception as e:
        logger.error(f"Error collecting network I/O metrics: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "query_cluster_network_io"
        }


async def shutdown_handler():
    """Handle graceful shutdown"""
    logger.info("Shutdown handler called")
    await cleanup_resources()
    logger.info("Shutdown complete")

# Add these helper functions at the end of the file, before main()

def _parse_duration_to_seconds(duration: str) -> int:
    """Parse Prometheus duration string to seconds"""
    import re
    
    # Handle common duration formats
    match = re.match(r'^(\d+)([smhd])$', duration.lower())
    if not match:
        return 300  # Default 5 minutes
    
    value = int(match.group(1))
    unit = match.group(2)
    
    multipliers = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    return value * multipliers.get(unit, 60)


def _apply_top_n_filtering(metrics_data: Dict[str, Any], top_n: int) -> None:
    """Apply top N filtering to statistics sections in metrics data"""
    for section_key, section_data in metrics_data.items():
        if isinstance(section_data, dict) and section_key.endswith('_metrics'):
            for metric_key, metric_data in section_data.items():
                if isinstance(metric_data, dict) and 'statistics' in metric_data:
                    stats = metric_data['statistics']
                    if 'top_6' in stats:
                        # Adjust the top results count
                        current_results = stats.get('top_6', [])
                        stats[f'top_{min(top_n, len(current_results))}'] = current_results[:top_n]
                        # Keep original key for compatibility but limit results
                        stats['top_6'] = current_results[:min(6, top_n)]


def _remove_statistics_from_results(metrics_data: Dict[str, Any]) -> None:
    """Remove statistical analysis from metrics data"""
    for section_key, section_data in metrics_data.items():
        if isinstance(section_data, dict) and section_key.endswith('_metrics'):
            for metric_key, metric_data in section_data.items():
                if isinstance(metric_data, dict) and 'statistics' in metric_data:
                    # Keep only basic info, remove detailed statistics
                    basic_stats = {
                        'count': metric_data['statistics'].get('count', 0)
                    }
                    metric_data['statistics'] = basic_stats


async def main():
    """Main entry point with improved error handling and graceful shutdown"""
    try:
        # Initialize components
        await initialize_components()
        logger.info("MCP server starting...")
        
        # Create tasks for server and shutdown handler
        server_task = asyncio.create_task(
            app.run_async(
                transport="streamable-http",
                host="0.0.0.0",
                port=8000,
                # Avoid deprecated websockets.legacy imports from uvicorn by disabling WS
                uvicorn_config={
                    "ws": "none"
                }
            )
        )
        
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        
        # Wait for either server completion or shutdown signal
        done, pending = await asyncio.wait(
            [server_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Cancel pending tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # If shutdown was triggered, clean up
        if shutdown_task in done:
            logger.info("Shutdown signal received, cleaning up...")
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass
            await shutdown_handler()
        
        # If server task completed (likely due to error), check for exceptions
        if server_task in done:
            try:
                await server_task
            except Exception as e:
                logger.error(f"Server task failed: {e}")
                raise
    
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down...")
        await shutdown_handler()
    except Exception as e:
        logger.error(f"Error starting server: {e}")
        await shutdown_handler()
        raise
    finally:
        logger.info("Main function exiting")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGracefully shutting down...")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)