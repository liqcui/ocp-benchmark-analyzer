#!/usr/bin/env python3
"""
OpenShift OVN-Kubernetes Network Analyzer MCP Server
Main server implementation using FastMCP with streamable-http transport
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

# Ensure project root is on sys.path
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from tools.ocp.cluster_info import ClusterInfoCollector
from tools.utils.promql_basequery import PrometheusBaseQuery, PrometheusQueryError
from tools.utils.promql_utility import mcpToolsUtility
from tools.net.network_io import NetworkIOCollector
from tools.net.network_l1 import NetworkL1Collector
from tools.net.network_socket4tcp import socketStatTCPCollector
from tools.net.network_socket4udp import SocketStatUDPCollector
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
warnings.filterwarnings("ignore", category=DeprecationWarning, message=r"HTTPResponse\.getheaders\(\) is deprecated")
warnings.filterwarnings("ignore", category=UserWarning, module="anyio.streams.memory")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

_server_log_level = os.environ.get("OVNK_LOG_LEVEL", "INFO").upper()
try:
    root_level = getattr(logging, _server_log_level, logging.INFO)
except Exception:
    root_level = logging.INFO
logging.getLogger().setLevel(root_level)
logger.setLevel(root_level)

# Reduce noise from libs
logging.getLogger("mcp.server.streamable_http").setLevel(logging.WARNING)
logging.getLogger("anyio").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

os.environ['TZ'] = 'UTC'
shutdown_event = asyncio.Event()

# ==================== Pydantic Models ====================

class MCPBaseModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class OCPClusterInfoResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    
class NetworkL1Request(MCPBaseModel):
    duration: str = Field(default="5m", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    metric_name: Optional[str] = Field(default=None, description="Specific metric name (optional)")
    timeout_seconds: Optional[int] = Field(default=120, ge=10, le=600, description="Server-side timeout in seconds")

class NetworkIORequest(MCPBaseModel):
    duration: str = Field(default="5m", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    include_metrics: Optional[List[str]] = Field(default=None, description="Filter specific metrics")
    node_groups: Optional[List[str]] = Field(default=None, description="Filter by node groups")

class NetworkSocketTCPRequest(MCPBaseModel):
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    include_all_workers: bool = Field(default=False, description="Include all workers (default: top 3)")
    metric_filter: Optional[List[str]] = Field(default=None, description="Filter specific metrics")

class NetworkSocketUDPRequest(MCPBaseModel):
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    include_all_workers: bool = Field(default=False, description="Include all workers (default: top 3)")

class NetworkSocketMemRequest(MCPBaseModel):
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    include_statistics: bool = Field(default=True, description="Include avg/max statistics")
    worker_top_n: int = Field(default=3, ge=1, le=10, description="Top N workers to return")
    include_all_roles: bool = Field(default=True, description="Include all node roles")
    metric_filter: Optional[List[str]] = Field(default=None, description="Filter specific metrics")

class NetworkSocketSoftnetRequest(MCPBaseModel):
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    step: str = Field(default="15s", description="Query step interval")
    include_summary: bool = Field(default=True, description="Include summary statistics")

class NetworkSocketIPRequest(MCPBaseModel):
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    step: str = Field(default="15s", description="Query step interval")

class NetStatTCPRequest(MCPBaseModel):
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    step: str = Field(default="15s", description="Query step interval")

class NetStatUDPRequest(MCPBaseModel):
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")

class HealthCheckRequest(MCPBaseModel):
    pass

class OCPClusterInfoRequest(MCPBaseModel):
    pass

# ==================== Response Models ====================

class NetworkMetricsResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: Optional[str] = None
    duration: Optional[str] = None

class ClusterInfoResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str

class HealthCheckResponse(MCPBaseModel):
    status: str
    timestamp: str
    mcp_server: Dict[str, Any]
    prometheus: Dict[str, Any]
    kubeapi: Dict[str, Any]

# ==================== Initialize FastMCP ====================

app = FastMCP("ovnk-network-analyzer")
# Alias for decorator usage consistency with other servers
mcp = app

# Global components
auth_manager: Optional[OpenShiftAuth] = None
config: Optional[Config] = None
prometheus_client: Optional[PrometheusBaseQuery] = None
cluster_info_collector: Optional[ClusterInfoCollector] = None

# ==================== Helper Functions ====================

def _sanitize_json_compat(value):
    """Recursively replace NaN/Inf floats with None for JSON compatibility"""
    try:
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        if isinstance(value, dict):
            return {k: _sanitize_json_compat(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_sanitize_json_compat(v) for v in value]
        if isinstance(value, tuple):
            return [_sanitize_json_compat(v) for v in value]
        return value
    except Exception:
        return None

async def cleanup_resources():
    """Clean up global resources on shutdown"""
    global auth_manager
    
    logger.info("Cleaning up resources...")
    
    try:
        if auth_manager:
            await auth_manager.cleanup()
    except Exception as e:
        logger.error(f"Error cleaning up auth manager: {e}")
    
    logger.info("Resource cleanup completed")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ==================== MCP Tools ====================

async def initialize_components():
    """Initialize global components with proper error handling"""
    global auth_manager, config, prometheus_client
    
    try:
        logger.info("="*70)
        logger.info("Initializing OVN-Kubernetes Network Analyzer components...")
        logger.info("="*70)
        
        # Initialize config
        config = Config()
        loaded_before = config.get_metrics_count()
        if loaded_before == 0:
            metrics_file_path = os.path.join(PROJECT_ROOT, 'config', 'metrics-net.yml')
            load_result = config.load_metrics_file(metrics_file_path)
            total_after = config.get_metrics_count()
            if total_after > 0:
                file_summary = config.get_file_summary()
                files_descr = ", ".join(
                    f"{v['file_name']}={v['metrics_count']}" for v in file_summary.values()
                ) or os.path.basename(metrics_file_path)
                logger.info(f"Metrics loaded: total={total_after}, files=[{files_descr}]")
            else:
                logger.warning(f"No metrics loaded: {load_result.get('error', 'no metrics found')}")
        else:
            file_summary = config.get_file_summary()
            total = config.get_metrics_count()
            files_descr = ", ".join(f"{v['file_name']}={v['metrics_count']}" for v in file_summary.values())
            logger.info(f"Metrics preloaded: total={total}, files=[{files_descr}]")
        
        # Initialize OpenShift authentication
        logger.info("🔗 Initializing OpenShift authentication...")
        auth_manager = OpenShiftAuth(config.kubeconfig_path)
        await auth_manager.initialize()
        logger.info("✅ OpenShift authentication initialized")
        
        # Initialize Prometheus client (prefer route URL over service DNS)
        prom_cfg = auth_manager.get_prometheus_config() or {}
        base_url = prom_cfg.get('url') or auth_manager.prometheus_url
        token = prom_cfg.get('token') or auth_manager.prometheus_token
        # If base_url points to cluster-internal service DNS, try to prefer a route-like URL from fallbacks
        fallbacks = prom_cfg.get('fallback_urls') or []
        def _is_route(u: str) -> bool:
            # Heuristic: OpenShift routes typically contain '.apps.' and are not svc.cluster.local
            return ('.apps.' in u) and ('svc.cluster.local' not in u)
        preferred_url = base_url
        # Prefer first route-like URL in fallbacks
        for u in fallbacks:
            if _is_route(u):
                preferred_url = u
                break
        prometheus_client = PrometheusBaseQuery(preferred_url, token)
        
        logger.info("="*70)
        logger.info("✅ All components initialized successfully!")
        logger.info("="*70)
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize components: {e}")
        raise

@mcp.tool()
async def get_mcp_health_status(request: HealthCheckRequest) -> HealthCheckResponse:
    """
    Get MCP server health status and connectivity checks.
    
    Verifies server status and connectivity to:
    - MCP server operational status
    - Prometheus connectivity and metrics availability
    - Kubernetes API server connectivity
    
    Returns:
        HealthCheckResponse: Server health status with component connectivity details
    """
    global auth_manager, prometheus_client
    
    try:
        if not auth_manager or not prometheus_client:
            try:
                await initialize_components()
            except Exception as init_error:
                return HealthCheckResponse(
                    status="error",
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    mcp_server={"running": False, "error": str(init_error)},
                    prometheus={"connected": False},
                    kubeapi={"connected": False}
                )

        # Test Prometheus
        prometheus_ok = False
        prometheus_error: Optional[str] = None
        try:
            prometheus_ok = await asyncio.wait_for(
                prometheus_client.test_connection(), timeout=10.0
            )
        except asyncio.TimeoutError:
            prometheus_error = "Connection timeout"
        except Exception as e:
            prometheus_error = str(e)

        # Test Kube API
        kubeapi_ok = False
        kubeapi_error: Optional[str] = None
        try:
            if auth_manager:
                kubeapi_ok = await asyncio.wait_for(
                    auth_manager.test_kubeapi_connection(), timeout=10.0
                )
        except asyncio.TimeoutError:
            kubeapi_error = "Connection timeout"
        except Exception as e:
            kubeapi_error = str(e)

        status = "healthy" if prometheus_ok and kubeapi_ok else ("degraded" if prometheus_ok or kubeapi_ok else "unhealthy")

        return HealthCheckResponse(
            status=status,
            timestamp=datetime.now(timezone.utc).isoformat(),
            mcp_server={"running": True, "transport": "streamable-http"},
            prometheus={
                "connected": prometheus_ok,
                "url": getattr(auth_manager, "prometheus_url", None) if auth_manager else None,
                "error": prometheus_error,
            },
            kubeapi={
                "connected": kubeapi_ok,
                "node_count": (auth_manager.cluster_info.get("node_count") if (auth_manager and auth_manager.cluster_info) else None),
                "error": kubeapi_error,
            },
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthCheckResponse(
            status="error",
            timestamp=datetime.now(timezone.utc).isoformat(),
            mcp_server={"running": False, "error": str(e)},
            prometheus={"connected": False},
            kubeapi={"connected": False}
        )

@mcp.tool()
async def get_ocp_cluster_info(request: OCPClusterInfoRequest) -> OCPClusterInfoResponse:
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
                    timestamp=datetime.now(timezone.utc).isoformat()
                )

        info = await cluster_info_collector.collect_cluster_info()
        return OCPClusterInfoResponse(
            status="success",
            data=cluster_info_collector.to_dict(info),
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    except Exception as e:
        logger.error(f"Error collecting OCP cluster info: {e}")
        return OCPClusterInfoResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(timezone.utc).isoformat()
        )

@mcp.tool()
async def get_network_io_node(request: NetworkIORequest) -> NetworkMetricsResponse:
    """
    Get comprehensive cluster node level network I/O metrics and performance statistics.
    
    Monitors network performance metrics including:
    - RX/TX bandwidth utilization on ocp node level(bits per second)
    - RX/TX packet rates  on ocp node level(packets per second)
    - Packet drops and errors on ocp node level(indicating congestion or issues)
    - Network saturation metrics  on ocp node level(percentage of capacity)
    - Interface status on ocp node level(up/down, carrier detection)
    - Network speed configuration on ocp node level
    - Connection tracking (conntrack entries and limits) on ocp node level
    - ARP table statistics on ocp node level
    - FIFO queue depths on ocp node level
    
    Metrics organized by node role with avg/max statistics.
    Worker nodes show top 3 performers.
    
    USE CASES:
    - Network bottleneck identification and saturation analysis
    - Packet drop troubleshooting and congestion detection
    - Connection tracking table utilization monitoring
    - Network capacity planning for infrastructure upgrades
    - API server network load analysis via gRPC streams
    - Interface health and carrier status monitoring
    - Network performance baseline establishment
    
    Args:
        duration: Time range for metrics (e.g., '5m', '15m', '1h', '6h', '1d'). Default: '5m'
        start_time: Optional start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        end_time: Optional end time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    
    Returns:
        NetworkMetricsResponse: Network I/O performance metrics grouped by node role
    """
    global prometheus_client, auth_manager, config
    duration = request.duration
    try:
        if not prometheus_client or not auth_manager or not config:
            await initialize_components()
        
        collector = NetworkIOCollector(
            prometheus_url=auth_manager.prometheus_url,
            token=auth_manager.prometheus_token,
            config=config
        )
        
        try:
            await collector.initialize()
            network_data = await asyncio.wait_for(
                collector.collect_all_metrics(duration=duration), timeout=60.0
            )
            
            return NetworkMetricsResponse(
                status="success",
                data=network_data,
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_io",
                duration=duration
            )
            
        finally:
            await collector.close()
        
    except asyncio.TimeoutError:
        return NetworkMetricsResponse(
            status="error",
            error="Timeout collecting network I/O metrics",
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_io",
            duration=duration
        )
    except Exception as e:
        logger.error(f"Error collecting network I/O: {e}")
        return NetworkMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_io",
            duration=duration
        )

@mcp.tool()
async def get_network_l1_stats(request: NetworkL1Request) -> NetworkMetricsResponse:
    """
    Get comprehensive Layer 1 network metrics in ocp cluster, it's not node usage.
    
    Monitors physical layer network metrics including:
    - Network interface operational status (up/down)
    - Traffic carrier detection status (link presence)
    - Network interface speeds (bits per second)
    - MTU configuration (Maximum Transmission Unit)
    - ARP table entry counts (average and maximum)
    
    Metrics are organized by node role (controlplane/worker/infra/workload).
    Worker nodes show top 3 by name for manageability.
    
    USE CASES:
    - Physical layer connectivity troubleshooting and link status verification
    - Network capacity planning through speed and MTU analysis
    - ARP table monitoring for network scaling and address exhaustion
    - Pre-maintenance validation of network interface configurations
    - Interface health monitoring and misconfiguration detection
    
    Args:
        duration: Time range for metrics (e.g., '5m', '15m', '1h', '6h', '1d'). Default: '5m'
        start_time: Optional start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        end_time: Optional end time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    
    Returns:
        NetworkMetricsResponse: Layer 1 network metrics with interface-level details per node
    """
    global prometheus_client, auth_manager, config
    duration = request.duration
    timeout_seconds = request.timeout_seconds or 120
    try:
        if not prometheus_client or not auth_manager or not config:
            await initialize_components()
        
        utility = mcpToolsUtility()
        collector = NetworkL1Collector(prometheus_client, config, utility)
        
        try:
            results = await asyncio.wait_for(
                collector.collect_all_metrics(duration), timeout=timeout_seconds
            )
        except Exception as first_err:
            # If unauthorized, refresh token and retry once
            err_text = str(first_err)
            if ('401' in err_text) or ('Unauthorized' in err_text) or isinstance(first_err, PrometheusQueryError):
                try:
                    # Reinitialize auth to refresh token
                    await auth_manager.initialize()
                    # Update client token and reset session
                    if prometheus_client:
                        prometheus_client.token = getattr(auth_manager, 'prometheus_token', None)
                        # Force new session with updated headers
                        if getattr(prometheus_client, 'session', None):
                            try:
                                await prometheus_client.close()
                            except Exception:
                                pass
                        # Next call will rebuild session with new Authorization header
                    # Retry once
                    results = await asyncio.wait_for(
                        collector.collect_all_metrics(duration), timeout=timeout_seconds
                    )
                except Exception as retry_err:
                    raise retry_err
            else:
                raise first_err
        
        return NetworkMetricsResponse(
            status="success" if 'error' not in results else "error",
            data=results,
            error=results.get('error'),
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_l1",
            duration=duration
        )
        
    except asyncio.TimeoutError:
        return NetworkMetricsResponse(
            status="error",
            error=f"Timeout collecting network L1 metrics after {timeout_seconds}s",
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_l1",
            duration=duration
        )
    except Exception as e:
        logger.error(f"Error collecting network L1 metrics: {e}")
        return NetworkMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_l1",
            duration=duration
        )

@mcp.tool()
async def get_network_socket_tcp_stats(request: NetworkSocketTCPRequest) -> NetworkMetricsResponse:
    """
    Get comprehensive TCP socket statistics across cluster nodes.
    
    Monitors TCP socket resource consumption including:
    - TCP sockets allocated (kernel allocations)
    - TCP sockets in-use (active connections)
    - TCP orphan connections (not attached to processes)
    - TCP time-wait connections (awaiting cleanup)
    - Total sockets used (all protocols)
    - Fragment buffers in-use (IP reassembly)
    - Raw sockets in-use (low-level operations)
    
    Statistics include avg/max values per node.
    Organized by node role (controlplane/worker/infra).
    Worker nodes show top 3 by maximum usage.
    
    USE CASES:
    - Socket exhaustion detection and capacity planning
    - Connection leak identification (orphan connections)
    - TIME_WAIT accumulation monitoring for port exhaustion
    - Socket resource consumption analysis per node
    - Network capacity planning for socket buffers
    - Troubleshooting "no buffer space available" errors
    
    Args:
        duration: Time range for metrics (e.g., '15m', '1h', '6h', '1d'). Default: '1h'
        start_time: Optional start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        end_time: Optional end time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    
    Returns:
        NetworkMetricsResponse: TCP socket statistics with per-node resource consumption
    """
    global config, auth_manager
    duration = request.duration
    try:
        if not config or not auth_manager:
            await initialize_components()
        
        async with socketStatTCPCollector(config, auth_manager) as collector:
            socket_data = await asyncio.wait_for(
                collector.collect_all_tcp_metrics(), timeout=30.0
            )
            
            return NetworkMetricsResponse(
                status="success",
                data=socket_data,
                timestamp=socket_data.get('collection_time', datetime.now(timezone.utc).isoformat()),
                category="network_socket_tcp"
            )
        
    except asyncio.TimeoutError:
        return NetworkMetricsResponse(
            status="error",
            error="Timeout collecting TCP socket statistics",
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_socket_tcp"
        )
    except Exception as e:
        logger.error(f"Error collecting TCP socket stats: {e}")
        return NetworkMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_socket_tcp"
        )

@mcp.tool()
async def get_network_socket_udp_stats(request: NetworkSocketUDPRequest) -> NetworkMetricsResponse:
    """
    Get UDP socket statistics across cluster nodes.
    
    Monitors UDP socket resource consumption including:
    - UDP sockets in-use (active datagram sockets)
    - UDP lite sockets in-use (lightweight UDP variant)
    
    Statistics include avg/max values per node.
    Organized by node role (controlplane/worker/infra/workload).
    Worker nodes show top 3 by maximum usage.
    
    USE CASES:
    - UDP socket resource consumption monitoring
    - Datagram-based application capacity planning
    - UDP socket exhaustion detection
    - DNS and service discovery socket usage analysis
    
    Args:
        duration: Time range for metrics (e.g., '15m', '1h', '6h', '1d'). Default: '1h'
        start_time: Optional start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        end_time: Optional end time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    
    Returns:
        NetworkMetricsResponse: UDP socket statistics with per-node consumption
    """
    global prometheus_client, config, auth_manager
    duration = request.duration
    try:
        if not prometheus_client or not config or not auth_manager:
            await initialize_components()
        
        utility = mcpToolsUtility()
        collector = SocketStatUDPCollector(prometheus_client, config, utility)
        
        udp_data = await asyncio.wait_for(
            collector.collect_all_metrics(), timeout=30.0
        )
        
        return NetworkMetricsResponse(
            status="success",
            data=udp_data,
            timestamp=udp_data.get('timestamp', datetime.now(timezone.utc).isoformat()),
            category="network_socket_udp"
        )
        
    except asyncio.TimeoutError:
        return NetworkMetricsResponse(
            status="error",
            error="Timeout collecting UDP socket statistics",
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_socket_udp"
        )
    except Exception as e:
        logger.error(f"Error collecting UDP socket stats: {e}")
        return NetworkMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_socket_udp"
        )

@mcp.tool()
async def get_network_socket_memory(request: NetworkSocketMemRequest) -> NetworkMetricsResponse:
    """
    Get comprehensive socket memory statistics across cluster nodes.
    
    Monitors kernel socket memory consumption including:
    - Fragment reassembly buffer memory (node_sockstat_FRAG_memory)
    - TCP kernel buffer memory in pages and bytes
    - UDP kernel buffer memory in pages and bytes
    - Socket memory allocation patterns
    
    Statistics include avg/max values per node.
    Organized by node role (controlplane/worker/infra/workload).
    Worker nodes show top 3 by maximum memory usage.
    
    USE CASES:
    - Socket memory exhaustion troubleshooting
    - Kernel buffer tuning for high-throughput workloads
    - Fragment buffer monitoring for PMTU issues
    - Socket memory leak detection
    - Network stack memory capacity planning
    - Troubleshooting "no buffer space available" errors
    
    Args:
        duration: Time range for metrics (e.g., '15m', '1h', '6h', '1d'). Default: '1h'
        start_time: Optional start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        end_time: Optional end time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    
    Returns:
        NetworkMetricsResponse: Socket memory statistics with kernel buffer analysis
    """
    global auth_manager, config
    duration = request.duration
    try:
        if not auth_manager or not config:
            await initialize_components()
        
        async with socketStatMemCollector(config=config, auth=auth_manager) as collector:
            socket_mem_data = await asyncio.wait_for(
                collector.collect_all_metrics(), timeout=30.0
            )
            
            return NetworkMetricsResponse(
                status="success",
                data=socket_mem_data,
                timestamp=socket_mem_data.get('timestamp', datetime.now(timezone.utc).isoformat()),
                category="network_socket_mem"
            )
        
    except asyncio.TimeoutError:
        return NetworkMetricsResponse(
            status="error",
            error="Timeout collecting socket memory metrics",
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_socket_mem"
        )
    except Exception as e:
        logger.error(f"Error collecting socket memory: {e}")
        return NetworkMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_socket_mem"
        )

@mcp.tool()
async def get_network_socket_softnet_stats(request: NetworkSocketSoftnetRequest) -> NetworkMetricsResponse:
    """
    Get network socket softnet statistics for packet processing performance.
    
    Monitors kernel softnet layer metrics including:
    - Packets processed (successful processing rate)
    - Packets dropped (backlog/quota exhaustion)
    - Out-of-quota events (processing limit hits)
    - CPU RPS statistics (Receive Packet Steering)
    - Flow limit counts (congestion indicators)
    
    Statistics include avg/max values per node over time period.
    Organized by node role (controlplane/worker/infra/workload).
    Worker nodes show top 3 by maximum packet processing.
    
    USE CASES:
    - Kernel network stack performance bottleneck identification
    - Packet drop analysis at softnet layer
    - CPU packet processing distribution analysis (RPS effectiveness)
    - Network stack tuning validation after sysctl changes
    - High packet rate workload capacity planning
    - Network processing limit detection and optimization
    
    Args:
        duration: Time range for metrics (e.g., '15m', '1h', '6h', '1d'). Default: '1h'
        start_time: Optional start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        end_time: Optional end time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    
    Returns:
        NetworkMetricsResponse: Softnet statistics with packet processing metrics
    """
    global prometheus_client, auth_manager, config
    duration = request.duration
    step = request.step
    try:
        if not prometheus_client or not auth_manager or not config:
            await initialize_components()
        
        # Calculate time range
        end_time = datetime.now(timezone.utc)
        duration_delta = prometheus_client.parse_duration(duration)
        start_time = end_time - duration_delta
        
        start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        async with socketStatSoftNetCollector(auth_manager, config) as collector:
            softnet_data = await asyncio.wait_for(
                collector.collect_all_softnet_metrics(start_str, end_str, step),
                timeout=45.0
            )
        
        return NetworkMetricsResponse(
            status="success",
            data=_sanitize_json_compat(softnet_data),
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_socket_softnet",
            duration=duration
        )
        
    except asyncio.TimeoutError:
        return NetworkMetricsResponse(
            status="error",
            error="Timeout collecting softnet statistics",
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_socket_softnet",
            duration=duration
        )
    except Exception as e:
        logger.error(f"Error collecting softnet stats: {e}")
        return NetworkMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_socket_softnet",
            duration=duration
        )

@mcp.tool()
async def get_network_socket_ip_stats(request: NetworkSocketIPRequest) -> NetworkMetricsResponse:
    """
    Get network socket IP-level statistics and ICMP metrics.
    
    Monitors IP layer metrics including:
    - IP incoming/outgoing octets (traffic bytes per second)
    - ICMP incoming/outgoing messages (connectivity monitoring)
    - ICMP errors (network issues and connectivity problems)
    
    Statistics include avg/max values per node over time period.
    Organized by node role (controlplane/worker/infra/workload).
    Worker nodes show top 3 by maximum traffic.
    
    USE CASES:
    - IP-level traffic pattern analysis
    - ICMP connectivity troubleshooting (ping monitoring)
    - Network error detection via ICMP error rates
    - Bandwidth monitoring at IP protocol layer
    - Network health baseline establishment
    - Cross-node traffic distribution analysis
    
    Args:
        duration: Time range for metrics (e.g., '15m', '1h', '6h', '1d'). Default: '1h'
        start_time: Optional start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        end_time: Optional end time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    
    Returns:
        NetworkMetricsResponse: IP-level statistics with traffic and ICMP metrics
    """
    global prometheus_client, auth_manager, config
    duration = request.duration
    try:
        if not prometheus_client or not auth_manager or not config:
            await initialize_components()
        
        async with socketStatIPCollector(auth_manager, config) as collector:
            # Calculate time range
            end_time = datetime.now(timezone.utc)
            duration_delta = prometheus_client.parse_duration(duration)
            start_time = end_time - duration_delta
            
            start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            
            metrics_data = await asyncio.wait_for(
                collector.collect_all_socket_ip_metrics(start_str, end_str, '15s'),
                timeout=45.0
            )
        
        return NetworkMetricsResponse(
            status="success",
            data=metrics_data,
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_socket_ip",
            duration=duration
        )
        
    except asyncio.TimeoutError:
        return NetworkMetricsResponse(
            status="error",
            error="Timeout collecting socket IP metrics",
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_socket_ip",
            duration=duration
        )
    except Exception as e:
        logger.error(f"Error collecting socket IP metrics: {e}")
        return NetworkMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_socket_ip",
            duration=duration
        )

@mcp.tool()
async def get_network_netstat_tcp_stats(request: NetStatTCPRequest) -> NetworkMetricsResponse:
    """
    Get comprehensive network netstat TCP metrics and connection statistics.
    
    Monitors TCP protocol-level metrics including:
    - TCP segments in/out (traffic volume)
    - TCP error conditions (listen overflow, drops, retransmissions)
    - TCP connection states (established, time-wait)
    - SYN cookie statistics (flood protection)
    - TCP timeout events
    - Receive queue drops and out-of-order packets
    
    Statistics include avg/max values per node over time period.
    Organized by node role (controlplane/worker/infra/workload).
    Worker nodes show top 3 by maximum metric values.
    
    USE CASES:
    - TCP stack performance analysis and optimization
    - Network error troubleshooting (retransmissions, drops)
    - SYN flood attack detection and mitigation
    - Connection capacity planning and limit management
    - TCP retransmission analysis for network quality
    - Buffer and queue management optimization
    - Protocol-level network health monitoring
    
    Args:
        duration: Time range for metrics (e.g., '15m', '1h', '6h', '1d'). Default: '1h'
        start_time: Optional start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        end_time: Optional end time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    
    Returns:
        NetworkMetricsResponse: TCP netstat metrics with connection and error statistics
    """
    global prometheus_client, auth_manager, config
    duration = request.duration
    try:
        if not prometheus_client or not auth_manager or not config:
            await initialize_components()
        
        # Calculate time range
        end_time = datetime.now(timezone.utc)
        duration_delta = prometheus_client.parse_duration(duration)
        start_time = end_time - duration_delta
        
        start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        collector = netStatTCPCollector(config, auth_manager, prometheus_client)
        tcp_metrics = await asyncio.wait_for(
            collector.collect_all_metrics(duration),
            timeout=60.0
        )
        
        return NetworkMetricsResponse(
            status="success",
            data=_sanitize_json_compat(tcp_metrics),
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_netstat_tcp",
            duration=duration
        )
        
    except asyncio.TimeoutError:
        return NetworkMetricsResponse(
            status="error",
            error="Timeout collecting netstat TCP metrics",
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_netstat_tcp",
            duration=duration
        )
    except Exception as e:
        logger.error(f"Error collecting netstat TCP metrics: {e}")
        return NetworkMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_netstat_tcp",
            duration=duration
        )

@mcp.tool()
async def get_network_netstat_udp_stats(request: NetStatUDPRequest) -> NetworkMetricsResponse:
    """
    Get network netstat UDP metrics and error statistics.
    
    Monitors UDP protocol-level metrics including:
    - UDP incoming/outgoing packets (traffic rates)
    - UDP receive errors (packet corruption or issues)
    - UDP no-listen errors (no process listening on port)
    - UDP lite receive errors (UDP lite variant issues)
    - UDP buffer errors (receive/transmit buffer exhaustion)
    
    Statistics include avg/max values per node over time period.
    Organized by node role (controlplane/worker/infra/workload).
    Worker nodes show top 3 by maximum metric values.
    
    USE CASES:
    - UDP stack performance analysis and optimization
    - Datagram error troubleshooting and packet loss detection
    - UDP buffer exhaustion monitoring
    - DNS and service discovery performance analysis
    - UDP-based application health monitoring
    - Protocol-level UDP traffic pattern analysis
    
    Args:
        duration: Time range for metrics (e.g., '15m', '1h', '6h', '1d'). Default: '1h'
        start_time: Optional start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        end_time: Optional end time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    
    Returns:
        NetworkMetricsResponse: UDP netstat metrics with traffic and error statistics
    """
    global prometheus_client, auth_manager, config
    duration = request.duration
    try:
        if not prometheus_client or not auth_manager or not config:
            await initialize_components()
        
        collector = netStatUDPCollector(auth_client=auth_manager, config=config)
        udp_metrics = await asyncio.wait_for(
            collector.collect_all_metrics(),
            timeout=45.0
        )
        
        return NetworkMetricsResponse(
            status="success",
            data=udp_metrics,
            timestamp=udp_metrics.get('timestamp', datetime.now(timezone.utc).isoformat()),
            category="network_netstat_udp",
            duration=duration
        )
        
    except asyncio.TimeoutError:
        return NetworkMetricsResponse(
            status="error",
            error="Timeout collecting netstat UDP metrics",
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_netstat_udp",
            duration=duration
        )
    except Exception as e:
        logger.error(f"Error collecting netstat UDP metrics: {e}")
        return NetworkMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(timezone.utc).isoformat(),
            category="network_netstat_udp",
            duration=duration
        )

# ==================== Startup and Main ====================

async def startup_event():
    """Startup event handler"""
    logger.info("")
    logger.info("🚀 Starting OVN-Kubernetes Network Analyzer MCP Server...")
    logger.info("")
    
    try:
        await initialize_components()
        logger.info("✅ OVN-Kubernetes Network Analyzer MCP Server started successfully!")
        logger.info("")
    except Exception as e:
        logger.error(f"❌ Failed to initialize: {e}")
        logger.error("")

async def shutdown_handler():
    """Handle graceful shutdown"""
    logger.info("Shutdown handler called")
    await cleanup_resources()
    logger.info("Shutdown complete")

async def main():
    """Main entry point with improved error handling and graceful shutdown"""
    try:
        # Initialize components
        await startup_event()
        
        # Create tasks for server and shutdown handler
        server_task = asyncio.create_task(
            app.run_async(
                transport="streamable-http",
                host="0.0.0.0",
                port=8002,
                uvicorn_config={"ws": "none"}
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
        
        # If server task completed, check for exceptions
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