#!/usr/bin/env python3
"""
OpenShift OVN-Kubernetes Analyzer MCP Server
Main server implementation using FastMCP with streamable-http transport
Refactored to match actual collector implementations
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

# Suppress warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning, module="anyio.streams.memory")

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
    from tools.ovnk.ovnk_baseinfo import ovnDBCollector
    from tools.ovnk.ovnk_kubelet_cni import KubeletCNICollector
    from tools.ovnk.ovnk_latency import OVNLatencyCollector
    from tools.ovnk.ovnk_ovs_usage import OVSUsageCollector
    from tools.pods.pods_usage import PodsUsageCollector
    from tools.ocp.cluster_apistats import apiUsageCollector
    from tools.ocp.cluster_info import ClusterInfoCollector
    from tools.net.network_io import NetworkIOCollector
    from tools.utils.promql_basequery import PrometheusBaseQuery
    from config.metrics_config_reader import Config
    from tools.node.node_usage import nodeUsageCollector

except ImportError as e:
    logger.error(f"Failed to import local modules: {e}")
    logger.error("Please ensure all modules are in the correct directory structure")
    sys.exit(1)


# Pydantic models
class MCPBaseModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

class DurationInput(MCPBaseModel):
    duration: str = Field(default="1h", description="Time duration for metrics collection (e.g., '5m', '1h', '6h')")

class TimeRangeInput(MCPBaseModel):
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    duration: Optional[str] = Field(default="1h", description="Duration if start/end not provided")

class ClusterInfoRequest(MCPBaseModel):
    include_node_details: bool = Field(default=True, description="Include detailed node information")
    include_resource_counts: bool = Field(default=True, description="Include resource counts")
    include_network_policies: bool = Field(default=True, description="Include network policy information")
    include_operator_status: bool = Field(default=True, description="Include cluster operator status")
    include_mcp_status: bool = Field(default=True, description="Include Machine Config Pool status")

class OVNKPodMetricsRequest(MCPBaseModel):
    duration: str = Field(default="1h", description="Query duration")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    include_node: bool = Field(default=True, description="Include ovnkube-node metrics")

class MultusPodMetricsRequest(MCPBaseModel):
    duration: str = Field(default="1h", description="Query duration")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")

class MetricResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: Optional[str] = None
    duration: Optional[str] = None

class NetworkIORequest(MCPBaseModel):
    duration: str = Field(default="5m", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    include_metrics: Optional[List[str]] = Field(default=None, description="Filter specific metrics")
    node_groups: Optional[List[str]] = Field(default=None, description="Filter by node groups")

class HealthCheckRequest(MCPBaseModel):
    pass

class NetworkMetricsResponse(MCPBaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: Optional[str] = None
    duration: Optional[str] = None

class HealthCheckResponse(MCPBaseModel):
    status: str
    timestamp: str
    mcp_server: Dict[str, Any]
    prometheus: Dict[str, Any]
    kubeapi: Dict[str, Any]


# Initialize MCP server
mcp = FastMCP("OpenShift OVN-Kubernetes Analyzer")

# Global variables for collectors
auth_manager = None
config = None
ovn_db_collector = None
kubelet_cni_collector = None
latency_collector = None
ovs_collector = None
pods_collector = None
api_collector = None
cluster_info_collector = None
network_collector = None
node_usage_collector = None

async def initialize_collectors():
    """Initialize all collectors with authentication"""
    global auth_manager, config, ovn_db_collector, kubelet_cni_collector
    global latency_collector, ovs_collector, pods_collector, api_collector
    global cluster_info_collector, network_collector

    try:
        logger.info("=" * 70)
        logger.info("Initializing OpenShift OVN-Kubernetes Analyzer components...")
        logger.info("=" * 70)
        
        # Initialize global config
        config = Config()
        
        # Load network metrics configuration
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        metrics_net_file = os.path.join(project_root, 'config', 'metrics-net.yml')
        load_result = config.load_metrics_file(metrics_net_file)
        if load_result.get('success'):
            logger.info(f"✅ Loaded {load_result.get('metrics_loaded', 0)} network metrics from {metrics_net_file}")
        else:
            logger.warning(f"⚠️ Failed to load network metrics file: {load_result.get('error', 'Unknown error')}")
        
        # Initialize OpenShift authentication
        logger.info("🔗 Initializing OpenShift authentication...")
        auth_manager = OpenShiftAuth(config.kubeconfig_path)
        await auth_manager.initialize()
        logger.info("✅ OpenShift authentication initialized successfully")
        
        # Initialize collectors
        logger.info("📊 Initializing metric collectors...")
        logger.info("-" * 70)
        
        # Cluster Info Collector
        logger.info("Initializing ClusterInfoCollector...")
        cluster_info_collector = ClusterInfoCollector()
        await cluster_info_collector.initialize()
        logger.info("✅ ClusterInfoCollector initialized")
        
        # OVN Database Collector
        logger.info("Initializing ovnDBCollector...")
        ovn_db_collector = ovnDBCollector(auth_manager)
        logger.info("✅ ovnDBCollector initialized")
        
        # Kubelet CNI Collector - Don't initialize here, create on-demand
        logger.info("✅ KubeletCNICollector will be created on-demand")
        
        # Latency Collector
        logger.info("Initializing OVNLatencyCollector...")
        latency_collector = OVNLatencyCollector(auth_manager)
        logger.info("✅ OVNLatencyCollector initialized")
        
        # OVS Usage Collector
        logger.info("Initializing OVSUsageCollector...")
        ovs_collector = OVSUsageCollector(auth_manager)
        logger.info("✅ OVSUsageCollector initialized")
        
        # Pods Usage Collector
        logger.info("Initializing PodsUsageCollector...")
        pods_collector = PodsUsageCollector(auth_manager)
        logger.info("✅ PodsUsageCollector initialized")
        
        # API Usage Collector
        logger.info("Initializing apiUsageCollector...")
        api_collector = apiUsageCollector(auth_manager)
        logger.info("✅ apiUsageCollector initialized")
        
        # Network IO Collector
        logger.info("Initializing NetworkIOCollector...")
        network_collector = NetworkIOCollector(
            prometheus_url=auth_manager.prometheus_url,
            token=getattr(auth_manager, 'prometheus_token', None)
        )
        await network_collector.initialize()
        logger.info("✅ NetworkIOCollector initialized")
        
        logger.info("-" * 70)
        logger.info("✅ All collectors initialized successfully!")
        logger.info("=" * 70)
        
        return True
        
    except Exception as e:
        logger.error("=" * 70)
        logger.error(f"❌ Failed to initialize collectors: {e}")
        logger.error("=" * 70)
        return False

@mcp.tool()
async def get_mcp_health_status(request: HealthCheckRequest | None = None) -> HealthCheckResponse:
    """
    Get MCP server health status and connectivity checks.
    
    Verifies server status and connectivity to:
    - MCP server operational status
    - Prometheus connectivity and metrics availability
    - Kubernetes API server connectivity
    
    Returns:
        HealthCheckResponse: Server health status with component connectivity details
    """
    global auth_manager, config
    
    try:
        if not auth_manager:
            try:
                await initialize_collectors()
            except Exception as init_error:
                logger.error(f"Failed to initialize collectors: {init_error}")
                return HealthCheckResponse(
                    status="error",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    mcp_server={"running": False, "error": str(init_error)},
                    prometheus={"connected": False},
                    kubeapi={"connected": False}
                )

        # Test Prometheus
        prometheus_ok = False
        prometheus_error: Optional[str] = None
        try:
            if auth_manager:
                # Get Prometheus config and test connection
                prometheus_config = auth_manager.get_prometheus_config()
                if prometheus_config and prometheus_config.get('url'):
                    # Create a temporary PrometheusBaseQuery to test connection
                    from tools.utils.promql_basequery import PrometheusBaseQuery
                    token = prometheus_config.get('token')
                    async with PrometheusBaseQuery(prometheus_config['url'], token) as prom:
                        # Try a simple query to test connectivity
                        test_result = await prom.query_instant("up")
                        prometheus_ok = test_result is not None
                else:
                    prometheus_error = "Prometheus URL not configured"
        except asyncio.TimeoutError:
            prometheus_error = "Connection timeout"
        except Exception as e:
            prometheus_error = str(e)
            logger.debug(f"Prometheus connection test failed: {e}")

        # Test Kube API
        kubeapi_ok = False
        kubeapi_error: Optional[str] = None
        try:
            if auth_manager:
                # Test Kubernetes API connection
                kubeapi_ok = await asyncio.wait_for(
                    auth_manager.test_kubeapi_connection(), timeout=10.0
                ) if hasattr(auth_manager, 'test_kubeapi_connection') else False
        except asyncio.TimeoutError:
            kubeapi_error = "Connection timeout"
        except Exception as e:
            kubeapi_error = str(e)
            logger.debug(f"KubeAPI connection test failed: {e}")

        status = "healthy" if prometheus_ok and kubeapi_ok else ("degraded" if prometheus_ok or kubeapi_ok else "unhealthy")

        return HealthCheckResponse(
            status=status,
            timestamp=datetime.now(pytz.UTC).isoformat(),
            mcp_server={"running": True, "transport": "streamable-http"},
            prometheus={
                "connected": prometheus_ok,
                "url": getattr(auth_manager, "prometheus_url", None) if auth_manager else None,
                "error": prometheus_error,
            },
            kubeapi={
                "connected": kubeapi_ok,
                "node_count": (auth_manager.cluster_info.get("node_count") if (auth_manager and hasattr(auth_manager, 'cluster_info') and auth_manager.cluster_info) else None),
                "error": kubeapi_error,
            },
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        return HealthCheckResponse(
            status="error",
            timestamp=datetime.now(pytz.UTC).isoformat(),
            mcp_server={"running": False, "error": str(e)},
            prometheus={"connected": False},
            kubeapi={"connected": False}
        )


@mcp.tool()
async def get_cluster_info(request: ClusterInfoRequest | None = None) -> MetricResponse:
    """
    Get comprehensive OpenShift cluster information and infrastructure details.
    
    Collects:
    - Cluster identification (name, version, platform)
    - Node information (master, infra, worker nodes with specs and status)
    - Resource counts (namespaces, pods, services, secrets, configmaps)
    - Network policy counts
    - Cluster operator status
    - Machine Config Pool status
    """
    try:
        if cluster_info_collector is None:
            return MetricResponse(
                status="error",
                error="Cluster info collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat()
            )

        info = await cluster_info_collector.collect_cluster_info()
        cluster_data = cluster_info_collector.to_dict(info)
        
        # Apply filtering if request provided
        if request:
            if not request.include_node_details:
                cluster_data.pop('master_nodes', None)
                cluster_data.pop('infra_nodes', None)
                cluster_data.pop('worker_nodes', None)
            
            if not request.include_resource_counts:
                for field in ['namespaces_count', 'pods_count', 'services_count',
                            'secrets_count', 'configmaps_count']:
                    cluster_data.pop(field, None)
            
            if not request.include_network_policies:
                for field in ['networkpolicies_count', 'adminnetworkpolicies_count',
                            'baselineadminnetworkpolicies_count', 'egressfirewalls_count',
                            'egressips_count', 'clusteruserdefinednetworks_count',
                            'userdefinednetworks_count']:
                    cluster_data.pop(field, None)
            
            if not request.include_operator_status:
                cluster_data.pop('unavailable_cluster_operators', None)
            
            if not request.include_mcp_status:
                cluster_data.pop('mcp_status', None)
        
        return MetricResponse(
            status="success",
            data=cluster_data,
            timestamp=datetime.now(pytz.UTC).isoformat(),
            category="cluster_info"
        )
    except Exception as e:
        logger.error(f"Error collecting cluster info: {e}")
        return MetricResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat()
        )

@mcp.tool()
async def get_ocp_node_usage(request: DurationInput | None = None) -> MetricResponse:
    """
    Get comprehensive node usage metrics for OpenShift cluster nodes.
    
    Monitors resource utilization metrics at the node and cgroup level for:
    - Master/Control Plane nodes (all nodes)
    - Infra nodes (all nodes)
    - Workload nodes (all nodes)
    - Worker nodes (top 3 by CPU usage)
    
    Metrics collected for each node group:
    - Node CPU usage by mode (user, system, idle, iowait, etc.)
    - Node memory used (active memory consumption)
    - Node memory cache and buffer (filesystem cache and buffers)
    - Cgroup CPU usage (CPU consumption per control group)
    - Cgroup RSS usage (Resident Set Size memory per control group)
    
    These metrics provide insights into:
    - Overall node resource utilization and capacity across all node roles
    - CPU contention and workload distribution patterns
    - Memory pressure and caching efficiency
    - Container-level resource consumption via cgroups
    - Potential resource bottlenecks affecting cluster performance
    
    Args:
        request: DurationInput with duration field. Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
    
    Returns:
        MetricResponse: Node usage metrics organized by node role (master/infra/workload/worker) with top 3 worker nodes
    """
    try:
        if request is None:
            request = DurationInput()
        
        duration = request.duration
        
        global auth_manager, node_usage_collector
        if not node_usage_collector:
            # Lazy initialize if startup initialization didn't complete
            if auth_manager is None:
                auth_manager = OpenShiftAuth(config.kubeconfig_path if config else None)
                try:
                    await auth_manager.initialize()
                except Exception:
                    return MetricResponse(
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
                return MetricResponse(
                    status="error",
                    error=f"Failed to initialize nodeUsageCollector: {e}",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=duration
                )
        
        # Collect metrics for master/infra/workload nodes and top 3 worker nodes
        combined_results = {
            'status': 'success',
            'timestamp': datetime.now(pytz.UTC).isoformat(),
            'duration': duration,
            'node_groups': {}
        }
        
        # Query master/controlplane nodes
        try:
            master_result = await node_usage_collector.collect_all_metrics(node_group='master', duration=duration)
            if master_result.get('status') == 'success':
                combined_results['node_groups']['master'] = master_result
            else:
                combined_results['node_groups']['master'] = {'status': 'error', 'error': master_result.get('error')}
        except Exception as e:
            logger.error(f"Error collecting master node metrics: {e}")
            combined_results['node_groups']['master'] = {'status': 'error', 'error': str(e)}
        
        # Query infra nodes
        try:
            infra_result = await node_usage_collector.collect_all_metrics(node_group='infra', duration=duration)
            if infra_result.get('status') == 'success':
                combined_results['node_groups']['infra'] = infra_result
            else:
                combined_results['node_groups']['infra'] = {'status': 'error', 'error': infra_result.get('error')}
        except Exception as e:
            logger.error(f"Error collecting infra node metrics: {e}")
            combined_results['node_groups']['infra'] = {'status': 'error', 'error': str(e)}
        
        # Query workload nodes
        try:
            workload_result = await node_usage_collector.collect_all_metrics(node_group='workload', duration=duration)
            if workload_result.get('status') == 'success':
                combined_results['node_groups']['workload'] = workload_result
            else:
                combined_results['node_groups']['workload'] = {'status': 'error', 'error': workload_result.get('error')}
        except Exception as e:
            logger.error(f"Error collecting workload node metrics: {e}")
            combined_results['node_groups']['workload'] = {'status': 'error', 'error': str(e)}
        
        # Query worker nodes (top 3)
        try:
            worker_result = await node_usage_collector.collect_all_metrics(node_group='worker', duration=duration, top_n_workers=3)
            if worker_result.get('status') == 'success':
                combined_results['node_groups']['worker'] = worker_result
            else:
                combined_results['node_groups']['worker'] = {'status': 'error', 'error': worker_result.get('error')}
        except Exception as e:
            logger.error(f"Error collecting worker node metrics: {e}")
            combined_results['node_groups']['worker'] = {'status': 'error', 'error': str(e)}
        
        # Determine overall status
        successful_groups = sum(1 for group in combined_results['node_groups'].values() if group.get('status') == 'success')
        total_groups = len(combined_results['node_groups'])
        
        if successful_groups == 0:
            combined_results['status'] = 'error'
            combined_results['error'] = 'All node group queries failed'
        elif successful_groups < total_groups:
            combined_results['status'] = 'partial'
        else:
            combined_results['status'] = 'success'
        
        return MetricResponse(
            status=combined_results.get('status', 'unknown'),
            data=combined_results,
            error=combined_results.get('error'),
            timestamp=combined_results.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            category="node_usage",
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting node usage metrics: {e}")
        return MetricResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            category="node_usage",
            duration=duration
        )


@mcp.tool()
async def get_ovn_database_info(request: TimeRangeInput | None = None) -> MetricResponse:
    """
    Get cluster-wide OVN database size summary with health recommendations.
    
    Provides:
    - Total OVN pods count
    - Database sizes (northbound and southbound) in MB
    - Grouped statistics by node role
    - Health recommendations based on size thresholds
    """
    try:
        if ovn_db_collector is None:
            return MetricResponse(
                status="error",
                error="OVN DB collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat()
            )
        
        result = await ovn_db_collector.get_cluster_summary()
        
        return MetricResponse(
            status=result.get('status', 'unknown'),
            data=result,
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            category="ovn_database_summary"
        )
        
    except Exception as e:
        logger.error(f"Error collecting OVN database summary: {e}")
        return MetricResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat()
        )

@mcp.tool()
async def get_kubelet_cni_stats(request: TimeRangeInput | None = None) -> MetricResponse:
    """
    Get Kubelet CNI and CRIO metrics for OpenShift nodes with integrated cluster summary.
    
    Collects comprehensive CNI metrics including:
    - CNI/CRIO CPU usage (percentage)
    - CNI/CRIO memory usage (bytes/MB)
    - CNI/CRIO network usage (bytes/packets)
    - Network drops and errors (packets)
    - Network utilization
    - Container threads count
    - I/O operations (read/write IOPS)
    
    All metrics are grouped by node role (controlplane, worker, infra, workload).
    
    Also includes integrated cluster summary with:
    - Total nodes count and distribution by role
    - CNI/CRIO CPU and memory usage indicators (max and average)
    - Network drops and errors totals
    - Thread counts and IOPS metrics
    - Overall cluster health status
    - Performance recommendations
    """
    try:
        if auth_manager is None:
            return MetricResponse(
                status="error",
                error="Authentication manager not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat()
            )
        
        # Parse request parameters
        duration = "1h"
        start_time = None
        end_time = None
        
        if request:
            if request.start_time and request.end_time:
                start_time = request.start_time
                end_time = request.end_time
            elif request.duration:
                duration = request.duration
        
        # Create a new collector instance for this request
        collector = KubeletCNICollector(
            ocp_auth=auth_manager,
            duration=duration,
            start_time=start_time,
            end_time=end_time
        )
        
        # Collect all metrics with integrated summary
        result = await collector.collect_all_metrics()
        
        return MetricResponse(
            status=result.get('status', 'unknown'),
            data=result,
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            category="kubelet_cni",
            duration=duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting Kubelet CNI metrics: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return MetricResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat()
        )

@mcp.tool()
async def get_ovn_latency_stats(request: DurationInput | None = None) -> MetricResponse:
    """
    Get comprehensive OVN-Kubernetes latency metrics.
    
    Collects latency metrics including:
    - CNI request latency (ADD/DEL operations) - p99
    - Pod annotation latency - p99
    - Pod first seen to LSP created latency - p99
    - Pod LSP creation latency - p99
    - Pod port binding latency - p99
    - Service sync latency (average and p99)
    - Network config application duration - p99
    - Controller/node ready duration
    - Controller sync duration (average and p95)
    
    All metrics include pod-level details with node mapping.
    """
    try:
        if latency_collector is None:
            return MetricResponse(
                status="error",
                error="Latency collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat()
            )
        
        if request is None:
            request = DurationInput()
        
        # Update collector duration if different
        if latency_collector.duration != request.duration:
            latency_collector.duration = request.duration
        
        result = await latency_collector.collect_all_metrics()
        
        return MetricResponse(
            status=result.get('status', 'unknown'),
            data=result,
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            category="ovn_latency",
            duration=request.duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting latency metrics: {e}")
        return MetricResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat()
        )

@mcp.tool()
async def get_ovs_usage(request: DurationInput | None = None) -> MetricResponse:
    """
    Get OVS (Open vSwitch) performance metrics.
    
    Collects OVS metrics including:
    - OVS vswitchd CPU usage (percentage)
    - OVSDB server CPU usage (percentage)
    - OVS database memory size (bytes/MB)
    - OVS vswitchd memory size (bytes/MB)
    - Datapath flows total count
    - Bridge flows (br-int and br-ex)
    - Stream connections (open)
    - Remote connection overflow/discarded counts
    - Megaflow cache hits/misses
    - Datapath packet/error rates
    - Interface RX/TX bytes rates
    - Interface RX dropped rate
    
    All metrics are organized by node/pod/bridge as applicable.
    """
    try:
        if ovs_collector is None:
            return MetricResponse(
                status="error",
                error="OVS collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat()
            )
        
        if request is None:
            request = DurationInput()
        
        # Update collector duration if different
        if ovs_collector.duration != request.duration:
            ovs_collector.duration = request.duration
        
        result = await ovs_collector.collect_all_metrics()
        
        return MetricResponse(
            status=result.get('status', 'unknown'),
            data=result,
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            category="ovs_metrics",
            duration=request.duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting OVS metrics: {e}")
        return MetricResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat()
        )

@mcp.tool()
async def get_ovnk_pods_usage(request: OVNKPodMetricsRequest | None = None) -> MetricResponse:
    """
    Get OVN-Kubernetes pod resource usage metrics for specific containers.
    
    Collects CPU and memory usage for OVN-Kubernetes containers:
    
    **ovnkube-controller pods:**
    - ovnkube-controller (main controller container)
    
    **ovnkube-node pods:**
    - ovn-controller (OVN controller container)
    - northd (Northd daemon container)
    - nbdb (Northbound database container)
    - sbdb (Southbound database container)
    
    All pods are from the openshift-ovn-kubernetes namespace.
    
    Returns CPU usage (percentage) and memory usage (RSS and working set in bytes/MB)
    with node mapping for each container.
    """
    try:
        if pods_collector is None:
            return MetricResponse(
                status="error",
                error="Pods collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat()
            )
        
        if request is None:
            request = OVNKPodMetricsRequest()
        
        # Collect metrics for ovnkube-node containers
        node_result = None
        if request.include_node:
            # Query for specific containers in ovnkube-node pods
            node_result = await pods_collector.collect_all_metrics(
                pod_pattern="ovnkube-node.*",
                container_pattern="(ovnkube-controller|ovn-controller|northd|nbdb|sbdb)",
                namespace_pattern="openshift-ovn-kubernetes",
                start_time=request.start_time,
                end_time=request.end_time
            )
        
        # Combine results
        combined_data = {
            'namespace': 'openshift-ovn-kubernetes',
            'duration': request.duration,
            'ovnkube_node_containers': node_result if node_result else {'status': 'skipped'}
        }
        
        # Determine overall status
        overall_status = "success"
        errors = []
        
        if node_result and node_result.get('status') == 'error':
            errors.append(f"Node containers: {node_result.get('error')}")
        
        if errors:
            overall_status = "partial" if len(errors) < 2 else "error"
            combined_data['errors'] = errors
        
        return MetricResponse(
            status=overall_status,
            data=combined_data,
            error="; ".join(errors) if errors else None,
            timestamp=datetime.now(pytz.UTC).isoformat(),
            category="ovnk_pods_usage",
            duration=request.duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting OVN-K pods usage metrics: {e}")
        return MetricResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat()
        )

@mcp.tool()
async def get_multus_pods_usage(request: MultusPodMetricsRequest | None = None) -> MetricResponse:
    """
    Get Multus pod resource usage metrics in openshift-multus namespace.
    
    Collects CPU and memory usage for Multus CNI pods:
    - multus-* pods (all Multus daemon pods)
    - All containers within Multus pods
    
    Returns CPU usage (percentage) and memory usage (RSS and working set in bytes/MB)
    with node mapping for each container.
    
    Multus is responsible for managing multiple network interfaces for pods.
    """
    try:
        if pods_collector is None:
            return MetricResponse(
                status="error",
                error="Pods collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat()
            )
        
        if request is None:
            request = MultusPodMetricsRequest()
        
        # Collect metrics for all multus pods
        result = await pods_collector.collect_all_metrics(
            pod_pattern="multus.*|network-metrics.*",
            container_pattern=".*",
            namespace_pattern="openshift-multus",
            start_time=request.start_time,
            end_time=request.end_time
        )
        
        # Enhance result with namespace context
        if result and result.get('status') == 'success':
            result['namespace'] = 'openshift-multus'
            result['description'] = 'Multus CNI pod resource usage metrics'
        
        return MetricResponse(
            status=result.get('status', 'unknown'),
            data=result,
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            category="multus_pods_usage",
            duration=request.duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting Multus pods usage metrics: {e}")
        return MetricResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat()
        )


@mcp.tool()
async def get_api_server_stats(request: DurationInput | None = None) -> MetricResponse:
    """
    Get Kubernetes API server performance metrics.
    
    Collects API server metrics including:
    - Read-only API calls latency (average and max)
    - Mutating API calls latency (average and max)
    - API request rate (requests per second)
    - API request errors
    - Current inflight requests
    - etcd request duration (p99)
    - Request latency by verb (p99)
    - Request duration by resource (p99)
    - Priority and Fairness metrics:
      - Request wait duration (p99)
      - Request execution duration (p99)
      - Request dispatch rate
      - Requests in queue
    
    All metrics include detailed labels (resource, verb, scope, operation, etc.).
    """
    try:
        if api_collector is None:
            return MetricResponse(
                status="error",
                error="API collector not initialized",
                timestamp=datetime.now(pytz.UTC).isoformat()
            )
        
        if request is None:
            request = DurationInput(duration="5m")
        
        # Update collector duration if different
        if api_collector.duration != request.duration:
            api_collector.duration = request.duration
        
        result = await api_collector.collect_all_metrics()
        
        return MetricResponse(
            status=result.get('status', 'unknown'),
            data=result,
            error=result.get('error'),
            timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
            category="api_server",
            duration=request.duration
        )
        
    except Exception as e:
        logger.error(f"Error collecting API server metrics: {e}")
        return MetricResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat()
        )

@mcp.tool()
async def get_network_io(request: NetworkIORequest) -> NetworkMetricsResponse:
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
    global auth_manager, config
    duration = request.duration
    try:
        if not auth_manager or not config:
            await initialize_collectors()
        
        # Get Prometheus config from auth_manager
        prometheus_config = auth_manager.get_prometheus_config()
        
        collector = NetworkIOCollector(
            prometheus_url=prometheus_config.get('url'),
            token=prometheus_config.get('token'),
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
                timestamp=datetime.now(pytz.UTC).isoformat(),
                category="network_io",
                duration=duration
            )
            
        finally:
            await collector.close()
        
    except asyncio.TimeoutError:
        return NetworkMetricsResponse(
            status="error",
            error="Timeout collecting network I/O metrics",
            timestamp=datetime.now(pytz.UTC).isoformat(),
            category="network_io",
            duration=duration
        )
    except Exception as e:
        logger.error(f"Error collecting network I/O: {e}")
        return NetworkMetricsResponse(
            status="error",
            error=str(e),
            timestamp=datetime.now(pytz.UTC).isoformat(),
            category="network_io",
            duration=duration
        )

async def startup_event():
    """Startup event handler"""
    logger.info("")
    logger.info("🚀 Starting OpenShift OVN-Kubernetes Analyzer MCP Server...")
    logger.info("")
    
    # Initialize collectors
    init_success = await initialize_collectors()
    if not init_success:
        logger.error("❌ Failed to initialize collectors. Server may not function properly.")
        logger.error("")
    else:
        logger.info("✅ OpenShift OVN-Kubernetes Analyzer MCP Server started successfully!")
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
            port = 8003

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
            logger.info(f"Starting MCP server on {host}:{port}")
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