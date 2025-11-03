"""
etcd Node Usage Collector Module
Collects and analyzes node usage metrics for master nodes with capacity information
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.metrics_config_reader import Config
from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility


class nodeUsageCollector:
    """Collector for node usage metrics"""
    
    def __init__(self, ocp_auth, prometheus_config: Dict[str, Any]):
        self.ocp_auth = ocp_auth
        self.prometheus_config = prometheus_config
        self.logger = logging.getLogger(__name__)
        
        # Initialize config and load metrics file
        self.config = Config()
        self._load_metrics_config()
        
        # Initialize utility for node operations and common functions
        self.utility = mcpToolsUtility(ocp_auth)
        
        # Extract token using utility function
        self._prom_token = mcpToolsUtility.extract_token_from_config(prometheus_config)

    def _load_metrics_config(self):
        """Load node metrics configuration file"""
        try:
            # Calculate project root (go up from tools/node/ to project root)
            project_root = Path(__file__).parent.parent.parent
            metrics_file_path = project_root / 'config' / 'metrics-node.yml'
            
            self.logger.info(f"Looking for metrics file at: {metrics_file_path.absolute()}")
            
            if not metrics_file_path.exists():
                self.logger.error(f"✗ Metrics file not found at: {metrics_file_path.absolute()}")
                self.logger.error(f"  Current file location: {Path(__file__).absolute()}")
                self.logger.error(f"  Project root: {project_root.absolute()}")
                
                # Try alternative paths
                alternative_paths = [
                    Path.cwd() / 'config' / 'metrics-node.yml',
                    Path.cwd().parent / 'config' / 'metrics-node.yml',
                ]
                
                for alt_path in alternative_paths:
                    if alt_path.exists():
                        self.logger.info(f"✓ Found metrics file at alternative location: {alt_path}")
                        metrics_file_path = alt_path
                        break
                else:
                    self.logger.error("✗ Could not find metrics file in any location")
                    return
            else:
                self.logger.info(f"✓ Found metrics file at: {metrics_file_path}")
            
            # Load the metrics file
            self.logger.info(f"Loading metrics from: {metrics_file_path.absolute()}")
            result = self.config.load_metrics_file(str(metrics_file_path.absolute()))
            
            if result.get('success'):
                self.logger.info(f"✓ Loaded {result.get('metrics_loaded', 0)} metrics from {result.get('file_name')}")
                self.logger.info(f"✓ Categories: {result.get('categories_loaded', [])}")
                
                # Verify metrics are actually loaded
                all_metrics = self.config.get_all_metrics()
                self.logger.info(f"✓ Total metrics in config: {self.config.get_metrics_count()}")
                self.logger.info(f"✓ Available categories: {list(all_metrics.keys())}")
                
                # List all loaded metric names for debugging
                for category, metrics_list in all_metrics.items():
                    metric_names = [m.get('name') for m in metrics_list]
                    self.logger.info(f"  Category '{category}': {metric_names}")
            else:
                self.logger.error(f"✗ Failed to load metrics: {result.get('error')}")
                
        except Exception as e:
            self.logger.error(f"✗ Error loading metrics config: {e}", exc_info=True)

    async def _query_range_wrap(self, prom: PrometheusBaseQuery, query: str, start: str, end: str, step: str) -> Dict[str, Any]:
        """Wrapper for range query"""
        data = await prom.query_range(query, start, end, step)
        return {'status': 'success', 'data': data}

    async def _query_instant_wrap(self, prom: PrometheusBaseQuery, query: str) -> Dict[str, Any]:
        """Wrapper for instant query"""
        data = await prom.query_instant(query)
        return {'status': 'success', 'data': data}
    
    async def _collect_node_memory_capacity(self, prom: PrometheusBaseQuery,
                                            master_nodes: List[str]) -> Dict[str, float]:
        """Collect total memory capacity for each node"""
        try:
            node_pattern = mcpToolsUtility.get_node_pattern(master_nodes)
            query = f'node_memory_MemTotal_bytes{{instance=~"{node_pattern}"}}'
            
            self.logger.debug(f"Querying node memory capacity: {query}")
            
            # Use instant query to get current capacity
            result = await self._query_instant_wrap(prom, query)
            
            if result['status'] != 'success':
                self.logger.warning(f"Failed to get memory capacity: {result.get('error')}")
                return {}
            
            raw_results = result.get('data', {}).get('result', [])
            
            # Extract capacity for each node
            capacities = {}
            for item in raw_results:
                instance = item.get('metric', {}).get('instance', 'unknown')
                value = item.get('value', [None, None])
                
                if len(value) >= 2:
                    try:
                        # Convert bytes to GB using utility function
                        capacity_bytes = float(value[1])
                        capacity_gb = mcpToolsUtility.bytes_to_gb(capacity_bytes)
                        capacities[instance] = capacity_gb
                        self.logger.info(f"Node {instance} memory capacity: {capacity_gb} GB")
                    except (ValueError, TypeError, IndexError) as e:
                        self.logger.warning(f"Failed to parse capacity for {instance}: {e}")
            
            return capacities
            
        except Exception as e:
            self.logger.error(f"Error collecting node memory capacity: {e}", exc_info=True)
            return {}
    
    def _verify_metric_loaded(self, metric_name: str) -> bool:
        """Verify a metric is loaded and log details if not found"""
        metric = self.config.get_metric_by_name(metric_name)
        
        if not metric:
            self.logger.error(f"✗ Metric '{metric_name}' not found in config")
            
            # Debug: show what we have
            all_metrics = self.config.get_all_metrics()
            self.logger.error(f"  Total metrics loaded: {self.config.get_metrics_count()}")
            self.logger.error(f"  Available categories: {list(all_metrics.keys())}")
            
            # Show all available metric names
            all_names = []
            for category, metrics_list in all_metrics.items():
                for m in metrics_list:
                    all_names.append(m.get('name', 'unnamed'))
            
            self.logger.error(f"  Available metric names: {all_names}")
            return False
        else:
            self.logger.debug(f"✓ Metric '{metric_name}' found in category '{metric.get('category')}'")
            return True
    
    async def _get_master_nodes(self) -> List[str]:
        """Return list of controlplane/master node names using node groups from utility."""
        try:
            groups = await self.utility.get_node_groups()
            controlplane = groups.get('controlplane', []) if isinstance(groups, dict) else []
            master_nodes = [n.get('name', '').split(':')[0] for n in controlplane]
            self.logger.info(f"Retrieved {len(master_nodes)} master nodes: {master_nodes}")
            return master_nodes
        except Exception as e:
            self.logger.error(f"Error getting master nodes: {e}", exc_info=True)
            return []
        
    async def collect_all_metrics(self, duration: str = "1h") -> Dict[str, Any]:
        """Collect all node usage metrics for master nodes"""
        try:
            self.logger.info("Starting node usage metrics collection")
            
            # Get master nodes
            master_nodes = await self._get_master_nodes()
            if not master_nodes:
                return {
                    'status': 'error',
                    'error': 'No master nodes found',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
            
            self.logger.info(f"Found {len(master_nodes)} master nodes: {master_nodes}")
            
            # Build prometheus config using utility function
            prom_config = mcpToolsUtility.build_prometheus_config(
                self.prometheus_config, 
                self.ocp_auth
            )
            
            if not prom_config.get('url'):
                return {
                    'status': 'error',
                    'error': 'Prometheus URL not configured',
                    'hint': "Provide prometheus_config['url'] or set PROMETHEUS_URL env var",
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
            
            async with PrometheusBaseQuery(prom_config['url'], self._prom_token) as prom:
                # Get time range
                start_str, end_str = prom.get_time_range_from_duration(duration)
                
                self.logger.info(f"Querying metrics from {start_str} to {end_str}")
                
                # First, collect node memory capacities
                node_capacities = await self._collect_node_memory_capacity(prom, master_nodes)
                
                # Collect metrics using range queries
                cpu_usage = await self._collect_node_cpu_usage(prom, master_nodes, start_str, end_str)
                memory_used = await self._collect_node_memory_used(
                    prom, master_nodes, start_str, end_str, node_capacities=node_capacities
                )
                memory_cache = await self._collect_node_memory_cache_buffer(
                    prom, master_nodes, start_str, end_str, node_capacities=node_capacities
                )
                cgroup_cpu = await self._collect_cgroup_cpu_usage(prom, master_nodes, start_str, end_str)
                cgroup_rss = await self._collect_cgroup_rss_usage(prom, master_nodes, start_str, end_str)
            
            # Aggregate results
            result = {
                'status': 'success',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'duration': duration,
                'time_range': {
                    'start': start_str,
                    'end': end_str
                },
                'node_group': 'master',
                'total_nodes': len(master_nodes),
                'node_capacities': {node: {'memory': capacity} for node, capacity in node_capacities.items()},
                'metrics': {
                    'cpu_usage': cpu_usage,
                    'memory_used': memory_used,
                    'memory_cache_buffer': memory_cache,
                    'cgroup_cpu_usage': cgroup_cpu,
                    'cgroup_rss_usage': cgroup_rss
                }
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error collecting node usage metrics: {e}", exc_info=True)
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
    
    async def _collect_node_cpu_usage(self, prom: PrometheusBaseQuery, 
                                     master_nodes: List[str], 
                                     start: str, end: str, 
                                     step: str = '15s') -> Dict[str, Any]:
        """Collect node CPU usage by mode for master nodes"""
        try:
            if not self._verify_metric_loaded('node_cpu_usage'):
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            metric_config = self.config.get_metric_by_name('node_cpu_usage')
            
            # Build query with node pattern using utility function
            node_pattern = mcpToolsUtility.get_node_pattern(master_nodes)
            query = f'sum by (instance, mode)(irate(node_cpu_seconds_total{{instance=~"{node_pattern}",job=~".*"}}[5m])) * 100'
            
            self.logger.debug(f"Querying CPU usage: {query}")
            
            # Execute range query
            result = await self._query_range_wrap(prom, query, start, end, step)
            
            if result['status'] != 'success':
                self.logger.error(f"Query failed: {result.get('error')}")
                return {'status': 'error', 'error': result.get('error', 'Query failed')}
            
            # Process raw results
            raw_results = result.get('data', {}).get('result', [])
            self.logger.info(f"Got {len(raw_results)} CPU series")
            
            # Group by instance to collect time series
            node_time_series = {}
            for item in raw_results:
                instance = item.get('metric', {}).get('instance', 'unknown')
                mode = item.get('metric', {}).get('mode', 'unknown')
                values = item.get('values', [])
                
                if instance not in node_time_series:
                    node_time_series[instance] = {'modes': {}}
                
                # Extract numeric values using utility function
                mode_values = mcpToolsUtility.extract_numeric_values(values)
                
                if mode_values:
                    node_time_series[instance]['modes'][mode] = mode_values
            
            # Calculate per-node statistics
            nodes_result = {}
            for instance, data in node_time_series.items():
                node_data = {'modes': {}, 'unit': 'percent'}
                
                # Calculate stats for each mode using utility function
                all_mode_values = []
                for mode, mode_values in data['modes'].items():
                    stats = mcpToolsUtility.calculate_time_series_stats(mode_values)
                    node_data['modes'][mode] = {
                        **stats,
                        'unit': 'percent'
                    }
                    all_mode_values.extend(mode_values)
                
                # Calculate total CPU usage across all modes
                total_stats = mcpToolsUtility.calculate_time_series_stats(all_mode_values)
                node_data['total'] = {
                    **total_stats,
                    'unit': 'percent'
                }
                
                nodes_result[instance] = node_data
                self.logger.info(f"Collected CPU usage for {instance}: {len(data['modes'])} modes")
            
            if not nodes_result:
                self.logger.warning("No CPU usage data collected for any nodes")
                return {
                    'status': 'partial',
                    'metric': 'node_cpu_usage',
                    'description': metric_config.get('description', 'CPU usage per node and mode'),
                    'nodes': {},
                    'warning': 'No data returned from Prometheus'
                }
            
            return {
                'status': 'success',
                'metric': 'node_cpu_usage',
                'description': metric_config.get('description', 'CPU usage per node and mode'),
                'nodes': nodes_result
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting node CPU usage: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}
    
    async def _collect_node_memory_used(self, prom: PrometheusBaseQuery,
                                       master_nodes: List[str],
                                       start: str, end: str,
                                       step: str = '15s',
                                       node_capacities: Dict[str, float] = None) -> Dict[str, Any]:
        """Collect node memory used for master nodes with capacity information"""
        try:
            metric_config = self.config.get_metric_by_name('node_memory_used')
            if not metric_config:
                self.logger.error("Metric 'node_memory_used' not found in config")
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            if node_capacities is None:
                node_capacities = {}
            
            node_pattern = mcpToolsUtility.get_node_pattern(master_nodes)
            query = f'(node_memory_MemTotal_bytes{{instance=~"{node_pattern}"}} - (node_memory_MemFree_bytes{{instance=~"{node_pattern}"}} + node_memory_Buffers_bytes{{instance=~"{node_pattern}"}} + node_memory_Cached_bytes{{instance=~"{node_pattern}"}}))'
            
            self.logger.debug(f"Querying memory used: {query}")
            
            result = await self._query_range_wrap(prom, query, start, end, step)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error', 'Query failed')}
            
            raw_results = result.get('data', {}).get('result', [])
            self.logger.info(f"Got {len(raw_results)} memory series")
            
            # Calculate per-node statistics
            nodes_result = {}
            for item in raw_results:
                instance = item.get('metric', {}).get('instance', 'unknown')
                values = item.get('values', [])
                
                # Extract numeric values using utility function
                numeric_values = mcpToolsUtility.extract_numeric_values(values)
                
                if numeric_values:
                    stats = mcpToolsUtility.calculate_time_series_stats(numeric_values)
                    # Convert bytes to GB using utility function
                    node_info = {
                        'avg': mcpToolsUtility.bytes_to_gb(stats['avg']),
                        'max': mcpToolsUtility.bytes_to_gb(stats['max']),
                        'unit': 'GB'
                    }
                    
                    # Add total_capacity if available
                    if instance in node_capacities:
                        node_info['total_capacity'] = node_capacities[instance]
                        self.logger.debug(f"Added capacity {node_capacities[instance]} GB for {instance}")
                    
                    nodes_result[instance] = node_info
                    self.logger.info(f"Collected memory for {instance}: {node_info['avg']} GB avg")
            
            return {
                'status': 'success',
                'metric': 'node_memory_used',
                'description': metric_config.get('description', 'Memory used per node'),
                'nodes': nodes_result
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting node memory used: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}
    
    async def _collect_node_memory_cache_buffer(self, prom: PrometheusBaseQuery,
                                               master_nodes: List[str],
                                               start: str, end: str,
                                               step: str = '15s',
                                               node_capacities: Dict[str, float] = None) -> Dict[str, Any]:
        """Collect node memory cache and buffer for master nodes with capacity information"""
        try:
            metric_config = self.config.get_metric_by_name('node_memory_cache_buffer')
            if not metric_config:
                self.logger.error("Metric 'node_memory_cache_buffer' not found in config")
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            if node_capacities is None:
                node_capacities = {}
            
            node_pattern = mcpToolsUtility.get_node_pattern(master_nodes)
            query = f'node_memory_Cached_bytes{{instance=~"{node_pattern}"}} + node_memory_Buffers_bytes{{instance=~"{node_pattern}"}}'
            
            self.logger.debug(f"Querying cache/buffer: {query}")
            
            result = await self._query_range_wrap(prom, query, start, end, step)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error', 'Query failed')}
            
            raw_results = result.get('data', {}).get('result', [])
            
            # Calculate per-node statistics
            nodes_result = {}
            for item in raw_results:
                instance = item.get('metric', {}).get('instance', 'unknown')
                values = item.get('values', [])
                
                # Extract numeric values using utility function
                numeric_values = mcpToolsUtility.extract_numeric_values(values)
                
                if numeric_values:
                    stats = mcpToolsUtility.calculate_time_series_stats(numeric_values)
                    node_info = {
                        'avg': mcpToolsUtility.bytes_to_gb(stats['avg']),
                        'max': mcpToolsUtility.bytes_to_gb(stats['max']),
                        'unit': 'GB'
                    }
                    
                    # Add total_capacity if available
                    if instance in node_capacities:
                        node_info['total_capacity'] = node_capacities[instance]
                        self.logger.debug(f"Added capacity {node_capacities[instance]} GB for {instance}")
                    
                    nodes_result[instance] = node_info
            
            return {
                'status': 'success',
                'metric': 'node_memory_cache_buffer',
                'description': metric_config.get('description', 'Memory cache and buffer per node'),
                'nodes': nodes_result
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting node memory cache/buffer: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}
    
    async def _collect_cgroup_cpu_usage(self, prom: PrometheusBaseQuery,
                                       master_nodes: List[str],
                                       start: str, end: str,
                                       step: str = '15s') -> Dict[str, Any]:
        """Collect cgroup CPU usage for master nodes"""
        try:
            metric_config = self.config.get_metric_by_name('cgroup_cpu_usage')
            if not metric_config:
                self.logger.error("Metric 'cgroup_cpu_usage' not found in config")
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            node_pattern = mcpToolsUtility.get_node_pattern(master_nodes)
            query = f'sum by (id, node) (rate(container_cpu_usage_seconds_total{{job=~".*", id=~"/system.slice|/system.slice/kubelet.service|/system.slice/ovs-vswitchd.service|/system.slice/crio.service|/system.slice/systemd-journald.service|/system.slice/ovsdb-server.service|/system.slice/systemd-udevd.service|/kubepods.slice", node=~"{node_pattern}"}}[5m])) * 100'
            
            self.logger.debug(f"Querying cgroup CPU: {query}")
            
            result = await self._query_range_wrap(prom, query, start, end, step)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error', 'Query failed')}
            
            raw_results = result.get('data', {}).get('result', [])
            self.logger.info(f"Got {len(raw_results)} cgroup CPU series")
            
            # Group by node to collect time series
            node_time_series = {}
            for item in raw_results:
                node = item.get('metric', {}).get('node', 'unknown')
                cgroup_id = item.get('metric', {}).get('id', 'unknown')
                values = item.get('values', [])
                
                if node not in node_time_series:
                    node_time_series[node] = {'cgroups': {}}
                
                # Extract numeric values using utility function
                cgroup_values = mcpToolsUtility.extract_numeric_values(values)
                
                if cgroup_values:
                    node_time_series[node]['cgroups'][cgroup_id] = cgroup_values
            
            # Calculate per-node statistics
            nodes_result = {}
            for node, data in node_time_series.items():
                node_data = {'cgroups': {}, 'unit': 'percent'}
                
                # Calculate stats for each cgroup using utility functions
                all_cgroup_values = []
                for cgroup_id, cgroup_values in data['cgroups'].items():
                    stats = mcpToolsUtility.calculate_time_series_stats(cgroup_values)
                    # Extract cgroup name using utility function
                    cgroup_name = mcpToolsUtility.extract_cgroup_name(cgroup_id)
                    node_data['cgroups'][cgroup_name] = {
                        **stats,
                        'unit': 'percent'
                    }
                    all_cgroup_values.extend(cgroup_values)
                
                # Calculate total across all cgroups
                total_stats = mcpToolsUtility.calculate_time_series_stats(all_cgroup_values)
                node_data['total'] = {
                    **total_stats,
                    'unit': 'percent'
                }
                
                nodes_result[node] = node_data
                self.logger.info(f"Collected cgroup CPU for {node}: {len(data['cgroups'])} cgroups")
            
            return {
                'status': 'success',
                'metric': 'cgroup_cpu_usage',
                'description': metric_config.get('description', 'Cgroup CPU usage per node'),
                'nodes': nodes_result
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting cgroup CPU usage: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}
    
    async def _collect_cgroup_rss_usage(self, prom: PrometheusBaseQuery,
                                       master_nodes: List[str],
                                       start: str, end: str,
                                       step: str = '15s') -> Dict[str, Any]:
        """Collect cgroup RSS memory usage for master nodes"""
        try:
            metric_config = self.config.get_metric_by_name('cgroup_rss_usage')
            if not metric_config:
                self.logger.error("Metric 'cgroup_rss_usage' not found in config")
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            node_pattern = mcpToolsUtility.get_node_pattern(master_nodes)
            query = f'sum by (id, node) (container_memory_rss{{job=~".*", id=~"/system.slice|/system.slice/kubelet.service|/system.slice/ovs-vswitchd.service|/system.slice/crio.service|/system.slice/systemd-journald.service|/system.slice/ovsdb-server.service|/system.slice/systemd-udevd.service|/kubepods.slice", node=~"{node_pattern}"}})'
            
            self.logger.debug(f"Querying cgroup RSS: {query}")
            
            result = await self._query_range_wrap(prom, query, start, end, step)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error', 'Query failed')}
            
            raw_results = result.get('data', {}).get('result', [])
            self.logger.info(f"Got {len(raw_results)} cgroup RSS series")
            
            # Group by node to collect time series
            node_time_series = {}
            for item in raw_results:
                node = item.get('metric', {}).get('node', 'unknown')
                cgroup_id = item.get('metric', {}).get('id', 'unknown')
                values = item.get('values', [])
                
                if node not in node_time_series:
                    node_time_series[node] = {'cgroups': {}}
                
                # Extract numeric values using utility function
                cgroup_values = mcpToolsUtility.extract_numeric_values(values)
                
                if cgroup_values:
                    node_time_series[node]['cgroups'][cgroup_id] = cgroup_values
            
            # Calculate per-node statistics
            nodes_result = {}
            for node, data in node_time_series.items():
                node_data = {'cgroups': {}, 'unit': 'GB'}
                
                # Calculate stats for each cgroup using utility functions
                all_cgroup_values = []
                for cgroup_id, cgroup_values in data['cgroups'].items():
                    stats = mcpToolsUtility.calculate_time_series_stats(cgroup_values)
                    # Extract cgroup name using utility function
                    cgroup_name = mcpToolsUtility.extract_cgroup_name(cgroup_id)
                    node_data['cgroups'][cgroup_name] = {
                        'avg': mcpToolsUtility.bytes_to_gb(stats['avg']),
                        'max': mcpToolsUtility.bytes_to_gb(stats['max']),
                        'unit': 'GB'
                    }
                    all_cgroup_values.extend(cgroup_values)
                
                # Calculate total across all cgroups
                total_stats = mcpToolsUtility.calculate_time_series_stats(all_cgroup_values)
                node_data['total'] = {
                    'avg': mcpToolsUtility.bytes_to_gb(total_stats['avg']),
                    'max': mcpToolsUtility.bytes_to_gb(total_stats['max']),
                    'unit': 'GB'
                }
                
                nodes_result[node] = node_data
                self.logger.info(f"Collected cgroup RSS for {node}: {len(data['cgroups'])} cgroups")
            
            return {
                'status': 'success',
                'metric': 'cgroup_rss_usage',
                'description': metric_config.get('description', 'Cgroup RSS usage per node'),
                'nodes': nodes_result
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting cgroup RSS usage: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}


async def main():
    """Main function for testing"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # This is a placeholder - you would need to initialize with actual auth
    print("Node Usage Collector module loaded successfully")
    print("Usage: collector = nodeUsageCollector(ocp_auth, prometheus_config)")
    print("       result = await collector.collect_all_metrics(duration='1h')")


if __name__ == "__main__":
    asyncio.run(main())