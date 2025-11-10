"""
Kubelet CNI Collector Module
Collects CNI and CRIO metrics for OpenShift nodes
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pytz
import os
import sys

# Ensure project root on sys.path for utils imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ocauth.openshift_auth import OpenShiftAuth as OCPAuth
from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from config.metrics_config_reader import Config


class KubeletCNICollector:
    """Collector for Kubelet CNI and CRIO metrics"""
    
    def __init__(self, ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None, metrics_file_path: str = None):
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.start_time = start_time
        self.end_time = end_time
        self.logger = logging.getLogger(__name__)
        self.utility = mcpToolsUtility(ocp_auth)
        
        # Load metrics configuration
        if metrics_file_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metrics_file_path = os.path.join(project_root, 'config', 'metrics-cni.yml')
        
        self.config = Config()
        load_result = self.config.load_metrics_file(metrics_file_path)
        
        # Get category-specific metrics
        self.category = "cni"
        self.cni_metrics = self.config.get_metrics_by_category(self.category)
        
        # Log metrics loading
        if load_result.get('success'):
            metrics_count = len(self.cni_metrics)
            self.logger.info(f"✅ Loaded {metrics_count} metrics from {self.category} category (KubeletCNICollector)")
        else:
            self.logger.warning(f"⚠️ No {self.category} metrics found in configuration")
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all CNI metrics and return comprehensive results with cluster summary"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                # Test connection first
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': "Prometheus connection failed",
                        'timestamp': datetime.now(pytz.UTC).isoformat()
                    }
                
                # Get node role mapping
                node_role_mapping = await self.utility.get_node_role_mapping(prom)
                
                # Collect results for all metrics
                results = {
                    'status': 'success',
                    'timestamp': datetime.now(pytz.UTC).isoformat(),
                    'duration': self.duration,
                    'start_time': self.start_time,
                    'end_time': self.end_time,
                    'category': 'cni',
                    'metrics': {}
                }
                
                # Process each metric dynamically from config
                for metric_config in self.cni_metrics:
                    metric_name = metric_config['name']
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        metric_result = await self._collect_generic_metric(prom, metric_config, node_role_mapping)
                        results['metrics'][metric_name] = metric_result
                        
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        results['metrics'][metric_name] = {
                            'status': 'error',
                            'error': str(e)
                        }
                
                # Calculate summary statistics
                successful_metrics = sum(1 for m in results['metrics'].values() if m.get('status') == 'success')
                total_metrics = len(results['metrics'])
                
                # Generate cluster summary
                cluster_summary = self._generate_cluster_summary(results['metrics'], node_role_mapping)
                
                # Add combined summary
                results['summary'] = {
                    'total_metrics': total_metrics,
                    'successful_metrics': successful_metrics,
                    'failed_metrics': total_metrics - successful_metrics,
                    'cluster_health': cluster_summary['cluster_health'],
                    'performance_indicators': cluster_summary['performance_indicators'],
                    'recommendations': cluster_summary['recommendations']
                }
                
                return results
                
        except Exception as e:
            self.logger.error(f"Error in collect_all_metrics: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(pytz.UTC).isoformat()
            }
    
    def _generate_cluster_summary(self, metrics_data: Dict[str, Any], node_role_mapping: Dict[str, str]) -> Dict[str, Any]:
        """Generate cluster summary from collected metrics data"""
        summary = {
            'cluster_health': {
                'total_nodes': 0,
                'nodes_by_role': {
                    'controlplane': 0,
                    'worker': 0,
                    'infra': 0,
                    'workload': 0
                },
                'overall_health': 'unknown'
            },
            'performance_indicators': {},
            'recommendations': []
        }
        
        # Count unique nodes by role from node_role_mapping
        role_counts = {
            'controlplane': 0,
            'worker': 0,
            'infra': 0,
            'workload': 0
        }
        for node, role in node_role_mapping.items():
            if '.' in node:  # Only count full node names to avoid duplicates
                if role in role_counts:
                    role_counts[role] += 1
        
        summary['cluster_health']['nodes_by_role'] = role_counts
        summary['cluster_health']['total_nodes'] = sum(role_counts.values())
        
        # Analyze CPU usage
        cni_cpu = metrics_data.get('cni_cpu_usage', {})
        if cni_cpu.get('status') == 'success':
            all_nodes = {}
            for role, nodes in cni_cpu.get('node_metrics_by_role', {}).items():
                all_nodes.update(nodes)
            
            if all_nodes:
                max_cpu = max([n.get('max_percent', 0) for n in all_nodes.values()], default=0)
                avg_cpu = sum([n.get('avg_percent', 0) for n in all_nodes.values()]) / len(all_nodes)
                
                summary['performance_indicators']['cni_cpu_max_percent'] = round(max_cpu, 2)
                summary['performance_indicators']['cni_cpu_avg_percent'] = round(avg_cpu, 2)
                
                if max_cpu > 80:
                    summary['recommendations'].append("High CNI CPU usage detected (>80%). Consider investigating kubelet performance.")
        
        # Analyze CRIO CPU usage
        crio_cpu = metrics_data.get('crio_cpu_usage', {})
        if crio_cpu.get('status') == 'success':
            all_nodes = {}
            for role, nodes in crio_cpu.get('node_metrics_by_role', {}).items():
                all_nodes.update(nodes)
            
            if all_nodes:
                max_cpu = max([n.get('max_percent', 0) for n in all_nodes.values()], default=0)
                avg_cpu = sum([n.get('avg_percent', 0) for n in all_nodes.values()]) / len(all_nodes)
                
                summary['performance_indicators']['crio_cpu_max_percent'] = round(max_cpu, 2)
                summary['performance_indicators']['crio_cpu_avg_percent'] = round(avg_cpu, 2)
                
                if max_cpu > 80:
                    summary['recommendations'].append("High CRIO CPU usage detected (>80%). Consider investigating container runtime performance.")
        
        # Analyze memory usage
        cni_memory = metrics_data.get('cni_memory_usage', {})
        if cni_memory.get('status') == 'success':
            all_nodes = {}
            for role, nodes in cni_memory.get('node_metrics_by_role', {}).items():
                all_nodes.update(nodes)
            
            if all_nodes:
                max_mem_mb = max([n.get('max_mb', 0) for n in all_nodes.values()], default=0)
                avg_mem_mb = sum([n.get('avg_mb', 0) for n in all_nodes.values()]) / len(all_nodes)
                
                summary['performance_indicators']['cni_memory_max_mb'] = round(max_mem_mb, 2)
                summary['performance_indicators']['cni_memory_avg_mb'] = round(avg_mem_mb, 2)
        
        # Analyze network drops
        cni_drops = metrics_data.get('cni_network_drop', {})
        if cni_drops.get('status') == 'success':
            all_nodes = {}
            for role, nodes in cni_drops.get('node_metrics_by_role', {}).items():
                all_nodes.update(nodes)
            
            if all_nodes:
                total_drops = sum([n.get('avg_packets', 0) for n in all_nodes.values()])
                max_drops = max([n.get('max_packets', 0) for n in all_nodes.values()], default=0)
                
                summary['performance_indicators']['network_drops_avg_total'] = round(total_drops, 2)
                summary['performance_indicators']['network_drops_max'] = round(max_drops, 2)
                
                if total_drops > 100:
                    summary['recommendations'].append("Network packet drops detected. Check network configuration and hardware.")
        
        # Analyze network errors
        cni_errors = metrics_data.get('cni_network_error', {})
        if cni_errors.get('status') == 'success':
            all_nodes = {}
            for role, nodes in cni_errors.get('node_metrics_by_role', {}).items():
                all_nodes.update(nodes)
            
            if all_nodes:
                total_errors = sum([n.get('avg_packets', 0) for n in all_nodes.values()])
                max_errors = max([n.get('max_packets', 0) for n in all_nodes.values()], default=0)
                
                summary['performance_indicators']['network_errors_avg_total'] = round(total_errors, 2)
                summary['performance_indicators']['network_errors_max'] = round(max_errors, 2)
                
                if total_errors > 50:
                    summary['recommendations'].append("Network errors detected. Investigate network infrastructure.")
        
        # Analyze thread counts
        container_threads = metrics_data.get('container_threads', {})
        if container_threads.get('status') == 'success':
            all_nodes = {}
            for role, nodes in container_threads.get('node_metrics_by_role', {}).items():
                all_nodes.update(nodes)
            
            if all_nodes:
                avg_threads = sum([n.get('avg_threads', 0) for n in all_nodes.values()]) / len(all_nodes)
                max_threads = max([n.get('max_threads', 0) for n in all_nodes.values()], default=0)
                
                summary['performance_indicators']['container_threads_avg'] = round(avg_threads, 0)
                summary['performance_indicators']['container_threads_max'] = round(max_threads, 0)
        
        # Analyze IOPS
        worker_read_iops = metrics_data.get('worker_io_read_iops', {})
        if worker_read_iops.get('status') == 'success':
            all_nodes = {}
            for role, nodes in worker_read_iops.get('node_metrics_by_role', {}).items():
                all_nodes.update(nodes)
            
            if all_nodes:
                avg_iops = sum([n.get('avg_iops', 0) for n in all_nodes.values()]) / len(all_nodes)
                max_iops = max([n.get('max_iops', 0) for n in all_nodes.values()], default=0)
                
                summary['performance_indicators']['worker_read_iops_avg'] = round(avg_iops, 2)
                summary['performance_indicators']['worker_read_iops_max'] = round(max_iops, 2)
        
        worker_write_iops = metrics_data.get('worker_io_write_iops', {})
        if worker_write_iops.get('status') == 'success':
            all_nodes = {}
            for role, nodes in worker_write_iops.get('node_metrics_by_role', {}).items():
                all_nodes.update(nodes)
            
            if all_nodes:
                avg_iops = sum([n.get('avg_iops', 0) for n in all_nodes.values()]) / len(all_nodes)
                max_iops = max([n.get('max_iops', 0) for n in all_nodes.values()], default=0)
                
                summary['performance_indicators']['worker_write_iops_avg'] = round(avg_iops, 2)
                summary['performance_indicators']['worker_write_iops_max'] = round(max_iops, 2)
        
        # Determine overall health
        if not summary['recommendations']:
            summary['cluster_health']['overall_health'] = 'healthy'
        elif len(summary['recommendations']) <= 2:
            summary['cluster_health']['overall_health'] = 'warning'
        else:
            summary['cluster_health']['overall_health'] = 'critical'
        
        return summary
    
    async def _collect_generic_metric(self, prom: PrometheusBaseQuery, metric_config: Dict[str, Any], node_role_mapping: Dict[str, str]) -> Dict[str, Any]:
        """Generic method to collect any CNI metric and group by node role"""
        query = metric_config['expr']
        metric_name = metric_config['name']
        unit = metric_config.get('unit', 'unknown')
        
        try:
            result = await self._query_with_stats(prom, query)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            # Process results by node and group by role
            role_groups = {
                'controlplane': {},
                'worker': {},
                'infra': {},
                'workload': {}
            }
            
            for series in result.get('series_data', []):
                # Try to get node name from various label fields
                node_name = series['labels'].get('node')
                
                # Fallback to instance label if node is not present
                if not node_name or node_name == 'unknown':
                    instance = series['labels'].get('instance', '')
                    if instance:
                        # Try to resolve instance to node name using utility
                        try:
                            resolved_node = await self.utility.resolve_node_from_instance(instance, prom)
                            if resolved_node and resolved_node != 'unknown':
                                node_name = resolved_node
                            else:
                                # Fallback: remove port and try matching
                                base_instance = instance.split(':')[0]
                                # Try to find matching node in node_role_mapping
                                for full_node_name in node_role_mapping.keys():
                                    if base_instance in full_node_name or full_node_name.startswith(base_instance):
                                        node_name = full_node_name
                                        break
                        except Exception as e:
                            self.logger.debug(f"Could not resolve instance {instance} to node: {e}")
                            # Fallback: use instance without port
                            node_name = instance.split(':')[0]
                
                if node_name and node_name != 'unknown':
                    stats = series['statistics']
                    
                    # Determine node role
                    node_role = node_role_mapping.get(node_name, 'worker')
                    
                    # Format stats based on unit
                    node_stats = self._format_stats_by_unit(stats, unit)
                    
                    # Add to appropriate role group
                    if node_role in role_groups:
                        role_groups[node_role][node_name] = node_stats
            
            return {
                'status': 'success',
                'metric': metric_name,
                'unit': unit,
                'description': metric_config.get('description', f'CNI metric: {metric_name}'),
                'node_metrics_by_role': role_groups,
                'total_nodes': sum(len(nodes) for nodes in role_groups.values())
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting metric {metric_name}: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def _format_stats_by_unit(self, stats: Dict[str, Any], unit: str) -> Dict[str, Any]:
        """Format statistics based on the metric unit"""
        if unit == 'percent':
            return {
                'avg_percent': round(stats.get('avg', 0), 2),
                'max_percent': round(stats.get('max', 0), 2),
                'min_percent': round(stats.get('min', 0), 2),
                'latest_percent': round(stats.get('latest', 0), 2) if stats.get('latest') is not None else None,
                'data_points': stats.get('count', 0)
            }
        elif unit == 'bytes':
            return {
                'avg_bytes': round(stats.get('avg', 0), 2),
                'max_bytes': round(stats.get('max', 0), 2),
                'min_bytes': round(stats.get('min', 0), 2),
                'latest_bytes': round(stats.get('latest', 0), 2) if stats.get('latest') is not None else None,
                'avg_mb': round(stats.get('avg', 0) / (1024 * 1024), 2),
                'max_mb': round(stats.get('max', 0) / (1024 * 1024), 2),
                'data_points': stats.get('count', 0)
            }
        elif unit == 'packets':
            return {
                'avg_packets': round(stats.get('avg', 0), 2),
                'max_packets': round(stats.get('max', 0), 2),
                'min_packets': round(stats.get('min', 0), 2),
                'latest_packets': round(stats.get('latest', 0), 2) if stats.get('latest') is not None else None,
                'data_points': stats.get('count', 0)
            }
        elif unit == 'threads':
            return {
                'avg_threads': round(stats.get('avg', 0), 0),
                'max_threads': round(stats.get('max', 0), 0),
                'min_threads': round(stats.get('min', 0), 0),
                'latest_threads': round(stats.get('latest', 0), 0) if stats.get('latest') is not None else None,
                'data_points': stats.get('count', 0)
            }
        elif unit == 'iops':
            return {
                'avg_iops': round(stats.get('avg', 0), 2),
                'max_iops': round(stats.get('max', 0), 2),
                'min_iops': round(stats.get('min', 0), 2),
                'latest_iops': round(stats.get('latest', 0), 2) if stats.get('latest') is not None else None,
                'data_points': stats.get('count', 0)
            }
        else:
            # Default format
            return {
                'avg_value': round(stats.get('avg', 0), 2),
                'max_value': round(stats.get('max', 0), 2),
                'min_value': round(stats.get('min', 0), 2),
                'latest_value': round(stats.get('latest', 0), 2) if stats.get('latest') is not None else None,
                'data_points': stats.get('count', 0)
            }
    
    async def _query_with_stats(self, prom_client: PrometheusBaseQuery, query: str) -> Dict[str, Any]:
        """Execute a range query and compute statistics per series and overall"""
        try:
            # Determine time range
            if self.start_time and self.end_time:
                start = self.start_time
                end = self.end_time
            else:
                start, end = prom_client.get_time_range_from_duration(self.duration)
            
            data = await prom_client.query_range(query, start, end, step='15s')

            series_data: List[Dict[str, Any]] = []
            all_values: List[float] = []

            for item in data.get('result', []) if isinstance(data, dict) else []:
                metric_labels = item.get('metric', {})
                values = []
                for ts, val in item.get('values', []) or []:
                    try:
                        v = float(val)
                    except (ValueError, TypeError):
                        continue
                    if v != float('inf') and v != float('-inf'):
                        values.append(v)

                stats: Dict[str, Any] = {}
                if values:
                    avg_v = sum(values) / len(values)
                    max_v = max(values)
                    min_v = min(values)
                    stats = {
                        'avg': avg_v,
                        'max': max_v,
                        'min': min_v,
                        'count': len(values),
                        'latest': values[-1]
                    }
                    all_values.extend(values)

                series_data.append({'labels': metric_labels, 'statistics': stats})

            overall_statistics: Dict[str, Any] = {}
            if all_values:
                overall_statistics = {
                    'avg': sum(all_values) / len(all_values),
                    'max': max(all_values),
                    'min': min(all_values),
                    'count': len(all_values)
                }

            return {
                'status': 'success',
                'series_data': series_data,
                'overall_statistics': overall_statistics
            }
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    # Individual metric collection methods
    
    async def collect_cni_cpu_usage(self) -> Dict[str, Any]:
        """Collect CNI CPU usage metric"""
        metric_config = self.config.get_metric_by_name('cni_cpu_usage')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_crio_cpu_usage(self) -> Dict[str, Any]:
        """Collect CRIO CPU usage metric"""
        metric_config = self.config.get_metric_by_name('crio_cpu_usage')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_cni_memory_usage(self) -> Dict[str, Any]:
        """Collect CNI memory usage metric"""
        metric_config = self.config.get_metric_by_name('cni_memory_usage')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_crio_memory_usage(self) -> Dict[str, Any]:
        """Collect CRIO memory usage metric"""
        metric_config = self.config.get_metric_by_name('crio_memory_usage')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_cni_network_usage(self) -> Dict[str, Any]:
        """Collect CNI network usage metric"""
        metric_config = self.config.get_metric_by_name('cni_network_usage')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_crio_network_usage(self) -> Dict[str, Any]:
        """Collect CRIO network usage metric"""
        metric_config = self.config.get_metric_by_name('crio_network_usage')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_cni_network_drop(self) -> Dict[str, Any]:
        """Collect CNI network drop metric"""
        metric_config = self.config.get_metric_by_name('cni_network_drop')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_crio_network_drop(self) -> Dict[str, Any]:
        """Collect CRIO network drop metric"""
        metric_config = self.config.get_metric_by_name('crio_network_drop')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_cni_network_error(self) -> Dict[str, Any]:
        """Collect CNI network error metric"""
        metric_config = self.config.get_metric_by_name('cni_network_error')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_crio_network_error(self) -> Dict[str, Any]:
        """Collect CRIO network error metric"""
        metric_config = self.config.get_metric_by_name('crio_network_error')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_cni_network_utilization(self) -> Dict[str, Any]:
        """Collect CNI network utilization metric"""
        metric_config = self.config.get_metric_by_name('cni_network_utilization')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_crio_network_utilization(self) -> Dict[str, Any]:
        """Collect CRIO network utilization metric"""
        metric_config = self.config.get_metric_by_name('crio_network_utilization')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_container_threads(self) -> Dict[str, Any]:
        """Collect container threads metric"""
        metric_config = self.config.get_metric_by_name('container_threads')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_control_plane_threads(self) -> Dict[str, Any]:
        """Collect control plane threads metric"""
        metric_config = self.config.get_metric_by_name('control_plane_threads')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_worker_io_read_iops(self) -> Dict[str, Any]:
        """Collect worker I/O read IOPS metric"""
        metric_config = self.config.get_metric_by_name('worker_io_read_iops')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_worker_io_write_iops(self) -> Dict[str, Any]:
        """Collect worker I/O write IOPS metric"""
        metric_config = self.config.get_metric_by_name('worker_io_write_iops')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_control_plane_io_read_iops(self) -> Dict[str, Any]:
        """Collect control plane I/O read IOPS metric"""
        metric_config = self.config.get_metric_by_name('control_plane_io_read_iops')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def collect_control_plane_io_write_iops(self) -> Dict[str, Any]:
        """Collect control plane I/O write IOPS metric"""
        metric_config = self.config.get_metric_by_name('control_plane_io_write_iops')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            node_role_mapping = await self.utility.get_node_role_mapping(prom)
            return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
    
    async def get_cluster_summary(self) -> Dict[str, Any]:
        """Get cluster-wide CNI performance summary (calls collect_all_metrics)"""
        return await self.collect_all_metrics()
    
    async def get_metric_by_name(self, metric_name: str) -> Dict[str, Any]:
        """Get a specific metric by name"""
        try:
            metric_config = self.config.get_metric_by_name(metric_name)
            if not metric_config:
                return {
                    'status': 'error',
                    'error': f'Metric {metric_name} not found in configuration'
                }
            
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                # Test connection first
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': "Prometheus connection failed"
                    }
                
                node_role_mapping = await self.utility.get_node_role_mapping(prom)
                return await self._collect_generic_metric(prom, metric_config, node_role_mapping)
                
        except Exception as e:
            self.logger.error(f"Error getting metric {metric_name}: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }


# Convenience functions for CNI metric collection

async def collect_cni_metrics(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Convenience function to collect all CNI metrics"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_all_metrics()


async def get_cni_cluster_summary(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Convenience function to get CNI cluster summary"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.get_cluster_summary()


async def get_specific_cni_metric(ocp_auth: OCPAuth, metric_name: str, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Convenience function to get a specific CNI metric"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.get_metric_by_name(metric_name)


# Individual metric convenience functions

async def get_cni_cpu_usage(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get CNI CPU usage"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_cni_cpu_usage()


async def get_crio_cpu_usage(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get CRIO CPU usage"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_crio_cpu_usage()


async def get_cni_memory_usage(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get CNI memory usage"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_cni_memory_usage()


async def get_crio_memory_usage(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get CRIO memory usage"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_crio_memory_usage()


async def get_cni_network_usage(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get CNI network usage"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_cni_network_usage()


async def get_crio_network_usage(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get CRIO network usage"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_crio_network_usage()


async def get_cni_network_drop(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get CNI network drops"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_cni_network_drop()


async def get_crio_network_drop(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get CRIO network drops"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_crio_network_drop()


async def get_cni_network_error(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get CNI network errors"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_cni_network_error()


async def get_crio_network_error(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get CRIO network errors"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_crio_network_error()


async def get_cni_network_utilization(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get CNI network utilization"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_cni_network_utilization()


async def get_crio_network_utilization(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get CRIO network utilization"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_crio_network_utilization()


async def get_container_threads(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get container threads count"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_container_threads()


async def get_control_plane_threads(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get control plane threads count"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_control_plane_threads()


async def get_worker_io_read_iops(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get worker I/O read IOPS"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_worker_io_read_iops()


async def get_worker_io_write_iops(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get worker I/O write IOPS"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_worker_io_write_iops()


async def get_control_plane_io_read_iops(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get control plane I/O read IOPS"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_control_plane_io_read_iops()


async def get_control_plane_io_write_iops(ocp_auth: OCPAuth, duration: str = "1h", start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get control plane I/O write IOPS"""
    collector = KubeletCNICollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_control_plane_io_write_iops()