"""
etcd Disk I/O Metrics Collector Module
Collects and processes disk I/O performance metrics for etcd monitoring
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import pytz
import os
import sys

# Ensure project root on sys.path for utils imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from config.metrics_config_reader import Config


class DiskIOCollector:
    """Collector for etcd disk I/O performance metrics"""
    
    def __init__(self, ocp_auth, duration: str = "1h", metrics_file_path: Optional[str] = None):
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.logger = logging.getLogger(__name__)
        
        # Determine which metrics file to load
        if metrics_file_path:
            # Use the provided metrics file path
            self.logger.info(f"Loading metrics from provided path: {metrics_file_path}")
            self.config = Config()
            load_result = self.config.load_metrics_file(metrics_file_path, category_filter=['disk_io'])
            
            if not load_result.get('success'):
                self.logger.error(f"Failed to load metrics file: {load_result.get('error')}")
                raise ValueError(f"Failed to load metrics file: {load_result.get('error')}")
            
            self.logger.info(f"✅ Loaded {load_result.get('metrics_loaded', 0)} disk_io metrics from {load_result.get('file_name')}")
        else:
            # Load from default location (metrics-disk.yml)
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            default_metrics_path = os.path.join(project_root, 'config', 'metrics-disk.yml')
            
            self.logger.info(f"Loading metrics from default path: {default_metrics_path}")
            self.config = Config()
            load_result = self.config.load_metrics_file(default_metrics_path, category_filter=['disk_io'])
            
            if not load_result.get('success'):
                self.logger.error(f"Failed to load default metrics file: {load_result.get('error')}")
                raise ValueError(f"Failed to load default metrics file: {load_result.get('error')}")
            
            self.logger.info(f"✅ Loaded {load_result.get('metrics_loaded', 0)} disk_io metrics from {load_result.get('file_name')}")
        
        self.utility = mcpToolsUtility(ocp_auth)
        self.timezone = pytz.UTC
        
        # Get disk I/O metrics from configuration
        self.metrics = self.config.get_metrics_by_category('disk_io')
        if not self.metrics:
            self.logger.warning("No disk_io metrics found in configuration")
            self.metrics = []
        
        self.logger.info(f"Initialized DiskIOCollector with {len(self.metrics)} metrics")
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all disk I/O metrics"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()

            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom_client:
                # Test connection
                connection_ok = await prom_client.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': "Prometheus connection failed",
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                # Collect all metrics
                # Map config metric names to collector method names
                name_normalization = {
                    'disk_io_container_disk_writes': 'container_disk_writes',
                    'disk_io_node_disk_throughput_read': 'node_disk_throughput_read',
                    'disk_io_node_disk_throughput_write': 'node_disk_throughput_write',
                    'disk_io_node_disk_iops_read': 'node_disk_iops_read',
                    'disk_io_node_disk_iops_write': 'node_disk_iops_write',
                    'disk_io_node_disk_read_time_seconds': 'node_disk_read_time_seconds',
                    'disk_io_node_disk_writes_time_seconds': 'node_disk_writes_time_seconds',
                    'disk_io_node_disk_io_time_seconds': 'node_disk_io_time_seconds',
                }

                results = {}
                for metric_config in self.metrics:
                    metric_name = metric_config['name']
                    normalized_name = name_normalization.get(metric_name, metric_name)
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        if normalized_name == 'container_disk_writes':
                            results[metric_name] = await self.collect_container_disk_writes(prom_client)
                        elif normalized_name == 'node_disk_throughput_read':
                            results[metric_name] = await self.collect_node_disk_metric(prom_client, metric_name)
                        elif normalized_name == 'node_disk_throughput_write':
                            results[metric_name] = await self.collect_node_disk_metric(prom_client, metric_name)
                        elif normalized_name == 'node_disk_iops_read':
                            results[metric_name] = await self.collect_node_disk_metric(prom_client, metric_name)
                        elif normalized_name == 'node_disk_iops_write':
                            results[metric_name] = await self.collect_node_disk_metric(prom_client, metric_name)
                        elif normalized_name == 'node_disk_read_time_seconds':
                            results[metric_name] = await self.collect_node_disk_metric(prom_client, metric_name)
                        elif normalized_name == 'node_disk_writes_time_seconds':
                            results[metric_name] = await self.collect_node_disk_metric(prom_client, metric_name)
                        elif normalized_name == 'node_disk_io_time_seconds':
                            results[metric_name] = await self.collect_node_disk_metric(prom_client, metric_name)
                        else:
                            self.logger.warning(f"Unknown metric: {metric_name}")
                            results[metric_name] = {
                                'status': 'error',
                                'error': f'Unknown metric: {metric_name}'
                            }
                            
                    except Exception as e:
                        self.logger.error(f"Error collecting {metric_name}: {e}")
                        results[metric_name] = {
                            'status': 'error',
                            'error': str(e)
                        }
                
                return {
                    'status': 'success',
                    'timestamp': datetime.now(self.timezone).isoformat(),
                    'duration': self.duration,
                    'metrics': results,
                    'total_metrics': len(results)
                }
                
        except Exception as e:
            self.logger.error(f"Error in collect_all_metrics: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def collect_container_disk_writes(self, prom_client: PrometheusBaseQuery) -> Dict[str, Any]:
        """Collect etcd container disk write metrics"""
        try:
            metric_config = self.config.get_metric_by_name('disk_io_container_disk_writes')
            if not metric_config:
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            query = metric_config['expr']
            self.logger.debug(f"Executing query: {query}")
            
            result = await self._query_with_stats(prom_client, query, self.duration)
            
            if result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': result.get('error', 'Query failed'),
                    'query': query
                }
            
            # Process results by master node
            master_nodes = await self._get_master_nodes(prom_client)
            node_results = {}
            
            for series in result.get('series_data', []):
                labels = series.get('labels', {})
                pod_name = labels.get('pod', '')
                
                # Map pod to node
                node_name = 'unknown'
                if pod_name:
                    node_name = await self._get_node_for_pod(pod_name, 'openshift-etcd')
                
                if node_name in master_nodes or node_name == 'unknown':
                    stats = series.get('statistics', {})
                    
                    if node_name not in node_results:
                        node_results[node_name] = {
                            'avg': 0,
                            'max': 0,
                            'series_count': 0
                        }
                    
                    if stats.get('avg') is not None:
                        node_results[node_name]['avg'] += stats['avg']
                        node_results[node_name]['series_count'] += 1
                    
                    if stats.get('max') is not None:
                        node_results[node_name]['max'] = max(
                            node_results[node_name]['max'], 
                            stats['max']
                        )
            
            # Calculate final averages
            for node_name in node_results:
                if node_results[node_name]['series_count'] > 0:
                    node_results[node_name]['avg'] = (
                        node_results[node_name]['avg'] / 
                        node_results[node_name]['series_count']
                    )
                del node_results[node_name]['series_count']
            
            return {
                'status': 'success',
                'title': metric_config.get('title', 'Container Disk Writes'),
                'unit': metric_config.get('unit', 'bytes_per_second'),
                'overall_stats': result.get('overall_statistics', {}),
                'nodes': node_results,
                'query': query
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting container_disk_writes: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def collect_node_disk_metric(self, prom_client: PrometheusBaseQuery, metric_name: str) -> Dict[str, Any]:
        """Generic collector for node disk metrics (throughput, IOPS, timing)"""
        try:
            metric_config = self.config.get_metric_by_name(metric_name)
            if not metric_config:
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            query = metric_config['expr']
            self.logger.debug(f"Executing query: {query}")
            
            result = await self._query_with_stats(prom_client, query, self.duration)
            
            if result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': result.get('error', 'Query failed'),
                    'query': query
                }
            
            # Process results by master node
            master_nodes = await self._get_master_nodes(prom_client)
            node_results = {}
            
            for series in result.get('series_data', []):
                labels = series.get('labels', {})
                instance = labels.get('instance', '')
                
                # Resolve instance to node name
                node_name = await self.utility.resolve_node_from_instance(instance)
                
                if node_name in master_nodes or node_name == 'unknown':
                    stats = series.get('statistics', {})
                    device = labels.get('device', 'unknown')
                    
                    if node_name not in node_results:
                        node_results[node_name] = {
                            'avg': 0,
                            'max': 0,
                            'devices': [],
                            'series_count': 0
                        }
                    
                    if device not in node_results[node_name]['devices']:
                        node_results[node_name]['devices'].append(device)
                    
                    if stats.get('avg') is not None:
                        node_results[node_name]['avg'] += stats['avg']
                        node_results[node_name]['series_count'] += 1
                    
                    if stats.get('max') is not None:
                        node_results[node_name]['max'] = max(
                            node_results[node_name]['max'],
                            stats['max']
                        )
            
            # Calculate final averages
            for node_name in node_results:
                if node_results[node_name]['series_count'] > 0:
                    node_results[node_name]['avg'] = (
                        node_results[node_name]['avg'] /
                        node_results[node_name]['series_count']
                    )
                del node_results[node_name]['series_count']
            
            return {
                'status': 'success',
                'title': metric_config.get('title', metric_name),
                'unit': metric_config.get('unit', ''),
                'overall_stats': result.get('overall_statistics', {}),
                'nodes': node_results,
                'query': query
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting {metric_name}: {e}")
            return {'status': 'error', 'error': str(e)}

    # Helper methods
    async def _query_with_stats(self, prom_client: PrometheusBaseQuery, query: str, duration: str) -> Dict[str, Any]:
        """Execute a range query and compute basic statistics per series and overall."""
        try:
            start, end = prom_client.get_time_range_from_duration(duration)
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
            self.logger.error(f"Error in _query_with_stats: {e}")
            return {'status': 'error', 'error': str(e)}

    async def _get_master_nodes(self, prom_client: PrometheusBaseQuery) -> set:
        """Return a set of controlplane node names."""
        groups = await self.utility.get_node_groups(prometheus_client=prom_client)
        controlplane = groups.get('controlplane', []) if isinstance(groups, dict) else []
        return {n.get('name', '').split(':')[0] for n in controlplane}

    async def _get_node_for_pod(self, pod_name: str, namespace: str) -> str:
        """Map pod to node using oc; results cached per namespace."""
        if not hasattr(self, '_pod_node_cache'):
            self._pod_node_cache = {}
        if namespace not in self._pod_node_cache:
            self._pod_node_cache[namespace] = self.utility.get_pod_to_node_mapping_via_oc(namespace=namespace)
        return self._pod_node_cache[namespace].get(pod_name, 'unknown')


# Convenience function for external usage
async def collect_disk_io_metrics(ocp_auth, duration: str = "1h") -> Dict[str, Any]:
    """Collect all disk I/O metrics"""
    collector = DiskIOCollector(ocp_auth, duration)
    return await collector.collect_all_metrics()