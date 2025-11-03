"""
etcd Network I/O Metrics Collector
Collects and analyzes network I/O metrics for etcd monitoring
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

from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from config.metrics_config_reader import Config


class NetworkIOCollector:
    """Network I/O metrics collector for etcd monitoring"""
    
    def __init__(self, ocp_auth, metrics_file_path: str = None):
        self.ocp_auth = ocp_auth
        self.prometheus_config = self.ocp_auth.get_prometheus_config()
        self.logger = logging.getLogger(__name__)
        # Cache for node mappings
        self._node_mappings = {}
        self._cache_valid = False
        self.utility = mcpToolsUtility(ocp_auth)
        
        # Load metrics configuration
        if metrics_file_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metrics_file_path = os.path.join(project_root, 'config', 'metrics-etcd.yml')
        
        self.config = Config()
        load_result = self.config.load_metrics_file(metrics_file_path)
        
        # Get category-specific metrics
        self.category = "network_io"
        network_metrics = self.config.get_metrics_by_category(self.category)
        
        # Log metrics loading
        if load_result.get('success'):
            metrics_count = len(network_metrics)
            self.logger.info(f"✅ Loaded {metrics_count} metrics from {self.category} category (NetworkIOCollector)")
        else:
            self.logger.warning(f"⚠️  No {self.category} metrics found in configuration")
                
    async def collect_metrics(self, duration: str = "1h") -> Dict[str, Any]:
        """Main entrypoint for collecting all network I/O metrics using names/titles from metrics-etcd.yml"""
        try:
            self.logger.info(f"Starting network I/O metrics collection for duration: {duration}")
            
            # Initialize mappings
            await self._update_node_mappings()
            
            # Get network_io metrics from config
            network_metrics = self.config.get_metrics_by_category('network_io')
            if not network_metrics:
                return {
                    'status': 'error',
                    'error': 'No network_io metrics found in configuration',
                    'category': 'network_io',
                    'timestamp': datetime.now(pytz.UTC).isoformat(),
                    'duration': duration
                }

            # Partition metrics by level based on standardized names and description
            pod_metric_names = set()
            node_metric_names = set()
            cluster_metric_names = set()

            for m in network_metrics:
                name = m.get('name', '')
                description = m.get('description', '').lower()
                
                # Check if metric has pod-level labels or description indicates pod-level
                if (name.startswith('network_io_container_') or 
                    name.startswith('network_io_network_peer_') or 
                    name.startswith('network_io_network_client_') or 
                    name.startswith('network_io_etcd_in_') or
                    name.startswith('network_io_etcd_out_') or
                    name == 'network_io_peer2peer_rtt_latency_p99' or
                    name == 'etcd_network_active_peers' or
                    'pod' in description or 'peer' in description):
                    pod_metric_names.add(name)
                elif name.startswith('network_io_node_network_'):
                    node_metric_names.add(name)
                elif name.startswith('network_io_grpc_active_'):
                    cluster_metric_names.add(name)
                else:
                    # Fallback: place unknown ones into cluster level
                    cluster_metric_names.add(name)

            # Build dictionaries {name: {expr, unit, title, description}}
            def to_dict(names_set):
                d = {}
                for m in network_metrics:
                    if m.get('name') in names_set:
                        d[m['name']] = {
                            'expr': m.get('expr', ''),
                            'unit': m.get('unit', 'unknown'),
                            'title': m.get('title', m.get('name')),
                            'description': m.get('description', '')
                        }
                return d

            pod_metrics = to_dict(pod_metric_names)
            node_metrics = to_dict(node_metric_names)
            cluster_metrics = to_dict(cluster_metric_names)
            
            results = {
                'status': 'success',
                'timestamp': datetime.now(pytz.UTC).isoformat(),
                'duration': duration,
                'data': {
                    'pods_metrics': {},
                    'node_metrics': {},
                    'cluster_metrics': {}
                },
                'category': 'network_io'
            }
            
            async with PrometheusBaseQuery(self.prometheus_config.get('url'), self.prometheus_config.get('token')) as prom:
                # Test connection
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': 'Prometheus connection failed',
                        'category': 'network_io',
                        'timestamp': datetime.now(pytz.UTC).isoformat(),
                        'duration': duration
                    }
                
                # Collect pod metrics
                pods_result = await self._collect_pod_metrics(prom, duration, pod_metrics)
                results['data']['pods_metrics'] = pods_result
                
                # Collect node metrics
                node_result = await self._collect_node_metrics(prom, duration, node_metrics)
                results['data']['node_metrics'] = node_result
                
                # Collect cluster metrics
                cluster_result = await self._collect_cluster_metrics(prom, duration, cluster_metrics)
                results['data']['cluster_metrics'] = cluster_result
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in network I/O metrics collection: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(pytz.UTC).isoformat(),
                'category': 'network_io',
                'duration': duration
            }

    async def _update_node_mappings(self):
        """Update node and pod mappings"""
        try:
            self._node_mappings = {
                'pod_to_node': self.utility.get_pod_to_node_mapping_via_oc(namespace='openshift-etcd'),
                'master_nodes': [n.get('name','') for n in (await self.utility.get_node_groups()).get('controlplane', [])]
            }
            self._cache_valid = True
            self.logger.debug("Updated node mappings cache")
        except Exception as e:
            self.logger.warning(f"Failed to update node mappings: {e}")
            self._cache_valid = False

    async def _collect_pod_metrics(self, prom: PrometheusBaseQuery, duration: str, pod_metrics: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Collect pod level network metrics based on config-provided names and expressions"""
        results = {}
        
        for metric_name, mconf in pod_metrics.items():
            query = mconf.get('expr', '')
            try:
                result = await self._query_with_stats(prom, query, duration)
                
                if result['status'] == 'success':
                    pod_stats = {}
                    
                    # Special handling for etcd_network_active_peers
                    if metric_name == 'etcd_network_active_peers':
                        # Process each series
                        for series in result.get('series_data', []):
                            labels = series['labels']
                            stats = series['statistics']
                            
                            pod_name = labels.get('pod', 'unknown')
                            node_name = self._resolve_node_name(pod_name)
                            local_member = labels.get('Local', 'unknown')
                            remote_member = labels.get('Remote', 'unknown')
                            
                            # Create a unique key for each pod-local-remote combination
                            key = f"{pod_name}_{local_member}_{remote_member}"
                            
                            pod_stats[key] = {
                                'pod': pod_name,
                                'node': node_name,
                                'local': local_member,
                                'remote': remote_member,
                                'avg': stats.get('avg'),
                                'max': stats.get('max'),
                                'latest': stats.get('latest')
                            }
                    else:
                        # Standard processing for other pod metrics
                        for series in result.get('series_data', []):
                            labels = series['labels']
                            stats = series['statistics']
                            
                            pod_name = labels.get('pod', 'unknown')
                            node_name = self._resolve_node_name(pod_name)
                            
                            pod_stats[pod_name] = {
                                'avg': stats.get('avg'),
                                'max': stats.get('max'),
                                'node': node_name
                            }
                    
                    results[metric_name] = {
                        'status': 'success',
                        'pods': pod_stats,
                        'unit': mconf.get('unit', 'unknown'),
                        'title': mconf.get('title', metric_name),
                        'description': mconf.get('description', ''),
                        'query': query
                    }
                else:
                    results[metric_name] = {
                        'status': 'error',
                        'error': result.get('error', 'Query failed')
                    }
                    
            except Exception as e:
                self.logger.error(f"Error collecting pod metric {metric_name}: {e}")
                results[metric_name] = {
                    'status': 'error',
                    'error': str(e)
                }
        
        return results

    async def _collect_node_metrics(self, prom: PrometheusBaseQuery, duration: str, node_metrics: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Collect node level network metrics based on config-provided names and expressions"""
        results = {}
        
        for metric_name, mconf in node_metrics.items():
            query = mconf.get('expr', '')
            try:
                result = await self._query_with_stats(prom, query, duration)
                
                if result['status'] == 'success':
                    node_stats = {}
                    
                    # Process each series and aggregate by node
                    node_aggregates = {}
                    for series in result.get('series_data', []):
                        labels = series['labels']
                        stats = series['statistics']
                        
                        # Resolve node name from instance
                        node_name = self._resolve_instance_to_node(labels.get('instance', 'unknown'))
                        
                        # Only include master nodes
                        if node_name not in self._node_mappings.get('master_nodes', []):
                            continue
                        
                        if node_name not in node_aggregates:
                            node_aggregates[node_name] = {
                                'avg_values': [],
                                'max_values': [],
                                'devices': []
                            }
                        
                        if stats.get('avg') is not None:
                            node_aggregates[node_name]['avg_values'].append(stats['avg'])
                        if stats.get('max') is not None:
                            node_aggregates[node_name]['max_values'].append(stats['max'])
                        
                        device = labels.get('device', 'unknown')
                        node_aggregates[node_name]['devices'].append(device)
                    
                    # Calculate final node stats
                    for node_name, agg_data in node_aggregates.items():
                        avg_values = agg_data['avg_values']
                        max_values = agg_data['max_values']
                        
                        node_stats[node_name] = {
                            'avg': sum(avg_values) if avg_values else None,
                            'max': max(max_values) if max_values else None,
                            'device_count': len(set(agg_data['devices']))
                        }
                    
                    results[metric_name] = {
                        'status': 'success',
                        'nodes': node_stats,
                        'unit': mconf.get('unit', 'unknown'),
                        'title': mconf.get('title', metric_name),
                        'description': mconf.get('description', ''),
                        'query': query
                    }
                else:
                    results[metric_name] = {
                        'status': 'error',
                        'error': result.get('error', 'Query failed')
                    }
                    
            except Exception as e:
                self.logger.error(f"Error collecting node metric {metric_name}: {e}")
                results[metric_name] = {
                    'status': 'error',
                    'error': str(e)
                }
        
        return results

    async def _collect_cluster_metrics(self, prom: PrometheusBaseQuery, duration: str, cluster_metrics: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Collect cluster level metrics based on config-provided names and expressions (e.g., gRPC streams)"""
        results = {}
        
        for metric_name, mconf in cluster_metrics.items():
            query = mconf.get('expr', '')
            try:
                result = await self._query_with_stats(prom, query, duration)
                
                if result['status'] == 'success':
                    overall_stats = result.get('overall_statistics', {})
                    
                    results[metric_name] = {
                        'status': 'success',
                        'avg': overall_stats.get('avg'),
                        'max': overall_stats.get('max'),
                        'latest': overall_stats.get('latest'),
                        'unit': mconf.get('unit', 'unknown'),
                        'title': mconf.get('title', metric_name),
                        'description': mconf.get('description', ''),
                        'query': query
                    }
                else:
                    results[metric_name] = {
                        'status': 'error',
                        'error': result.get('error', 'Query failed')
                    }
                    
            except Exception as e:
                self.logger.error(f"Error collecting cluster metric {metric_name}: {e}")
                results[metric_name] = {
                    'status': 'error',
                    'error': str(e)
                }
        
        return results

    def _resolve_node_name(self, pod_name: str) -> str:
        """Resolve node name from pod name"""
        pod_to_node = self._node_mappings.get('pod_to_node', {})
        return pod_to_node.get(pod_name, 'unknown')

    def _resolve_instance_to_node(self, instance: str) -> str:
        """Resolve node name from Prometheus instance label"""
        try:
            # Instance format can be: IP:port, hostname:port, or just hostname
            if ':' in instance:
                host_part = instance.split(':')[0]
            else:
                host_part = instance
            
            # Check if it's an IP address
            if self._is_ip_address(host_part):
                # For IP addresses, we need to map to node names
                # This would require additional logic to map IPs to nodes
                # For now, return the IP as is
                return host_part
            
            # If it's a hostname, check if it matches any master node
            master_nodes = self._node_mappings.get('master_nodes', [])
            for node in master_nodes:
                if host_part in node or node in host_part:
                    return node
            
            return host_part
            
        except Exception as e:
            self.logger.warning(f"Error resolving instance {instance} to node: {e}")
            return instance

    def _is_ip_address(self, address: str) -> bool:
        """Check if a string is an IP address"""
        try:
            import ipaddress
            ipaddress.ip_address(address)
            return True
        except ValueError:
            return False

    async def _query_with_stats(self, prom_client: PrometheusBaseQuery, query: str, duration: str) -> Dict[str, Any]:
        """Execute a range query and compute basic statistics per series and overall."""
        try:
            start, end = prom_client.get_time_range_from_duration(duration)
            data = await prom_client.query_range(query, start, end, step='15s')

            series_data: List[Dict[str, Any]] = []
            all_values: List[float] = []

            for item in data.get('result', []) if isinstance(data, dict) else []:
                metric_labels = item.get('metric', {})
                values_pairs = item.get('values', []) or []

                numeric_values = []
                for ts, val in values_pairs:
                    try:
                        v = float(val)
                    except (ValueError, TypeError):
                        continue
                    if v != float('inf') and v != float('-inf'):
                        numeric_values.append(v)

                stats: Dict[str, Any] = {}
                if numeric_values:
                    avg_v = sum(numeric_values) / len(numeric_values)
                    max_v = max(numeric_values)
                    min_v = min(numeric_values)
                    stats = {
                        'avg': avg_v,
                        'max': max_v,
                        'min': min_v,
                        'count': len(numeric_values),
                        'latest': numeric_values[-1]
                    }
                    all_values.extend(numeric_values)

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

    async def get_network_summary(self, duration: str = "1h") -> Dict[str, Any]:
        """Get network I/O summary across all metrics"""
        try:
            full_results = await self.collect_metrics(duration)
            
            if full_results['status'] != 'success':
                return full_results
            
            data = full_results.get('data', {})
            
            summary = {
                'status': 'success',
                'timestamp': full_results['timestamp'],
                'duration': duration,
                'network_health': 'healthy',
                'summary': {
                    'pods_metrics_count': len([m for m in data.get('pods_metrics', {}).values() if m.get('status') == 'success']),
                    'node_metrics_count': len([m for m in data.get('node_metrics', {}).values() if m.get('status') == 'success']),
                    'cluster_metrics_count': len([m for m in data.get('cluster_metrics', {}).values() if m.get('status') == 'success']),
                    'total_etcd_pods': len(self._node_mappings.get('pod_to_node', {})),
                    'total_master_nodes': len(self._node_mappings.get('master_nodes', []))
                },
                'alerts': []
            }
            
            # Check for high peer latency
            pods_metrics = data.get('pods_metrics', {})
            if 'network_io_peer2peer_rtt_latency_p99' in pods_metrics:
                latency_data = pods_metrics['network_io_peer2peer_rtt_latency_p99']
                if latency_data.get('status') == 'success':
                    for pod_name, pod_data in latency_data.get('pods', {}).items():
                        avg_latency = pod_data.get('avg', 0)
                        if avg_latency and avg_latency > 0.1:  # 100ms threshold
                            summary['alerts'].append({
                                'level': 'warning',
                                'metric': 'peer_latency',
                                'message': f'High peer latency on pod {pod_name}: {avg_latency:.3f}s'
                            })
            
            # Set health status based on alerts
            if summary['alerts']:
                summary['network_health'] = 'degraded' if any(a['level'] == 'warning' for a in summary['alerts']) else 'critical'
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating network summary: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(pytz.UTC).isoformat()
            }