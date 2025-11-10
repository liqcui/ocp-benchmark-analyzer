"""
OVN Database Size Collector Module
Collects OVN Northbound and Southbound database size metrics
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


class ovnDBCollector:
    """Collector for OVN Database Size metrics"""
    
    def __init__(self, ocp_auth: OCPAuth, duration: str = "1h", 
                 start_time: str = None, end_time: str = None,
                 metrics_file_path: str = None):
        """
        Initialize OVN DB Collector
        
        Args:
            ocp_auth: OpenShift authentication object
            duration: Duration string (e.g., '1h', '30m') - used if start_time/end_time not provided
            start_time: Optional start time (ISO format)
            end_time: Optional end time (ISO format)
            metrics_file_path: Optional path to metrics configuration file
        """
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.start_time = start_time
        self.end_time = end_time
        self.logger = logging.getLogger(__name__)
        self.utility = mcpToolsUtility(ocp_auth)
        
        # Load metrics configuration
        if metrics_file_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metrics_file_path = os.path.join(project_root, 'config', 'metrics-ovn.yml')
        
        self.config = Config()
        load_result = self.config.load_metrics_file(metrics_file_path)
        
        # Get category-specific metrics
        self.category = "ovn"
        self.ovn_metrics = self.config.get_metrics_by_category(self.category)
        
        # Log metrics loading
        if load_result.get('success'):
            metrics_count = len(self.ovn_metrics)
            self.logger.info(f"✅ Loaded {metrics_count} metrics from {self.category} category (ovnDBCollector)")
        else:
            self.logger.warning(f"⚠️ No {self.category} metrics found in configuration")
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all OVN DB size metrics and return comprehensive results"""
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
                
                # Get time range
                if self.start_time and self.end_time:
                    start = self.start_time
                    end = self.end_time
                    time_range_type = "absolute"
                else:
                    start, end = prom.get_time_range_from_duration(self.duration)
                    time_range_type = "duration"
                
                # Collect results for all metrics
                results = {
                    'status': 'success',
                    'timestamp': datetime.now(pytz.UTC).isoformat(),
                    'time_range': {
                        'type': time_range_type,
                        'duration': self.duration if time_range_type == 'duration' else None,
                        'start': start,
                        'end': end
                    },
                    'category': 'ovn',
                    'metrics': {}
                }
                
                # Get node role mapping for grouping
                node_role_mapping = await self.utility.get_node_role_mapping(prom)
                
                # Process each metric dynamically from config
                for metric_config in self.ovn_metrics:
                    metric_name = metric_config['name']
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        metric_result = await self._collect_generic_metric(
                            prom, metric_config, start, end, node_role_mapping
                        )
                        results['metrics'][metric_name] = metric_result
                        
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        results['metrics'][metric_name] = {
                            'status': 'error',
                            'error': str(e)
                        }
                
                # Add summary
                successful_metrics = sum(1 for m in results['metrics'].values() if m.get('status') == 'success')
                total_metrics = len(results['metrics'])
                
                results['summary'] = {
                    'total_metrics': total_metrics,
                    'successful_metrics': successful_metrics,
                    'failed_metrics': total_metrics - successful_metrics
                }
                
                return results
                
        except Exception as e:
            self.logger.error(f"Error in collect_all_metrics: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(pytz.UTC).isoformat()
            }
    
    async def _collect_generic_metric(self, prom: PrometheusBaseQuery, 
                                      metric_config: Dict[str, Any],
                                      start: str, end: str,
                                      node_role_mapping: Dict[str, str]) -> Dict[str, Any]:
        """Generic method to collect any OVN metric with node grouping"""
        query = metric_config['expr']
        metric_name = metric_config['name']
        
        try:
            result = await self._query_with_stats(prom, query, start, end)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            # Get pod to node mapping for openshift-ovn-kubernetes namespace
            pod_to_node = self.utility.get_pod_to_node_mapping_via_oc(namespace="openshift-ovn-kubernetes")
            
            # Process results by pod with node grouping
            pod_stats = {}
            node_groups = {
                'controlplane': {'pods': [], 'stats': []},
                'worker': {'pods': [], 'stats': []},
                'infra': {'pods': [], 'stats': []},
                'workload': {'pods': [], 'stats': []}
            }
            
            for series in result.get('series_data', []):
                pod_name = series['labels'].get('pod', 'unknown')
                
                if pod_name != 'unknown':
                    stats = series['statistics']
                    
                    # Get full node name from pod mapping
                    node_name = pod_to_node.get(pod_name, 'unknown')
                    
                    # Determine node role using full node name
                    node_role = node_role_mapping.get(node_name, 'unknown')
                    
                    # Format stats based on metric type (database size in bytes)
                    pod_stat = {
                        'avg_bytes': round(stats.get('avg', 0), 2),
                        'max_bytes': round(stats.get('max', 0), 2),
                        'min_bytes': round(stats.get('min', 0), 2),
                        'latest_bytes': round(stats.get('latest', 0), 2) if stats.get('latest') is not None else None,
                        'avg_mb': round(stats.get('avg', 0) / (1024 * 1024), 2),
                        'max_mb': round(stats.get('max', 0) / (1024 * 1024), 2),
                        'data_points': stats.get('count', 0),
                        'node': node_name,
                        'role': node_role
                    }
                    
                    pod_stats[pod_name] = pod_stat
                    
                    # Add to node group if role is known
                    if node_role in node_groups:
                        node_groups[node_role]['pods'].append(pod_name)
                        node_groups[node_role]['stats'].append(pod_stat)
            
            # Calculate aggregated stats per node group
            group_summary = {}
            for role, group_data in node_groups.items():
                if group_data['stats']:
                    all_avg_bytes = [s['avg_bytes'] for s in group_data['stats']]
                    all_max_bytes = [s['max_bytes'] for s in group_data['stats']]
                    
                    group_summary[role] = {
                        'pod_count': len(group_data['pods']),
                        'avg_bytes': round(sum(all_avg_bytes) / len(all_avg_bytes), 2) if all_avg_bytes else 0,
                        'max_bytes': round(max(all_max_bytes), 2) if all_max_bytes else 0,
                        'avg_mb': round(sum(all_avg_bytes) / len(all_avg_bytes) / (1024 * 1024), 2) if all_avg_bytes else 0,
                        'max_mb': round(max(all_max_bytes) / (1024 * 1024), 2) if all_max_bytes else 0,
                        'pods': group_data['pods']
                    }
                else:
                    group_summary[role] = {
                        'pod_count': 0,
                        'avg_bytes': 0,
                        'max_bytes': 0,
                        'avg_mb': 0,
                        'max_mb': 0,
                        'pods': []
                    }
            
            return {
                'status': 'success',
                'metric': metric_name,
                'title': metric_config.get('title', metric_name.replace('_', ' ').title()),
                'unit': metric_config.get('unit', 'bytes'),
                'description': metric_config.get('description', f'OVN metric: {metric_name}'),
                'pod_metrics': pod_stats,
                'grouped_by_role': group_summary,
                'total_pods': len(pod_stats),
                'overall_stats': result.get('overall_statistics', {})
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting metric {metric_name}: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def _query_with_stats(self, prom_client: PrometheusBaseQuery, 
                                query: str, start: str, end: str) -> Dict[str, Any]:
        """Execute a range query and compute basic statistics per series and overall."""
        try:
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
    
    # Separated functions for each metric
    
    async def collect_nbdb_size(self) -> Dict[str, Any]:
        """Collect OVN Northbound database size metric"""
        metric_config = self.config.get_metric_by_name('nbdb_db_size_bytes')
        if not metric_config:
            return {
                'status': 'error',
                'error': 'Metric nbdb_db_size_bytes not found in configuration'
            }
        
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': "Prometheus connection failed"
                    }
                
                # Get time range
                if self.start_time and self.end_time:
                    start = self.start_time
                    end = self.end_time
                else:
                    start, end = prom.get_time_range_from_duration(self.duration)
                
                # Get node role mapping
                node_role_mapping = await self.utility.get_node_role_mapping(prom)
                
                return await self._collect_generic_metric(prom, metric_config, start, end, node_role_mapping)
                
        except Exception as e:
            self.logger.error(f"Error collecting NBDB size: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def collect_sbdb_size(self) -> Dict[str, Any]:
        """Collect OVN Southbound database size metric"""
        metric_config = self.config.get_metric_by_name('sbdb_db_size_bytes')
        if not metric_config:
            return {
                'status': 'error',
                'error': 'Metric sbdb_db_size_bytes not found in configuration'
            }
        
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': "Prometheus connection failed"
                    }
                
                # Get time range
                if self.start_time and self.end_time:
                    start = self.start_time
                    end = self.end_time
                else:
                    start, end = prom.get_time_range_from_duration(self.duration)
                
                # Get node role mapping
                node_role_mapping = await self.utility.get_node_role_mapping(prom)
                
                return await self._collect_generic_metric(prom, metric_config, start, end, node_role_mapping)
                
        except Exception as e:
            self.logger.error(f"Error collecting SBDB size: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def get_cluster_summary(self) -> Dict[str, Any]:
        """Get cluster-wide OVN database size summary"""
        try:
            full_results = await self.collect_all_metrics()
            
            if full_results['status'] != 'success':
                return full_results
            
            # Extract cluster-wide insights
            summary = {
                'status': 'success',
                'timestamp': full_results['timestamp'],
                'time_range': full_results['time_range'],
                'cluster_health': {
                    'total_ovn_pods': 0,
                    'pods_with_data': 0
                },
                'database_sizes': {},
                'grouped_by_role': {},
                'recommendations': []
            }
            
            # Analyze both NBDB and SBDB
            for db_type, metric_name in [('northbound', 'nbdb_db_size_bytes'), 
                                         ('southbound', 'sbdb_db_size_bytes')]:
                db_metric = full_results['metrics'].get(metric_name, {})
                if db_metric.get('status') == 'success':
                    pod_metrics = db_metric.get('pod_metrics', {})
                    grouped_data = db_metric.get('grouped_by_role', {})
                    
                    # Update total pod count
                    summary['cluster_health']['total_ovn_pods'] = max(
                        summary['cluster_health']['total_ovn_pods'],
                        len(pod_metrics)
                    )
                    summary['cluster_health']['pods_with_data'] = max(
                        summary['cluster_health']['pods_with_data'],
                        len([p for p in pod_metrics.values() if p['data_points'] > 0])
                    )
                    
                    # Calculate overall database stats
                    if pod_metrics:
                        all_avg = [s['avg_mb'] for s in pod_metrics.values()]
                        all_max = [s['max_mb'] for s in pod_metrics.values()]
                        
                        summary['database_sizes'][db_type] = {
                            'avg_mb': round(sum(all_avg) / len(all_avg), 2) if all_avg else 0,
                            'max_mb': round(max(all_max), 2) if all_max else 0,
                            'total_pods': len(pod_metrics)
                        }
                        
                        # Add role-based grouping
                        summary['grouped_by_role'][db_type] = grouped_data
                        
                        # Add recommendations based on size thresholds
                        max_size_mb = max(all_max) if all_max else 0
                        if max_size_mb > 1024:  # > 1GB
                            summary['recommendations'].append(
                                f"{db_type.capitalize()} database size exceeds 1GB. Consider database compaction."
                            )
                        elif max_size_mb > 512:  # > 512MB
                            summary['recommendations'].append(
                                f"{db_type.capitalize()} database size is approaching 512MB. Monitor for growth."
                            )
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating cluster summary: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(pytz.UTC).isoformat()
            }
    
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
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': "Prometheus connection failed"
                    }
                
                # Get time range
                if self.start_time and self.end_time:
                    start = self.start_time
                    end = self.end_time
                else:
                    start, end = prom.get_time_range_from_duration(self.duration)
                
                # Get node role mapping
                node_role_mapping = await self.utility.get_node_role_mapping(prom)
                
                return await self._collect_generic_metric(prom, metric_config, start, end, node_role_mapping)
                
        except Exception as e:
            self.logger.error(f"Error getting metric {metric_name}: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }


# Convenience functions for metric collection

async def collect_ovn_db_metrics(ocp_auth: OCPAuth, duration: str = "1h", 
                                 start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Convenience function to collect all OVN database metrics"""
    collector = ovnDBCollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_all_metrics()


async def get_ovn_db_cluster_summary(ocp_auth: OCPAuth, duration: str = "1h",
                                     start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Convenience function to get OVN database cluster summary"""
    collector = ovnDBCollector(ocp_auth, duration, start_time, end_time)
    return await collector.get_cluster_summary()


async def collect_nbdb_size(ocp_auth: OCPAuth, duration: str = "1h",
                           start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Convenience function to collect NBDB size metric"""
    collector = ovnDBCollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_nbdb_size()


async def collect_sbdb_size(ocp_auth: OCPAuth, duration: str = "1h",
                           start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Convenience function to collect SBDB size metric"""
    collector = ovnDBCollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_sbdb_size()


async def get_specific_ovn_metric(ocp_auth: OCPAuth, metric_name: str, 
                                  duration: str = "1h",
                                  start_time: str = None, end_time: str = None) -> Dict[str, Any]:
    """Convenience function to get a specific OVN metric"""
    collector = ovnDBCollector(ocp_auth, duration, start_time, end_time)
    return await collector.get_metric_by_name(metric_name)