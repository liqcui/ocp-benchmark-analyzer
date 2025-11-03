"""
etcd Disk WAL Fsync Collector Module
Collects Write-Ahead Log fsync performance metrics for etcd monitoring
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


class DiskWALFsyncCollector:
    """Collector for etcd WAL fsync performance metrics"""
    
    def __init__(self, ocp_auth: OCPAuth, duration: str = "1h", metrics_file_path: str = None):
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.logger = logging.getLogger(__name__)
        self.utility = mcpToolsUtility(ocp_auth)
        
        # Load metrics configuration
        if metrics_file_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metrics_file_path = os.path.join(project_root, 'config', 'metrics-etcd.yml')
        
        self.config = Config()
        load_result = self.config.load_metrics_file(metrics_file_path)
        
        # Get category-specific metrics
        self.category = "disk_wal_fsync"
        self.wal_fsync_metrics = self.config.get_metrics_by_category(self.category)
        
        # Log metrics loading
        if load_result.get('success'):
            metrics_count = len(self.wal_fsync_metrics)
            self.logger.info(f"✅ Loaded {metrics_count} metrics from {self.category} category (DiskWALFsyncCollector)")
        else:
            self.logger.warning(f"⚠️  No {self.category} metrics found in configuration")
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all WAL fsync metrics and return comprehensive results"""
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
                
                # Collect results for all metrics
                results = {
                    'status': 'success',
                    'timestamp': datetime.now(pytz.UTC).isoformat(),
                    'duration': self.duration,
                    'category': 'disk_wal_fsync',
                    'metrics': {}
                }
                
                # Process each metric dynamically from config
                for metric_config in self.wal_fsync_metrics:
                    metric_name = metric_config['name']
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        metric_result = await self._collect_generic_metric(prom, metric_config)
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
    
    async def _collect_generic_metric(self, prom: PrometheusBaseQuery, metric_config: Dict[str, Any]) -> Dict[str, Any]:
        """Generic method to collect any WAL fsync metric"""
        query = metric_config['expr']
        metric_name = metric_config['name']
        
        try:
            result = await self._query_with_stats(prom, query, self.duration)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            # Process results by pod
            pod_stats = {}
            for series in result.get('series_data', []):
                pod_name = series['labels'].get('pod', 'unknown')
                
                if pod_name != 'unknown':
                    stats = series['statistics']
                    
                    # Format based on metric type
                    if 'duration' in metric_name and 'rate' in metric_name:
                        # Duration rate metrics
                        pod_stats[pod_name] = {
                            'avg_rate_seconds': round(stats.get('avg', 0), 6),
                            'max_rate_seconds': round(stats.get('max', 0), 6),
                            'min_rate_seconds': round(stats.get('min', 0), 6),
                            'latest_rate_seconds': round(stats.get('latest', 0), 6) if stats.get('latest') is not None else None,
                            'data_points': stats.get('count', 0)
                        }
                    elif 'duration' in metric_name and 'sum' in metric_name and 'rate' not in metric_name:
                        # Duration sum metrics
                        pod_stats[pod_name] = {
                            'avg_sum_seconds': round(stats.get('avg', 0), 6),
                            'max_sum_seconds': round(stats.get('max', 0), 6),
                            'min_sum_seconds': round(stats.get('min', 0), 6),
                            'latest_sum_seconds': round(stats.get('latest', 0), 6) if stats.get('latest') is not None else None,
                            'data_points': stats.get('count', 0)
                        }
                    elif 'count' in metric_name and 'rate' in metric_name:
                        # Count rate metrics
                        pod_stats[pod_name] = {
                            'avg_ops_per_sec': round(stats.get('avg', 0), 3),
                            'max_ops_per_sec': round(stats.get('max', 0), 3),
                            'min_ops_per_sec': round(stats.get('min', 0), 3),
                            'latest_ops_per_sec': round(stats.get('latest', 0), 3) if stats.get('latest') is not None else None,
                            'data_points': stats.get('count', 0)
                        }
                    elif 'count' in metric_name:
                        # Count metrics
                        pod_stats[pod_name] = {
                            'avg_count': round(stats.get('avg', 0), 0),
                            'max_count': round(stats.get('max', 0), 0),
                            'min_count': round(stats.get('min', 0), 0),
                            'latest_count': round(stats.get('latest', 0), 0) if stats.get('latest') is not None else None,
                            'data_points': stats.get('count', 0)
                        }
                    elif 'p99' in metric_name or 'percentile' in metric_name.lower():
                        # Percentile metrics
                        pod_stats[pod_name] = {
                            'avg_seconds': round(stats.get('avg', 0), 6),
                            'max_seconds': round(stats.get('max', 0), 6),
                            'min_seconds': round(stats.get('min', 0), 6),
                            'latest_seconds': round(stats.get('latest', 0), 6) if stats.get('latest') is not None else None,
                            'data_points': stats.get('count', 0)
                        }
                    else:
                        # Default format for unknown metric types
                        pod_stats[pod_name] = {
                            'avg_value': round(stats.get('avg', 0), 6),
                            'max_value': round(stats.get('max', 0), 6),
                            'min_value': round(stats.get('min', 0), 6),
                            'latest_value': round(stats.get('latest', 0), 6) if stats.get('latest') is not None else None,
                            'data_points': stats.get('count', 0)
                        }
            
            # Get node mapping for pods
            node_mapping = self.utility.get_pod_to_node_mapping_via_oc(namespace="openshift-etcd")
            
            return {
                'status': 'success',
                'metric': metric_name,
                'title': metric_config.get('title', metric_name.replace('_', ' ').title()),
                'unit': metric_config.get('unit', 'unknown'),
                'description': self._get_metric_description(metric_name),
                'pod_metrics': pod_stats,
                'node_mapping': {pod: node_mapping.get(pod, 'unknown') for pod in pod_stats.keys()},
                'total_pods': len(pod_stats),
                'overall_stats': result.get('overall_statistics', {})
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting metric {metric_name}: {e}")
            return {'status': 'error', 'error': str(e)}
    
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
            return {'status': 'error', 'error': str(e)}
    
    def _get_metric_description(self, metric_name: str) -> str:
        """Get description for a metric based on its name"""
        descriptions = {
            'disk_wal_fsync_seconds_duration_p99': '99th percentile WAL fsync duration per etcd pod',
            'disk_wal_fsync_duration_seconds_sum_rate': 'Rate of WAL fsync duration sum per etcd pod',
            'disk_wal_fsync_duration_sum': 'Cumulative WAL fsync duration per etcd pod',
            'disk_wal_fsync_duration_seconds_count_rate': 'Rate of WAL fsync operations per etcd pod',
            'disk_wal_fsync_duration_seconds_count': 'Total count of WAL fsync operations per etcd pod'
        }
        return descriptions.get(metric_name, f'WAL fsync metric: {metric_name}')
    
    async def get_cluster_summary(self) -> Dict[str, Any]:
        """Get cluster-wide WAL fsync performance summary"""
        try:
            full_results = await self.collect_all_metrics()
            
            if full_results['status'] != 'success':
                return full_results
            
            # Extract cluster-wide insights
            summary = {
                'status': 'success',
                'timestamp': full_results['timestamp'],
                'cluster_health': {
                    'total_etcd_pods': 0,
                    'pods_with_data': 0,
                    'wal_fsync_performance': 'unknown'
                },
                'performance_indicators': {},
                'recommendations': []
            }
            
            # Analyze p99 latency
            p99_metric = full_results['metrics'].get('disk_wal_fsync_seconds_duration_p99', {})
            if p99_metric.get('status') == 'success':
                pod_metrics = p99_metric.get('pod_metrics', {})
                summary['cluster_health']['total_etcd_pods'] = len(pod_metrics)
                summary['cluster_health']['pods_with_data'] = len([p for p in pod_metrics.values() if p['data_points'] > 0])
                
                # Check if any pod has p99 latency > 100ms (performance concern)
                high_latency_pods = [pod for pod, stats in pod_metrics.items() 
                                   if stats.get('max_seconds', 0) > 0.1]
                
                # Performance assessment
                max_latency = max([stats.get('max_seconds', 0) for stats in pod_metrics.values()], default=0)
                avg_latency = sum([stats.get('avg_seconds', 0) for stats in pod_metrics.values()]) / len(pod_metrics) if pod_metrics else 0
                
                if max_latency < 0.01:  # < 10ms
                    summary['cluster_health']['wal_fsync_performance'] = 'excellent'
                elif max_latency < 0.05:  # < 50ms
                    summary['cluster_health']['wal_fsync_performance'] = 'good'
                elif max_latency < 0.1:   # < 100ms
                    summary['cluster_health']['wal_fsync_performance'] = 'warning'
                else:  # >= 100ms
                    summary['cluster_health']['wal_fsync_performance'] = 'critical'
                
                summary['performance_indicators']['high_latency_pods'] = high_latency_pods
                summary['performance_indicators']['max_p99_latency_seconds'] = max_latency
                summary['performance_indicators']['avg_p99_latency_seconds'] = avg_latency
                
                # Add recommendations
                if max_latency > 0.1:
                    summary['recommendations'].append("High WAL fsync latency detected (>100ms). Check disk I/O performance.")
                if len(high_latency_pods) > 1:
                    summary['recommendations'].append("Multiple pods showing high latency. Consider cluster-wide storage optimization.")
            
            # Analyze operation rates
            rate_metric = full_results['metrics'].get('disk_wal_fsync_duration_seconds_count_rate', {})
            if rate_metric.get('status') == 'success':
                pod_metrics = rate_metric.get('pod_metrics', {})
                total_ops = sum([stats.get('avg_ops_per_sec', 0) for stats in pod_metrics.values()])
                max_single_pod_ops = max([stats.get('max_ops_per_sec', 0) for stats in pod_metrics.values()], default=0)
                
                summary['performance_indicators']['total_ops_per_sec'] = round(total_ops, 3)
                summary['performance_indicators']['max_ops_per_sec_single_pod'] = round(max_single_pod_ops, 3)
                
                # Add operation rate recommendations
                if total_ops > 1000:
                    summary['recommendations'].append("High WAL fsync operation rate. Monitor for potential performance impact.")
                elif total_ops < 1:
                    summary['recommendations'].append("Very low WAL fsync rate. Check if etcd cluster is receiving writes.")
            
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
                # Test connection first
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': "Prometheus connection failed"
                    }
                
                return await self._collect_generic_metric(prom, metric_config)
                
        except Exception as e:
            self.logger.error(f"Error getting metric {metric_name}: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }


# Convenience functions for individual metric collection
async def collect_wal_fsync_metrics(ocp_auth: OCPAuth, duration: str = "1h") -> Dict[str, Any]:
    """Convenience function to collect all WAL fsync metrics"""
    collector = DiskWALFsyncCollector(ocp_auth, duration)
    return await collector.collect_all_metrics()


async def get_wal_fsync_cluster_summary(ocp_auth: OCPAuth, duration: str = "1h") -> Dict[str, Any]:
    """Convenience function to get WAL fsync cluster summary"""
    collector = DiskWALFsyncCollector(ocp_auth, duration)
    return await collector.get_cluster_summary()


async def get_specific_wal_fsync_metric(ocp_auth: OCPAuth, metric_name: str, duration: str = "1h") -> Dict[str, Any]:
    """Convenience function to get a specific WAL fsync metric"""
    collector = DiskWALFsyncCollector(ocp_auth, duration)
    return await collector.get_metric_by_name(metric_name)