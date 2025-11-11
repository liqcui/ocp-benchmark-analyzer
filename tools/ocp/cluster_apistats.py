"""
Kubernetes API Server Metrics Collector Module
Collects API server performance metrics for monitoring
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


class apiUsageCollector:
    """Collector for Kubernetes API server metrics"""
    
    def __init__(self, ocp_auth: OCPAuth, duration: str = "5m", metrics_file_path: str = None):
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.logger = logging.getLogger(__name__)
        self.utility = mcpToolsUtility(ocp_auth)
        
        # Load metrics configuration
        if metrics_file_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metrics_file_path = os.path.join(project_root, 'config', 'metrics-api.yml')
        
        self.config = Config()
        load_result = self.config.load_metrics_file(metrics_file_path)
        
        # Get category-specific metrics
        self.category = "api_server"
        self.api_server_metrics = self.config.get_metrics_by_category(self.category)
        
        # Log metrics loading
        if load_result.get('success'):
            metrics_count = len(self.api_server_metrics)
            self.logger.info(f"✅ Loaded {metrics_count} metrics from {self.category} category (apiUsageCollector)")
        else:
            self.logger.warning(f"⚠️ No {self.category} metrics found in configuration")
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all API server metrics and return comprehensive results with cluster summary"""
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
                    'category': 'api_server',
                    'metrics': {}
                }
                
                # Process each metric dynamically from config
                for metric_config in self.api_server_metrics:
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
                
                # Generate cluster summary and add it to results
                cluster_summary = self._generate_cluster_summary(results)
                results['cluster_summary'] = cluster_summary
                
                return results
                
        except Exception as e:
            self.logger.error(f"Error in collect_all_metrics: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(pytz.UTC).isoformat()
            }
    
    async def _collect_generic_metric(self, prom: PrometheusBaseQuery, metric_config: Dict[str, Any]) -> Dict[str, Any]:
        """Generic method to collect any API server metric"""
        query = metric_config['expr']
        metric_name = metric_config['name']
        
        # Replace template variables with actual values
        query = self._replace_query_variables(query)
        
        try:
            result = await self._query_with_stats(prom, query, self.duration)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            # Process results by series
            series_stats = []
            for series in result.get('series_data', []):
                labels = series['labels']
                stats = series['statistics']
                
                # Extract relevant labels - only include if they exist and aren't empty
                series_info = {
                    'labels': self._format_labels(labels)
                }
                
                # Only add label fields if they exist in the metric
                if 'resource' in labels and labels['resource']:
                    series_info['resource'] = labels['resource']
                if 'verb' in labels and labels['verb']:
                    series_info['verb'] = labels['verb']
                if 'scope' in labels and labels['scope']:
                    series_info['scope'] = labels['scope']
                if 'instance' in labels and labels['instance']:
                    series_info['instance'] = labels['instance']
                if 'operation' in labels and labels['operation']:
                    series_info['operation'] = labels['operation']
                if 'type' in labels and labels['type']:
                    series_info['type'] = labels['type']
                if 'flow_schema' in labels and labels['flow_schema']:
                    series_info['flow_schema'] = labels['flow_schema']
                if 'priority_level' in labels and labels['priority_level']:
                    series_info['priority_level'] = labels['priority_level']
                if 'code' in labels and labels['code']:
                    series_info['code'] = labels['code']
                if 'request_kind' in labels and labels['request_kind']:
                    series_info['request_kind'] = labels['request_kind']
                
                # Format statistics based on metric type and unit
                unit = metric_config.get('unit', 'unknown')
                if unit == 'seconds':
                    series_info.update({
                        'avg_seconds': round(stats.get('avg', 0), 6),
                        'max_seconds': round(stats.get('max', 0), 6),
                        'data_points': stats.get('count', 0)
                    })
                    # Store sort key for top 5
                    series_info['_sort_value'] = stats.get('max', 0)
                elif unit == 'requests_per_second':
                    series_info.update({
                        'avg_requests_per_sec': round(stats.get('avg', 0), 3),
                        'max_requests_per_sec': round(stats.get('max', 0), 3),
                        'data_points': stats.get('count', 0)
                    })
                    # Store sort key for top 5
                    series_info['_sort_value'] = stats.get('max', 0)
                elif unit == 'count':
                    series_info.update({
                        'avg_count': round(stats.get('avg', 0), 2),
                        'max_count': round(stats.get('max', 0), 2),
                        'data_points': stats.get('count', 0)
                    })
                    # Store sort key for top 5
                    series_info['_sort_value'] = stats.get('max', 0)
                else:
                    series_info.update({
                        'avg_value': round(stats.get('avg', 0), 6),
                        'max_value': round(stats.get('max', 0), 6),
                        'data_points': stats.get('count', 0)
                    })
                    # Store sort key for top 5
                    series_info['_sort_value'] = stats.get('max', 0)
                
                series_stats.append(series_info)
            
            # Resolve node names for instances if available
            node_mapping = {}
            for series_info in series_stats:
                instance = series_info.get('instance')
                if instance:
                    node_name = await self.utility.resolve_node_from_instance(instance, prom)
                    node_mapping[instance] = node_name
                    series_info['node_name'] = node_name
            
            # Sort by the sort value (max) and get top 3
            series_stats.sort(key=lambda x: x.get('_sort_value', 0), reverse=True)
            top_3 = []
            all_series = []
            
            for i, series_info in enumerate(series_stats):
                # Remove the internal sort key
                series_info.pop('_sort_value', None)
                
                all_series.append(series_info)
                if i < 3:
                    top_3.append(series_info)
            
            return {
                'status': 'success',
                'metric': metric_name,
                'title': metric_config.get('title', metric_name.replace('_', ' ').title()),
                'unit': metric_config.get('unit', 'unknown'),
                'description': metric_config.get('description', ''),
                'top_3': top_3,
                'all_series': all_series,
                'total_series': len(series_stats),
                'overall_stats': result.get('overall_statistics', {})
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting metric {metric_name}: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def _format_labels(self, labels: Dict[str, str]) -> str:
        """Format labels into a readable string"""
        priority_keys = ['resource', 'verb', 'scope', 'operation', 'type', 
                        'flow_schema', 'priority_level', 'code', 'request_kind']
        parts = []
        
        for key in priority_keys:
            if key in labels and labels[key]:
                parts.append(f"{key}={labels[key]}")
        
        # Add any remaining non-standard labels
        for key, value in labels.items():
            if key not in priority_keys and key not in ['__name__', 'job', 'instance', 'apiserver']:
                parts.append(f"{key}={value}")
        
        return ', '.join(parts) if parts else 'no_labels'
    
    def _replace_query_variables(self, query: str) -> str:
        """Replace Grafana template variables with actual values or wildcards
        
        Template variables like $apiserver, $instance, $resource need to be replaced
        with actual PromQL patterns for queries to work.
        """
        # Common template variable replacements
        replacements = {
            '$apiserver': '.*',  # Match any apiserver
            '$instance': '.*',   # Match any instance
            '$resource': '.*',   # Match any resource
            '$verb': '.*',       # Match any verb
            '$flow_schema': '.*', # Match any flow schema
            '$priority_level': '.*', # Match any priority level
            '$interval': self.duration,  # Use the duration passed to collector
        }
        
        # Replace all template variables
        result_query = query
        for template_var, replacement in replacements.items():
            result_query = result_query.replace(template_var, replacement)
        
        return result_query
    
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
                    stats = {
                        'avg': avg_v,
                        'max': max_v,
                        'count': len(values)
                    }
                    all_values.extend(values)

                series_data.append({'labels': metric_labels, 'statistics': stats})

            overall_statistics: Dict[str, Any] = {}
            if all_values:
                overall_statistics = {
                    'avg': sum(all_values) / len(all_values),
                    'max': max(all_values),
                    'count': len(all_values)
                }

            return {
                'status': 'success',
                'series_data': series_data,
                'overall_statistics': overall_statistics
            }
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    # Individual metric collection functions
    
    async def get_avg_ro_apicalls_latency(self) -> Dict[str, Any]:
        """Get average read-only API calls latency"""
        metric_config = self.config.get_metric_by_name('avg_ro_apicalls_latency')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_max_ro_apicalls_latency(self) -> Dict[str, Any]:
        """Get maximum read-only API calls latency"""
        metric_config = self.config.get_metric_by_name('max_ro_apicalls_latency')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_avg_mutating_apicalls_latency(self) -> Dict[str, Any]:
        """Get average mutating API calls latency"""
        metric_config = self.config.get_metric_by_name('avg_mutating_apicalls_latency')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_max_mutating_apicalls_latency(self) -> Dict[str, Any]:
        """Get maximum mutating API calls latency"""
        metric_config = self.config.get_metric_by_name('max_mutating_apicalls_latency')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_api_request_rate(self) -> Dict[str, Any]:
        """Get API server request rate"""
        metric_config = self.config.get_metric_by_name('api_request_rate')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_api_request_errors(self) -> Dict[str, Any]:
        """Get API server request errors"""
        metric_config = self.config.get_metric_by_name('api_request_errors')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_api_server_current_inflight_requests(self) -> Dict[str, Any]:
        """Get current number of inflight requests"""
        metric_config = self.config.get_metric_by_name('api_server_current_inflight_requests')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_etcd_request_duration(self) -> Dict[str, Any]:
        """Get 99th percentile etcd request duration"""
        metric_config = self.config.get_metric_by_name('etcd_request_duration')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_request_latency_p99_by_verb(self) -> Dict[str, Any]:
        """Get 99th percentile request latency by verb"""
        metric_config = self.config.get_metric_by_name('request_latency_p99_by_verb')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_request_duration_p99_by_resource(self) -> Dict[str, Any]:
        """Get 99th percentile request duration by resource"""
        metric_config = self.config.get_metric_by_name('request_duration_p99_by_resource')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_pf_request_wait_duration_p99(self) -> Dict[str, Any]:
        """Get Priority and Fairness 99th percentile request wait duration"""
        metric_config = self.config.get_metric_by_name('pf_request_wait_duration_p99')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_pf_request_execution_duration_p99(self) -> Dict[str, Any]:
        """Get Priority and Fairness 99th percentile request execution duration"""
        metric_config = self.config.get_metric_by_name('pf_request_execution_duration_p99')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_pf_request_dispatch_rate(self) -> Dict[str, Any]:
        """Get Priority and Fairness request dispatch rate"""
        metric_config = self.config.get_metric_by_name('pf_request_dispatch_rate')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_pf_request_in_queue(self) -> Dict[str, Any]:
        """Get Priority and Fairness current requests in queue"""
        metric_config = self.config.get_metric_by_name('pf_request_in_queue')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_cluster_summary(self) -> Dict[str, Any]:
        """Get cluster-wide API server performance summary (standalone version)"""
        full_results = await self.collect_all_metrics()
        
        # The cluster_summary is already included in full_results
        # This method returns just the summary portion for backward compatibility
        if full_results.get('status') == 'success':
            return full_results.get('cluster_summary', {})
        else:
            return {
                'status': 'error',
                'error': full_results.get('error', 'Unknown error'),
                'timestamp': full_results.get('timestamp')
            }
    
    def _generate_cluster_summary(self, full_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate cluster-wide API server performance summary from collected metrics"""
        try:
            # Extract cluster-wide insights
            summary = {
                'status': 'success',
                'timestamp': full_results['timestamp'],
                'cluster_health': {
                    'api_server_status': 'unknown',
                    'total_metrics_collected': 0
                },
                'performance_indicators': {},
                'recommendations': [],
                'top_issues': []
            }
            
            # Analyze read-only API call latency
            ro_latency = full_results['metrics'].get('max_ro_apicalls_latency', {})
            if ro_latency.get('status') == 'success':
                overall_stats = ro_latency.get('overall_stats', {})
                max_latency = overall_stats.get('max', 0)
                avg_latency = overall_stats.get('avg', 0)
                
                summary['performance_indicators']['read_only_latency'] = {
                    'max_seconds': round(max_latency, 6),
                    'avg_seconds': round(avg_latency, 6),
                    'top_3_resources': ro_latency.get('top_3', [])
                }
                
                # Performance assessment
                if max_latency < 1.0:
                    status = 'excellent'
                elif max_latency < 5.0:
                    status = 'good'
                elif max_latency < 10.0:
                    status = 'warning'
                else:
                    status = 'critical'
                
                summary['cluster_health']['api_server_status'] = status
                
                if max_latency > 10.0:
                    summary['recommendations'].append("High read-only API latency detected (>10s). Investigate API server performance.")
                    summary['top_issues'].append({
                        'severity': 'critical',
                        'metric': 'read_only_latency',
                        'value': f"{max_latency:.3f}s",
                        'threshold': '10s',
                        'message': 'Read-only API calls experiencing high latency'
                    })
                elif max_latency > 5.0:
                    summary['top_issues'].append({
                        'severity': 'warning',
                        'metric': 'read_only_latency',
                        'value': f"{max_latency:.3f}s",
                        'threshold': '5s',
                        'message': 'Read-only API calls showing elevated latency'
                    })
            
            # Analyze mutating API call latency
            mutating_latency = full_results['metrics'].get('max_mutating_apicalls_latency', {})
            if mutating_latency.get('status') == 'success':
                overall_stats = mutating_latency.get('overall_stats', {})
                max_latency = overall_stats.get('max', 0)
                avg_latency = overall_stats.get('avg', 0)
                
                summary['performance_indicators']['mutating_latency'] = {
                    'max_seconds': round(max_latency, 6),
                    'avg_seconds': round(avg_latency, 6),
                    'top_3_resources': mutating_latency.get('top_3', [])
                }
                
                if max_latency > 30.0:
                    summary['recommendations'].append("High mutating API latency detected (>30s). Check API server and etcd performance.")
                    summary['top_issues'].append({
                        'severity': 'critical',
                        'metric': 'mutating_latency',
                        'value': f"{max_latency:.3f}s",
                        'threshold': '30s',
                        'message': 'Mutating API calls experiencing high latency'
                    })
                elif max_latency > 10.0:
                    summary['top_issues'].append({
                        'severity': 'warning',
                        'metric': 'mutating_latency',
                        'value': f"{max_latency:.3f}s",
                        'threshold': '10s',
                        'message': 'Mutating API calls showing elevated latency'
                    })
            
            # Analyze error rates
            error_rate = full_results['metrics'].get('api_request_errors', {})
            if error_rate.get('status') == 'success':
                overall_stats = error_rate.get('overall_stats', {})
                avg_errors = overall_stats.get('avg', 0)
                max_errors = overall_stats.get('max', 0)
                
                summary['performance_indicators']['error_rate'] = {
                    'avg_errors_per_sec': round(avg_errors, 3),
                    'max_errors_per_sec': round(max_errors, 3),
                    'top_3_errors': error_rate.get('top_3', [])
                }
                
                if avg_errors > 10:
                    summary['recommendations'].append("High API error rate detected. Review API server logs and client requests.")
                    summary['top_issues'].append({
                        'severity': 'critical',
                        'metric': 'error_rate',
                        'value': f"{avg_errors:.3f} errors/sec",
                        'threshold': '10 errors/sec',
                        'message': 'High API request error rate'
                    })
                elif avg_errors > 1:
                    summary['top_issues'].append({
                        'severity': 'warning',
                        'metric': 'error_rate',
                        'value': f"{avg_errors:.3f} errors/sec",
                        'threshold': '1 error/sec',
                        'message': 'Elevated API request error rate'
                    })
            
            # Analyze API request rate
            request_rate = full_results['metrics'].get('api_request_rate', {})
            if request_rate.get('status') == 'success':
                overall_stats = request_rate.get('overall_stats', {})
                avg_rate = overall_stats.get('avg', 0)
                max_rate = overall_stats.get('max', 0)
                
                summary['performance_indicators']['request_rate'] = {
                    'avg_requests_per_sec': round(avg_rate, 3),
                    'max_requests_per_sec': round(max_rate, 3),
                    'top_3_resources': request_rate.get('top_3', [])
                }
                
                if avg_rate > 1000:
                    summary['recommendations'].append("Very high API request rate. Monitor for capacity issues.")
            
            # Analyze inflight requests
            inflight = full_results['metrics'].get('api_server_current_inflight_requests', {})
            if inflight.get('status') == 'success':
                overall_stats = inflight.get('overall_stats', {})
                avg_inflight = overall_stats.get('avg', 0)
                max_inflight = overall_stats.get('max', 0)
                
                summary['performance_indicators']['inflight_requests'] = {
                    'avg_count': round(avg_inflight, 2),
                    'max_count': round(max_inflight, 2),
                    'breakdown': inflight.get('top_3', [])
                }
                
                if max_inflight > 1000:
                    summary['recommendations'].append("High number of inflight requests. API server may be under stress.")
                    summary['top_issues'].append({
                        'severity': 'warning',
                        'metric': 'inflight_requests',
                        'value': f"{max_inflight:.0f} requests",
                        'threshold': '1000 requests',
                        'message': 'High number of concurrent inflight requests'
                    })
            
            # Analyze etcd request duration
            etcd_latency = full_results['metrics'].get('etcd_request_duration', {})
            if etcd_latency.get('status') == 'success':
                overall_stats = etcd_latency.get('overall_stats', {})
                avg_latency = overall_stats.get('avg', 0)
                max_latency = overall_stats.get('max', 0)
                
                summary['performance_indicators']['etcd_latency'] = {
                    'avg_seconds': round(avg_latency, 6),
                    'max_seconds': round(max_latency, 6),
                    'top_3_operations': etcd_latency.get('top_3', [])
                }
                
                if max_latency > 0.1:
                    summary['recommendations'].append("High etcd latency detected (>100ms). Check etcd cluster performance.")
                    summary['top_issues'].append({
                        'severity': 'critical',
                        'metric': 'etcd_latency',
                        'value': f"{max_latency*1000:.1f}ms",
                        'threshold': '100ms',
                        'message': 'etcd requests experiencing high latency'
                    })
                elif max_latency > 0.05:
                    summary['top_issues'].append({
                        'severity': 'warning',
                        'metric': 'etcd_latency',
                        'value': f"{max_latency*1000:.1f}ms",
                        'threshold': '50ms',
                        'message': 'etcd requests showing elevated latency'
                    })
            
            # Analyze Priority and Fairness metrics
            pf_wait = full_results['metrics'].get('pf_request_wait_duration_p99', {})
            if pf_wait.get('status') == 'success':
                overall_stats = pf_wait.get('overall_stats', {})
                avg_wait = overall_stats.get('avg', 0)
                max_wait = overall_stats.get('max', 0)
                
                summary['performance_indicators']['priority_fairness_wait'] = {
                    'avg_seconds': round(avg_wait, 6),
                    'max_seconds': round(max_wait, 6),
                    'top_3_flows': pf_wait.get('top_3', [])
                }
                
                if max_wait > 1.0:
                    summary['recommendations'].append("High Priority & Fairness wait time (>1s). Review flow schemas and priority levels.")
                    summary['top_issues'].append({
                        'severity': 'warning',
                        'metric': 'pf_wait_duration',
                        'value': f"{max_wait:.3f}s",
                        'threshold': '1s',
                        'message': 'High Priority & Fairness queue wait times'
                    })
            
            # Count successful metrics
            summary['cluster_health']['total_metrics_collected'] = full_results['summary']['successful_metrics']
            
            # Sort issues by severity
            severity_order = {'critical': 0, 'warning': 1, 'info': 2}
            summary['top_issues'].sort(key=lambda x: severity_order.get(x.get('severity', 'info'), 3))
            
            # Add overall health score
            critical_issues = sum(1 for issue in summary['top_issues'] if issue.get('severity') == 'critical')
            warning_issues = sum(1 for issue in summary['top_issues'] if issue.get('severity') == 'warning')
            
            if critical_issues > 0:
                summary['cluster_health']['overall_score'] = 'critical'
            elif warning_issues > 2:
                summary['cluster_health']['overall_score'] = 'warning'
            elif warning_issues > 0:
                summary['cluster_health']['overall_score'] = 'good'
            else:
                summary['cluster_health']['overall_score'] = 'excellent'
            
            summary['cluster_health']['issues_count'] = {
                'critical': critical_issues,
                'warning': warning_issues,
                'total': len(summary['top_issues'])
            }
            
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


# Convenience functions for API usage collection
async def collect_api_metrics(ocp_auth: OCPAuth, duration: str = "5m") -> Dict[str, Any]:
    """Convenience function to collect all API server metrics"""
    collector = apiUsageCollector(ocp_auth, duration)
    return await collector.collect_all_metrics()


async def get_api_cluster_summary(ocp_auth: OCPAuth, duration: str = "5m") -> Dict[str, Any]:
    """Convenience function to get API server cluster summary"""
    collector = apiUsageCollector(ocp_auth, duration)
    return await collector.get_cluster_summary()


async def get_specific_api_metric(ocp_auth: OCPAuth, metric_name: str, duration: str = "5m") -> Dict[str, Any]:
    """Convenience function to get a specific API server metric"""
    collector = apiUsageCollector(ocp_auth, duration)
    return await collector.get_metric_by_name(metric_name)