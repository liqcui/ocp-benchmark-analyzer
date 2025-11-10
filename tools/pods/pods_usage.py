"""
Pods Usage Collector Module
Collects CPU and memory usage metrics for pods from Prometheus
Reads metrics configuration from config/metrics-pods.yml
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


class PodsUsageCollector:
    """Collector for pods CPU and memory usage metrics"""
    
    def __init__(self, ocp_auth: OCPAuth, duration: str = "1h", metrics_file_path: str = None):
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.logger = logging.getLogger(__name__)
        self.utility = mcpToolsUtility(ocp_auth)
        
        # Load metrics configuration
        if metrics_file_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metrics_file_path = os.path.join(project_root, 'config', 'metrics-pods.yml')
        
        self.config = Config()
        load_result = self.config.load_metrics_file(metrics_file_path)
        
        # Get category-specific metrics
        self.category = "pods"
        self.pods_metrics = self.config.get_metrics_by_category(self.category)
        
        # Log metrics loading
        if load_result.get('success'):
            metrics_count = len(self.pods_metrics)
            self.logger.info(f"✅ Loaded {metrics_count} metrics from {self.category} category (PodsUsageCollector)")
        else:
            self.logger.warning(f"⚠️ No {self.category} metrics found in configuration")
    
    def _substitute_variables(self, expr: str, pod_pattern: str = ".*", 
                            container_pattern: str = ".*", 
                            namespace_pattern: str = ".*") -> str:
        """Substitute template variables in metric expressions"""
        # Replace template variables with actual patterns
        expr = expr.replace('$pod_name', pod_pattern)
        expr = expr.replace('$container_name', container_pattern)
        expr = expr.replace('$namespace', namespace_pattern)
        return expr
    
    async def collect_all_metrics(self, pod_pattern: str = ".*", 
                                 container_pattern: str = ".*",
                                 namespace_pattern: str = ".*",
                                 start_time: Optional[str] = None,
                                 end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect all pods usage metrics and return comprehensive results"""
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
                
                # Determine time range
                if start_time and end_time:
                    time_range = (start_time, end_time)
                    duration_used = None
                else:
                    time_range = None
                    duration_used = self.duration
                
                # Collect results for all metrics
                results = {
                    'status': 'success',
                    'timestamp': datetime.now(pytz.UTC).isoformat(),
                    'duration': duration_used,
                    'time_range': {'start': start_time, 'end': end_time} if start_time and end_time else None,
                    'category': 'pods',
                    'filters': {
                        'pod_pattern': pod_pattern,
                        'container_pattern': container_pattern,
                        'namespace_pattern': namespace_pattern
                    },
                    'metrics': {}
                }
                
                # Process each metric dynamically from config
                for metric_config in self.pods_metrics:
                    metric_name = metric_config['name']
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        if time_range:
                            metric_result = await self._collect_metric_time_range(
                                prom, metric_config, pod_pattern, container_pattern, 
                                namespace_pattern, time_range[0], time_range[1]
                            )
                        else:
                            metric_result = await self._collect_metric_duration(
                                prom, metric_config, pod_pattern, container_pattern,
                                namespace_pattern, duration_used
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
    
    async def _collect_metric_duration(self, prom: PrometheusBaseQuery, 
                                      metric_config: Dict[str, Any],
                                      pod_pattern: str,
                                      container_pattern: str,
                                      namespace_pattern: str,
                                      duration: str) -> Dict[str, Any]:
        """Collect a metric over a duration"""
        query = self._substitute_variables(
            metric_config['expr'], pod_pattern, container_pattern, namespace_pattern
        )
        metric_name = metric_config['name']
        
        try:
            result = await self._query_with_stats(prom, query, duration)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            return self._format_metric_result(result, metric_config, pod_pattern, namespace_pattern)
            
        except Exception as e:
            self.logger.error(f"Error collecting metric {metric_name}: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def _collect_metric_time_range(self, prom: PrometheusBaseQuery,
                                        metric_config: Dict[str, Any],
                                        pod_pattern: str,
                                        container_pattern: str,
                                        namespace_pattern: str,
                                        start_time: str,
                                        end_time: str) -> Dict[str, Any]:
        """Collect a metric over a specific time range"""
        query = self._substitute_variables(
            metric_config['expr'], pod_pattern, container_pattern, namespace_pattern
        )
        metric_name = metric_config['name']
        
        try:
            result = await self._query_range_with_stats(prom, query, start_time, end_time)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            return self._format_metric_result(result, metric_config, pod_pattern, namespace_pattern)
            
        except Exception as e:
            self.logger.error(f"Error collecting metric {metric_name}: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def _format_metric_result(self, result: Dict[str, Any], 
                             metric_config: Dict[str, Any],
                             pod_pattern: str,
                             namespace_pattern: str) -> Dict[str, Any]:
        """Format metric result with pod and node information"""
        metric_name = metric_config['name']
        unit = metric_config.get('unit', 'unknown')
        
        # Process results by pod and container
        pod_container_stats = {}
        
        for series in result.get('series_data', []):
            labels = series['labels']
            pod_name = labels.get('pod', 'unknown')
            container_name = labels.get('container', '')
            namespace = labels.get('namespace', 'unknown')
            
            # Skip if pod is unknown
            if pod_name == 'unknown':
                continue
            
            # Create key for pod/container combination
            if container_name and container_name not in ['', 'POD']:
                key = f"{pod_name}:{container_name}"
            else:
                key = pod_name
            
            stats = series['statistics']
            
            # Format based on unit type
            formatted_stats = self._format_stats_by_unit(stats, unit)
            
            pod_container_stats[key] = {
                'pod_name': pod_name,
                'container_name': container_name if container_name and container_name != 'POD' else None,
                'namespace': namespace,
                'stats': formatted_stats
            }
        
        # Get node mapping for pods
        node_mapping = {}
        if namespace_pattern != ".*":
            node_mapping = self.utility.get_pod_to_node_mapping_via_oc(namespace=namespace_pattern)
        else:
            # Get mapping across common namespaces
            node_mapping = self._get_global_pod_node_mapping()
        
        # Add node names to results
        for key, data in pod_container_stats.items():
            pod_name = data['pod_name']
            data['node_name'] = node_mapping.get(pod_name, 'unknown')
        
        return {
            'status': 'success',
            'metric': metric_name,
            'title': metric_config.get('title', metric_name.replace('_', ' ').title()),
            'unit': unit,
            'description': metric_config.get('description', ''),
            'pod_container_metrics': pod_container_stats,
            'total_entries': len(pod_container_stats),
            'overall_stats': self._format_stats_by_unit(result.get('overall_statistics', {}), unit)
        }
    
    def _format_stats_by_unit(self, stats: Dict[str, Any], unit: str) -> Dict[str, Any]:
        """Format statistics based on unit type"""
        if not stats:
            return {}
        
        formatted = {}
        
        if unit == 'bytes':
            # Convert bytes to appropriate unit
            if 'avg' in stats:
                formatted['avg_value'], formatted['avg_unit'] = self._format_memory_bytes(stats['avg'])
            if 'max' in stats:
                formatted['max_value'], formatted['max_unit'] = self._format_memory_bytes(stats['max'])
            if 'min' in stats:
                formatted['min_value'], formatted['min_unit'] = self._format_memory_bytes(stats['min'])
            if 'latest' in stats and stats['latest'] is not None:
                formatted['latest_value'], formatted['latest_unit'] = self._format_memory_bytes(stats['latest'])
        elif unit == 'percent':
            # Format percentages
            formatted['avg_percent'] = round(stats.get('avg', 0), 2)
            formatted['max_percent'] = round(stats.get('max', 0), 2)
            formatted['min_percent'] = round(stats.get('min', 0), 2)
            if 'latest' in stats and stats['latest'] is not None:
                formatted['latest_percent'] = round(stats['latest'], 2)
        elif unit == 'count':
            # Format counts as integers
            formatted['avg_count'] = round(stats.get('avg', 0), 0)
            formatted['max_count'] = round(stats.get('max', 0), 0)
            formatted['min_count'] = round(stats.get('min', 0), 0)
            if 'latest' in stats and stats['latest'] is not None:
                formatted['latest_count'] = round(stats['latest'], 0)
        else:
            # Default formatting
            formatted['avg_value'] = round(stats.get('avg', 0), 4)
            formatted['max_value'] = round(stats.get('max', 0), 4)
            formatted['min_value'] = round(stats.get('min', 0), 4)
            if 'latest' in stats and stats['latest'] is not None:
                formatted['latest_value'] = round(stats['latest'], 4)
        
        formatted['data_points'] = stats.get('count', 0)
        
        return formatted
    
    def _format_memory_bytes(self, bytes_value: float) -> tuple:
        """Convert bytes to appropriate unit (GB, MB, KB, B)"""
        if bytes_value >= 1024**3:  # GB
            return round(bytes_value / (1024**3), 2), 'GB'
        elif bytes_value >= 1024**2:  # MB
            return round(bytes_value / (1024**2), 2), 'MB'
        elif bytes_value >= 1024:  # KB
            return round(bytes_value / 1024, 2), 'KB'
        else:
            return round(bytes_value, 2), 'B'
    
    def _get_global_pod_node_mapping(self) -> Dict[str, str]:
        """Get pod to node mapping across multiple namespaces"""
        try:
            # Get all pods info across namespaces (uses full node names)
            all_pod_info = self.utility.get_all_pods_info_across_namespaces()
            
            # Extract just pod name -> node name mapping
            mapping = {pod: info['node_name'] for pod, info in all_pod_info.items()}
            
            return mapping
        except Exception as e:
            self.logger.warning(f"Could not get global pod-node mapping: {e}")
            return {}
    
    async def _query_with_stats(self, prom_client: PrometheusBaseQuery, 
                               query: str, duration: str) -> Dict[str, Any]:
        """Execute a range query and compute statistics per series and overall"""
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
    
    async def _query_range_with_stats(self, prom_client: PrometheusBaseQuery,
                                     query: str, start_time: str, 
                                     end_time: str) -> Dict[str, Any]:
        """Execute a range query with specific time range and compute statistics"""
        try:
            data = await prom_client.query_range(query, start_time, end_time, step='15s')

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
    
    async def get_metric_by_name(self, metric_name: str,
                                pod_pattern: str = ".*",
                                container_pattern: str = ".*",
                                namespace_pattern: str = ".*",
                                start_time: Optional[str] = None,
                                end_time: Optional[str] = None) -> Dict[str, Any]:
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
                
                # Collect metric with time range or duration
                if start_time and end_time:
                    return await self._collect_metric_time_range(
                        prom, metric_config, pod_pattern, container_pattern,
                        namespace_pattern, start_time, end_time
                    )
                else:
                    return await self._collect_metric_duration(
                        prom, metric_config, pod_pattern, container_pattern,
                        namespace_pattern, self.duration
                    )
                
        except Exception as e:
            self.logger.error(f"Error getting metric {metric_name}: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    # Individual metric collection methods
    
    async def collect_cpu_usage(self, pod_pattern: str = ".*",
                               container_pattern: str = ".*",
                               namespace_pattern: str = ".*",
                               start_time: Optional[str] = None,
                               end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect container CPU usage percentage"""
        return await self.get_metric_by_name(
            'container_cpu_usage_percent',
            pod_pattern, container_pattern, namespace_pattern,
            start_time, end_time
        )
    
    async def collect_memory_rss(self, pod_pattern: str = ".*",
                                container_pattern: str = ".*",
                                namespace_pattern: str = ".*",
                                start_time: Optional[str] = None,
                                end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect container memory RSS usage"""
        return await self.get_metric_by_name(
            'container_memory_rss_bytes',
            pod_pattern, container_pattern, namespace_pattern,
            start_time, end_time
        )
    
    async def collect_memory_working_set(self, pod_pattern: str = ".*",
                                        container_pattern: str = ".*",
                                        namespace_pattern: str = ".*",
                                        start_time: Optional[str] = None,
                                        end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect container memory working set"""
        return await self.get_metric_by_name(
            'container_memory_working_set_bytes',
            pod_pattern, container_pattern, namespace_pattern,
            start_time, end_time
        )
    
    async def collect_pod_status(self, start_time: Optional[str] = None,
                                end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect pod status by phase"""
        return await self.get_metric_by_name(
            'pod-status',
            start_time=start_time,
            end_time=end_time
        )
    
    async def collect_namespace_status(self, start_time: Optional[str] = None,
                                      end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect namespace status by phase"""
        return await self.get_metric_by_name(
            'namespace-status',
            start_time=start_time,
            end_time=end_time
        )
    
    async def collect_pod_distribution(self, start_time: Optional[str] = None,
                                      end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect pod distribution across nodes"""
        return await self.get_metric_by_name(
            'pod_distribution',
            start_time=start_time,
            end_time=end_time
        )


# Convenience functions for metric collection

async def collect_pods_metrics(ocp_auth: OCPAuth, 
                              duration: str = "1h",
                              pod_pattern: str = ".*",
                              container_pattern: str = ".*",
                              namespace_pattern: str = ".*",
                              start_time: Optional[str] = None,
                              end_time: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to collect all pods metrics"""
    collector = PodsUsageCollector(ocp_auth, duration)
    return await collector.collect_all_metrics(
        pod_pattern, container_pattern, namespace_pattern,
        start_time, end_time
    )


async def collect_specific_pod_metric(ocp_auth: OCPAuth,
                                     metric_name: str,
                                     duration: str = "1h",
                                     pod_pattern: str = ".*",
                                     container_pattern: str = ".*",
                                     namespace_pattern: str = ".*",
                                     start_time: Optional[str] = None,
                                     end_time: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to collect a specific pod metric"""
    collector = PodsUsageCollector(ocp_auth, duration)
    return await collector.get_metric_by_name(
        metric_name, pod_pattern, container_pattern, namespace_pattern,
        start_time, end_time
    )


async def collect_cpu_usage(ocp_auth: OCPAuth,
                           duration: str = "1h",
                           pod_pattern: str = ".*",
                           container_pattern: str = ".*",
                           namespace_pattern: str = ".*") -> Dict[str, Any]:
    """Convenience function to collect CPU usage"""
    collector = PodsUsageCollector(ocp_auth, duration)
    return await collector.collect_cpu_usage(
        pod_pattern, container_pattern, namespace_pattern
    )


async def collect_memory_usage(ocp_auth: OCPAuth,
                              duration: str = "1h",
                              pod_pattern: str = ".*",
                              container_pattern: str = ".*",
                              namespace_pattern: str = ".*") -> Dict[str, Any]:
    """Convenience function to collect memory working set usage"""
    collector = PodsUsageCollector(ocp_auth, duration)
    return await collector.collect_memory_working_set(
        pod_pattern, container_pattern, namespace_pattern
    )