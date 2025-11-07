"""
Prometheus Pods Usage Collector Module
Collects CPU and memory usage statistics for pods with support for instant and duration queries
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery, PrometheusQueryError
from tools.ovnk_benchmark_prometheus_utility import mcpToolsUtility
from ocauth.openshift_auth import OpenShiftAuth


class PodsUsageCollector:
    """Collects and analyzes pod usage metrics from Prometheus"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery, auth: Optional[OpenShiftAuth] = None):
        self.prometheus_client = prometheus_client
        self.auth = auth
        self.utility = mcpToolsUtility(auth_client=auth)
        
    def _get_default_queries(self, pod_pattern: str = ".*", container_pattern: str = ".*", namespace_pattern: str = ".*", interval: str = "1m") -> Dict[str, str]:
        """Get default PromQL queries for pod usage"""
        matchers = self._build_label_matchers(pod_pattern, container_pattern, namespace_pattern)
        
        # Include container in grouping if specific container pattern is used
        group_by = "pod"
        if container_pattern != ".*":
            group_by = "pod, container"
        
        return {
            'memory_usage': f'sum by({group_by}) (container_memory_rss{matchers})',
            'memory_working_set': f'sum by({group_by}) (container_memory_working_set_bytes{matchers})',
            'cpu_usage': f'sum by({group_by}) (rate(container_cpu_usage_seconds_total{matchers}[{interval}])) * 100',
            'memory_limit': f'sum by({group_by}) (container_spec_memory_limit_bytes{matchers})',
            'cpu_limit': f'sum by({group_by}) (container_spec_cpu_quota{matchers}) / sum by({group_by}) (container_spec_cpu_period{matchers}) * 100'
        }

    def _build_label_matchers(self, pod_pattern: str, container_pattern: str, namespace_pattern: str) -> str:
        """Safely build PromQL label matchers, skipping wildcards and escaping quotes."""
        def _needs_matcher(pattern: str) -> bool:
            return bool(pattern) and pattern != ".*"

        def _escape(value: str) -> str:
            return value.replace('"', '\\"')

        parts: List[str] = []
        if _needs_matcher(pod_pattern):
            parts.append(f'pod=~"{_escape(pod_pattern)}"')
        if _needs_matcher(container_pattern):
            parts.append(f'container=~"{_escape(container_pattern)}"')
        if _needs_matcher(namespace_pattern):
            parts.append(f'namespace=~"{_escape(namespace_pattern)}"')

        # Always exclude empty container and infra container
        parts.append('container!=""')
        parts.append('container!="POD"')

        return '{' + ', '.join(parts) + '}'
    
    def _get_ovn_queries(self, interval: str = "1m") -> Dict[str, str]:
        """Get OVN-specific PromQL queries aggregated by pod (no container grouping)"""
        return {
            # Memory queries for OVN pods - aggregated by pod only
            'memory_usage': 'sum by(pod) (container_memory_working_set_bytes{namespace=~"openshift-ovn-kubernetes", pod=~"ovnkube-.*", container!="", container!="POD"})',
            'memory_rss': 'sum by(pod) (container_memory_rss{namespace=~"openshift-ovn-kubernetes", pod=~"ovnkube-.*", container!="", container!="POD"})',
            
            # CPU queries for OVN pods - aggregated by pod only
            'cpu_usage': f'sum by(pod) (rate(container_cpu_usage_seconds_total{{namespace=~"openshift-ovn-kubernetes", pod=~"ovnkube-.*", container!="", container!="POD"}}[{interval}])) * 100',
            
            # Resource limits for context - aggregated by pod only
            'memory_limit': 'sum by(pod) (container_spec_memory_limit_bytes{namespace=~"openshift-ovn-kubernetes", pod=~"ovnkube-.*", container!="", container!="POD"})',
            'cpu_limit': 'sum by(pod) (container_spec_cpu_quota{namespace=~"openshift-ovn-kubernetes", pod=~"ovnkube-.*", container!="", container!="POD"}) / sum by(pod) (container_spec_cpu_period{namespace=~"openshift-ovn-kubernetes", pod=~"ovnkube-.*", container!="", container!="POD"}) * 100'
        }
    
    async def _get_pod_node_mapping(self, namespace_pattern: str = ".*") -> Dict[str, Dict[str, str]]:
        """Get pod to node and namespace mapping using utility functions as first choice"""
        try:
            # First choice: Use utility function for specific namespace
            if namespace_pattern != ".*":
                pod_info = self.utility.get_pod_full_info_via_oc(namespace=namespace_pattern)
                return pod_info
            
            # For wildcard namespace, try global query first
            try:
                all_pod_info = self.utility.get_all_pods_info_via_oc_global()
                if all_pod_info:
                    return all_pod_info
            except Exception:
                pass
            
            # Fallback: Get info across common namespaces
            all_pod_info = self.utility.get_all_pods_info_across_namespaces()
            return all_pod_info
            
        except Exception as e:
            print(f"Warning: Could not get pod-node mapping: {e}")
            return {}
    
    def _extract_identifiers_from_metric(self, metric: Dict[str, Any]) -> Tuple[str, str, str]:
        """Extract pod name, container name, and namespace from Prometheus metric labels"""
        pod_name = 'unknown'
        container_name = 'unknown'
        namespace = 'unknown'
        
        if 'metric' in metric:
            pod_name = metric['metric'].get('pod', 'unknown')
            container_name = metric['metric'].get('container', 'unknown')
            namespace = metric['metric'].get('namespace', 'unknown')
        
        return pod_name, container_name, namespace
    
    def _calculate_stats(self, values: List[float]) -> Dict[str, float]:
        """Calculate min, max, avg from a list of values"""
        if not values:
            return {'min': 0.0, 'max': 0.0, 'avg': 0.0}
        
        filtered_values = [v for v in values if v is not None and v >= 0]
        if not filtered_values:
            return {'min': 0.0, 'max': 0.0, 'avg': 0.0}
        
        return {
            'min': min(filtered_values),
            'max': max(filtered_values),
            'avg': sum(filtered_values) / len(filtered_values)
        }
    
    def _format_memory_bytes(self, bytes_value: float) -> Tuple[float, str]:
        """Format memory bytes to appropriate unit"""
        if bytes_value >= 1024**3:  # GB
            return round(bytes_value / (1024**3), 2), 'GB'
        elif bytes_value >= 1024**2:  # MB
            return round(bytes_value / (1024**2), 2), 'MB'
        elif bytes_value >= 1024:  # KB
            return round(bytes_value / 1024, 2), 'KB'
        else:
            return round(bytes_value, 2), 'B'
    
    async def collect_instant_usage(self, 
                                   pod_pattern: str = ".*", 
                                   container_pattern: str = ".*",
                                   namespace_pattern: str = ".*",
                                   custom_queries: Optional[Dict[str, str]] = None,
                                   use_ovn_queries: bool = False,
                                   time: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect instant pod usage metrics
        
        Args:
            pod_pattern: Regular expression pattern for pod names
            container_pattern: Regular expression pattern for container names  
            namespace_pattern: Regular expression pattern for namespace names
            custom_queries: Custom PromQL queries dictionary
            use_ovn_queries: Use predefined OVN queries (aggregated by pod, no containers)
            time: Optional specific timestamp (UTC)
            
        Returns:
            Dictionary containing usage summary and separate top 5 pods for CPU and memory
        """
        try:
            # Determine queries to use
            if custom_queries:
                queries = custom_queries
                include_containers = container_pattern != ".*"
            elif use_ovn_queries:
                queries = self._get_ovn_queries()
                include_containers = False  # Always aggregate by pod for OVN queries
            else:
                queries = self._get_default_queries(pod_pattern, container_pattern, namespace_pattern)
                include_containers = container_pattern != ".*"
            
            # Execute queries
            results = await self.prometheus_client.query_multiple_instant(queries, time)
            
            # Get pod-node mapping using utility as first choice
            pod_info_mapping = await self._get_pod_node_mapping(namespace_pattern)
            
            # Process results - group by pod and container if needed
            usage_data = {}
            
            for query_name, result in results.items():
                if 'error' in result:
                    print(f"Warning: Query {query_name} failed: {result['error']}")
                    continue
                
                if 'result' not in result:
                    continue
                
                for metric in result['result']:
                    pod_name, container_name, metric_namespace = self._extract_identifiers_from_metric(metric)
                    
                    # Create unique key for pod/container combination
                    if include_containers:
                        key = f"{pod_name}:{container_name}"
                    else:
                        key = pod_name
                    
                    if key not in usage_data:
                        # Get node name and namespace from mapping first, then fallback to metric
                        pod_info = pod_info_mapping.get(pod_name, {})
                        node_name = pod_info.get('node_name', 'unknown')
                        namespace = pod_info.get('namespace', metric_namespace if metric_namespace != 'unknown' else 'unknown')
                        
                        usage_data[key] = {
                            'pod_name': pod_name,
                            'node_name': node_name,
                            'namespace': namespace,
                            'metrics': {}
                        }
                        
                        if include_containers:
                            usage_data[key]['container_name'] = container_name
                    
                    # Extract value
                    if 'value' in metric:
                        timestamp, value = metric['value']
                        try:
                            numeric_value = float(value) if value != 'NaN' else 0.0
                        except (ValueError, TypeError):
                            numeric_value = 0.0
                        
                        usage_data[key]['metrics'][query_name] = {
                            'value': numeric_value,
                            'timestamp': float(timestamp)
                        }
            
            # Create summary with separate top 5 rankings
            summary = self._create_usage_summary(usage_data, is_instant=True, include_containers=include_containers)
            
            return summary
            
        except Exception as e:
            raise PrometheusQueryError(f"Failed to collect instant usage: {str(e)}")
    
    async def collect_duration_usage(self,
                                   duration: str,
                                   pod_pattern: str = ".*",
                                   container_pattern: str = ".*", 
                                   namespace_pattern: str = ".*",
                                   custom_queries: Optional[Dict[str, str]] = None,
                                   use_ovn_queries: bool = False,
                                   step: str = "15s",
                                   end_time: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect pod usage metrics over a duration
        
        Args:
            duration: Duration string (e.g., '5m', '1h', '1d')
            pod_pattern: Regular expression pattern for pod names
            container_pattern: Regular expression pattern for container names
            namespace_pattern: Regular expression pattern for namespace names  
            custom_queries: Custom PromQL queries dictionary
            use_ovn_queries: Use predefined OVN queries (aggregated by pod, no containers)
            step: Query resolution step
            end_time: Optional end time (UTC)
            
        Returns:
            Dictionary containing usage summary and separate top 5 pods for CPU and memory
        """
        try:
            # Get time range
            start_time, actual_end_time = self.prometheus_client.get_time_range_from_duration(duration, end_time)
            
            # Determine queries to use with dynamic rate window
            if custom_queries:
                queries = custom_queries
                include_containers = container_pattern != ".*"
            elif use_ovn_queries:
                rate_window = self._select_rate_window(duration)
                queries = self._get_ovn_queries(interval=rate_window)
                include_containers = False  # Always aggregate by pod for OVN queries
            else:
                rate_window = self._select_rate_window(duration)
                queries = self._get_default_queries(pod_pattern, container_pattern, namespace_pattern, interval=rate_window)
                include_containers = container_pattern != ".*"
            
            # Override include_containers if container_pattern is wildcard
            if container_pattern == ".*":
                include_containers = False
            
            # Execute range queries
            results = await self.prometheus_client.query_multiple_range(queries, start_time, actual_end_time, step)
            
            # Get pod-node mapping using utility as first choice
            pod_info_mapping = await self._get_pod_node_mapping(namespace_pattern)
            
            # Process results - group by pod and container if needed
            usage_data = {}
            
            for query_name, result in results.items():
                if 'error' in result:
                    print(f"Warning: Query {query_name} failed: {result['error']}")
                    continue
                
                if 'result' not in result:
                    continue
                
                for metric in result['result']:
                    pod_name, container_name, metric_namespace = self._extract_identifiers_from_metric(metric)
                    
                    # Create unique key for pod/container combination
                    if include_containers:
                        key = f"{pod_name}:{container_name}"
                    else:
                        key = pod_name
                    
                    if key not in usage_data:
                        # Get node name and namespace from mapping first
                        pod_info = pod_info_mapping.get(pod_name, {})
                        node_name = pod_info.get('node_name', 'unknown')
                        namespace = pod_info.get('namespace', metric_namespace)
                        
                        usage_data[key] = {
                            'pod_name': pod_name,
                            'node_name': node_name,
                            'namespace': namespace,
                            'metrics': {}
                        }
                        
                        if include_containers:
                            usage_data[key]['container_name'] = container_name
                    
                    # Extract time series values
                    if 'values' in metric:
                        values = []
                        timestamps = []
                        
                        for timestamp, value in metric['values']:
                            try:
                                numeric_value = float(value) if value != 'NaN' else None
                                if numeric_value is not None and numeric_value >= 0:
                                    values.append(numeric_value)
                                    timestamps.append(float(timestamp))
                            except (ValueError, TypeError):
                                continue
                        
                        if values:
                            stats = self._calculate_stats(values)
                            usage_data[key]['metrics'][query_name] = {
                                'min': stats['min'],
                                'max': stats['max'],
                                'avg': stats['avg'],
                                'data_points': len(values),
                                'start_time': min(timestamps) if timestamps else None,
                                'end_time': max(timestamps) if timestamps else None
                            }
            
            # Create summary with separate top 5 rankings
            summary = self._create_usage_summary(usage_data, is_instant=False, include_containers=include_containers)
            summary['query_info'] = {
                'duration': duration,
                'start_time': start_time,
                'end_time': actual_end_time,
                'step': step
            }
            
            return summary
            
        except Exception as e:
            raise PrometheusQueryError(f"Failed to collect duration usage: {str(e)}")

    def _select_rate_window(self, duration: str) -> str:
        """Select a rate/irate window based on overall query duration to match Grafana-like smoothing."""
        try:
            td = self.prometheus_client.parse_duration(duration)
            total_seconds = td.total_seconds()
            if total_seconds <= 600:  # <= 10m
                return "30s"
            if total_seconds <= 3600:  # <= 1h
                return "1m"
            if total_seconds <= 21600:  # <= 6h
                return "2m"
            if total_seconds <= 43200:  # <= 12h
                return "5m"
            if total_seconds <= 86400:  # <= 24h
                return "10m"
            return "15m"
        except Exception:
            return "1m"
    
    def _create_usage_summary(self, usage_data: Dict[str, Any], is_instant: bool, include_containers: bool = False) -> Dict[str, Any]:
        """Create usage summary from collected usage data with separate top 5 CPU and memory rankings"""
        
        # Prepare entries for ranking
        entries = []
        
        for key, data in usage_data.items():
            metrics = data.get('metrics', {})
            
            # Get CPU usage (try different metric names)
            cpu_usage = 0.0
            for cpu_metric in ['cpu_usage', 'cpu_ovnkube_node_pods', 'cpu-ovnkube-node-pods']:
                if cpu_metric in metrics:
                    if is_instant:
                        cpu_usage = metrics[cpu_metric].get('value', 0.0)
                    else:
                        cpu_usage = metrics[cpu_metric].get('avg', 0.0)
                    break
            
            # Get memory usage (try different metric names, prioritize working set for OVN) 
            memory_usage = 0.0
            # For OVN queries, prioritize memory_usage (working_set) over memory_rss
            memory_metrics_priority = ['memory_usage', 'memory_working_set', 'memory_rss', 'memory_ovnkube_node_pods', 'memory-ovnkube-node-pods']
            for mem_metric in memory_metrics_priority:
                if mem_metric in metrics:
                    if is_instant:
                        memory_usage = metrics[mem_metric].get('value', 0.0)
                    else:
                        memory_usage = metrics[mem_metric].get('avg', 0.0)
                    break
            
            entry = {
                'pod_name': data['pod_name'],
                'node_name': data['node_name'],
                'namespace': data['namespace'],
                'cpu_usage': cpu_usage,
                'memory_usage': memory_usage,
                'data': data
            }
            
            if include_containers:
                entry['container_name'] = data.get('container_name', 'unknown')
            
            entries.append(entry)
        
        # Create separate top 5 rankings for CPU and memory
        cpu_rankings = sorted(entries, key=lambda x: x['cpu_usage'], reverse=True)[:5]
        memory_rankings = sorted(entries, key=lambda x: x['memory_usage'], reverse=True)[:5]
        
        # Create formatted summary with UTC timezone
        summary = {
            'collection_timestamp': datetime.now(timezone.utc).isoformat(),
            'collection_type': 'instant' if is_instant else 'duration',
            'total_analyzed': len(usage_data),
            'include_containers': include_containers,
            'top_5_cpu_usage': [],
            'top_5_memory_usage': []
        }
        
        # Format CPU top 5 usage
        for rank, entry in enumerate(cpu_rankings, 1):
            cpu_summary = self._format_usage_entry(entry, rank, is_instant, include_containers, focus='cpu')
            summary['top_5_cpu_usage'].append(cpu_summary)
        
        # Format Memory top 5 usage
        for rank, entry in enumerate(memory_rankings, 1):
            memory_summary = self._format_usage_entry(entry, rank, is_instant, include_containers, focus='memory')
            summary['top_5_memory_usage'].append(memory_summary)
        
        return summary
    
    def _format_usage_entry(self, entry: Dict[str, Any], rank: int, is_instant: bool, include_containers: bool, focus: str = 'all') -> Dict[str, Any]:
        """Format a single usage entry for the summary (max 5 levels deep)"""
        data = entry['data']
        
        usage_summary = {
            'rank': rank,
            'pod_name': entry['pod_name'],
            'node_name': entry['node_name'],
            'namespace': entry['namespace']
        }
        
        if include_containers:
            usage_summary['container_name'] = entry.get('container_name', 'unknown')
        
        # Format metrics based on focus (keep within 5 level limit)
        usage_summary['metrics'] = {}
        for metric_name, metric_data in data.get('metrics', {}).items():
            # Filter metrics based on focus
            if focus == 'cpu' and 'cpu' not in metric_name.lower():
                continue
            elif focus == 'memory' and 'memory' not in metric_name.lower():
                continue
            
            if 'cpu' in metric_name.lower():
                if is_instant:
                    usage_summary['metrics'][metric_name] = {
                        'value': round(metric_data.get('value', 0.0), 2),
                        'unit': '%'
                    }
                else:
                    usage_summary['metrics'][metric_name] = {
                        'min': round(metric_data.get('min', 0.0), 2),
                        'avg': round(metric_data.get('avg', 0.0), 2),
                        'max': round(metric_data.get('max', 0.0), 2),
                        'unit': '%'
                    }
                    
            elif 'memory' in metric_name.lower():
                if is_instant:
                    value = metric_data.get('value', 0.0)
                    formatted_value, unit = self._format_memory_bytes(value)
                    usage_summary['metrics'][metric_name] = {
                        'value': formatted_value,
                        'unit': unit
                    }
                else:
                    min_val, min_unit = self._format_memory_bytes(metric_data.get('min', 0.0))
                    avg_val, avg_unit = self._format_memory_bytes(metric_data.get('avg', 0.0))
                    max_val, max_unit = self._format_memory_bytes(metric_data.get('max', 0.0))
                    
                    usage_summary['metrics'][metric_name] = {
                        'min': min_val,
                        'avg': avg_val, 
                        'max': max_val,
                        'unit': max_unit
                    }
        
        return usage_summary


# Example usage functions
async def collect_ovn_instant_usage(prometheus_client: PrometheusBaseQuery, auth: Optional[OpenShiftAuth] = None) -> Dict[str, Any]:
    """Collect instant OVN pod usage"""
    collector = PodsUsageCollector(prometheus_client, auth)
    return await collector.collect_instant_usage(use_ovn_queries=True)


async def collect_ovn_duration_usage(prometheus_client: PrometheusBaseQuery, duration: str = "1h", auth: Optional[OpenShiftAuth] = None) -> Dict[str, Any]:
    """Collect OVN pod usage over duration"""
    collector = PodsUsageCollector(prometheus_client, auth)
    return await collector.collect_duration_usage(duration=duration, use_ovn_queries=True)


async def collect_custom_usage(prometheus_client: PrometheusBaseQuery, 
                             queries: Dict[str, str],
                             duration: Optional[str] = None,
                             auth: Optional[OpenShiftAuth] = None) -> Dict[str, Any]:
    """Collect usage with custom queries"""
    collector = PodsUsageCollector(prometheus_client, auth)
    
    if duration:
        return await collector.collect_duration_usage(duration=duration, custom_queries=queries)
    else:
        return await collector.collect_instant_usage(custom_queries=queries)