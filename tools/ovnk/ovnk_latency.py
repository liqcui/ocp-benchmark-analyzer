#!/usr/bin/env python3
"""
Enhanced OVN-Kubernetes Latency Collector - Refactored Version
Collects and analyzes latency metrics from OVN-Kubernetes components with improved structure
"""

import os
import json
import yaml
import asyncio
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

from tools.ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery, PrometheusQueryError
from tools.ovnk_benchmark_prometheus_utility import mcpToolsUtility
from ocauth.openshift_auth import auth


class OVNLatencyCollector:
    """Enhanced collector for OVN-Kubernetes latency metrics with improved structure"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery):
        self.prometheus_client = prometheus_client
        self.auth = auth
        self.utility = mcpToolsUtility(auth_client=auth)
        
        # Cache for pod-to-node mappings
        self._pod_node_cache = {}
        self._cache_timestamp = None
        self._cache_expiry_minutes = 10
        
        # Keep original default metrics configuration
        self.default_metrics = [
            {
                'query': 'topk(10, ovnkube_controller_ready_duration_seconds)',
                'metricName': 'ovnkube_controller_ready_duration_seconds',
                'unit': 'seconds',
                'type': 'ready_duration',
                'component': 'controller'
            },
            {
                'query': 'topk(10, ovnkube_node_ready_duration_seconds)',
                'metricName': 'ovnkube_node_ready_duration_seconds', 
                'unit': 'seconds',
                'type': 'ready_duration',
                'component': 'node'
            },
            {
                'query': 'topk(20, ovnkube_controller_sync_duration_seconds)', 
                'metricName': 'ovnkube_controller_sync_duration_seconds',
                'unit': 'seconds',
                'type': 'sync_duration',
                'component': 'controller'
            }
        ]
        
        # Enhanced metrics configuration with new queries (removed duplicates)
        self.extended_metrics = [
            # CNI queries
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_node_cni_request_duration_seconds_bucket{command="ADD"}[2m])) > 0)',
                'metricName': 'cni_request_add_latency_p99',
                'unit': 'seconds',
                'type': 'cni_latency',
                'component': 'node'
            },
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_node_cni_request_duration_seconds_bucket{command="DEL"}[2m])) > 0)',
                'metricName': 'cni_request_del_latency_p99',
                'unit': 'seconds',
                'type': 'cni_latency',
                'component': 'node'
            },
            
            # Pod creation latency metrics (updated with new PromQL)
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_controller_pod_creation_latency_seconds_bucket[2m]))) > 0',
                'metricName': 'pod_annotation_latency_p99',
                'unit': 'seconds',
                'type': 'pod_annotation_latency',
                'component': 'controller'
            },
            {
                'query': 'histogram_quantile(0.99, sum by(pod, le) (rate(ovnkube_controller_pod_lsp_created_port_binding_duration_seconds_bucket[2m])))',
                'metricName': 'pod_lsp_created_p99',
                'unit': 'seconds',
                'type': 'pod_creation_latency',
                'component': 'controller'
            },
            {
                'query': 'histogram_quantile(0.99, sum by(pod, le) (rate(ovnkube_controller_pod_port_binding_port_binding_chassis_duration_seconds_bucket[2m])))',
                'metricName': 'pod_port_binding_p99',
                'unit': 'seconds',
                'type': 'pod_creation_latency',
                'component': 'controller'
            },
            {
                'query': 'histogram_quantile(0.99, sum by(pod, le) (rate(ovnkube_controller_pod_port_binding_chassis_port_binding_up_duration_seconds_bucket[2m])))',
                'metricName': 'pod_port_binding_up_p99',
                'unit': 'seconds',
                'type': 'pod_creation_latency',
                'component': 'controller'
            },
            {
                'query': 'histogram_quantile(0.99, sum by(pod, le) (rate(ovnkube_controller_pod_first_seen_lsp_created_duration_seconds_bucket[2m])))',
                'metricName': 'pod_first_seen_lsp_created_p99',
                'unit': 'seconds',
                'type': 'pod_creation_latency',
                'component': 'controller'
            },
            
            # Service latency metrics
            {
                'query': 'sum by (pod) (rate(ovnkube_controller_sync_service_latency_seconds_sum[2m])) / sum by (pod) (rate(ovnkube_controller_sync_service_latency_seconds_count[2m]))',
                'metricName': 'sync_service_latency',
                'unit': 'seconds',
                'type': 'service_latency',
                'component': 'controller'
            },
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_controller_sync_service_latency_seconds_bucket[2m])) > 0)',
                'metricName': 'sync_service_latency_p99',
                'unit': 'seconds',
                'type': 'service_latency',
                'component': 'controller'
            },
            
            # Network configuration application metrics (renamed from network_programming)
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_controller_network_programming_duration_seconds_bucket[2m])) > 0)',
                'metricName': 'apply_network_config_pod_duration_p99',
                'unit': 'seconds',
                'type': 'apply_network_configuration',
                'component': 'controller'
            },
            {
                'query': 'histogram_quantile(0.99, sum by (pod, le) (rate(ovnkube_controller_network_programming_service_duration_seconds_bucket[2m])) > 0)',
                'metricName': 'apply_network_config_service_duration_p99',
                'unit': 'seconds',
                'type': 'apply_network_configuration',
                'component': 'controller'
            }
        ]
        
        self.metrics_config = []
        self._load_metrics_config()
    
    def _load_metrics_config(self) -> None:
        """Load metrics configuration from metrics-latency.yml or use defaults"""
        metrics_file = Path('metrics-latency.yml')
        
        # Start with existing default metrics
        self.metrics_config = self.default_metrics.copy()
        
        if metrics_file.exists():
            try:
                with open(metrics_file, 'r') as f:
                    config = yaml.safe_load(f)
                    yaml_metrics = config.get('metrics', [])
                    
                    # Process YAML metrics and replace $interval placeholder
                    processed_yaml_metrics = []
                    for metric in yaml_metrics:
                        processed_metric = metric.copy()
                        if '$interval' in processed_metric.get('query', ''):
                            processed_metric['query'] = processed_metric['query'].replace('$interval', '2m')
                        
                        # Set default values if not specified
                        processed_metric.setdefault('type', 'extended_latency')
                        processed_metric.setdefault('component', 'unknown')
                        processed_metric.setdefault('unit', 'seconds')
                        
                        processed_yaml_metrics.append(processed_metric)
                    
                    # Add YAML metrics to existing config
                    self.metrics_config.extend(processed_yaml_metrics)
                    print(f"Loaded {len(yaml_metrics)} additional metrics from metrics-latency.yml")
            except Exception as e:
                print(f"Failed to load metrics-latency.yml: {e}")
                print("Using hardcoded extended metrics as fallback")
                self.metrics_config.extend(self.extended_metrics)
        else:
            print("metrics-latency.yml not found, using hardcoded extended metrics")
            self.metrics_config.extend(self.extended_metrics)
    
    async def _refresh_pod_node_cache(self) -> None:
        """Refresh the pod-to-node mapping cache"""
        current_time = datetime.now()
        
        if (self._cache_timestamp and 
            self._pod_node_cache and 
            (current_time - self._cache_timestamp).total_seconds() < (self._cache_expiry_minutes * 60)):
            return
        
        print("Refreshing pod-to-node mapping cache...")
        
        try:
            namespaces = [
                'openshift-ovn-kubernetes', 
                'openshift-multus', 
                'kube-system', 
                'default',
                'openshift-monitoring',
                'openshift-network-operator'
            ]
            
            all_pod_info = self.utility.get_all_pods_info_across_namespaces(namespaces)
            
            self._pod_node_cache = {}
            
            for pod_name, info in all_pod_info.items():
                node_name = info.get('node_name', 'unknown')
                namespace = info.get('namespace', 'unknown')
                
                lookup_keys = [pod_name]
                
                # Add short name patterns
                if '-' in pod_name:
                    parts = pod_name.split('-')
                    if len(parts) >= 2:
                        base_name = '-'.join(parts[:2])
                        lookup_keys.append(base_name)
                        suffix = parts[-1]
                        lookup_keys.append(f"{base_name}-{suffix}")
                
                # Add namespace-qualified keys
                lookup_keys.extend([
                    f"{namespace}/{pod_name}",
                    f"{pod_name}.{namespace}"
                ])
                
                for key in lookup_keys:
                    if key:
                        self._pod_node_cache[key] = {
                            'node_name': node_name,
                            'namespace': namespace,
                            'full_pod_name': pod_name
                        }
            
            self._cache_timestamp = current_time
            print(f"Pod-to-node cache refreshed with {len(all_pod_info)} pods")
            
        except Exception as e:
            print(f"Failed to refresh pod-node cache: {e}")
    
    def _extract_pod_name_from_labels(self, labels: Dict[str, str]) -> str:
        """Extract pod name from metric labels"""
        pod_name_candidates = [
            labels.get('pod'),
            labels.get('kubernetes_pod_name'), 
            labels.get('pod_name'),
            labels.get('exported_pod'),
        ]
        
        for candidate in pod_name_candidates:
            if candidate and candidate != 'unknown' and candidate.strip():
                return candidate.strip()
        
        # Extract from instance label
        instance = labels.get('instance', '')
        if instance and ':' in instance:
            pod_part = instance.split(':')[0]
            if not re.match(r'^\d+\.\d+\.\d+\.\d+$', pod_part) and pod_part.strip():
                return pod_part.strip()
        
        # Extract from job label if it contains pod info
        job = labels.get('job', '')
        if job and 'ovnkube' in job:
            return job.strip()
        
        return 'unknown'
    
    def _find_node_name_for_pod(self, pod_name: str) -> str:
        """Find node name for a given pod using the cache"""
        if not pod_name or pod_name == 'unknown':
            return 'unknown'
        
        if pod_name in self._pod_node_cache:
            return self._pod_node_cache[pod_name]['node_name']
        
        # Fuzzy matching for OVN pods
        search_patterns = [pod_name]
        
        if 'ovnkube' in pod_name:
            if 'controller' in pod_name:
                search_patterns.extend(['ovnkube-controller'])
            elif 'node' in pod_name:
                search_patterns.extend(['ovnkube-node'])
                if '-' in pod_name:
                    parts = pod_name.split('-')
                    if len(parts) >= 3:
                        search_patterns.append(f"ovnkube-node-{parts[-1]}")
            elif 'master' in pod_name:
                search_patterns.extend(['ovnkube-master'])
        
        for pattern in search_patterns:
            if pattern in self._pod_node_cache:
                return self._pod_node_cache[pattern]['node_name']
        
        # Partial matching as last resort
        for cached_key, cached_info in self._pod_node_cache.items():
            if (pod_name in cached_key or 
                cached_key in pod_name or 
                (len(pod_name) > 5 and pod_name[:10] in cached_key)):
                return cached_info['node_name']
        
        return 'unknown'
    
    def _convert_duration_to_readable(self, seconds: float) -> Dict[str, Any]:
        """Convert duration in seconds to readable format"""
        if seconds == 0:
            return {'value': 0, 'unit': 'ms'}
        elif seconds < 1:
            return {'value': round(seconds * 1000, 2), 'unit': 'ms'}
        elif seconds < 60:
            return {'value': round(seconds, 3), 'unit': 's'}
        elif seconds < 3600:
            return {'value': round(seconds / 60, 2), 'unit': 'min'}
        else:
            return {'value': round(seconds / 3600, 2), 'unit': 'h'}
    
    def _extract_resource_info_from_labels(self, labels: Dict[str, str]) -> Dict[str, str]:
        """Extract resource information from metric labels"""
        resource_info = {
            'resource_name': 'unknown',
            'resource_namespace': 'unknown'
        }
        
        # Common label patterns for resource name
        if 'name' in labels:
            resource_info['resource_name'] = labels['name']
        elif 'resource_name' in labels:
            resource_info['resource_name'] = labels['resource_name']
        elif 'object_name' in labels:
            resource_info['resource_name'] = labels['object_name']
        elif 'service' in labels:
            resource_info['resource_name'] = labels['service']
        elif 'resource' in labels:
            resource_info['resource_name'] = labels['resource']
        
        # For controller sync metrics, try to extract resource type
        if 'resource' in labels or 'kind' in labels:
            resource_type = labels.get('resource', labels.get('kind', 'unknown'))
            if resource_type != 'unknown':
                resource_info['resource_name'] = resource_type
        
        # Special case for sync duration metrics
        if resource_info['resource_name'] == 'unknown' and any('sync' in k.lower() for k in labels.keys()):
            resource_info['resource_name'] = 'all watchers'
        
        # Namespace extraction
        if 'namespace' in labels:
            resource_info['resource_namespace'] = labels['namespace']
        elif 'object_namespace' in labels:
            resource_info['resource_namespace'] = labels['object_namespace']
        
        return resource_info
    
    def _process_metric_data(self, formatted_result: List[Dict], metric_config: Dict) -> List[Dict[str, Any]]:
        """Process metric data points with pod and node name resolution"""
        processed_data = []
        metric_name = metric_config['metricName']
        unit = metric_config.get('unit', 'seconds')
        
        # Check if this needs resource_name for sync duration metrics
        include_resource_name = 'sync_duration' in metric_name or metric_name == 'ovnkube_controller_sync_duration_seconds'
        
        for item in formatted_result:
            value = item.get('value')
            
            if value is None or (isinstance(value, (int, float)) and value <= 0):
                continue
            
            labels = item.get('labels', {})
            pod_name = self._extract_pod_name_from_labels(labels)
            node_name = self._find_node_name_for_pod(pod_name)
            
            data_point = {
                'pod_name': pod_name,
                'node_name': node_name,
                'value': value,
                'readable_value': self._convert_duration_to_readable(value) if unit == 'seconds' else {'value': round(value, 4), 'unit': unit}
            }
            
            # For service metrics, use service name as identifier
            if 'service' in labels and metric_config.get('type') == 'apply_network_configuration':
                service_name = labels['service']
                data_point.update({
                    'service_name': service_name,
                    'pod_name': 'N/A',
                    'node_name': 'N/A'
                })
            
            # Add resource_name only for sync duration metrics
            if include_resource_name:
                resource_info = self._extract_resource_info_from_labels(labels)
                data_point['resource_name'] = resource_info.get('resource_name', 'all watchers')
            
            processed_data.append(data_point)
        
        return processed_data
    
    def _calculate_statistics(self, data_points: List[Dict], metric_config: Dict) -> Dict[str, Any]:
        """Calculate statistics for data points"""
        if not data_points:
            return {'count': 0}
        
        values = [dp['value'] for dp in data_points if dp.get('value') is not None and dp.get('value') > 0]
        if not values:
            return {'count': 0}
        
        sorted_data = sorted(data_points, key=lambda x: x.get('value', 0), reverse=True)
        
        max_value = max(values)
        avg_value = sum(values) / len(values)
        
        # Get top entries based on metric type
        if metric_config.get('type') == 'sync_duration' and metric_config.get('metricName') == 'ovnkube_controller_sync_duration_seconds':
            top_entries = sorted_data[:20]
            top_key = 'top_20'
        else:
            top_entries = sorted_data[:5]
            top_key = 'top_5'
        
        return {
            'count': len(data_points),
            'max_value': max_value,
            'avg_value': avg_value,
            'readable_max': self._convert_duration_to_readable(max_value),
            'readable_avg': self._convert_duration_to_readable(avg_value),
            top_key: top_entries
        }
    
    async def _collect_metric(self, metric_name: str, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Generic method to collect any configured metric"""
        metric_config = next((m for m in self.metrics_config if m['metricName'] == metric_name), None)
        if not metric_config:
            return {'error': f'{metric_name} metric not configured'}
        
        await self._refresh_pod_node_cache()
        
        try:
            query = metric_config['query']
            print(f"Executing query for {metric_name}: {query}")
            
            if duration:
                start_time, end_time_actual = self.prometheus_client.get_time_range_from_duration(duration, end_time)
                result = await self.prometheus_client.query_range(query, start_time, end_time_actual, step='30s')
                formatted_result = self.prometheus_client.format_range_query_result(result)
            else:
                result = await self.prometheus_client.query_instant(query, time)
                formatted_result = self.prometheus_client.format_query_result(result)
            
            if not formatted_result:
                return {
                    'metric_name': metric_config['metricName'],
                    'component': metric_config.get('component', 'unknown'),
                    'unit': metric_config.get('unit', 'seconds'),
                    'statistics': {'count': 0},
                    'query_type': 'duration' if duration else 'instant'
                }
            
            valid_data = [item for item in formatted_result if item.get('value') is not None and item.get('value') != 0]
            if not valid_data:
                return {
                    'metric_name': metric_config['metricName'],
                    'component': metric_config.get('component', 'unknown'),
                    'unit': metric_config.get('unit', 'seconds'),
                    'statistics': {'count': 0},
                    'query_type': 'duration' if duration else 'instant'
                }
            
            processed_data = self._process_metric_data(formatted_result, metric_config)
            stats = self._calculate_statistics(processed_data, metric_config)
            
            return {
                'metric_name': metric_config['metricName'],
                'component': metric_config.get('component', 'unknown'),
                'unit': metric_config.get('unit', 'seconds'),
                'statistics': stats,
                'query_type': 'duration' if duration else 'instant'
            }
        except Exception as e:
            print(f"Error collecting metric {metric_name}: {str(e)}")
            return {'error': str(e)}
    
    # Individual metric collection functions for default metrics
    async def collect_controller_ready_duration(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect controller ready duration metrics"""
        return await self._collect_metric('ovnkube_controller_ready_duration_seconds', time, duration, end_time)
    
    async def collect_node_ready_duration(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect node ready duration metrics"""
        return await self._collect_metric('ovnkube_node_ready_duration_seconds', time, duration, end_time)
    
    async def collect_controller_sync_duration(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect controller sync duration metrics"""
        return await self._collect_metric('ovnkube_controller_sync_duration_seconds', time, duration, end_time)
    
    # Individual metric collection functions for extended metrics
    async def collect_cni_request_add_latency_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect CNI ADD request latency 99th percentile metrics"""
        return await self._collect_metric('cni_request_add_latency_p99', time, duration, end_time)
    
    async def collect_cni_request_del_latency_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect CNI DEL request latency 99th percentile metrics"""
        return await self._collect_metric('cni_request_del_latency_p99', time, duration, end_time)
    
    async def collect_pod_annotation_latency_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect pod annotation latency 99th percentile metrics"""
        return await self._collect_metric('pod_annotation_latency_p99', time, duration, end_time)
    
    async def collect_pod_lsp_created_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect pod LSP created latency 99th percentile metrics"""
        return await self._collect_metric('pod_lsp_created_p99', time, duration, end_time)
    
    async def collect_pod_port_binding_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect pod port binding latency 99th percentile metrics"""
        return await self._collect_metric('pod_port_binding_p99', time, duration, end_time)
    
    async def collect_pod_port_binding_up_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect pod port binding up latency 99th percentile metrics"""
        return await self._collect_metric('pod_port_binding_up_p99', time, duration, end_time)
    
    async def collect_pod_first_seen_lsp_created_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect pod first seen LSP created latency 99th percentile metrics"""
        return await self._collect_metric('pod_first_seen_lsp_created_p99', time, duration, end_time)
    
    async def collect_sync_service_latency(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect sync service latency metrics"""
        return await self._collect_metric('sync_service_latency', time, duration, end_time)
    
    async def collect_sync_service_latency_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect sync service latency 99th percentile metrics"""
        return await self._collect_metric('sync_service_latency_p99', time, duration, end_time)
    
    async def collect_apply_network_config_pod_duration_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect apply network config pod duration 99th percentile metrics"""
        return await self._collect_metric('apply_network_config_pod_duration_p99', time, duration, end_time)
    
    async def collect_apply_network_config_service_duration_p99(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
        """Collect apply network config service duration 99th percentile metrics"""
        return await self._collect_metric('apply_network_config_service_duration_p99', time, duration, end_time)
    
    async def collect_comprehensive_latency_metrics(self, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None, include_controller_metrics: bool = True, include_node_metrics: bool = True, include_extended_metrics: bool = True) -> Dict[str, Any]:
        """Collect all OVN latency metrics comprehensively"""
        print(f"Collecting comprehensive OVN latency metrics... (Query type: {'duration' if duration else 'instant'})")
        
        results = {
            'collection_timestamp': time or datetime.now(timezone.utc).isoformat(),
            'timezone': 'UTC',
            'query_type': 'duration' if duration else 'instant',
            'ready_duration_metrics': {},
            'sync_duration_metrics': {},
            'cni_latency_metrics': {},
            'pod_annotation_metrics': {},
            'pod_creation_metrics': {},
            'service_latency_metrics': {},
            'network_config_metrics': {},
            'summary': {}
        }
        
        if duration:
            start_time, end_time_actual = self.prometheus_client.get_time_range_from_duration(duration, end_time)
            results['query_parameters'] = {'duration': duration, 'start_time': start_time, 'end_time': end_time_actual}
        
        try:
            # Define metric collection tasks
            all_metric_tasks = []
            
            if include_controller_metrics:
                all_metric_tasks.extend([
                    ('controller_ready_duration', 'ready_duration_metrics', self.collect_controller_ready_duration(time, duration, end_time)),
                    ('controller_sync_duration', 'sync_duration_metrics', self.collect_controller_sync_duration(time, duration, end_time)),
                ])
            
            if include_node_metrics:
                all_metric_tasks.extend([
                    ('node_ready_duration', 'ready_duration_metrics', self.collect_node_ready_duration(time, duration, end_time)),
                ])
            
            if include_extended_metrics:
                all_metric_tasks.extend([
                    ('cni_add_latency_p99', 'cni_latency_metrics', self.collect_cni_request_add_latency_p99(time, duration, end_time)),
                    ('cni_del_latency_p99', 'cni_latency_metrics', self.collect_cni_request_del_latency_p99(time, duration, end_time)),
                    
                    ('pod_annotation_latency_p99', 'pod_annotation_metrics', self.collect_pod_annotation_latency_p99(time, duration, end_time)),
                    
                    ('pod_lsp_created_p99', 'pod_creation_metrics', self.collect_pod_lsp_created_p99(time, duration, end_time)),
                    ('pod_port_binding_p99', 'pod_creation_metrics', self.collect_pod_port_binding_p99(time, duration, end_time)),
                    ('pod_port_binding_up_p99', 'pod_creation_metrics', self.collect_pod_port_binding_up_p99(time, duration, end_time)),
                    ('pod_first_seen_lsp_created_p99', 'pod_creation_metrics', self.collect_pod_first_seen_lsp_created_p99(time, duration, end_time)),
                    
                    ('sync_service_latency', 'service_latency_metrics', self.collect_sync_service_latency(time, duration, end_time)),
                    ('sync_service_latency_p99', 'service_latency_metrics', self.collect_sync_service_latency_p99(time, duration, end_time)),
                    
                    ('apply_network_config_pod_p99', 'network_config_metrics', self.collect_apply_network_config_pod_duration_p99(time, duration, end_time)),
                    ('apply_network_config_service_p99', 'network_config_metrics', self.collect_apply_network_config_service_duration_p99(time, duration, end_time)),
                ])
            
            # Execute tasks with timeout
            per_metric_timeout = 600.0  # 10 minutes
            sem = asyncio.Semaphore(4)  # Limit concurrency
            
            async def run_task(coro):
                async with sem:
                    try:
                        return await asyncio.wait_for(coro, timeout=per_metric_timeout)
                    except asyncio.TimeoutError:
                        return {'error': f'metric timeout after {per_metric_timeout}s'}
            
            task_results = await asyncio.gather(*[run_task(coro) for _, _, coro in all_metric_tasks], return_exceptions=True)
            
            # Organize results by category
            for (metric_key, category, _), result in zip(all_metric_tasks, task_results):
                if isinstance(result, Exception):
                    result = {'error': str(result)}
                results[category][metric_key] = result
            
            # Generate summary
            self._generate_summary(results)
            
        except Exception as e:
            results['error'] = str(e)
            print(f"Error collecting metrics: {e}")
        
        return results
    
    def _generate_summary(self, results: Dict[str, Any]) -> None:
        """Generate overall summary for metrics collection"""
        summary = {
            'total_metrics': 0,
            'successful_metrics': 0,
            'failed_metrics': 0,
            'top_latencies': [],
            'component_breakdown': {'controller': 0, 'node': 0, 'unknown': 0}
        }
        
        all_metric_values = []
        
        # Process each category
        categories = ['ready_duration_metrics', 'sync_duration_metrics', 
                     'cni_latency_metrics', 'pod_annotation_metrics',
                     'pod_creation_metrics', 'service_latency_metrics', 'network_config_metrics']
        
        for category in categories:
            if category in results:
                category_data = results[category]
                
                for metric_name, metric_result in category_data.items():
                    summary['total_metrics'] += 1
                    
                    if 'error' in metric_result:
                        summary['failed_metrics'] += 1
                    else:
                        summary['successful_metrics'] += 1
                        
                        # Track component
                        component = metric_result.get('component', 'unknown')
                        if component in summary['component_breakdown']:
                            summary['component_breakdown'][component] += 1
                        
                        # Extract statistics
                        stats = metric_result.get('statistics', {})
                        if stats.get('max_value') is not None and stats.get('count', 0) > 0:
                            max_val = stats['max_value']
                            avg_val = stats.get('avg_value', max_val)
                            
                            if max_val > 0:
                                all_metric_values.append({
                                    'metric_name': metric_result.get('metric_name', metric_name),
                                    'component': component,
                                    'max_value': max_val,
                                    'avg_value': avg_val,
                                    'readable_max': stats.get('readable_max', {}),
                                    'readable_avg': stats.get('readable_avg', {}),
                                    'data_points': stats.get('count', 0)
                                })
        
        # Top latencies (top 10)
        if all_metric_values:
            sorted_metrics = sorted(all_metric_values, key=lambda x: x['max_value'], reverse=True)
            summary['top_latencies'] = sorted_metrics[:10]
            
            # Overall statistics
            all_max_values = [m['max_value'] for m in all_metric_values if m['max_value'] > 0]
            if all_max_values:
                summary['overall_max_latency'] = {
                    'value': max(all_max_values),
                    'readable': self._convert_duration_to_readable(max(all_max_values)),
                    'metric': next(m['metric_name'] for m in sorted_metrics if m['max_value'] == max(all_max_values))
                }
                
                summary['overall_avg_latency'] = {
                    'value': sum(all_max_values) / len(all_max_values),
                    'readable': self._convert_duration_to_readable(sum(all_max_values) / len(all_max_values))
                }
        
        results['summary'] = summary


# Convenience functions
async def collect_enhanced_ovn_latency_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None, include_controller_metrics: bool = True, include_node_metrics: bool = True, include_extended_metrics: bool = True) -> Dict[str, Any]:
    """Collect enhanced OVN latency metrics"""
    collector = OVNLatencyCollector(prometheus_client)
    return await collector.collect_comprehensive_latency_metrics(time, duration, end_time, include_controller_metrics, include_node_metrics, include_extended_metrics)


async def get_enhanced_ovn_latency_summary_json(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> str:
    """Collect enhanced OVN latency metrics and return as JSON string"""
    results = await collect_enhanced_ovn_latency_metrics(prometheus_client, time, duration, end_time)
    return json.dumps(results, indent=2, default=str)


# Individual metric convenience functions for backwards compatibility
async def get_controller_ready_duration_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Get controller ready duration metrics"""
    collector = OVNLatencyCollector(prometheus_client)
    return await collector.collect_controller_ready_duration(time, duration, end_time)


async def get_controller_sync_duration_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Get controller sync duration metrics"""
    collector = OVNLatencyCollector(prometheus_client)
    return await collector.collect_controller_sync_duration(time, duration, end_time)


async def get_pod_latency_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Get all pod-related latency metrics"""
    collector = OVNLatencyCollector(prometheus_client)
    
    base_timestamp = time or datetime.now(timezone.utc).isoformat()
    results = {
        'collection_timestamp': base_timestamp,
        'query_type': 'duration' if duration else 'instant',
        'pod_annotation_latency_p99': await collector.collect_pod_annotation_latency_p99(time, duration, end_time),
        'pod_lsp_created_p99': await collector.collect_pod_lsp_created_p99(time, duration, end_time),
        'pod_port_binding_p99': await collector.collect_pod_port_binding_p99(time, duration, end_time),
        'pod_port_binding_up_p99': await collector.collect_pod_port_binding_up_p99(time, duration, end_time),
        'pod_first_seen_lsp_created_p99': await collector.collect_pod_first_seen_lsp_created_p99(time, duration, end_time)
    }
    
    if duration:
        start_time, end_time_actual = collector.prometheus_client.get_time_range_from_duration(duration, end_time)
        results['query_parameters'] = {'duration': duration, 'start_time': start_time, 'end_time': end_time_actual}
        
    return results


async def get_cni_latency_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Get all CNI-related latency metrics"""
    collector = OVNLatencyCollector(prometheus_client)
    
    base_timestamp = time or datetime.now(timezone.utc).isoformat()
    results = {
        'collection_timestamp': base_timestamp,
        'query_type': 'duration' if duration else 'instant',
        'cni_add_latency_p99': await collector.collect_cni_request_add_latency_p99(time, duration, end_time),
        'cni_del_latency_p99': await collector.collect_cni_request_del_latency_p99(time, duration, end_time)
    }
    
    if duration:
        start_time, end_time_actual = collector.prometheus_client.get_time_range_from_duration(duration, end_time)
        results['query_parameters'] = {'duration': duration, 'start_time': start_time, 'end_time': end_time_actual}
    
    return results


async def get_service_latency_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None, duration: Optional[str] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Get all service-related latency metrics"""
    collector = OVNLatencyCollector(prometheus_client)
    
    base_timestamp = time or datetime.now(timezone.utc).isoformat()
    results = {
        'collection_timestamp': base_timestamp,
        'query_type': 'duration' if duration else 'instant',
        'sync_service_latency': await collector.collect_sync_service_latency(time, duration, end_time),
        'sync_service_latency_p99': await collector.collect_sync_service_latency_p99(time, duration, end_time)
    }
    
    if duration:
        start_time, end_time_actual = collector.prometheus_client.get_time_range_from_duration(duration, end_time)
        results['query_parameters'] = {'duration': duration, 'start_time': start_time, 'end_time': end_time_actual}
    
    return results


# Legacy compatibility functions (maintain existing API)
async def collect_ovn_sync_metrics(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None) -> Dict[str, Any]:
    """Legacy function - collect OVN sync metrics and return as dictionary (instant query only)"""
    collector = OVNLatencyCollector(prometheus_client)
    return await collector.collect_comprehensive_latency_metrics(time)


async def collect_ovn_sync_metrics_duration(prometheus_client: PrometheusBaseQuery, duration: str, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Legacy function - collect OVN sync metrics over duration and return as dictionary"""
    collector = OVNLatencyCollector(prometheus_client)
    return await collector.collect_comprehensive_latency_metrics(None, duration, end_time)


async def get_ovn_sync_summary_json(prometheus_client: PrometheusBaseQuery, time: Optional[str] = None) -> str:
    """Legacy function - collect OVN sync metrics and return as JSON string (instant query only)"""
    results = await collect_ovn_sync_metrics(prometheus_client, time)
    return json.dumps(results, indent=2, default=str)


async def main():
    """Example usage of OVNLatencyCollector"""
    await auth.initialize()
    
    prometheus_client = PrometheusBaseQuery(
        prometheus_url=auth.prometheus_url,
        token=auth.prometheus_token
    )
    
    collector = OVNLatencyCollector(prometheus_client)
    
    try:
        print("Testing comprehensive latency collection...")
        results = await collector.collect_comprehensive_latency_metrics()
        
        print(f"\nCollection Summary:")
        print(f"Total metrics: {results['summary']['total_metrics']}")
        print(f"Successful: {results['summary']['successful_metrics']}")
        print(f"Failed: {results['summary']['failed_metrics']}")
        
        # Show top latencies
        if results['summary']['top_latencies']:
            print(f"\nTop 5 latencies:")
            for i, metric in enumerate(results['summary']['top_latencies'][:5]):
                print(f"  {i+1}. {metric['metric_name']}: {metric['readable_max']['value']}{metric['readable_max']['unit']}")
        
    except Exception as e:
        print(f"Error during testing: {e}")
    finally:
        await prometheus_client.close()


if __name__ == "__main__":
    asyncio.run(main())