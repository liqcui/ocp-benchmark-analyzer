#!/usr/bin/env python3
"""
OVN-Kubernetes Latency Collector - Refactored Version
Collects and analyzes latency metrics from OVN-Kubernetes components
Uses config/metrics-latency.yml for metric definitions
"""

import asyncio
import logging
import math
from typing import Dict, List, Any, Optional
from datetime import datetime
import pytz
import os
import sys

# Ensure project root on sys.path for utils imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

from ocauth.openshift_auth import OpenShiftAuth
from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from config.metrics_config_reader import Config

# Configure logging
logger = logging.getLogger(__name__)


class OVNLatencyCollector:
    """Collector for OVN-Kubernetes latency metrics"""
    
    def __init__(self, ocp_auth: OpenShiftAuth, duration: str = "1h", metrics_file_path: str = None):
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.logger = logging.getLogger(__name__)
        self.utility = mcpToolsUtility(ocp_auth)
        
        # Load metrics configuration
        if metrics_file_path is None:
            metrics_file_path = os.path.join(project_root, 'config', 'metrics-latency.yml')
        
        self.config = Config()
        load_result = self.config.load_metrics_file(metrics_file_path)
        
        # Get category-specific metrics
        self.category = "latency"
        self.latency_metrics = self.config.get_metrics_by_category(self.category)
        
        # Cache for pod-to-node mappings
        self._pod_node_cache = {}
        self._cache_timestamp = None
        self._cache_expiry_minutes = 10
        
        # Log metrics loading
        if load_result.get('success'):
            metrics_count = len(self.latency_metrics)
            self.logger.info(f"✅ Loaded {metrics_count} metrics from {self.category} category (OVNLatencyCollector)")
        else:
            self.logger.warning(f"⚠️  No {self.category} metrics found in configuration")
    
    async def _refresh_pod_node_cache(self) -> None:
        """Refresh the pod-to-node mapping cache"""
        current_time = datetime.now(pytz.UTC)
        
        if (self._cache_timestamp and 
            self._pod_node_cache and 
            (current_time - self._cache_timestamp).total_seconds() < (self._cache_expiry_minutes * 60)):
            return
        
        self.logger.info("Refreshing pod-to-node mapping cache...")
        
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
                
                # Only use full pod name - no short names
                self._pod_node_cache[pod_name] = {
                    'node_name': node_name,
                    'namespace': namespace,
                    'full_pod_name': pod_name
                }
            
            self._cache_timestamp = current_time
            self.logger.info(f"Pod-to-node cache refreshed with {len(all_pod_info)} pods")
            
        except Exception as e:
            self.logger.error(f"Failed to refresh pod-node cache: {e}")
    
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
            if pod_part.strip() and not pod_part.replace('.', '').replace(':', '').isdigit():
                return pod_part.strip()
        
        return 'unknown'
    
    def _find_node_name_for_pod(self, pod_name: str) -> str:
        """Find node name for a given pod using the cache - full name only"""
        if not pod_name or pod_name == 'unknown':
            return 'unknown'
        
        # Only exact match - no fuzzy matching
        if pod_name in self._pod_node_cache:
            return self._pod_node_cache[pod_name]['node_name']
        
        return 'unknown'
    
    async def _query_with_stats(self, prom_client: PrometheusBaseQuery, query: str, duration: str) -> Dict[str, Any]:
        """Execute a range query and compute basic statistics per series and overall"""
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
                        # Check for NaN and infinity
                        if not (math.isnan(v) or math.isinf(v)):
                            values.append(v)
                    except (ValueError, TypeError):
                        continue

                stats: Dict[str, Any] = {}
                if values:
                    avg_v = sum(values) / len(values)
                    max_v = max(values)
                    stats = {
                        'avg': avg_v,
                        'max': max_v,
                        'count': len(values),
                        'latest': values[-1]
                    }
                    all_values.extend(values)
                else:
                    # No valid values - set stats to None to indicate no data
                    stats = {
                        'avg': None,
                        'max': None,
                        'count': 0,
                        'latest': None
                    }

                series_data.append({'labels': metric_labels, 'statistics': stats})

            overall_statistics: Dict[str, Any] = {}
            if all_values:
                overall_statistics = {
                    'avg': sum(all_values) / len(all_values),
                    'max': max(all_values),
                    'count': len(all_values)
                }
            else:
                # No valid values overall
                overall_statistics = {
                    'avg': None,
                    'max': None,
                    'count': 0
                }

            return {
                'status': 'success',
                'series_data': series_data,
                'overall_statistics': overall_statistics
            }
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    async def _collect_generic_metric(self, prom: PrometheusBaseQuery, metric_config: Dict[str, Any]) -> Dict[str, Any]:
        """Generic method to collect any latency metric"""
        query = metric_config['expr']
        metric_name = metric_config['name']
        
        try:
            # Refresh pod-node cache
            await self._refresh_pod_node_cache()
            
            result = await self._query_with_stats(prom, query, self.duration)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            # Process results by pod
            pod_metrics = {}
            for series in result.get('series_data', []):
                labels = series['labels']
                pod_name = self._extract_pod_name_from_labels(labels)
                
                if pod_name != 'unknown':
                    node_name = self._find_node_name_for_pod(pod_name)
                    stats = series['statistics']
                    
                    # Format based on metric unit
                    unit = metric_config.get('unit', 'seconds')
                    
                    # Handle None values properly - don't round None, return None
                    avg_val = stats.get('avg')
                    max_val = stats.get('max')
                    latest_val = stats.get('latest')
                    
                    pod_metrics[pod_name] = {
                        'pod_name': pod_name,
                        'node_name': node_name,
                        'avg_value': round(avg_val, 6) if avg_val is not None else None,
                        'max_value': round(max_val, 6) if max_val is not None else None,
                        'latest_value': round(latest_val, 6) if latest_val is not None else None,
                        'data_points': stats.get('count', 0),
                        'unit': unit
                    }
            
            # Handle overall stats - return None instead of 0 when no valid data
            overall_stats_raw = result.get('overall_statistics', {})
            overall_avg = overall_stats_raw.get('avg')
            overall_max = overall_stats_raw.get('max')
            
            return {
                'status': 'success',
                'metric_name': metric_name,
                'title': metric_config.get('title', metric_name.replace('_', ' ').title()),
                'unit': metric_config.get('unit', 'seconds'),
                'description': metric_config.get('description', f'Latency metric: {metric_name}'),
                'pod_metrics': pod_metrics,
                'total_pods': len(pod_metrics),
                'overall_stats': {
                    'avg': round(overall_avg, 6) if overall_avg is not None else None,
                    'max': round(overall_max, 6) if overall_max is not None else None,
                    'count': overall_stats_raw.get('count', 0)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting metric {metric_name}: {e}")
            return {'status': 'error', 'error': str(e)}
    
    # Individual metric collection methods
    async def collect_cni_request_add_latency_p99(self) -> Dict[str, Any]:
        """Collect CNI ADD request latency p99"""
        metric_config = self.config.get_metric_by_name('cni_request_add_latency_p99')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_cni_request_del_latency_p99(self) -> Dict[str, Any]:
        """Collect CNI DEL request latency p99"""
        metric_config = self.config.get_metric_by_name('cni_request_del_latency_p99')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_pod_annotation_latency_p99(self) -> Dict[str, Any]:
        """Collect pod annotation latency p99"""
        metric_config = self.config.get_metric_by_name('pod_annotation_latency_p99')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_pod_first_seen_lsp_created_p99(self) -> Dict[str, Any]:
        """Collect pod first seen to LSP created latency p99"""
        metric_config = self.config.get_metric_by_name('pod_first_seen_lsp_created_p99')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_pod_lsp_created_p99(self) -> Dict[str, Any]:
        """Collect pod LSP created latency p99"""
        metric_config = self.config.get_metric_by_name('pod_lsp_created_p99')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_pod_port_binding_p99(self) -> Dict[str, Any]:
        """Collect pod port binding latency p99"""
        metric_config = self.config.get_metric_by_name('pod_port_binding_p99')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_pod_port_binding_up_p99(self) -> Dict[str, Any]:
        """Collect pod port binding up latency p99"""
        metric_config = self.config.get_metric_by_name('pod_port_binding_up_p99')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_sync_service_latency(self) -> Dict[str, Any]:
        """Collect service sync latency average"""
        metric_config = self.config.get_metric_by_name('sync_service_latency')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_sync_service_latency_p99(self) -> Dict[str, Any]:
        """Collect service sync latency p99"""
        metric_config = self.config.get_metric_by_name('sync_service_latency_p99')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_apply_network_config_pod_duration_p99(self) -> Dict[str, Any]:
        """Collect network config application latency p99 for pods"""
        metric_config = self.config.get_metric_by_name('apply_network_config_pod_duration_p99')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_apply_network_config_service_duration_p99(self) -> Dict[str, Any]:
        """Collect network config application latency p99 for services"""
        metric_config = self.config.get_metric_by_name('apply_network_config_service_duration_p99')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_ovnkube_controller_ready_duration_seconds(self) -> Dict[str, Any]:
        """Collect controller ready duration"""
        metric_config = self.config.get_metric_by_name('ovnkube_controller_ready_duration_seconds')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_ovnkube_node_ready_duration_seconds(self) -> Dict[str, Any]:
        """Collect node ready duration"""
        metric_config = self.config.get_metric_by_name('ovnkube_node_ready_duration_seconds')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_ovnkube_controller_sync_duration_seconds(self) -> Dict[str, Any]:
        """Collect controller sync duration"""
        metric_config = self.config.get_metric_by_name('ovnkube_controller_sync_duration_seconds')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all latency metrics and return comprehensive results"""
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
                    'timezone': 'UTC',
                    'duration': self.duration,
                    'category': 'latency',
                    'metrics': {}
                }
                
                # Process each metric dynamically from config
                for metric_config in self.latency_metrics:
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


# Convenience functions for metric collection
async def collect_ovn_latency_metrics(ocp_auth: OpenShiftAuth, duration: str = "1h") -> Dict[str, Any]:
    """Convenience function to collect all OVN latency metrics"""
    collector = OVNLatencyCollector(ocp_auth, duration)
    return await collector.collect_all_metrics()


async def get_specific_ovn_latency_metric(ocp_auth: OpenShiftAuth, metric_name: str, duration: str = "1h") -> Dict[str, Any]:
    """Convenience function to get a specific OVN latency metric"""
    collector = OVNLatencyCollector(ocp_auth, duration)
    return await collector.get_metric_by_name(metric_name)


# Individual metric convenience functions
async def get_cni_add_latency(ocp_auth: OpenShiftAuth, duration: str = "1h") -> Dict[str, Any]:
    """Get CNI ADD request latency"""
    collector = OVNLatencyCollector(ocp_auth, duration)
    return await collector.collect_cni_request_add_latency_p99()


async def get_cni_del_latency(ocp_auth: OpenShiftAuth, duration: str = "1h") -> Dict[str, Any]:
    """Get CNI DEL request latency"""
    collector = OVNLatencyCollector(ocp_auth, duration)
    return await collector.collect_cni_request_del_latency_p99()


async def get_pod_annotation_latency(ocp_auth: OpenShiftAuth, duration: str = "1h") -> Dict[str, Any]:
    """Get pod annotation latency"""
    collector = OVNLatencyCollector(ocp_auth, duration)
    return await collector.collect_pod_annotation_latency_p99()


async def get_controller_ready_duration(ocp_auth: OpenShiftAuth, duration: str = "1h") -> Dict[str, Any]:
    """Get controller ready duration"""
    collector = OVNLatencyCollector(ocp_auth, duration)
    return await collector.collect_ovnkube_controller_ready_duration_seconds()


async def get_node_ready_duration(ocp_auth: OpenShiftAuth, duration: str = "1h") -> Dict[str, Any]:
    """Get node ready duration"""
    collector = OVNLatencyCollector(ocp_auth, duration)
    return await collector.collect_ovnkube_node_ready_duration_seconds()


async def get_controller_sync_duration(ocp_auth: OpenShiftAuth, duration: str = "1h") -> Dict[str, Any]:
    """Get controller sync duration"""
    collector = OVNLatencyCollector(ocp_auth, duration)
    return await collector.collect_ovnkube_controller_sync_duration_seconds()