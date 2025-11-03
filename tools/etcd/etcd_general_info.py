"""
etcd General Info Collector - Fixed Version
Collects general etcd cluster information and health metrics with node/pod-level details
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import pytz
import os
import sys

# Ensure project root on sys.path for utils imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from config.metrics_config_reader import Config

class GeneralInfoCollector:
    """Collector for general etcd cluster information"""
    
    def __init__(self, ocp_auth, metrics_file_path: str = None):
        self.ocp_auth = ocp_auth
        self.logger = logging.getLogger(__name__)
        self.timezone = pytz.UTC
        self.tools_utility = mcpToolsUtility(ocp_auth)
        
        # Load metrics configuration
        if metrics_file_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metrics_file_path = os.path.join(project_root, 'config', 'metrics-etcd.yml')
        
        self.config = Config()
        load_result = self.config.load_metrics_file(metrics_file_path)
        
        # Get category-specific metrics
        self.category = "general_info"
        general_metrics = self.config.get_metrics_by_category(self.category)
        
        # Log metrics loading
        if load_result.get('success'):
            metrics_count = len(general_metrics)
            self.logger.info(f"✅ Loaded {metrics_count} metrics from {self.category} category (GeneralInfoCollector)")
        else:
            self.logger.warning(f"⚠️  No {self.category} metrics found in configuration")

    async def collect_metrics(self, duration: str = "1h") -> Dict[str, Any]:
        """Collect all general info metrics with node/pod level details"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': 'Prometheus connection failed',
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                general_metrics = self.config.get_metrics_by_category('general_info')
                
                collected_data = {
                    'timestamp': datetime.now(self.timezone).isoformat(),
                    'duration': duration,
                    'category': 'general_info',
                    'pod_metrics': {}
                }
                
                # Collect each metric with pod-level breakdown
                for metric in general_metrics:
                    metric_name = metric['name']
                    metric_query = metric['expr']
                    
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        result = await self._query_with_stats(prom, metric_query, duration)
                        
                        # Process result based on metric type
                        if metric_name in ['apiserver_storage_objects_max_top20', 'cluster_usage_resources_sum_top20']:
                            # Special handling for resource-based metrics
                            resources = await self._process_resource_metrics(result)
                            collected_data['pod_metrics'][metric_name] = {
                                'title': metric.get('title', metric_name),
                                'unit': metric.get('unit', 'unknown'),
                                'query': metric_query,
                                'resources': resources
                            }
                        elif metric_name == 'cpu_io_utilization_iowait':
                            # Use the dedicated method that filters to master nodes only
                            masters_result = await self.get_cpu_io_utilization_iowait_per_node(duration)
                            node_metrics = {}
                            if isinstance(masters_result, dict):
                                node_metrics = masters_result.get('nodes', {})
                            collected_data['pod_metrics'][metric_name] = {
                                'title': metric.get('title', metric_name),
                                'unit': metric.get('unit', 'unknown'),
                                'query': metric_query,
                                'nodes': node_metrics,
                                'scope': 'masters_only'
                            }
                        elif metric_name in ['vmstat_pgmajfault_total', 'vmstat_pgmajfault_rate']:
                            # Node-level metrics that should return node data
                            node_metrics = await self._process_metric_for_nodes(result, metric_name)
                            # Filter to master nodes only
                            master_nodes = await self._get_master_nodes()
                            filtered_node_metrics = self._filter_master_nodes_only(node_metrics, master_nodes)
                            collected_data['pod_metrics'][metric_name] = {
                                'title': metric.get('title', metric_name),
                                'unit': metric.get('unit', 'unknown'),
                                'query': metric_query,
                                'nodes': filtered_node_metrics,
                                'scope': 'masters_only'
                            }
                        else:
                            # Default pod-level processing with fallback
                            pod_metrics = await self._process_metric_for_pods(result, metric_name)
                            
                            # If no pods found, try to get just the values without pod breakdown
                            if not pod_metrics:
                                simple_values = self._extract_simple_values(result)
                                if simple_values:
                                    collected_data['pod_metrics'][metric_name] = {
                                        'title': metric.get('title', metric_name),
                                        'unit': metric.get('unit', 'unknown'),
                                        'query': metric_query,
                                        'values': simple_values
                                    }
                                else:
                                    collected_data['pod_metrics'][metric_name] = {
                                        'title': metric.get('title', metric_name),
                                        'unit': metric.get('unit', 'unknown'),
                                        'query': metric_query,
                                        'pods': {},
                                        'note': 'No data returned from Prometheus for this metric'
                                    }
                            else:
                                collected_data['pod_metrics'][metric_name] = {
                                    'title': metric.get('title', metric_name),
                                    'unit': metric.get('unit', 'unknown'),
                                    'query': metric_query,
                                    'pods': pod_metrics
                                }
                        
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        collected_data['pod_metrics'][metric_name] = {
                            'title': metric.get('title', metric_name),
                            'unit': metric.get('unit', 'unknown'),
                            'query': metric_query,
                            'error': str(e)
                        }
                
                return {
                    'status': 'success',
                    'data': collected_data
                }
                
        except Exception as e:
            self.logger.error(f"Error collecting general info metrics: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    def _extract_simple_values(self, result: Dict[str, Any]) -> Optional[Dict[str, float]]:
        """Extract simple aggregated values when pod breakdown isn't available"""
        if result['status'] != 'success':
            return None
        
        try:
            series_data = result.get('series_data', [])
            if not series_data:
                return None
            
            all_values = []
            for series in series_data:
                values = series.get('values', [])
                for value_data in values:
                    if isinstance(value_data, dict):
                        val = value_data.get('value')
                    elif isinstance(value_data, list) and len(value_data) >= 2:
                        val = value_data[1]
                    else:
                        continue
                    
                    if val is not None and val != 'NaN':
                        try:
                            all_values.append(float(val))
                        except (ValueError, TypeError):
                            continue
            
            if all_values:
                return {
                    'avg': round(sum(all_values) / len(all_values), 4),
                    'max': round(max(all_values), 4),
                    'min': round(min(all_values), 4),
                    'count': len(all_values),
                    'latest': round(all_values[-1], 4)
                }
            
        except Exception as e:
            self.logger.error(f"Error extracting simple values: {e}")
        
        return None
    
    async def get_cpu_usage_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get CPU usage metrics by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_pods_cpu_usage', duration)
    
    async def get_memory_usage_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get memory usage metrics by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_pods_memory_usage', duration)
    
    async def get_db_space_used_percent_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get database space usage percentage by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_db_space_used_percent', duration)
    
    async def get_db_physical_size_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get database physical size by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_db_physical_size', duration)
    
    async def get_db_logical_size_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get database logical size by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_db_logical_size', duration)
    
    async def get_cpu_io_utilization_iowait_per_node(self, duration: str = "1h") -> Dict[str, Any]:
        """Get CPU IO utilization (iowait) by master node with avg and max values - only returns master nodes"""
        try:
            # Get master nodes first
            master_nodes = await self._get_master_nodes()
            if not master_nodes:
                return {
                    'status': 'error',
                    'error': 'No master nodes found in cluster',
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
            
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': 'Prometheus connection failed',
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                # Build query specifically for master nodes
                master_pattern = '|'.join(master_nodes)
                
                # Try different query variations
                queries = [
                    f'(sum(irate(node_cpu_seconds_total{{mode="iowait",instance=~"({master_pattern}):.*"}}[2m])) by (instance) / count(node_cpu_seconds_total{{instance=~"({master_pattern}):.*"}}) by (instance)) * 100',
                    '(sum(irate(node_cpu_seconds_total{mode="iowait"}[2m])) by (instance) / count(node_cpu_seconds_total) by (instance)) * 100'
                ]
                
                node_metrics = {}
                
                for i, query in enumerate(queries):
                    self.logger.info(f"Trying CPU iowait query {i+1}")
                    result = await self._query_with_stats(prom, query, duration)
                    
                    if result['status'] == 'success':
                        all_node_metrics = await self._process_metric_for_nodes(result, 'cpu_io_utilization_iowait')
                        node_metrics = self._filter_master_nodes_only(all_node_metrics, master_nodes)
                        
                        if node_metrics:
                            self.logger.info(f"Found CPU iowait data for {len(node_metrics)} master nodes")
                            break
                
                return {
                    'status': 'success',
                    'metric': 'cpu_io_utilization_iowait',
                    'title': 'CPU IO Utilization IOWait (Master Nodes Only)',
                    'unit': 'percent',
                    'duration': duration,
                    'nodes': node_metrics,
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"Error getting CPU IO utilization iowait: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'metric': 'cpu_io_utilization_iowait',
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    def _filter_master_nodes_only(self, all_node_metrics: Dict[str, Dict[str, float]], master_nodes: List[str]) -> Dict[str, Dict[str, float]]:
        """Filter node metrics to include only master nodes"""
        filtered_metrics = {}
        
        for node_key, metrics in all_node_metrics.items():
            # Clean node_key (remove port if present)
            clean_node_key = node_key.split(':')[0]
            
            # Check direct match
            for master_node in master_nodes:
                if (master_node in clean_node_key or clean_node_key in master_node or
                    master_node == clean_node_key):
                    filtered_metrics[master_node] = metrics
                    break
        
        return filtered_metrics
    
    async def _get_single_metric_per_pod(self, metric_name: str, duration: str) -> Dict[str, Any]:
        """Get a single metric with avg and max values per etcd pod"""
        try:
            metric_config = self.config.get_metric_by_name(metric_name)
            if not metric_config:
                return {
                    'status': 'error',
                    'error': f'Metric {metric_name} not found in configuration',
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
            
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': 'Prometheus connection failed',
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                result = await self._query_with_stats(prom, metric_config['expr'], duration)
                
                # Process result for pod breakdown
                pod_metrics = await self._process_metric_for_pods(result, metric_name)
                
                # If no pods, try simple values
                if not pod_metrics:
                    simple_values = self._extract_simple_values(result)
                    if simple_values:
                        return {
                            'status': 'success',
                            'metric': metric_name,
                            'title': metric_config.get('title', metric_name),
                            'unit': metric_config.get('unit', 'unknown'),
                            'duration': duration,
                            'values': simple_values,
                            'timestamp': datetime.now(self.timezone).isoformat()
                        }
                
                return {
                    'status': 'success',
                    'metric': metric_name,
                    'title': metric_config.get('title', metric_name),
                    'unit': metric_config.get('unit', 'unknown'),
                    'duration': duration,
                    'pods': pod_metrics,
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"Error getting metric {metric_name}: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'metric': metric_name,
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def _process_metric_for_pods(self, result: Dict[str, Any], metric_name: str) -> Dict[str, Dict[str, float]]:
        """Process Prometheus metric result to extract pod-level avg and max values"""
        pod_data = {}
        
        if result['status'] != 'success':
            return pod_data
        
        try:
            series_data = result.get('series_data', [])
            
            if not series_data:
                self.logger.debug(f"No series data for metric {metric_name}")
                return pod_data
            
            # Build pod to node mapping
            pod_to_node = self.tools_utility.get_pod_to_node_mapping_via_oc(namespace='openshift-etcd')
            
            # Process each series
            for series in series_data:
                labels = series.get('labels', {})
                values = series.get('values', [])
                
                # Extract pod name from labels
                pod_name = self._extract_pod_name(labels)
                
                # If no pod name found, try instance field
                if not pod_name or pod_name == 'unknown':
                    # Check if this is an instance-based metric
                    instance = labels.get('instance', '')
                    if instance and 'etcd' in instance.lower():
                        pod_name = instance.split(':')[0]
                
                if not pod_name or pod_name == 'unknown':
                    self.logger.debug(f"Could not extract pod name from labels: {labels}")
                    continue
                
                # Calculate statistics
                numeric_values = self._extract_numeric_from_values(values)
                
                if numeric_values:
                    pod_data[pod_name] = {
                        'avg': round(sum(numeric_values) / len(numeric_values), 4),
                        'max': round(max(numeric_values), 4),
                        'min': round(min(numeric_values), 4),
                        'count': len(numeric_values),
                        'node': pod_to_node.get(pod_name, 'unknown')
                    }
        
        except Exception as e:
            self.logger.error(f"Error processing metric data for {metric_name}: {e}")
        
        return pod_data
    
    async def _process_metric_for_nodes(self, result: Dict[str, Any], metric_name: str) -> Dict[str, Dict[str, float]]:
        """Process Prometheus metric result to extract node-level avg and max values"""
        node_data = {}
        
        if result['status'] != 'success':
            return node_data
        
        try:
            series_data = result.get('series_data', [])
            
            for series in series_data:
                labels = series.get('labels', {})
                values = series.get('values', [])
                
                # Extract node name
                node_name = self._extract_node_name(labels)
                if not node_name or node_name == 'unknown':
                    continue
                
                # Calculate statistics
                numeric_values = self._extract_numeric_from_values(values)
                
                if numeric_values:
                    node_data[node_name] = {
                        'avg': round(sum(numeric_values) / len(numeric_values), 4),
                        'max': round(max(numeric_values), 4),
                        'min': round(min(numeric_values), 4),
                        'count': len(numeric_values)
                    }
        
        except Exception as e:
            self.logger.error(f"Error processing node metric {metric_name}: {e}")
        
        return node_data
    
    def _extract_numeric_from_values(self, values: List) -> List[float]:
        """Extract numeric values from various value formats"""
        numeric_values = []
        
        for value_data in values:
            val = None
            
            if isinstance(value_data, dict):
                val = value_data.get('value')
            elif isinstance(value_data, list) and len(value_data) >= 2:
                val = value_data[1]
            
            if val is not None and val != 'NaN':
                try:
                    numeric_values.append(float(val))
                except (ValueError, TypeError):
                    continue
        
        return numeric_values
    
    async def _process_resource_metrics(self, result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process resource metrics result to extract resource names and their values"""
        resources = []
        
        if result['status'] != 'success':
            return resources
        
        try:
            series_data = result.get('series_data', [])
            
            for series in series_data:
                labels = series.get('labels', {})
                values = series.get('values', [])
                
                resource_name = self._extract_resource_name(labels)
                numeric_values = self._extract_numeric_from_values(values)
                
                if resource_name != 'unknown' and numeric_values:
                    max_value = max(numeric_values)
                    if max_value > 0:
                        resources.append({
                            'resource_name': resource_name,
                            'max_value': round(max_value, 4)
                        })
            
            # Sort by max_value descending
            resources.sort(key=lambda x: x['max_value'], reverse=True)
        
        except Exception as e:
            self.logger.error(f"Error processing resource metrics: {e}")
        
        return resources
    
    def _extract_pod_name(self, labels: Dict[str, Any]) -> str:
        """Extract pod name from Prometheus metric labels"""
        pod_name_fields = ['pod', 'pod_name', 'kubernetes_pod_name', 'name']
        
        for field in pod_name_fields:
            if field in labels and labels[field]:
                pod_name = str(labels[field])
                if 'etcd' in pod_name.lower():
                    return pod_name
        
        # Try instance field
        if 'instance' in labels:
            instance = str(labels['instance'])
            if 'etcd' in instance.lower() and ':' in instance:
                return instance.split(':')[0]
        
        return 'unknown'
    
    def _extract_node_name(self, labels: Dict[str, Any]) -> str:
        """Extract node name from Prometheus metric labels"""
        node_name_fields = ['node', 'instance', 'kubernetes_node', 'node_name']
        
        for field in node_name_fields:
            if field in labels and labels[field]:
                node_name = str(labels[field])
                if ':' in node_name:
                    node_name = node_name.split(':')[0]
                return node_name
        
        return 'unknown'
    
    def _extract_resource_name(self, labels: Dict[str, Any]) -> str:
        """Extract resource name from Prometheus metric labels"""
        resource_fields = ['resource', 'group_resource', 'name', '__name__']
        
        for field in resource_fields:
            if field in labels and labels[field]:
                return str(labels[field])
        
        # Try group + resource combination
        if 'group' in labels and 'resource' in labels:
            group = str(labels['group'])
            resource = str(labels['resource'])
            return f"{group}/{resource}" if group != 'core' else resource
        
        return 'unknown'
    
    async def _get_master_nodes(self) -> List[str]:
        """Get list of master/controlplane node names"""
        groups = await self.tools_utility.get_node_groups()
        controlplane = groups.get('controlplane', []) if isinstance(groups, dict) else []
        return [n.get('name', '').split(':')[0] for n in controlplane if n.get('name')]
    
    async def _query_with_stats(self, prom_client: PrometheusBaseQuery, query: str, duration: str) -> Dict[str, Any]:
        """Execute a range query and compute statistics"""
        try:
            start, end = prom_client.get_time_range_from_duration(duration)
            data = await prom_client.query_range(query, start, end, step='15s')
            
            series_data = []
            all_values = []
            
            for item in data.get('result', []) if isinstance(data, dict) else []:
                metric_labels = item.get('metric', {})
                values_pairs = item.get('values', []) or []
                
                series_values = []
                numeric_values = []
                
                for ts, val in values_pairs:
                    series_values.append([ts, val])
                    try:
                        v = float(val)
                        if v != float('inf') and v != float('-inf'):
                            numeric_values.append(v)
                    except (ValueError, TypeError):
                        continue
                
                stats = {}
                if numeric_values:
                    stats = {
                        'avg': sum(numeric_values) / len(numeric_values),
                        'max': max(numeric_values),
                        'min': min(numeric_values),
                        'count': len(numeric_values),
                        'latest': numeric_values[-1]
                    }
                    all_values.extend(numeric_values)
                
                series_data.append({
                    'labels': metric_labels,
                    'statistics': stats,
                    'values': series_values
                })
            
            overall_statistics = {}
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
            self.logger.error(f"Error in query_with_stats: {e}")
            return {'status': 'error', 'error': str(e)}