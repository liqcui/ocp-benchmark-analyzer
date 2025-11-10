#!/usr/bin/env python3
"""
OVNK Benchmark Network I/O Collector (PromQL)
Renamed from: ovnk_benchmark_prometheus_network_io.py
"""

import asyncio
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from statistics import mean

# Import base query and utilities
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from config.metrics_config_reader import Config

logger = logging.getLogger(__name__)


class NetworkIOCollector:
    """Collector for network I/O metrics with node grouping and analysis"""
    
    def __init__(self, prometheus_url: str, token: Optional[str] = None, config: Optional[Config] = None):
        self.prometheus_client = PrometheusBaseQuery(prometheus_url, token)
        self.utility = mcpToolsUtility()
        # Prefer provided config; otherwise load network metrics from metrics-net.yml
        if config is None:
            self.config = Config()
            # Try to load metrics-net.yml from common locations
            import os
            import sys
            # Get project root (assuming this file is in tools/net/)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            metrics_net_file = os.path.join(project_root, 'config', 'metrics-net.yml')
            load_result = self.config.load_metrics_file(metrics_net_file)
            if not load_result.get('success'):
                logger.warning(f"Failed to load metrics file {metrics_net_file}: {load_result.get('error', 'Unknown error')}")
        else:
            self.config = config
        
        # Get network_io metrics from config
        self.metrics = self.config.get_metrics_by_category('network_io')
        
        # If no network_io metrics found, try to load metrics-net.yml
        if not self.metrics:
            logger.warning("No network_io metrics found in config, attempting to load metrics-net.yml")
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            metrics_net_file = os.path.join(project_root, 'config', 'metrics-net.yml')
            load_result = self.config.load_metrics_file(metrics_net_file)
            if load_result.get('success'):
                self.metrics = self.config.get_metrics_by_category('network_io')
                logger.info(f"✅ Loaded {len(self.metrics)} network_io metrics from {metrics_net_file}")
            else:
                logger.error(f"Failed to load metrics file {metrics_net_file}: {load_result.get('error', 'Unknown error')}")
        
        logger.info(f"Initialized NetworkIOCollector with {len(self.metrics)} metrics")
    
    async def initialize(self):
        """Initialize the collector"""
        await self.prometheus_client._ensure_session()
    
    async def close(self):
        """Close connections"""
        await self.prometheus_client.close()
    
    def _get_time_range(self, duration: str = "5m") -> tuple[str, str]:
        """Get time range in UTC"""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - self._parse_duration(duration)
        return (
            start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        )
    
    def _parse_duration(self, duration: str) -> timedelta:
        """Parse duration string"""
        duration = duration.strip().lower()
        if duration.endswith('s'):
            return timedelta(seconds=int(duration[:-1]))
        elif duration.endswith('m'):
            return timedelta(minutes=int(duration[:-1]))
        elif duration.endswith('h'):
            return timedelta(hours=int(duration[:-1]))
        elif duration.endswith('d'):
            return timedelta(days=int(duration[:-1]))
        return timedelta(minutes=5)
    
    def _resolve_query_variables(self, expr: str) -> str:
        """Resolve Prometheus template variables in query expressions"""
        resolved = expr
        
        # Replace $node_name with regex pattern based on context
        # If used with exact match (instance="$node_name"), replace with regex version
        resolved = re.sub(r'instance="?\$node_name"?', 'instance=~".*"', resolved)
        resolved = re.sub(r'node="?\$node_name"?', 'node=~".*"', resolved)
        
        # Replace remaining $node_name (already in regex context)
        resolved = resolved.replace('$node_name', '.*')
        
        # Replace job variables
        resolved = re.sub(r'job="?\$job"?', 'job=~".*"', resolved)
        resolved = resolved.replace('$job', '.*')
        
        # Replace rate interval variables
        resolved = resolved.replace('$__rate_interval', '5m')
        resolved = resolved.replace('[$__rate_interval]', '[5m]')
        
        # Handle node_node typo in some queries
        resolved = resolved.replace('$node_node', '.*')
        
        return resolved
    
    def _extract_node_from_labels(self, labels: Dict[str, str]) -> str:
        """Extract node name from metric labels"""
        return (labels.get('instance', '') or 
                labels.get('node', '') or 
                labels.get('kubernetes_node', '') or
                labels.get('nodename', '')).split(':')[0]
    
    def _calculate_stats(self, values: List[float]) -> Dict[str, float]:
        """Calculate avg and max from values"""
        if not values:
            return {'avg': 0.0, 'max': 0.0}
        return {
            'avg': round(mean(values), 2),
            'max': round(max(values), 2)
        }
    
    async def _query_metric_instant(self, metric_expr: str) -> Dict[str, float]:
        """Query metric instantly and return node->value mapping"""
        # Resolve template variables
        resolved_expr = self._resolve_query_variables(metric_expr)
        
        try:
            result = await self.prometheus_client.query_instant(resolved_expr)
            
            node_values = {}
            for item in result.get('result', []):
                labels = item.get('metric', {})
                node_name = self._extract_node_from_labels(labels)
                
                if not node_name:
                    continue
                
                if 'value' in item:
                    try:
                        value = float(item['value'][1])
                        if value != float('inf') and value != float('-inf'):
                            node_values[node_name] = value
                    except (ValueError, TypeError):
                        continue
            
            return node_values
            
        except Exception as e:
            logger.error(f"Error in instant query '{metric_expr[:100]}...': {e}")
            return {}
    
    async def _query_metric_range(self, metric_expr: str, duration: str = "5m") -> Dict[str, List[float]]:
        """Query metric and return node->values mapping"""
        start_time, end_time = self._get_time_range(duration)
        
        # Resolve template variables
        resolved_expr = self._resolve_query_variables(metric_expr)
        
        try:
            result = await self.prometheus_client.query_range(
                resolved_expr, start_time, end_time, step='15s'
            )
            
            node_values = {}
            for item in result.get('result', []):
                labels = item.get('metric', {})
                node_name = self._extract_node_from_labels(labels)
                
                if not node_name:
                    continue
                
                values = []
                for timestamp, value in item.get('values', []):
                    try:
                        v = float(value)
                        if v != float('inf') and v != float('-inf'):
                            values.append(v)
                    except (ValueError, TypeError):
                        continue
                
                if values:
                    if node_name not in node_values:
                        node_values[node_name] = []
                    node_values[node_name].extend(values)
            
            return node_values
            
        except Exception as e:
            logger.error(f"Error querying metric '{metric_expr[:100]}...': {e}")
            logger.debug(f"Resolved query: {resolved_expr}")
            return {}
    
    async def _query_metric_with_fallback(self, metric_expr: str, duration: str = "5m") -> Dict[str, List[float]]:
        """Query metric with fallback from range to instant query"""
        # First try range query
        node_values = await self._query_metric_range(metric_expr, duration)
        
        # If range query returns empty results, try instant query
        if not node_values:
            logger.debug(f"Range query returned no results, trying instant query")
            instant_values = await self._query_metric_instant(metric_expr)
            # Convert instant values to list format for consistency
            node_values = {node: [value] for node, value in instant_values.items()}
        
        return node_values
    
    def _aggregate_by_node(self, node_values: Dict[str, List[float]]) -> Dict[str, List[float]]:
        """Aggregate metrics by node, summing across devices/interfaces"""
        aggregated = {}
        
        for node_key, values in node_values.items():
            # Extract base node name (remove port if present)
            node_name = node_key.split(':')[0]
            
            if node_name not in aggregated:
                aggregated[node_name] = []
            aggregated[node_name].extend(values)
        
        return aggregated
    
    def _process_metric_results(self, node_values: Dict[str, List[float]], 
                                node_groups: Dict[str, List[Dict[str, Any]]], 
                                metric_name: str, unit: str) -> Dict[str, Any]:
        """Process metric results and organize by node groups"""
        result = {
            'metric': metric_name,
            'unit': unit,
            'controlplane': {'nodes': [], 'unit': unit},
            'worker': {'top3': []},
            'infra': {'nodes': [], 'unit': unit},
            'workload': {'nodes': [], 'unit': unit}
        }
        
        # Aggregate metrics by node (sum across devices/interfaces)
        aggregated_values = self._aggregate_by_node(node_values)
        
        if not aggregated_values:
            logger.warning(f"No data found for metric: {metric_name}")
            return result
        
        logger.debug(f"Processing {metric_name}: {len(aggregated_values)} nodes found")
        
        # Create node name to role mapping
        node_role_map = {}
        for role, nodes in node_groups.items():
            for node_info in nodes:
                node_name = node_info['name']
                # Handle both full and short names
                node_role_map[node_name] = role
                if '.' in node_name:
                    short_name = node_name.split('.')[0]
                    node_role_map[short_name] = role
        
        # Process each node's values
        worker_nodes = []
        
        for node_name, values in aggregated_values.items():
            stats = self._calculate_stats(values)
            
            # Find node role
            role = node_role_map.get(node_name)
            if not role and '.' not in node_name:
                # Try to find full name match
                for full_name, r in node_role_map.items():
                    if full_name.startswith(node_name + '.'):
                        role = r
                        break
            
            if not role:
                role = 'worker'  # Default to worker if unknown
            
            node_data = {
                'node': node_name,
                'avg': stats['avg'],
                'max': stats['max'],
                'unit': unit
            }
            
            if role == 'controlplane':
                result['controlplane']['nodes'].append(node_data)
            elif role == 'worker':
                worker_nodes.append(node_data)
            elif role == 'infra':
                result['infra']['nodes'].append(node_data)
            elif role == 'workload':
                result['workload']['nodes'].append(node_data)
        
        # Sort workers by max value and get top 3
        worker_nodes.sort(key=lambda x: x['max'], reverse=True)
        result['worker']['top3'] = worker_nodes[:3]
        
        return result
    
    async def network_io_node_network_rx_utilization(self, duration: str = "5m") -> Dict[str, Any]:
        """Network RX utilization in bits per second"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_network_rx_utilization'), None)
        if not metric:
            logger.warning("Metric 'network_io_node_network_rx_utilization' not found in config")
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        if not node_values:
            logger.debug(f"No node values returned for {metric['name']}")
        
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        if not node_groups:
            logger.warning(f"No node groups found for {metric['name']} - this will result in empty metrics")
        
        result = self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
        if not result.get('controlplane', {}).get('nodes') and not result.get('worker', {}).get('top3'):
            logger.debug(f"Processed result for {metric['name']} is empty - node_values: {len(node_values)}, node_groups: {len(node_groups) if node_groups else 0}")
        
        return result
    
    async def network_io_node_network_tx_utilization(self, duration: str = "5m") -> Dict[str, Any]:
        """Network TX utilization in bits per second"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_network_tx_utilization'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_network_rx_package(self, duration: str = "5m") -> Dict[str, Any]:
        """Network RX packets per second"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_network_rx_package'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_network_tx_package(self, duration: str = "5m") -> Dict[str, Any]:
        """Network TX packets per second"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_network_tx_package'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_network_rx_drop(self, duration: str = "5m") -> Dict[str, Any]:
        """Network RX drops per second"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_network_rx_drop'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_network_tx_drop(self, duration: str = "5m") -> Dict[str, Any]:
        """Network TX drops per second"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_network_tx_drop'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_grpc_active_watch_streams(self, duration: str = "5m") -> Dict[str, Any]:
        """GRPC active watch streams"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_grpc_active_watch_streams'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_nf_conntrack_entries(self, duration: str = "5m") -> Dict[str, Any]:
        """NF conntrack entries count"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_nf_conntrack_entries'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_nf_conntrack_entries_limit(self, duration: str = "5m") -> Dict[str, Any]:
        """NF conntrack entries limit"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_nf_conntrack_entries_limit'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_saturation_rx(self, duration: str = "5m") -> Dict[str, Any]:
        """Network saturation RX"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_saturation_rx'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_saturation_tx(self, duration: str = "5m") -> Dict[str, Any]:
        """Network saturation TX"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_saturation_tx'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_error_rx(self, duration: str = "5m") -> Dict[str, Any]:
        """Network receive errors per second"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_error_rx'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_error_tx(self, duration: str = "5m") -> Dict[str, Any]:
        """Network transmit errors per second"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_error_tx'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_network_up(self, duration: str = "5m") -> Dict[str, Any]:
        """Network interface up status"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_network_up'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_traffic_carrier(self, duration: str = "5m") -> Dict[str, Any]:
        """Network carrier status"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_traffic_carrier'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_speed_bytes(self, duration: str = "5m") -> Dict[str, Any]:
        """Network speed in bits per second"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_speed_bytes'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_arp_entries(self, duration: str = "5m") -> Dict[str, Any]:
        """ARP entries count"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_arp_entries'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_nodec_receive_fifo_total(self, duration: str = "5m") -> Dict[str, Any]:
        """Receive FIFO total"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_nodec_receive_fifo_total'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def network_io_node_transit_fifo_total(self, duration: str = "5m") -> Dict[str, Any]:
        """Transmit FIFO total"""
        metric = next((m for m in self.metrics if m['name'] == 'network_io_node_transit_fifo_total'), None)
        if not metric:
            return {}
        
        node_values = await self._query_metric_with_fallback(metric['expr'], duration)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        return self._process_metric_results(node_values, node_groups, metric['name'], metric['unit'])
    
    async def collect_all_metrics(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect all network I/O metrics"""
        logger.info(f"Collecting all network_io metrics for duration: {duration}")
        
        results = {
            'category': 'network_io',
            'duration': duration,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'metrics': {}
        }
        
        # Collect all metrics concurrently
        metric_methods = [
            self.network_io_node_network_rx_utilization,
            self.network_io_node_network_tx_utilization,
            self.network_io_node_network_rx_package,
            self.network_io_node_network_tx_package,
            self.network_io_node_network_rx_drop,
            self.network_io_node_network_tx_drop,
            self.network_io_grpc_active_watch_streams,
            self.network_io_node_nf_conntrack_entries,
            self.network_io_node_nf_conntrack_entries_limit,
            self.network_io_node_saturation_rx,
            self.network_io_node_saturation_tx,
            self.network_io_node_error_rx,
            self.network_io_node_error_tx,
            self.network_io_node_network_up,
            self.network_io_node_traffic_carrier,
            self.network_io_node_speed_bytes,
            self.network_io_node_arp_entries,
            self.network_io_nodec_receive_fifo_total,
            self.network_io_node_transit_fifo_total
        ]
        
        tasks = [method(duration) for method in metric_methods]
        metric_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        metrics_with_data = 0
        metrics_with_errors = 0
        metrics_empty = 0
        
        for method, result in zip(metric_methods, metric_results):
            metric_name = method.__name__
            if isinstance(result, Exception):
                logger.error(f"Error collecting {metric_name}: {result}")
                results['metrics'][metric_name] = {'error': str(result)}
                metrics_with_errors += 1
            else:
                results['metrics'][metric_name] = result
                # Check if metric has any data
                has_data = False
                if isinstance(result, dict):
                    for role_key in ['controlplane', 'infra', 'workload']:
                        if result.get(role_key, {}).get('nodes'):
                            has_data = True
                            break
                    if not has_data and result.get('worker', {}).get('top3'):
                        has_data = True
                
                if has_data:
                    metrics_with_data += 1
                else:
                    metrics_empty += 1
        
        logger.info(f"Collection complete: {metrics_with_data} with data, {metrics_empty} empty, {metrics_with_errors} errors")
        
        # Add summary
        results['summary'] = {
            'total_metrics': len(metric_methods),
            'with_data': metrics_with_data,
            'empty': metrics_empty,
            'errors': metrics_with_errors
        }
        
        return results
 

async def main():
    """Example usage"""
    from ocauth.openshift_auth import OpenShiftAuth
    
    # Initialize authentication
    auth = OpenShiftAuth()
    await auth.initialize()
    prom_config = auth.get_prometheus_config()
    
    # Create collector
    collector = NetworkIOCollector(
        prometheus_url=prom_config['url'],
        token=prom_config['token']
    )
    
    try:
        await collector.initialize()
        
        # Collect all metrics
        results = await collector.collect_all_metrics(duration="5m")
        
        # Display results
        import json
        print(json.dumps(results, indent=2))
        
    finally:
        await collector.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())


