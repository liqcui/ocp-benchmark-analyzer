#!/usr/bin/env python3
"""
Network Socket SoftNet Statistics Collector
Collects softnet statistics from Prometheus for OpenShift nodes
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from config.metrics_config_reader import Config

logger = logging.getLogger(__name__)


class socketStatSoftNetCollector:
    """Collector for network socket softnet statistics"""
    
    def __init__(self, auth_client, config: Config):
        self.auth_client = auth_client
        self.config = config
        self.prometheus_client = PrometheusBaseQuery(
            auth_client.prometheus_url,
            auth_client.prometheus_token
        )
        self.utility = mcpToolsUtility(auth_client)
        self.metrics = self.config.get_metrics_by_category('network_socket_softnet')
        
    async def __aenter__(self):
        await self.prometheus_client.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.prometheus_client.__aexit__(exc_type, exc_val, exc_tb)
    
    def _get_metric_config(self, metric_name: str) -> Optional[Dict[str, Any]]:
        """Get metric configuration by name"""
        for metric in self.metrics:
            if metric['name'] == metric_name:
                return metric
        return None
    
    async def _query_metric_by_node(
        self, 
        metric_name: str, 
        start: str, 
        end: str, 
        step: str = '15s'
    ) -> Dict[str, List[float]]:
        """Query metric and organize by node (keyed by node identifier)"""
        metric_config = self._get_metric_config(metric_name)
        if not metric_config:
            logger.warning(f"Metric {metric_name} not found in config")
            return {}
        
        query_template = metric_config['expr']
        query = query_template.replace('$node_name', '.*')
        
        try:
            result = await self.prometheus_client.query_range(query, start, end, step)
            node_data: Dict[str, List[float]] = {}
            
            for item in result.get('result', []):
                metric_labels = item.get('metric', {})
                # Extract node name from common labels, prefer instance, fallback to node/nodename
                raw_name = (
                    metric_labels.get('instance', '') or
                    metric_labels.get('node', '') or
                    metric_labels.get('nodename', '')
                )
                # Remove port if any
                node_name = raw_name.split(':')[0] if ':' in raw_name else raw_name
                
                values = []
                for ts, val in item.get('values', []):
                    try:
                        v = float(val)
                        if v >= 0:
                            values.append(v)
                    except (ValueError, TypeError):
                        continue
                
                if values and node_name:
                    if node_name not in node_data:
                        node_data[node_name] = []
                    node_data[node_name].extend(values)
            
            return node_data
            
        except Exception as e:
            logger.error(f"Error querying metric {metric_name}: {e}")
            return {}
    
    def _calculate_stats(self, values: List[float]) -> Dict[str, float]:
        """Calculate average and max from values"""
        if not values:
            return {'avg': 0.0, 'max': 0.0}
        avg_val = round(sum(values) / len(values), 2)
        max_val = round(max(values), 2)
        return {'avg': avg_val, 'max': max_val}
    
    async def _collect_metric_for_nodes(
        self, metric_name: str, start: str, end: str, step: str = '15s'
    ) -> Dict[str, Any]:
        """Collect metric statistics grouped by node role"""
        metric_config = self._get_metric_config(metric_name)
        if not metric_config:
            return {'error': f'Metric {metric_name} not found'}
        
        node_data = await self._query_metric_by_node(metric_name, start, end, step)
        if not node_data:
            return {
                'metric': metric_name,
                'unit': metric_config.get('unit', ''),
                'description': metric_config.get('description', ''),
                'nodes': {'controlplane': {}, 'worker': {}, 'infra': {}, 'workload': {}}
            }
        
        # Build node role map using utility.get_node_groups (same approach as TCP netstat)
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        node_role_map: Dict[str, str] = {}
        for role, nodes in node_groups.items():
            for node in nodes:
                full_name = node.get('name', '')
                if not full_name:
                    continue
                node_role_map[full_name] = role
                # Also map short name (without domain) to the same role
                if '.' in full_name:
                    short_name = full_name.split('.')[0]
                    node_role_map[short_name] = role
        
        role_data: Dict[str, Dict[str, Dict[str, float]]] = {
            'controlplane': {}, 'worker': {}, 'infra': {}, 'workload': {}
        }
        
        for node_name, values in node_data.items():
            # Map node to role using prepared map; default to worker if unknown
            role = node_role_map.get(node_name)
            if role is None and '.' in node_name:
                # Try short/long mapping opposites
                short = node_name.split('.')[0]
                role = node_role_map.get(short)
            if role is None:
                role = 'worker'
            stats = self._calculate_stats(values)
            # Keep node_name as-is (may be FQDN); keys will count correctly
            role_data[role][node_name] = stats
        
        if role_data['worker']:
            worker_sorted = sorted(
                role_data['worker'].items(), key=lambda x: x[1]['max'], reverse=True
            )[:3]
            role_data['worker'] = dict(worker_sorted)
        
        return {
            'metric': metric_name,
            'unit': metric_config.get('unit', ''),
            'description': metric_config.get('description', ''),
            'nodes': role_data
        }
    
    async def collect_softnet_processed_total(
        self, start: str, end: str, step: str = '15s'
    ) -> Dict[str, Any]:
        """Collect softnet processed packets statistics"""
        return await self._collect_metric_for_nodes(
            'node_softnet_packages_processed_total', start, end, step
        )
    
    async def collect_softnet_dropped_total(
        self, start: str, end: str, step: str = '15s'
    ) -> Dict[str, Any]:
        """Collect softnet dropped packets statistics"""
        return await self._collect_metric_for_nodes(
            'node_softnet_packages_dropped_total', start, end, step
        )
    
    async def collect_softnet_out_of_quota(
        self, start: str, end: str, step: str = '15s'
    ) -> Dict[str, Any]:
        """Collect softnet out of quota events"""
        return await self._collect_metric_for_nodes(
            'softnet_out_of_quota', start, end, step
        )
    
    async def collect_softnet_cpu_rps(
        self, start: str, end: str, step: str = '15s'
    ) -> Dict[str, Any]:
        """Collect softnet CPU RPS statistics"""
        return await self._collect_metric_for_nodes(
            'softnet_cpu_rps', start, end, step
        )
    
    async def collect_softnet_flow_limit_count(
        self, start: str, end: str, step: str = '15s'
    ) -> Dict[str, Any]:
        """Collect softnet flow limit count"""
        return await self._collect_metric_for_nodes(
            'softnet_flow_limit_count', start, end, step
        )
    
    async def collect_all_softnet_metrics(
        self, start: str, end: str, step: str = '15s'
    ) -> Dict[str, Any]:
        """Collect all softnet metrics"""
        results = await asyncio.gather(
            self.collect_softnet_processed_total(start, end, step),
            self.collect_softnet_dropped_total(start, end, step),
            self.collect_softnet_out_of_quota(start, end, step),
            self.collect_softnet_cpu_rps(start, end, step),
            self.collect_softnet_flow_limit_count(start, end, step),
            return_exceptions=True
        )
        
        metric_names = [
            'softnet_processed_total', 'softnet_dropped_total',
            'softnet_out_of_quota', 'softnet_cpu_rps', 'softnet_flow_limit_count'
        ]
        
        metrics_data = {}
        for name, result in zip(metric_names, results):
            metrics_data[name] = {'error': str(result)} if isinstance(result, Exception) else result
        
        summary = self._calculate_summary(metrics_data)
        
        return {
            'category': 'network_socket_softnet',
            'collection_time': datetime.now(timezone.utc).isoformat(),
            'time_range': {'start': start, 'end': end, 'step': step},
            'metrics': metrics_data,
            'summary': summary
        }
    
    def _calculate_summary(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate summary statistics across all metrics"""
        summary = {
            'total_metrics': len(metrics_data),
            'metrics_with_data': 0,
            'metrics_with_errors': 0,
            'node_counts': {'controlplane': 0, 'worker': 0, 'infra': 0, 'workload': 0}
        }
        
        all_nodes = set()
        for metric_name, metric_data in metrics_data.items():
            if 'error' in metric_data:
                summary['metrics_with_errors'] += 1
                continue
            
            summary['metrics_with_data'] += 1
            nodes_by_role = metric_data.get('nodes', {})
            for role, nodes in nodes_by_role.items():
                if isinstance(nodes, dict):
                    node_count = len(nodes)
                    if node_count > summary['node_counts'][role]:
                        summary['node_counts'][role] = node_count
                    all_nodes.update(nodes.keys())
        
        summary['total_unique_nodes'] = len(all_nodes)
        return summary


async def collect_softnet_statistics(
    auth_client, config: Config, start_time: str, end_time: str, step: str = '15s'
) -> Dict[str, Any]:
    """
    Convenience function to collect all softnet statistics
    
    Args:
        auth_client: OpenShift authentication client
        config: Configuration object
        start_time: Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        end_time: End time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        step: Query step interval (default: 15s)
    
    Returns:
        Dictionary containing all softnet statistics grouped by node role
    """
    async with socketStatSoftNetCollector(auth_client, config) as collector:
        return await collector.collect_all_softnet_metrics(start_time, end_time, step)