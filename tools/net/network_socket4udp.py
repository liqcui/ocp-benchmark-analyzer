#!/usr/bin/env python3
"""
OVNK Benchmark Prometheus Network Socket UDP Collector
Collects UDP socket statistics from Prometheus for OVN-Kubernetes benchmarking

This module provides the SocketStatUDPCollector class for collecting UDP socket
statistics from Prometheus. It groups nodes by role (controlplane/worker/infra/workload)
and returns top 3 workers by usage while including all other node types.

Usage:
    collector = SocketStatUDPCollector(prometheus_client, config, utility)
    result = await collector.collect_all_metrics()
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SocketStatUDPCollector:
    """Collector for UDP socket statistics"""
    
    def __init__(self, prometheus_client, config, utility):
        """
        Initialize UDP socket collector
        
        Args:
            prometheus_client: PrometheusBaseQuery instance
            config: Configuration object
            utility: mcpToolsUtility instance
        """
        self.prometheus_client = prometheus_client
        self.config = config
        self.utility = utility
        
        # Get UDP socket metrics from config
        self.metrics = self.config.get_metrics_by_category('network_socket_udp')
        
    async def collect_socket_udp_inuse(self, node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Collect UDP inuse socket statistics"""
        metric = next((m for m in self.metrics if m['name'] == 'socket_udp_inuse'), None)
        if not metric:
            return {}
        
        results = {}
        for role, nodes in node_groups.items():
            role_results = []
            
            for node in nodes:
                node_name = node['name']
                query = metric['expr'].replace('$node_name', node_name)
                
                try:
                    data = await self.prometheus_client.query_instant(query)
                    formatted = self.prometheus_client.format_query_result(data)
                    
                    if formatted:
                        values = [r['value'] for r in formatted if r.get('value') is not None]
                        if values:
                            role_results.append({
                                'node': node_name,
                                'avg': round(sum(values) / len(values), 2),
                                'max': round(max(values), 2)
                            })
                except Exception as e:
                    logger.debug(f"Error querying {node_name}: {e}")
            
            # Filter top 3 for workers
            if role == 'worker' and len(role_results) > 3:
                role_results = sorted(role_results, key=lambda x: x['max'], reverse=True)[:3]
            
            results[role] = role_results
        
        return {
            'metric': 'socket_udp_inuse',
            'unit': metric.get('unit', 'count'),
            'description': metric.get('description', ''),
            'data': results
        }
    
    async def collect_socket_udp_lite_inuse(self, node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Collect UDP lite inuse socket statistics"""
        metric = next((m for m in self.metrics if m['name'] == 'socket_udp_lite_inuse'), None)
        if not metric:
            return {}
        
        results = {}
        for role, nodes in node_groups.items():
            role_results = []
            
            for node in nodes:
                node_name = node['name']
                query = metric['expr'].replace('$node_name', node_name)
                
                try:
                    data = await self.prometheus_client.query_instant(query)
                    formatted = self.prometheus_client.format_query_result(data)
                    
                    if formatted:
                        values = [r['value'] for r in formatted if r.get('value') is not None]
                        if values:
                            role_results.append({
                                'node': node_name,
                                'avg': round(sum(values) / len(values), 2),
                                'max': round(max(values), 2)
                            })
                except Exception as e:
                    logger.debug(f"Error querying {node_name}: {e}")
            
            # Filter top 3 for workers
            if role == 'worker' and len(role_results) > 3:
                role_results = sorted(role_results, key=lambda x: x['max'], reverse=True)[:3]
            
            results[role] = role_results
        
        return {
            'metric': 'socket_udp_lite_inuse',
            'unit': metric.get('unit', 'count'),
            'description': metric.get('description', ''),
            'data': results
        }
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """
        Collect all UDP socket metrics
        
        Returns:
            JSON format result with avg, max usage, unit per node grouped by role
        """
        # Get node groups
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        # Collect all metrics concurrently
        tasks = [
            self.collect_socket_udp_inuse(node_groups),
            self.collect_socket_udp_lite_inuse(node_groups)
        ]
        
        metric_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Build final result
        result = {
            'category': 'network_socket_udp',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'metrics': []
        }
        
        for metric_data in metric_results:
            if isinstance(metric_data, dict) and metric_data:
                result['metrics'].append(metric_data)
        
        return result


async def main():
    """Main function for testing"""
    import sys
    sys.path.insert(0, '.')
    
    from tools.utils.promql_basequery import PrometheusBaseQuery
    from config.metrics_config_reader import Config
    from tools.utils.promql_utility import mcpToolsUtility
    
    # Initialize configuration
    config = Config(metrics_file='config/metrics-net.yml')
    
    # Get Prometheus URL from config or environment
    prometheus_url = config.prometheus.url
    prometheus_token = config.prometheus.token
    
    if not prometheus_url:
        print("Error: PROMETHEUS_URL not configured")
        return
    
    # Initialize components
    async with PrometheusBaseQuery(prometheus_url, prometheus_token) as prom_client:
        utility = mcpToolsUtility()
        collector = SocketStatUDPCollector(prom_client, config, utility)
        
        # Collect metrics
        print("Collecting UDP socket statistics...")
        result = await collector.collect_all_metrics()
        
        # Print result
        import json
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(main())