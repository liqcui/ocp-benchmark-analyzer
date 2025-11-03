#!/usr/bin/env python3
"""
Network Socket Memory Collector
Collects and analyzes network socket memory metrics from Prometheus

This module collects network socket memory statistics and organizes them by node groups:
- controlplane: All master/control-plane nodes
- infra: All infrastructure nodes  
- workload: All workload nodes
- worker: Top 3 worker nodes by metric value

Results include avg and max values for each node in JSON format.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone

from ocauth.openshift_auth import OpenShiftAuth as OVNKAuth
from tools.utils.promql_basequery import PrometheusBaseQuery
from config.metrics_config_reader import Config
from tools.utils.promql_utility import mcpToolsUtility

logger = logging.getLogger(__name__)


class socketStatMemCollector:
    """Collector for network socket memory statistics
    
    Supports two initialization styles for compatibility:
    1) socketStatMemCollector(config, auth)
    2) socketStatMemCollector(prometheus_client, config, utility)
    """
    
    def __init__(self, first_arg, second_arg: Optional[Any] = None, third_arg: Optional[Any] = None):
        # Compatibility: allow (prometheus_client, config, utility) or (config, auth)
        if isinstance(first_arg, PrometheusBaseQuery):
            # New style: provided Prometheus client and utility directly
            self.prometheus_client: Optional[PrometheusBaseQuery] = first_arg
            self.config: Config = second_arg  # type: ignore[assignment]
            self.utility = third_arg if third_arg is not None else mcpToolsUtility()
            self.auth: Optional[OVNKAuth] = None
        else:
            # Original style: provided config and optional auth
            self.config = first_arg
            self.auth = second_arg
            self.prometheus_client = None
            self.utility = mcpToolsUtility(auth_client=self.auth)
        
        self.category = "network_socket_mem"
        
    async def __aenter__(self):
        """Async context manager entry"""
        if self.prometheus_client is None and self.auth and self.auth.prometheus_url:
            self.prometheus_client = PrometheusBaseQuery(
                prometheus_url=self.auth.prometheus_url,
                token=self.auth.token
            )
            await self.prometheus_client.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.prometheus_client:
            try:
                await self.prometheus_client.__aexit__(exc_type, exc_val, exc_tb)
            except Exception:
                pass
    
    def _get_metrics_config(self) -> List[Dict[str, Any]]:
        """Get metrics configuration for network_socket_mem category"""
        return self.config.get_metrics_by_category(self.category)
    
    async def _execute_metric_query(self, metric_name: str, query_expr: str) -> Dict[str, Any]:
        """Execute a single metric query and return formatted results"""
        if not self.prometheus_client:
            return {'error': 'Prometheus client not initialized'}
        
        try:
            result = await self.prometheus_client.query_instant(query_expr)
            formatted = self.prometheus_client.format_query_result(result, include_labels=True)
            return {'data': formatted}
        except Exception as e:
            logger.error(f"Error executing query for {metric_name}: {e}")
            return {'error': str(e)}
    
    def _extract_node_name(self, instance: str) -> str:
        """Extract node name from instance label"""
        # Handle formats like "node-name:9100" or "node-name"
        node = instance.split(':')[0]
        # Remove FQDN suffix if present
        if '.' in node:
            node = node.split('.')[0]
        return node
    
    def _calculate_stats(self, values: List[float]) -> Dict[str, float]:
        """Calculate avg and max from list of values"""
        if not values:
            return {'avg': 0.0, 'max': 0.0}
        return {
            'avg': sum(values) / len(values),
            'max': max(values)
        }
    
    async def _process_metric_by_node_groups(self, metric_name: str, metric_data: Dict[str, Any], 
                                              node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Process metric data and organize by node groups"""
        if 'error' in metric_data:
            return {'error': metric_data['error']}
        
        # Create node name to role mapping
        node_to_role = {}
        for role, nodes in node_groups.items():
            for node_info in nodes:
                node_name = node_info['name']
                # Store both full name and short name
                node_to_role[node_name] = role
                if '.' in node_name:
                    short_name = node_name.split('.')[0]
                    node_to_role[short_name] = role
        
        # Organize data by role
        role_data = {
            'controlplane': {},
            'worker': {},
            'infra': {},
            'workload': {}
        }
        
        for item in metric_data.get('data', []):
            instance = item.get('labels', {}).get('instance', '')
            if not instance:
                continue
            
            node_name = self._extract_node_name(instance)
            role = node_to_role.get(node_name, 'worker')
            value = item.get('value')
            
            if value is None:
                continue
            
            if node_name not in role_data[role]:
                role_data[role][node_name] = []
            role_data[role][node_name].append(float(value))
        
        # Calculate statistics and format results
        result = {}
        
        for role in ['controlplane', 'infra', 'workload']:
            if role_data[role]:
                result[role] = {}
                for node_name, values in role_data[role].items():
                    result[role][node_name] = self._calculate_stats(values)
        
        # For workers, get top 3 by max value
        if role_data['worker']:
            worker_stats = {}
            for node_name, values in role_data['worker'].items():
                worker_stats[node_name] = self._calculate_stats(values)
            
            # Sort by max value and take top 3
            top_workers = sorted(
                worker_stats.items(),
                key=lambda x: x[1]['max'],
                reverse=True
            )[:3]
            
            result['worker'] = {node: stats for node, stats in top_workers}
        
        return result
    
    async def collect_node_sockstat_frag_memory(self) -> Dict[str, Any]:
        """Collect node_sockstat_FRAG_memory metric"""
        metric_config = self.config.get_metric_by_name('node_sockstat_FRAG_memory')
        if not metric_config:
            return {'error': 'Metric configuration not found'}
        
        query = metric_config['expr']
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        # Replace $node_name with .* to get all nodes
        query = query.replace('$node_name', '.*')
        
        metric_data = await self._execute_metric_query('node_sockstat_FRAG_memory', query)
        result = await self._process_metric_by_node_groups('node_sockstat_FRAG_memory', metric_data, node_groups)
        
        return {
            'metric': 'node_sockstat_FRAG_memory',
            'unit': metric_config.get('unit', 'count'),
            'description': metric_config.get('description', ''),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'nodes': result
        }
    
    async def collect_tcp_kernel_buffer_memory_pages(self) -> Dict[str, Any]:
        """Collect TCP_Kernel_Buffer_Memory_Pages metric"""
        metric_config = self.config.get_metric_by_name('TCP_Kernel_Buffer_Memory_Pages')
        if not metric_config:
            return {'error': 'Metric configuration not found'}
        
        query = metric_config['expr']
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        query = query.replace('$node_name', '.*')
        
        metric_data = await self._execute_metric_query('TCP_Kernel_Buffer_Memory_Pages', query)
        result = await self._process_metric_by_node_groups('TCP_Kernel_Buffer_Memory_Pages', metric_data, node_groups)
        
        return {
            'metric': 'TCP_Kernel_Buffer_Memory_Pages',
            'unit': metric_config.get('unit', 'count'),
            'description': metric_config.get('description', ''),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'nodes': result
        }
    
    async def collect_udp_kernel_buffer_memory_pages(self) -> Dict[str, Any]:
        """Collect UDP_Kernel_Buffer_Memory_Pages metric"""
        metric_config = self.config.get_metric_by_name('UDP_Kernel_Buffer_Memory_Pages')
        if not metric_config:
            return {'error': 'Metric configuration not found'}
        
        query = metric_config['expr']
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        query = query.replace('$node_name', '.*')
        
        metric_data = await self._execute_metric_query('UDP_Kernel_Buffer_Memory_Pages', query)
        result = await self._process_metric_by_node_groups('UDP_Kernel_Buffer_Memory_Pages', metric_data, node_groups)
        
        return {
            'metric': 'UDP_Kernel_Buffer_Memory_Pages',
            'unit': metric_config.get('unit', 'count'),
            'description': metric_config.get('description', ''),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'nodes': result
        }
    
    async def collect_node_sockstat_tcp_mem_bytes(self) -> Dict[str, Any]:
        """Collect node_sockstat_TCP_mem_bytes metric"""
        metric_config = self.config.get_metric_by_name('node_sockstat_TCP_mem_bytes')
        if not metric_config:
            return {'error': 'Metric configuration not found'}
        
        query = metric_config['expr']
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        query = query.replace('$node_name', '.*')
        
        metric_data = await self._execute_metric_query('node_sockstat_TCP_mem_bytes', query)
        result = await self._process_metric_by_node_groups('node_sockstat_TCP_mem_bytes', metric_data, node_groups)
        
        return {
            'metric': 'node_sockstat_TCP_mem_bytes',
            'unit': metric_config.get('unit', 'bytes'),
            'description': metric_config.get('description', ''),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'nodes': result
        }
    
    async def collect_node_sockstat_udp_mem_bytes(self) -> Dict[str, Any]:
        """Collect node_sockstat_UDP_mem_bytes metric"""
        metric_config = self.config.get_metric_by_name('node_sockstat_UDP_mem_bytes')
        if not metric_config:
            return {'error': 'Metric configuration not found'}
        
        query = metric_config['expr']
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        
        query = query.replace('$node_name', '.*')
        
        metric_data = await self._execute_metric_query('node_sockstat_UDP_mem_bytes', query)
        result = await self._process_metric_by_node_groups('node_sockstat_UDP_mem_bytes', metric_data, node_groups)
        
        return {
            'metric': 'node_sockstat_UDP_mem_bytes',
            'unit': metric_config.get('unit', 'bytes'),
            'description': metric_config.get('description', ''),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'nodes': result
        }
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all network socket memory metrics"""
        if not self.prometheus_client:
            return {
                'category': self.category,
                'error': 'Prometheus client not initialized',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        metrics = self._get_metrics_config()
        
        if not metrics:
            return {
                'category': self.category,
                'error': 'No metrics found in configuration',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        
        # Execute all metric collection concurrently
        tasks = [
            self.collect_node_sockstat_frag_memory(),
            self.collect_tcp_kernel_buffer_memory_pages(),
            self.collect_udp_kernel_buffer_memory_pages(),
            self.collect_node_sockstat_tcp_mem_bytes(),
            self.collect_node_sockstat_udp_mem_bytes()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Format results
        metrics_data = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error collecting metric: {result}")
                continue
            if result and 'error' not in result:
                metrics_data.append(result)
        
        return {
            'category': self.category,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'timezone': 'UTC',
            'metrics_count': len(metrics_data),
            'metrics': metrics_data
        }


# Backward-compatible alias expected by server imports
SocketStatMemCollector = socketStatMemCollector


async def main():
    """Main function for testing"""
    import sys
    
    # Initialize configuration
    config = Config(metrics_file='config/metrics-net.yml')
    
    # Validate configuration
    validation = config.validate_config()
    if not validation['valid']:
        print(f"Configuration errors: {validation['errors']}")
        sys.exit(1)
    
    # Initialize authentication
    auth = OVNKAuth()
    if not await auth.authenticate():
        print("Authentication failed")
        sys.exit(1)
    
    # Collect metrics
    async with socketStatMemCollector(config=config, auth=auth) as collector:
        results = await collector.collect_all_metrics()
        
        # Print results
        import json
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())