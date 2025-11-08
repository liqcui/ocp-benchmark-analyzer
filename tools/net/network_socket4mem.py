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
import os
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from pathlib import Path

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
    
    def __init__(self, first_arg=None, second_arg: Optional[Any] = None, third_arg: Optional[Any] = None, **kwargs):
        # Compatibility: support both positional and keyword initialization styles
        prometheus_client_kw = kwargs.get('prometheus_client')
        config_kw = kwargs.get('config')
        utility_kw = kwargs.get('utility')
        auth_kw = kwargs.get('auth') or kwargs.get('auth_client')

        if prometheus_client_kw is not None or config_kw is not None or utility_kw is not None or auth_kw is not None:
            # Keyword-args path
            if isinstance(prometheus_client_kw, PrometheusBaseQuery):
                self.prometheus_client: Optional[PrometheusBaseQuery] = prometheus_client_kw
                self.config: Config = config_kw  # type: ignore[assignment]
                self.utility = utility_kw if utility_kw is not None else mcpToolsUtility()
                self.auth: Optional[OVNKAuth] = auth_kw
            else:
                # config/auth provided
                self.config = config_kw if config_kw is not None else first_arg
                self.auth = auth_kw if auth_kw is not None else second_arg
                self.prometheus_client = None
                self.utility = utility_kw if utility_kw is not None else mcpToolsUtility(auth_client=self.auth)
        else:
            # Positional path
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
        
        # Load metrics configuration
        logger.info("Initializing socketStatMemCollector...")
        self._load_metrics_config()
        
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
    
    def _load_metrics_config(self):
        """Load metrics configuration from config/metrics-net.yml"""
        try:
            # Ensure we have a Config instance
            if not isinstance(self.config, Config):
                self.config = Config()
            
            # Find the config/metrics-net.yml file
            base_dir = Path(__file__).parent.parent.parent  # Go up to project root
            metrics_path = base_dir / 'config' / 'metrics-net.yml'
            
            # Try relative path if not found
            if not metrics_path.exists():
                metrics_path = Path('config') / 'metrics-net.yml'
            
            if metrics_path.exists():
                result = self.config.load_metrics_file(str(metrics_path))
                if result.get('success'):
                    metrics_count = result.get('metrics_loaded', 0)
                    logger.info(f"✅ Loaded {metrics_count} metrics from {self.category} category (socketStatMemCollector)")
                else:
                    logger.warning(f"Failed to load metrics from {metrics_path}: {result.get('error')}")
            else:
                logger.warning(f"Metrics file not found at {metrics_path}")
                
        except Exception as e:
            logger.warning(f"Could not load metrics configuration: {e}")
    
    def _get_metrics_config(self) -> List[Dict[str, Any]]:
        """Get metrics configuration for network_socket_mem category"""
        return self.config.get_metrics_by_category(self.category)
    
    async def _execute_metric_query(self, metric_name: str, query_expr: str) -> Dict[str, Any]:
        """Execute a single metric query and return raw Prometheus data"""
        if not self.prometheus_client:
            return {'error': 'Prometheus client not initialized'}
        
        try:
            logger.debug(f"Executing query for {metric_name}: {query_expr}")
            result = await self.prometheus_client.query_instant(query_expr)
            
            # Return the raw Prometheus data structure
            # Result should have: {'resultType': 'vector', 'result': [...]}
            logger.debug(f"Query returned {len(result.get('result', []))} results for {metric_name}")
            return result
            
        except Exception as e:
            logger.error(f"Error executing query for {metric_name}: {e}")
            return {'error': str(e)}
    
    def _extract_node_from_labels(self, labels: Dict[str, Any]) -> str:
        """Extract node identifier from metric labels"""
        # Try common label keys for node information
        for key in ['node', 'nodename', 'kubernetes_node', 'instance', 'exported_instance']:
            val = labels.get(key)
            if val:
                # Remove port if present (e.g., "node:9100" -> "node")
                node = str(val).split(':')[0]
                return node
        
        # Fallback: try any label containing 'node'
        for k, v in labels.items():
            if 'node' in k.lower() and v:
                node = str(v).split(':')[0]
                return node
        
        return ''
    
    def _build_zero_result(self, node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Prefill zero stats for nodes by role to avoid empty results"""
        result = {}
        # Helper to choose full names preferentially and de-duplicate
        def unique_full_names(nodes: List[Dict[str, Any]]) -> List[str]:
            fulls = set()
            short_to_full = {}
            names = [n.get('name') for n in nodes if isinstance(n, dict) and n.get('name')]
            # Build short->full mapping
            for name in names:
                if '.' in name:
                    fulls.add(name)
                    short_to_full[name.split('.')[0]] = name
            unique: List[str] = []
            seen = set()
            for name in names:
                preferred = short_to_full.get(name.split('.')[0], name)
                if preferred not in seen:
                    seen.add(preferred)
                    unique.append(preferred)
            return unique
        # Include all nodes for controlplane/infra/workload
        for role in ['controlplane', 'infra', 'workload']:
            if node_groups.get(role):
                result[role] = {}
                for node_name in unique_full_names(node_groups[role]):
                    result[role][node_name] = {'avg': 0.0, 'max': 0.0}
        # Workers: limit to top 3 entries (zeros)
        if node_groups.get('worker'):
            result['worker'] = {}
            worker_names = unique_full_names(node_groups['worker'])[:3]
            for node_name in worker_names:
                result['worker'][node_name] = {'avg': 0.0, 'max': 0.0}
        return result
    
    async def _process_metric_by_node_groups(self, metric_name: str, metric_data: Dict[str, Any], 
                                              node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Process metric data and organize by node groups"""
        if 'error' in metric_data:
            logger.warning(f"Error in metric data for {metric_name}: {metric_data['error']}")
            return self._build_zero_result(node_groups)
        
        # Extract results from Prometheus response: support both raw and formatted outputs
        results = []
        if isinstance(metric_data, dict):
            if 'result' in metric_data and isinstance(metric_data.get('result'), list):
                results = metric_data.get('result') or []
            elif 'data' in metric_data and isinstance(metric_data.get('data'), dict):
                data_block = metric_data.get('data') or {}
                if 'result' in data_block and isinstance(data_block.get('result'), list):
                    results = data_block.get('result') or []
            elif 'data' in metric_data and isinstance(metric_data.get('data'), list):
                # formatted list
                results = metric_data.get('data') or []
        
        if not results:
            logger.warning(f"No results returned for {metric_name}")
            return self._build_zero_result(node_groups)
        
        logger.debug(f"Processing {len(results)} results for {metric_name}")
        
        # Build node name to role mapping (both FQDN and short names)
        node_to_role = {}
        node_to_full_name = {}  # Map short name -> full name
        
        for role, nodes in node_groups.items():
            for node_info in nodes:
                full_name = node_info['name']
                node_to_role[full_name] = role
                
                # Add short name mapping
                if '.' in full_name:
                    short_name = full_name.split('.')[0]
                    node_to_role[short_name] = role
                    node_to_full_name[short_name] = full_name
        
        logger.debug(f"Built node mapping with {len(node_to_role)} entries")
        
        # Organize data by role
        role_data = {
            'controlplane': {},
            'worker': {},
            'infra': {},
            'workload': {}
        }
        
        # Process each result
        for item in results:
            # Support raw vector items ({metric:{...}, value:[ts, "v"]})
            # and formatted items ({labels:{...}, value: "v" or float})
            metric_labels = item.get('metric') or item.get('labels') or {}
            node_name = self._extract_node_from_labels(metric_labels)
            if not node_name:
                logger.debug(f"Could not extract node name from labels: {metric_labels}")
                continue
            
            value_raw = item.get('value')
            value: Optional[float] = None
            if isinstance(value_raw, (list, tuple)) and len(value_raw) >= 2:
                # Raw vector [timestamp, "value"]
                try:
                    value = float(value_raw[1])
                except (ValueError, TypeError):
                    value = None
            else:
                # Formatted numeric or numeric string
                try:
                    value = float(value_raw)
                except (ValueError, TypeError):
                    value = None
            
            if value is None:
                logger.debug(f"Invalid or missing value for item: {item}")
                continue
            
            # Match node to role (try both full and short names)
            short_node = node_name.split('.')[0] if '.' in node_name else node_name
            role = node_to_role.get(node_name) or node_to_role.get(short_node, 'worker')
            
            # Get the full node name to use as key
            full_node_name = node_to_full_name.get(short_node, node_name)
            
            # Initialize node data list if not exists
            if full_node_name not in role_data[role]:
                role_data[role][full_node_name] = []
            
            role_data[role][full_node_name].append(value)
            logger.debug(f"Added value {value} for node {full_node_name} in role {role}")
        
        # Calculate statistics and format results
        result = {}
        
        # Process controlplane, infra, workload - include all nodes
        for role in ['controlplane', 'infra', 'workload']:
            if role_data[role]:
                result[role] = {}
                for node_name, values in role_data[role].items():
                    result[role][node_name] = self._calculate_stats(values)
                logger.debug(f"Role {role}: {len(result[role])} nodes with data")
            else:
                # Prefill zeros to avoid empty roles if nodes exist
                if node_groups.get(role):
                    result[role] = {}
                    # De-duplicate and prefer full names
                    fulls = set()
                    short_to_full = {}
                    for node_info in node_groups[role]:
                        name = node_info.get('name')
                        if not name:
                            continue
                        if '.' in name:
                            fulls.add(name)
                            short_to_full[name.split('.')[0]] = name
                    used = set()
                    for node_info in node_groups[role]:
                        name = node_info.get('name')
                        if not name:
                            continue
                        preferred = short_to_full.get(name.split('.')[0], name)
                        if preferred in used:
                            continue
                        used.add(preferred)
                        result[role][preferred] = {'avg': 0.0, 'max': 0.0}
        
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
            logger.debug(f"Selected top 3 workers: {list(result['worker'].keys())}")
        else:
            # Prefill zeros for worker role if nodes exist but no data matched
            if node_groups.get('worker'):
                result['worker'] = {}
                # De-duplicate and prefer full names, then limit to top 3
                worker_nodes = node_groups['worker']
                fulls = set()
                short_to_full = {}
                for node_info in worker_nodes:
                    name = node_info.get('name')
                    if not name:
                        continue
                    if '.' in name:
                        fulls.add(name)
                        short_to_full[name.split('.')[0]] = name
                unique: List[str] = []
                used = set()
                for node_info in worker_nodes:
                    name = node_info.get('name')
                    if not name:
                        continue
                    preferred = short_to_full.get(name.split('.')[0], name)
                    if preferred in used:
                        continue
                    used.add(preferred)
                    unique.append(preferred)
                for node_name in unique[:3]:
                    result['worker'][node_name] = {'avg': 0.0, 'max': 0.0}
        
        return result
    
    def _calculate_stats(self, values: List[float]) -> Dict[str, float]:
        """Calculate avg and max from list of values"""
        if not values:
            return {'avg': 0.0, 'max': 0.0}
        return {
            'avg': round(sum(values) / len(values), 2),
            'max': round(max(values), 2)
        }
    
    async def _collect_single_metric(self, metric_name: str) -> Dict[str, Any]:
        """Collect a single metric by name"""
        metric_config = self.config.get_metric_by_name(metric_name)
        if not metric_config:
            logger.error(f'Metric configuration not found for {metric_name}')
            return {'error': 'Metric configuration not found'}
        
        query = metric_config['expr']
        
        # Get node groups for organizing results
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        logger.debug(f"Retrieved node groups: {[(role, len(nodes)) for role, nodes in node_groups.items()]}")
        
        # Replace $node_name with .* to get all nodes
        query = query.replace('$node_name', '.*')
        logger.info(f"query in _collect_single_metric: {query}")
        
        # Execute query
        metric_data = await self._execute_metric_query(metric_name, query)
        
        # Process and organize by node groups
        result = await self._process_metric_by_node_groups(metric_name, metric_data, node_groups)
        
        return {
            'metric': metric_name,
            'unit': metric_config.get('unit', 'count'),
            'description': metric_config.get('description', ''),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'nodes': result
        }
    
    async def collect_node_sockstat_frag_memory(self) -> Dict[str, Any]:
        """Collect node_sockstat_FRAG_memory metric"""
        return await self._collect_single_metric('node_sockstat_FRAG_memory')
    
    async def collect_tcp_kernel_buffer_memory_pages(self) -> Dict[str, Any]:
        """Collect TCP_Kernel_Buffer_Memory_Pages metric"""
        return await self._collect_single_metric('TCP_Kernel_Buffer_Memory_Pages')
    
    async def collect_udp_kernel_buffer_memory_pages(self) -> Dict[str, Any]:
        """Collect UDP_Kernel_Buffer_Memory_Pages metric"""
        return await self._collect_single_metric('UDP_Kernel_Buffer_Memory_Pages')
    
    async def collect_node_sockstat_tcp_mem_bytes(self) -> Dict[str, Any]:
        """Collect node_sockstat_TCP_mem_bytes metric"""
        return await self._collect_single_metric('node_sockstat_TCP_mem_bytes')
    
    async def collect_node_sockstat_udp_mem_bytes(self) -> Dict[str, Any]:
        """Collect node_sockstat_UDP_mem_bytes metric"""
        return await self._collect_single_metric('node_sockstat_UDP_mem_bytes')
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all network socket memory metrics"""
        if not self.prometheus_client:
            return {
                'status': 'error',
                'category': self.category,
                'error': 'Prometheus client not initialized',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        
        metrics = self._get_metrics_config()
        
        if not metrics:
            logger.warning(f"No metrics found for category {self.category}")
            return {
                'status': 'error',
                'category': self.category,
                'error': 'No metrics found in configuration',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        
        logger.info(f"Collecting {len(metrics)} metrics for category {self.category}")
        
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
                logger.info(f"✓ Collected metric: {result.get('metric')} - nodes: {list(result.get('nodes', {}).keys())}")
            else:
                logger.warning(f"Failed to collect metric: {result}")
        
        return {
            'status': 'success',
            'data': {
                'category': self.category,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'timezone': 'UTC',
                'metrics_count': len(metrics_data),
                'metrics': metrics_data
            },
            'error': None,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'category': self.category,
            'duration': None
        }


# Backward-compatible alias expected by server imports
SocketStatMemCollector = socketStatMemCollector


async def main():
    """Main function for testing"""
    import sys
    
    # Initialize configuration
    config = Config()
    
    # Load config/metrics-net.yml
    base_dir = Path(__file__).parent.parent.parent
    metrics_path = base_dir / 'config' / 'metrics-net.yml'
    
    if not metrics_path.exists():
        metrics_path = Path('config') / 'metrics-net.yml'
    
    if metrics_path.exists():
        result = config.load_metrics_file(str(metrics_path))
        print(f"Load result: {result}")
    else:
        print(f"Metrics file not found at {metrics_path}")
        sys.exit(1)
    
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