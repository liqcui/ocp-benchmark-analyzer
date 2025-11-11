#!/usr/bin/env python3
"""
OVNK Benchmark Prometheus Network Socket TCP Collector
Collects TCP socket statistics from Prometheus metrics
"""

import asyncio
import logging
import re
import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from config.metrics_config_reader import Config

logger = logging.getLogger(__name__)


class socketStatTCPCollector:
    """Collector for network socket TCP metrics"""
    
    def __init__(self, config: Config, auth_client=None):
        self.config = config
        self.auth_client = auth_client
        self.utility = mcpToolsUtility(auth_client=auth_client)
        self.prometheus_client: Optional[PrometheusBaseQuery] = None
        
        # Get TCP socket metrics from config
        self.tcp_metrics = config.get_metrics_by_category('network_socket_tcp')
        
    async def __aenter__(self):
        """Async context manager entry"""
        # Prefer explicit config; otherwise fall back to discovered auth_client settings
        prometheus_url = self.config.prometheus.url
        token = self.config.prometheus.token
        if not prometheus_url and self.auth_client:
            try:
                prometheus_url = getattr(self.auth_client, 'prometheus_url', None)
                token = getattr(self.auth_client, 'prometheus_token', token)
            except Exception:
                prometheus_url = None
        if prometheus_url:
            self.prometheus_client = PrometheusBaseQuery(
                prometheus_url=prometheus_url,
                token=token
            )
            await self.prometheus_client.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.prometheus_client:
            await self.prometheus_client.__aexit__(exc_type, exc_val, exc_tb)
    
    def _calculate_stats(self, values: List[float]) -> Dict[str, Any]:
        """Calculate avg and max from list of values"""
        if not values:
            return {'avg': 0.0, 'max': 0.0}
        
        return {
            'avg': round(sum(values) / len(values), 2),
            'max': round(max(values), 2)
        }
    
    def _get_top_workers(self, node_data: Dict[str, Dict], top_n: int = 3) -> Dict[str, Dict]:
        """Get top N worker nodes by max value"""
        if not node_data:
            return {}
        
        # Sort by max value descending
        sorted_nodes = sorted(
            node_data.items(),
            key=lambda x: x[1].get('max', 0),
            reverse=True
        )
        
        return dict(sorted_nodes[:top_n])
    
    def _resolve_query_variables(self, expr: str) -> str:
        """Resolve template variables like $node_name for Prometheus queries"""
        resolved = expr
        # Replace equality match with regex match to include all nodes
        resolved = re.sub(r'instance="?\$node_name"?', 'instance=~".*"', resolved)
        resolved = re.sub(r'node="?\$node_name"?', 'node=~".*"', resolved)
        # Replace remaining occurrences
        resolved = resolved.replace('$node_name', '.*')
        # Generic job variable support
        resolved = re.sub(r'job="?\$job"?', 'job=~".*"', resolved)
        resolved = resolved.replace('$job', '.*')
        # Rate interval placeholder (not typically used here but safe)
        resolved = resolved.replace('$__rate_interval', '5m').replace('[$__rate_interval]', '[5m]')
        return resolved

    async def _query_metric(self, metric_expr: str, metric_name: str) -> Dict[str, List[float]]:
        """Query a metric and return values grouped by node"""
        if not self.prometheus_client:
            logger.warning(f"No prometheus client available for {metric_name}")
            return {}
        
        try:
            resolved_expr = self._resolve_query_variables(metric_expr)
            logger.debug(f"Resolved expression for {metric_name}: {resolved_expr}")
            result = await self.prometheus_client.query_instant(resolved_expr)
            node_values = {}
            
            if 'result' in result:
                logger.debug(f"Metric {metric_name}: found {len(result['result'])} results")
                
                for item in result['result']:
                    metric_labels = item.get('metric', {})
                    
                    # Extract node name from various label patterns
                    instance = metric_labels.get('instance', '')
                    node = metric_labels.get('node', '')
                    nodename = metric_labels.get('nodename', '')
                    
                    # Try instance first (remove port if present)
                    node_name = instance.split(':')[0] if instance else ''
                    if not node_name:
                        node_name = node
                    if not node_name:
                        node_name = nodename
                    
                    if not node_name:
                        logger.debug(f"No node name found in labels: {metric_labels}")
                        continue
                    
                    # Extract value
                    value_data = item.get('value', [])
                    if len(value_data) >= 2:
                        try:
                            value = float(value_data[1])
                            if node_name not in node_values:
                                node_values[node_name] = []
                            node_values[node_name].append(value)
                            logger.debug(f"Added value {value} for node {node_name}")
                        except (ValueError, TypeError) as e:
                            logger.debug(f"Could not parse value: {value_data[1]}, error: {e}")
                            continue
                
                logger.info(f"Metric {metric_name}: collected data for {len(node_values)} nodes")
            else:
                logger.warning(f"Metric {metric_name}: no 'result' key in response")
            
            return node_values
            
        except Exception as e:
            logger.error(f"Error querying metric {metric_name}: {e}", exc_info=True)
            return {}
    
    async def _collect_metric_data(self, metric_name: str) -> Dict[str, Any]:
        """Generic method to collect data for any TCP metric"""
        metric = next((m for m in self.tcp_metrics if m['name'] == metric_name), None)
        if not metric:
            return {'error': f'Metric {metric_name} not found in configuration'}
        
        logger.info(f"Collecting metric: {metric_name}")
        logger.debug(f"Metric expression: {metric['expr']}")
        
        node_values = await self._query_metric(metric['expr'], metric['name'])
        logger.info(f"Query returned data for {len(node_values)} nodes")
        
        node_groups = await self.utility.get_node_groups(self.prometheus_client)
        logger.info(f"Node groups: {[(k, len(v)) for k, v in node_groups.items()]}")
        
        # Normalize node_values: consolidate short and full name entries
        # If we have both short (e.g., "ip-10-0-58-109") and full (e.g., "ip-10-0-58-109.us-east-2.compute.internal")
        # names for the same node, merge them and keep only the full name
        normalized_node_values = {}
        short_name_to_full = {}  # Map short names to their full names
        
        # First pass: build mapping of short names to full names from node_groups
        for role, nodes_info in node_groups.items():
            for node_info in nodes_info:
                full_name = node_info['name']
                short_name = full_name.split('.')[0]
                if short_name != full_name:
                    # If multiple nodes have same short name, prefer the one that matches exactly
                    if short_name not in short_name_to_full:
                        short_name_to_full[short_name] = full_name
        
        # Track which keys we've processed to avoid duplicates
        processed_keys = set()
        
        # Second pass: consolidate node_values
        # Process full names first, then short names to avoid conflicts
        full_name_keys = []
        short_name_keys = []
        other_keys = []
        
        for nv_key in node_values.keys():
            # Check if this key is a short name that has a corresponding full name
            if nv_key in short_name_to_full:
                short_name_keys.append(nv_key)
            else:
                # Check if this key is already a full name (exists in node_groups)
                is_full_name = False
                for role, nodes_info in node_groups.items():
                    for node_info in nodes_info:
                        if node_info['name'] == nv_key or node_info['name'].lower() == nv_key.lower():
                            is_full_name = True
                            break
                    if is_full_name:
                        break
                
                if is_full_name:
                    full_name_keys.append(nv_key)
                else:
                    other_keys.append(nv_key)
        
        # Process full names first
        for nv_key in full_name_keys:
            normalized_node_values[nv_key] = node_values[nv_key]
            processed_keys.add(nv_key)
        
        # Then process short names, mapping them to full names
        for nv_key in short_name_keys:
            if nv_key in processed_keys:
                continue
            full_name = short_name_to_full[nv_key]
            # If full name already exists in normalized_node_values, merge the values
            if full_name in normalized_node_values:
                # Merge values: combine lists
                normalized_node_values[full_name].extend(node_values[nv_key])
                logger.debug(f"Merged short name {nv_key} into full name {full_name}")
            else:
                # Use full name for the short name entry
                normalized_node_values[full_name] = node_values[nv_key]
                logger.debug(f"Mapped short name {nv_key} to full name {full_name}")
            processed_keys.add(nv_key)
        
        # Finally, process other keys (unknown format)
        for nv_key in other_keys:
            normalized_node_values[nv_key] = node_values[nv_key]
        
        # Build a set of all full node names for filtering (only full names, no short names)
        all_full_node_names = set()
        for role, nodes_info in node_groups.items():
            for node_info in nodes_info:
                full_name = node_info['name']
                all_full_node_names.add(full_name)
                all_full_node_names.add(full_name.lower())
        
        # Final filtering: remove any short name keys that have been mapped to full names
        # Only keep keys that are full names (exist in node_groups)
        final_node_values = {}
        for nv_key, nv_values in normalized_node_values.items():
            # Check if this is a full name (exists in node_groups)
            is_full_name = False
            for role, nodes_info in node_groups.items():
                for node_info in nodes_info:
                    if node_info['name'] == nv_key or node_info['name'].lower() == nv_key.lower():
                        is_full_name = True
                        break
                if is_full_name:
                    break
            
            if is_full_name:
                final_node_values[nv_key] = nv_values
            else:
                # Check if this is a short name that was already mapped
                # If it's in short_name_to_full, it was already mapped, so skip it
                if nv_key not in short_name_to_full:
                    # Unknown format, keep it but log a warning
                    final_node_values[nv_key] = nv_values
                    logger.debug(f"Keeping unmapped key: {nv_key}")
                else:
                    logger.debug(f"Removing short name key that was mapped: {nv_key}")
        
        node_values = final_node_values
        logger.info(f"After final filtering: {len(node_values)} unique nodes (only full names)")
        
        result = {
            'metric': metric['name'],
            'unit': metric['unit'],
            'description': metric['description'],
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'nodes': {}
        }
        
        # Track which node_values keys have been used to prevent duplicates
        used_node_value_keys = set()
        
        for role, nodes_info in node_groups.items():
            if not nodes_info:
                continue
                
            role_data = {}
            
            for node_info in nodes_info:
                node_name = node_info['name']
                
                # Try multiple name variations, prioritizing full node name
                values = None
                matched_key = None
                
                # First try exact full name matches (case-sensitive and case-insensitive)
                for name_variant in [node_name, node_name.lower()]:
                    if name_variant in node_values and name_variant not in used_node_value_keys:
                        values = node_values[name_variant]
                        matched_key = name_variant
                        logger.debug(f"Found values for {node_name} using exact full name match: {name_variant}")
                        break
                
                # If still no match, try partial matching - only match full names
                if not values:
                    # Sort keys by length (longest first) to prefer full names over short names
                    sorted_keys = sorted(node_values.keys(), key=len, reverse=True)
                    for nv_key in sorted_keys:
                        if nv_key in used_node_value_keys:
                            continue
                        # Only match if the key is a full name (exists in all_full_node_names)
                        # and it matches the node_name (either exact or the node_name starts with it)
                        if nv_key in all_full_node_names:
                            if node_name == nv_key or node_name.lower() == nv_key.lower() or node_name.startswith(nv_key) or nv_key.startswith(node_name):
                                values = node_values[nv_key]
                                matched_key = nv_key
                                logger.debug(f"Found values for {node_name} using partial match with full name: {nv_key}")
                                break
                
                if values:
                    # Always use full node name in the result, never short names
                    role_data[node_name] = self._calculate_stats(values)
                    # Mark this key as used to prevent duplicate matches
                    if matched_key:
                        used_node_value_keys.add(matched_key)
                else:
                    logger.debug(f"No values found for node {node_name} (tried full name variants)")
            
            # Apply top 3 filter for workers
            if role == 'worker' and len(role_data) > 3:
                logger.info(f"Filtering {len(role_data)} workers to top 3")
                role_data = self._get_top_workers(role_data, 3)
            
            if role_data:
                result['nodes'][role] = role_data
        
        logger.info(f"Result contains {sum(len(v) for v in result['nodes'].values())} total nodes across {len(result['nodes'])} roles")
        return result
    
    async def collect_socket_tcp_allocated(self) -> Dict[str, Any]:
        """Collect socket TCP allocated metric"""
        return await self._collect_metric_data('socket_tcp_allocated')
    
    async def collect_socket_tcp_inuse(self) -> Dict[str, Any]:
        """Collect socket TCP inuse metric"""
        return await self._collect_metric_data('socket_tcp_inuse')
    
    async def collect_socket_tcp_orphan(self) -> Dict[str, Any]:
        """Collect socket TCP orphan metric"""
        return await self._collect_metric_data('socket_tcp_orphan')
    
    async def collect_socket_tcp_tw(self) -> Dict[str, Any]:
        """Collect socket TCP time_wait metric"""
        return await self._collect_metric_data('socket_tcp_tw')
    
    async def collect_socket_used(self) -> Dict[str, Any]:
        """Collect socket used metric"""
        return await self._collect_metric_data('socket_used')
    
    async def collect_socket_frag_inuse(self) -> Dict[str, Any]:
        """Collect socket FRAG inuse metric"""
        return await self._collect_metric_data('node_sockstat_FRAG_inuse')
    
    async def collect_socket_raw_inuse(self) -> Dict[str, Any]:
        """Collect socket RAW inuse metric"""
        return await self._collect_metric_data('node_sockstat_RAW_inuse')
    
    async def collect_all_tcp_metrics(self) -> Dict[str, Any]:
        """Collect all TCP socket metrics"""
        tasks = {
            'socket_tcp_allocated': self.collect_socket_tcp_allocated(),
            'socket_tcp_inuse': self.collect_socket_tcp_inuse(),
            'socket_tcp_orphan': self.collect_socket_tcp_orphan(),
            'socket_tcp_tw': self.collect_socket_tcp_tw(),
            'socket_used': self.collect_socket_used(),
            'socket_frag_inuse': self.collect_socket_frag_inuse(),
            'socket_raw_inuse': self.collect_socket_raw_inuse()
        }
        
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        collected_data = {
            'category': 'network_socket_tcp',
            'collection_time': datetime.now(timezone.utc).isoformat(),
            'timezone': 'UTC',
            'metrics': {}
        }
        
        for metric_name, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                collected_data['metrics'][metric_name] = {
                    'error': str(result)
                }
                logger.error(f"Error collecting {metric_name}: {result}", exc_info=result)
            else:
                collected_data['metrics'][metric_name] = result
        
        return collected_data
    
    async def diagnose_connection(self) -> Dict[str, Any]:
        """Diagnose connection and data availability"""
        diagnosis = {
            'prometheus_client': self.prometheus_client is not None,
            'prometheus_url': self.config.prometheus.url if self.config.prometheus.url else 'Not configured',
            'metrics_loaded': len(self.tcp_metrics),
            'node_groups': {},
            'sample_query': {}
        }
        
        # Test node groups
        try:
            node_groups = await self.utility.get_node_groups(self.prometheus_client)
            diagnosis['node_groups'] = {
                role: {
                    'count': len(nodes),
                    'sample_names': [n['name'] for n in nodes[:2]]
                }
                for role, nodes in node_groups.items()
            }
        except Exception as e:
            diagnosis['node_groups'] = {'error': str(e)}
        
        # Test a sample query
        if self.tcp_metrics and self.prometheus_client:
            try:
                sample_metric = self.tcp_metrics[0]
                sample_expr = self._resolve_query_variables(sample_metric['expr'])
                result = await self.prometheus_client.query_instant(sample_expr)
                
                if 'result' in result and result['result']:
                    sample_item = result['result'][0]
                    diagnosis['sample_query'] = {
                        'metric': sample_metric['name'],
                        'result_count': len(result['result']),
                        'sample_labels': list(sample_item.get('metric', {}).keys()),
                        'sample_instance': sample_item.get('metric', {}).get('instance', 'N/A'),
                        'sample_node': sample_item.get('metric', {}).get('node', 'N/A')
                    }
                else:
                    diagnosis['sample_query'] = {
                        'metric': sample_metric['name'],
                        'result_count': 0,
                        'message': 'No data returned'
                    }
            except Exception as e:
                diagnosis['sample_query'] = {'error': str(e)}
        elif self.tcp_metrics and not self.prometheus_client:
            diagnosis['sample_query'] = {'error': 'Prometheus client not initialized'}
        
        return diagnosis


async def main():
    """Main function for testing"""
    # Initialize config
    config = Config(metrics_file='config/metrics-net.yml')
    
    # Initialize collector
    async with socketStatTCPCollector(config) as collector:
        # First, run diagnosis
        print("=" * 60)
        print("DIAGNOSIS")
        print("=" * 60)
        diagnosis = await collector.diagnose_connection()
        print(json.dumps(diagnosis, indent=2))
        
        print("\n" + "=" * 60)
        print("COLLECTING METRICS")
        print("=" * 60)
        
        # Collect all metrics
        result = await collector.collect_all_tcp_metrics()
        
        # Print result
        print(json.dumps(result, indent=2))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())