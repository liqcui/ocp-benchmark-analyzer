#!/usr/bin/env python3
"""
OVNK Benchmark Prometheus Network Netstat UDP Collector
Collects and analyzes UDP network statistics per node
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
import re

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from config.metrics_config_reader import Config
from ocauth.openshift_auth import OpenShiftAuth


class netStatUDPCollector:
    """Collector for network netstat UDP metrics"""
    
    def __init__(self, auth_client: Optional[OpenShiftAuth] = None, config: Optional[Config] = None):
        self.auth_client = auth_client
        # Prefer provided config; otherwise load network metrics from metrics-net.yml
        self.config = config or Config(metrics_file="config/metrics-net.yml")
        self.utility = mcpToolsUtility(auth_client=auth_client)
        self.prometheus_client: Optional[PrometheusBaseQuery] = None
        
    async def _ensure_prometheus_client(self) -> PrometheusBaseQuery:
        """Ensure Prometheus client is initialized"""
        if not self.prometheus_client:
            prometheus_url = getattr(self.config.prometheus, 'url', None)
            prometheus_token = getattr(self.config.prometheus, 'token', None)
            
            if self.auth_client:
                # Prefer discovered attributes when config missing
                if not prometheus_url:
                    # Try direct attribute then config export
                    prometheus_url = getattr(self.auth_client, 'prometheus_url', None)
                    if not prometheus_url:
                        prom_cfg = self.auth_client.get_prometheus_config() or {}
                        prometheus_url = prom_cfg.get('url')
                if not prometheus_token:
                    # Prefer explicit prometheus_token; fallback to auth token or config export
                    prometheus_token = getattr(self.auth_client, 'prometheus_token', None) or getattr(self.auth_client, 'token', None)
                    if not prometheus_token:
                        prom_cfg = self.auth_client.get_prometheus_config() or {}
                        prometheus_token = prom_cfg.get('token')
            
            if not prometheus_url:
                raise ValueError("Prometheus URL not configured")
            
            self.prometheus_client = PrometheusBaseQuery(
                prometheus_url=prometheus_url,
                token=prometheus_token
            )
        
        return self.prometheus_client
    
    def _get_metrics_config(self) -> List[Dict[str, Any]]:
        """Get network_netstat_udp metrics from config"""
        return self.config.get_metrics_by_category('network_netstat_udp')
    
    def _substitute_node_pattern(self, expr: str, node_pattern: str) -> str:
        """Replace $node_name placeholder with actual node pattern"""
        # Use regex match for instance label to support multiple nodes and optional port
        expr = expr.replace('instance="$node_name"', 'instance=~"$node_name"')
        return expr.replace('$node_name', node_pattern)
    
    def _build_node_regex(self, node_names: List[str]) -> str:
        """Build a regex that matches any of the provided node names with optional :port"""
        # Prefer full names; de-duplicate short/full pairs
        preferred: List[str] = []
        short_to_full: Dict[str, str] = {}
        seen: set = set()
        for name in node_names:
            if not name:
                continue
            if '.' in name:
                short_to_full[name.split('.')[0]] = name
        for name in node_names:
            if not name:
                continue
            short = name.split('.')[0]
            preferred_name = short_to_full.get(short, name)
            if preferred_name not in seen:
                seen.add(preferred_name)
                preferred.append(preferred_name)
        # Escape dots without introducing backslashes that break PromQL string parsing.
        # Use "[.]" for literal dot. Hyphens are safe and should not be escaped.
        def _promql_safe(n: str) -> str:
            return n.replace('.', '[.]')
        parts = [f"{_promql_safe(n)}(:[0-9]+)?" for n in preferred]
        if not parts:
            return ".*"
        return "(" + "|".join(parts) + ")"
    
    async def _query_metric_for_nodes(
        self, 
        metric_name: str, 
        expr: str, 
        node_pattern: str,
        duration: str = '5m'
    ) -> Dict[str, float]:
        """Query a metric for specific nodes and return node -> value mapping"""
        prom_client = await self._ensure_prometheus_client()
        
        # Substitute node pattern
        query = self._substitute_node_pattern(expr, node_pattern)
        
        # Get time range for query
        end_time = datetime.now(timezone.utc)
        start_time = end_time - prom_client.parse_duration(duration)
        
        # Execute range query
        result = await prom_client.query_range(
            query=query,
            start=start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            end=end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            step='15s'
        )
        
        # Parse results and extract node values
        node_values = {}
        
        if 'result' in result:
            for item in result['result']:
                metric_labels = item.get('metric', {})
                
                # Extract node name from labels
                node_name = (
                    metric_labels.get('instance', '') or
                    metric_labels.get('node', '') or
                    metric_labels.get('kubernetes_node', '')
                )
                
                # Clean node name (remove port if present)
                if ':' in node_name:
                    node_name = node_name.split(':')[0]
                
                if not node_name:
                    continue
                
                # Extract values from time series
                values = item.get('values', [])
                numeric_values = []
                
                for ts, val in values:
                    try:
                        v = float(val)
                        if v >= 0:  # Filter out negative values
                            numeric_values.append(v)
                    except (ValueError, TypeError):
                        continue
                
                if numeric_values:
                    # Store max value for this node
                    node_values[node_name] = max(numeric_values)
        
        return node_values
    
    def _calculate_stats(self, values: List[float]) -> Dict[str, float]:
        """Calculate avg and max from list of values"""
        if not values:
            return {'avg': 0.0, 'max': 0.0}
        
        return {
            'avg': round(sum(values) / len(values), 2),
            'max': round(max(values), 2)
        }
    
    def _get_top_n_nodes(self, node_values: Dict[str, float], n: int = 3) -> Dict[str, float]:
        """Get top N nodes by value"""
        sorted_nodes = sorted(node_values.items(), key=lambda x: x[1], reverse=True)
        return dict(sorted_nodes[:n])
    
    async def collect_udp_error_rx_in_errors(self, node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Collect UDP error rx in errors metric"""
        metric_config = self.config.get_metric_by_name('udp_error_rx_in_errors')
        if not metric_config:
            return {}
        
        expr = metric_config['expr']
        unit = metric_config.get('unit', 'packets_per_second')
        
        result = {
            'metric': 'udp_error_rx_in_errors',
            'unit': unit,
            'groups': {}
        }
        
        for role, nodes in node_groups.items():
            if not nodes:
                continue
            
            node_names = [n['name'] for n in nodes]
            node_pattern = self._build_node_regex(node_names)
            
            node_values = await self._query_metric_for_nodes(
                'udp_error_rx_in_errors',
                expr,
                node_pattern
            )
            
            if role == 'worker':
                node_values = self._get_top_n_nodes(node_values, 3)
            
            if node_values:
                values = list(node_values.values())
                stats = self._calculate_stats(values)
                
                result['groups'][role] = {
                    'avg': stats['avg'],
                    'max': stats['max'],
                    'nodes': {name: round(val, 2) for name, val in node_values.items()}
                }
        
        return result
    
    async def collect_udp_error_no_listen(self, node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Collect UDP error no listen metric"""
        metric_config = self.config.get_metric_by_name('udp_error_no_listen')
        if not metric_config:
            return {}
        
        expr = metric_config['expr']
        unit = metric_config.get('unit', 'packets_per_second')
        
        result = {
            'metric': 'udp_error_no_listen',
            'unit': unit,
            'groups': {}
        }
        
        for role, nodes in node_groups.items():
            if not nodes:
                continue
            
            node_names = [n['name'] for n in nodes]
            node_pattern = self._build_node_regex(node_names)
            
            node_values = await self._query_metric_for_nodes(
                'udp_error_no_listen',
                expr,
                node_pattern
            )
            
            if role == 'worker':
                node_values = self._get_top_n_nodes(node_values, 3)
            
            if node_values:
                values = list(node_values.values())
                stats = self._calculate_stats(values)
                
                result['groups'][role] = {
                    'avg': stats['avg'],
                    'max': stats['max'],
                    'nodes': {name: round(val, 2) for name, val in node_values.items()}
                }
        
        return result
    
    async def collect_udp_error_lite_rx_in_errors(self, node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Collect UDP error lite rx in errors metric"""
        metric_config = self.config.get_metric_by_name('udp_error_lite_rx_in_errors')
        if not metric_config:
            return {}
        
        expr = metric_config['expr']
        unit = metric_config.get('unit', 'packets_per_second')
        
        result = {
            'metric': 'udp_error_lite_rx_in_errors',
            'unit': unit,
            'groups': {}
        }
        
        for role, nodes in node_groups.items():
            if not nodes:
                continue
            
            node_names = [n['name'] for n in nodes]
            node_pattern = self._build_node_regex(node_names)
            
            node_values = await self._query_metric_for_nodes(
                'udp_error_lite_rx_in_errors',
                expr,
                node_pattern
            )
            
            if role == 'worker':
                node_values = self._get_top_n_nodes(node_values, 3)
            
            if node_values:
                values = list(node_values.values())
                stats = self._calculate_stats(values)
                
                result['groups'][role] = {
                    'avg': stats['avg'],
                    'max': stats['max'],
                    'nodes': {name: round(val, 2) for name, val in node_values.items()}
                }
        
        return result
    
    async def collect_udp_error_rx_in_buffer_errors(self, node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Collect UDP error rx in buffer errors metric"""
        metric_config = self.config.get_metric_by_name('udp_error_rx_in_buffer_errors')
        if not metric_config:
            return {}
        
        expr = metric_config['expr']
        unit = metric_config.get('unit', 'packets_per_second')
        
        result = {
            'metric': 'udp_error_rx_in_buffer_errors',
            'unit': unit,
            'groups': {}
        }
        
        for role, nodes in node_groups.items():
            if not nodes:
                continue
            
            node_names = [n['name'] for n in nodes]
            node_pattern = self._build_node_regex(node_names)
            
            node_values = await self._query_metric_for_nodes(
                'udp_error_rx_in_buffer_errors',
                expr,
                node_pattern
            )
            
            if role == 'worker':
                node_values = self._get_top_n_nodes(node_values, 3)
            
            if node_values:
                values = list(node_values.values())
                stats = self._calculate_stats(values)
                
                result['groups'][role] = {
                    'avg': stats['avg'],
                    'max': stats['max'],
                    'nodes': {name: round(val, 2) for name, val in node_values.items()}
                }
        
        return result
    
    async def collect_udp_error_tx_buffer_errors(self, node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Collect UDP error tx buffer errors metric"""
        metric_config = self.config.get_metric_by_name('udp_error_tx_buffer_errors')
        if not metric_config:
            return {}
        
        expr = metric_config['expr']
        unit = metric_config.get('unit', 'packets_per_second')
        
        result = {
            'metric': 'udp_error_tx_buffer_errors',
            'unit': unit,
            'groups': {}
        }
        
        for role, nodes in node_groups.items():
            if not nodes:
                continue
            
            node_names = [n['name'] for n in nodes]
            node_pattern = self._build_node_regex(node_names)
            
            node_values = await self._query_metric_for_nodes(
                'udp_error_tx_buffer_errors',
                expr,
                node_pattern
            )
            
            if role == 'worker':
                node_values = self._get_top_n_nodes(node_values, 3)
            
            if node_values:
                values = list(node_values.values())
                stats = self._calculate_stats(values)
                
                result['groups'][role] = {
                    'avg': stats['avg'],
                    'max': stats['max'],
                    'nodes': {name: round(val, 2) for name, val in node_values.items()}
                }
        
        return result
    
    async def collect_nestat_udp_in(self, node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Collect UDP in metric"""
        metric_config = self.config.get_metric_by_name('nestat_udp_in')
        if not metric_config:
            return {}
        
        expr = metric_config['expr']
        unit = metric_config.get('unit', 'packets_per_second')
        
        result = {
            'metric': 'nestat_udp_in',
            'unit': unit,
            'groups': {}
        }
        
        for role, nodes in node_groups.items():
            if not nodes:
                continue
            
            node_names = [n['name'] for n in nodes]
            node_pattern = self._build_node_regex(node_names)
            
            node_values = await self._query_metric_for_nodes(
                'nestat_udp_in',
                expr,
                node_pattern
            )
            
            if role == 'worker':
                node_values = self._get_top_n_nodes(node_values, 3)
            
            if node_values:
                values = list(node_values.values())
                stats = self._calculate_stats(values)
                
                result['groups'][role] = {
                    'avg': stats['avg'],
                    'max': stats['max'],
                    'nodes': {name: round(val, 2) for name, val in node_values.items()}
                }
        
        return result
    
    async def collect_netstat_udp_out(self, node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Collect UDP out metric"""
        metric_config = self.config.get_metric_by_name('netstat_udp_out')
        if not metric_config:
            return {}
        
        expr = metric_config['expr']
        unit = metric_config.get('unit', 'packets_per_second')
        
        result = {
            'metric': 'netstat_udp_out',
            'unit': unit,
            'groups': {}
        }
        
        for role, nodes in node_groups.items():
            if not nodes:
                continue
            
            node_names = [n['name'] for n in nodes]
            node_pattern = self._build_node_regex(node_names)
            
            node_values = await self._query_metric_for_nodes(
                'netstat_udp_out',
                expr,
                node_pattern
            )
            
            if role == 'worker':
                node_values = self._get_top_n_nodes(node_values, 3)
            
            if node_values:
                values = list(node_values.values())
                stats = self._calculate_stats(values)
                
                result['groups'][role] = {
                    'avg': stats['avg'],
                    'max': stats['max'],
                    'nodes': {name: round(val, 2) for name, val in node_values.items()}
                }
        
        return result
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all UDP netstat metrics"""
        prom_client = await self._ensure_prometheus_client()
        try:
            # Get node groups
            node_groups = await self.utility.get_node_groups(prom_client)
            
            # Collect all metrics
            metrics_results = []
            
            # Get all network_netstat_udp metrics from config
            udp_metrics = self._get_metrics_config()
            
            for metric_config in udp_metrics:
                metric_name = metric_config.get('name', '')
                
                if metric_name == 'udp_error_rx_in_errors':
                    result = await self.collect_udp_error_rx_in_errors(node_groups)
                elif metric_name == 'udp_error_no_listen':
                    result = await self.collect_udp_error_no_listen(node_groups)
                elif metric_name == 'udp_error_lite_rx_in_errors':
                    result = await self.collect_udp_error_lite_rx_in_errors(node_groups)
                elif metric_name == 'udp_error_rx_in_buffer_errors':
                    result = await self.collect_udp_error_rx_in_buffer_errors(node_groups)
                elif metric_name == 'udp_error_tx_buffer_errors':
                    result = await self.collect_udp_error_tx_buffer_errors(node_groups)
                elif metric_name == 'nestat_udp_in':
                    result = await self.collect_nestat_udp_in(node_groups)
                elif metric_name == 'netstat_udp_out':
                    result = await self.collect_netstat_udp_out(node_groups)
                else:
                    continue
                
                if result:
                    metrics_results.append(result)
            
            # Build final result
            return {
                'category': 'network_netstat_udp',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'timezone': 'UTC',
                'metrics': metrics_results
            }
        finally:
            # Ensure Prometheus session is closed to prevent resource warnings
            try:
                await prom_client.close()
            except Exception:
                pass
    
    async def close(self):
        """Close resources"""
        if self.prometheus_client:
            await self.prometheus_client.close()


async def main():
    """Example usage"""
    # Initialize auth client
    auth_client = OpenShiftAuth()
    await auth_client.initialize()
    
    # Create collector
    collector = netStatUDPCollector(auth_client=auth_client)
    
    try:
        # Collect all metrics
        results = await collector.collect_all_metrics()
        
        # Print results
        import json
        print(json.dumps(results, indent=2))
        
    finally:
        await collector.close()
        await auth_client.close()


if __name__ == '__main__':
    asyncio.run(main())