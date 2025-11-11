#!/usr/bin/env python3
"""
Network Socket IP Statistics Collector
Collects network socket IP-level metrics from Prometheus
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from ocauth.openshift_auth import OpenShiftAuth
from config.metrics_config_reader import Config

logger = logging.getLogger(__name__)


class socketStatIPCollector:
    """Collector for network socket IP statistics"""
    
    def __init__(self, auth_client: OpenShiftAuth, config: Config):
        self.auth_client = auth_client
        self.config = config
        self.prometheus_client = PrometheusBaseQuery(
            auth_client.prometheus_url,
            auth_client.prometheus_token
        )
        self.utility = mcpToolsUtility(auth_client)
        # Use network_netstat_ip category as per metrics-net.yml
        self.metrics_config = config.get_metrics_by_category('network_netstat_ip')
        if not self.metrics_config:
            # Fallback to network_socket_ip if needed
            self.metrics_config = config.get_metrics_by_category('network_socket_ip')
        # All metrics by category for fuzzy fallback lookups
        self._all_metrics_by_category = self.config.get_all_metrics() if hasattr(self.config, 'get_all_metrics') else {}

    def _normalize(self, s: str) -> str:
        """Normalize a metric name for fuzzy comparison"""
        try:
            import re
            return re.sub(r'[^a-z0-9]+', '', s.lower())
        except Exception:
            return s.lower()

    def _find_metric_info(self, preferred_names: List[str]) -> Optional[Dict[str, Any]]:
        """
        Resolve metric configuration by trying:
        1) Exact match in current category list
        2) Exact match across all categories
        3) Fuzzy match (normalized substring) across all categories
        """
        if not isinstance(preferred_names, list):
            preferred_names = [preferred_names]
        # Try exact in current category
        for name in preferred_names:
            for m in self.metrics_config:
                if m.get('name') == name:
                    return m
        # Try exact across all categories
        for name in preferred_names:
            for cat_metrics in self._all_metrics_by_category.values():
                for m in cat_metrics:
                    if m.get('name') == name:
                        return m
        # Fuzzy match
        normalized_targets = [self._normalize(n) for n in preferred_names]
        for cat_metrics in self._all_metrics_by_category.values():
            for m in cat_metrics:
                nm = self._normalize(str(m.get('name', '')))
                if any(t in nm for t in normalized_targets):
                    return m
        return None
        
    async def __aenter__(self):
        await self.prometheus_client._ensure_session()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.prometheus_client.close()
    
    def _calculate_statistics(self, values: List[float]) -> Dict[str, float]:
        """Calculate avg and max from list of values"""
        if not values:
            return {"avg": 0.0, "max": 0.0}
        return {
            "avg": sum(values) / len(values),
            "max": max(values)
        }
    
    def _format_node_results(self, metric_data: Dict[str, Any], node_groups: Dict[str, List[Dict]], 
                            metric_name: str, unit: str, top_n: int = 3) -> Dict[str, Any]:
        """Format results by node groups with top N for workers"""
        # Build role buckets, prefilling with zero rows when data absent
        roles_template = {role: [] for role in ['controlplane', 'worker', 'infra', 'workload']}
        if 'result' not in metric_data or not metric_data['result']:
            result_zero = {}
            for role in roles_template.keys():
                nodes = node_groups.get(role, [])
                if nodes:
                    # Filter to only use full names (FQDN with dots)
                    node_map = {}  # prefix -> full_name
                    for node_info in nodes:
                        node_name = node_info.get('name') or ''
                        if '.' in node_name:  # Full name
                            prefix = node_name.split('.')[0]
                            if prefix not in node_map or len(node_name) > len(node_map[prefix]):
                                node_map[prefix] = node_name
                    
                    role_rows = []
                    seen_prefixes = set()
                    for node_info in nodes:
                        node_name = node_info.get('name') or ''
                        prefix = node_name.split('.')[0] if node_name else ''
                        
                        # Skip if we've already processed this node (by prefix)
                        if prefix in seen_prefixes:
                            continue
                        
                        # Use full name if available, otherwise skip short names
                        if prefix in node_map:
                            full_node_name = node_map[prefix]
                        elif '.' in node_name:
                            full_node_name = node_name
                        else:
                            # Short name without full name - skip it
                            continue
                        
                        seen_prefixes.add(prefix)
                        role_rows.append({
                            "node": full_node_name,
                            "avg": 0.0,
                            "max": 0.0,
                            "unit": unit
                        })
                    # Apply top N for workers only
                    if role == 'worker' and len(role_rows) > top_n:
                        role_rows = role_rows[:top_n]
                    result_zero[role] = role_rows
                else:
                    result_zero[role] = []
            return result_zero
        
        # Parse all results - prefer full node names (FQDN) over short names
        node_values: Dict[str, List[float]] = {}
        for item in metric_data['result']:
            labels = item.get('metric', {}) or {}
            instance = labels.get('instance', '')
            node = labels.get('node', '')
            nodename = labels.get('nodename', '')
            node_name = ''
            # Prefer nodename (usually FQDN), then node, then instance
            if nodename:
                node_name = nodename
            elif node:
                node_name = node
            elif instance:
                node_name = instance.split(':')[0]
            
            values = []
            for ts, val in item.get('values', []):
                try:
                    v = float(val)
                    if v >= 0:
                        values.append(v)
                except (ValueError, TypeError):
                    continue
            
            if values and node_name:
                # Check if we already have a variant of this node (short vs full name)
                # If one is a prefix of the other, merge into the longer (full) name
                node_prefix = node_name.split('.')[0]
                merged = False
                for existing_node in list(node_values.keys()):
                    existing_prefix = existing_node.split('.')[0]
                    # If they share the same prefix (same node, different formats)
                    if node_prefix == existing_prefix:
                        # Use the longer name (full FQDN)
                        target_key = existing_node if len(existing_node) >= len(node_name) else node_name
                        if target_key not in node_values:
                            node_values[target_key] = []
                        # Merge values from both
                        node_values[target_key].extend(values)
                        if existing_node != target_key and existing_node in node_values:
                            node_values[target_key].extend(node_values.pop(existing_node))
                        merged = True
                        break
                
                if not merged:
                    if node_name not in node_values:
                        node_values[node_name] = []
                    node_values[node_name].extend(values)
        
        # Clean up node_values: remove short names, keep only full names (FQDN)
        # Build a mapping of short names to full names
        short_to_full = {}
        full_names = {}
        for nv_key in list(node_values.keys()):
            if '.' in nv_key:  # Full name (FQDN)
                prefix = nv_key.split('.')[0]
                if prefix not in short_to_full or len(nv_key) > len(short_to_full[prefix]):
                    short_to_full[prefix] = nv_key
                full_names[nv_key] = node_values[nv_key]
            else:  # Short name
                if nv_key not in short_to_full:
                    short_to_full[nv_key] = None
        
        # Merge short name values into full names
        for nv_key in list(node_values.keys()):
            if '.' not in nv_key:  # Short name
                prefix = nv_key
                if prefix in short_to_full and short_to_full[prefix]:
                    full_name = short_to_full[prefix]
                    if full_name in full_names:
                        full_names[full_name].extend(node_values[nv_key])
                    else:
                        full_names[full_name] = node_values[nv_key]
        
        # Replace node_values with only full names
        node_values = full_names
        
        # Organize by node groups - filter to only use full names
        result = {role: [] for role in ['controlplane', 'worker', 'infra', 'workload']}
        for role in ['controlplane', 'worker', 'infra', 'workload']:
            nodes = node_groups.get(role, [])
            if not nodes:
                # Keep empty list for roles with no nodes
                continue
            
            # Filter nodes to only use full names (FQDN with dots)
            # Group by prefix to find full name for each node
            node_map = {}  # prefix -> full_name
            for node_info in nodes:
                node_name = node_info['name']
                if '.' in node_name:  # Full name
                    prefix = node_name.split('.')[0]
                    if prefix not in node_map or len(node_name) > len(node_map[prefix]):
                        node_map[prefix] = node_name
            
            role_data = []
            seen_prefixes = set()
            for node_info in nodes:
                node_name = node_info['name']
                prefix = node_name.split('.')[0]
                
                # Skip if we've already processed this node (by prefix)
                if prefix in seen_prefixes:
                    continue
                
                # Use full name if available, otherwise skip short names
                if prefix in node_map:
                    full_node_name = node_map[prefix]
                elif '.' in node_name:
                    full_node_name = node_name
                else:
                    # Short name without full name - skip it
                    continue
                
                seen_prefixes.add(prefix)
                
                # Match against full node names only
                values = node_values.get(full_node_name)
                
                # Try case-insensitive exact match
                if not values:
                    for nv_key, nv_values in node_values.items():
                        if nv_key.lower() == full_node_name.lower():
                            values = nv_values
                            break
                
                # Fallback: match by prefix
                if not values:
                    for nv_key, nv_values in node_values.items():
                        if nv_key.startswith(prefix + '.'):
                            values = nv_values
                            break
                
                # Create entry only with full node name
                if values:
                    stats = self._calculate_statistics(values)
                    role_data.append({
                        "node": full_node_name,
                        "avg": round(stats["avg"], 2),
                        "max": round(stats["max"], 2),
                        "unit": unit
                    })
            
            # Sort by max value descending
            role_data.sort(key=lambda x: x["max"], reverse=True)
            
            # Apply top N for workers only
            if role == 'worker' and len(role_data) > top_n:
                result[role] = role_data[:top_n]
            else:
                result[role] = role_data
            
            # If still no rows for this role, prefill zeros for visibility
            # Use the same node_map we built earlier to ensure only full names
            if not result[role] and nodes:
                prefill = []
                seen_prefixes = set()
                for node_info in nodes:
                    node_name = node_info.get('name') or ''
                    prefix = node_name.split('.')[0] if node_name else ''
                    
                    # Skip if we've already processed this node (by prefix)
                    if prefix in seen_prefixes:
                        continue
                    
                    # Use full name if available, otherwise skip short names
                    if prefix in node_map:
                        full_node_name = node_map[prefix]
                    elif '.' in node_name:
                        full_node_name = node_name
                    else:
                        # Short name without full name - skip it
                        continue
                    
                    seen_prefixes.add(prefix)
                    prefill.append({
                        "node": full_node_name,
                        "avg": 0.0,
                        "max": 0.0,
                        "unit": unit
                    })
                if role == 'worker' and len(prefill) > top_n:
                    prefill = prefill[:top_n]
                result[role] = prefill
        
        return result
    
    async def collect_netstat_ip_in_octets(self, start: str, end: str, 
                                          step: str = "15s") -> Dict[str, Any]:
        """Collect IP incoming octets per second"""
        # Try common aliases used in different configs
        metric_info = self._find_metric_info([
            'netstat_ip_in_octets',
            'node_netstat_Ip_InOctets',
            'node_netstat_IpExt_InOctets',
            'node_netstat_IpExt_InOctets_rate'
        ])
        if not metric_info:
            return {"error": "Metric configuration not found"}
        
        try:
            node_groups = await self.utility.get_node_groups(self.prometheus_client)
            query = metric_info['expr']
            result = await self.prometheus_client.query_range(query, start, end, step)
            
            return {
                "metric": metric_info['name'],
                "description": metric_info['description'],
                "nodes": self._format_node_results(result, node_groups, 
                                                   metric_info['name'], metric_info['unit'])
            }
        except Exception as e:
            logger.error(f"Error collecting {metric_info['name']}: {e}")
            return {"error": str(e)}
    
    async def collect_netstat_ip_out_octets(self, start: str, end: str, 
                                           step: str = "15s") -> Dict[str, Any]:
        """Collect IP outgoing octets per second"""
        metric_info = self._find_metric_info([
            'netstat_ip_out_octets',
            'node_netstat_Ip_OutOctets',
            'node_netstat_IpExt_OutOctets',
            'node_netstat_IpExt_OutOctets_rate'
        ])
        if not metric_info:
            return {"error": "Metric configuration not found"}
        
        try:
            node_groups = await self.utility.get_node_groups(self.prometheus_client)
            query = metric_info['expr']
            result = await self.prometheus_client.query_range(query, start, end, step)
            
            return {
                "metric": metric_info['name'],
                "description": metric_info['description'],
                "nodes": self._format_node_results(result, node_groups, 
                                                   metric_info['name'], metric_info['unit'])
            }
        except Exception as e:
            logger.error(f"Error collecting {metric_info['name']}: {e}")
            return {"error": str(e)}
    
    async def collect_icmp_in_msgs(self, start: str, end: str, 
                                   step: str = "15s") -> Dict[str, Any]:
        """Collect ICMP incoming messages per second"""
        metric_info = next((m for m in self.metrics_config if m['name'] == 'node_netstat_Icmp_InMsgs'), None)
        if not metric_info:
            return {"error": "Metric configuration not found"}
        
        try:
            node_groups = await self.utility.get_node_groups(self.prometheus_client)
            query = metric_info['expr']
            result = await self.prometheus_client.query_range(query, start, end, step)
            
            return {
                "metric": metric_info['name'],
                "description": metric_info['description'],
                "nodes": self._format_node_results(result, node_groups, 
                                                   metric_info['name'], metric_info['unit'])
            }
        except Exception as e:
            logger.error(f"Error collecting {metric_info['name']}: {e}")
            return {"error": str(e)}
    
    async def collect_icmp_out_msgs(self, start: str, end: str, 
                                    step: str = "15s") -> Dict[str, Any]:
        """Collect ICMP outgoing messages per second"""
        metric_info = next((m for m in self.metrics_config if m['name'] == 'node_netstat_Icmp_OutMsgs'), None)
        if not metric_info:
            return {"error": "Metric configuration not found"}
        
        try:
            node_groups = await self.utility.get_node_groups(self.prometheus_client)
            query = metric_info['expr']
            result = await self.prometheus_client.query_range(query, start, end, step)
            
            return {
                "metric": metric_info['name'],
                "description": metric_info['description'],
                "nodes": self._format_node_results(result, node_groups, 
                                                   metric_info['name'], metric_info['unit'])
            }
        except Exception as e:
            logger.error(f"Error collecting {metric_info['name']}: {e}")
            return {"error": str(e)}
    
    async def collect_icmp_in_errors(self, start: str, end: str, 
                                     step: str = "15s") -> Dict[str, Any]:
        """Collect ICMP incoming errors per second"""
        metric_info = next((m for m in self.metrics_config if m['name'] == 'node_netstat_Icmp_InErrors'), None)
        if not metric_info:
            return {"error": "Metric configuration not found"}
        
        try:
            node_groups = await self.utility.get_node_groups(self.prometheus_client)
            query = metric_info['expr']
            result = await self.prometheus_client.query_range(query, start, end, step)
            
            return {
                "metric": metric_info['name'],
                "description": metric_info['description'],
                "nodes": self._format_node_results(result, node_groups, 
                                                   metric_info['name'], metric_info['unit'])
            }
        except Exception as e:
            logger.error(f"Error collecting {metric_info['name']}: {e}")
            return {"error": str(e)}
    
    async def collect_all_socket_ip_metrics(self, start: str, end: str, 
                                           step: str = "15s") -> Dict[str, Any]:
        """Collect all network socket IP metrics"""
        try:
            # Collect all metrics concurrently
            tasks = [
                self.collect_netstat_ip_in_octets(start, end, step),
                self.collect_netstat_ip_out_octets(start, end, step),
                self.collect_icmp_in_msgs(start, end, step),
                self.collect_icmp_out_msgs(start, end, step),
                self.collect_icmp_in_errors(start, end, step)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Organize results
            metrics = {}
            metric_names = [
                'netstat_ip_in_octets',
                'netstat_ip_out_octets', 
                'node_netstat_Icmp_InMsgs',
                'node_netstat_Icmp_OutMsgs',
                'node_netstat_Icmp_InErrors'
            ]
            
            for idx, result in enumerate(results):
                metric_name = metric_names[idx] if idx < len(metric_names) else f"metric_{idx}"
                if isinstance(result, Exception):
                    metrics[metric_name] = {"error": str(result)}
                else:
                    metrics[metric_name] = result
            
            # Get node groups summary
            node_groups = await self.utility.get_node_groups(self.prometheus_client)
            node_summary = {
                role: len(nodes) for role, nodes in node_groups.items()
            }
            
            return {
                "category": "network_netstat_ip",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "time_range": {
                    "start": start,
                    "end": end,
                    "step": step
                },
                "node_groups_summary": node_summary,
                "metrics": metrics
            }
            
        except Exception as e:
            logger.error(f"Error collecting socket IP metrics: {e}")
            return {
                "category": "network_netstat_ip",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }


async def collect_socket_ip_metrics(auth_client: OpenShiftAuth, config: Config,
                                    start: str, end: str, step: str = "15s") -> Dict[str, Any]:
    """Helper function to collect socket IP metrics"""
    async with socketStatIPCollector(auth_client, config) as collector:
        return await collector.collect_all_socket_ip_metrics(start, end, step)