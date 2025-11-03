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
        if 'result' not in metric_data or not metric_data['result']:
            return {"error": "No data available"}
        
        # Parse all results
        node_values: Dict[str, List[float]] = {}
        for item in metric_data['result']:
            instance = item.get('metric', {}).get('instance', '')
            node_name = instance.split(':')[0] if ':' in instance else instance
            
            values = []
            for ts, val in item.get('values', []):
                try:
                    v = float(val)
                    if v >= 0:
                        values.append(v)
                except (ValueError, TypeError):
                    continue
            
            if values and node_name:
                if node_name not in node_values:
                    node_values[node_name] = []
                node_values[node_name].extend(values)
        
        # Organize by node groups
        result = {}
        for role in ['controlplane', 'worker', 'infra', 'workload']:
            nodes = node_groups.get(role, [])
            if not nodes:
                continue
                
            role_data = []
            for node_info in nodes:
                node_name = node_info['name']
                short_name = node_name.split('.')[0]
                
                # Try both full and short names
                values = node_values.get(node_name) or node_values.get(short_name)
                if values:
                    stats = self._calculate_statistics(values)
                    role_data.append({
                        "node": node_name,
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
        
        return result
    
    async def collect_netstat_ip_in_octets(self, start: str, end: str, 
                                          step: str = "15s") -> Dict[str, Any]:
        """Collect IP incoming octets per second"""
        metric_info = next((m for m in self.metrics_config if m['name'] == 'netstat_ip_in_octets'), None)
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
        metric_info = next((m for m in self.metrics_config if m['name'] == 'netstat_ip_out_octets'), None)
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