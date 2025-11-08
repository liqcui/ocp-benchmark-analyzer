#!/usr/bin/env python3
"""
Network L1 Metrics Collector (PromQL)
Renamed from: ovnk_benchmark_prometheus_network_l1.py
Updated to use OpenShift route-based Prometheus discovery
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class NetworkL1Collector:
    """Collector for Network Layer 1 metrics"""
    
    def __init__(self, prometheus_client, config, utility, openshift_auth=None):
        """
        Initialize Network L1 Collector
        
        Args:
            prometheus_client: PrometheusBaseQuery instance
            config: Config instance
            utility: mcpToolsUtility instance
            openshift_auth: OpenShiftAuth instance (optional, for route discovery)
        """
        self.prometheus_client = prometheus_client
        self.config = config
        self.utility = utility
        self.openshift_auth = openshift_auth
        self.category = "network_l1"
        
        # Get metrics for this category
        self.metrics = self.config.get_metrics_by_category(self.category)
        logger.info(f"Initialized NetworkL1Collector with {len(self.metrics)} metrics")
        
        # Flag to track if we've already tried to update Prometheus URL
        self._prometheus_url_updated = False
    
    async def _ensure_prometheus_connectivity(self):
        """
        Ensure Prometheus is reachable, using route discovery if needed.
        This method updates the prometheus_client URL if the current one is failing.
        """
        if self._prometheus_url_updated:
            return  # Already attempted update
        
        if not self.openshift_auth:
            logger.warning("OpenShiftAuth not provided, cannot discover Prometheus route")
            return
        
        try:
            # Check if OpenShiftAuth has already discovered Prometheus
            if not self.openshift_auth.prometheus_url:
                logger.info("Prometheus URL not yet discovered, initializing OpenShift authentication...")
                await self.openshift_auth.initialize()
            
            # Get Prometheus config from OpenShiftAuth (includes route-based URL and fallbacks)
            prom_config = self.openshift_auth.get_prometheus_config()
            new_url = prom_config.get('url')
            fallback_urls = prom_config.get('fallback_urls', [])
            
            if new_url and new_url != self.prometheus_client.base_url:
                logger.info(f"Updating Prometheus URL from '{self.prometheus_client.base_url}' to '{new_url}' (route-based)")
                self.prometheus_client.base_url = new_url
                
                # Update token if available
                if prom_config.get('token'):
                    self.prometheus_client.token = prom_config['token']
                    logger.info("Updated Prometheus authentication token")
                
                # Update verification settings
                if 'verify' in prom_config:
                    self.prometheus_client.verify = prom_config['verify']
                    logger.info(f"Updated Prometheus TLS verification: {prom_config['verify']}")
                
                # Store fallback URLs if the client supports them
                if hasattr(self.prometheus_client, 'fallback_urls'):
                    self.prometheus_client.fallback_urls = fallback_urls
                    logger.info(f"Configured {len(fallback_urls)} fallback URLs")
            
            self._prometheus_url_updated = True
            
        except Exception as e:
            logger.error(f"Error updating Prometheus connectivity: {e}")
            # Don't raise - allow collection to proceed with existing config
    
    async def collect_all_metrics(self, duration: str = "5m") -> Dict[str, Any]:
        """
        Collect all network L1 metrics
        
        Args:
            duration: Time range for queries (default: 5m)
            
        Returns:
            JSON formatted results grouped by node role
        """
        try:
            # Ensure Prometheus connectivity before collecting metrics
            await self._ensure_prometheus_connectivity()
            
            # Get node groups
            node_groups = await self.utility.get_node_groups(self.prometheus_client)
            
            # Collect metrics for each group
            results = {
                "category": self.category,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "duration": duration,
                "node_groups": {}
            }
            
            # Process each node group
            for role in ['controlplane', 'infra', 'workload', 'worker']:
                nodes = node_groups.get(role, [])
                if not nodes:
                    continue
                
                role_results = await self._collect_for_role(role, nodes, duration)
                if role_results:
                    results["node_groups"][role] = role_results
            
            return results
            
        except Exception as e:
            logger.error(f"Error collecting network L1 metrics: {e}")
            return {
                "category": self.category,
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    async def _collect_for_role(self, role: str, nodes: List[Dict], duration: str) -> Dict[str, Any]:
        """
        Collect metrics for a specific node role
        
        Args:
            role: Node role (controlplane/worker/infra/workload)
            nodes: List of nodes in this role
            duration: Time range for queries
            
        Returns:
            Metrics results for this role
        """
        role_results = {
            "count": len(nodes),
            "metrics": {}
        }
        
        # Collect each metric
        for metric_config in self.metrics:
            metric_name = metric_config['name']
            try:
                metric_data = await self._collect_metric(metric_name, metric_config, nodes, role)
                if metric_data:
                    role_results["metrics"][metric_name] = metric_data
            except Exception as e:
                logger.error(f"Error collecting {metric_name} for {role}: {e}")
                role_results["metrics"][metric_name] = {"error": str(e)}
        
        return role_results
    
    async def _collect_metric(self, metric_name: str, metric_config: Dict, nodes: List[Dict], role: str) -> Dict[str, Any]:
        """
        Collect a specific metric
        
        Args:
            metric_name: Name of the metric
            metric_config: Metric configuration
            nodes: List of nodes
            role: Node role
            
        Returns:
            Metric data
        """
        if metric_name == "network_l1_node_network_up":
            return await self.get_network_up_status(nodes, role)
        elif metric_name == "network_l1_node_traffic_carrier":
            return await self.get_traffic_carrier_status(nodes, role)
        elif metric_name == "network_l1_node_network_speed_bytes":
            return await self.get_network_speed(nodes, role)
        elif metric_name == "network_l1_node_network_mtu_bytes":
            return await self.get_network_mtu(nodes, role)
        elif metric_name == "network_l1_node_arp_entries":
            return await self.get_arp_entries(nodes, role)
        else:
            logger.warning(f"Unknown metric: {metric_name}")
            return {}
    
    async def get_network_up_status(self, nodes: List[Dict], role: str) -> Dict[str, Any]:
        """Get network operational status (Yes/No) for each node"""
        try:
            result_data = {
                "title": "Network Up Status",
                "unit": "status",
                "nodes": {}
            }
            
            nodes_to_process = self._select_nodes(nodes, role)
            
            for node in nodes_to_process:
                node_name = node['name']
                
                # Remove operstate filter to get all interfaces
                query = f'node_network_up{{instance=~"{node_name}.*",job="node-exporter"}}'
                result = await self.prometheus_client.query_instant(query)
                
                node_data = {
                    "interfaces": {}
                }
                
                if 'result' in result and result['result']:
                    for item in result['result']:
                        metric = item.get('metric', {})
                        device = metric.get('device', 'unknown')
                        value = item.get('value', [None, '0'])[1]
                        
                        try:
                            node_data["interfaces"][device] = "Yes" if float(value) == 1 else "No"
                        except (ValueError, TypeError):
                            node_data["interfaces"][device] = "No"
                
                result_data["nodes"][node_name] = node_data
            
            return result_data
            
        except Exception as e:
            logger.error(f"Error getting network up status: {e}")
            return {"error": str(e)}

    async def get_traffic_carrier_status(self, nodes: List[Dict], role: str) -> Dict[str, Any]:
        """Get traffic carrier status (Yes/No) for each node"""
        try:
            result_data = {
                "title": "Traffic Carrier Status",
                "unit": "status",
                "nodes": {}
            }
            
            nodes_to_process = self._select_nodes(nodes, role)
            
            for node in nodes_to_process:
                node_name = node['name']
                
                # Try both FQDN and short hostname patterns
                query = f'node_network_carrier{{instance=~"{node_name}.*",job="node-exporter"}}'
                result = await self.prometheus_client.query_instant(query)
                
                node_data = {
                    "status": "No",
                    "interfaces": {}
                }
                
                if 'result' in result and result['result']:
                    node_data["status"] = "Yes"
                    for item in result['result']:
                        metric = item.get('metric', {})
                        device = metric.get('device', 'unknown')
                        value = item.get('value', [None, '0'])[1]
                        
                        node_data["interfaces"][device] = "Yes" if float(value) == 1 else "No"
                
                result_data["nodes"][node_name] = node_data
            
            return result_data
            
        except Exception as e:
            logger.error(f"Error getting traffic carrier status: {e}")
            return {"error": str(e)}

    async def get_network_speed(self, nodes: List[Dict], role: str) -> Dict[str, Any]:
        """Get network speed (max value) for each node"""
        try:
            result_data = {
                "title": "Network Speed",
                "unit": "bits_per_second",
                "nodes": {}
            }
            
            nodes_to_process = self._select_nodes(nodes, role)
            
            for node in nodes_to_process:
                node_name = node['name']
                
                # Try both FQDN and short hostname patterns
                query = f'node_network_speed_bytes{{instance=~"{node_name}.*",job="node-exporter"}} * 8'
                result = await self.prometheus_client.query_instant(query)
                
                node_data = {
                    "max_speed": 0,
                    "interfaces": {}
                }
                
                if 'result' in result and result['result']:
                    max_speed = 0
                    for item in result['result']:
                        metric = item.get('metric', {})
                        device = metric.get('device', 'unknown')
                        value = item.get('value', [None, '0'])[1]
                        
                        try:
                            speed = float(value)
                            node_data["interfaces"][device] = speed
                            if speed > max_speed:
                                max_speed = speed
                        except (ValueError, TypeError):
                            node_data["interfaces"][device] = 0
                    
                    node_data["max_speed"] = max_speed
                
                result_data["nodes"][node_name] = node_data
            
            return result_data
            
        except Exception as e:
            logger.error(f"Error getting network speed: {e}")
            return {"error": str(e)}

    async def get_network_mtu(self, nodes: List[Dict], role: str) -> Dict[str, Any]:
        """Get network MTU (max value) for each node"""
        try:
            result_data = {
                "title": "Network MTU",
                "unit": "bytes",
                "nodes": {}
            }
            
            nodes_to_process = self._select_nodes(nodes, role)
            
            for node in nodes_to_process:
                node_name = node['name']
                
                # Try both FQDN and short hostname patterns
                query = f'node_network_mtu_bytes{{instance=~"{node_name}.*",job="node-exporter"}}'
                result = await self.prometheus_client.query_instant(query)
                
                node_data = {
                    "max_mtu": 0,
                    "interfaces": {}
                }
                
                if 'result' in result and result['result']:
                    max_mtu = 0
                    for item in result['result']:
                        metric = item.get('metric', {})
                        device = metric.get('device', 'unknown')
                        value = item.get('value', [None, '0'])[1]
                        
                        try:
                            mtu = float(value)
                            node_data["interfaces"][device] = mtu
                            if mtu > max_mtu:
                                max_mtu = mtu
                        except (ValueError, TypeError):
                            node_data["interfaces"][device] = 0
                    
                    node_data["max_mtu"] = max_mtu
                
                result_data["nodes"][node_name] = node_data
            
            return result_data
            
        except Exception as e:
            logger.error(f"Error getting network MTU: {e}")
            return {"error": str(e)}

    async def get_arp_entries(self, nodes: List[Dict], role: str) -> Dict[str, Any]:
        """Get ARP entries (avg and max) for each interface on each node over time range"""
        try:
            result_data = {
                "title": "ARP Entries",
                "unit": "count",
                "nodes": {}
            }
            
            nodes_to_process = self._select_nodes(nodes, role)
            
            # Get time range (5 minutes)
            start, end = self.prometheus_client.get_time_range_from_duration("5m")
            
            for node in nodes_to_process:
                node_name = node['name']
                
                # Query for ARP entries per device over time range
                query = f'node_arp_entries{{instance=~"{node_name}.*",job="node-exporter"}}'
                result = await self.prometheus_client.query_range(query, start, end, step='15s')
                
                node_data = {
                    "interfaces": {}
                }
                
                if 'result' in result and result['result']:
                    for item in result['result']:
                        metric = item.get('metric', {})
                        device = metric.get('device', 'unknown')
                        values = item.get('values', [])
                        
                        # Extract numeric values from time series
                        numeric_values = []
                        for timestamp, value in values:
                            try:
                                numeric_values.append(float(value))
                            except (ValueError, TypeError):
                                continue
                        
                        if numeric_values:
                            node_data["interfaces"][device] = {
                                "avg": round(sum(numeric_values) / len(numeric_values)),
                                "max": round(max(numeric_values))
                            }
                        else:
                            node_data["interfaces"][device] = {
                                "avg": 0,
                                "max": 0
                            }
                
                if node_data["interfaces"]:  # Only add node if it has interfaces
                    result_data["nodes"][node_name] = node_data
            
            return result_data
            
        except Exception as e:
            logger.error(f"Error getting ARP entries: {e}")
            return {"error": str(e)}

    def _select_nodes(self, nodes: List[Dict], role: str) -> List[Dict]:
        """
        Select nodes based on role
        - For worker: return top 3 by name
        - For others: return all
        """
        if role == 'worker' and len(nodes) > 3:
            # Sort by name and take top 3
            sorted_nodes = sorted(nodes, key=lambda x: x['name'])
            return sorted_nodes[:3]
        return nodes