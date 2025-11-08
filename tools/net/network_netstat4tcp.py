#!/usr/bin/env python3
"""
Network TCP Netstat Collector
Collects TCP network statistics from Prometheus and groups by node roles
"""

import asyncio
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.metrics_config_reader import Config
from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from ocauth.openshift_auth import OpenShiftAuth


class netStatTCPCollector:
    """Collector for TCP Network Statistics"""
    
    def __init__(self, config: Config, auth: OpenShiftAuth, prometheus_client: PrometheusBaseQuery):
        self.config = config
        self.auth = auth
        self.prometheus = prometheus_client
        self.utility = mcpToolsUtility(auth)
        self.category = "network_netstat_tcp"
        
    async def collect_all_metrics(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect all TCP netstat metrics"""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - self._parse_duration(duration)
        
        # Get metrics configuration
        metrics = self.config.get_metrics_by_category(self.category)
        if not metrics:
            return {"error": f"No metrics found for category: {self.category}"}
        
        # Get node groups
        node_groups = await self.utility.get_node_groups(self.prometheus)
        
        # Collect all metrics
        results = {
            "collection_time": end_time.isoformat(),
            "duration": duration,
            "timezone": "UTC",
            "metrics": {}
        }
        
        for metric in metrics:
            metric_name = metric.get('name')
            if metric_name:
                metric_data = await self._collect_metric(
                    metric, 
                    start_time, 
                    end_time, 
                    node_groups
                )
                results["metrics"][metric_name] = metric_data
        
        return results
    
    async def _collect_metric(self, metric: Dict[str, Any], start_time: datetime, 
                              end_time: datetime, node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Collect data for a single metric"""
        metric_name = metric.get('name')
        expr = metric.get('expr', '')
        unit = metric.get('unit', '')
        description = metric.get('description', '')
        
        try:
            # Query Prometheus
            # Replace Grafana-style variable with a wildcard so Prometheus accepts the query
            query_expr = expr.replace('$node_name', '.*')
            result = await self.prometheus.query_range(
                query=query_expr,
                start=start_time.isoformat(),
                end=end_time.isoformat(),
                step='15s'
            )
            
            # Process results by node groups
            processed_data = self._process_by_node_groups(result, node_groups)
            
            return {
                "description": description,
                "unit": unit,
                "data": processed_data
            }
            
        except Exception as e:
            return {
                "description": description,
                "unit": unit,
                "error": str(e),
                "data": {}
            }
    
    def _process_by_node_groups(self, query_result: Dict[str, Any], 
                                 node_groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Process query results and group by node roles"""
        
        # Create node role mapping
        node_role_map = {}
        for role, nodes in node_groups.items():
            for node in nodes:
                node_name = node['name']
                # Add both full name and short name
                node_role_map[node_name] = role
                if '.' in node_name:
                    short_name = node_name.split('.')[0]
                    node_role_map[short_name] = role
        
        # Organize data by node
        node_data = {}
        
        if 'result' in query_result:
            for item in query_result['result']:
                metric_labels = item.get('metric', {})
                values = item.get('values', [])
                
                # Extract node name from various label possibilities
                node_name = (metric_labels.get('instance', '') or 
                           metric_labels.get('node', '') or 
                           metric_labels.get('nodename', ''))
                
                # Clean up node name (remove port if present)
                if ':' in node_name:
                    node_name = node_name.split(':')[0]
                
                if not node_name or not values:
                    continue
                
                # Calculate statistics
                numeric_values = []
                for ts, val in values:
                    try:
                        v = float(val)
                        if v >= 0:  # Filter out negative values
                            numeric_values.append(v)
                    except (ValueError, TypeError):
                        continue
                
                if not numeric_values:
                    continue
                
                stats = {
                    "avg": round(sum(numeric_values) / len(numeric_values), 2),
                    "max": round(max(numeric_values), 2)
                }
                
                # Determine node role
                role = node_role_map.get(node_name)
                if role is None and '.' in node_name:
                    short_name = node_name.split('.')[0]
                    role = node_role_map.get(short_name)
                if role is None:
                    # Default to worker so data does not get dropped
                    role = 'worker'
                
                if node_name not in node_data:
                    node_data[node_name] = {
                        "role": role,
                        "stats": stats
                    }
        
        # Group by roles and apply filtering
        grouped_data = {
            "controlplane": [],
            "worker": [],
            "infra": [],
            "workload": []
        }
        
        for node_name, data in node_data.items():
            role = data['role']
            if role in grouped_data:
                grouped_data[role].append({
                    "node": node_name,
                    "avg": data['stats']['avg'],
                    "max": data['stats']['max']
                })
        
        # Sort and filter workers (top 3 only)
        if grouped_data['worker']:
            grouped_data['worker'] = sorted(
                grouped_data['worker'],
                key=lambda x: x['max'],
                reverse=True
            )[:3]
        
        # Sort other roles by max value
        for role in ['controlplane', 'infra', 'workload']:
            if grouped_data[role]:
                grouped_data[role] = sorted(
                    grouped_data[role],
                    key=lambda x: x['max'],
                    reverse=True
                )
        
        return grouped_data
    
    def _parse_duration(self, duration: str) -> timedelta:
        """Parse duration string to timedelta"""
        duration = duration.strip().lower()
        
        if duration.endswith('s'):
            return timedelta(seconds=int(duration[:-1]))
        elif duration.endswith('m'):
            return timedelta(minutes=int(duration[:-1]))
        elif duration.endswith('h'):
            return timedelta(hours=int(duration[:-1]))
        elif duration.endswith('d'):
            return timedelta(days=int(duration[:-1]))
        else:
            return timedelta(minutes=int(duration))
    
    # Individual metric collection methods
    
    async def collect_tcp_in(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP in segments"""
        return await self._collect_single_metric("netstat_tcp_in", duration)
    
    async def collect_tcp_out(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP out segments"""
        return await self._collect_single_metric("netstat_tcp_out", duration)
    
    async def collect_tcp_listen_overflow(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP listen overflow errors"""
        return await self._collect_single_metric("netstat_tcp_error_listen_overflow", duration)
    
    async def collect_tcp_listen_drops(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP listen drops"""
        return await self._collect_single_metric("netstat_tcp_error_listen_drops", duration)
    
    async def collect_tcp_sync_retrans(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP sync retransmissions"""
        return await self._collect_single_metric("netstat_tcp_error_sync_retrans", duration)
    
    async def collect_tcp_segments_retrans(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP segment retransmissions"""
        return await self._collect_single_metric("netstat_tcp_error_segments_retrans", duration)
    
    async def collect_tcp_receive_errors(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP receive errors"""
        return await self._collect_single_metric("netstat_tcp_error_receive_in_errors", duration)
    
    async def collect_tcp_rst_sent(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP RST sent"""
        return await self._collect_single_metric("netstat_tcp_error_rst_sent_out_errors", duration)
    
    async def collect_tcp_rcv_queue_drop(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP receive queue drops"""
        return await self._collect_single_metric("netstat_tcp_error_receive_quene_drop", duration)
    
    async def collect_tcp_out_order_queue(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP out of order queue"""
        return await self._collect_single_metric("netstat_tcp_error_out_order_queue", duration)
    
    async def collect_tcp_timeouts(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP timeouts"""
        return await self._collect_single_metric("netstat_tcp_error_tcp_timeouts", duration)
    
    async def collect_tcp_syncookie_failures(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP SYN cookie failures"""
        return await self._collect_single_metric("node_tcp_sync_cookie_failures", duration)
    
    async def collect_tcp_syncookie_validated(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP SYN cookie validated"""
        return await self._collect_single_metric("node_tcp_sync_cookie_validated", duration)
    
    async def collect_tcp_syncookie_sent(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP SYN cookie sent"""
        return await self._collect_single_metric("node_tcp_sync_cookie_sent", duration)
    
    async def collect_tcp_max_conn(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP max connections"""
        return await self._collect_single_metric("node_netstat_Tcp_MaxConn", duration)
    
    async def collect_tcp_curr_estab(self, duration: str = "5m") -> Dict[str, Any]:
        """Collect TCP current established connections"""
        return await self._collect_single_metric("node_netstat_Tcp_CurrEstab", duration)
    
    async def _collect_single_metric(self, metric_name: str, duration: str) -> Dict[str, Any]:
        """Helper method to collect a single metric"""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - self._parse_duration(duration)
        
        # Get metric configuration
        metric = self.config.get_metric_by_name(metric_name)
        if not metric:
            return {"error": f"Metric not found: {metric_name}"}
        
        # Get node groups
        node_groups = await self.utility.get_node_groups(self.prometheus)
        
        # Collect metric data
        result = await self._collect_metric(metric, start_time, end_time, node_groups)
        
        return {
            "metric": metric_name,
            "collection_time": end_time.isoformat(),
            "duration": duration,
            "timezone": "UTC",
            **result
        }


async def main():
    """Example usage"""
    try:
        # Initialize configuration
        config = Config()
        
        # Initialize OpenShift authentication
        auth = OpenShiftAuth()
        await auth.initialize()
        
        # Test Prometheus connection
        prom_config = auth.get_prometheus_config()
        if not await auth.test_prometheus_connection():
            print("❌ Failed to connect to Prometheus")
            return
        
        # Initialize Prometheus client
        prometheus = PrometheusBaseQuery(
            prometheus_url=prom_config['url'],
            token=prom_config['token']
        )
        
        # Initialize collector
        collector = netStatTCPCollector(config, auth, prometheus)
        
        # Collect all metrics
        print("📊 Collecting TCP netstat metrics...")
        results = await collector.collect_all_metrics(duration="5m")
        
        # Print results
        import json
        print(json.dumps(results, indent=2))
        
        # Example: Collect specific metric
        print("\n📊 Collecting TCP retransmissions...")
        retrans_result = await collector.collect_tcp_segments_retrans(duration="5m")
        print(json.dumps(retrans_result, indent=2))
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'prometheus' in locals():
            await prometheus.close()


if __name__ == "__main__":
    asyncio.run(main())