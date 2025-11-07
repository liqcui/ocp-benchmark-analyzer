"""
OVS Usage Collector for OpenShift/Kubernetes
Collects CPU, RAM, and flow metrics for OVS components
File: /tools/ovnk_benchmark_prometheus_ovnk_ovs.py
"""

import json
import asyncio
import yaml
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from pathlib import Path
import statistics
import sys
import os

# Import the base modules
sys.path.append('/tools')
from .ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery, PrometheusQueryError
from ocauth.openshift_auth import OpenShiftAuth


class OVSUsageCollector:
    """Collect OVS usage metrics from Prometheus"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery, auth_client: OpenShiftAuth):
        self.prometheus_client = prometheus_client
        self.auth_client = auth_client
        self.metrics_config = self._load_metrics_config()
        
        # Default PromQL queries if metrics.yaml is not available
        self.default_queries = {
            'ovs-vswitchd-cpu-usage': {
                'query': 'sum by(node) (irate(container_cpu_usage_seconds_total{id=~"/system.slice/ovs-vswitchd.service"}[5m])*100)',
                'unit': 'percent'
            },
            'ovsdb-server-cpu-usage': {
                'query': 'sum(irate(container_cpu_usage_seconds_total{id=~"/system.slice/ovsdb-server.service"}[2m]) * 100) by (node)',
                'unit': 'percent'
            },
            'ovs-db-memory': {
                'query': 'ovs_db_process_resident_memory_bytes',
                'unit': 'bytes'
            },
            'ovs-vswitchd-memory': {
                'query': 'ovs_vswitchd_process_resident_memory_bytes',
                'unit': 'bytes'
            }
        }
    
    def _load_metrics_config(self) -> Dict[str, Any]:
        """Load metrics configuration from metrics.yaml"""
        try:
            metrics_path = Path('metrics.yaml')
            if metrics_path.exists():
                with open(metrics_path, 'r') as f:
                    return yaml.safe_load(f)
            else:
                print("⚠️  metrics.yaml not found, using default queries")
                return {}
        except Exception as e:
            print(f"⚠️  Error loading metrics.yaml: {e}")
            return {}
    
    def _get_query_config(self, metric_name: str) -> Dict[str, str]:
        """Get query configuration from metrics.yaml or defaults"""
        if self.metrics_config:
            # Search in metrics config
            for metric in self.metrics_config.get('metrics', []):
                if metric.get('metricName') == metric_name or metric_name in metric.get('metricName', ''):
                    return {
                        'query': metric.get('query', ''),
                        'unit': metric.get('unit', '')
                    }
        
        # Fallback to default queries
        return self.default_queries.get(metric_name, {})
    
    def _convert_bytes_to_readable(self, bytes_value: float) -> Tuple[float, str]:
        """Convert bytes to KB, MB, or GB"""
        if bytes_value >= 1024**3:  # GB
            return round(bytes_value / (1024**3), 2), 'GB'
        elif bytes_value >= 1024**2:  # MB
            return round(bytes_value / (1024**2), 2), 'MB'
        elif bytes_value >= 1024:  # KB
            return round(bytes_value / 1024, 2), 'KB'
        else:
            return round(bytes_value, 2), 'bytes'
    
    def _calculate_stats(self, values: List[float]) -> Dict[str, float]:
        """Calculate min, avg, max statistics"""
        if not values:
            return {'min': 0, 'avg': 0, 'max': 0}
        
        return {
            'min': round(min(values), 2),
            'avg': round(statistics.mean(values), 2),
            'max': round(max(values), 2)
        }
    
    async def query_ovs_cpu_usage(self, duration: Optional[str] = None, time: Optional[str] = None) -> Dict[str, Any]:
        """Query CPU usage for OVS components"""
        try:
            results = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'query_type': 'instant' if not duration else 'range',
                'ovs_vswitchd_cpu': [],
                'ovsdb_server_cpu': [],
                'summary': {
                    'ovs_vswitchd_top10': [],
                    'ovsdb_server_top10': []
                }
            }
            
            # Get query configurations
            vswitchd_config = self._get_query_config('ovs-vswitchd-cpu-usage')
            ovsdb_config = self._get_query_config('ovsdb-server-cpu-usage')
            
            queries = {
                'ovs_vswitchd': vswitchd_config.get('query', self.default_queries['ovs-vswitchd-cpu-usage']['query']),
                'ovsdb_server': ovsdb_config.get('query', self.default_queries['ovsdb-server-cpu-usage']['query'])
            }
            
            if duration:
                # Range query
                start_time, end_time = self.prometheus_client.get_time_range_from_duration(duration, time)
                query_results = await self.prometheus_client.query_multiple_range(
                    queries, start_time, end_time, '15s'
                )
            else:
                # Instant query
                query_results = await self.prometheus_client.query_multiple_instant(queries, time)
            
            # Process OVS vswitchd results
            if 'ovs_vswitchd' in query_results and 'error' not in query_results['ovs_vswitchd']:
                formatted_results = self.prometheus_client.format_query_result(
                    query_results['ovs_vswitchd'], include_labels=True
                )
                
                for result in formatted_results:
                    node_name = result.get('labels', {}).get('node', 'unknown')
                    
                    if duration and 'values' in result:
                        # Range query - calculate stats
                        values = [v['value'] for v in result['values'] if v['value'] is not None]
                        stats = self._calculate_stats(values)
                    else:
                        # Instant query
                        value = result.get('value', 0) or 0
                        stats = {'min': value, 'avg': value, 'max': value}
                    
                    results['ovs_vswitchd_cpu'].append({
                        'node_name': node_name,
                        **stats,
                        'unit': '%'
                    })
            
            # Process OVSDB server results
            if 'ovsdb_server' in query_results and 'error' not in query_results['ovsdb_server']:
                formatted_results = self.prometheus_client.format_query_result(
                    query_results['ovsdb_server'], include_labels=True
                )
                
                for result in formatted_results:
                    node_name = result.get('labels', {}).get('node', 'unknown')
                    
                    if duration and 'values' in result:
                        values = [v['value'] for v in result['values'] if v['value'] is not None]
                        stats = self._calculate_stats(values)
                    else:
                        value = result.get('value', 0) or 0
                        stats = {'min': value, 'avg': value, 'max': value}
                    
                    results['ovsdb_server_cpu'].append({
                        'node_name': node_name,
                        **stats,
                        'unit': '%'
                    })
            
            # Generate top 10 summaries
            results['summary']['ovs_vswitchd_top10'] = sorted(
                results['ovs_vswitchd_cpu'], 
                key=lambda x: x['max'], 
                reverse=True
            )[:10]
            
            results['summary']['ovsdb_server_top10'] = sorted(
                results['ovsdb_server_cpu'], 
                key=lambda x: x['max'], 
                reverse=True
            )[:10]
            
            return results
            
        except Exception as e:
            return {
                'error': f'Failed to query OVS CPU usage: {str(e)}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
    
    async def query_ovs_memory_usage(self, duration: Optional[str] = None, time: Optional[str] = None) -> Dict[str, Any]:
        """Query RAM usage for OVS components"""
        try:
            results = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'query_type': 'instant' if not duration else 'range',
                'ovs_db_memory': [],
                'ovs_vswitchd_memory': [],
                'summary': {
                    'ovs_db_top10': [],
                    'ovs_vswitchd_top10': []
                }
            }
            
            # Get query configurations
            db_config = self._get_query_config('ovs-db-memory')
            vswitchd_config = self._get_query_config('ovs-vswitchd-memory')
            
            queries = {
                'ovs_db_memory': db_config.get('query', self.default_queries['ovs-db-memory']['query']),
                'ovs_vswitchd_memory': vswitchd_config.get('query', self.default_queries['ovs-vswitchd-memory']['query'])
            }
            
            if duration:
                start_time, end_time = self.prometheus_client.get_time_range_from_duration(duration, time)
                query_results = await self.prometheus_client.query_multiple_range(
                    queries, start_time, end_time, '15s'
                )
            else:
                query_results = await self.prometheus_client.query_multiple_instant(queries, time)
            
            # Process OVS DB memory results
            if 'ovs_db_memory' in query_results and 'error' not in query_results['ovs_db_memory']:
                formatted_results = self.prometheus_client.format_query_result(
                    query_results['ovs_db_memory'], include_labels=True
                )
                
                for result in formatted_results:
                    pod_name = result.get('labels', {}).get('pod', result.get('labels', {}).get('instance', 'unknown'))
                    
                    if duration and 'values' in result:
                        values = [v['value'] for v in result['values'] if v['value'] is not None]
                        byte_stats = self._calculate_stats(values)
                    else:
                        value = result.get('value', 0) or 0
                        byte_stats = {'min': value, 'avg': value, 'max': value}
                    
                    # Convert to readable units
                    min_val, min_unit = self._convert_bytes_to_readable(byte_stats['min'])
                    avg_val, avg_unit = self._convert_bytes_to_readable(byte_stats['avg'])
                    max_val, max_unit = self._convert_bytes_to_readable(byte_stats['max'])
                    
                    results['ovs_db_memory'].append({
                        'pod_name': pod_name,
                        'min': min_val,
                        'avg': avg_val,
                        'max': max_val,
                        'unit': max_unit  # Use max unit as representative
                    })
            
            # Process OVS vswitchd memory results
            if 'ovs_vswitchd_memory' in query_results and 'error' not in query_results['ovs_vswitchd_memory']:
                formatted_results = self.prometheus_client.format_query_result(
                    query_results['ovs_vswitchd_memory'], include_labels=True
                )
                
                for result in formatted_results:
                    pod_name = result.get('labels', {}).get('pod', result.get('labels', {}).get('instance', 'unknown'))
                    
                    if duration and 'values' in result:
                        values = [v['value'] for v in result['values'] if v['value'] is not None]
                        byte_stats = self._calculate_stats(values)
                    else:
                        value = result.get('value', 0) or 0
                        byte_stats = {'min': value, 'avg': value, 'max': value}
                    
                    # Convert to readable units
                    min_val, min_unit = self._convert_bytes_to_readable(byte_stats['min'])
                    avg_val, avg_unit = self._convert_bytes_to_readable(byte_stats['avg'])
                    max_val, max_unit = self._convert_bytes_to_readable(byte_stats['max'])
                    
                    results['ovs_vswitchd_memory'].append({
                        'pod_name': pod_name,
                        'min': min_val,
                        'avg': avg_val,
                        'max': max_val,
                        'unit': max_unit
                    })
            
            # Generate top 10 summaries (by max usage)
            results['summary']['ovs_db_top10'] = sorted(
                results['ovs_db_memory'],
                key=lambda x: x['max'],
                reverse=True
            )[:10]
            
            results['summary']['ovs_vswitchd_top10'] = sorted(
                results['ovs_vswitchd_memory'],
                key=lambda x: x['max'],
                reverse=True
            )[:10]
            
            return results
            
        except Exception as e:
            return {
                'error': f'Failed to query OVS memory usage: {str(e)}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
    
    async def query_ovs_dp_flows_total(self, duration: Optional[str] = None, time: Optional[str] = None) -> Dict[str, Any]:
        """Query ovs_vswitchd_dp_flows_total metric"""
        try:
            query = 'ovs_vswitchd_dp_flows_total'
            
            if duration:
                start_time, end_time = self.prometheus_client.get_time_range_from_duration(duration, time)
                result = await self.prometheus_client.query_range(query, start_time, end_time, '15s')
            else:
                result = await self.prometheus_client.query_instant(query, time)
            
            formatted_results = self.prometheus_client.format_query_result(result, include_labels=True)
            
            flows_data = []
            for item in formatted_results:
                instance = item.get('labels', {}).get('instance', 'unknown')
                
                if duration and 'values' in item:
                    values = [v['value'] for v in item['values'] if v['value'] is not None]
                    stats = self._calculate_stats(values)
                else:
                    value = item.get('value', 0) or 0
                    stats = {'min': value, 'avg': value, 'max': value}
                
                flows_data.append({
                    'instance': instance,
                    **stats,
                    'unit': 'flows'
                })
            
            # Sort by max flows and get top 10
            top_10 = sorted(flows_data, key=lambda x: x['max'], reverse=True)[:10]
            
            return {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'query_type': 'instant' if not duration else 'range',
                'metric': 'ovs_vswitchd_dp_flows_total',
                'data': flows_data,
                'top_10': top_10
            }
            
        except Exception as e:
            return {
                'error': f'Failed to query dp flows: {str(e)}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
    
    async def query_ovs_bridge_flows_total(self, duration: Optional[str] = None, time: Optional[str] = None) -> Dict[str, Any]:
        """Query ovs_vswitchd_bridge_flows_total for br-int and br-ex bridges"""
        try:
            queries = {
                'br_int': 'ovs_vswitchd_bridge_flows_total{bridge="br-int"}',
                'br_ex': 'ovs_vswitchd_bridge_flows_total{bridge="br-ex"}'
            }
            
            if duration:
                start_time, end_time = self.prometheus_client.get_time_range_from_duration(duration, time)
                query_results = await self.prometheus_client.query_multiple_range(
                    queries, start_time, end_time, '15s'
                )
            else:
                query_results = await self.prometheus_client.query_multiple_instant(queries, time)
            
            results = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'query_type': 'instant' if not duration else 'range',
                'br_int_flows': [],
                'br_ex_flows': [],
                'top_10': {
                    'br_int': [],
                    'br_ex': []
                }
            }
            
            # Process br-int results
            if 'br_int' in query_results and 'error' not in query_results['br_int']:
                formatted_results = self.prometheus_client.format_query_result(
                    query_results['br_int'], include_labels=True
                )
                
                for result in formatted_results:
                    instance = result.get('labels', {}).get('instance', 'unknown')
                    
                    if duration and 'values' in result:
                        values = [v['value'] for v in result['values'] if v['value'] is not None]
                        stats = self._calculate_stats(values)
                    else:
                        value = result.get('value', 0) or 0
                        stats = {'min': value, 'avg': value, 'max': value}
                    
                    results['br_int_flows'].append({
                        'instance': instance,
                        'bridge': 'br-int',
                        **stats,
                        'unit': 'flows'
                    })
            
            # Process br-ex results
            if 'br_ex' in query_results and 'error' not in query_results['br_ex']:
                formatted_results = self.prometheus_client.format_query_result(
                    query_results['br_ex'], include_labels=True
                )
                
                for result in formatted_results:
                    instance = result.get('labels', {}).get('instance', 'unknown')
                    
                    if duration and 'values' in result:
                        values = [v['value'] for v in result['values'] if v['value'] is not None]
                        stats = self._calculate_stats(values)
                    else:
                        value = result.get('value', 0) or 0
                        stats = {'min': value, 'avg': value, 'max': value}
                    
                    results['br_ex_flows'].append({
                        'instance': instance,
                        'bridge': 'br-ex',
                        **stats,
                        'unit': 'flows'
                    })
            
            # Generate top 10 for each bridge
            results['top_10']['br_int'] = sorted(
                results['br_int_flows'],
                key=lambda x: x['max'],
                reverse=True
            )[:10]
            
            results['top_10']['br_ex'] = sorted(
                results['br_ex_flows'],
                key=lambda x: x['max'],
                reverse=True
            )[:10]
            
            return results
            
        except Exception as e:
            return {
                'error': f'Failed to query bridge flows: {str(e)}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
    
    async def query_ovs_connection_metrics(self, duration: Optional[str] = None, time: Optional[str] = None) -> Dict[str, Any]:
        """Query OVS connection metrics: stream_open, rconn_overflow, rconn_discarded"""
        try:
            queries = {
                'stream_open': 'sum(ovs_vswitchd_stream_open)',
                'rconn_overflow': 'sum(ovs_vswitchd_rconn_overflow)',
                'rconn_discarded': 'sum(ovs_vswitchd_rconn_discarded)'
            }
            
            if duration:
                start_time, end_time = self.prometheus_client.get_time_range_from_duration(duration, time)
                query_results = await self.prometheus_client.query_multiple_range(
                    queries, start_time, end_time, '15s'
                )
            else:
                query_results = await self.prometheus_client.query_multiple_instant(queries, time)
            
            results = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'query_type': 'instant' if not duration else 'range',
                'connection_metrics': {}
            }
            
            for metric_name, query_result in query_results.items():
                if 'error' not in query_result:
                    formatted_results = self.prometheus_client.format_query_result(query_result)
                    
                    if formatted_results:
                        result = formatted_results[0]  # Sum queries return single result
                        
                        if duration and 'values' in result:
                            values = [v['value'] for v in result['values'] if v['value'] is not None]
                            stats = self._calculate_stats(values)
                        else:
                            value = result.get('value', 0) or 0
                            stats = {'min': value, 'avg': value, 'max': value}
                        
                        results['connection_metrics'][metric_name] = {
                            **stats,
                            'unit': 'count'
                        }
                    else:
                        results['connection_metrics'][metric_name] = {
                            'min': 0, 'avg': 0, 'max': 0, 'unit': 'count'
                        }
                else:
                    results['connection_metrics'][metric_name] = {
                        'error': query_result['error']
                    }
            
            return results
            
        except Exception as e:
            return {
                'error': f'Failed to query connection metrics: {str(e)}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
    
    async def collect_all_ovs_metrics(self, duration: Optional[str] = None, time: Optional[str] = None) -> Dict[str, Any]:
        """Collect all OVS metrics in one comprehensive report"""
        try:
            print(f"🔍 Collecting OVS metrics {'for duration: ' + duration if duration else 'instant query'}...")
            
            # Collect all metrics concurrently
            tasks = [
                self.query_ovs_cpu_usage(duration, time),
                self.query_ovs_memory_usage(duration, time),
                self.query_ovs_dp_flows_total(duration, time),
                self.query_ovs_bridge_flows_total(duration, time),
                self.query_ovs_connection_metrics(duration, time)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            comprehensive_report = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'collection_type': 'instant' if not duration else f'range_{duration}',
                'cpu_usage': results[0] if not isinstance(results[0], Exception) else {'error': str(results[0])},
                'memory_usage': results[1] if not isinstance(results[1], Exception) else {'error': str(results[1])},
                'dp_flows': results[2] if not isinstance(results[2], Exception) else {'error': str(results[2])},
                'bridge_flows': results[3] if not isinstance(results[3], Exception) else {'error': str(results[3])},
                'connection_metrics': results[4] if not isinstance(results[4], Exception) else {'error': str(results[4])}
            }
            
            print("✅ OVS metrics collection completed")
            return comprehensive_report
            
        except Exception as e:
            return {
                'error': f'Failed to collect comprehensive OVS metrics: {str(e)}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }


# Example usage and testing
async def main():
    """Main function for testing OVS collector"""
    try:
        # Initialize authentication
        auth = OpenShiftAuth()
        await auth.initialize()
        
        # Test Prometheus connection
        if not await auth.test_prometheus_connection():
            print("❌ Cannot connect to Prometheus")
            return
        
        # Initialize Prometheus client
        async with PrometheusBaseQuery(auth.prometheus_url, auth.prometheus_token) as prometheus_client:
            # Initialize OVS collector
            ovs_collector = OVSUsageCollector(prometheus_client, auth)
            
            print("\n=== Testing Instant Queries ===")
            
            # Test CPU usage
            cpu_results = await ovs_collector.query_ovs_cpu_usage()
            print(f"CPU Usage Results: {json.dumps(cpu_results, indent=2)}")
            
            # Test Memory usage
            memory_results = await ovs_collector.query_ovs_memory_usage()
            print(f"Memory Usage Results: {json.dumps(memory_results, indent=2)}")
            
            # Test DP flows
            dp_flows = await ovs_collector.query_ovs_dp_flows_total()
            print(f"DP Flows: {json.dumps(dp_flows, indent=2)}")
            
            print("\n=== Testing Range Queries (5m) ===")
            
            # Test with 5-minute duration
            range_results = await ovs_collector.collect_all_ovs_metrics(duration='5m')
            print(f"5-minute Range Results: {json.dumps(range_results, indent=2)}")
            
    except Exception as e:
        print(f"❌ Error in main: {e}")


if __name__ == "__main__":
    asyncio.run(main())