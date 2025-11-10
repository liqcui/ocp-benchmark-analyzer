"""
OVS Usage Collector for OpenShift/Kubernetes
Collects CPU, RAM, flow, and performance metrics for OVS components
Enhanced version using metrics configuration from metrics-ovs.yml
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pytz
import os
import sys

# Ensure project root on sys.path for utils imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ocauth.openshift_auth import OpenShiftAuth as OCPAuth
from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from config.metrics_config_reader import Config


class OVSUsageCollector:
    """Collector for OVS usage and performance metrics"""
    
    def __init__(self, ocp_auth: OCPAuth, duration: str = "1h", metrics_file_path: str = None):
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.logger = logging.getLogger(__name__)
        self.utility = mcpToolsUtility(ocp_auth)
        
        # Load metrics configuration
        if metrics_file_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metrics_file_path = os.path.join(project_root, 'config', 'metrics-ovs.yml')
        
        self.config = Config()
        load_result = self.config.load_metrics_file(metrics_file_path)
        
        # Get category-specific metrics
        self.category = "ovs"
        self.ovs_metrics = self.config.get_metrics_by_category(self.category)
        
        # Log metrics loading
        if load_result.get('success'):
            metrics_count = len(self.ovs_metrics)
            self.logger.info(f"✅ Loaded {metrics_count} metrics from {self.category} category (OVSUsageCollector)")
        else:
            self.logger.warning(f"⚠️ No {self.category} metrics found in configuration")
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all OVS metrics and return comprehensive results"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                # Test connection first
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': "Prometheus connection failed",
                        'timestamp': datetime.now(pytz.UTC).isoformat()
                    }
                
                # Collect results for all metrics
                results = {
                    'status': 'success',
                    'timestamp': datetime.now(pytz.UTC).isoformat(),
                    'duration': self.duration,
                    'category': 'ovs',
                    'metrics': {}
                }
                
                # Process each metric dynamically from config
                for metric_config in self.ovs_metrics:
                    metric_name = metric_config['name']
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        metric_result = await self._collect_generic_metric(prom, metric_config)
                        results['metrics'][metric_name] = metric_result
                        
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        results['metrics'][metric_name] = {
                            'status': 'error',
                            'error': str(e)
                        }
                
                # Add summary
                successful_metrics = sum(1 for m in results['metrics'].values() if m.get('status') == 'success')
                total_metrics = len(results['metrics'])
                
                results['summary'] = {
                    'total_metrics': total_metrics,
                    'successful_metrics': successful_metrics,
                    'failed_metrics': total_metrics - successful_metrics
                }
                
                # Generate cluster summary and add to results
                cluster_summary = self._generate_cluster_summary(results)
                results['cluster_health'] = cluster_summary['cluster_health']
                results['performance_indicators'] = cluster_summary['performance_indicators']
                results['recommendations'] = cluster_summary['recommendations']
                
                return results
                
        except Exception as e:
            self.logger.error(f"Error in collect_all_metrics: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(pytz.UTC).isoformat()
            }
    
    async def _collect_generic_metric(self, prom: PrometheusBaseQuery, metric_config: Dict[str, Any]) -> Dict[str, Any]:
        """Generic method to collect any OVS metric"""
        query = metric_config['expr']
        metric_name = metric_config['name']
        unit = metric_config.get('unit', 'unknown')
        
        try:
            result = await self._query_with_stats(prom, query, self.duration)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            # Process results based on metric labels
            metric_stats = {}
            
            for series in result.get('series_data', []):
                labels = series['labels']
                stats = series['statistics']
                
                # Determine grouping key (node, pod, instance, or bridge)
                grouping_key = None
                grouping_value = 'unknown'
                
                if 'node' in labels:
                    grouping_key = 'node'
                    grouping_value = labels['node']
                elif 'pod' in labels:
                    grouping_key = 'pod'
                    grouping_value = labels['pod']
                elif 'instance' in labels:
                    grouping_key = 'instance'
                    grouping_value = labels['instance']
                elif 'bridge' in labels:
                    grouping_key = 'bridge'
                    grouping_value = labels['bridge']
                    # Also include instance for bridge metrics
                    if 'instance' in labels:
                        grouping_value = f"{labels['bridge']}@{labels['instance']}"
                
                if grouping_value != 'unknown':
                    # Format stats based on unit
                    formatted_stats = self._format_stats_by_unit(stats, unit)
                    formatted_stats['data_points'] = stats.get('count', 0)
                    
                    # Add additional context
                    if grouping_key == 'bridge' and 'instance' in labels:
                        formatted_stats['bridge'] = labels['bridge']
                        formatted_stats['instance'] = labels['instance']
                    
                    metric_stats[grouping_value] = formatted_stats
            
            # Get node mapping for pods if applicable
            node_mapping = {}
            if any('pod' in key or '@' in key for key in metric_stats.keys()):
                node_mapping = self.utility.get_pod_to_node_mapping_via_oc(namespace="openshift-ovn-kubernetes")
            
            return {
                'status': 'success',
                'metric': metric_name,
                'title': metric_config.get('title', metric_name.replace('_', ' ').title()),
                'unit': unit,
                'description': metric_config.get('description', f'OVS metric: {metric_name}'),
                'metric_data': metric_stats,
                'node_mapping': node_mapping,
                'total_entries': len(metric_stats),
                'overall_stats': result.get('overall_statistics', {})
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting metric {metric_name}: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def _format_stats_by_unit(self, stats: Dict[str, Any], unit: str) -> Dict[str, Any]:
        """Format statistics based on the metric unit"""
        if not stats:
            return {}
        
        # Determine precision based on unit
        precision = 2
        if unit == 'percent':
            precision = 2
        elif unit == 'bytes':
            precision = 0
        elif unit in ['bytes_per_second', 'packets_per_second', 'errors_per_second']:
            precision = 3
        elif 'flow' in unit:
            precision = 0
        elif 'count' in unit:
            precision = 0
        
        formatted = {
            'avg': round(stats.get('avg', 0), precision),
            'max': round(stats.get('max', 0), precision),
            'min': round(stats.get('min', 0), precision),
        }
        
        if stats.get('latest') is not None:
            formatted['latest'] = round(stats['latest'], precision)
        
        return formatted
    
    async def _query_with_stats(self, prom_client: PrometheusBaseQuery, query: str, duration: str) -> Dict[str, Any]:
        """Execute a range query and compute basic statistics per series and overall."""
        try:
            start, end = prom_client.get_time_range_from_duration(duration)
            data = await prom_client.query_range(query, start, end, step='15s')

            series_data: List[Dict[str, Any]] = []
            all_values: List[float] = []

            for item in data.get('result', []) if isinstance(data, dict) else []:
                metric_labels = item.get('metric', {})
                values = []
                for ts, val in item.get('values', []) or []:
                    try:
                        v = float(val)
                    except (ValueError, TypeError):
                        continue
                    if v != float('inf') and v != float('-inf'):
                        values.append(v)

                stats: Dict[str, Any] = {}
                if values:
                    avg_v = sum(values) / len(values)
                    max_v = max(values)
                    min_v = min(values)
                    stats = {
                        'avg': avg_v,
                        'max': max_v,
                        'min': min_v,
                        'count': len(values),
                        'latest': values[-1]
                    }
                    all_values.extend(values)

                series_data.append({'labels': metric_labels, 'statistics': stats})

            overall_statistics: Dict[str, Any] = {}
            if all_values:
                overall_statistics = {
                    'avg': sum(all_values) / len(all_values),
                    'max': max(all_values),
                    'min': min(all_values),
                    'count': len(all_values)
                }

            return {
                'status': 'success',
                'series_data': series_data,
                'overall_statistics': overall_statistics
            }
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    # Individual metric collection methods
    
    async def get_ovs_vswitchd_cpu_usage(self) -> Dict[str, Any]:
        """Get OVS vswitchd CPU usage per node"""
        metric_config = self.config.get_metric_by_name('ovs_vswitchd_cpu_usage')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovsdb_server_cpu_usage(self) -> Dict[str, Any]:
        """Get OVSDB server CPU usage per node"""
        metric_config = self.config.get_metric_by_name('ovsdb_server_cpu_usage')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_db_memory_size(self) -> Dict[str, Any]:
        """Get OVS database memory size"""
        metric_config = self.config.get_metric_by_name('ovs_db_memory_size_bytes')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_vswitch_memory_size(self) -> Dict[str, Any]:
        """Get OVS vswitchd memory size"""
        metric_config = self.config.get_metric_by_name('ovs_vswitch_memory_size_bytes')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_datapath_flows_total(self) -> Dict[str, Any]:
        """Get total datapath flows"""
        metric_config = self.config.get_metric_by_name('ovs_datapath_flows_total')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_bridge_flows_br_int(self) -> Dict[str, Any]:
        """Get br-int bridge flows"""
        metric_config = self.config.get_metric_by_name('ovs_bridge_flows_br_int')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_bridge_flows_br_ex(self) -> Dict[str, Any]:
        """Get br-ex bridge flows"""
        metric_config = self.config.get_metric_by_name('ovs_bridge_flows_br_ex')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_stream_connections_open(self) -> Dict[str, Any]:
        """Get open OVS stream connections"""
        metric_config = self.config.get_metric_by_name('ovs_stream_connections_open')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_rconn_overflow_total(self) -> Dict[str, Any]:
        """Get remote connection overflow count"""
        metric_config = self.config.get_metric_by_name('ovs_rconn_overflow_total')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_rconn_discarded_total(self) -> Dict[str, Any]:
        """Get remote connections discarded count"""
        metric_config = self.config.get_metric_by_name('ovs_rconn_discarded_total')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_megaflow_cache_hits(self) -> Dict[str, Any]:
        """Get megaflow cache hits"""
        metric_config = self.config.get_metric_by_name('ovs_megaflow_cache_hits')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_megaflow_cache_misses(self) -> Dict[str, Any]:
        """Get megaflow cache misses"""
        metric_config = self.config.get_metric_by_name('ovs_megaflow_cache_misses')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_datapath_packet_rate(self) -> Dict[str, Any]:
        """Get datapath packet processing rate"""
        metric_config = self.config.get_metric_by_name('ovs_datapath_packet_rate')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_datapath_error_rate(self) -> Dict[str, Any]:
        """Get datapath error rate"""
        metric_config = self.config.get_metric_by_name('ovs_datapath_error_rate')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_interface_rx_bytes_rate(self) -> Dict[str, Any]:
        """Get OVS interface receive bytes rate"""
        metric_config = self.config.get_metric_by_name('ovs_interface_rx_bytes_rate')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_interface_tx_bytes_rate(self) -> Dict[str, Any]:
        """Get OVS interface transmit bytes rate"""
        metric_config = self.config.get_metric_by_name('ovs_interface_tx_bytes_rate')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    async def get_ovs_interface_rx_dropped_rate(self) -> Dict[str, Any]:
        """Get OVS interface receive packets dropped rate"""
        metric_config = self.config.get_metric_by_name('ovs_interface_rx_dropped_rate')
        if not metric_config:
            return {'status': 'error', 'error': 'Metric not found in configuration'}
        
        prometheus_config = self.ocp_auth.get_prometheus_config()
        async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
            return await self._collect_generic_metric(prom, metric_config)
    
    def _generate_cluster_summary(self, full_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate cluster-wide OVS performance summary from collected metrics"""
        summary = {
            'cluster_health': {
                'total_nodes': 0,
                'nodes_with_data': 0,
                'ovs_performance': 'unknown'
            },
            'performance_indicators': {},
            'recommendations': []
        }
        
        if full_results.get('status') != 'success':
            return summary
        
        # Analyze CPU usage
        cpu_vswitchd = full_results['metrics'].get('ovs_vswitchd_cpu_usage', {})
        if cpu_vswitchd.get('status') == 'success':
            metric_data = cpu_vswitchd.get('metric_data', {})
            summary['cluster_health']['total_nodes'] = len(metric_data)
            
            max_cpu = max([stats.get('max', 0) for stats in metric_data.values()], default=0)
            avg_cpu = sum([stats.get('avg', 0) for stats in metric_data.values()]) / len(metric_data) if metric_data else 0
            
            summary['performance_indicators']['ovs_vswitchd_max_cpu_percent'] = max_cpu
            summary['performance_indicators']['ovs_vswitchd_avg_cpu_percent'] = round(avg_cpu, 2)
            
            if max_cpu > 80:
                summary['recommendations'].append("High OVS vswitchd CPU usage detected (>80%). Consider resource optimization.")
        
        # Analyze memory usage
        memory_vswitchd = full_results['metrics'].get('ovs_vswitch_memory_size_bytes', {})
        if memory_vswitchd.get('status') == 'success':
            metric_data = memory_vswitchd.get('metric_data', {})
            
            max_mem_bytes = max([stats.get('max', 0) for stats in metric_data.values()], default=0)
            max_mem_gb = round(max_mem_bytes / (1024**3), 2)
            
            summary['performance_indicators']['ovs_vswitchd_max_memory_gb'] = max_mem_gb
            
            if max_mem_gb > 2:
                summary['recommendations'].append(f"High OVS vswitchd memory usage detected ({max_mem_gb}GB). Monitor for memory leaks.")
        
        # Analyze flow counts
        flows = full_results['metrics'].get('ovs_datapath_flows_total', {})
        if flows.get('status') == 'success':
            metric_data = flows.get('metric_data', {})
            
            total_flows = sum([stats.get('avg', 0) for stats in metric_data.values()])
            max_flows = max([stats.get('max', 0) for stats in metric_data.values()], default=0)
            
            summary['performance_indicators']['total_datapath_flows'] = int(total_flows)
            summary['performance_indicators']['max_datapath_flows'] = int(max_flows)
            
            if max_flows > 100000:
                summary['recommendations'].append("Very high flow count detected (>100k). Monitor flow table performance.")
        
        # Determine overall health
        if not summary['recommendations']:
            summary['cluster_health']['ovs_performance'] = 'good'
        elif len(summary['recommendations']) <= 2:
            summary['cluster_health']['ovs_performance'] = 'warning'
        else:
            summary['cluster_health']['ovs_performance'] = 'critical'
        
        return summary
    
    async def get_metric_by_name(self, metric_name: str) -> Dict[str, Any]:
        """Get a specific metric by name"""
        try:
            metric_config = self.config.get_metric_by_name(metric_name)
            if not metric_config:
                return {
                    'status': 'error',
                    'error': f'Metric {metric_name} not found in configuration'
                }
            
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': "Prometheus connection failed"
                    }
                
                return await self._collect_generic_metric(prom, metric_config)
                
        except Exception as e:
            self.logger.error(f"Error getting metric {metric_name}: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }


# Convenience functions for metric collection

async def collect_ovs_metrics(ocp_auth: OCPAuth, duration: str = "1h") -> Dict[str, Any]:
    """Convenience function to collect all OVS metrics"""
    collector = OVSUsageCollector(ocp_auth, duration)
    return await collector.collect_all_metrics()


async def get_specific_ovs_metric(ocp_auth: OCPAuth, metric_name: str, duration: str = "1h") -> Dict[str, Any]:
    """Convenience function to get a specific OVS metric"""
    collector = OVSUsageCollector(ocp_auth, duration)
    return await collector.get_metric_by_name(metric_name)


# Example usage
async def main():
    """Main function for testing OVS collector"""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize authentication
        auth = OCPAuth()
        await auth.initialize()
        
        # Test Prometheus connection
        if not await auth.test_prometheus_connection():
            print("❌ Cannot connect to Prometheus")
            return
        
        # Initialize OVS collector
        collector = OVSUsageCollector(auth, duration='5m')
        
        print("\n=== Collecting All OVS Metrics ===")
        results = await collector.collect_all_metrics()
        
        print(f"\nStatus: {results['status']}")
        print(f"Total Metrics: {results['summary']['total_metrics']}")
        print(f"Successful: {results['summary']['successful_metrics']}")
        
        # Print sample metric
        if results['metrics']:
            first_metric = list(results['metrics'].keys())[0]
            print(f"\nSample Metric: {first_metric}")
            print(f"Data: {results['metrics'][first_metric]}")
        
        # Print cluster summary (now included in results)
        print("\n=== Cluster Summary ===")
        print(f"Cluster Health: {results.get('cluster_health', {})}")
        print(f"Performance Indicators: {results.get('performance_indicators', {})}")
        print(f"Recommendations: {results.get('recommendations', [])}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())