"""
OVN Basic Info Collector Module - Enhanced
Collects basic OVN database information, alerts, pod distribution, and latency metrics from Prometheus
"""

import asyncio
import json
import logging
from typing import Dict, Any, Optional, List
from .ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery, PrometheusQueryError
from .ovnk_benchmark_prometheus_utility import mcpToolsUtility

logger = logging.getLogger(__name__)


class ovnBasicInfoCollector:
    """Enhanced collector for OVN database information, alerts, pod distribution, and latency metrics"""
    
    def __init__(self, prometheus_url: str, token: Optional[str] = None):
        """
        Initialize the OVN Basic Info Collector
        
        Args:
            prometheus_url: Prometheus server URL
            token: Optional authentication token
        """
        self.prometheus_url = prometheus_url
        self.token = token
        self.utility = mcpToolsUtility()
        
        # Default OVN database metrics
        self.default_metrics = {
            "ovn_northbound_db_size": 'ovn_db_db_size_bytes{db_name=~"OVN_Northbound"}',
            "ovn_southbound_db_size": 'ovn_db_db_size_bytes{db_name=~"OVN_Southbound"}'
        }
        
        # Updated alerts query to get top 15
        self.alerts_query = 'topk(15,sum(ALERTS{severity!="none"}) by (alertname, severity))'
        self.pod_distribution_query = 'count(kube_pod_info{}) by (node)'
        
        # Default latency metrics (hardcoded fallback)
        self.default_latency_metrics = {
            "apiserver_request_duration": 'histogram_quantile(0.99, sum(rate(apiserver_request_duration_seconds_bucket[5m])) by (le, verb, resource))',
            "etcd_request_duration": 'histogram_quantile(0.99, sum(rate(etcd_request_duration_seconds_bucket[5m])) by (le, operation))',
            "ovn_controller_latency": 'histogram_quantile(0.95, sum(rate(ovn_controller_sb_db_transaction_duration_seconds_bucket[5m])) by (le))',
            "network_latency": 'histogram_quantile(0.95, sum(rate(network_rtt_seconds_bucket[5m])) by (le, destination))'
        }
        
        logger.debug(f"Initialized ovnBasicInfoCollector with URL={prometheus_url}")
    
    def _load_latency_metrics_from_file(self, file_path: str = "metrics-latency.yml") -> Dict[str, str]:
        """
        Load latency metrics from YAML file
        
        Args:
            file_path: Path to metrics-latency.yml file
            
        Returns:
            Dictionary of metric_name -> query_string
        """
        try:
            import yaml
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)
            
            metrics = {}
            if isinstance(data, dict) and 'metrics' in data:
                for metric_name, config in data['metrics'].items():
                    if isinstance(config, dict) and 'query' in config:
                        metrics[metric_name] = config['query']
                    elif isinstance(config, str):
                        metrics[metric_name] = config
            
            logger.info(f"Loaded {len(metrics)} latency metrics from {file_path}")
            return metrics if metrics else self.default_latency_metrics
            
        except Exception as e:
            logger.warning(f"Could not load metrics from {file_path}: {e}. Using hardcoded defaults.")
            return self.default_latency_metrics
    
    async def collect_max_values(self, metrics: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Collect maximum values for OVN database metrics (existing function - unchanged)
        
        Args:
            metrics: Optional dictionary of metric_name -> query_string
                    If None, uses default metrics
                    
        Returns:
            Dictionary with metric names and their maximum values in JSON format
        """
        if metrics is None:
            metrics = self.default_metrics
        
        logger.info(f"Collecting max values for metrics: {list(metrics.keys())}")
        
        async with PrometheusBaseQuery(self.prometheus_url, self.token) as prom:
            results = {}
            
            for metric_name, query in metrics.items():
                try:
                    logger.debug(f"Querying metric {metric_name}: {query}")
                    
                    # Query the metric
                    raw_result = await prom.query_instant(query)
                    formatted_result = prom.format_query_result(raw_result, include_labels=True)
                    
                    # Find maximum value
                    max_value = None
                    max_labels = None
                    
                    for item in formatted_result:
                        if item.get('value') is not None:
                            if max_value is None or item['value'] > max_value:
                                max_value = item['value']
                                max_labels = item.get('labels', {})
                    
                    # Store result
                    results[metric_name] = {
                        "max_value": max_value,
                        "labels": max_labels,
                        "unit": "bytes"  # Default unit for db_size_bytes metrics
                    }
                    
                    logger.debug(f"Metric {metric_name} max_value={max_value}")
                    
                except Exception as e:
                    logger.error(f"Error querying metric {metric_name}: {e}")
                    results[metric_name] = {
                        "max_value": None,
                        "error": str(e)
                    }
            
            logger.info(f"Collected max values for {len(results)} metrics")
            return results
    
    async def collect_top_alerts(self) -> Dict[str, Any]:
        """
        Collect top 15 alerts by severity with separated avg and max values per alertname
        
        Returns:
            Dictionary with top alerts information including per-alertname avg/max statistics
        """
        logger.info("Collecting top alerts with per-alertname avg and max")
        
        async with PrometheusBaseQuery(self.prometheus_url, self.token) as prom:
            try:
                raw_result = await prom.query_instant(self.alerts_query)
                formatted_result = prom.format_query_result(raw_result, include_labels=True)
                
                # Filter out items with valid values
                valid_alerts = [item for item in formatted_result if item.get('value') is not None]
                
                if not valid_alerts:
                    return {
                        "metric_name": "alerts_summary",
                        "query": self.alerts_query,
                        "alerts": [],
                        "total_alert_types": 0,
                        "alertname_statistics": {}
                    }
                
                # Group alerts by alertname
                alertname_groups = {}
                for item in valid_alerts:
                    labels = item.get('labels', {})
                    alertname = labels.get('alertname', 'unknown')
                    count = item.get('value', 0)
                    
                    if alertname not in alertname_groups:
                        alertname_groups[alertname] = []
                    
                    alertname_groups[alertname].append(count)
                
                # Sort all alerts by value (count) descending for display
                sorted_alerts = sorted(valid_alerts, key=lambda x: x['value'], reverse=True)
                
                # Prepare alert data for display
                alerts_data = []
                for item in sorted_alerts:
                    labels = item.get('labels', {})
                    alerts_data.append({
                        "alert_name": labels.get('alertname', 'unknown'),
                        "severity": labels.get('severity', 'unknown'),
                        "count": item.get('value', 0),
                        "timestamp": item.get('timestamp')
                    })
                
                # Calculate separated avg/max for each alertname
                alertname_statistics = {}
                for alertname, counts in alertname_groups.items():
                    alertname_statistics[alertname] = {
                        "avg_count": round(sum(counts) / len(counts), 2),
                        "max_count": max(counts)
                    }
                
                return {
                    "metric_name": "alerts_summary",
                    "query": self.alerts_query,
                    "alerts": alerts_data,
                    "total_alert_types": len(alerts_data),
                    "alertname_statistics": alertname_statistics
                }
                
            except Exception as e:
                logger.error(f"Error collecting alerts: {e}")
                return {
                    "metric_name": "alerts_summary",
                    "error": str(e),
                    "alerts": [],
                    "total_alert_types": 0,
                    "alertname_statistics": {}
                }
    
    async def collect_pod_distribution(self) -> Dict[str, Any]:
        """
        Collect pod distribution across nodes
        
        Returns:
            Dictionary with top 6 nodes by pod count
        """
        logger.info("Collecting pod distribution")
        
        async with PrometheusBaseQuery(self.prometheus_url, self.token) as prom:
            try:
                raw_result = await prom.query_instant(self.pod_distribution_query)
                formatted_result = prom.format_query_result(raw_result, include_labels=True)
                
                # Sort by value (pod count) descending and take top 6
                sorted_nodes = sorted(
                    [item for item in formatted_result if item.get('value') is not None],
                    key=lambda x: x['value'],
                    reverse=True
                )[:6]
                
                # Get node labels for role detection
                node_labels = await self.utility.get_all_node_labels(prom)
                
                distribution_data = []
                for item in sorted_nodes:
                    labels = item.get('labels', {})
                    node_name = labels.get('node', 'unknown')
                    
                    # Determine node role
                    node_role = 'unknown'
                    if node_name in node_labels:
                        node_role = self.utility.determine_node_role(node_name, node_labels[node_name])
                    
                    distribution_data.append({
                        "node_name": node_name,
                        "node_role": node_role,
                        "pod_count": int(item.get('value', 0)),
                        "timestamp": item.get('timestamp')
                    })
                
                return {
                    "metric_name": "pod_distribution",
                    "query": self.pod_distribution_query,
                    "top_nodes": distribution_data,
                    "total_nodes": len(distribution_data)
                }
                
            except Exception as e:
                logger.error(f"Error collecting pod distribution: {e}")
                return {
                    "metric_name": "pod_distribution",
                    "error": str(e),
                    "top_nodes": [],
                    "total_nodes": 0
                }
    
    async def collect_latency_metrics(self, metrics_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect latency metrics from file or use defaults
        
        Args:
            metrics_file: Path to metrics-latency.yml file
            
        Returns:
            Dictionary with latency metrics (top 6 results per metric)
        """
        logger.info("Collecting latency metrics")
        
        # Load metrics from file or use defaults
        if metrics_file:
            latency_metrics = self._load_latency_metrics_from_file(metrics_file)
        else:
            latency_metrics = self.default_latency_metrics
        
        async with PrometheusBaseQuery(self.prometheus_url, self.token) as prom:
            results = {}
            
            for metric_name, query in latency_metrics.items():
                try:
                    logger.debug(f"Querying latency metric {metric_name}: {query}")
                    
                    raw_result = await prom.query_instant(query)
                    formatted_result = prom.format_query_result(raw_result, include_labels=True)
                    
                    # Sort by value descending and take top 6
                    sorted_results = sorted(
                        [item for item in formatted_result if item.get('value') is not None],
                        key=lambda x: x['value'],
                        reverse=True
                    )[:6]
                    
                    metric_data = []
                    for item in sorted_results:
                        metric_data.append({
                            "value": item.get('value'),
                            "labels": item.get('labels', {}),
                            "timestamp": item.get('timestamp')
                        })
                    
                    results[metric_name] = {
                        "query": query,
                        "top_values": metric_data,
                        "unit": "seconds",
                        "count": len(metric_data)
                    }
                    
                except Exception as e:
                    logger.error(f"Error querying latency metric {metric_name}: {e}")
                    results[metric_name] = {
                        "query": query,
                        "error": str(e),
                        "top_values": [],
                        "count": 0
                    }
            
            return {
                "metric_name": "latency_summary",
                "metrics": results,
                "total_metrics": len(results)
            }
    
    async def collect_comprehensive_summary(self, metrics_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect all metrics and assemble into comprehensive JSON summary
        
        Args:
            metrics_file: Optional path to metrics-latency.yml file
            
        Returns:
            Complete JSON summary with all metrics (max 5 levels deep)
        """
        logger.info("Collecting comprehensive metrics summary")
        
        try:
            # Collect all metrics concurrently
            tasks = [
                self.collect_max_values(),
                get_pod_phase_counts(self.prometheus_url, self.token),
                self.collect_top_alerts(),
                self.collect_pod_distribution(),
                self.collect_latency_metrics(metrics_file)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Assemble summary
            summary = {
                "collection_timestamp": asyncio.get_event_loop().time(),
                "prometheus_url": self.prometheus_url,
                "metrics": {
                    "ovn_database": results[0] if not isinstance(results[0], Exception) else {"error": str(results[0])},
                    "pod_status": results[1] if not isinstance(results[1], Exception) else {"error": str(results[1])},
                    "alerts": results[2] if not isinstance(results[2], Exception) else {"error": str(results[2])},
                    "pod_distribution": results[3] if not isinstance(results[3], Exception) else {"error": str(results[3])},
                    "latency": results[4] if not isinstance(results[4], Exception) else {"error": str(results[4])}
                }
            }
            
            # Add summary statistics
            summary["summary"] = {
                "total_metric_categories": 5,
                "successful_collections": sum(1 for r in results if not isinstance(r, Exception)),
                "failed_collections": sum(1 for r in results if isinstance(r, Exception))
            }
            
            logger.info("Comprehensive summary collection completed")
            return summary
            
        except Exception as e:
            logger.error(f"Error collecting comprehensive summary: {e}")
            return {
                "collection_timestamp": asyncio.get_event_loop().time(),
                "prometheus_url": self.prometheus_url,
                "error": str(e),
                "metrics": {},
                "summary": {"total_metric_categories": 0, "successful_collections": 0, "failed_collections": 1}
            }
    
    def to_json(self, results: Dict[str, Any]) -> str:
        """
        Convert results to JSON format (existing function - unchanged)
        
        Args:
            results: Results dictionary from collect_max_values()
            
        Returns:
            JSON formatted string
        """
        return json.dumps(results, indent=2)


async def get_pod_phase_counts(prometheus_url: str, token: Optional[str] = None) -> Dict[str, Any]:
    """
    Get pod counts by phase from Prometheus (existing function - unchanged)
    
    Args:
        prometheus_url: Prometheus server URL
        token: Optional authentication token
        
    Returns:
        Dictionary with pod counts by phase in JSON format
    """
    query = "sum(kube_pod_status_phase{}) by (phase) > 0"
    
    logger.info("Collecting pod phase counts")
    logger.debug(f"Pod phase query: {query}")
    
    async with PrometheusBaseQuery(prometheus_url, token) as prom:
        try:
            # Execute the instant query
            raw_result = await prom.query_instant(query)
            formatted_result = prom.format_query_result(raw_result, include_labels=True)
            
            # Process results into phase counts
            phase_counts = {}
            total_pods = 0
            
            for item in formatted_result:
                if item.get('value') is not None and item.get('labels'):
                    phase = item['labels'].get('phase', 'unknown')
                    count = int(item['value'])
                    phase_counts[phase] = count
                    total_pods += count
                    logger.debug(f"Phase {phase}: {count} pods")
            
            # Prepare result
            result = {
                "metric_name": "pod-status",
                "total_pods": total_pods,
                "phases": phase_counts,
                "query_type": "instant",
                "timestamp": None  # Will be filled with actual timestamp if needed
            }
            
            # Add timestamp from first result if available
            if formatted_result and formatted_result[0].get('timestamp'):
                result["timestamp"] = formatted_result[0]['timestamp']
            
            logger.info(f"Collected pod phase counts: {len(phase_counts)} phases, {total_pods} total pods")
            return result
            
        except Exception as e:
            logger.error(f"Error querying pod phases: {e}")
            return {
                "metric_name": "pod-status",
                "error": str(e),
                "query_type": "instant"
            }


def get_pod_phase_counts_json(prometheus_url: str, token: Optional[str] = None) -> str:
    """
    Get pod counts by phase as JSON string (existing function - unchanged)
    
    Args:
        prometheus_url: Prometheus server URL
        token: Optional authentication token
        
    Returns:
        JSON formatted string with pod phase counts
    """
    loop = asyncio.get_event_loop()
    if loop.is_running():
        # If we're already in an async context, create a task
        task = asyncio.create_task(get_pod_phase_counts(prometheus_url, token))
        result = asyncio.run_coroutine_threadsafe(task, loop).result()
    else:
        # If not in async context, run normally
        result = asyncio.run(get_pod_phase_counts(prometheus_url, token))
    
    return json.dumps(result, indent=2)


# Convenience functions for new metrics
async def get_top_alerts_summary(prometheus_url: str, token: Optional[str] = None) -> Dict[str, Any]:
    """
    Get top alerts summary with avg and max statistics
    
    Args:
        prometheus_url: Prometheus server URL
        token: Optional authentication token
        
    Returns:
        Dictionary with top alerts summary including statistics
    """
    collector = ovnBasicInfoCollector(prometheus_url, token)
    return await collector.collect_top_alerts()


async def get_pod_distribution_summary(prometheus_url: str, token: Optional[str] = None) -> Dict[str, Any]:
    """
    Get pod distribution summary
    
    Args:
        prometheus_url: Prometheus server URL
        token: Optional authentication token
        
    Returns:
        Dictionary with pod distribution summary
    """
    collector = ovnBasicInfoCollector(prometheus_url, token)
    return await collector.collect_pod_distribution()


async def get_latency_metrics_summary(prometheus_url: str, token: Optional[str] = None, metrics_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Get latency metrics summary
    
    Args:
        prometheus_url: Prometheus server URL
        token: Optional authentication token
        metrics_file: Optional path to metrics-latency.yml
        
    Returns:
        Dictionary with latency metrics summary
    """
    collector = ovnBasicInfoCollector(prometheus_url, token)
    return await collector.collect_latency_metrics(metrics_file)


async def get_comprehensive_metrics_summary(prometheus_url: str, token: Optional[str] = None, metrics_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Get comprehensive metrics summary including all metric types
    
    Args:
        prometheus_url: Prometheus server URL
        token: Optional authentication token
        metrics_file: Optional path to metrics-latency.yml
        
    Returns:
        Complete JSON summary with all metrics
    """
    collector = ovnBasicInfoCollector(prometheus_url, token)
    return await collector.collect_comprehensive_summary(metrics_file)


# Example usage
async def main():
    """Example usage of the enhanced ovnBasicInfoCollector"""
    prometheus_url = "https://your-prometheus-server"
    token = "your-token-here"  # Optional
    
    # Collect comprehensive summary
    collector = ovnBasicInfoCollector(prometheus_url, token)
    
    # Get comprehensive summary including all metrics
    summary = await collector.collect_comprehensive_summary()
    print("Comprehensive Metrics Summary:")
    print(json.dumps(summary, indent=2))
    
    # Individual metric collection examples
    print("\n=== Individual Metric Examples ===")
    
    # Top alerts with statistics
    alerts = await collector.collect_top_alerts()
    print("\nTop Alerts with Statistics:")
    print(json.dumps(alerts, indent=2))
    
    # Pod distribution
    pod_dist = await collector.collect_pod_distribution()
    print("\nPod Distribution:")
    print(json.dumps(pod_dist, indent=2))
    
    # Latency metrics
    latency = await collector.collect_latency_metrics()
    print("\nLatency Metrics:")
    print(json.dumps(latency, indent=2))


if __name__ == "__main__":
    asyncio.run(main())