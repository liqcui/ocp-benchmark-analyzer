"""
etcd Disk Compact and Defrag Collector Module
Collects database compaction and defragmentation metrics
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pytz
import os
import sys

# Ensure project root on sys.path for utils imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from config.metrics_config_reader import Config
from ocauth.openshift_auth import OpenShiftAuth as OCPAuth


class CompactDefragCollector:
    """Collector for etcd disk compact and defrag metrics"""
    def __init__(self, ocp_auth: OCPAuth, duration: str = "1h", metrics_file_path: str = None):
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.logger = logging.getLogger(__name__)
        self.utility = mcpToolsUtility(ocp_auth)
        self.timezone = pytz.UTC
        self.etcd_namespace = "openshift-etcd"
        
        # Load metrics configuration
        if metrics_file_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metrics_file_path = os.path.join(project_root, 'config', 'metrics-etcd.yml')
        
        self.config = Config()
        load_result = self.config.load_metrics_file(metrics_file_path)
        
        # Get category-specific metrics
        self.category = "disk_compact_defrag"
        compact_defrag_metrics = self.config.get_metrics_by_category(self.category)
        
        # Log metrics loading
        if load_result.get('success'):
            metrics_count = len(compact_defrag_metrics)
            self.logger.info(f"✅ Loaded {metrics_count} metrics from {self.category} category (CompactDefragCollector)")
        else:
            self.logger.warning(f"⚠️  No {self.category} metrics found in configuration")

    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all compact and defrag metrics"""
        try:
            # Get prometheus configuration
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            # Get metrics configuration for disk_compact_defrag category
            metrics = self.config.get_metrics_by_category("disk_compact_defrag")
            
            if not metrics:
                return {
                    "status": "error",
                    "error": "No disk_compact_defrag metrics found in configuration",
                    "timestamp": datetime.now(self.timezone).isoformat()
                }
            
            # Get pod to node mapping and prepare node-exporter mapping
            pod_node_mapping = self.utility.get_pod_to_node_mapping_via_oc(namespace=self.etcd_namespace)
            node_exporter_mapping = self.utility.get_pod_to_node_mapping_via_oc(namespace='openshift-monitoring')

            # Collect each metric
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                master_nodes = await self._get_master_nodes(prom)
                self.logger.info(f"Master nodes found: {master_nodes}")
                self.logger.info(f"Node-exporter mapping: {list(node_exporter_mapping.keys())[:5]}...")  # Show first 5

                result = {
                    "status": "success",
                    "timestamp": datetime.now(self.timezone).isoformat(),
                    "duration": self.duration,
                    "category": "disk_compact_defrag",
                    "metrics": {},
                    "summaries": {},  # New: individual metric summaries
                    "summary": {
                        "total_metrics": len(metrics),
                        "collected_metrics": 0,
                        "failed_metrics": 0
                    },
                    "master_nodes": master_nodes
                }
                for metric_config in metrics:
                    metric_name = metric_config['name']
                    
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        metric_result = await self._collect_single_metric(
                            prom, metric_config, pod_node_mapping, node_exporter_mapping, master_nodes
                        )
                        result["metrics"][metric_name] = metric_result
                        
                        # Create formatted summary for each metric
                        result["summaries"][metric_name] = self._create_metric_summary(metric_result)
                        
                        if metric_result["status"] == "success":
                            result["summary"]["collected_metrics"] += 1
                        else:
                            result["summary"]["failed_metrics"] += 1
                    
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        result["metrics"][metric_name] = {
                            "status": "error",
                            "error": str(e),
                            "timestamp": datetime.now(self.timezone).isoformat()
                        }
                        result["summaries"][metric_name] = {
                            "status": "error",
                            "error": str(e),
                            "metric_name": metric_name
                        }
                        result["summary"]["failed_metrics"] += 1
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in collect_all_metrics: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }

    async def collect_metrics(self, duration: str = "1h") -> Dict[str, Any]:
        """Backward-compatible entrypoint expected by server; delegates to collect_all_metrics."""
        self.duration = duration
        result = await self.collect_all_metrics()
        # Adapt shape to what the server expects: top-level 'data'
        if result.get("status") == "success":
            return {
                "status": "success",
                "data": {
                    "pods_metrics": result.get("metrics", {}),
                    "metric_summaries": result.get("summaries", {}),  # New: include summaries
                    "summary": result.get("summary", {}),
                    "master_nodes": result.get("master_nodes", [])
                },
                "error": None,
                "timestamp": result.get("timestamp", datetime.now(self.timezone).isoformat()),
                "category": "disk_compact_defrag",
                "duration": self.duration
            }
        else:
            return {
                "status": result.get("status", "error"),
                "data": None,
                "error": result.get("error"),
                "timestamp": result.get("timestamp", datetime.now(self.timezone).isoformat()),
                "category": "disk_compact_defrag",
                "duration": self.duration
            }

    async def _collect_single_metric(self, prom: PrometheusBaseQuery, 
                                   metric_config: Dict[str, Any], 
                                   pod_node_mapping: Dict[str, str],
                                   node_exporter_mapping: Dict[str, str],
                                   master_nodes: List[str]) -> Dict[str, Any]:
        """Collect a single metric with statistics"""
        metric_name = metric_config['name']
        
        try:
            # Query metric data
            query_result = await self._query_with_stats(prom, metric_config['expr'], self.duration)
            
            if query_result['status'] != 'success':
                return {
                    "status": "error",
                    "error": query_result.get('error', 'Query failed'),
                    "query": metric_config['expr'],
                    "timestamp": datetime.now(self.timezone).isoformat()
                }
            
            # Process the results
            metric_result = {
                "status": "success",
                "name": metric_name,
                "unit": metric_config.get('unit', ''),
                "description": metric_config.get('description', ''),
                "query": metric_config['expr'],
                "timestamp": datetime.now(self.timezone).isoformat(),
                "data": {}
            }
            
            # Process per-pod/instance data
            series_data = query_result.get('series_data', [])
            pods_data = {}
            filtered_data = {}  # For vmstat metrics filtered by master nodes
            
            # Check if this is a vmstat metric that should be filtered by master nodes
            is_vmstat_metric = 'vmstat' in metric_name
            
            for series in series_data:
                labels = series.get('labels', {})
                stats = series.get('statistics', {})
                
                if is_vmstat_metric:
                    # For vmstat metrics, use instance label and filter by master nodes
                    instance = labels.get('instance', '')
                    
                    # Extract node name from instance (format could be IP:port or hostname:port)
                    node_name = self._extract_node_from_instance(instance, master_nodes)
                    
                    # Only include if it's a master node
                    if node_name in master_nodes:
                        # Use instance as key for vmstat metrics
                        filtered_data[instance] = {
                            "node": node_name,
                            "avg": stats.get('avg'),
                            "max": stats.get('max'),
                            "min": stats.get('min'),
                            "latest": stats.get('latest'),
                            "count": stats.get('count', 0)
                        }
                        self.logger.debug(f"Included vmstat metric for master node {node_name} (instance: {instance})")
                    else:
                        self.logger.debug(f"Filtered out non-master node for vmstat metric: {node_name} (instance: {instance})")
                else:
                    # For non-vmstat metrics, use existing logic
                    pod_name = labels.get('pod', labels.get('instance', 'unknown'))
                    
                    # Get node name for this pod
                    node_name = 'unknown'
                    if pod_name in pod_node_mapping:
                        node_name = pod_node_mapping[pod_name]
                    elif pod_name in node_exporter_mapping:
                        node_name = node_exporter_mapping[pod_name]
                    elif 'node-exporter' in pod_name:
                        # Try to find the node name in node_exporter_mapping
                        for ne_pod, ne_node in node_exporter_mapping.items():
                            if ne_pod == pod_name:
                                node_name = ne_node
                                break
                    
                    pods_data[pod_name] = {
                        "node": node_name,
                        "avg": stats.get('avg'),
                        "max": stats.get('max'),
                        "min": stats.get('min'),
                        "latest": stats.get('latest'),
                        "count": stats.get('count', 0)
                    }
            
            # For vmstat metrics, use filtered data; for others, use regular pod data
            if is_vmstat_metric:
                metric_result["data"]["instances"] = filtered_data
                metric_result["data"]["total_instances"] = len(filtered_data)
                
                # Calculate overall statistics only for master nodes
                if filtered_data:
                    all_values = []
                    latest_values = []
                    for instance_data in filtered_data.values():
                        if instance_data.get('avg') is not None:
                            all_values.append(instance_data['avg'])
                        if instance_data.get('latest') is not None:
                            latest_values.append(instance_data['latest'])
                    
                    if all_values:
                        metric_result["overall"] = {
                            "avg": sum(all_values) / len(all_values),
                            "max": max(all_values),
                            "min": min(all_values),
                            "latest": latest_values[-1] if latest_values else None,
                            "count": len(all_values)
                        }
                    else:
                        metric_result["overall"] = {
                            "avg": None,
                            "max": None,
                            "min": None,
                            "latest": None,
                            "count": 0
                        }
                else:
                    metric_result["overall"] = {
                        "avg": None,
                        "max": None,
                        "min": None,
                        "latest": None,
                        "count": 0
                    }
            else:
                # Regular metrics (non-vmstat)
                metric_result["data"]["pods"] = pods_data
                metric_result["data"]["total_pods"] = len(pods_data)
                
                # Overall statistics (use existing from query_result)
                overall_stats = query_result.get('overall_statistics', {})
                metric_result["overall"] = {
                    "avg": overall_stats.get('avg'),
                    "max": overall_stats.get('max'),
                    "min": overall_stats.get('min'),
                    "latest": overall_stats.get('latest'),
                    "count": overall_stats.get('count', 0)
                }
            
            return metric_result
            
        except Exception as e:
            self.logger.error(f"Error collecting metric {metric_name}: {e}")
            return {
                "status": "error",
                "error": str(e),
                "query": metric_config['expr'],
                "timestamp": datetime.now(self.timezone).isoformat()
            }
       
    def _extract_node_from_instance(self, instance: str, master_nodes: List[str]) -> str:
        """Extract node name from instance string and match with master nodes"""
        if not instance:
            return 'unknown'
        
        # Remove port if present (format: hostname:port or ip:port)
        node_candidate = instance.split(':')[0]
        
        # Direct match with master nodes
        if node_candidate in master_nodes:
            return node_candidate
        
        # Try to match by IP address resolution or partial matching
        for master_node in master_nodes:
            # Check if the instance contains the master node name
            if master_node.lower() in instance.lower() or instance.lower() in master_node.lower():
                return master_node
        
        # If no match found, return the extracted candidate
        self.logger.debug(f"Could not match instance '{instance}' to any master node, using '{node_candidate}'")
        return node_candidate

    async def collect_compaction_duration(self) -> Dict[str, Any]:
        """Collect compaction duration metrics"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                # Get metrics for compaction duration
                compaction_metrics = [
                    'debugging_mvcc_db_compaction_duration_sum_delta',
                    'debugging_mvcc_db_compaction_duration_sum'
                ]
                
                result = {
                    "status": "success",
                    "timestamp": datetime.now(self.timezone).isoformat(),
                    "metrics": {}
                }
                
                pod_node_mapping = self.utility.get_pod_to_node_mapping_via_oc(namespace=self.etcd_namespace)
                node_exporter_mapping = self.utility.get_pod_to_node_mapping_via_oc(namespace='openshift-monitoring')
                master_nodes = await self._get_master_nodes(prom)
                
                for metric_name in compaction_metrics:
                    metric_config = self.config.get_metric_by_name(metric_name)
                    if metric_config:
                        result["metrics"][metric_name] = await self._collect_single_metric(
                            prom, metric_config, pod_node_mapping, node_exporter_mapping, master_nodes
                        )
                
                return result
                
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }
    
    async def collect_defrag_duration(self) -> Dict[str, Any]:
        """Collect defragmentation duration metrics"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                # Get metrics for defrag duration
                defrag_metrics = [
                    'defrag_inflight_duration_sum', 
                    'disk_backend_defrag_duration_sum_rate',
                    'disk_backend_defrag_duration_sum'
                ]
                
                result = {
                    "status": "success",
                    "timestamp": datetime.now(self.timezone).isoformat(),
                    "metrics": {}
                }
                
                pod_node_mapping = self.utility.get_pod_to_node_mapping_via_oc(namespace=self.etcd_namespace)
                node_exporter_mapping = self.utility.get_pod_to_node_mapping_via_oc(namespace='openshift-monitoring')
                master_nodes = await self._get_master_nodes(prom)
                
                for metric_name in defrag_metrics:
                    metric_config = self.config.get_metric_by_name(metric_name)
                    if metric_config:
                        result["metrics"][metric_name] = await self._collect_single_metric(
                            prom, metric_config, pod_node_mapping, node_exporter_mapping, master_nodes
                        )
                
                return result
                
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }
    
    async def collect_vmstat_pgfault(self) -> Dict[str, Any]:
        """Collect VMStat page fault metrics (filtered by master nodes)"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                # Get metrics for VMStat page faults
                vmstat_metrics = [
                    'vmstat_pgmajfault_rate',
                    'vmstat_pgmajfault_total'
                ]
                
                result = {
                    "status": "success",
                    "timestamp": datetime.now(self.timezone).isoformat(),
                    "metrics": {}
                }
                
                pod_node_mapping = self.utility.get_pod_to_node_mapping_via_oc(namespace=self.etcd_namespace)
                node_exporter_mapping = self.utility.get_pod_to_node_mapping_via_oc(namespace='openshift-monitoring')
                master_nodes = await self._get_master_nodes(prom)
                
                self.logger.info(f"Collecting vmstat metrics filtered by master nodes: {master_nodes}")
                
                for metric_name in vmstat_metrics:
                    metric_config = self.config.get_metric_by_name(metric_name)
                    if metric_config:
                        result["metrics"][metric_name] = await self._collect_single_metric(
                            prom, metric_config, pod_node_mapping, node_exporter_mapping, master_nodes
                        )
                
                return result
                
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }
    
    async def collect_compacted_keys(self) -> Dict[str, Any]:
        """Collect compacted keys metrics"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                metric_name = 'debugging_mvcc_db_compacted_keys'
                metric_config = self.config.get_metric_by_name(metric_name)
                
                if not metric_config:
                    return {
                        "status": "error",
                        "error": f"Metric {metric_name} not found in configuration",
                        "timestamp": datetime.now(self.timezone).isoformat()
                    }
                
                pod_node_mapping = self.utility.get_pod_to_node_mapping_via_oc(namespace=self.etcd_namespace)
                node_exporter_mapping = self.utility.get_pod_to_node_mapping_via_oc(namespace='openshift-monitoring')
                master_nodes = await self._get_master_nodes(prom)
                
                result = await self._collect_single_metric(
                    prom, metric_config, pod_node_mapping, node_exporter_mapping, master_nodes
                )
                
                return {
                    "status": "success",
                    "timestamp": datetime.now(self.timezone).isoformat(),
                    "metric": result
                }
                
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }
    
    async def get_summary(self) -> Dict[str, Any]:
        """Get enhanced summary of all compact/defrag metrics"""
        try:
            all_metrics = await self.collect_all_metrics()
            
            if all_metrics["status"] != "success":
                return all_metrics
            
            # Create enhanced summary with readable values
            summary = {
                "status": "success",
                "timestamp": datetime.now(self.timezone).isoformat(),
                "category": "disk_compact_defrag",
                "duration": self.duration,
                "master_nodes": all_metrics.get("master_nodes", []),
                "overview": {
                    "compaction": {},
                    "defragmentation": {},
                    "page_faults": {},
                    "snapshots": {}
                },
                "metric_summaries": all_metrics.get("summaries", {}),  # Include individual summaries
                "health_assessment": {}
            }
            
            metrics_data = all_metrics.get("metrics", {})
            summaries = all_metrics.get("summaries", {})
            
            # Enhanced compaction summary
            compaction_metrics = [k for k in metrics_data.keys() if 'compaction' in k.lower()]
            if compaction_metrics:
                compaction_durations = []
                compaction_keys = []
                
                for metric_name in compaction_metrics:
                    summary_data = summaries.get(metric_name, {})
                    if summary_data.get("status") == "success":
                        if "duration" in metric_name.lower():
                            avg_raw = summary_data.get("statistics", {}).get("avg", {}).get("raw")
                            if avg_raw is not None:
                                compaction_durations.append(avg_raw)
                        elif "keys" in metric_name.lower():
                            avg_raw = summary_data.get("statistics", {}).get("avg", {}).get("raw")
                            if avg_raw is not None:
                                compaction_keys.append(avg_raw)
                
                summary["overview"]["compaction"] = {
                    "total_metrics": len(compaction_metrics),
                    "avg_duration": {
                        "raw": sum(compaction_durations) / len(compaction_durations) if compaction_durations else None,
                        "formatted": self._format_value_with_unit(
                            sum(compaction_durations) / len(compaction_durations) if compaction_durations else None,
                            "milliseconds"
                        )
                    },
                    "avg_keys_compacted": {
                        "raw": sum(compaction_keys) / len(compaction_keys) if compaction_keys else None,
                        "formatted": self._format_value_with_unit(
                            sum(compaction_keys) / len(compaction_keys) if compaction_keys else None,
                            "count"
                        )
                    }
                }
            
            # Enhanced defragmentation summary
            defrag_metrics = [k for k in metrics_data.keys() if 'defrag' in k.lower()]
            if defrag_metrics:
                defrag_durations = []
                
                for metric_name in defrag_metrics:
                    summary_data = summaries.get(metric_name, {})
                    if summary_data.get("status") == "success":
                        avg_raw = summary_data.get("statistics", {}).get("avg", {}).get("raw")
                        if avg_raw is not None:
                            defrag_durations.append(avg_raw)
                
                summary["overview"]["defragmentation"] = {
                    "total_metrics": len(defrag_metrics),
                    "avg_duration": {
                        "raw": sum(defrag_durations) / len(defrag_durations) if defrag_durations else None,
                        "formatted": self._format_value_with_unit(
                            sum(defrag_durations) / len(defrag_durations) if defrag_durations else None,
                            "seconds"
                        )
                    }
                }
            
            # Enhanced page faults summary
            pgfault_metrics = [k for k in metrics_data.keys() if 'pgmajfault' in k.lower()]
            if pgfault_metrics:
                fault_rates = []
                fault_totals = []
                
                for metric_name in pgfault_metrics:
                    summary_data = summaries.get(metric_name, {})
                    if summary_data.get("status") == "success":
                        avg_raw = summary_data.get("statistics", {}).get("avg", {}).get("raw")
                        if avg_raw is not None:
                            if "rate" in metric_name.lower():
                                fault_rates.append(avg_raw)
                            else:
                                fault_totals.append(avg_raw)
                
                summary["overview"]["page_faults"] = {
                    "total_metrics": len(pgfault_metrics),
                    "avg_fault_rate": {
                        "raw": sum(fault_rates) / len(fault_rates) if fault_rates else None,
                        "formatted": self._format_value_with_unit(
                            sum(fault_rates) / len(fault_rates) if fault_rates else None,
                            "faults/s"
                        )
                    },
                    "avg_total_faults": {
                        "raw": sum(fault_totals) / len(fault_totals) if fault_totals else None,
                        "formatted": self._format_value_with_unit(
                            sum(fault_totals) / len(fault_totals) if fault_totals else None,
                            "count"
                        )
                    },
                    "filtered_by_master_nodes": True
                }
            
            # Snapshot metrics summary
            snapshot_metrics = [k for k in metrics_data.keys() if 'snapshot' in k.lower()]
            if snapshot_metrics:
                snapshot_durations = []
                
                for metric_name in snapshot_metrics:
                    summary_data = summaries.get(metric_name, {})
                    if summary_data.get("status") == "success":
                        avg_raw = summary_data.get("statistics", {}).get("avg", {}).get("raw")
                        if avg_raw is not None:
                            snapshot_durations.append(avg_raw)
                
                summary["overview"]["snapshots"] = {
                    "total_metrics": len(snapshot_metrics),
                    "avg_duration": {
                        "raw": sum(snapshot_durations) / len(snapshot_durations) if snapshot_durations else None,
                        "formatted": self._format_value_with_unit(
                            sum(snapshot_durations) / len(snapshot_durations) if snapshot_durations else None,
                            "seconds"
                        )
                    }
                }
            
            # Health assessment
            summary["health_assessment"] = self._assess_overall_health(summaries)
            
            return summary
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(self.timezone).isoformat()
            }

    def _format_value_with_unit(self, value: float, unit: str) -> str:
        """Format metric value with appropriate unit and readability"""
        if value is None:
            return "N/A"
        
        # Handle different unit types
        if unit.lower() in ['milliseconds', 'ms']:
            if value >= 1000:
                return f"{value/1000:.2f} seconds"
            else:
                return f"{value:.2f} ms"
        elif unit.lower() in ['seconds', 's']:
            if value >= 60:
                minutes = value / 60
                if minutes >= 60:
                    hours = minutes / 60
                    return f"{hours:.2f} hours"
                else:
                    return f"{minutes:.2f} minutes"
            else:
                return f"{value:.3f} seconds"
        elif unit.lower() in ['bytes']:
            if value >= 1024**3:
                return f"{value/(1024**3):.2f} GB"
            elif value >= 1024**2:
                return f"{value/(1024**2):.2f} MB"
            elif value >= 1024:
                return f"{value/1024:.2f} KB"
            else:
                return f"{value:.0f} bytes"
        elif unit.lower() in ['count', 'faults']:
            return f"{int(value):,}"
        elif unit.lower() in ['per_second', 'faults/s']:
            return f"{value:.3f}/sec"
        elif unit.lower() in ['per_day']:
            return f"{value:.1f}/day"
        elif unit.lower() in ['percent', '%']:
            return f"{value:.2f}%"
        else:
            # Default formatting for unknown units
            if value >= 1000000:
                return f"{value/1000000:.2f}M {unit}"
            elif value >= 1000:
                return f"{value/1000:.2f}K {unit}"
            else:
                return f"{value:.3f} {unit}"

    def _create_metric_summary(self, metric_result: Dict[str, Any]) -> Dict[str, Any]:
        """Create a formatted summary for a metric"""
        if metric_result.get("status") != "success":
            return {
                "status": "error",
                "error": metric_result.get("error", "Unknown error")
            }
        
        overall_stats = metric_result.get("overall", {})
        unit = metric_result.get("unit", "")
        
        summary = {
            "metric_name": metric_result.get("name", ""),
            "description": metric_result.get("description", ""),
            "unit": unit,
            "status": "success",
            "statistics": {
                "avg": {
                    "raw": overall_stats.get("avg"),
                    "formatted": self._format_value_with_unit(overall_stats.get("avg"), unit)
                },
                "max": {
                    "raw": overall_stats.get("max"),
                    "formatted": self._format_value_with_unit(overall_stats.get("max"), unit)
                },
                "min": {
                    "raw": overall_stats.get("min"),
                    "formatted": self._format_value_with_unit(overall_stats.get("min"), unit)
                },
                "latest": {
                    "raw": overall_stats.get("latest"),
                    "formatted": self._format_value_with_unit(overall_stats.get("latest"), unit)
                },
                "count": overall_stats.get("count", 0)
            },
            "data_points": overall_stats.get("count", 0),
            "timestamp": metric_result.get("timestamp")
        }
        
        # Add interpretation based on metric type
        summary["interpretation"] = self._interpret_metric_values(
            metric_result.get("name", ""), overall_stats, unit
        )
        
        return summary

    def _interpret_metric_values(self, metric_name: str, stats: Dict[str, Any], unit: str) -> Dict[str, str]:
        """Provide interpretation of metric values"""
        interpretation = {}
        
        avg_val = stats.get("avg")
        max_val = stats.get("max")
        
        if "compaction" in metric_name.lower():
            if "duration" in metric_name.lower():
                if avg_val is not None:
                    if unit.lower() in ['milliseconds', 'ms']:
                        if avg_val > 1000:  # > 1 second
                            interpretation["avg"] = "High - compaction taking significant time"
                        elif avg_val > 500:
                            interpretation["avg"] = "Moderate - compaction duration acceptable"
                        else:
                            interpretation["avg"] = "Good - fast compaction"
                    elif unit.lower() in ['seconds', 's']:
                        if avg_val > 10:
                            interpretation["avg"] = "High - compaction taking too long"
                        elif avg_val > 2:
                            interpretation["avg"] = "Moderate - compaction duration acceptable"
                        else:
                            interpretation["avg"] = "Good - fast compaction"
            elif "keys" in metric_name.lower():
                if avg_val is not None:
                    if avg_val > 1000000:
                        interpretation["avg"] = "High - many keys being compacted"
                    elif avg_val > 100000:
                        interpretation["avg"] = "Moderate - normal compaction activity"
                    else:
                        interpretation["avg"] = "Low - minimal compaction activity"
        
        elif "defrag" in metric_name.lower():
            if "duration" in metric_name.lower():
                if avg_val is not None:
                    if unit.lower() in ['seconds', 's']:
                        if avg_val > 30:
                            interpretation["avg"] = "High - defragmentation taking long time"
                        elif avg_val > 10:
                            interpretation["avg"] = "Moderate - defragmentation duration acceptable"
                        else:
                            interpretation["avg"] = "Good - fast defragmentation"
        
        elif "pgmajfault" in metric_name.lower():
            if avg_val is not None:
                if "rate" in metric_name.lower():
                    if avg_val > 10:
                        interpretation["avg"] = "High - frequent major page faults"
                    elif avg_val > 1:
                        interpretation["avg"] = "Moderate - some page fault activity"
                    else:
                        interpretation["avg"] = "Low - minimal page fault activity"
                else:  # total
                    interpretation["avg"] = f"Total page faults: {int(avg_val):,}"
        
        elif "snapshot" in metric_name.lower():
            if avg_val is not None and unit.lower() in ['seconds', 's']:
                if avg_val > 5:
                    interpretation["avg"] = "High - snapshot duration concerning"
                elif avg_val > 1:
                    interpretation["avg"] = "Moderate - snapshot duration acceptable"
                else:
                    interpretation["avg"] = "Good - fast snapshot creation"
        
        return interpretation

    def _assess_overall_health(self, summaries: Dict[str, Any]) -> Dict[str, str]:
        """Assess overall health based on metric summaries"""
        assessment = {
            "compaction_health": "unknown",
            "defragmentation_health": "unknown", 
            "page_fault_health": "unknown",
            "overall_health": "unknown"
        }
        
        # Assess compaction health
        compaction_issues = 0
        compaction_total = 0
        
        for metric_name, summary in summaries.items():
            if "compaction" in metric_name.lower() and summary.get("status") == "success":
                compaction_total += 1
                interpretation = summary.get("interpretation", {})
                if "High" in interpretation.get("avg", ""):
                    compaction_issues += 1
        
        if compaction_total > 0:
            if compaction_issues == 0:
                assessment["compaction_health"] = "good"
            elif compaction_issues / compaction_total < 0.5:
                assessment["compaction_health"] = "moderate"
            else:
                assessment["compaction_health"] = "concerning"
        
        # Assess defragmentation health
        defrag_issues = 0
        defrag_total = 0
        
        for metric_name, summary in summaries.items():
            if "defrag" in metric_name.lower() and summary.get("status") == "success":
                defrag_total += 1
                interpretation = summary.get("interpretation", {})
                if "High" in interpretation.get("avg", ""):
                    defrag_issues += 1
        
        if defrag_total > 0:
            if defrag_issues == 0:
                assessment["defragmentation_health"] = "good"
            elif defrag_issues / defrag_total < 0.5:
                assessment["defragmentation_health"] = "moderate"
            else:
                assessment["defragmentation_health"] = "concerning"
        
        # Assess page fault health
        pgfault_issues = 0
        pgfault_total = 0
        
        for metric_name, summary in summaries.items():
            if "pgmajfault" in metric_name.lower() and summary.get("status") == "success":
                pgfault_total += 1
                interpretation = summary.get("interpretation", {})
                if "High" in interpretation.get("avg", ""):
                    pgfault_issues += 1
        
        if pgfault_total > 0:
            if pgfault_issues == 0:
                assessment["page_fault_health"] = "good"
            elif pgfault_issues / pgfault_total < 0.5:
                assessment["page_fault_health"] = "moderate"
            else:
                assessment["page_fault_health"] = "concerning"
        
        # Overall health assessment
        health_scores = [
            assessment["compaction_health"],
            assessment["defragmentation_health"], 
            assessment["page_fault_health"]
        ]
        
        if all(h == "good" for h in health_scores if h != "unknown"):
            assessment["overall_health"] = "good"
        elif any(h == "concerning" for h in health_scores):
            assessment["overall_health"] = "concerning"
        elif any(h == "moderate" for h in health_scores):
            assessment["overall_health"] = "moderate"
        else:
            assessment["overall_health"] = "unknown"
        
        return assessment

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

    async def _get_master_nodes(self, prom_client: PrometheusBaseQuery) -> List[str]:
        """Return a list of controlplane node names using utility grouping."""
        groups = await self.utility.get_node_groups(prometheus_client=prom_client)
        controlplane = groups.get('controlplane', []) if isinstance(groups, dict) else []
        return [n.get('name', '').split(':')[0] for n in controlplane]