"""
etcd Performance Deep Drive Analyzer
Comprehensive performance analysis module for etcd clusters
"""

import asyncio
import logging
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import pytz

# Import existing modules
from tools.etcd.etcd_general_info import GeneralInfoCollector
from tools.etcd.etcd_disk_wal_fsync import DiskWALFsyncCollector
from tools.disk.disk_io import DiskIOCollector
from tools.net.network_io import NetworkIOCollector
from tools.etcd.etcd_disk_backend_commit import DiskBackendCommitCollector
from tools.etcd.etcd_disk_compact_defrag import CompactDefragCollector
from analysis.utils.analysis_utility import etcdAnalyzerUtility
from tools.node.node_usage import nodeUsageCollector
from config.metrics_config_reader import Config

class etcdDeepDriveAnalyzer:
    """Deep drive analyzer for etcd cluster performance"""
    def __init__(self, ocp_auth, duration: str = "1h"):
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.logger = logging.getLogger(__name__)
        self.timezone = pytz.UTC
        self.utility = etcdAnalyzerUtility()
        
        # Initialize collectors
        self.general_collector = GeneralInfoCollector(ocp_auth)
        self.wal_fsync_collector = DiskWALFsyncCollector(ocp_auth, duration)
        self.disk_io_collector = DiskIOCollector(ocp_auth, duration)
        
        # Initialize NetworkIOCollector with proper parameters
        # Extract prometheus_url and token from ocp_auth
        prometheus_url = getattr(ocp_auth, 'prometheus_url', None)
        if not prometheus_url:
            prometheus_url = getattr(ocp_auth, 'prom_url', None)
        prometheus_token = getattr(ocp_auth, 'prometheus_token', None)
        if not prometheus_token:
            prometheus_token = getattr(ocp_auth, 'token', None) or getattr(ocp_auth, 'bearer_token', None)
        
        # Load network metrics config
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        metrics_net_file = os.path.join(project_root, 'config', 'metrics-net.yml')
        network_config = Config()
        load_result = network_config.load_metrics_file(metrics_net_file)
        if not load_result.get('success'):
            self.logger.warning(f"Failed to load network metrics file: {load_result.get('error', 'Unknown error')}")
        
        self.network_collector = NetworkIOCollector(
            prometheus_url=prometheus_url,
            token=prometheus_token,
            config=network_config
        )
        
        self.backend_commit_collector = DiskBackendCommitCollector(ocp_auth)
        self.compact_defrag_collector = CompactDefragCollector(ocp_auth, duration)
        
        # Initialize node usage collector
        # Extract prometheus config from ocp_auth
        prometheus_config = self._get_prometheus_config_from_auth(ocp_auth)
        self.node_usage_collector = nodeUsageCollector(ocp_auth, prometheus_config)
        
        self.test_id = self.utility.generate_test_id()

    async def analyze_performance_deep_drive(self) -> Dict[str, Any]:
        """Comprehensive performance analysis of etcd cluster"""
        try:
            self.logger.info(f"Starting etcd performance deep drive analysis with test ID: {self.test_id}")
            
            analysis_result = {
                "test_id": self.test_id,
                "timestamp": self.utility.format_timestamp(),
                "duration": self.duration,
                "timezone": "UTC",
                "status": "success",
                "data": {
                    "general_info_data": [],
                    "wal_fsync_data": [],
                    "disk_io_data": [],
                    "network_data": {},
                    "backend_commit_data": [],
                    "compact_defrag_data": [],
                    "node_usage_data": {}
                },
                "analysis": {},
                "summary": {}
            }
            
            analysis_result["data"]["general_info_data"] = await self._collect_general_info_metrics()
            analysis_result["data"]["wal_fsync_data"] = await self._collect_wal_fsync_metrics()
            analysis_result["data"]["disk_io_data"] = await self._collect_disk_io_metrics()
            analysis_result["data"]["network_data"] = await self._collect_network_io_metrics()
            analysis_result["data"]["backend_commit_data"] = await self._collect_backend_commit_metrics()
            analysis_result["data"]["compact_defrag_data"] = await self._collect_compact_defrag_metrics()
            analysis_result["data"]["node_usage_data"] = await self._collect_node_usage_metrics()
            
            analysis_result["analysis"] = self.utility.analyze_latency_patterns(analysis_result["data"])
            if analysis_result["data"]["node_usage_data"].get("status") == "success":
                node_resource_analysis = self.utility.analyze_node_resource_utilization(
                    analysis_result["data"]["node_usage_data"]
                )
                analysis_result["analysis"]["node_resource_utilization"] = node_resource_analysis
            
            summary_input = {**analysis_result["data"], "latency_analysis": analysis_result["analysis"]}
            analysis_result["summary"] = self.utility.create_performance_summary(summary_input)
            
            self.logger.info(f"Successfully completed etcd performance deep drive analysis")
            return analysis_result
            
        except Exception as e:
            self.logger.error(f"Error in performance deep drive analysis: {e}")
            return {
                "test_id": self.test_id,
                "timestamp": self.utility.format_timestamp(),
                "status": "error",
                "error": str(e)
            }
    
    async def _collect_general_info_metrics(self) -> List[Dict[str, Any]]:
        target_metrics = [
            'etcd_pods_cpu_usage',
            'etcd_pods_memory_usage',
            'proposal_failure_rate',
            'proposal_pending_total',
            'proposal_commit_rate',
            'proposal_apply_rate',
            'total_proposals_committed',
            'leader_changes_rate',
            'etcd_slow_applies',
            'etcd_slow_read_indexes',
            'etcd_mvcc_put_operations_rate',
            'etcd_mvcc_delete_operations_rate',
            'etcd_network_heartbeat_send_failures',
            'etcd_server_health_failures',
            'vmstat_pgmajfault_total',
            'vmstat_pgmajfault_rate'
        ]
        general_metrics = []
        try:
            for metric_name in target_metrics:
                try:
                    result = await self.general_collector._get_single_metric_per_pod(metric_name, self.duration)
                    if result.get('status') == 'success':
                        pods_data = result.get('pods', {})
                        unit = result.get('unit', 'unknown')
                        for pod_name, pod_stats in pods_data.items():
                            general_metrics.append({
                                "metric_name": metric_name,
                                "pod_name": pod_name,
                                "avg": pod_stats.get('avg'),
                                "max": pod_stats.get('max'),
                                "unit": unit
                            })
                except Exception as e:
                    self.logger.error(f"Error collecting general metric {metric_name}: {e}")
                    continue
        except Exception as e:
            self.logger.error(f"Error in _collect_general_info_metrics: {e}")
        return general_metrics
    
    async def _collect_wal_fsync_metrics(self) -> List[Dict[str, Any]]:
        target_metrics = [
            'disk_wal_fsync_seconds_duration_p99',
            'disk_wal_fsync_duration_seconds_sum_rate',
            'disk_wal_fsync_duration_sum',
            'disk_wal_fsync_duration_seconds_count_rate',
            'disk_wal_fsync_duration_seconds_count'
        ]
        wal_metrics = []
        try:
            for metric_name in target_metrics:
                try:
                    result = await self.wal_fsync_collector.get_metric_by_name(metric_name)
                    if result.get('status') == 'success':
                        pod_metrics_data = result.get('pod_metrics', {})
                        unit = result.get('unit', 'unknown')
                        for pod_name, pod_stats in pod_metrics_data.items():
                            avg_value = None
                            max_value = None
                            if 'p99' in metric_name or 'percentile' in metric_name.lower():
                                avg_value = pod_stats.get('avg_seconds')
                                max_value = pod_stats.get('max_seconds')
                            elif 'rate' in metric_name and 'duration' in metric_name:
                                avg_value = pod_stats.get('avg_rate_seconds')
                                max_value = pod_stats.get('max_rate_seconds')
                            elif 'sum' in metric_name and 'rate' not in metric_name:
                                avg_value = pod_stats.get('avg_sum_seconds')
                                max_value = pod_stats.get('max_sum_seconds')
                            elif 'count' in metric_name and 'rate' in metric_name:
                                avg_value = pod_stats.get('avg_ops_per_sec')
                                max_value = pod_stats.get('max_ops_per_sec')
                            elif 'count' in metric_name:
                                avg_value = pod_stats.get('avg_count')
                                max_value = pod_stats.get('max_count')
                            else:
                                avg_value = pod_stats.get('avg_value')
                                max_value = pod_stats.get('max_value')
                            wal_metrics.append({
                                "metric_name": metric_name,
                                "pod_name": pod_name,
                                "avg": avg_value,
                                "max": max_value,
                                "unit": unit
                            })
                except Exception as e:
                    self.logger.error(f"Error collecting WAL metric {metric_name}: {e}")
                    continue
        except Exception as e:
            self.logger.error(f"Error in _collect_wal_fsync_metrics: {e}")
        return wal_metrics
    
    async def _collect_disk_io_metrics(self) -> List[Dict[str, Any]]:
        disk_metrics = []
        try:
            all_metrics_result = await self.disk_io_collector.collect_all_metrics()
            if all_metrics_result.get('status') == 'success':
                metrics_data = all_metrics_result.get('metrics', {})
                for metric_name, metric_data in metrics_data.items():
                    if metric_data.get('status') == 'success':
                        nodes_data = metric_data.get('nodes', {})
                        unit = metric_data.get('unit', 'unknown')
                        for node_name, node_stats in nodes_data.items():
                            disk_metrics.append({
                                "metric_name": metric_name,
                                "node_name": node_name,
                                "avg": node_stats.get('avg'),
                                "max": node_stats.get('max'),
                                "unit": unit,
                                "devices": node_stats.get('devices', [])
                            })
        except Exception as e:
            self.logger.error(f"Error in _collect_disk_io_metrics: {e}")
        return disk_metrics
    
    async def _collect_network_io_metrics(self) -> Dict[str, Any]:
        network_data = {"pod_metrics": [], "node_metrics": [], "cluster_metrics": []}
        try:
            # Ensure network collector is initialized
            await self.network_collector.initialize()
            result = await self.network_collector.collect_all_metrics(self.duration)
            
            # collect_all_metrics returns: {category, duration, timestamp, metrics: {metric_name: metric_result}, summary}
            # metric_result structure: {metric, unit, controlplane: {nodes: []}, worker: {top3: []}, infra: {nodes: []}, workload: {nodes: []}}
            
            metrics = result.get('metrics', {})
            
            # Process node-level metrics
            node_target_metrics = [
                'network_io_node_network_rx_utilization',
                'network_io_node_network_tx_utilization',
                'network_io_node_network_rx_package',
                'network_io_node_network_tx_package',
                'network_io_node_network_rx_drop',
                'network_io_node_network_tx_drop'
            ]
            
            for metric_name in node_target_metrics:
                # The metric_name in metrics dict is the method name, e.g., 'network_io_node_network_rx_utilization'
                metric_result = metrics.get(metric_name)
                if not metric_result or isinstance(metric_result, dict) and 'error' in metric_result:
                    continue
                
                if isinstance(metric_result, dict):
                    unit = metric_result.get('unit', 'unknown')
                    
                    # Extract nodes from controlplane, infra, and workload
                    for role in ['controlplane', 'infra', 'workload']:
                        role_data = metric_result.get(role, {})
                        nodes = role_data.get('nodes', [])
                        for node_info in nodes:
                            node_name = node_info.get('node')
                            network_data["node_metrics"].append({
                                "metric_name": metric_name,
                                "node_name": node_name,
                                "avg": node_info.get('avg'),
                                "max": node_info.get('max'),
                                "unit": unit
                            })
                    
                    # Extract top 3 worker nodes
                    worker_data = metric_result.get('worker', {})
                    top3_workers = worker_data.get('top3', [])
                    for node_info in top3_workers:
                        node_name = node_info.get('node')
                        network_data["node_metrics"].append({
                            "metric_name": metric_name,
                            "node_name": node_name,
                            "avg": node_info.get('avg'),
                            "max": node_info.get('max'),
                            "unit": unit
                        })
            
            # Process cluster-level metrics
            cluster_target_metrics = [
                'network_io_grpc_active_watch_streams'
            ]
            
            for metric_name in cluster_target_metrics:
                metric_result = metrics.get(metric_name)
                if not metric_result or isinstance(metric_result, dict) and 'error' in metric_result:
                    continue
                
                if isinstance(metric_result, dict):
                    unit = metric_result.get('unit', 'unknown')
                    # For cluster metrics, aggregate across all nodes
                    all_values = []
                    for role in ['controlplane', 'infra', 'workload']:
                        role_data = metric_result.get(role, {})
                        nodes = role_data.get('nodes', [])
                        for node_info in nodes:
                            if node_info.get('avg') is not None:
                                all_values.append(node_info.get('avg'))
                            if node_info.get('max') is not None:
                                all_values.append(node_info.get('max'))
                    
                    worker_data = metric_result.get('worker', {})
                    top3_workers = worker_data.get('top3', [])
                    for node_info in top3_workers:
                        if node_info.get('avg') is not None:
                            all_values.append(node_info.get('avg'))
                        if node_info.get('max') is not None:
                            all_values.append(node_info.get('max'))
                    
                    if all_values:
                        network_data["cluster_metrics"].append({
                            "metric_name": metric_name,
                            "test_id": self.test_id,
                            "avg": sum(all_values) / len(all_values) if all_values else None,
                            "max": max(all_values) if all_values else None,
                            "unit": unit
                        })
            
            # Note: Pod-level metrics (container_network_rx, peer2peer_latency, etc.) are not available
            # in the current NetworkIOCollector implementation which focuses on node-level metrics
            # These are left as empty arrays for now
            
        except Exception as e:
            self.logger.error(f"Error in _collect_network_io_metrics: {e}")
        finally:
            # Ensure network collector session is closed
            try:
                await self.network_collector.close()
            except Exception as close_err:
                self.logger.debug(f"Error closing network collector: {close_err}")
        return network_data
    
    async def _collect_backend_commit_metrics(self) -> List[Dict[str, Any]]:
        backend_metrics = []
        try:
            collected = await self.backend_commit_collector.collect_metrics(self.duration)
            if collected.get('status') != 'success':
                return backend_metrics
            metrics_map = collected.get('data', {}).get('pods_metrics', {})
            for metric_name, metric_result in metrics_map.items():
                if metric_result.get('status') != 'success':
                    continue
                unit = metric_result.get('unit', 'unknown')
                for pod_name, pod_stats in metric_result.get('pods', {}).items():
                    backend_metrics.append({
                        "metric_name": metric_name,
                        "pod_name": pod_name,
                        "avg": pod_stats.get('avg'),
                        "max": pod_stats.get('max'),
                        "unit": unit
                    })
        except Exception as e:
            self.logger.error(f"Error in _collect_backend_commit_metrics: {e}")
        return backend_metrics
    
    async def _collect_compact_defrag_metrics(self) -> List[Dict[str, Any]]:
        target_metrics = [
            'debugging_mvcc_db_compacted_keys',
            'debugging_mvcc_db_compaction_duration_sum_delta',
            'debugging_mvcc_db_compaction_duration_sum',
            'debugging_snapshot_duration',
            'disk_backend_defrag_duration_sum_rate',
            'disk_backend_defrag_duration_sum'
        ]
        compact_defrag_metrics = []
        try:
            all_metrics_result = await self.compact_defrag_collector.collect_all_metrics()
            if all_metrics_result.get('status') == 'success':
                metrics_data = all_metrics_result.get('metrics', {})
                for metric_name in target_metrics:
                    if metric_name in metrics_data:
                        metric_data = metrics_data[metric_name]
                        if metric_data.get('status') == 'success':
                            pods_data = {}
                            unit = metric_data.get('unit', 'unknown')
                            if 'data' in metric_data:
                                if 'pods' in metric_data['data']:
                                    pods_data = metric_data['data']['pods']
                                elif 'instances' in metric_data['data']:
                                    pods_data = metric_data['data']['instances']
                            elif 'pods' in metric_data:
                                pods_data = metric_data['pods']
                            elif 'instances' in metric_data:
                                pods_data = metric_data['instances']
                            for pod_name, pod_stats in pods_data.items():
                                compact_defrag_metrics.append({
                                    "metric_name": metric_name,
                                    "pod_name": pod_name,
                                    "avg": pod_stats.get('avg'),
                                    "max": pod_stats.get('max'),
                                    "unit": unit
                                })
        except Exception as e:
            self.logger.error(f"Error in _collect_compact_defrag_metrics: {e}")
        return compact_defrag_metrics

    async def _collect_node_usage_metrics(self) -> Dict[str, Any]:
        """Collect node usage metrics for master nodes"""
        try:
            self.logger.info("Collecting node usage metrics")
            result = await self.node_usage_collector.collect_all_metrics(node_group='master', duration=self.duration)
            if result.get('status') == 'success':
                self.logger.info(
                    f"Successfully collected node usage metrics for {result.get('total_nodes', 0)} nodes"
                )
                metrics = result.get('metrics', {})
                for metric_name, metric_data in metrics.items():
                    if metric_data.get('status') == 'success':
                        node_count = len(metric_data.get('nodes', {}))
                        self.logger.info(f"  - {metric_name}: {node_count} nodes")
            else:
                self.logger.warning(
                    f"Node usage collection returned status: {result.get('status')}"
                )
                if 'error' in result:
                    self.logger.error(f"Node usage collection error: {result['error']}")
            return result
        except Exception as e:
            self.logger.error(f"Error in _collect_node_usage_metrics: {e}", exc_info=True)
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def analyze_bottlenecks(self) -> Dict[str, Any]:
        try:
            analysis_data = await self.analyze_performance_deep_drive()
            if analysis_data.get('status') != 'success':
                return analysis_data
            bottleneck_analysis = {
                "test_id": self.test_id,
                "timestamp": self.utility.format_timestamp(),
                "bottleneck_analysis": {
                    "disk_io_bottlenecks": [],
                    "network_bottlenecks": [],
                    "memory_bottlenecks": [],
                    "consensus_bottlenecks": []
                },
                "root_cause_analysis": [],
                "performance_recommendations": []
            }
            data = analysis_data.get('data', {})
            await self._analyze_disk_bottlenecks(data, bottleneck_analysis)
            await self._analyze_network_bottlenecks(data, bottleneck_analysis)
            await self._analyze_memory_bottlenecks(data, bottleneck_analysis)
            await self._analyze_consensus_bottlenecks(data, bottleneck_analysis)
            await self._analyze_node_resource_bottlenecks(data, bottleneck_analysis)
            bottleneck_analysis["root_cause_analysis"] = self._generate_root_cause_analysis(bottleneck_analysis)
            bottleneck_analysis["performance_recommendations"] = self._generate_performance_recommendations(bottleneck_analysis)
            return bottleneck_analysis
        except Exception as e:
            self.logger.error(f"Error in analyze_bottlenecks: {e}")
            return {
                "test_id": self.test_id,
                "timestamp": self.utility.format_timestamp(),
                "status": "error",
                "error": str(e)
            }
        finally:
            # Ensure network collector session is closed after bottleneck analysis
            try:
                await self.network_collector.close()
            except Exception as close_err:
                self.logger.debug(f"Error closing network collector after bottleneck analysis: {close_err}")
    
    async def _analyze_disk_bottlenecks(self, data: Dict[str, Any], analysis: Dict[str, Any]):
        try:
            wal_data = data.get('wal_fsync_data', [])
            for metric in wal_data:
                if 'p99' in metric.get('metric_name', '') and metric.get('avg'):
                    if metric['avg'] > 0.1:
                        analysis['bottleneck_analysis']['disk_io_bottlenecks'].append({
                            "type": "wal_fsync_high_latency",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "high",
                            "description": "WAL fsync P99 latency exceeds 100ms threshold"
                        })
                    elif metric['avg'] > 0.05:
                        analysis['bottleneck_analysis']['disk_io_bottlenecks'].append({
                            "type": "wal_fsync_elevated_latency",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "medium",
                            "description": "WAL fsync P99 latency elevated above 50ms"
                        })
            backend_data = data.get('backend_commit_data', [])
            for metric in backend_data:
                if 'p99' in metric.get('metric_name', '') and metric.get('avg'):
                    if metric['avg'] > 0.05:
                        analysis['bottleneck_analysis']['disk_io_bottlenecks'].append({
                            "type": "backend_commit_high_latency",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "high",
                            "description": "Backend commit P99 latency exceeds 50ms threshold"
                        })
            disk_data = data.get('disk_io_data', [])
            for metric in disk_data:
                if 'throughput' in metric.get('metric_name', ''):
                    if metric.get('avg', 0) < 1024 * 1024:
                        analysis['bottleneck_analysis']['disk_io_bottlenecks'].append({
                            "type": "low_disk_throughput",
                            "node": metric.get('node_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "medium",
                            "description": "Disk throughput below expected levels"
                        })
        except Exception as e:
            self.logger.error(f"Error analyzing disk bottlenecks: {e}")
    
    async def _analyze_network_bottlenecks(self, data: Dict[str, Any], analysis: Dict[str, Any]):
        try:
            network_data = data.get('network_data', {})
            pod_metrics = network_data.get('pod_metrics', [])
            for metric in pod_metrics:
                if 'peer2peer_latency' in metric.get('metric_name', '') and metric.get('avg'):
                    if metric['avg'] > 0.1:
                        analysis['bottleneck_analysis']['network_bottlenecks'].append({
                            "type": "high_peer_latency",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "high",
                            "description": "Peer-to-peer latency exceeds 100ms threshold"
                        })
            node_metrics = network_data.get('node_metrics', [])
            node_data = {}
            for metric in node_metrics:
                node_name = metric.get('node_name')
                metric_name = metric.get('metric_name', '')
                if node_name not in node_data:
                    node_data[node_name] = { 'rx_bytes_per_sec': 0, 'tx_bytes_per_sec': 0, 'nic_bandwidth_bps': None }
                if 'network_rx_utilization' in metric_name:
                    node_data[node_name]['rx_bytes_per_sec'] = metric.get('avg', 0)
                elif 'network_tx_utilization' in metric_name:
                    node_data[node_name]['tx_bytes_per_sec'] = metric.get('avg', 0)
            for node_name, node_info in node_data.items():
                try:
                    nic_bandwidth_bps = await self._get_nic_bandwidth(node_name)
                    if nic_bandwidth_bps:
                        node_info['nic_bandwidth_bps'] = nic_bandwidth_bps
                        if node_info['rx_bytes_per_sec'] > 0:
                            rx_utilization_pct = (node_info['rx_bytes_per_sec'] / nic_bandwidth_bps) * 100
                            if rx_utilization_pct > 80:
                                analysis['bottleneck_analysis']['network_bottlenecks'].append({
                                    "type": "high_network_utilization",
                                    "node": node_name,
                                    "value": round(rx_utilization_pct, 2),
                                    "unit": "percent",
                                    "severity": "high",
                                    "description": f"Network RX utilization exceeds 80%"
                                })
                            elif rx_utilization_pct > 60:
                                analysis['bottleneck_analysis']['network_bottlenecks'].append({
                                    "type": "elevated_network_utilization",
                                    "node": node_name,
                                    "value": round(rx_utilization_pct, 2),
                                    "unit": "percent",
                                    "severity": "medium",
                                    "description": f"Network RX utilization elevated above 60%"
                                })
                        if node_info['tx_bytes_per_sec'] > 0:
                            tx_utilization_pct = (node_info['tx_bytes_per_sec'] / nic_bandwidth_bps) * 100
                            if tx_utilization_pct > 80:
                                analysis['bottleneck_analysis']['network_bottlenecks'].append({
                                    "type": "high_network_utilization",
                                    "node": node_name,
                                    "value": round(tx_utilization_pct, 2),
                                    "unit": "percent",
                                    "severity": "high",
                                    "description": f"Network TX utilization exceeds 80%"
                                })
                            elif tx_utilization_pct > 60:
                                analysis['bottleneck_analysis']['network_bottlenecks'].append({
                                    "type": "elevated_network_utilization",
                                    "node": node_name,
                                    "value": round(tx_utilization_pct, 2),
                                    "unit": "percent",
                                    "severity": "medium",
                                    "description": f"Network TX utilization elevated above 60%"
                                })
                except Exception as e:
                    self.logger.error(f"Error calculating network utilization for node {node_name}: {e}")
                    continue
            for metric in node_metrics:
                if 'drop' in metric.get('metric_name', '') and metric.get('avg'):
                    if metric['avg'] > 0:
                        direction = 'RX' if 'rx_drop' in metric.get('metric_name', '') else 'TX'
                        analysis['bottleneck_analysis']['network_bottlenecks'].append({
                            "type": "packet_drops",
                            "node": metric.get('node_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "medium",
                            "description": f"Network {direction} packet drops detected"
                        })
        except Exception as e:
            self.logger.error(f"Error analyzing network bottlenecks: {e}")
    
    async def _get_nic_bandwidth(self, node_name: str) -> Optional[float]:
        try:
            bandwidth_query = f'node_network_speed_bytes{{instance=~".*{node_name}.*"}}'
            try:
                # Ensure session is available (reinitialize if needed)
                if not hasattr(self.network_collector.prometheus_client, 'session') or \
                   self.network_collector.prometheus_client.session is None or \
                   self.network_collector.prometheus_client.session.closed:
                    await self.network_collector.initialize()
                
                # Use prometheus_client directly to query
                result = await self.network_collector.prometheus_client.query_instant(bandwidth_query)
                if result and result.get('result') and len(result['result']) > 0:
                    max_bandwidth = max([float(item['value'][1]) for item in result['result']])
                    return max_bandwidth
            except Exception as query_err:
                self.logger.debug(f"Could not query NIC bandwidth for {node_name}: {query_err}")
                pass
            # Fallback to default values based on node type
            if 'master' in node_name.lower() or 'control' in node_name.lower():
                return 10 * 1024 * 1024 * 1024 / 8
            elif 'worker' in node_name.lower():
                return 1 * 1024 * 1024 * 1024 / 8
            else:
                return 1 * 1024 * 1024 * 1024 / 8
        except Exception as e:
            self.logger.error(f"Error getting NIC bandwidth for {node_name}: {e}")
            return 1 * 1024 * 1024 * 1024 / 8
    
    def _format_bandwidth(self, bytes_per_sec: float) -> str:
        if bytes_per_sec >= 1024**3:
            return f"{bytes_per_sec/(1024**3):.2f}GB"
        elif bytes_per_sec >= 1024**2:
            return f"{bytes_per_sec/(1024**2):.2f}MB"
        elif bytes_per_sec >= 1024:
            return f"{bytes_per_sec/1024:.2f}KB"
        else:
            return f"{bytes_per_sec:.0f}B"
    
    async def _analyze_memory_bottlenecks(self, data: Dict[str, Any], analysis: Dict[str, Any]):
        try:
            general_data = data.get('general_info_data', [])
            for metric in general_data:
                if metric.get('metric_name') == 'memory_usage' and metric.get('avg'):
                    if metric['avg'] > 80:
                        analysis['bottleneck_analysis']['memory_bottlenecks'].append({
                            "type": "high_memory_usage",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "high",
                            "description": "Memory usage exceeds 80%"
                        })
                    elif metric['avg'] > 70:
                        analysis['bottleneck_analysis']['memory_bottlenecks'].append({
                            "type": "elevated_memory_usage",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "medium",
                            "description": "Memory usage elevated above 70%"
                        })
        except Exception as e:
            self.logger.error(f"Error analyzing memory bottlenecks: {e}")
    
    async def _analyze_consensus_bottlenecks(self, data: Dict[str, Any], analysis: Dict[str, Any]):
        try:
            general_data = data.get('general_info_data', [])
            for metric in general_data:
                metric_name = metric.get('metric_name', '')
                if metric_name == 'proposal_failure_rate' and metric.get('avg'):
                    if metric['avg'] > 0:
                        analysis['bottleneck_analysis']['consensus_bottlenecks'].append({
                            "type": "proposal_failures",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "high",
                            "description": "Proposal failures detected in etcd consensus"
                        })
                elif metric_name == 'proposal_pending_total' and metric.get('avg'):
                    if metric['avg'] > 10:
                        analysis['bottleneck_analysis']['consensus_bottlenecks'].append({
                            "type": "high_pending_proposals",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "medium",
                            "description": "High number of pending proposals"
                        })
                elif metric_name == 'slow_applies' and metric.get('avg'):
                    if metric['avg'] > 0:
                        analysis['bottleneck_analysis']['consensus_bottlenecks'].append({
                            "type": "slow_applies",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "medium",
                            "description": "Slow apply operations detected"
                        })
                elif metric_name == 'leader_changes_rate' and metric.get('avg'):
                    if metric['avg'] > 1:
                        analysis['bottleneck_analysis']['consensus_bottlenecks'].append({
                            "type": "frequent_leader_changes",
                            "pod": metric.get('pod_name'),
                            "value": metric['avg'],
                            "unit": metric.get('unit'),
                            "severity": "high",
                            "description": "Frequent leader changes indicating cluster instability"
                        })
        except Exception as e:
            self.logger.error(f"Error analyzing consensus bottlenecks: {e}")



    def _generate_root_cause_analysis(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate root cause analysis based on identified bottlenecks"""
        root_causes: List[Dict[str, Any]] = []
        bottlenecks = analysis.get('bottleneck_analysis', {})

        if bottlenecks.get('disk_io_bottlenecks'):
            root_causes.append({
                "category": "disk_io",
                "root_cause": "Storage subsystem performance degradation",
                "evidence": "High WAL fsync latency and/or backend commit latency",
                "impact": "Increased write operation latency affecting cluster performance",
                "likelihood": "high"
            })

        if bottlenecks.get('network_bottlenecks'):
            root_causes.append({
                "category": "network",
                "root_cause": "Network connectivity or bandwidth issues",
                "evidence": "High peer-to-peer latency, network utilization, or packet drops",
                "impact": "Degraded cluster communication and consensus performance",
                "likelihood": "high"
            })

        if bottlenecks.get('memory_bottlenecks'):
            root_causes.append({
                "category": "memory",
                "root_cause": "Insufficient memory resources or memory leaks",
                "evidence": "High memory usage on etcd pods or nodes",
                "impact": "Potential pod restarts and performance degradation",
                "likelihood": "medium"
            })

        if bottlenecks.get('consensus_bottlenecks'):
            root_causes.append({
                "category": "consensus",
                "root_cause": "etcd cluster consensus mechanism under stress",
                "evidence": "Proposal failures, slow applies, or frequent leader changes",
                "impact": "Degraded cluster stability and write performance",
                "likelihood": "high"
            })

        return root_causes

    def _generate_performance_recommendations(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate performance optimization recommendations from bottlenecks"""
        recommendations: List[Dict[str, Any]] = []
        bottlenecks = analysis.get('bottleneck_analysis', {})

        # Disk I/O recommendations
        if bottlenecks.get('disk_io_bottlenecks'):
            recommendations.extend([
                {
                    "category": "disk_io",
                    "priority": "high",
                    "recommendation": "Upgrade storage to high-performance NVMe SSDs",
                    "rationale": "Reduce WAL fsync and backend commit latency"
                },
                {
                    "category": "disk_io",
                    "priority": "medium",
                    "recommendation": "Ensure etcd data directory is on dedicated storage",
                    "rationale": "Prevent I/O contention with other workloads"
                }
            ])

        # Network recommendations
        if bottlenecks.get('network_bottlenecks'):
            recommendations.extend([
                {
                    "category": "network",
                    "priority": "high",
                    "recommendation": "Optimize network topology and reduce network hops",
                    "rationale": "Minimize peer-to-peer communication latency"
                },
                {
                    "category": "network",
                    "priority": "medium",
                    "recommendation": "Increase network bandwidth or use dedicated network interfaces",
                    "rationale": "Reduce network utilization and eliminate packet drops"
                }
            ])

        # Memory recommendations
        if bottlenecks.get('memory_bottlenecks'):
            recommendations.extend([
                {
                    "category": "memory",
                    "priority": "high",
                    "recommendation": "Increase memory limits for etcd pods and master nodes",
                    "rationale": "Prevent memory pressure and potential OOM kills"
                },
                {
                    "category": "memory",
                    "priority": "medium",
                    "recommendation": "Monitor for memory leaks and optimize database size",
                    "rationale": "Maintain stable memory usage patterns"
                }
            ])

        # Consensus recommendations
        if bottlenecks.get('consensus_bottlenecks'):
            recommendations.extend([
                {
                    "category": "consensus",
                    "priority": "high",
                    "recommendation": "Review etcd configuration and tune performance parameters",
                    "rationale": "Optimize consensus algorithm performance"
                },
                {
                    "category": "consensus",
                    "priority": "medium",
                    "recommendation": "Schedule regular database compaction and defragmentation",
                    "rationale": "Improve apply operation performance and reduce DB bloat"
                }
            ])

        if not recommendations:
            recommendations.append({
                "category": "general",
                "priority": "low",
                "recommendation": "Continue monitoring etcd cluster performance",
                "rationale": "No significant bottlenecks detected in current analysis"
            })

        return recommendations

    async def _analyze_node_resource_bottlenecks(self, data: Dict[str, Any], analysis: Dict[str, Any]):
        """Analyze node resource bottlenecks using node_usage_data"""
        try:
            node_usage_data = data.get('node_usage_data', {})
            if node_usage_data.get('status') != 'success':
                self.logger.warning("Node usage data not available for bottleneck analysis")
                return

            metrics = node_usage_data.get('metrics', {})

            # CPU usage bottlenecks
            cpu_usage = metrics.get('cpu_usage', {})
            if cpu_usage.get('status') == 'success':
                nodes = cpu_usage.get('nodes', {})
                for node_name, node_data in nodes.items():
                    total = node_data.get('total', {})
                    modes = node_data.get('modes', {})
                    idle_max = modes.get('idle', {}).get('max', 0)
                    estimated_cores = int(idle_max / 100) if idle_max > 0 else 40
                    raw_avg = total.get('avg', 0)
                    avg_utilization = (raw_avg / estimated_cores) if estimated_cores > 0 else 0
                    if avg_utilization > 85:
                        analysis['bottleneck_analysis']['disk_io_bottlenecks'].append({
                            "type": "high_node_cpu_usage",
                            "node": node_name,
                            "value": round(avg_utilization, 2),
                            "unit": "percent",
                            "severity": "high",
                            "description": "Master node CPU utilization exceeds 85%"
                        })
                    elif avg_utilization > 70:
                        analysis['bottleneck_analysis']['disk_io_bottlenecks'].append({
                            "type": "elevated_node_cpu_usage",
                            "node": node_name,
                            "value": round(avg_utilization, 2),
                            "unit": "percent",
                            "severity": "medium",
                            "description": "Master node CPU utilization elevated above 70%"
                        })

            # Memory usage bottlenecks
            memory_used = metrics.get('memory_used', {})
            if memory_used.get('status') == 'success':
                nodes = memory_used.get('nodes', {})
                for node_name, node_data in nodes.items():
                    avg_used = node_data.get('avg', 0)
                    total_capacity = node_data.get('total_capacity', 0)
                    if total_capacity > 0:
                        avg_percent = (avg_used / total_capacity) * 100
                        if avg_percent > 85:
                            analysis['bottleneck_analysis']['memory_bottlenecks'].append({
                                "type": "high_node_memory_usage",
                                "node": node_name,
                                "value": round(avg_percent, 2),
                                "unit": "percent",
                                "severity": "high",
                                "description": "Master node memory utilization exceeds 85%"
                            })
                        elif avg_percent > 70:
                            analysis['bottleneck_analysis']['memory_bottlenecks'].append({
                                "type": "elevated_node_memory_usage",
                                "node": node_name,
                                "value": round(avg_percent, 2),
                                "unit": "percent",
                                "severity": "medium",
                                "description": "Master node memory utilization elevated above 70%"
                            })

            # Cgroup CPU signal for consensus pressure
            cgroup_cpu = metrics.get('cgroup_cpu_usage', {})
            if cgroup_cpu.get('status') == 'success':
                nodes = cgroup_cpu.get('nodes', {})
                for node_name, node_data in nodes.items():
                    cgroups = node_data.get('cgroups', {})
                    kubepods_usage = cgroups.get('kubepods.slice', {}).get('avg', 0)
                    if kubepods_usage > 150:
                        analysis['bottleneck_analysis']['consensus_bottlenecks'].append({
                            "type": "high_kubepods_cpu",
                            "node": node_name,
                            "value": round(kubepods_usage, 2),
                            "unit": "percent",
                            "severity": "medium",
                            "description": "High CPU usage by kubepods.slice on master node"
                        })
        except Exception as e:
            self.logger.error(f"Error analyzing node resource bottlenecks: {e}", exc_info=True)

    def _get_prometheus_config_from_auth(self, ocp_auth) -> Dict[str, Any]:
        """Extract Prometheus configuration from ocp_auth"""
        try:
            if hasattr(ocp_auth, 'prometheus_config'):
                return ocp_auth.prometheus_config
            if isinstance(ocp_auth, dict):
                return ocp_auth.get('prometheus_config', {})
            config: Dict[str, Any] = {}
            if hasattr(ocp_auth, 'prometheus_url'):
                config['url'] = ocp_auth.prometheus_url
            elif hasattr(ocp_auth, 'prom_url'):
                config['url'] = ocp_auth.prom_url
            # Prefer a dedicated Prometheus token if available
            token = None
            if hasattr(ocp_auth, 'prometheus_token'):
                token = getattr(ocp_auth, 'prometheus_token', None)
            if not token and (hasattr(ocp_auth, 'token') or hasattr(ocp_auth, 'bearer_token')):
                token = getattr(ocp_auth, 'token', None) or getattr(ocp_auth, 'bearer_token', None)
            if token:
                # Expose as explicit token for downstream clients
                config['token'] = token
                config['headers'] = {
                    'Authorization': f'Bearer {token}'
                }
            return config
        except Exception as e:
            self.logger.warning(f"Could not extract prometheus config from ocp_auth: {e}")
            return {}
