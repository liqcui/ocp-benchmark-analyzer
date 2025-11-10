"""
Deep Drive ELT module for OVN-Kubernetes comprehensive performance analysis
Extract, Load, Transform module for deep drive analysis results
"""

import logging
from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime

from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class deepDriveELT(EltUtility):
    """Extract, Load, Transform class for Deep Drive analysis data"""
    
    def __init__(self):
        super().__init__()

    def _to_float_safe(self, value, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            if isinstance(value, (int, float)):
                return float(value)
            return float(str(value).strip())
        except Exception:
            return default

    def _extract_stat(self, metrics: Dict[str, Any], metric_key: str, stat_candidates: List[str]) -> float:
        """Extract statistical values from nested metrics structure"""
        try:
            if not isinstance(metrics, dict):
                return 0.0
            
            # Handle both direct access and nested access
            metric_data = metrics.get(metric_key, {})
            if not isinstance(metric_data, dict):
                return 0.0
                
            for key in stat_candidates:
                if key in metric_data and metric_data.get(key) is not None:
                    return self._to_float_safe(metric_data.get(key), 0.0)
            return 0.0
        except Exception:
            return 0.0

    def extract_deepdrive_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract deep drive analysis data from JSON - UPDATED"""
        try:
            extracted = {
                'metadata': self._extract_metadata(data),
                'basic_info': self._extract_basic_info(data),
                'resource_usage': self._extract_resource_usage(data),
                'latency_analysis': self._extract_latency_analysis(data),
                'latency_summary_metrics': self._extract_latency_summary_metrics(data),  # NEW
                'performance_insights': self._extract_performance_insights(data),
                'node_analysis': self._extract_node_analysis(data),
                'ovs_metrics': self._extract_ovs_summary(data)
            }
            return extracted
            
        except Exception as e:
            logger.error(f"Failed to extract deep drive data: {e}")
            return {'error': str(e)}

    def _extract_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata information"""
        return {
            'analysis_timestamp': data.get('analysis_timestamp', ''),
            'analysis_type': data.get('analysis_type', ''),
            'query_duration': data.get('query_duration', ''),
            'timezone': data.get('timezone', 'UTC'),
            'components_analyzed': data.get('execution_metadata', {}).get('components_analyzed', 0),
            'tool_name': data.get('execution_metadata', {}).get('tool_name', ''),
            'timeout_seconds': data.get('execution_metadata', {}).get('timeout_seconds', 0)
        }
 
    def _extract_basic_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract basic cluster information"""
        basic_info = data.get('basic_info', {})
        
        # Pod counts and phases
        pod_info = []
        pod_counts = basic_info.get('pod_counts', {})
        if pod_counts:
            pod_info.append({
                'Metric': 'Total Pods',
                'Value': pod_counts.get('total_pods', 0),
                'Status': 'info'
            })
            
            phases = pod_counts.get('phases', {})
            for phase, count in phases.items():
                status = 'success' if phase == 'Running' else 'warning' if phase == 'Failed' else 'info'
                pod_info.append({
                    'Metric': f'Pods {phase}',
                    'Value': count,
                    'Status': status
                })

        # Database sizes
        db_info = []
        db_sizes = basic_info.get('database_sizes', {})
        for db_name, db_data in db_sizes.items():
            size_mb = db_data.get('size_mb', 0)
            db_info.append({
                'Database': db_name.replace('_', ' ').title(),
                'Max DB Size': f"{size_mb:.1f} MB" if isinstance(size_mb, (int, float)) else size_mb,
                'Status': 'success' if (isinstance(size_mb, (int, float)) and size_mb < 10) else 'warning'
            })

        # Alerts summary - FIXED to handle both alerts and top_alerts structures
        alerts_info = []
        alerts_summary = basic_info.get('alerts_summary', {})
        
        # Try both 'alerts' and 'top_alerts' keys to handle different data structures
        alerts_data = alerts_summary.get('alerts', [])
        if not alerts_data:
            alerts_data = alerts_summary.get('top_alerts', [])
        
        alertname_stats = alerts_summary.get('alertname_statistics', {})
        
        # Create a combined view showing both individual alerts and statistics
        processed_alertnames = set()
        
        for idx, alert in enumerate(alerts_data[:5], 1):  # Top 5 alerts
            alert_name = alert.get('alert_name', '')
            severity = alert.get('severity', 'unknown')
            count = alert.get('count', 0)
            
            # Get avg/max stats for this alertname if available
            stats = alertname_stats.get(alert_name, {})
            avg_count = stats.get('avg_count', count)
            max_count = stats.get('max_count', count)
            
            status = 'danger' if severity == 'critical' else 'warning' if severity == 'warning' else 'info'
            
            # Show count with avg/max if different
            if avg_count != max_count:
                count_display = f"{count} (avg: {avg_count}, max: {max_count})"
            else:
                count_display = str(count)
            
            alerts_info.append({
                'Rank': f"🔥 {idx}" if idx == 1 and severity in ['critical', 'warning'] else idx,
                'Alert': alert_name,
                'Severity': severity.upper(),
                'Count': count_display,
                'Status': status
            })
            
            processed_alertnames.add(alert_name)
        
        # Add any alertname statistics that weren't in the main alerts list
        remaining_stats = {name: stats for name, stats in alertname_stats.items() 
                        if name not in processed_alertnames}
        
        for alert_name, stats in list(remaining_stats.items())[:3]:  # Add up to 3 more
            avg_count = stats.get('avg_count', 0)
            max_count = stats.get('max_count', 0)
            
            alerts_info.append({
                'Rank': len(alerts_info) + 1,
                'Alert': alert_name,
                'Severity': 'UNKNOWN',
                'Count': f"avg: {avg_count}, max: {max_count}",
                'Status': 'info'
            })

        return {
            'pod_status': pod_info,
            'database_sizes': db_info,
            'alerts': alerts_info
        }

    def _extract_resource_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:

        """Extract resource usage data"""
        # OVNKube pods CPU usage
        ovnkube_pods = data.get('ovnkube_pods_cpu', {})
        top_cpu_pods = []
        pods_usage_detailed: List[Dict[str, Any]] = []
        
        # Create memory lookup maps from both CPU and memory sections
        node_pods_memory_map = {}
        cp_pods_memory_map = {}
        
        # Build memory map from ovnkube_node_pods.top_5_memory
        node_pods_memory = ovnkube_pods.get('ovnkube_node_pods', {}).get('top_5_memory', [])
        for mem_pod in node_pods_memory:
            pod_name = mem_pod.get('pod_name', '')
            if pod_name:
                node_pods_memory_map[pod_name] = mem_pod.get('metrics', {})
        
        # Build memory map from ovnkube_control_plane_pods.top_5_memory  
        cp_pods_memory = ovnkube_pods.get('ovnkube_control_plane_pods', {}).get('top_5_memory', [])
        for mem_pod in cp_pods_memory:
            pod_name = mem_pod.get('pod_name', '')
            if pod_name:
                cp_pods_memory_map[pod_name] = mem_pod.get('metrics', {})

        # Node pods
        node_pods_cpu = ovnkube_pods.get('ovnkube_node_pods', {}).get('top_5_cpu', [])
        for pod in node_pods_cpu[:5]:
            cpu_usage = pod.get('metrics', {}).get('cpu_usage', {})
            pod_name = pod.get('pod_name', '')
            
            # Get memory data from memory map
            memory_metrics = node_pods_memory_map.get(pod_name, {})
            memory_usage = memory_metrics.get('memory_usage', {})
            
            rank = pod.get('rank', 0)
            top_cpu_pods.append({
                'Rank': f"🏆 {rank}" if rank == 1 else rank,
                'Pod': self.truncate_text(pod_name, 25),
                'Node': self.truncate_node_name(pod.get('node_name', ''), 20),
                'CPU %': f"{cpu_usage.get('avg', 0):.2f}",
                'Memory MB': f"{memory_usage.get('avg', 0):.1f}" if memory_usage and memory_usage.get('avg', 0) > 0 else "N/A"
            })

            pods_usage_detailed.append({
                'Scope': 'Node Pod',
                'Pod': pod_name,
                'Node': pod.get('node_name', ''),
                'Avg CPU %': round(cpu_usage.get('avg', 0.0), 2),
                'Max CPU %': round(cpu_usage.get('max', 0.0), 2),
                'Avg Mem MB': round(memory_usage.get('avg', 0.0), 1) if memory_usage else 0.0,
                'Max Mem MB': round(memory_usage.get('max', 0.0), 1) if memory_usage else 0.0
            })

        # Control plane pods
        cp_pods_cpu = ovnkube_pods.get('ovnkube_control_plane_pods', {}).get('top_5_cpu', [])
        
        for pod in cp_pods_cpu:
            cpu_usage = pod.get('metrics', {}).get('cpu_usage', {})
            pod_name = pod.get('pod_name', '')
            
            # Get memory data from memory map
            memory_metrics = cp_pods_memory_map.get(pod_name, {})
            memory_usage = memory_metrics.get('memory_usage', {})
            
            rank = len(top_cpu_pods) + 1
            
            top_cpu_pods.append({
                'Rank': rank,
                'Pod': self.truncate_text(pod_name, 25),
                'Node': self.truncate_node_name(pod.get('node_name', ''), 20),
                'CPU %': f"{cpu_usage.get('avg', 0):.2f}",
                'Memory MB': f"{memory_usage.get('avg', 0):.1f}" if memory_usage and memory_usage.get('avg', 0) > 0 else "N/A"
            })

            pods_usage_detailed.append({
                'Scope': 'Control Pod',
                'Pod': pod_name,
                'Node': pod.get('node_name', ''),
                'Avg CPU %': round(cpu_usage.get('avg', 0.0), 2),
                'Max CPU %': round(cpu_usage.get('max', 0.0), 2),
                'Avg Mem MB': round(memory_usage.get('avg', 0.0), 1) if memory_usage else 0.0,
                'Max Mem MB': round(memory_usage.get('max', 0.0), 1) if memory_usage else 0.0
            })

            # OVN containers usage
            container_usage = []
            containers_usage_detailed: List[Dict[str, Any]] = []
            ovn_containers = data.get('ovn_containers', {}).get('containers', {})
            
            for container_name, container_data in ovn_containers.items():
                if 'error' not in container_data:
                    cpu_data = container_data.get('top_5_cpu', [])
                    mem_data = container_data.get('top_5_memory', [])
                    
                    if cpu_data:
                        top_cpu = cpu_data[0]
                        cpu_metrics = top_cpu.get('metrics', {}).get('cpu_usage', {})
                        
                        mem_metrics = top_cpu.get('metrics', {}).get('memory_usage', {})
                        if not mem_metrics and mem_data:
                            pod_name = top_cpu.get('pod_name', '')
                            for mem_entry in mem_data:
                                if mem_entry.get('pod_name', '') == pod_name:
                                    mem_metrics = mem_entry.get('metrics', {}).get('memory_usage', {})
                                    break
                            if not mem_metrics and mem_data:
                                mem_metrics = mem_data[0].get('metrics', {}).get('memory_usage', {})
                        
                        status = 'danger' if container_name == 'ovnkube_controller' and cpu_metrics.get('avg', 0) > 0.5 else 'success'
                        
                        container_usage.append({
                            'Container': container_name.replace('_', ' ').title(),
                            'CPU %': f"{cpu_metrics.get('avg', 0):.3f}",
                            'Memory MB': f"{mem_metrics.get('avg', 0):.1f}" if mem_metrics and mem_metrics.get('avg', 0) > 0 else "N/A",
                            'Status': status
                        })

                        containers_usage_detailed.append({
                            'Container': container_name,
                            'Pod': top_cpu.get('pod_name', ''),
                            'Node': top_cpu.get('node_name', ''),
                            'Avg CPU %': round(cpu_metrics.get('avg', 0.0), 3),
                            'Max CPU %': round(cpu_metrics.get('max', 0.0), 3),
                            'Avg Mem MB': round(mem_metrics.get('avg', 0.0), 1) if mem_metrics else 0.0,
                            'Max Mem MB': round(mem_metrics.get('max', 0.0), 1) if mem_metrics else 0.0
                        })

            # Nodes usage detailed (per-node avg/max) - FIXED
            nodes_usage = data.get('nodes_usage', {})
            nodes_usage_detailed: List[Dict[str, Any]] = []
            nodes_network_usage: List[Dict[str, Any]] = []
            
            if nodes_usage:
                # Controlplane nodes
                cp = nodes_usage.get('controlplane_nodes', {})
                for node in (cp.get('individual_nodes', []) or []):
                    node_name = node.get('name') or node.get('instance', '')
                    
                    # CPU and Memory from the JSON structure
                    avg_cpu = self._to_float_safe(node.get('cpu_usage', {}).get('avg'), 0.0)
                    max_cpu = self._to_float_safe(node.get('cpu_usage', {}).get('max'), 0.0)
                    avg_mem = self._to_float_safe(node.get('memory_usage', {}).get('avg'), 0.0)
                    max_mem = self._to_float_safe(node.get('memory_usage', {}).get('max'), 0.0)
                    
                    nodes_usage_detailed.append({
                        'Node Group': '🔥 Control Plane' if avg_cpu > 15 else 'Control Plane',
                        'Node Name': node_name,
                        'Avg CPU %': round(avg_cpu, 2),
                        'Max CPU %': round(max_cpu, 2),
                        'Avg Mem MB': round(avg_mem, 1),
                        'Max Mem MB': round(max_mem, 1)
                    })
                    
                    # Network usage
                    avg_rx = self._to_float_safe(node.get('network_rx', {}).get('avg'), 0.0)
                    max_rx = self._to_float_safe(node.get('network_rx', {}).get('max'), 0.0)
                    avg_tx = self._to_float_safe(node.get('network_tx', {}).get('avg'), 0.0)
                    max_tx = self._to_float_safe(node.get('network_tx', {}).get('max'), 0.0)
                    
                    nodes_network_usage.append({
                        'Node Group': '🔥 Control Plane' if avg_rx > 200000 else 'Control Plane',
                        'Node Name': node_name,
                        'Avg Network RX': f"{avg_rx/1024:.1f} KB/s",
                        'Max Network RX': f"{max_rx/1024:.1f} KB/s",
                        'Avg Network TX': f"{avg_tx/1024:.1f} KB/s",
                        'Max Network TX': f"{max_tx/1024:.1f} KB/s"
                    })

                # Infra nodes
                infra = nodes_usage.get('infra_nodes', {})
                for node in (infra.get('individual_nodes', []) or []):
                    node_name = node.get('name') or node.get('instance', '')
                    
                    avg_cpu = self._to_float_safe(node.get('cpu_usage', {}).get('avg'), 0.0)
                    max_cpu = self._to_float_safe(node.get('cpu_usage', {}).get('max'), 0.0)
                    avg_mem = self._to_float_safe(node.get('memory_usage', {}).get('avg'), 0.0)
                    max_mem = self._to_float_safe(node.get('memory_usage', {}).get('max'), 0.0)
                    
                    nodes_usage_detailed.append({
                        'Node Group': '🔥 Infra' if avg_cpu > 15 else 'Infra',
                        'Node Name': node_name,
                        'Avg CPU %': round(avg_cpu, 2),
                        'Max CPU %': round(max_cpu, 2),
                        'Avg Mem MB': round(avg_mem, 1),
                        'Max Mem MB': round(max_mem, 1)
                    })
                    
                    # Network usage
                    avg_rx = self._to_float_safe(node.get('network_rx', {}).get('avg'), 0.0)
                    max_rx = self._to_float_safe(node.get('network_rx', {}).get('max'), 0.0)
                    avg_tx = self._to_float_safe(node.get('network_tx', {}).get('avg'), 0.0)
                    max_tx = self._to_float_safe(node.get('network_tx', {}).get('max'), 0.0)
                    
                    nodes_network_usage.append({
                        'Node Group': '🔥 Infra' if avg_rx > 200000 else 'Infra',
                        'Node Name': node_name,
                        'Avg Network RX': f"{avg_rx/1024:.1f} KB/s",
                        'Max Network RX': f"{max_rx/1024:.1f} KB/s",
                        'Avg Network TX': f"{avg_tx/1024:.1f} KB/s",
                        'Max Network TX': f"{max_tx/1024:.1f} KB/s"
                    })

                # Top5 worker nodes
                top5 = nodes_usage.get('top5_worker_nodes', {})
                for idx, node in enumerate((top5.get('individual_nodes', []) or []), 1):
                    node_name = node.get('name') or node.get('instance', '')
                    
                    avg_cpu = self._to_float_safe(node.get('cpu_usage', {}).get('avg'), 0.0)
                    max_cpu = self._to_float_safe(node.get('cpu_usage', {}).get('max'), 0.0)
                    avg_mem = self._to_float_safe(node.get('memory_usage', {}).get('avg'), 0.0)
                    max_mem = self._to_float_safe(node.get('memory_usage', {}).get('max'), 0.0)
                    
                    group_name = f'🏆 Top{idx} Workers' if idx == 1 else f'Top{idx} Workers'
                    if avg_cpu > 70:
                        group_name = f'🔥 {group_name}'
                        
                    nodes_usage_detailed.append({
                        'Node Group': group_name,
                        'Node Name': node_name,
                        'Avg CPU %': round(avg_cpu, 2),
                        'Max CPU %': round(max_cpu, 2),
                        'Avg Mem MB': round(avg_mem, 1),
                        'Max Mem MB': round(max_mem, 1)
                    })
                    
                    # Network usage
                    avg_rx = self._to_float_safe(node.get('network_rx', {}).get('avg'), 0.0)
                    max_rx = self._to_float_safe(node.get('network_rx', {}).get('max'), 0.0)
                    avg_tx = self._to_float_safe(node.get('network_tx', {}).get('avg'), 0.0)
                    max_tx = self._to_float_safe(node.get('network_tx', {}).get('max'), 0.0)
                    
                    net_group_name = f'🏆 Top{idx} Workers' if idx == 1 else f'Top{idx} Workers'
                    if avg_rx > 500000:  # High network usage threshold
                        net_group_name = f'🔥 {net_group_name}'
                        
                    nodes_network_usage.append({
                        'Node Group': net_group_name,
                        'Node Name': node_name,
                        'Avg Network RX': f"{avg_rx/1024:.1f} KB/s",
                        'Max Network RX': f"{max_rx/1024:.1f} KB/s",
                        'Avg Network TX': f"{avg_tx/1024:.1f} KB/s",
                        'Max Network TX': f"{max_tx/1024:.1f} KB/s"
                    })

            return {
                'top_cpu_pods': top_cpu_pods,
                'container_usage': container_usage,
                'pods_usage_detailed': pods_usage_detailed,
                'containers_usage_detailed': containers_usage_detailed,
                'nodes_usage_detailed': nodes_usage_detailed,
                'nodes_network_usage': nodes_network_usage  # NEW TABLE
            }

    def _extract_latency_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract latency analysis data with separate tables for each metric type"""
        latency_metrics = data.get('latency_metrics', {})
        
        # Extract controller ready duration
        controller_ready = []
        ready_duration = latency_metrics.get('ready_duration', {})
        controller_ready_data = ready_duration.get('controller_ready_duration', {})
        if controller_ready_data:
            top_pods = controller_ready_data.get('top_5_pods', [])
            for idx, pod_data in enumerate(top_pods, 1):
                rank_display = f"🏆 {idx}" if idx == 1 else idx
                controller_ready.append({
                    'Rank': rank_display,
                    'Metric': 'controller_ready_duration',
                    'Pod Name': pod_data.get('pod_name', ''),
                    'Node Name': self.truncate_node_name(pod_data.get('node_name', ''), 25),
                    'Value': f"{pod_data.get('value', 0):.3f}s"
                })
        
        # Extract node ready duration
        node_ready = []
        node_ready_data = ready_duration.get('node_ready_duration', {})
        if node_ready_data:
            top_pods = node_ready_data.get('top_5_pods', [])
            for idx, pod_data in enumerate(top_pods, 1):
                # Highlight critical latency (> 3 seconds)
                value = pod_data.get('value', 0)
                rank_display = f"🔥 {idx}" if value > 3.0 and idx == 1 else f"🏆 {idx}" if idx == 1 else idx
                node_ready.append({
                    'Rank': rank_display,
                    'Metric': 'node_ready_duration',
                    'Pod Name': pod_data.get('pod_name', ''),
                    'Node Name': self.truncate_node_name(pod_data.get('node_name', ''), 25),
                    'Value': f"{value:.3f}s"
                })
        
        # Extract sync duration
        sync_duration = []
        sync_duration_data = latency_metrics.get('sync_duration', {})
        controller_sync_data = sync_duration_data.get('controller_sync_duration', {})
        if controller_sync_data:
            top_controllers = controller_sync_data.get('top_20_controllers', [])
            for idx, controller_data in enumerate(top_controllers[:20], 1):
                rank_display = f"🏆 {idx}" if idx == 1 else idx
                sync_duration.append({
                    'Rank': rank_display,
                    'Metric': 'controller_sync_duration',
                    'Pod Name': controller_data.get('pod_name', ''),
                    'Node Name': self.truncate_node_name(controller_data.get('node_name', ''), 25),
                    'Resource': controller_data.get('resource_name', ''),
                    'Value': f"{controller_data.get('value', 0):.3f}s"
                })
        
        # Extract pod annotation latency (pod_latency)
        pod_latency = []
        pod_annotation = latency_metrics.get('pod_annotation', {})
        pod_annotation_data = pod_annotation.get('pod_annotation_latency_p99', {})
        if pod_annotation_data:
            top_pods = pod_annotation_data.get('top_5_pods', [])
            for idx, pod_data in enumerate(top_pods, 1):
                # Highlight high latency (> 1 second)
                value = pod_data.get('value', 0)
                rank_display = f"⚠️ {idx}" if value > 1.0 and idx == 1 else f"🏆 {idx}" if idx == 1 else idx
                pod_latency.append({
                    'Rank': rank_display,
                    'Metric': 'pod_annotation_latency_p99',
                    'Pod Name': pod_data.get('pod_name', ''),
                    'Node Name': self.truncate_node_name(pod_data.get('node_name', ''), 25),
                    'Value': f"{value:.3f}s"
                })
        
        # Extract CNI latency
        cni_latency = []
        cni_data = latency_metrics.get('cni_latency', {})
        
        # CNI Add latency
        cni_add_data = cni_data.get('cni_add_latency_p99', {})
        if cni_add_data:
            top_pods = cni_add_data.get('top_5_pods', [])
            for idx, pod_data in enumerate(top_pods, 1):
                rank_display = f"🏆 {idx}" if idx == 1 else idx
                cni_latency.append({
                    'Rank': rank_display,
                    'Metric': 'cni_add_latency_p99',
                    'Pod Name': pod_data.get('pod_name', ''),
                    'Node Name': self.truncate_node_name(pod_data.get('node_name', ''), 25),
                    'Value': f"{pod_data.get('value', 0):.3f}s"
                })
        
        # CNI Del latency
        cni_del_data = cni_data.get('cni_del_latency_p99', {})
        if cni_del_data:
            top_pods = cni_del_data.get('top_5_pods', [])
            for idx, pod_data in enumerate(top_pods, 1):
                rank_display = idx + len(cni_latency)
                cni_latency.append({
                    'Rank': rank_display,
                    'Metric': 'cni_del_latency_p99',
                    'Pod Name': pod_data.get('pod_name', ''),
                    'Node Name': self.truncate_node_name(pod_data.get('node_name', ''), 25),
                    'Value': f"{pod_data.get('value', 0):.3f}s"
                })
        
        # Extract service latency
        service_latency = []
        service_data = latency_metrics.get('service_latency', {})
        
        # Sync service latency
        sync_service_data = service_data.get('sync_service_latency', {})
        if sync_service_data:
            top_pods = sync_service_data.get('top_5_pods', [])
            for idx, pod_data in enumerate(top_pods, 1):
                rank_display = f"🏆 {idx}" if idx == 1 else idx
                service_latency.append({
                    'Rank': rank_display,
                    'Metric': 'sync_service_latency',
                    'Pod Name': pod_data.get('pod_name', ''),
                    'Node Name': self.truncate_node_name(pod_data.get('node_name', ''), 25),
                    'Value': f"{pod_data.get('value', 0):.6f}s"
                })
        
        # Sync service latency p99
        sync_service_p99_data = service_data.get('sync_service_latency_p99', {})
        if sync_service_p99_data:
            top_pods = sync_service_p99_data.get('top_5_pods', [])
            for idx, pod_data in enumerate(top_pods, 1):
                rank_display = idx + len(service_latency)
                service_latency.append({
                    'Rank': rank_display,
                    'Metric': 'sync_service_latency_p99',
                    'Pod Name': pod_data.get('pod_name', ''),
                    'Node Name': self.truncate_node_name(pod_data.get('node_name', ''), 25),
                    'Value': f"{pod_data.get('value', 0):.3f}s"
                })
        
        # Extract network programming latency
        network_programming = []
        network_config = latency_metrics.get('network_config', {})
        network_config_data = network_config.get('apply_network_config_pod_p99', {})
        if network_config_data:
            top_pods = network_config_data.get('top_5_pods', [])
            for idx, pod_data in enumerate(top_pods, 1):
                rank_display = f"🏆 {idx}" if idx == 1 else idx
                network_programming.append({
                    'Rank': rank_display,
                    'Metric': 'apply_network_config_pod_p99',
                    'Pod Name': pod_data.get('pod_name', ''),
                    'Node Name': self.truncate_node_name(pod_data.get('node_name', ''), 25),
                    'Value': f"{pod_data.get('value', 0):.3f}s"
                })

        # Extract latency summary from performance analysis
        perf_analysis = data.get('performance_analysis', {})
        latency_analysis = perf_analysis.get('latency_analysis', {})
        
        latency_summary = []
        latency_summary.append({
            'Property': 'Overall Latency Health',
            'Value': latency_analysis.get('overall_latency_health', 'unknown').upper()
        })
        
        critical_issues = latency_analysis.get('critical_latency_issues', [])
        latency_summary.append({
            'Property': 'Critical Issues Count',
            'Value': str(len(critical_issues))
        })
        
        high_latency_components = latency_analysis.get('high_latency_components', [])
        latency_summary.append({
            'Property': 'High Latency Components',
            'Value': str(len(high_latency_components))
        })
        
        # Performance analysis summary
        perf_summary_data = perf_analysis.get('performance_summary', {})
        performance_summary = []
        
        performance_summary.append({
            'Property': 'Overall Score',
            'Value': f"{perf_summary_data.get('overall_score', 0):.1f}/100"
        })
        
        performance_summary.append({
            'Property': 'Performance Grade',
            'Value': perf_summary_data.get('performance_grade', 'N/A')
        })
        
        component_scores = perf_summary_data.get('component_scores', {})
        for component, score in component_scores.items():
            performance_summary.append({
                'Property': component.replace('_', ' ').title(),
                'Value': f"{score:.1f}/100"
            })
        
        # Key findings and recommendations
        findings_and_recommendations = []
        key_findings = perf_analysis.get('key_findings', [])
        recommendations = perf_analysis.get('recommendations', [])
        
        for finding in key_findings[:3]:
            findings_and_recommendations.append({
                'Type': 'Finding',
                'Description': finding
            })
        
        for rec in recommendations[:3]:
            findings_and_recommendations.append({
                'Type': 'Recommendation', 
                'Description': rec
            })

        return {
            'controller_ready_duration': controller_ready,
            'node_ready_duration': node_ready,
            'sync_duration': sync_duration,
            'pod_latency': pod_latency,
            'cni_latency': cni_latency,
            'service_latency': service_latency,
            'network_programming': network_programming,
            'latency_summary': latency_summary,
            'performance_summary': performance_summary,
            'findings_and_recommendations': findings_and_recommendations
        }

    def _extract_latency_summary_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract latency summary metrics with avg/max from formatted_summary - NEW FUNCTION"""
        try:
            formatted_summary = data.get('formatted_summary', {})
            latency_metrics = formatted_summary.get('latency_metrics', {})
            
            if not latency_metrics:
                return {'latency_overview': []}
            
            # Extract summary from the summary section
            summary = latency_metrics.get('summary', {})
            top_latencies = summary.get('top_latencies', [])
            
            latency_overview = []
            
            for idx, metric in enumerate(top_latencies, 1):
                metric_name = metric.get('metric_name', '')
                component = metric.get('component', '')
                max_value = metric.get('max_value', 0)
                avg_value = metric.get('avg_value', 0)
                data_points = metric.get('data_points', 0)
                
                # Get readable values
                readable_max = metric.get('readable_max', {})
                readable_avg = metric.get('readable_avg', {})
                
                max_display = f"{readable_max.get('value', max_value)} {readable_max.get('unit', 's')}"
                avg_display = f"{readable_avg.get('value', avg_value)} {readable_avg.get('unit', 's')}"
                
                # Highlight critical and top 1
                rank_display = f"🏆 {idx}" if idx == 1 else idx
                if max_value > 3.0:  # Critical threshold for seconds
                    rank_display = f"🔥 {idx}" if idx == 1 else f"⚠️ {idx}"
                elif max_value > 1.0:  # Warning threshold
                    rank_display = f"⚠️ {idx}" if idx == 1 else idx
                
                latency_overview.append({
                    'Rank': rank_display,
                    'Metric': self.truncate_metric_name(metric_name, 35),
                    'Component': component.title(),
                    'Max Latency': max_display,
                    'Avg Latency': avg_display,
                    'Data Points': str(data_points)
                })
            
            # Overall summary stats
            overall_stats = []
            overall_max = summary.get('overall_max_latency', {})
            overall_avg = summary.get('overall_avg_latency', {})
            
            if overall_max:
                max_readable = overall_max.get('readable', {})
                max_display = f"{max_readable.get('value', 0)} {max_readable.get('unit', 's')}"
                overall_stats.append({
                    'Property': 'Overall Max Latency',
                    'Value': max_display,
                    'Metric': overall_max.get('metric', '')
                })
            
            if overall_avg:
                avg_readable = overall_avg.get('readable', {})
                avg_display = f"{avg_readable.get('value', 0)} {avg_readable.get('unit', 's')}"
                overall_stats.append({
                    'Property': 'Overall Avg Latency', 
                    'Value': avg_display,
                    'Metric': 'All Metrics'
                })
            
            # Component breakdown
            component_breakdown = summary.get('component_breakdown', {})
            for comp, count in component_breakdown.items():
                if comp != 'unknown' or count > 0:
                    overall_stats.append({
                        'Property': f'{comp.title()} Metrics',
                        'Value': str(count),
                        'Metric': 'Count'
                    })
            
            return {
                'latency_overview': latency_overview,
                'overall_stats': overall_stats
            }
            
        except Exception as e:
            logger.error(f"Failed to extract latency summary metrics: {e}")
            return {'latency_overview': []}

    def _extract_performance_insights(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract performance insights"""
        perf_analysis = data.get('performance_analysis', {})
        perf_summary = perf_analysis.get('performance_summary', {})
        
        # Performance summary
        summary_data = []
        summary_data.append({
            'Metric': 'Overall Score',
            'Value': f"{perf_summary.get('overall_score', 0)}/100"
        })
        summary_data.append({
            'Metric': 'Performance Grade',
            'Value': perf_summary.get('performance_grade', 'D')
        })
        
        component_scores = perf_summary.get('component_scores', {})
        for component, score in component_scores.items():
            summary_data.append({
                'Metric': component.replace('_', ' ').title(),
                'Value': f"{score}/100"
            })

        # Key findings and recommendations
        findings = []
        key_findings = perf_analysis.get('key_findings', [])
        recommendations = perf_analysis.get('recommendations', [])
        
        for idx, finding in enumerate(key_findings[:5], 1):
            findings.append({
                'Type': 'Finding',
                'Description': finding
            })
        
        for idx, rec in enumerate(recommendations[:5], 1):
            findings.append({
                'Type': 'Recommendation',
                'Description': rec
            })

        return {
            'performance_summary': summary_data,
            'insights': findings
        }

    def _extract_node_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract node analysis data"""
        nodes_usage = data.get('nodes_usage', {})
        node_summary = []
        
        # Control plane nodes
        cp_nodes = nodes_usage.get('controlplane_nodes', {})
        cp_summary = cp_nodes.get('summary', {})
        if cp_summary:
            cpu_max = cp_summary.get('cpu_usage', {}).get('max', 0)
            mem_max = cp_summary.get('memory_usage', {}).get('max', 0)
            status = 'warning' if cpu_max > 70 else 'success'
            
            node_summary.append({
                'Node Type': 'Control Plane',
                'Count': cp_nodes.get('count', 0),
                'Max CPU %': f"{cpu_max:.1f}",
                'Max Memory MB': f"{mem_max:.0f}",
                'Status': status
            })

        # Worker nodes
        worker_nodes = nodes_usage.get('top5_worker_nodes', {})
        worker_summary = worker_nodes.get('summary', {})
        if worker_summary:
            cpu_max = worker_summary.get('cpu_usage', {}).get('max', 0)
            mem_max = worker_summary.get('memory_usage', {}).get('max', 0)
            status = 'danger' if cpu_max > 80 else 'warning' if cpu_max > 70 else 'success'
            
            highlight = "🔥 Worker" if cpu_max > 80 else "Worker"
            
            node_summary.append({
                'Node Type': highlight,
                'Count': worker_nodes.get('count', 0),
                'Max CPU %': f"{cpu_max:.1f}",
                'Max Memory MB': f"{mem_max:.0f}",
                'Status': status
            })

        # Individual worker nodes
        individual_workers = []
        worker_nodes_list = worker_nodes.get('individual_nodes', [])
        for node in worker_nodes_list[:5]:
            cpu_max = node.get('cpu_usage', {}).get('max', 0)
            rank = node.get('rank', 0)
            
            rank_display = f"🏆 {rank}" if rank == 1 else rank
            
            individual_workers.append({
                'Rank': rank_display,
                'Node': self.truncate_node_name(node.get('name', ''), 25),
                'CPU %': f"{cpu_max:.1f}",
                'Memory MB': f"{node.get('memory_usage', {}).get('max', 0):.0f}"
            })

        return {
            'node_summary': node_summary,
            'top_worker_nodes': individual_workers
        }
 
    def _extract_ovs_summary(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVS metrics summary with separate tables for each component"""
        ovs_data = data.get('ovs_metrics', {})
        
        # CPU usage tables
        ovs_vswitchd_cpu = []
        ovsdb_server_cpu = []
        
        # OVS vSwitchd CPU
        vswitchd_cpu_data = ovs_data.get('cpu_usage', {}).get('ovs_vswitchd_top5', [])
        for idx, node_data in enumerate(vswitchd_cpu_data, 1):
            avg_cpu = node_data.get('avg', 0)
            max_cpu = node_data.get('max', 0)
            
            # Highlight top performance and high usage
            rank_display = f"🏆 {idx}" if idx == 1 else idx
            if avg_cpu > 2:
                rank_display = f"🔥 {idx}" if idx == 1 else f"⚠️ {idx}"
            
            ovs_vswitchd_cpu.append({
                'Rank': rank_display,
                'Node': self.truncate_node_name(node_data.get('node_name', ''), 25),
                'Avg CPU %': f"{avg_cpu:.2f}",
                'Max CPU %': f"{max_cpu:.2f}"
            })
        
        # OVSDB Server CPU
        ovsdb_cpu_data = ovs_data.get('cpu_usage', {}).get('ovsdb_server_top5', [])
        for idx, node_data in enumerate(ovsdb_cpu_data, 1):
            avg_cpu = node_data.get('avg', 0)
            max_cpu = node_data.get('max', 0)
            
            rank_display = f"🏆 {idx}" if idx == 1 else idx
            if avg_cpu > 0.15:
                rank_display = f"🔥 {idx}" if idx == 1 else f"⚠️ {idx}"
            
            ovsdb_server_cpu.append({
                'Rank': rank_display,
                'Node': self.truncate_node_name(node_data.get('node_name', ''), 25),
                'Avg CPU %': f"{avg_cpu:.2f}",
                'Max CPU %': f"{max_cpu:.2f}"
            })
        
        # Memory usage tables
        ovs_db_memory = []
        ovs_vswitchd_memory = []
        
        # OVS DB Memory
        ovs_db_mem_data = ovs_data.get('memory_usage', {}).get('ovs_db_top5', [])
        for idx, pod_data in enumerate(ovs_db_mem_data, 1):
            avg_mem = pod_data.get('avg', 0)
            max_mem = pod_data.get('max', 0)
            
            rank_display = f"🏆 {idx}" if idx == 1 else idx
            if avg_mem > 15:  # > 15MB
                rank_display = f"🔥 {idx}" if idx == 1 else f"⚠️ {idx}"
            
            ovs_db_memory.append({
                'Rank': rank_display,
                'Pod': self.truncate_text(pod_data.get('pod_name', ''), 25),
                'Avg Memory MB': f"{avg_mem:.1f}",
                'Max Memory MB': f"{max_mem:.1f}"
            })
        
        # OVS vSwitchd Memory
        ovs_vswitchd_mem_data = ovs_data.get('memory_usage', {}).get('ovs_vswitchd_top5', [])
        for idx, pod_data in enumerate(ovs_vswitchd_mem_data, 1):
            avg_mem = pod_data.get('avg', 0)
            max_mem = pod_data.get('max', 0)
            
            rank_display = f"🏆 {idx}" if idx == 1 else idx
            if avg_mem > 60:  # > 60MB
                rank_display = f"🔥 {idx}" if idx == 1 else f"⚠️ {idx}"
            
            ovs_vswitchd_memory.append({
                'Rank': rank_display,
                'Pod': self.truncate_text(pod_data.get('pod_name', ''), 25),
                'Avg Memory MB': f"{avg_mem:.1f}",
                'Max Memory MB': f"{max_mem:.1f}"
            })
        
        # Flow metrics tables
        dp_flows_table = []
        br_int_flows_table = []
        br_ex_flows_table = []
        
        flows_data = ovs_data.get('flows_metrics', {})
        
        # DP Flows
        dp_flows_data = flows_data.get('dp_flows_top5', [])
        for idx, flow_data in enumerate(dp_flows_data, 1):
            avg_flows = flow_data.get('avg', 0)
            max_flows = flow_data.get('max', 0)
            
            rank_display = f"🏆 {idx}" if idx == 1 else idx
            if avg_flows > 500:
                rank_display = f"🔥 {idx}" if idx == 1 else f"⚠️ {idx}"
            
            dp_flows_table.append({
                'Rank': rank_display,
                'Instance': flow_data.get('instance', ''),
                'Avg Flows': f"{avg_flows:.0f}",
                'Max Flows': f"{max_flows:.0f}"
            })
        
        # BR-INT Flows
        br_int_data = flows_data.get('br_int_top5', [])
        for idx, flow_data in enumerate(br_int_data, 1):
            avg_flows = flow_data.get('avg', 0)
            max_flows = flow_data.get('max', 0)
            
            rank_display = f"🏆 {idx}" if idx == 1 else idx
            if avg_flows > 4000:
                rank_display = f"🔥 {idx}" if idx == 1 else f"⚠️ {idx}"
            
            br_int_flows_table.append({
                'Rank': rank_display,
                'Instance': flow_data.get('instance', ''),
                'Avg Flows': f"{avg_flows:.0f}",
                'Max Flows': f"{max_flows:.0f}"
            })
        
        # BR-EX Flows
        br_ex_data = flows_data.get('br_ex_top5', [])
        for idx, flow_data in enumerate(br_ex_data, 1):
            avg_flows = flow_data.get('avg', 0)
            max_flows = flow_data.get('max', 0)
            
            rank_display = f"🏆 {idx}" if idx == 1 else idx
            
            br_ex_flows_table.append({
                'Rank': rank_display,
                'Instance': flow_data.get('instance', ''),
                'Avg Flows': f"{avg_flows:.0f}",
                'Max Flows': f"{max_flows:.0f}"
            })
        
        # Connection metrics table
        connection_metrics_table = []
        conn_data = ovs_data.get('connection_metrics', {})
        
        for metric_name, metric_data in conn_data.items():
            if isinstance(metric_data, dict):
                avg_val = metric_data.get('avg', 0)
                max_val = metric_data.get('max', 0)
                
                # Highlight problematic connections
                status = 'success'
                if metric_name in ['rconn_overflow', 'rconn_discarded'] and max_val > 0:
                    status = 'danger'
                elif metric_name == 'stream_open' and avg_val < 2:
                    status = 'warning'
                
                display_name = metric_name.replace('_', ' ').title()
                
                connection_metrics_table.append({
                    'Metric': display_name,
                    'Avg Value': f"{avg_val:.0f}",
                    'Max Value': f"{max_val:.0f}",
                    'Status': status
                })

        return {
            'ovs_vswitchd_cpu': ovs_vswitchd_cpu,
            'ovsdb_server_cpu': ovsdb_server_cpu,
            'ovs_db_memory': ovs_db_memory,
            'ovs_vswitchd_memory': ovs_vswitchd_memory,
            'dp_flows': dp_flows_table,
            'br_int_flows': br_int_flows_table,
            'br_ex_flows': br_ex_flows_table,
            'connection_metrics': connection_metrics_table
        }

    def summarize_deepdrive_data(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary for deep drive analysis data"""
        try:
            if not isinstance(structured_data, dict) or not structured_data:
                return "No data available for deep drive summary"
            summary_parts = []
            
            # Basic cluster info
            metadata = structured_data.get('metadata') or {}
            if metadata:
                analysis_type = metadata.get('analysis_type', 'unknown')
                duration = metadata.get('query_duration', 'unknown')
                components = metadata.get('components_analyzed', 0)
                summary_parts.append(f"Comprehensive {analysis_type.replace('_', ' ')} analysis over {duration} covering {components} components")
            
            # Performance analysis summary
            latency_analysis = structured_data.get('latency_analysis') or {}
            performance_summary = latency_analysis.get('performance_summary', [])
            
            # Extract performance grade and overall score
            grade = 'N/A'
            overall_score = 'N/A'
            for item in performance_summary:
                if item.get('Property') == 'Performance Grade':
                    grade = item.get('Value', 'N/A')
                elif item.get('Property') == 'Overall Score':
                    overall_score = item.get('Value', 'N/A')
            
            if grade != 'N/A' and overall_score != 'N/A':
                summary_parts.append(f"Overall performance score: {overall_score} (Grade: {grade})")
            
            # Latency health status
            latency_summary = latency_analysis.get('latency_summary', [])
            for item in latency_summary:
                if item.get('Property') == 'Overall Latency Health':
                    health_status = item.get('Value', 'unknown')
                    if 'CRITICAL' in health_status:
                        summary_parts.append(f"CRITICAL latency issues detected requiring immediate attention")
                    break
            
            # Top critical latency issues
            node_ready = latency_analysis.get('node_ready_duration', [])
            if node_ready:
                top_issue = node_ready[0]
                value = top_issue.get('Value', '0s')
                summary_parts.append(f"Highest node ready latency: {value} on {top_issue.get('Node Name', 'unknown')}")
            
            # Resource usage highlights
            resource_usage = structured_data.get('resource_usage') or {}
            top_cpu_pods = resource_usage.get('top_cpu_pods', [])
            if top_cpu_pods:
                top_pod = top_cpu_pods[0]
                summary_parts.append(f"Top CPU consuming pod: {top_pod.get('Pod', 'unknown')} ({top_pod.get('CPU %', '0')}%)")

            # Node analysis
            node_analysis = structured_data.get('node_analysis') or {}
            top_workers = node_analysis.get('top_worker_nodes', [])
            if top_workers:
                top_worker = top_workers[0]
                cpu_usage = top_worker.get('CPU %', '0')
                summary_parts.append(f"Highest worker CPU usage: {cpu_usage}% on {top_worker.get('Node', 'unknown')}")
            
            # Performance insights
            perf_insights = structured_data.get('performance_insights') or {}
            perf_summary = perf_insights.get('performance_summary', [])

            return " • ".join(summary_parts) if summary_parts else "Deep drive analysis completed with comprehensive latency and performance metrics"
            
        except Exception as e:
            logger.error(f"Failed to generate deep drive summary: {e}")
            return f"Deep drive analysis summary generation failed: {str(e)}"

    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data to DataFrames - UPDATED"""
        try:
            if not isinstance(structured_data, dict) or not structured_data:
                return {}
            dataframes = {}
            
            # Existing transformations...
            # [Keep all existing code from the original function]
            
            # Analysis metadata
            metadata = structured_data.get('metadata') or {}
            if metadata:
                metadata_list = []
                for key, value in metadata.items():
                    if value:
                        metadata_list.append({
                            'Property': key.replace('_', ' ').title(),
                            'Value': str(value)
                        })
                if metadata_list:
                    df = pd.DataFrame(metadata_list)
                    dataframes['analysis_metadata'] = self.limit_dataframe_columns(df, 2, 'analysis_metadata')
            
            # Basic info tables
            basic_info = structured_data.get('basic_info') or {}
            
            # Pod status
            pod_status = basic_info.get('pod_status', [])
            if pod_status:
                df = pd.DataFrame(pod_status)
                dataframes['cluster_overview'] = self.limit_dataframe_columns(df, 2, 'cluster_overview')
            
            # Database sizes
            db_sizes = basic_info.get('database_sizes', [])
            if db_sizes:
                df = pd.DataFrame(db_sizes)
                dataframes['ovn_db_size'] = self.limit_dataframe_columns(df, 3, 'ovn_db_size')
            
            # Alerts
            alerts = basic_info.get('alerts', [])
            if alerts:
                df = pd.DataFrame(alerts)
                dataframes['alerts'] = self.limit_dataframe_columns(df, 4, 'alerts')
            
            # Resource usage tables (existing code remains same)
            resource_usage = structured_data.get('resource_usage') or {}
            
            pods_usage_detailed = resource_usage.get('pods_usage_detailed', [])
            if pods_usage_detailed:
                df = pd.DataFrame(pods_usage_detailed)
                dataframes['pods_usage_detailed'] = df

            containers_usage_detailed = resource_usage.get('containers_usage_detailed', [])
            if containers_usage_detailed:
                df = pd.DataFrame(containers_usage_detailed)
                dataframes['containers_usage_detailed'] = df

            nodes_usage_detailed = resource_usage.get('nodes_usage_detailed', [])
            if nodes_usage_detailed:
                df = pd.DataFrame(nodes_usage_detailed)
                dataframes['nodes_usage_detailed'] = df

            nodes_network_usage = resource_usage.get('nodes_network_usage', [])
            if nodes_network_usage:
                df = pd.DataFrame(nodes_network_usage)
                dataframes['nodes_network_usage'] = df
            
            # NEW: Latency summary metrics
            latency_summary_metrics = structured_data.get('latency_summary_metrics') or {}
            
            latency_overview = latency_summary_metrics.get('latency_overview', [])
            if latency_overview:
                df = pd.DataFrame(latency_overview)
                dataframes['latency_overview'] = df  # Don't limit to show all columns
            
            overall_stats = latency_summary_metrics.get('overall_stats', [])
            if overall_stats:
                df = pd.DataFrame(overall_stats)
                dataframes['latency_overall_stats'] = self.limit_dataframe_columns(df, 3, 'latency_overall_stats')
            
            # Existing latency analysis tables
            latency_analysis = structured_data.get('latency_analysis') or {}
            
            # Controller ready duration
            controller_ready = latency_analysis.get('controller_ready_duration', [])
            if controller_ready:
                df = pd.DataFrame(controller_ready)
                dataframes['controller_ready_duration'] = df
            
            # Node ready duration
            node_ready = latency_analysis.get('node_ready_duration', [])
            if node_ready:
                df = pd.DataFrame(node_ready)
                dataframes['node_ready_duration'] = df
            
            # Sync duration (top 20)
            sync_duration = latency_analysis.get('sync_duration', [])
            if sync_duration:
                df = pd.DataFrame(sync_duration)
                dataframes['sync_duration'] = df
            
            # Pod latency
            pod_latency = latency_analysis.get('pod_latency', [])
            if pod_latency:
                df = pd.DataFrame(pod_latency)
                dataframes['pod_latency'] = df
            
            # CNI latency
            cni_latency = latency_analysis.get('cni_latency', [])
            if cni_latency:
                df = pd.DataFrame(cni_latency)
                dataframes['cni_latency'] = df
            
            # Service latency
            service_latency = latency_analysis.get('service_latency', [])
            if service_latency:
                df = pd.DataFrame(service_latency)
                dataframes['service_latency'] = df
            
            # Network programming latency
            network_programming = latency_analysis.get('network_programming', [])
            if network_programming:
                df = pd.DataFrame(network_programming)
                dataframes['network_programming'] = df
            
            # Latency summary
            latency_summary = latency_analysis.get('latency_summary', [])
            if latency_summary:
                df = pd.DataFrame(latency_summary)
                dataframes['latency_summary'] = self.limit_dataframe_columns(df, 2, 'latency_summary')
            
            # Performance summary
            performance_summary = latency_analysis.get('performance_summary', [])
            if performance_summary:
                df = pd.DataFrame(performance_summary)
                dataframes['performance_summary'] = self.limit_dataframe_columns(df, 2, 'performance_summary')
            
            # Findings and recommendations
            findings_recs = latency_analysis.get('findings_and_recommendations', [])
            if findings_recs:
                df = pd.DataFrame(findings_recs)
                dataframes['findings_and_recommendations'] = self.limit_dataframe_columns(df, 2, 'findings_and_recommendations')
            
            # Node analysis
            node_analysis = structured_data.get('node_analysis') or {}
            
            node_summary = node_analysis.get('node_summary', [])
            if node_summary:
                df = pd.DataFrame(node_summary)
                dataframes['node_summary'] = self.limit_dataframe_columns(df, 5, 'node_summary')
            
            # OVS metrics (existing code remains same)
            ovs_metrics = structured_data.get('ovs_metrics') or {}
            
            ovs_vswitchd_cpu = ovs_metrics.get('ovs_vswitchd_cpu', [])
            if ovs_vswitchd_cpu:
                df = pd.DataFrame(ovs_vswitchd_cpu)
                dataframes['ovs_vswitchd_cpu'] = self.limit_dataframe_columns(df, 4, 'ovs_vswitchd_cpu')
            
            ovsdb_server_cpu = ovs_metrics.get('ovsdb_server_cpu', [])
            if ovsdb_server_cpu:
                df = pd.DataFrame(ovsdb_server_cpu)
                dataframes['ovsdb_server_cpu'] = self.limit_dataframe_columns(df, 4, 'ovsdb_server_cpu')
            
            ovs_db_memory = ovs_metrics.get('ovs_db_memory', [])
            if ovs_db_memory:
                df = pd.DataFrame(ovs_db_memory)
                dataframes['ovs_db_memory'] = self.limit_dataframe_columns(df, 4, 'ovs_db_memory')
            
            ovs_vswitchd_memory = ovs_metrics.get('ovs_vswitchd_memory', [])
            if ovs_vswitchd_memory:
                df = pd.DataFrame(ovs_vswitchd_memory)
                dataframes['ovs_vswitchd_memory'] = self.limit_dataframe_columns(df, 4, 'ovs_vswitchd_memory')
            
            dp_flows = ovs_metrics.get('dp_flows', [])
            if dp_flows:
                df = pd.DataFrame(dp_flows)
                dataframes['ovs_dp_flows'] = self.limit_dataframe_columns(df, 4, 'ovs_dp_flows')
            
            br_int_flows = ovs_metrics.get('br_int_flows', [])
            if br_int_flows:
                df = pd.DataFrame(br_int_flows)
                dataframes['ovs_br_int_flows'] = self.limit_dataframe_columns(df, 4, 'ovs_br_int_flows')
            
            br_ex_flows = ovs_metrics.get('br_ex_flows', [])
            if br_ex_flows:
                df = pd.DataFrame(br_ex_flows)
                dataframes['ovs_br_ex_flows'] = self.limit_dataframe_columns(df, 4, 'ovs_br_ex_flows')
            
            connection_metrics = ovs_metrics.get('connection_metrics', [])
            if connection_metrics:
                df = pd.DataFrame(connection_metrics)
                dataframes['ovs_connection_metrics'] = self.limit_dataframe_columns(df, 4, 'ovs_connection_metrics')
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform deep drive data to DataFrames: {e}")
            return {}

    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables with enhanced styling for deep drive analysis - UPDATED"""
        try:
            html_tables = {}
            
            # Define table priorities with new latency summary tables first
            table_priorities = {
                'analysis_metadata': 0,
                'cluster_overview': 1,
                'node_summary': 2,
                'nodes_usage_detailed': 3,
                'nodes_network_usage': 4,
                'ovn_db_size': 5,
                'pods_usage_detailed': 6,
                'containers_usage_detailed': 7,
                'ovs_vswitchd_cpu': 8,
                'ovsdb_server_cpu': 9,
                'ovs_db_memory': 10,
                'ovs_vswitchd_memory': 11,
                'ovs_dp_flows': 12,
                'ovs_br_int_flows': 13,
                'ovs_br_ex_flows': 14,
                'ovs_connection_metrics': 15,                
                'latency_summary': 16,
                'latency_overall_stats': 17,  # NEW: Overall latency statistics                        
                'latency_overview': 18,  # NEW: Priority placement for new summary table  
                'controller_ready_duration': 19,
                'node_ready_duration': 20,
                'sync_duration': 21,
                'pod_latency': 22,
                'cni_latency': 23,
                'service_latency': 24,
                'network_programming': 25,
                'alerts': 26,
                'findings_and_recommendations': 27,
                'performance_summary': 28             
            }
            
            # Sort tables by priority
            sorted_tables = sorted(
                dataframes.items(),
                key=lambda x: table_priorities.get(x[0], 999)
            )
            
            for table_name, df in sorted_tables:
                if df.empty:
                    continue
                
                styled_df = df.copy()
                
                # NEW: Latency overview table highlighting
                if table_name == 'latency_overview':
                    for idx, row in styled_df.iterrows():
                        # Highlight critical latencies
                        max_latency = str(row.get('Max Latency', ''))
                        if 's' in max_latency:  # seconds
                            try:
                                value = float(max_latency.split()[0])
                                if value > 3.0:
                                    styled_df.at[idx, 'Max Latency'] = f'<span class="text-danger font-weight-bold">{max_latency}</span>'
                                elif value > 1.0:
                                    styled_df.at[idx, 'Max Latency'] = f'<span class="text-warning font-weight-bold">{max_latency}</span>'
                            except (ValueError, IndexError):
                                pass
                        
                        # Highlight component
                        component = str(row.get('Component', ''))
                        if component.lower() == 'controller':
                            styled_df.at[idx, 'Component'] = f'<span class="badge badge-primary">{component}</span>'
                        elif component.lower() == 'node':
                            styled_df.at[idx, 'Component'] = f'<span class="badge badge-success">{component}</span>'
                
                # NEW: Overall stats highlighting
                elif table_name == 'latency_overall_stats':
                    for idx, row in styled_df.iterrows():
                        prop = str(row.get('Property', ''))
                        value = str(row.get('Value', ''))
                        
                        if 'Overall Max Latency' in prop and 's' in value:
                            try:
                                val = float(value.split()[0])
                                if val > 3.0:
                                    styled_df.at[idx, 'Value'] = f'<span class="text-danger font-weight-bold">{value}</span>'
                                elif val > 1.0:
                                    styled_df.at[idx, 'Value'] = f'<span class="text-warning font-weight-bold">{value}</span>'
                            except (ValueError, IndexError):
                                pass
                
                # Existing highlighting code for other tables...
                # [Keep all existing table highlighting logic from the original function]
                
                # Latency tables highlighting
                elif table_name == 'controller_ready_duration':
                    for idx, row in styled_df.iterrows():
                        value_str = str(row.get('Value', '0s'))
                        try:
                            value_float = float(value_str.replace('s', ''))
                            if value_float > 0.8:
                                styled_df.at[idx, 'Value'] = f'<span class="text-warning font-weight-bold">{value_str}</span>'
                        except (ValueError, TypeError):
                            pass
                
                elif table_name == 'node_ready_duration':
                    for idx, row in styled_df.iterrows():
                        value_str = str(row.get('Value', '0s'))
                        try:
                            value_float = float(value_str.replace('s', ''))
                            if value_float > 3.0:
                                styled_df.at[idx, 'Value'] = f'<span class="text-danger font-weight-bold">{value_str}</span>'
                            elif value_float > 2.0:
                                styled_df.at[idx, 'Value'] = f'<span class="text-warning font-weight-bold">{value_str}</span>'
                        except (ValueError, TypeError):
                            pass
                
                elif table_name == 'pod_latency':
                    for idx, row in styled_df.iterrows():
                        value_str = str(row.get('Value', '0s'))
                        try:
                            value_float = float(value_str.replace('s', ''))
                            if value_float > 1.0:
                                styled_df.at[idx, 'Value'] = f'<span class="text-warning font-weight-bold">{value_str}</span>'
                        except (ValueError, TypeError):
                            pass
                
                elif table_name == 'latency_summary':
                    for idx, row in styled_df.iterrows():
                        prop = str(row.get('Property', ''))
                        value = str(row.get('Value', ''))
                        if 'Overall Latency Health' in prop and 'CRITICAL' in value:
                            styled_df.at[idx, 'Value'] = f'<span class="badge badge-danger">{value}</span>'
                        elif 'Critical Issues Count' in prop and value != '0':
                            styled_df.at[idx, 'Value'] = f'<span class="text-danger font-weight-bold">{value}</span>'
                
                elif table_name == 'performance_summary':
                    for idx, row in styled_df.iterrows():
                        prop = str(row.get('Property', ''))
                        value = str(row.get('Value', ''))
                        if 'Performance Grade' in prop:
                            if value in ['D', 'F']:
                                styled_df.at[idx, 'Value'] = f'<span class="badge badge-danger">{value}</span>'
                            elif value == 'C':
                                styled_df.at[idx, 'Value'] = f'<span class="badge badge-warning">{value}</span>'
                            elif value in ['A', 'B']:
                                styled_df.at[idx, 'Value'] = f'<span class="badge badge-success">{value}</span>'
                
                elif table_name == 'findings_and_recommendations':
                    for idx, row in styled_df.iterrows():
                        desc = str(row.get('Description', ''))
                        if 'URGENT' in desc or 'critical' in desc.lower():
                            styled_df.at[idx, 'Description'] = f'<span class="text-danger">{desc}</span>'
                        elif 'warning' in desc.lower():
                            styled_df.at[idx, 'Description'] = f'<span class="text-warning">{desc}</span>'
                
                # Apply existing styling for other tables (OVS, nodes, etc.)
                elif table_name == 'ovs_vswitchd_cpu':
                    for idx, row in styled_df.iterrows():
                        avg_cpu_str = str(row.get('Avg CPU %', '0'))
                        try:
                            avg_cpu = float(avg_cpu_str)
                            if avg_cpu > 2:
                                styled_df.at[idx, 'Avg CPU %'] = f'<span class="text-danger font-weight-bold">{avg_cpu_str}</span>'
                            elif avg_cpu > 1:
                                styled_df.at[idx, 'Avg CPU %'] = f'<span class="text-warning font-weight-bold">{avg_cpu_str}</span>'
                        except (ValueError, TypeError):
                            pass
                
                elif table_name == 'alerts':
                    for idx, row in styled_df.iterrows():
                        severity = str(row.get('Severity', ''))
                        if 'CRITICAL' in severity:
                            styled_df.at[idx, 'Severity'] = f'<span class="badge badge-danger">{severity}</span>'
                        elif 'WARNING' in severity:
                            styled_df.at[idx, 'Severity'] = f'<span class="badge badge-warning">{severity}</span>'
                        else:
                            styled_df.at[idx, 'Severity'] = f'<span class="badge badge-info">{severity}</span>'
                
                elif table_name == 'nodes_usage_detailed':
                    for idx, row in styled_df.iterrows():
                        max_cpu = row.get('Max CPU %', 0)
                        if isinstance(max_cpu, (int, float)) and max_cpu > 70:
                            styled_df.at[idx, 'Max CPU %'] = f'<span class="text-danger font-weight-bold">{max_cpu}%</span>'
                        elif isinstance(max_cpu, (int, float)) and max_cpu > 50:
                            styled_df.at[idx, 'Max CPU %'] = f'<span class="text-warning font-weight-bold">{max_cpu}%</span>'
                
                elif table_name == 'nodes_network_usage':
                    for idx, row in styled_df.iterrows():
                        max_rx_str = str(row.get('Max Network RX', '0 KB/s'))
                        try:
                            max_rx_val = float(max_rx_str.split()[0])
                            if max_rx_val > 1000:
                                styled_df.at[idx, 'Max Network RX'] = f'<span class="text-danger font-weight-bold">{max_rx_str}</span>'
                            elif max_rx_val > 500:
                                styled_df.at[idx, 'Max Network RX'] = f'<span class="text-warning font-weight-bold">{max_rx_str}</span>'
                        except (ValueError, IndexError):
                            pass
                
                # Generate HTML table
                html_table = self.create_html_table(styled_df, table_name)
                
                # Add custom styling for important tables
                if table_name in ['latency_overview', 'latency_overall_stats']:  # NEW: Highlight new latency tables
                    html_table = f'<div class="border border-success rounded p-2 mb-3">{html_table}</div>'
                elif table_name in ['performance_summary', 'latency_summary', 'alerts', 'findings_and_recommendations']:
                    html_table = f'<div class="border border-primary rounded p-2 mb-3">{html_table}</div>'
                elif table_name in ['node_ready_duration', 'controller_ready_duration', 'pod_latency']:
                    html_table = f'<div class="border border-warning rounded p-2 mb-3">{html_table}</div>'
                
                html_tables[table_name] = html_table
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Failed to generate deep drive HTML tables: {e}")
            return {}