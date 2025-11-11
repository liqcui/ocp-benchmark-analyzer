"""
Extract, Load, Transform module for Kubelet CNI Metrics
Handles CNI and CRIO metrics from tools/ovnk/ovnk_kubelet_cni.py
ONLY contains kubelet_cni specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class kubeletCNIELT(utilityELT):
    """Extract, Load, Transform class for Kubelet CNI metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'cni_cpu_usage': {
                'title': 'CNI CPU Usage',
                'unit': 'percent',
                'thresholds': {'critical': 80.0, 'warning': 60.0}
            },
            'crio_cpu_usage': {
                'title': 'CRIO CPU Usage',
                'unit': 'percent',
                'thresholds': {'critical': 80.0, 'warning': 60.0}
            },
            'cni_memory_usage': {
                'title': 'CNI Memory Usage',
                'unit': 'bytes',
                'thresholds': {'critical': 512.0, 'warning': 256.0}  # MB
            },
            'crio_memory_usage': {
                'title': 'CRIO Memory Usage',
                'unit': 'bytes',
                'thresholds': {'critical': 512.0, 'warning': 256.0}  # MB
            },
            'cni_network_usage': {
                'title': 'CNI Network Usage',
                'unit': 'bytes',
                'thresholds': {'critical': 1048576.0, 'warning': 524288.0}  # bytes
            },
            'crio_network_usage': {
                'title': 'CRIO Network Usage',
                'unit': 'bytes',
                'thresholds': {'critical': 1048576.0, 'warning': 524288.0}
            },
            'cni_network_drop': {
                'title': 'CNI Network Drops',
                'unit': 'packets',
                'thresholds': {'critical': 100.0, 'warning': 50.0}
            },
            'crio_network_drop': {
                'title': 'CRIO Network Drops',
                'unit': 'packets',
                'thresholds': {'critical': 100.0, 'warning': 50.0}
            },
            'cni_network_error': {
                'title': 'CNI Network Errors',
                'unit': 'packets',
                'thresholds': {'critical': 50.0, 'warning': 25.0}
            },
            'crio_network_error': {
                'title': 'CRIO Network Errors',
                'unit': 'packets',
                'thresholds': {'critical': 50.0, 'warning': 25.0}
            },
            'cni_network_utilization': {
                'title': 'CNI Network Utilization',
                'unit': 'bytes',
                'thresholds': {'critical': 1048576.0, 'warning': 524288.0}
            },
            'crio_network_utilization': {
                'title': 'CRIO Network Utilization',
                'unit': 'bytes',
                'thresholds': {'critical': 1048576.0, 'warning': 524288.0}
            },
            'container_threads': {
                'title': 'Container Threads',
                'unit': 'threads',
                'thresholds': {'critical': 2000.0, 'warning': 1500.0}
            },
            'control_plane_threads': {
                'title': 'Control Plane Threads',
                'unit': 'threads',
                'thresholds': {'critical': 2000.0, 'warning': 1500.0}
            },
            'worker_io_read_iops': {
                'title': 'Worker Read IOPS',
                'unit': 'iops',
                'thresholds': {'critical': 50.0, 'warning': 30.0}
            },
            'worker_io_write_iops': {
                'title': 'Worker Write IOPS',
                'unit': 'iops',
                'thresholds': {'critical': 50.0, 'warning': 30.0}
            },
            'control_plane_io_read_iops': {
                'title': 'Control Plane Read IOPS',
                'unit': 'iops',
                'thresholds': {'critical': 50.0, 'warning': 30.0}
            },
            'control_plane_io_write_iops': {
                'title': 'Control Plane Write IOPS',
                'unit': 'iops',
                'thresholds': {'critical': 50.0, 'warning': 30.0}
            }
        }
    
    def extract_kubelet_cni(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract Kubelet CNI information from ovnk_kubelet_cni.py output"""
        
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'kubelet_cni_overview': [],
        }
        
        # Initialize all role-based tables for all metrics
        for metric_name in self.metric_configs.keys():
            for role in ['controlplane', 'infra', 'worker', 'workload']:
                structured[f'{metric_name}_{role}'] = []
        
        metrics = actual_data.get('metrics', {})
        
        # Process each metric
        for metric_name, metric_data in metrics.items():
            if metric_data.get('status') != 'success':
                continue
            
            metric_config = self.metric_configs.get(metric_name)
            if not metric_config:
                continue
            
            # Extract based on metric type
            if 'cpu' in metric_name:
                self._extract_cpu_metric(metric_name, metric_data, structured)
            elif 'memory' in metric_name:
                self._extract_memory_metric(metric_name, metric_data, structured)
            elif 'network' in metric_name:
                self._extract_network_metric(metric_name, metric_data, structured)
            elif 'threads' in metric_name:
                self._extract_threads_metric(metric_name, metric_data, structured)
            elif 'iops' in metric_name:
                self._extract_iops_metric(metric_name, metric_data, structured)
        
        # Generate overview
        self._generate_overview(actual_data, structured)
        
        return structured
    
    def _extract_cpu_metric(self, metric_name: str, metric_data: Dict[str, Any], 
                           structured: Dict[str, Any]):
        """Extract CPU usage metrics"""
        node_metrics_by_role = metric_data.get('node_metrics_by_role', {})
        thresholds = self.metric_configs[metric_name]['thresholds']
        
        # Collect all values for top identification
        all_values = []
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            nodes = node_metrics_by_role.get(role, {})
            for node_name, node_data in nodes.items():
                max_val = node_data.get('max_percent', 0)
                all_values.append((role, node_name, node_data, max_val))
        
        # Find top 1
        top_max = max((v[3] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        for role, node_name, node_data, max_val in all_values:
            table_key = f'{metric_name}_{role}'
            is_top = (max_val == top_max and max_val > 0)
            
            avg_percent = node_data.get('avg_percent', 0)
            max_percent = node_data.get('max_percent', 0)
            min_percent = node_data.get('min_percent', 0)
            latest_percent = node_data.get('latest_percent', 0)
            
            # Format with highlighting
            avg_display = self._format_percent_with_threshold(avg_percent, thresholds, False)
            max_display = self._format_percent_with_threshold(max_percent, thresholds, is_top)
            min_display = f"{min_percent:.2f}%"
            latest_display = f"{latest_percent:.2f}%" if latest_percent is not None else 'N/A'
            
            structured[table_key].append({
                'Node': self.truncate_node_name(node_name),
                'Avg CPU': avg_display,
                'Max CPU': max_display,
                'Min CPU': min_display,
                'Latest CPU': latest_display,
                'Data Points': node_data.get('data_points', 0)
            })
    
    def _extract_memory_metric(self, metric_name: str, metric_data: Dict[str, Any], 
                               structured: Dict[str, Any]):
        """Extract memory usage metrics"""
        node_metrics_by_role = metric_data.get('node_metrics_by_role', {})
        thresholds = self.metric_configs[metric_name]['thresholds']
        
        # Collect all values for top identification
        all_values = []
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            nodes = node_metrics_by_role.get(role, {})
            for node_name, node_data in nodes.items():
                max_mb = node_data.get('max_mb', 0)
                all_values.append((role, node_name, node_data, max_mb))
        
        # Find top 1
        top_max = max((v[3] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        for role, node_name, node_data, max_mb in all_values:
            table_key = f'{metric_name}_{role}'
            is_top = (max_mb == top_max and max_mb > 0)
            
            avg_mb = node_data.get('avg_mb', 0)
            max_mb = node_data.get('max_mb', 0)
            
            # Format with highlighting
            avg_display = self._format_memory_mb_with_threshold(avg_mb, thresholds, False)
            max_display = self._format_memory_mb_with_threshold(max_mb, thresholds, is_top)
            
            structured[table_key].append({
                'Node': self.truncate_node_name(node_name),
                'Avg Memory': avg_display,
                'Max Memory': max_display,
                'Data Points': node_data.get('data_points', 0)
            })
    
    def _extract_network_metric(self, metric_name: str, metric_data: Dict[str, Any], 
                                structured: Dict[str, Any]):
        """Extract network metrics (usage, drops, errors, utilization)"""
        node_metrics_by_role = metric_data.get('node_metrics_by_role', {})
        thresholds = self.metric_configs[metric_name]['thresholds']
        unit = self.metric_configs[metric_name]['unit']
        
        # Collect all values for top identification
        all_values = []
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            nodes = node_metrics_by_role.get(role, {})
            for node_name, node_data in nodes.items():
                if unit == 'bytes':
                    max_val = node_data.get('max_mb', 0)
                elif unit == 'packets':
                    max_val = node_data.get('max_packets', 0)
                else:
                    max_val = 0
                all_values.append((role, node_name, node_data, max_val))
        
        # Find top 1
        top_max = max((v[3] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        for role, node_name, node_data, max_val in all_values:
            table_key = f'{metric_name}_{role}'
            is_top = (max_val == top_max and max_val > 0)
            
            if unit == 'bytes':
                avg_val = node_data.get('avg_mb', 0)
                max_val = node_data.get('max_mb', 0)
                
                avg_display = self._format_network_bytes_with_threshold(avg_val, thresholds, False)
                max_display = self._format_network_bytes_with_threshold(max_val, thresholds, is_top)
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Avg Usage': avg_display,
                    'Max Usage': max_display,
                    'Data Points': node_data.get('data_points', 0)
                })
            elif unit == 'packets':
                avg_packets = node_data.get('avg_packets', 0)
                max_packets = node_data.get('max_packets', 0)
                
                avg_display = self._format_packets_with_threshold(avg_packets, thresholds, False)
                max_display = self._format_packets_with_threshold(max_packets, thresholds, is_top)
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Avg Packets': avg_display,
                    'Max Packets': max_display,
                    'Data Points': node_data.get('data_points', 0)
                })
    
    def _extract_threads_metric(self, metric_name: str, metric_data: Dict[str, Any], 
                                structured: Dict[str, Any]):
        """Extract thread count metrics"""
        node_metrics_by_role = metric_data.get('node_metrics_by_role', {})
        thresholds = self.metric_configs[metric_name]['thresholds']
        
        # Collect all values for top identification
        all_values = []
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            nodes = node_metrics_by_role.get(role, {})
            for node_name, node_data in nodes.items():
                max_threads = node_data.get('max_threads', 0)
                all_values.append((role, node_name, node_data, max_threads))
        
        # Find top 1
        top_max = max((v[3] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        for role, node_name, node_data, max_threads in all_values:
            table_key = f'{metric_name}_{role}'
            is_top = (max_threads == top_max and max_threads > 0)
            
            avg_threads = node_data.get('avg_threads', 0)
            max_threads = node_data.get('max_threads', 0)
            min_threads = node_data.get('min_threads', 0)
            
            avg_display = self._format_threads_with_threshold(avg_threads, thresholds, False)
            max_display = self._format_threads_with_threshold(max_threads, thresholds, is_top)
            
            structured[table_key].append({
                'Node': self.truncate_node_name(node_name),
                'Avg Threads': avg_display,
                'Max Threads': max_display,
                'Min Threads': int(min_threads),
                'Data Points': node_data.get('data_points', 0)
            })
    
    def _extract_iops_metric(self, metric_name: str, metric_data: Dict[str, Any], 
                            structured: Dict[str, Any]):
        """Extract IOPS metrics"""
        node_metrics_by_role = metric_data.get('node_metrics_by_role', {})
        thresholds = self.metric_configs[metric_name]['thresholds']
        
        # Collect all values for top identification
        all_values = []
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            nodes = node_metrics_by_role.get(role, {})
            for node_name, node_data in nodes.items():
                max_iops = node_data.get('max_iops', 0)
                all_values.append((role, node_name, node_data, max_iops))
        
        # Find top 1
        top_max = max((v[3] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        for role, node_name, node_data, max_iops in all_values:
            table_key = f'{metric_name}_{role}'
            is_top = (max_iops == top_max and max_iops > 0)
            
            avg_iops = node_data.get('avg_iops', 0)
            max_iops = node_data.get('max_iops', 0)
            min_iops = node_data.get('min_iops', 0)
            
            avg_display = self._format_iops_with_threshold(avg_iops, thresholds, False)
            max_display = self._format_iops_with_threshold(max_iops, thresholds, is_top)
            
            structured[table_key].append({
                'Node': self.truncate_node_name(node_name),
                'Avg IOPS': avg_display,
                'Max IOPS': max_display,
                'Min IOPS': f"{min_iops:.2f}",
                'Data Points': node_data.get('data_points', 0)
            })
    
    def _format_percent_with_threshold(self, value: float, thresholds: Dict[str, float], 
                                      is_top: bool) -> str:
        """Format percentage with threshold highlighting"""
        formatted = f"{value:.2f}%"
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
        elif value >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {formatted}</span>'
        elif value >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{formatted}</span>'
        else:
            return formatted
    
    def _format_memory_mb_with_threshold(self, value_mb: float, thresholds: Dict[str, float], 
                                        is_top: bool) -> str:
        """Format memory in MB with threshold highlighting"""
        formatted = f"{value_mb:.2f} MB"
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
        elif value_mb >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {formatted}</span>'
        elif value_mb >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{formatted}</span>'
        else:
            return formatted
    
    def _format_network_bytes_with_threshold(self, value_mb: float, thresholds: Dict[str, float], 
                                            is_top: bool) -> str:
        """Format network bytes with threshold highlighting"""
        # Convert MB threshold to bytes for comparison
        value_bytes = value_mb * 1024 * 1024
        formatted = self.format_bytes_per_second(value_bytes).replace('/s', '')
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
        elif value_bytes >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {formatted}</span>'
        elif value_bytes >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{formatted}</span>'
        else:
            return formatted
    
    def _format_packets_with_threshold(self, value: float, thresholds: Dict[str, float], 
                                      is_top: bool) -> str:
        """Format packet count with threshold highlighting"""
        formatted = f"{value:.2f}"
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
        elif value >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {formatted}</span>'
        elif value >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{formatted}</span>'
        else:
            return formatted
    
    def _format_threads_with_threshold(self, value: float, thresholds: Dict[str, float], 
                                      is_top: bool) -> str:
        """Format thread count with threshold highlighting"""
        formatted = self.format_threads(value)
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
        elif value >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {formatted}</span>'
        elif value >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{formatted}</span>'
        else:
            return formatted
    
    def _format_iops_with_threshold(self, value: float, thresholds: Dict[str, float], 
                                   is_top: bool) -> str:
        """Format IOPS with threshold highlighting"""
        formatted = self.format_iops(value)
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
        elif value >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {formatted}</span>'
        elif value >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{formatted}</span>'
        else:
            return formatted
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate kubelet CNI overview"""
        metrics = data.get('metrics', {})
        summary = data.get('summary', {})
        
        # Extract cluster health info
        cluster_health = summary.get('cluster_health', {})
        nodes_by_role = cluster_health.get('nodes_by_role', {})
        
        # Count successful metrics per role
        role_metrics = {}
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            node_count = nodes_by_role.get(role, 0)
            if node_count > 0:
                metrics_count = sum(
                    1 for metric_name, metric_data in metrics.items()
                    if metric_data.get('status') == 'success' and
                    metric_data.get('node_metrics_by_role', {}).get(role, {})
                )
                role_metrics[role] = {
                    'nodes': node_count,
                    'metrics': metrics_count
                }
        
        # Build overview
        for role, counts in role_metrics.items():
            health = summary.get('cluster_health', {}).get('overall_health', 'unknown')
            status_badge = self.create_status_badge(health, health.title())
            
            structured['kubelet_cni_overview'].append({
                'Role': role.title(),
                'Nodes': counts['nodes'],
                'Metrics Collected': counts['metrics'],
                'Status': status_badge
            })
    
    def summarize_kubelet_cni(self, data: Dict[str, Any]) -> str:
        """Generate kubelet CNI summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('kubelet_cni_overview', [])
            
            if not overview_data:
                # Fallback: synthesize from data
                roles = ['controlplane', 'infra', 'worker', 'workload']
                for role in roles:
                    role_tables = [k for k in data.keys() if k.endswith(f'_{role}')]
                    non_empty = [t for t in role_tables if isinstance(data.get(t), list) and len(data.get(t)) > 0]
                    if non_empty:
                        first_table = data.get(non_empty[0], [])
                        unique_nodes = {str(row.get('Node')) for row in first_table if isinstance(row, dict)}
                        overview_data.append({
                            'Role': role.title(),
                            'Nodes': len(unique_nodes),
                            'Metrics Collected': len(non_empty),
                            'Status': self.create_status_badge('success', 'Active')
                        })
            
            total_nodes = sum(int(item.get('Nodes', 0)) for item in overview_data)
            total_metrics = sum(int(item.get('Metrics Collected', 0)) for item in overview_data)
            
            if total_nodes > 0:
                summary_items.append(f"<li>Total Nodes: {total_nodes}</li>")
            if total_metrics > 0:
                summary_items.append(f"<li>Total Metrics: {total_metrics}</li>")
            
            # Role breakdown
            for item in overview_data:
                role = item.get('Role', 'Unknown')
                nodes = item.get('Nodes', 0)
                metrics = item.get('Metrics Collected', 0)
                if nodes > 0:
                    summary_items.append(f"<li>{role}: {nodes} nodes, {metrics} metrics</li>")
            
            return (
                "<div class=\"kubelet-cni-summary\">"
                "<h4>Kubelet CNI Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate kubelet CNI summary: {e}")
            return f"Kubelet CNI metrics collected from {len(data)} metric groups"
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        dataframes = {}
        
        try:
            for key, value in structured_data.items():
                if isinstance(value, list) and value:
                    df = pd.DataFrame(value)
                    if not df.empty:
                        # Decode unicode in object columns
                        for col in df.columns:
                            if df[col].dtype == 'object':
                                df[col] = df[col].astype(str).apply(self.decode_unicode_escapes)
                        
                        dataframes[key] = df
        
        except Exception as e:
            logger.error(f"Failed to transform kubelet CNI data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        # Define metric groups
        metric_groups = {
            'CNI CPU Usage': 'cni_cpu_usage',
            'CRIO CPU Usage': 'crio_cpu_usage',
            'CNI Memory Usage': 'cni_memory_usage',
            'CRIO Memory Usage': 'crio_memory_usage',
            'CNI Network Usage': 'cni_network_usage',
            'CRIO Network Usage': 'crio_network_usage',
            'CNI Network Drops': 'cni_network_drop',
            'CRIO Network Drops': 'crio_network_drop',
            'CNI Network Errors': 'cni_network_error',
            'CRIO Network Errors': 'crio_network_error',
            'CNI Network Utilization': 'cni_network_utilization',
            'CRIO Network Utilization': 'crio_network_utilization',
            'Container Threads': 'container_threads',
            'Control Plane Threads': 'control_plane_threads',
            'Worker Read IOPS': 'worker_io_read_iops',
            'Worker Write IOPS': 'worker_io_write_iops',
            'Control Plane Read IOPS': 'control_plane_io_read_iops',
            'Control Plane Write IOPS': 'control_plane_io_write_iops'
        }
        
        try:
            # Overview table first
            if 'kubelet_cni_overview' in dataframes and not dataframes['kubelet_cni_overview'].empty:
                html_tables['kubelet_cni_overview'] = self.create_html_table(
                    dataframes['kubelet_cni_overview'], 
                    'Kubelet CNI Overview'
                )
            
            # Generate tables for each metric group and role
            for metric_name, metric_prefix in metric_groups.items():
                for role in ['controlplane', 'infra', 'worker', 'workload']:
                    table_key = f'{metric_prefix}_{role}'
                    if table_key in dataframes and not dataframes[table_key].empty:
                        display_name = f"{metric_name} - {role.title()}"
                        html_tables[table_key] = self.create_html_table(
                            dataframes[table_key], 
                            display_name
                        )
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for kubelet CNI: {e}")
        
        return html_tables