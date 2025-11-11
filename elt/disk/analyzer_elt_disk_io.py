"""
Extract, Load, Transform module for Disk I/O Metrics
Handles disk I/O data from tools/disk/disk_io.py
ONLY contains disk_io specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class diskIOELT(utilityELT):
    """Extract, Load, Transform class for disk I/O metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'disk_io_container_disk_writes': {
                'title': 'Container Disk Writes',
                'unit': 'bytes_per_second',
                'thresholds': {'critical': 500000000, 'warning': 100000000}  # 500MB/s, 100MB/s
            },
            'disk_io_node_disk_throughput_read': {
                'title': 'Disk Read Throughput',
                'unit': 'bytes_per_second',
                'thresholds': {'critical': 1000000000, 'warning': 500000000}  # 1GB/s, 500MB/s
            },
            'disk_io_node_disk_throughput_write': {
                'title': 'Disk Write Throughput',
                'unit': 'bytes_per_second',
                'thresholds': {'critical': 1000000000, 'warning': 500000000}  # 1GB/s, 500MB/s
            },
            'disk_io_node_disk_iops_read': {
                'title': 'Disk Read IOPS',
                'unit': 'operations_per_second',
                'thresholds': {'critical': 100, 'warning': 50}
            },
            'disk_io_node_disk_iops_write': {
                'title': 'Disk Write IOPS',
                'unit': 'operations_per_second',
                'thresholds': {'critical': 100, 'warning': 50}
            },
            'disk_io_node_disk_read_time_seconds': {
                'title': 'Disk Read Latency',
                'unit': 'seconds',
                'thresholds': {'critical': 0.050, 'warning': 0.020}  # 50ms, 20ms
            },
            'disk_io_node_disk_writes_time_seconds': {
                'title': 'Disk Write Latency',
                'unit': 'seconds',
                'thresholds': {'critical': 0.050, 'warning': 0.020}  # 50ms, 20ms
            },
            'disk_io_node_disk_io_time_seconds': {
                'title': 'Disk I/O Time',
                'unit': 'seconds',
                'thresholds': {'critical': 0.100, 'warning': 0.050}  # 100ms, 50ms
            }
        }
    
    def extract_disk_io(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract disk I/O information from disk_io.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'disk_io_overview': [],
        }
        
        # Get metrics data
        metrics = actual_data.get('metrics', {})
        
        # Group nodes by role (extract from node names)
        node_roles = self._categorize_nodes_by_role(metrics)
        
        # Initialize role-based tables for each metric
        metric_types = [
            'container_disk_writes',
            'disk_throughput_read', 
            'disk_throughput_write',
            'disk_iops_read',
            'disk_iops_write',
            'disk_read_time',
            'disk_write_time',
            'disk_io_time'
        ]
        
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            for metric_type in metric_types:
                structured[f'{metric_type}_{role}'] = []
        
        # Process each metric type
        for metric_name, metric_data in metrics.items():
            if metric_data.get('status') != 'success':
                continue
            
            if metric_name == 'disk_io_container_disk_writes':
                self._extract_container_disk_writes(metric_data, structured, node_roles)
            elif metric_name == 'disk_io_node_disk_throughput_read':
                self._extract_disk_throughput_read(metric_data, structured, node_roles)
            elif metric_name == 'disk_io_node_disk_throughput_write':
                self._extract_disk_throughput_write(metric_data, structured, node_roles)
            elif metric_name == 'disk_io_node_disk_iops_read':
                self._extract_disk_iops_read(metric_data, structured, node_roles)
            elif metric_name == 'disk_io_node_disk_iops_write':
                self._extract_disk_iops_write(metric_data, structured, node_roles)
            elif metric_name == 'disk_io_node_disk_read_time_seconds':
                self._extract_disk_read_time(metric_data, structured, node_roles)
            elif metric_name == 'disk_io_node_disk_writes_time_seconds':
                self._extract_disk_write_time(metric_data, structured, node_roles)
            elif metric_name == 'disk_io_node_disk_io_time_seconds':
                self._extract_disk_io_time(metric_data, structured, node_roles)
        
        # Generate overview
        self._generate_overview(actual_data, structured, node_roles)
        
        return structured
    
    def _categorize_nodes_by_role(self, metrics: Dict[str, Any]) -> Dict[str, str]:
        """Categorize nodes by role using node labels from Kubernetes API.
        
        Queries node role from node-role.kubernetes.io/master and other role labels,
        with fallback to name-based inference if labels are unavailable.
        """
        node_roles = {}
        
        # Collect all unique node names from metrics
        for metric_name, metric_data in metrics.items():
            if 'nodes' in metric_data:
                for node_name in metric_data['nodes'].keys():
                    if node_name not in node_roles:
                        # Query role from node labels (node-role.kubernetes.io/master, etc.)
                        role = self.get_node_role_from_labels(node_name)
                        node_roles[node_name] = role
        
        return node_roles
    
    def _extract_container_disk_writes(self, metric_data: Dict[str, Any], 
                                       structured: Dict[str, Any], 
                                       node_roles: Dict[str, str]):
        """Extract container disk writes for each role"""
        nodes = metric_data.get('nodes', {})
        
        # Collect all values for top identification
        all_values = []
        for node_name, node_data in nodes.items():
            role = node_roles.get(node_name, 'workload')
            avg_val = float(node_data.get('avg', 0))
            max_val = float(node_data.get('max', 0))
            all_values.append((node_name, role, avg_val, max_val))
        
        # Find top 1 average
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows by role
        thresholds = self.metric_configs['disk_io_container_disk_writes']['thresholds']
        for node_name, role, avg_val, max_val in all_values:
            table_key = f'container_disk_writes_{role}'
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.format_and_highlight(
                avg_val, 'bytes_per_second', thresholds, is_top
            )
            max_display = self.format_bytes_per_second(max_val)
            
            structured[table_key].append({
                'Metric Name': self.metric_configs['disk_io_container_disk_writes']['title'],
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Avg Writes': avg_display,
                'Max Writes': max_display
            })
    
    def _extract_disk_throughput_read(self, metric_data: Dict[str, Any], 
                                      structured: Dict[str, Any], 
                                      node_roles: Dict[str, str]):
        """Extract disk read throughput for each role"""
        nodes = metric_data.get('nodes', {})
        
        # Collect all values
        all_values = []
        for node_name, node_data in nodes.items():
            role = node_roles.get(node_name, 'workload')
            avg_val = float(node_data.get('avg', 0))
            max_val = float(node_data.get('max', 0))
            devices = node_data.get('devices', [])
            all_values.append((node_name, role, avg_val, max_val, devices))
        
        # Find top 1 average
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows by role
        thresholds = self.metric_configs['disk_io_node_disk_throughput_read']['thresholds']
        for node_name, role, avg_val, max_val, devices in all_values:
            table_key = f'disk_throughput_read_{role}'
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.format_and_highlight(
                avg_val, 'bytes_per_second', thresholds, is_top
            )
            max_display = self.format_bytes_per_second(max_val)
            devices_str = ', '.join(devices) if devices else 'All'
            
            structured[table_key].append({
                'Metric Name': self.metric_configs['disk_io_node_disk_throughput_read']['title'],
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Devices': devices_str,
                'Avg Read': avg_display,
                'Max Read': max_display
            })
    
    def _extract_disk_throughput_write(self, metric_data: Dict[str, Any], 
                                       structured: Dict[str, Any], 
                                       node_roles: Dict[str, str]):
        """Extract disk write throughput for each role"""
        nodes = metric_data.get('nodes', {})
        
        # Collect all values
        all_values = []
        for node_name, node_data in nodes.items():
            role = node_roles.get(node_name, 'workload')
            avg_val = float(node_data.get('avg', 0))
            max_val = float(node_data.get('max', 0))
            devices = node_data.get('devices', [])
            all_values.append((node_name, role, avg_val, max_val, devices))
        
        # Find top 1 average
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows by role
        thresholds = self.metric_configs['disk_io_node_disk_throughput_write']['thresholds']
        for node_name, role, avg_val, max_val, devices in all_values:
            table_key = f'disk_throughput_write_{role}'
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.format_and_highlight(
                avg_val, 'bytes_per_second', thresholds, is_top
            )
            max_display = self.format_bytes_per_second(max_val)
            devices_str = ', '.join(devices) if devices else 'All'
            
            structured[table_key].append({
                'Metric Name': self.metric_configs['disk_io_node_disk_throughput_write']['title'],
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Devices': devices_str,
                'Avg Write': avg_display,
                'Max Write': max_display
            })
    
    def _extract_disk_iops_read(self, metric_data: Dict[str, Any], 
                                structured: Dict[str, Any], 
                                node_roles: Dict[str, str]):
        """Extract disk read IOPS for each role"""
        nodes = metric_data.get('nodes', {})
        
        # Collect all values
        all_values = []
        for node_name, node_data in nodes.items():
            role = node_roles.get(node_name, 'workload')
            avg_val = float(node_data.get('avg', 0))
            max_val = float(node_data.get('max', 0))
            devices = node_data.get('devices', [])
            all_values.append((node_name, role, avg_val, max_val, devices))
        
        # Find top 1 average
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows by role
        thresholds = self.metric_configs['disk_io_node_disk_iops_read']['thresholds']
        for node_name, role, avg_val, max_val, devices in all_values:
            table_key = f'disk_iops_read_{role}'
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.format_and_highlight(
                avg_val, 'operations_per_second', thresholds, is_top
            )
            max_display = self.format_operations_per_second(max_val)
            devices_str = ', '.join(devices) if devices else 'All'
            
            structured[table_key].append({
                'Metric Name': self.metric_configs['disk_io_node_disk_iops_read']['title'],
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Devices': devices_str,
                'Avg Read IOPS': avg_display,
                'Max Read IOPS': max_display
            })
    
    def _extract_disk_iops_write(self, metric_data: Dict[str, Any], 
                                 structured: Dict[str, Any], 
                                 node_roles: Dict[str, str]):
        """Extract disk write IOPS for each role"""
        nodes = metric_data.get('nodes', {})
        
        # Collect all values
        all_values = []
        for node_name, node_data in nodes.items():
            role = node_roles.get(node_name, 'workload')
            avg_val = float(node_data.get('avg', 0))
            max_val = float(node_data.get('max', 0))
            devices = node_data.get('devices', [])
            all_values.append((node_name, role, avg_val, max_val, devices))
        
        # Find top 1 average
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows by role
        thresholds = self.metric_configs['disk_io_node_disk_iops_write']['thresholds']
        for node_name, role, avg_val, max_val, devices in all_values:
            table_key = f'disk_iops_write_{role}'
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.format_and_highlight(
                avg_val, 'operations_per_second', thresholds, is_top
            )
            max_display = self.format_operations_per_second(max_val)
            devices_str = ', '.join(devices) if devices else 'All'
            
            structured[table_key].append({
                'Metric Name': self.metric_configs['disk_io_node_disk_iops_write']['title'],
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Devices': devices_str,
                'Avg Write IOPS': avg_display,
                'Max Write IOPS': max_display
            })
    
    def _extract_disk_read_time(self, metric_data: Dict[str, Any], 
                                structured: Dict[str, Any], 
                                node_roles: Dict[str, str]):
        """Extract disk read latency for each role"""
        nodes = metric_data.get('nodes', {})
        
        # Collect all values (convert to milliseconds)
        all_values = []
        for node_name, node_data in nodes.items():
            role = node_roles.get(node_name, 'workload')
            avg_val = float(node_data.get('avg', 0)) * 1000  # Convert to ms
            max_val = float(node_data.get('max', 0)) * 1000  # Convert to ms
            devices = node_data.get('devices', [])
            all_values.append((node_name, role, avg_val, max_val, devices))
        
        # Find top 1 average (highest latency)
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows by role
        thresholds = self.get_latency_thresholds_ms()
        for node_name, role, avg_val, max_val, devices in all_values:
            table_key = f'disk_read_time_{role}'
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.highlight_latency_value(avg_val, is_top)
            max_display = self.format_latency_ms(max_val / 1000)
            devices_str = ', '.join(devices) if devices else 'All'
            
            structured[table_key].append({
                'Metric Name': self.metric_configs['disk_io_node_disk_read_time_seconds']['title'],
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Devices': devices_str,
                'Avg Read Latency': avg_display,
                'Max Read Latency': max_display
            })
    
    def _extract_disk_write_time(self, metric_data: Dict[str, Any], 
                                 structured: Dict[str, Any], 
                                 node_roles: Dict[str, str]):
        """Extract disk write latency for each role"""
        nodes = metric_data.get('nodes', {})
        
        # Collect all values (convert to milliseconds)
        all_values = []
        for node_name, node_data in nodes.items():
            role = node_roles.get(node_name, 'workload')
            avg_val = float(node_data.get('avg', 0)) * 1000  # Convert to ms
            max_val = float(node_data.get('max', 0)) * 1000  # Convert to ms
            devices = node_data.get('devices', [])
            all_values.append((node_name, role, avg_val, max_val, devices))
        
        # Find top 1 average (highest latency)
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows by role
        thresholds = self.get_latency_thresholds_ms()
        for node_name, role, avg_val, max_val, devices in all_values:
            table_key = f'disk_write_time_{role}'
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.highlight_latency_value(avg_val, is_top)
            max_display = self.format_latency_ms(max_val / 1000)
            devices_str = ', '.join(devices) if devices else 'All'
            
            structured[table_key].append({
                'Metric Name': self.metric_configs['disk_io_node_disk_writes_time_seconds']['title'],
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Devices': devices_str,
                'Avg Write Latency': avg_display,
                'Max Write Latency': max_display
            })
    
    def _extract_disk_io_time(self, metric_data: Dict[str, Any], 
                              structured: Dict[str, Any], 
                              node_roles: Dict[str, str]):
        """Extract disk I/O time for each role"""
        nodes = metric_data.get('nodes', {})
        
        # Collect all values (convert to milliseconds)
        all_values = []
        for node_name, node_data in nodes.items():
            role = node_roles.get(node_name, 'workload')
            avg_val = float(node_data.get('avg', 0)) * 1000  # Convert to ms
            max_val = float(node_data.get('max', 0)) * 1000  # Convert to ms
            devices = node_data.get('devices', [])
            all_values.append((node_name, role, avg_val, max_val, devices))
        
        # Find top 1 average (highest I/O time)
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows by role
        thresholds = {'critical': 100.0, 'warning': 50.0}  # 100ms, 50ms
        for node_name, role, avg_val, max_val, devices in all_values:
            table_key = f'disk_io_time_{role}'
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.highlight_latency_value(avg_val, is_top)
            max_display = self.format_latency_ms(max_val / 1000)
            devices_str = ', '.join(devices) if devices else 'All'
            
            structured[table_key].append({
                'Metric Name': self.metric_configs['disk_io_node_disk_io_time_seconds']['title'],
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Devices': devices_str,
                'Avg I/O Time': avg_display,
                'Max I/O Time': max_display
            })
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any], 
                          node_roles: Dict[str, str]):
        """Generate disk I/O overview by role"""
        # Count nodes per role
        role_counts = {}
        for node_name, role in node_roles.items():
            role_counts[role] = role_counts.get(role, 0) + 1
        
        # Count metrics collected
        metrics = data.get('metrics', {})
        metrics_count = len([m for m in metrics.values() if m.get('status') == 'success'])
        
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            if role in role_counts:
                structured['disk_io_overview'].append({
                    'Role': role.title(),
                    'Nodes': role_counts[role],
                    'Metrics Collected': metrics_count,
                    'Status': self.create_status_badge('success', 'Active')
                })
    
    def summarize_disk_io(self, data: Dict[str, Any]) -> str:
        """Generate disk I/O summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('disk_io_overview', [])
            
            total_nodes = sum(int(item.get('Nodes', 0)) for item in overview_data)
            if total_nodes > 0:
                summary_items.append(f"<li>Total Nodes: {total_nodes}</li>")
            
            # Role breakdown
            for item in overview_data:
                role = item.get('Role', 'Unknown')
                nodes = item.get('Nodes', 0)
                metrics = item.get('Metrics Collected', 0)
                if nodes > 0:
                    summary_items.append(f"<li>{role}: {nodes} nodes, {metrics} metrics</li>")
            
            return (
                "<div class=\"disk-io-summary\">"
                "<h4>Disk I/O Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate disk I/O summary: {e}")
            return f"Disk I/O metrics collected from {len(data)} role groups"
    
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
            logger.error(f"Failed to transform disk I/O data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        # Define metric groups
        metric_groups = {
            'Container Disk Writes': 'container_disk_writes',
            'Disk Read Throughput': 'disk_throughput_read',
            'Disk Write Throughput': 'disk_throughput_write',
            'Disk Read IOPS': 'disk_iops_read',
            'Disk Write IOPS': 'disk_iops_write',
            'Disk Read Latency': 'disk_read_time',
            'Disk Write Latency': 'disk_write_time',
            'Disk I/O Time': 'disk_io_time'
        }
        
        try:
            # Overview table first
            if 'disk_io_overview' in dataframes and not dataframes['disk_io_overview'].empty:
                html_tables['disk_io_overview'] = self.create_html_table(
                    dataframes['disk_io_overview'], 
                    'Disk I/O Overview'
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
            logger.error(f"Failed to generate HTML tables for disk I/O: {e}")
        
        return html_tables