"""
Extract, Load, Transform module for etcd Performance Deep Drive Metrics
Handles performance deep drive data from analysis/etcd/etcd_performance_deepdrive.py
ONLY contains performance_deep_drive specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class performanceDeepDriveELT(utilityELT):
    """Extract, Load, Transform class for etcd performance deep drive metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            # General Info metrics
            'etcd_pods_cpu_usage': {'title': 'etcd Pods CPU Usage', 'unit': 'percent'},
            'etcd_pods_memory_usage': {'title': 'etcd Pods Memory Usage', 'unit': 'MB'},
            'proposal_failure_rate': {'title': 'Proposal Failure Rate', 'unit': 'per_second'},
            'proposal_pending_total': {'title': 'Pending Proposals', 'unit': 'count'},
            'proposal_commit_rate': {'title': 'Proposal Commit Rate', 'unit': 'per_second'},
            'proposal_apply_rate': {'title': 'Proposal Apply Rate', 'unit': 'per_second'},
            'total_proposals_committed': {'title': 'Total Proposals Committed', 'unit': 'count'},
            'leader_changes_rate': {'title': 'Leader Changes Rate', 'unit': 'per_second'},
            'etcd_mvcc_put_operations_rate': {'title': 'MVCC Put Operations Rate', 'unit': 'per_second'},
            'etcd_mvcc_delete_operations_rate': {'title': 'MVCC Delete Operations Rate', 'unit': 'per_second'},
            'etcd_server_health_failures': {'title': 'Server Health Failures', 'unit': 'per_second'},
            # WAL Fsync metrics
            'disk_wal_fsync_seconds_duration_p99': {'title': 'WAL Fsync P99 Latency', 'unit': 'seconds'},
            'disk_wal_fsync_duration_seconds_sum_rate': {'title': 'WAL Fsync Duration Sum Rate', 'unit': 'seconds'},
            'disk_wal_fsync_duration_sum': {'title': 'WAL Fsync Duration Sum', 'unit': 'seconds'},
            'disk_wal_fsync_duration_seconds_count_rate': {'title': 'WAL Fsync Operations Rate', 'unit': 'count'},
            'disk_wal_fsync_duration_seconds_count': {'title': 'WAL Fsync Operations Count', 'unit': 'count'},
            # Disk I/O metrics
            'disk_io_container_disk_writes': {'title': 'Container Disk Writes', 'unit': 'bytes_per_second'},
            'disk_io_node_disk_throughput_read': {'title': 'Disk Read Throughput', 'unit': 'bytes_per_second'},
            'disk_io_node_disk_throughput_write': {'title': 'Disk Write Throughput', 'unit': 'bytes_per_second'},
            'disk_io_node_disk_iops_read': {'title': 'Disk Read IOPS', 'unit': 'operations_per_second'},
            'disk_io_node_disk_iops_write': {'title': 'Disk Write IOPS', 'unit': 'operations_per_second'},
            'disk_io_node_disk_read_time_seconds': {'title': 'Disk Read Latency', 'unit': 'seconds'},
            'disk_io_node_disk_writes_time_seconds': {'title': 'Disk Write Latency', 'unit': 'seconds'},
            'disk_io_node_disk_io_time_seconds': {'title': 'Disk I/O Time', 'unit': 'seconds'},
            # Backend Commit metrics
            'disk_backend_commit_duration_seconds_p99': {'title': 'Backend Commit P99 Latency', 'unit': 'seconds'},
            'disk_backend_commit_duration_sum_rate': {'title': 'Backend Commit Duration Sum Rate', 'unit': 'seconds'},
            'disk_backend_commit_duration_sum': {'title': 'Backend Commit Duration Sum', 'unit': 'seconds'},
            'disk_backend_commit_duration_count_rate': {'title': 'Backend Commit Operations Rate', 'unit': 'count'},
            'disk_backend_commit_duration_count': {'title': 'Backend Commit Operations Count', 'unit': 'count'},
            # Compact Defrag metrics
            'debugging_mvcc_db_compacted_keys': {'title': 'DB Compacted Keys', 'unit': 'count'},
            'debugging_mvcc_db_compaction_duration_sum_delta': {'title': 'DB Compaction Rate', 'unit': 'milliseconds'},
            'debugging_mvcc_db_compaction_duration_sum': {'title': 'DB Compaction Duration', 'unit': 'milliseconds'},
            'debugging_snapshot_duration': {'title': 'Snapshot Duration', 'unit': 'seconds'},
            'disk_backend_defrag_duration_sum_rate': {'title': 'Defrag Rate', 'unit': 'seconds'},
            'disk_backend_defrag_duration_sum': {'title': 'Defrag Duration', 'unit': 'seconds'},
            # Network metrics
            'network_io_node_network_rx_utilization': {'title': 'Network RX Utilization', 'unit': 'bits_per_second'},
            'network_io_node_network_tx_utilization': {'title': 'Network TX Utilization', 'unit': 'bits_per_second'},
            'network_io_node_network_rx_package': {'title': 'Network RX Packets', 'unit': 'packets_per_second'},
            'network_io_node_network_tx_package': {'title': 'Network TX Packets', 'unit': 'packets_per_second'},
            'network_io_node_network_rx_drop': {'title': 'Network RX Drops', 'unit': 'packets_per_second'},
            'network_io_node_network_tx_drop': {'title': 'Network TX Drops', 'unit': 'packets_per_second'},
        }
    
    def extract_performance_deep_drive(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract performance deep drive information from etcd_performance_deepdrive.py output"""
        
        # Handle nested data structure - get the actual data section
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        # Also preserve summary and analysis from top level for overview
        summary = data.get('summary', {})
        analysis = data.get('analysis', {})
        test_id = data.get('test_id', '')
        duration = data.get('duration', '1h')
        
        structured = {
            'performance_deep_drive_overview': [],
            'general_info': [],
            'wal_fsync': [],
            'disk_io': [],
            'network_io': [],
            'backend_commit': [],
            'compact_defrag': [],
            'node_usage': []
        }
        
        # Extract general_info_data
        general_info_data = actual_data.get('general_info_data', [])
        for metric in general_info_data:
            self._extract_general_info_metric(metric, structured)
        
        # Extract wal_fsync_data
        wal_fsync_data = actual_data.get('wal_fsync_data', [])
        for metric in wal_fsync_data:
            self._extract_wal_fsync_metric(metric, structured)
        
        # Extract disk_io_data
        disk_io_data = actual_data.get('disk_io_data', [])
        for metric in disk_io_data:
            self._extract_disk_io_metric(metric, structured)
        
        # Extract network_data
        network_data = actual_data.get('network_data', {})
        node_metrics = network_data.get('node_metrics', [])
        for metric in node_metrics:
            self._extract_network_metric(metric, structured)
        
        # Extract backend_commit_data
        backend_commit_data = actual_data.get('backend_commit_data', [])
        for metric in backend_commit_data:
            self._extract_backend_commit_metric(metric, structured)
        
        # Extract compact_defrag_data
        compact_defrag_data = actual_data.get('compact_defrag_data', [])
        for metric in compact_defrag_data:
            self._extract_compact_defrag_metric(metric, structured)
        
        # Extract node_usage_data
        node_usage_data = actual_data.get('node_usage_data', {})
        if node_usage_data.get('status') == 'success':
            self._extract_node_usage_metrics(node_usage_data, structured)
        
        # Generate overview with summary data
        overview_data = {
            'summary': summary,
            'test_id': test_id,
            'duration': duration
        }
        self._generate_overview(overview_data, structured)
        
        return structured
    
    def _infer_role(self, node_name: str = '', pod_name: str = '') -> str:
        """Infer node role from node name or pod name using node labels"""
        # Use the node name to query role from labels
        if node_name and node_name != 'unknown':
            role = self.get_node_role_from_labels(node_name)
            return role
        
        # Fallback: infer from pod name if node name not available
        if pod_name:
            name_lower = pod_name.lower()
            # etcd pods run on control plane nodes
            if 'etcd' in name_lower:
                return 'controlplane'
            elif 'master' in name_lower or 'control' in name_lower:
                return 'controlplane'
            elif 'infra' in name_lower:
                return 'infra'
            elif 'workload' in name_lower:
                return 'workload'
        
        # Default fallback
        return 'worker'
    
    def _extract_general_info_metric(self, metric: Dict[str, Any], structured: Dict[str, Any]):
        """Extract general info metric"""
        metric_name = metric.get('metric_name', '')
        pod_name = metric.get('pod_name', '')
        avg_val = metric.get('avg', 0)
        max_val = metric.get('max', 0)
        unit = metric.get('unit', '')
        
        config = self.metric_configs.get(metric_name, {})
        metric_title = config.get('title', metric_name.replace('_', ' ').title())
        role = self._infer_role(pod_name=pod_name)
        
        structured['general_info'].append({
            'Metric Name': metric_title,
            'Role': role.title(),
            'Pod': self.truncate_node_name(pod_name, 30),
            'Avg': self.format_value_with_unit(avg_val, unit),
            'Max': self.format_value_with_unit(max_val, unit),
            'Unit': unit
        })
    
    def _extract_wal_fsync_metric(self, metric: Dict[str, Any], structured: Dict[str, Any]):
        """Extract WAL fsync metric"""
        metric_name = metric.get('metric_name', '')
        pod_name = metric.get('pod_name', '')
        avg_val = metric.get('avg', 0)
        max_val = metric.get('max', 0)
        unit = metric.get('unit', '')
        
        config = self.metric_configs.get(metric_name, {})
        metric_title = config.get('title', metric_name.replace('_', ' ').title())
        role = self._infer_role(pod_name=pod_name)
        
        structured['wal_fsync'].append({
            'Metric Name': metric_title,
            'Role': role.title(),
            'Pod': self.truncate_node_name(pod_name, 30),
            'Avg': self.format_value_with_unit(avg_val, unit),
            'Max': self.format_value_with_unit(max_val, unit),
            'Unit': unit
        })
    
    def _extract_disk_io_metric(self, metric: Dict[str, Any], structured: Dict[str, Any]):
        """Extract disk I/O metric"""
        metric_name = metric.get('metric_name', '')
        node_name = metric.get('node_name', '')
        avg_val = metric.get('avg', 0)
        max_val = metric.get('max', 0)
        unit = metric.get('unit', '')
        devices = metric.get('devices', [])
        
        config = self.metric_configs.get(metric_name, {})
        metric_title = config.get('title', metric_name.replace('_', ' ').title())
        role = self._infer_role(node_name=node_name)
        devices_str = ', '.join(devices) if devices else 'All'
        
        structured['disk_io'].append({
            'Metric Name': metric_title,
            'Role': role.title(),
            'Node': self.truncate_node_name(node_name),
            'Devices': devices_str,
            'Avg': self.format_value_with_unit(avg_val, unit),
            'Max': self.format_value_with_unit(max_val, unit),
            'Unit': unit
        })
    
    def _extract_network_metric(self, metric: Dict[str, Any], structured: Dict[str, Any]):
        """Extract network metric"""
        metric_name = metric.get('metric_name', '')
        node_name = metric.get('node_name', '')
        avg_val = metric.get('avg', 0)
        max_val = metric.get('max', 0)
        unit = metric.get('unit', '')
        
        config = self.metric_configs.get(metric_name, {})
        metric_title = config.get('title', metric_name.replace('_', ' ').title())
        role = self._infer_role(node_name=node_name)
        
        structured['network_io'].append({
            'Metric Name': metric_title,
            'Role': role.title(),
            'Node': self.truncate_node_name(node_name),
            'Avg': self.format_value_with_unit(avg_val, unit),
            'Max': self.format_value_with_unit(max_val, unit),
            'Unit': unit
        })
    
    def _extract_backend_commit_metric(self, metric: Dict[str, Any], structured: Dict[str, Any]):
        """Extract backend commit metric"""
        metric_name = metric.get('metric_name', '')
        pod_name = metric.get('pod_name', '')
        avg_val = metric.get('avg', 0)
        max_val = metric.get('max', 0)
        unit = metric.get('unit', '')
        
        config = self.metric_configs.get(metric_name, {})
        metric_title = config.get('title', metric_name.replace('_', ' ').title())
        role = self._infer_role(pod_name=pod_name)
        
        structured['backend_commit'].append({
            'Metric Name': metric_title,
            'Role': role.title(),
            'Pod': self.truncate_node_name(pod_name, 30),
            'Avg': self.format_value_with_unit(avg_val, unit),
            'Max': self.format_value_with_unit(max_val, unit),
            'Unit': unit
        })
    
    def _extract_compact_defrag_metric(self, metric: Dict[str, Any], structured: Dict[str, Any]):
        """Extract compact defrag metric"""
        metric_name = metric.get('metric_name', '')
        pod_name = metric.get('pod_name', '')
        avg_val = metric.get('avg', 0)
        max_val = metric.get('max', 0)
        unit = metric.get('unit', '')
        
        config = self.metric_configs.get(metric_name, {})
        metric_title = config.get('title', metric_name.replace('_', ' ').title())
        role = self._infer_role(pod_name=pod_name)
        
        structured['compact_defrag'].append({
            'Metric Name': metric_title,
            'Role': role.title(),
            'Pod': self.truncate_node_name(pod_name, 30),
            'Avg': self.format_value_with_unit(avg_val, unit),
            'Max': self.format_value_with_unit(max_val, unit),
            'Unit': unit
        })
    
    def _extract_node_usage_metrics(self, node_usage_data: Dict[str, Any], structured: Dict[str, Any]):
        """Extract node usage metrics"""
        metrics = node_usage_data.get('metrics', {})
        
        # Extract CPU usage
        cpu_usage = metrics.get('cpu_usage', {})
        if cpu_usage.get('status') == 'success':
            nodes = cpu_usage.get('nodes', {})
            for node_name, node_data in nodes.items():
                role = self._infer_role(node_name=node_name)
                total = node_data.get('total', {})
                avg_val = total.get('avg', 0)
                max_val = total.get('max', 0)
                
                structured['node_usage'].append({
                    'Metric Name': 'CPU Usage',
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Avg': self.format_percentage(avg_val),
                    'Max': self.format_percentage(max_val),
                    'Unit': 'percent'
                })
        
        # Extract Memory usage
        memory_used = metrics.get('memory_used', {})
        if memory_used.get('status') == 'success':
            nodes = memory_used.get('nodes', {})
            for node_name, node_data in nodes.items():
                role = self._infer_role(node_name=node_name)
                avg_val = node_data.get('avg', 0)
                max_val = node_data.get('max', 0)
                total_capacity = node_data.get('total_capacity', 0)
                
                structured['node_usage'].append({
                    'Metric Name': 'Memory Used',
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Avg': self.format_value_with_unit(avg_val, 'GB'),
                    'Max': self.format_value_with_unit(max_val, 'GB'),
                    'Capacity': self.format_value_with_unit(total_capacity, 'GB'),
                    'Unit': 'GB'
                })
        
        # Extract Cgroup CPU usage
        cgroup_cpu = metrics.get('cgroup_cpu_usage', {})
        if cgroup_cpu.get('status') == 'success':
            nodes = cgroup_cpu.get('nodes', {})
            for node_name, node_data in nodes.items():
                role = self._infer_role(node_name=node_name)
                cgroups = node_data.get('cgroups', {})
                kubepods = cgroups.get('kubepods.slice', {})
                avg_val = kubepods.get('avg', 0)
                max_val = kubepods.get('max', 0)
                
                structured['node_usage'].append({
                    'Metric Name': 'Kubepods CPU Usage',
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Avg': self.format_percentage(avg_val),
                    'Max': self.format_percentage(max_val),
                    'Unit': 'percent'
                })
        
        # Extract Cgroup RSS usage
        cgroup_rss = metrics.get('cgroup_rss_usage', {})
        if cgroup_rss.get('status') == 'success':
            nodes = cgroup_rss.get('nodes', {})
            for node_name, node_data in nodes.items():
                role = self._infer_role(node_name=node_name)
                cgroups = node_data.get('cgroups', {})
                kubepods = cgroups.get('kubepods.slice', {})
                avg_val = kubepods.get('avg', 0)
                max_val = kubepods.get('max', 0)
                
                structured['node_usage'].append({
                    'Metric Name': 'Kubepods RSS Usage',
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Avg': self.format_value_with_unit(avg_val, 'GB'),
                    'Max': self.format_value_with_unit(max_val, 'GB'),
                    'Unit': 'GB'
                })
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate performance deep drive overview"""
        summary = data.get('summary', {})
        categories = summary.get('categories', {})
        total_metrics = summary.get('total_metrics_collected', 0)
        overall_health = summary.get('overall_health', 'unknown')
        
        structured['performance_deep_drive_overview'].append({
            'Category': 'Performance Deep Drive',
            'Total Metrics': total_metrics,
            'Overall Health': overall_health.title(),
            'Duration': data.get('duration', '1h'),
            'Test ID': data.get('test_id', 'unknown')
        })
        
        # Add category breakdown
        if categories:
            for category, info in categories.items():
                structured['performance_deep_drive_overview'].append({
                    'Category': category.replace('_', ' ').title(),
                    'Total Metrics': info.get('count', 0),
                    'Overall Health': info.get('status', 'unknown').title(),
                    'Duration': '',
                    'Test ID': ''
                })
    
    def summarize_performance_deep_drive(self, data: Dict[str, Any]) -> str:
        """Generate performance deep drive summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('performance_deep_drive_overview', [])
            
            if overview_data:
                main_overview = overview_data[0] if overview_data else {}
                total_metrics = main_overview.get('Total Metrics', 0)
                overall_health = main_overview.get('Overall Health', 'unknown')
                duration = main_overview.get('Duration', '1h')
                test_id = main_overview.get('Test ID', 'unknown')
                
                summary_items.append(f"<li>Total Metrics Collected: {total_metrics}</li>")
                summary_items.append(f"<li>Overall Health: {overall_health}</li>")
                summary_items.append(f"<li>Duration: {duration}</li>")
                summary_items.append(f"<li>Test ID: {test_id}</li>")
                
                # Category breakdown
                for item in overview_data[1:]:
                    category = item.get('Category', '')
                    count = item.get('Total Metrics', 0)
                    status = item.get('Overall Health', '')
                    if count > 0:
                        summary_items.append(f"<li>{category}: {count} metrics ({status})</li>")
            
            return (
                "<div class=\"performance-deep-drive-summary\">"
                "<h4>etcd Performance Deep Drive Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate performance deep drive summary: {e}")
            return f"etcd Performance Deep Drive metrics collected"
    
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
            logger.error(f"Failed to transform performance deep drive data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            # Overview table first
            if 'performance_deep_drive_overview' in dataframes and not dataframes['performance_deep_drive_overview'].empty:
                html_tables['performance_deep_drive_overview'] = self.create_html_table(
                    dataframes['performance_deep_drive_overview'], 
                    'Performance Deep Drive Overview'
                )
            
            # Generate tables for each category
            category_order = [
                'general_info',
                'wal_fsync',
                'disk_io',
                'network_io',
                'backend_commit',
                'compact_defrag',
                'node_usage'
            ]
            
            for category in category_order:
                if category in dataframes and not dataframes[category].empty:
                    display_name = category.replace('_', ' ').title()
                    html_tables[category] = self.create_html_table(
                        dataframes[category], 
                        display_name
                    )
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for performance deep drive: {e}")
        
        return html_tables

