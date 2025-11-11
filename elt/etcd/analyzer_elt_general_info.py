"""
Extract, Load, Transform module for ETCD General Info Metrics
Handles general info data from tools/etcd/etcd_general_info.py
ONLY contains general_info specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class generalInfoELT(utilityELT):
    """Extract, Load, Transform class for etcd general info metrics data"""
    
    def __init__(self):
        super().__init__()
        # Metric configurations with display info
        self.metric_configs = {
            'etcd_pods_cpu_usage': {
                'title': 'etcd Pods CPU Usage',
                'unit': 'percent',
                'thresholds': {'critical': 80.0, 'warning': 60.0}
            },
            'etcd_pods_memory_usage': {
                'title': 'etcd Pods Memory Usage',
                'unit': 'MB',
                'thresholds': {'critical': 256.0, 'warning': 200.0}
            },
            'etcd_db_space_used_percent': {
                'title': 'etcd DB Space Used',
                'unit': 'percent',
                'thresholds': {'critical': 80.0, 'warning': 60.0}
            },
            'etcd_db_physical_size': {
                'title': 'etcd DB Physical Size',
                'unit': 'MB',
                'thresholds': {'critical': 8000.0, 'warning': 6000.0}
            },
            'etcd_db_logical_size': {
                'title': 'etcd DB Logical Size',
                'unit': 'MB',
                'thresholds': {'critical': 4000.0, 'warning': 3000.0}
            },
            'cpu_io_utilization_iowait': {
                'title': 'CPU I/O Wait',
                'unit': 'percent',
                'thresholds': {'critical': 10.0, 'warning': 5.0},
                'scope': 'node'
            },
            'vmstat_pgmajfault_total': {
                'title': 'VM Major Page Faults Total',
                'unit': 'faults',
                'thresholds': {'critical': 10000.0, 'warning': 5000.0},
                'scope': 'node'
            },
            'vmstat_pgmajfault_rate': {
                'title': 'VM Major Page Fault Rate',
                'unit': 'faults/s',
                'thresholds': {'critical': 1.0, 'warning': 0.5},
                'scope': 'node'
            },
            'proposal_failure_rate': {
                'title': 'Proposal Failure Rate',
                'unit': 'per_second',
                'thresholds': {'critical': 0.1, 'warning': 0.01}
            },
            'proposal_pending_total': {
                'title': 'Pending Proposals',
                'unit': 'count',
                'thresholds': {'critical': 10.0, 'warning': 5.0}
            },
            'proposal_commit_rate': {
                'title': 'Proposal Commit Rate',
                'unit': 'per_second',
                'thresholds': {'critical': 1000.0, 'warning': 500.0}
            },
            'proposal_apply_rate': {
                'title': 'Proposal Apply Rate',
                'unit': 'per_second',
                'thresholds': {'critical': 1000.0, 'warning': 500.0}
            },
            'total_proposals_committed': {
                'title': 'Total Proposals Committed',
                'unit': 'count',
                'thresholds': {}
            },
            'leader_changes_rate': {
                'title': 'Leader Changes Rate',
                'unit': 'per_second',
                'thresholds': {'critical': 1.0, 'warning': 0.1}
            },
            'etcd_has_leader': {
                'title': 'etcd Has Leader',
                'unit': 'boolean',
                'thresholds': {}
            },
            'leader_elections_per_day': {
                'title': 'Leader Elections Per Day',
                'unit': 'per_day',
                'thresholds': {'critical': 1.0, 'warning': 0.5}
            },
            'etcd_mvcc_put_operations_rate': {
                'title': 'MVCC Put Operations Rate',
                'unit': 'per_second',
                'thresholds': {}
            },
            'etcd_mvcc_delete_operations_rate': {
                'title': 'MVCC Delete Operations Rate',
                'unit': 'per_second',
                'thresholds': {}
            },
            'etcd_server_health_failures': {
                'title': 'Server Health Failures',
                'unit': 'per_second',
                'thresholds': {'critical': 0.1, 'warning': 0.01}
            },
            'etcd_debugging_mvcc_total_keys': {
                'title': 'MVCC Total Keys',
                'unit': 'count',
                'thresholds': {}
            }
        }
    
    def extract_general_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract general info from etcd_general_info.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'general_info_overview': [],
        }
        
        pod_metrics = actual_data.get('pod_metrics', {})
        
        # Extract pod-level metrics (single pod in this case)
        for metric_name, metric_data in pod_metrics.items():
            if isinstance(metric_data, dict) and 'pods' in metric_data:
                self._extract_pod_metric(metric_name, metric_data, structured)
            elif isinstance(metric_data, dict) and 'nodes' in metric_data:
                self._extract_node_metric(metric_name, metric_data, structured)
            elif isinstance(metric_data, dict) and 'resources' in metric_data:
                self._extract_resource_metric(metric_name, metric_data, structured)
        
        # Generate overview
        self._generate_overview(actual_data, structured)
        
        return structured
    
    def _infer_role(self, node_name: str, pod_name: str = '') -> str:
        """Infer node role from node name or pod name using node labels.
        
        First tries to query node role from Kubernetes node labels (node-role.kubernetes.io/master),
        then falls back to name-based inference.
        """
        # Use the node name to query role from labels
        if node_name:
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
    
    def _extract_pod_metric(self, metric_name: str, metric_data: Dict[str, Any], 
                           structured: Dict[str, Any]):
        """Extract pod-level metric"""
        table_key = metric_name
        if table_key not in structured:
            structured[table_key] = []
        
        config = self.metric_configs.get(metric_name, {})
        title = config.get('title', metric_name.replace('_', ' ').title())
        unit = metric_data.get('unit', config.get('unit', ''))
        pods = metric_data.get('pods', {})
        
        if not pods:
            return
        
        # Collect all values for top identification
        all_values = []
        for pod_name, pod_stats in pods.items():
            avg_val = pod_stats.get('avg', 0)
            max_val = pod_stats.get('max', 0)
            all_values.append((pod_name, avg_val, max_val, pod_stats))
        
        # Find top 1 by max value
        top_max = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        thresholds = config.get('thresholds', {})
        for pod_name, avg_val, max_val, pod_stats in all_values:
            is_top = (max_val == top_max and max_val > 0)
            
            # Format avg
            avg_display = self.highlight_general_info_values(
                avg_val, metric_name, unit, is_top=False
            )
            
            # Format max with top highlight
            max_display = self.highlight_general_info_values(
                max_val, metric_name, unit, is_top=is_top
            )
            
            node_name = pod_stats.get('node', 'unknown')
            role = self._infer_role(node_name, pod_name)
            
            structured[table_key].append({
                'Metric Name': title,
                'Role': role.title(),
                'Pod': self.truncate_text(pod_name, max_length=60),
                'Node': self.truncate_node_name(node_name),
                'Avg': avg_display,
                'Max': max_display,
                'Count': pod_stats.get('count', 0)
            })
    
    def _extract_node_metric(self, metric_name: str, metric_data: Dict[str, Any], 
                            structured: Dict[str, Any]):
        """Extract node-level metric"""
        table_key = metric_name
        if table_key not in structured:
            structured[table_key] = []
        
        config = self.metric_configs.get(metric_name, {})
        title = config.get('title', metric_name.replace('_', ' ').title())
        unit = metric_data.get('unit', config.get('unit', ''))
        nodes = metric_data.get('nodes', {})
        
        if not nodes:
            return
        
        # Collect all values for top identification
        all_values = []
        for node_name, node_stats in nodes.items():
            avg_val = node_stats.get('avg', 0)
            max_val = node_stats.get('max', 0)
            all_values.append((node_name, avg_val, max_val, node_stats))
        
        # Find top 1 by max value
        top_max = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        thresholds = config.get('thresholds', {})
        for node_name, avg_val, max_val, node_stats in all_values:
            is_top = (max_val == top_max and max_val > 0)
            
            # Format avg
            avg_display = self.highlight_general_info_values(
                avg_val, metric_name, unit, is_top=False
            )
            
            # Format max with top highlight
            max_display = self.highlight_general_info_values(
                max_val, metric_name, unit, is_top=is_top
            )
            
            role = self._infer_role(node_name)
            
            structured[table_key].append({
                'Metric Name': title,
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Avg': avg_display,
                'Max': max_display,
                'Count': node_stats.get('count', 0)
            })
    
    def _extract_resource_metric(self, metric_name: str, metric_data: Dict[str, Any], 
                                structured: Dict[str, Any]):
        """Extract resource-based metric (e.g., apiserver_storage_objects_max_top20)"""
        table_key = metric_name
        if table_key not in structured:
            structured[table_key] = []
        
        config = self.metric_configs.get(metric_name, {})
        title = config.get('title', metric_name.replace('_', ' ').title())
        resources = metric_data.get('resources', [])
        
        if not resources:
            return
        
        # Resources are already sorted by max_value descending
        # Highlight top 3
        for idx, resource in enumerate(resources[:20]):  # Top 20
            resource_name = resource.get('resource_name', '')
            max_value = resource.get('max_value', 0)
            
            if idx == 0:  # Top 1
                value_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {int(max_value)}</span>'
            elif idx == 1:  # Top 2
                value_display = f'<span class="text-info font-weight-bold">⭐ {int(max_value)}</span>'
            elif idx == 2:  # Top 3
                value_display = f'<span class="text-success font-weight-bold">{int(max_value)}</span>'
            else:
                value_display = str(int(max_value))
            
            structured[table_key].append({
                'Metric Name': title,
                'Rank': idx + 1,
                'Resource Name': resource_name,
                'Max Objects': value_display
            })
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate general info overview"""
        pod_metrics = data.get('pod_metrics', {})
        
        # Count metrics by type
        pod_metric_count = 0
        node_metric_count = 0
        resource_metric_count = 0
        
        for metric_name, metric_data in pod_metrics.items():
            if isinstance(metric_data, dict):
                if 'pods' in metric_data:
                    pod_metric_count += 1
                elif 'nodes' in metric_data:
                    node_metric_count += 1
                elif 'resources' in metric_data:
                    resource_metric_count += 1
        
        structured['general_info_overview'].append({
            'Category': 'etcd General Info',
            'Duration': data.get('duration', '1h'),
            'Pod Metrics': pod_metric_count,
            'Node Metrics': node_metric_count,
            'Resource Metrics': resource_metric_count,
            'Total Metrics': pod_metric_count + node_metric_count + resource_metric_count,
            'Status': self.create_status_badge('success', 'Complete')
        })
    
    def summarize_general_info(self, data: Dict[str, Any]) -> str:
        """Generate general info summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('general_info_overview', [])
            
            if overview_data:
                overview = overview_data[0]
                total_metrics = overview.get('Total Metrics', 0)
                duration = overview.get('Duration', '1h')
                summary_items.append(f"<li>Total Metrics Collected: {total_metrics}</li>")
                summary_items.append(f"<li>Duration: {duration}</li>")
                
                pod_metrics = overview.get('Pod Metrics', 0)
                node_metrics = overview.get('Node Metrics', 0)
                resource_metrics = overview.get('Resource Metrics', 0)
                
                if pod_metrics > 0:
                    summary_items.append(f"<li>Pod-level Metrics: {pod_metrics}</li>")
                if node_metrics > 0:
                    summary_items.append(f"<li>Node-level Metrics: {node_metrics}</li>")
                if resource_metrics > 0:
                    summary_items.append(f"<li>Resource Metrics: {resource_metrics}</li>")
            
            return (
                "<div class=\"general-info-summary\">"
                "<h4>etcd General Info Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate general info summary: {e}")
            return f"etcd General Info metrics collected"
    
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
            logger.error(f"Failed to transform general info data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            # Overview table first
            if 'general_info_overview' in dataframes and not dataframes['general_info_overview'].empty:
                html_tables['general_info_overview'] = self.create_html_table(
                    dataframes['general_info_overview'], 
                    'etcd General Info Overview'
                )
            
            # Generate tables for each metric
            metric_order = [
                'etcd_pods_cpu_usage',
                'etcd_pods_memory_usage',
                'cpu_io_utilization_iowait',
                'etcd_db_space_used_percent',
                'etcd_db_physical_size',
                'etcd_db_logical_size',
                'vmstat_pgmajfault_total',
                'vmstat_pgmajfault_rate',
                'apiserver_storage_objects_max_top20',
                'proposal_failure_rate',
                'proposal_pending_total',
                'proposal_commit_rate',
                'proposal_apply_rate',
                'total_proposals_committed',
                'leader_changes_rate',
                'etcd_has_leader',
                'leader_elections_per_day',
                'etcd_mvcc_put_operations_rate',
                'etcd_mvcc_delete_operations_rate',
                'etcd_server_health_failures',
                'etcd_debugging_mvcc_total_keys'
            ]
            
            for metric_name in metric_order:
                if metric_name in dataframes and not dataframes[metric_name].empty:
                    config = self.metric_configs.get(metric_name, {})
                    display_name = config.get('title', metric_name.replace('_', ' ').title())
                    html_tables[metric_name] = self.create_html_table(
                        dataframes[metric_name], 
                        display_name
                    )
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for general info: {e}")
        
        return html_tables