"""
Extract, Load, Transform module for etcd Compact and Defrag Metrics
Handles compact/defrag data from tools/etcd/etcd_disk_compact_defrag.py
ONLY contains compact_defrag specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class compactDefragELT(utilityELT):
    """Extract, Load, Transform class for etcd compact and defrag metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'debugging_mvcc_db_compacted_keys': {
                'title': 'DB Compacted Keys',
                'unit': 'count',
                'thresholds': {'critical': 50000, 'warning': 30000}
            },
            'debugging_mvcc_db_compaction_duration_sum_delta': {
                'title': 'DB Compaction Rate',
                'unit': 'milliseconds',
                'thresholds': {'critical': 100, 'warning': 50}
            },
            'debugging_mvcc_db_compaction_duration_sum': {
                'title': 'DB Compaction Duration',
                'unit': 'milliseconds',
                'thresholds': {'critical': 5000, 'warning': 3000}
            },
            'debugging_snapshot_duration': {
                'title': 'Snapshot Duration',
                'unit': 'seconds',
                'thresholds': {'critical': 5.0, 'warning': 2.0}
            },
            'disk_backend_defrag_duration_sum_rate': {
                'title': 'Defrag Rate',
                'unit': 'seconds',
                'thresholds': {'critical': 10.0, 'warning': 5.0}
            },
            'disk_backend_defrag_duration_sum': {
                'title': 'Defrag Duration',
                'unit': 'seconds',
                'thresholds': {'critical': 30.0, 'warning': 10.0}
            }
        }
    
    def extract_compact_defrag(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract compact defrag information from etcd_disk_compact_defrag.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'compact_defrag_overview': [],
            'compacted_keys': [],
            'compaction_rate': [],
            'compaction_duration': [],
            'snapshot_duration': [],
            'defrag_rate': [],
            'defrag_duration': []
        }
        
        # Extract pods_metrics or metrics
        metrics_data = actual_data.get('pods_metrics') or actual_data.get('metrics', {})
        master_nodes = actual_data.get('master_nodes', [])
        
        if not metrics_data:
            return structured
        
        # Process each metric
        for metric_name, metric_info in metrics_data.items():
            if metric_info.get('status') != 'success':
                continue
            
            if metric_name == 'debugging_mvcc_db_compacted_keys':
                self._extract_compacted_keys(metric_info, structured)
            elif metric_name == 'debugging_mvcc_db_compaction_duration_sum_delta':
                self._extract_compaction_rate(metric_info, structured)
            elif metric_name == 'debugging_mvcc_db_compaction_duration_sum':
                self._extract_compaction_duration(metric_info, structured)
            elif metric_name == 'debugging_snapshot_duration':
                self._extract_snapshot_duration(metric_info, structured)
            elif metric_name == 'disk_backend_defrag_duration_sum_rate':
                self._extract_defrag_rate(metric_info, structured)
            elif metric_name == 'disk_backend_defrag_duration_sum':
                self._extract_defrag_duration(metric_info, structured)
        
        # Generate overview
        self._generate_overview(metrics_data, master_nodes, structured)
        
        return structured
    
    def _infer_role(self, node_name: str, pod_name: str = '') -> str:
        """Infer node role from node name or pod name using node labels.
        
        First tries to query node role from Kubernetes node labels (node-role.kubernetes.io/master),
        then falls back to name-based inference.
        """
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
    
    def _extract_compacted_keys(self, metric_info: Dict[str, Any], 
                                structured: Dict[str, Any]):
        """Extract compacted keys metric"""
        pods_data = metric_info.get('data', {}).get('pods', {})
        
        # Collect all values for top identification
        all_values = []
        for pod_name, pod_info in pods_data.items():
            if pod_name == 'unknown' and pod_info.get('avg', 0) == 0:
                continue
            avg_val = float(pod_info.get('avg', 0))
            all_values.append((pod_name, pod_info, avg_val))
        
        # Find top 1
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        thresholds = self.metric_configs['debugging_mvcc_db_compacted_keys']['thresholds']
        for pod_name, pod_info, avg_val in all_values:
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.highlight_critical_values(
                avg_val, thresholds, '', is_top
            )
            max_display = self.highlight_critical_values(
                pod_info.get('max', 0), thresholds, ''
            )
            
            node_name = pod_info.get('node', 'unknown')
            role = self._infer_role(node_name, pod_name)
            
            structured['compacted_keys'].append({
                'Metric Name': self.metric_configs['debugging_mvcc_db_compacted_keys']['title'],
                'Role': role.title(),
                'Pod': self.truncate_node_name(pod_name, 30),
                'Node': self.truncate_node_name(node_name, 25),
                'Average': avg_display,
                'Maximum': max_display,
                'Latest': int(pod_info.get('latest', 0))
            })
    
    def _extract_compaction_rate(self, metric_info: Dict[str, Any], 
                                 structured: Dict[str, Any]):
        """Extract compaction rate (delta) metric"""
        pods_data = metric_info.get('data', {}).get('pods', {})
        
        # Collect all values for top identification
        all_values = []
        for pod_name, pod_info in pods_data.items():
            if pod_name == 'unknown' and pod_info.get('avg', 0) == 0:
                continue
            avg_val = float(pod_info.get('avg', 0))
            all_values.append((pod_name, pod_info, avg_val))
        
        # Find top 1
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        thresholds = self.metric_configs['debugging_mvcc_db_compaction_duration_sum_delta']['thresholds']
        for pod_name, pod_info, avg_val in all_values:
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.format_and_highlight(
                avg_val, 'milliseconds', thresholds, is_top
            )
            max_display = self.format_and_highlight(
                pod_info.get('max', 0), 'milliseconds', thresholds
            )
            
            node_name = pod_info.get('node', 'unknown')
            role = self._infer_role(node_name, pod_name)
            
            structured['compaction_rate'].append({
                'Metric Name': self.metric_configs['debugging_mvcc_db_compaction_duration_sum_delta']['title'],
                'Role': role.title(),
                'Pod': self.truncate_node_name(pod_name, 30),
                'Node': self.truncate_node_name(node_name, 25),
                'Average': avg_display,
                'Maximum': max_display,
                'Latest': self.format_value_with_unit(pod_info.get('latest', 0), 'milliseconds')
            })
    
    def _extract_compaction_duration(self, metric_info: Dict[str, Any], 
                                    structured: Dict[str, Any]):
        """Extract compaction duration (cumulative) metric"""
        pods_data = metric_info.get('data', {}).get('pods', {})
        
        # Collect all values for top identification
        all_values = []
        for pod_name, pod_info in pods_data.items():
            if pod_name == 'unknown' and pod_info.get('avg', 0) == 0:
                continue
            avg_val = float(pod_info.get('avg', 0))
            all_values.append((pod_name, pod_info, avg_val))
        
        # Find top 1
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        thresholds = self.metric_configs['debugging_mvcc_db_compaction_duration_sum']['thresholds']
        for pod_name, pod_info, avg_val in all_values:
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.format_and_highlight(
                avg_val, 'milliseconds', thresholds, is_top
            )
            max_display = self.format_and_highlight(
                pod_info.get('max', 0), 'milliseconds', thresholds
            )
            
            node_name = pod_info.get('node', 'unknown')
            role = self._infer_role(node_name, pod_name)
            
            structured['compaction_duration'].append({
                'Metric Name': self.metric_configs['debugging_mvcc_db_compaction_duration_sum']['title'],
                'Role': role.title(),
                'Pod': self.truncate_node_name(pod_name, 30),
                'Node': self.truncate_node_name(node_name, 25),
                'Average': avg_display,
                'Maximum': max_display,
                'Latest': self.format_value_with_unit(pod_info.get('latest', 0), 'milliseconds')
            })
    
    def _extract_snapshot_duration(self, metric_info: Dict[str, Any], 
                                   structured: Dict[str, Any]):
        """Extract snapshot duration metric"""
        pods_data = metric_info.get('data', {}).get('pods', {})
        
        # Collect all values for top identification
        all_values = []
        for pod_name, pod_info in pods_data.items():
            if pod_name == 'unknown':
                continue
            avg_val = float(pod_info.get('avg', 0))
            all_values.append((pod_name, pod_info, avg_val))
        
        # Find top 1
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        thresholds = self.metric_configs['debugging_snapshot_duration']['thresholds']
        for pod_name, pod_info, avg_val in all_values:
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.format_and_highlight(
                avg_val, 'seconds', thresholds, is_top
            )
            max_display = self.format_and_highlight(
                pod_info.get('max', 0), 'seconds', thresholds
            )
            
            node_name = pod_info.get('node', 'unknown')
            role = self._infer_role(node_name, pod_name)
            
            structured['snapshot_duration'].append({
                'Metric Name': self.metric_configs['debugging_snapshot_duration']['title'],
                'Role': role.title(),
                'Pod': self.truncate_node_name(pod_name, 30),
                'Node': self.truncate_node_name(node_name, 25),
                'Average': avg_display,
                'Maximum': max_display,
                'Latest': self.format_value_with_unit(pod_info.get('latest', 0), 'seconds')
            })
    
    def _extract_defrag_rate(self, metric_info: Dict[str, Any], 
                            structured: Dict[str, Any]):
        """Extract defrag rate metric"""
        pods_data = metric_info.get('data', {}).get('pods', {})
        
        # Collect all values for top identification
        all_values = []
        for pod_name, pod_info in pods_data.items():
            if pod_name == 'unknown' and pod_info.get('avg', 0) == 0:
                continue
            avg_val = float(pod_info.get('avg', 0))
            all_values.append((pod_name, pod_info, avg_val))
        
        # Find top 1
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        thresholds = self.metric_configs['disk_backend_defrag_duration_sum_rate']['thresholds']
        for pod_name, pod_info, avg_val in all_values:
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.format_and_highlight(
                avg_val, 'seconds', thresholds, is_top
            )
            max_display = self.format_and_highlight(
                pod_info.get('max', 0), 'seconds', thresholds
            )
            
            node_name = pod_info.get('node', 'unknown')
            role = self._infer_role(node_name, pod_name)
            
            structured['defrag_rate'].append({
                'Metric Name': self.metric_configs['disk_backend_defrag_duration_sum_rate']['title'],
                'Role': role.title(),
                'Pod': self.truncate_node_name(pod_name, 30),
                'Node': self.truncate_node_name(node_name, 25),
                'Average': avg_display,
                'Maximum': max_display,
                'Latest': self.format_value_with_unit(pod_info.get('latest', 0), 'seconds')
            })
    
    def _extract_defrag_duration(self, metric_info: Dict[str, Any], 
                                structured: Dict[str, Any]):
        """Extract defrag duration (cumulative) metric"""
        pods_data = metric_info.get('data', {}).get('pods', {})
        
        # Collect all values for top identification
        all_values = []
        for pod_name, pod_info in pods_data.items():
            if pod_name == 'unknown' and pod_info.get('avg', 0) == 0:
                continue
            avg_val = float(pod_info.get('avg', 0))
            all_values.append((pod_name, pod_info, avg_val))
        
        # Find top 1
        top_avg = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        thresholds = self.metric_configs['disk_backend_defrag_duration_sum']['thresholds']
        for pod_name, pod_info, avg_val in all_values:
            is_top = (avg_val == top_avg and avg_val > 0)
            
            avg_display = self.format_and_highlight(
                avg_val, 'seconds', thresholds, is_top
            )
            max_display = self.format_and_highlight(
                pod_info.get('max', 0), 'seconds', thresholds
            )
            
            node_name = pod_info.get('node', 'unknown')
            role = self._infer_role(node_name, pod_name)
            
            structured['defrag_duration'].append({
                'Metric Name': self.metric_configs['disk_backend_defrag_duration_sum']['title'],
                'Role': role.title(),
                'Pod': self.truncate_node_name(pod_name, 30),
                'Node': self.truncate_node_name(node_name, 25),
                'Average': avg_display,
                'Maximum': max_display,
                'Latest': self.format_value_with_unit(pod_info.get('latest', 0), 'seconds')
            })
    
    def _generate_overview(self, metrics_data: Dict[str, Any], 
                          master_nodes: List[str], 
                          structured: Dict[str, Any]):
        """Generate compact defrag overview"""
        total_metrics = len([m for m in metrics_data.values() if m.get('status') == 'success'])
        
        # Count metrics by category
        compaction_count = sum(1 for k in metrics_data.keys() 
                              if 'compaction' in k.lower() or 'compacted' in k.lower())
        defrag_count = sum(1 for k in metrics_data.keys() if 'defrag' in k.lower())
        snapshot_count = sum(1 for k in metrics_data.keys() if 'snapshot' in k.lower())
        
        structured['compact_defrag_overview'].append({
            'Category': 'Compaction',
            'Metrics': compaction_count,
            'Master Nodes': len(master_nodes),
            'Status': self.create_status_badge('success', 'Active')
        })
        
        if defrag_count > 0:
            structured['compact_defrag_overview'].append({
                'Category': 'Defragmentation',
                'Metrics': defrag_count,
                'Master Nodes': len(master_nodes),
                'Status': self.create_status_badge('success', 'Active')
            })
        
        if snapshot_count > 0:
            structured['compact_defrag_overview'].append({
                'Category': 'Snapshot',
                'Metrics': snapshot_count,
                'Master Nodes': len(master_nodes),
                'Status': self.create_status_badge('success', 'Active')
            })
    
    def summarize_compact_defrag(self, data: Dict[str, Any]) -> str:
        """Generate compact defrag summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('compact_defrag_overview', [])
            
            if overview_data:
                total_metrics = sum(int(item.get('Metrics', 0)) for item in overview_data)
                master_nodes = overview_data[0].get('Master Nodes', 0) if overview_data else 0
                
                summary_items.append(f"<li>Total Metrics: {total_metrics} across {master_nodes} master nodes</li>")
                
                for item in overview_data:
                    category = item.get('Category', 'Unknown')
                    metrics = item.get('Metrics', 0)
                    summary_items.append(f"<li>{category}: {metrics} metrics collected</li>")
            
            # Add metric-specific summaries
            if data.get('compacted_keys'):
                avg_keys = sum(self.extract_numeric_value(row.get('Average', 0)) 
                             for row in data['compacted_keys']) / len(data['compacted_keys'])
                summary_items.append(f"<li>Average Compacted Keys: {self.format_count_value(avg_keys)}</li>")
            
            if data.get('compaction_duration'):
                avg_duration = sum(self.extract_numeric_value(row.get('Average', 0)) 
                                 for row in data['compaction_duration']) / len(data['compaction_duration'])
                summary_items.append(f"<li>Average Compaction Duration: {self.format_value_with_unit(avg_duration, 'milliseconds')}</li>")
            
            return (
                "<div class=\"compact-defrag-summary\">"
                "<h4>Compact & Defrag Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate compact defrag summary: {e}")
            return f"Compact & Defrag metrics collected from {len(data)} categories"
    
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
            logger.error(f"Failed to transform compact defrag data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        # Define table order and display names
        table_configs = {
            'compact_defrag_overview': 'Compact & Defrag Overview',
            'compacted_keys': 'DB Compacted Keys',
            'compaction_rate': 'DB Compaction Rate',
            'compaction_duration': 'DB Compaction Duration',
            'snapshot_duration': 'Snapshot Duration',
            'defrag_rate': 'Defrag Rate',
            'defrag_duration': 'Defrag Duration'
        }
        
        try:
            for table_key, display_name in table_configs.items():
                if table_key in dataframes and not dataframes[table_key].empty:
                    html_tables[table_key] = self.create_html_table(
                        dataframes[table_key], 
                        display_name
                    )
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for compact defrag: {e}")
        
        return html_tables