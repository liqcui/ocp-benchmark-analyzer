"""
Extract, Load, Transform module for etcd Backend Commit Metrics
Handles disk backend commit duration metrics from tools/etcd/etcd_disk_backend_commit.py
ONLY contains backend_commit specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class backendCommitELT(utilityELT):
    """Extract, Load, Transform class for backend commit metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'disk_backend_commit_duration_seconds_p99': {
                'title': 'P99 Commit Latency',
                'unit': 'seconds',
                'thresholds': {'critical': 0.050, 'warning': 0.020}  # 50ms, 20ms
            },
            'disk_backend_commit_duration_sum_rate': {
                'title': 'Commit Duration Rate',
                'unit': 'seconds',
                'thresholds': {}
            },
            'disk_backend_commit_duration_sum': {
                'title': 'Total Commit Duration',
                'unit': 'seconds',
                'thresholds': {}
            },
            'disk_backend_commit_duration_count_rate': {
                'title': 'Commit Operations Rate',
                'unit': 'ops_per_second',
                'thresholds': {}
            },
            'disk_backend_commit_duration_count': {
                'title': 'Total Commit Operations',
                'unit': 'count',
                'thresholds': {}
            }
        }
    
    def extract_backend_commit(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract backend commit information from etcd_disk_backend_commit.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        # Support both 'metrics' and 'pods_metrics' keys
        metrics_data = actual_data.get('metrics') or actual_data.get('pods_metrics')
        if not metrics_data or not isinstance(metrics_data, dict):
            return {'error': 'No metrics data found in backend commit result'}
        
        structured = {
            'backend_commit_overview': [],
        }
        
        # Initialize role-based tables for each metric
        roles = ['controlplane', 'infra', 'worker', 'workload']
        metric_types = ['p99_latency', 'duration_sum_rate', 'duration_sum', 'count_rate', 'count']
        
        for role in roles:
            for metric_type in metric_types:
                structured[f'{metric_type}_{role}'] = []
        
        # Extract pods data and categorize by role
        pods_by_role = self._categorize_pods_by_role(metrics_data)
        
        # Process each metric type
        for metric_name, metric_data in metrics_data.items():
            if metric_data.get('status') != 'success':
                continue
            
            if metric_name == 'disk_backend_commit_duration_seconds_p99':
                self._extract_p99_latency(metric_data, structured, pods_by_role)
            elif metric_name == 'disk_backend_commit_duration_sum_rate':
                self._extract_duration_sum_rate(metric_data, structured, pods_by_role)
            elif metric_name == 'disk_backend_commit_duration_sum':
                self._extract_duration_sum(metric_data, structured, pods_by_role)
            elif metric_name == 'disk_backend_commit_duration_count_rate':
                self._extract_count_rate(metric_data, structured, pods_by_role)
            elif metric_name == 'disk_backend_commit_duration_count':
                self._extract_count(metric_data, structured, pods_by_role)
        
        # Generate overview
        self._generate_overview(metrics_data, structured, pods_by_role)
        
        return structured
    
    def _categorize_pods_by_role(self, metrics_data: Dict[str, Any]) -> Dict[str, List[str]]:
        """Categorize pods by role based on pod names"""
        pods_by_role = {
            'controlplane': [],
            'infra': [],
            'worker': [],
            'workload': []
        }
        
        # Collect all unique pod names from all metrics
        all_pods = set()
        for metric_data in metrics_data.values():
            if isinstance(metric_data, dict) and 'pods' in metric_data:
                all_pods.update(metric_data['pods'].keys())
        
        # Categorize pods
        for pod_name in all_pods:
            pod_lower = pod_name.lower()
            if 'master' in pod_lower or 'control' in pod_lower:
                pods_by_role['controlplane'].append(pod_name)
            elif 'infra' in pod_lower:
                pods_by_role['infra'].append(pod_name)
            elif 'worker' in pod_lower:
                pods_by_role['worker'].append(pod_name)
            else:
                # Default to controlplane for etcd pods
                pods_by_role['controlplane'].append(pod_name)
        
        return pods_by_role
    
    def _extract_p99_latency(self, metric_data: Dict[str, Any], 
                            structured: Dict[str, Any], 
                            pods_by_role: Dict[str, List[str]]):
        """Extract P99 latency for each role"""
        pods_data = metric_data.get('pods', {})
        
        for role, pod_list in pods_by_role.items():
            if not pod_list:
                continue
            
            table_key = f'p99_latency_{role}'
            
            # Collect all values for top identification
            all_values = []
            for pod_name in pod_list:
                pod_metrics = pods_data.get(pod_name, {})
                if pod_metrics:
                    avg = pod_metrics.get('avg', 0)
                    max_val = pod_metrics.get('max', 0)
                    all_values.append((pod_name, avg, max_val))
            
            # Find top 1 by max value
            top_max = max((v[2] for v in all_values), default=0) if all_values else 0
            
            # Format rows
            thresholds = self.metric_configs['disk_backend_commit_duration_seconds_p99']['thresholds']
            for pod_name, avg, max_val in all_values:
                pod_metrics = pods_data.get(pod_name, {})
                is_top = (max_val == top_max and max_val > 0)
                
                # Convert to milliseconds for display
                avg_ms = avg * 1000
                max_ms = max_val * 1000
                min_ms = pod_metrics.get('min', 0) * 1000
                latest_ms = pod_metrics.get('latest', 0) * 1000
                
                structured[table_key].append({
                    'Metric Name': self.metric_configs['disk_backend_commit_duration_seconds_p99']['title'],
                    'Role': role.title(),
                    'Pod': self.truncate_node_name(pod_name, 30),
                    'Avg Latency': self._format_and_highlight_latency(avg_ms, thresholds),
                    'Max Latency': self._format_and_highlight_latency(max_ms, thresholds, is_top),
                    'Min Latency': self.format_latency_ms(min_ms / 1000),
                    'Latest': self.format_latency_ms(latest_ms / 1000),
                    'Samples': pod_metrics.get('count', 0)
                })
    
    def _extract_duration_sum_rate(self, metric_data: Dict[str, Any], 
                                   structured: Dict[str, Any], 
                                   pods_by_role: Dict[str, List[str]]):
        """Extract duration sum rate for each role"""
        pods_data = metric_data.get('pods', {})
        
        for role, pod_list in pods_by_role.items():
            if not pod_list:
                continue
            
            table_key = f'duration_sum_rate_{role}'
            
            all_values = []
            for pod_name in pod_list:
                pod_metrics = pods_data.get(pod_name, {})
                if pod_metrics:
                    max_val = pod_metrics.get('max', 0)
                    all_values.append((pod_name, max_val))
            
            top_max = max((v[1] for v in all_values), default=0) if all_values else 0
            
            for pod_name, max_val in all_values:
                pod_metrics = pods_data.get(pod_name, {})
                is_top = (max_val == top_max and max_val > 0)
                
                avg_display = self.format_latency_seconds(pod_metrics.get('avg', 0))
                max_display = self.format_latency_seconds(max_val)
                if is_top:
                    max_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {max_display}</span>'
                
                structured[table_key].append({
                    'Metric Name': self.metric_configs['disk_backend_commit_duration_sum_rate']['title'],
                    'Role': role.title(),
                    'Pod': self.truncate_node_name(pod_name, 30),
                    'Avg Rate': avg_display,
                    'Max Rate': max_display,
                    'Min Rate': self.format_latency_seconds(pod_metrics.get('min', 0)),
                    'Latest': self.format_latency_seconds(pod_metrics.get('latest', 0)),
                    'Samples': pod_metrics.get('count', 0)
                })
    
    def _extract_duration_sum(self, metric_data: Dict[str, Any], 
                             structured: Dict[str, Any], 
                             pods_by_role: Dict[str, List[str]]):
        """Extract total duration sum for each role"""
        pods_data = metric_data.get('pods', {})
        
        for role, pod_list in pods_by_role.items():
            if not pod_list:
                continue
            
            table_key = f'duration_sum_{role}'
            
            all_values = []
            for pod_name in pod_list:
                pod_metrics = pods_data.get(pod_name, {})
                if pod_metrics:
                    latest = pod_metrics.get('latest', 0)
                    all_values.append((pod_name, latest))
            
            top_latest = max((v[1] for v in all_values), default=0) if all_values else 0
            
            for pod_name, latest in all_values:
                pod_metrics = pods_data.get(pod_name, {})
                is_top = (latest == top_latest and latest > 0)
                
                latest_display = f"{latest:.2f} s"
                if is_top:
                    latest_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {latest_display}</span>'
                
                structured[table_key].append({
                    'Metric Name': self.metric_configs['disk_backend_commit_duration_sum']['title'],
                    'Role': role.title(),
                    'Pod': self.truncate_node_name(pod_name, 30),
                    'Total Duration': latest_display,
                    'Avg Duration': f"{pod_metrics.get('avg', 0):.2f} s",
                    'Max Duration': f"{pod_metrics.get('max', 0):.2f} s",
                    'Samples': pod_metrics.get('count', 0)
                })
    
    def _extract_count_rate(self, metric_data: Dict[str, Any], 
                           structured: Dict[str, Any], 
                           pods_by_role: Dict[str, List[str]]):
        """Extract commit count rate for each role"""
        pods_data = metric_data.get('pods', {})
        
        for role, pod_list in pods_by_role.items():
            if not pod_list:
                continue
            
            table_key = f'count_rate_{role}'
            
            all_values = []
            for pod_name in pod_list:
                pod_metrics = pods_data.get(pod_name, {})
                if pod_metrics:
                    max_val = pod_metrics.get('max', 0)
                    all_values.append((pod_name, max_val))
            
            top_max = max((v[1] for v in all_values), default=0) if all_values else 0
            
            for pod_name, max_val in all_values:
                pod_metrics = pods_data.get(pod_name, {})
                is_top = (max_val == top_max and max_val > 0)
                
                max_display = self.format_operations_per_second(max_val)
                if is_top:
                    max_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {max_display}</span>'
                
                structured[table_key].append({
                    'Metric Name': self.metric_configs['disk_backend_commit_duration_count_rate']['title'],
                    'Role': role.title(),
                    'Pod': self.truncate_node_name(pod_name, 30),
                    'Avg Rate': self.format_operations_per_second(pod_metrics.get('avg', 0)),
                    'Max Rate': max_display,
                    'Min Rate': self.format_operations_per_second(pod_metrics.get('min', 0)),
                    'Latest': self.format_operations_per_second(pod_metrics.get('latest', 0)),
                    'Samples': pod_metrics.get('count', 0)
                })
    
    def _extract_count(self, metric_data: Dict[str, Any], 
                      structured: Dict[str, Any], 
                      pods_by_role: Dict[str, List[str]]):
        """Extract total commit count for each role"""
        pods_data = metric_data.get('pods', {})
        
        for role, pod_list in pods_by_role.items():
            if not pod_list:
                continue
            
            table_key = f'count_{role}'
            
            all_values = []
            for pod_name in pod_list:
                pod_metrics = pods_data.get(pod_name, {})
                if pod_metrics:
                    latest = pod_metrics.get('latest', 0)
                    all_values.append((pod_name, latest))
            
            top_latest = max((v[1] for v in all_values), default=0) if all_values else 0
            
            for pod_name, latest in all_values:
                pod_metrics = pods_data.get(pod_name, {})
                is_top = (latest == top_latest and latest > 0)
                
                latest_display = self.format_count_value(latest)
                if is_top:
                    latest_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {latest_display}</span>'
                
                structured[table_key].append({
                    'Metric Name': self.metric_configs['disk_backend_commit_duration_count']['title'],
                    'Role': role.title(),
                    'Pod': self.truncate_node_name(pod_name, 30),
                    'Total Operations': latest_display,
                    'Avg Operations': self.format_count_value(pod_metrics.get('avg', 0)),
                    'Max Operations': self.format_count_value(pod_metrics.get('max', 0)),
                    'Samples': pod_metrics.get('count', 0)
                })
    
    def _format_and_highlight_latency(self, value_ms: float, 
                                     thresholds: Dict[str, float], 
                                     is_top: bool = False) -> str:
        """Format and highlight latency value"""
        formatted = self.format_latency_ms(value_ms / 1000)
        threshold_ms = {k: v * 1000 for k, v in thresholds.items()}
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
        elif value_ms >= threshold_ms.get('critical', float('inf')):
            return f'<span class="text-danger font-weight-bold">⚠️ {formatted}</span>'
        elif value_ms >= threshold_ms.get('warning', float('inf')):
            return f'<span class="text-warning font-weight-bold">{formatted}</span>'
        else:
            return f'<span class="text-success">{formatted}</span>'
    
    def _generate_overview(self, metrics_data: Dict[str, Any], 
                          structured: Dict[str, Any], 
                          pods_by_role: Dict[str, List[str]]):
        """Generate backend commit overview"""
        for role, pod_list in pods_by_role.items():
            if not pod_list:
                continue
            
            metrics_count = sum(1 for m in metrics_data.values() 
                              if m.get('status') == 'success')
            
            structured['backend_commit_overview'].append({
                'Role': role.title(),
                'Pods': len(pod_list),
                'Metrics Collected': metrics_count,
                'Status': self.create_status_badge('success', 'Active')
            })
    
    def summarize_backend_commit(self, data: Dict[str, Any]) -> str:
        """Generate backend commit summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('backend_commit_overview', [])
            
            total_pods = sum(int(item.get('Pods', 0)) for item in overview_data)
            if total_pods > 0:
                summary_items.append(f"<li>Total etcd Pods: {total_pods}</li>")
            
            # Role breakdown
            for item in overview_data:
                role = item.get('Role', 'Unknown')
                pods = item.get('Pods', 0)
                metrics = item.get('Metrics Collected', 0)
                if pods > 0:
                    summary_items.append(f"<li>{role}: {pods} pods, {metrics} metrics</li>")
            
            return (
                "<div class=\"backend-commit-summary\">"
                "<h4>Backend Commit Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate backend commit summary: {e}")
            return f"Backend commit metrics collected from {len(data)} role groups"
    
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
            logger.error(f"Failed to transform backend commit data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        # Define metric groups
        metric_groups = {
            'P99 Commit Latency': 'p99_latency',
            'Duration Sum Rate': 'duration_sum_rate',
            'Total Duration Sum': 'duration_sum',
            'Commit Operations Rate': 'count_rate',
            'Total Commit Operations': 'count'
        }
        
        try:
            # Overview table first
            if 'backend_commit_overview' in dataframes and not dataframes['backend_commit_overview'].empty:
                html_tables['backend_commit_overview'] = self.create_html_table(
                    dataframes['backend_commit_overview'], 
                    'Backend Commit Overview'
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
            logger.error(f"Failed to generate HTML tables for backend commit: {e}")
        
        return html_tables