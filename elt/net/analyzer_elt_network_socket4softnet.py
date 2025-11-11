"""
Extract, Load, Transform module for Network Socket SoftNet Metrics
Handles softnet statistics data from tools/net/network_socket4softnet.py
ONLY contains network_socket_softnet specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class networkSocketSoftnetELT(utilityELT):
    """Extract, Load, Transform class for network socket softnet metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'softnet_processed_total': {
                'title': 'Softnet Processed Total',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 5000, 'warning': 1000}
            },
            'softnet_dropped_total': {
                'title': 'Softnet Dropped Total',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 10, 'warning': 1}
            },
            'softnet_out_of_quota': {
                'title': 'Softnet Out of Quota',
                'unit': 'eps',
                'thresholds': {'critical': 10, 'warning': 1}
            },
            'softnet_cpu_rps': {
                'title': 'Softnet CPU RPS',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 1000, 'warning': 500}
            },
            'softnet_flow_limit_count': {
                'title': 'Softnet Flow Limit Count',
                'unit': 'count_per_second',
                'thresholds': {'critical': 100, 'warning': 50}
            }
        }
    
    def extract_network_socket_softnet(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract network socket softnet information from network_socket4softnet.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'softnet_overview': [],
        }
        
        # Initialize all role-based tables for each metric
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            structured[f'softnet_processed_total_{role}'] = []
            structured[f'softnet_dropped_total_{role}'] = []
            structured[f'softnet_out_of_quota_{role}'] = []
            structured[f'softnet_cpu_rps_{role}'] = []
            structured[f'softnet_flow_limit_count_{role}'] = []
        
        metrics = actual_data.get('metrics', {})
        
        # Process each metric
        for metric_name, metric_data in metrics.items():
            if 'error' in metric_data:
                continue
            
            if metric_name == 'softnet_processed_total':
                self._extract_softnet_processed_total(metric_data, structured)
            elif metric_name == 'softnet_dropped_total':
                self._extract_softnet_dropped_total(metric_data, structured)
            elif metric_name == 'softnet_out_of_quota':
                self._extract_softnet_out_of_quota(metric_data, structured)
            elif metric_name == 'softnet_cpu_rps':
                self._extract_softnet_cpu_rps(metric_data, structured)
            elif metric_name == 'softnet_flow_limit_count':
                self._extract_softnet_flow_limit_count(metric_data, structured)
        
        # Generate overview
        self._generate_overview(actual_data, structured)
        
        return structured
    
    def _extract_softnet_processed_total(self, metric_data: Dict[str, Any], 
                                         structured: Dict[str, Any]):
        """Extract softnet processed total for all roles"""
        config = self.metric_configs['softnet_processed_total']
        nodes = metric_data.get('nodes', {})
        
        for role in ['controlplane', 'worker', 'infra', 'workload']:
            if role not in nodes:
                continue
            
            table_key = f'softnet_processed_total_{role}'
            role_nodes = nodes[role]
            
            # Collect all values for top identification
            all_values = []
            for node_name, stats in role_nodes.items():
                avg_val = float(stats.get('avg', 0))
                max_val = float(stats.get('max', 0))
                all_values.append((node_name, avg_val, max_val))
            
            # Find top 1 by max value
            top_max = max((v[2] for v in all_values), default=0) if all_values else 0
            
            # Format rows
            for node_name, avg_val, max_val in all_values:
                is_top = (max_val == top_max and max_val > 0)
                
                avg_display = self.format_and_highlight(
                    avg_val, config['unit'], config['thresholds'], False
                )
                max_display = self.format_and_highlight(
                    max_val, config['unit'], config['thresholds'], is_top
                )
                
                structured[table_key].append({
                    'Metric Name': config['title'],
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Avg': avg_display,
                    'Max': max_display
                })
    
    def _extract_softnet_dropped_total(self, metric_data: Dict[str, Any], 
                                       structured: Dict[str, Any]):
        """Extract softnet dropped total for all roles"""
        config = self.metric_configs['softnet_dropped_total']
        nodes = metric_data.get('nodes', {})
        
        for role in ['controlplane', 'worker', 'infra', 'workload']:
            if role not in nodes:
                continue
            
            table_key = f'softnet_dropped_total_{role}'
            role_nodes = nodes[role]
            
            all_values = []
            for node_name, stats in role_nodes.items():
                avg_val = float(stats.get('avg', 0))
                max_val = float(stats.get('max', 0))
                all_values.append((node_name, avg_val, max_val))
            
            top_max = max((v[2] for v in all_values), default=0) if all_values else 0
            
            for node_name, avg_val, max_val in all_values:
                is_top = (max_val == top_max and max_val > 0)
                
                avg_display = self.format_and_highlight(
                    avg_val, config['unit'], config['thresholds'], False
                )
                max_display = self.format_and_highlight(
                    max_val, config['unit'], config['thresholds'], is_top
                )
                
                structured[table_key].append({
                    'Metric Name': config['title'],
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Avg': avg_display,
                    'Max': max_display
                })
    
    def _extract_softnet_out_of_quota(self, metric_data: Dict[str, Any], 
                                      structured: Dict[str, Any]):
        """Extract softnet out of quota for all roles"""
        config = self.metric_configs['softnet_out_of_quota']
        nodes = metric_data.get('nodes', {})
        
        for role in ['controlplane', 'worker', 'infra', 'workload']:
            if role not in nodes:
                continue
            
            table_key = f'softnet_out_of_quota_{role}'
            role_nodes = nodes[role]
            
            all_values = []
            for node_name, stats in role_nodes.items():
                avg_val = float(stats.get('avg', 0))
                max_val = float(stats.get('max', 0))
                all_values.append((node_name, avg_val, max_val))
            
            top_max = max((v[2] for v in all_values), default=0) if all_values else 0
            
            for node_name, avg_val, max_val in all_values:
                is_top = (max_val == top_max and max_val > 0)
                
                # Format as plain numbers for eps unit
                if avg_val >= config['thresholds']['critical']:
                    avg_display = f'<span class="text-danger font-weight-bold">⚠️ {avg_val:g}</span>'
                elif avg_val >= config['thresholds']['warning']:
                    avg_display = f'<span class="text-warning font-weight-bold">{avg_val:g}</span>'
                else:
                    avg_display = f'{avg_val:g}'
                
                if is_top:
                    max_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {max_val:g}</span>'
                elif max_val >= config['thresholds']['critical']:
                    max_display = f'<span class="text-danger font-weight-bold">⚠️ {max_val:g}</span>'
                elif max_val >= config['thresholds']['warning']:
                    max_display = f'<span class="text-warning font-weight-bold">{max_val:g}</span>'
                else:
                    max_display = f'{max_val:g}'
                
                structured[table_key].append({
                    'Metric Name': config['title'],
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Avg': avg_display,
                    'Max': max_display
                })
    
    def _extract_softnet_cpu_rps(self, metric_data: Dict[str, Any], 
                                 structured: Dict[str, Any]):
        """Extract softnet CPU RPS for all roles"""
        config = self.metric_configs['softnet_cpu_rps']
        nodes = metric_data.get('nodes', {})
        
        for role in ['controlplane', 'worker', 'infra', 'workload']:
            if role not in nodes:
                continue
            
            table_key = f'softnet_cpu_rps_{role}'
            role_nodes = nodes[role]
            
            all_values = []
            for node_name, stats in role_nodes.items():
                avg_val = float(stats.get('avg', 0))
                max_val = float(stats.get('max', 0))
                all_values.append((node_name, avg_val, max_val))
            
            top_max = max((v[2] for v in all_values), default=0) if all_values else 0
            
            for node_name, avg_val, max_val in all_values:
                is_top = (max_val == top_max and max_val > 0)
                
                avg_display = self.format_and_highlight(
                    avg_val, config['unit'], config['thresholds'], False
                )
                max_display = self.format_and_highlight(
                    max_val, config['unit'], config['thresholds'], is_top
                )
                
                structured[table_key].append({
                    'Metric Name': config['title'],
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Avg': avg_display,
                    'Max': max_display
                })
    
    def _extract_softnet_flow_limit_count(self, metric_data: Dict[str, Any], 
                                          structured: Dict[str, Any]):
        """Extract softnet flow limit count for all roles"""
        config = self.metric_configs['softnet_flow_limit_count']
        nodes = metric_data.get('nodes', {})
        
        for role in ['controlplane', 'worker', 'infra', 'workload']:
            if role not in nodes:
                continue
            
            table_key = f'softnet_flow_limit_count_{role}'
            role_nodes = nodes[role]
            
            all_values = []
            for node_name, stats in role_nodes.items():
                avg_val = float(stats.get('avg', 0))
                max_val = float(stats.get('max', 0))
                all_values.append((node_name, avg_val, max_val))
            
            top_max = max((v[2] for v in all_values), default=0) if all_values else 0
            
            for node_name, avg_val, max_val in all_values:
                is_top = (max_val == top_max and max_val > 0)
                
                # Format as count per second
                avg_display = self.format_and_highlight(
                    avg_val, 'count', config['thresholds'], False
                )
                max_display = self.format_and_highlight(
                    max_val, 'count', config['thresholds'], is_top
                )
                
                structured[table_key].append({
                    'Metric Name': config['title'],
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Avg': avg_display,
                    'Max': max_display
                })
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate softnet overview"""
        summary = data.get('summary', {})
        metrics_data = data.get('metrics', {})
        
        node_counts = summary.get('node_counts', {
            'controlplane': 0, 'worker': 0, 'infra': 0, 'workload': 0
        })
        
        for role in ['controlplane', 'worker', 'infra', 'workload']:
            node_count = node_counts.get(role, 0)
            if node_count == 0:
                continue
            
            # Count metrics with data for this role
            metrics_with_data = 0
            for metric_name, metric_data in metrics_data.items():
                if 'error' in metric_data:
                    continue
                role_nodes = metric_data.get('nodes', {}).get(role, {})
                if role_nodes:
                    metrics_with_data += 1
            
            structured['softnet_overview'].append({
                'Role': role.title(),
                'Nodes': node_count,
                'Metrics Collected': metrics_with_data,
                'Status': self.create_status_badge('success', 'Active')
            })
    
    def summarize_network_socket_softnet(self, data: Dict[str, Any]) -> str:
        """Generate network socket softnet summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('softnet_overview', [])
            
            # Fallback: synthesize overview if missing
            if not overview_data:
                roles = ['controlplane', 'infra', 'worker', 'workload']
                metric_prefixes = [
                    'softnet_processed_total', 'softnet_dropped_total',
                    'softnet_out_of_quota', 'softnet_cpu_rps', 'softnet_flow_limit_count'
                ]
                synthesized: List[Dict[str, Any]] = []
                for role in roles:
                    role_tables = [f"{p}_{role}" for p in metric_prefixes]
                    non_empty_tables = [
                        t for t in role_tables
                        if isinstance(data.get(t), list) and len(data.get(t)) > 0
                    ]
                    if not non_empty_tables:
                        continue
                    
                    first_table = data.get(non_empty_tables[0], [])
                    unique_nodes = {
                        str(row.get('Node')) for row in first_table 
                        if isinstance(row, dict) and 'Node' in row
                    }
                    synthesized.append({
                        'Role': role.title(),
                        'Nodes': len(unique_nodes),
                        'Metrics Collected': len(non_empty_tables),
                        'Status': self.create_status_badge('success', 'Active')
                    })
                if synthesized:
                    overview_data = synthesized
            
            total_nodes = sum(int(item.get('Nodes', 0)) for item in overview_data)
            if total_nodes > 0:
                summary_items.append(f"<li>Total Nodes: {total_nodes}</li>")
            
            # Role breakdown
            for item in overview_data:
                role = item.get('Role', 'Unknown')
                nodes = item.get('Nodes', 0)
                metrics = item.get('Metrics Collected', 0)
                if nodes > 0:
                    summary_items.append(
                        f"<li>{role}: {nodes} nodes, {metrics} softnet metrics</li>"
                    )
            
            return (
                "<div class=\"network-socket-softnet-summary\">"
                "<h4>Network Socket SoftNet Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate softnet summary: {e}")
            return "Network Socket SoftNet metrics collected"
    
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
            logger.error(f"Failed to transform softnet data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        # Define metric groups
        metric_groups = {
            'Softnet Processed Total': 'softnet_processed_total',
            'Softnet Dropped Total': 'softnet_dropped_total',
            'Softnet Out of Quota': 'softnet_out_of_quota',
            'Softnet CPU RPS': 'softnet_cpu_rps',
            'Softnet Flow Limit Count': 'softnet_flow_limit_count'
        }
        
        try:
            # Overview table first
            if 'softnet_overview' in dataframes and not dataframes['softnet_overview'].empty:
                html_tables['softnet_overview'] = self.create_html_table(
                    dataframes['softnet_overview'], 
                    'Network Socket SoftNet Overview'
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
            logger.error(f"Failed to generate HTML tables for softnet: {e}")
        
        return html_tables