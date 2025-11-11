"""
Extract, Load, Transform module for Network Netstat TCP Metrics
Handles network TCP netstat data from tools/net/network_netstat4tcp.py
ONLY contains network_netstat_tcp specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class networkNetstatTCPELT(utilityELT):
    """Extract, Load, Transform class for network TCP netstat metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'netstat_tcp_in': {
                'title': 'TCP In Segments',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 100000, 'warning': 50000}
            },
            'netstat_tcp_out': {
                'title': 'TCP Out Segments',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 100000, 'warning': 50000}
            },
            'netstat_tcp_error_listen_overflow': {
                'title': 'TCP Listen Overflow Errors',
                'unit': 'count',
                'thresholds': {'critical': 100, 'warning': 10}
            },
            'netstat_tcp_error_listen_drops': {
                'title': 'TCP Listen Drops',
                'unit': 'count',
                'thresholds': {'critical': 100, 'warning': 10}
            },
            'netstat_tcp_error_sync_retrans': {
                'title': 'TCP SYN Retransmissions',
                'unit': 'count',
                'thresholds': {'critical': 1000, 'warning': 100}
            },
            'netstat_tcp_error_segments_retrans': {
                'title': 'TCP Segment Retransmissions',
                'unit': 'count',
                'thresholds': {'critical': 1000, 'warning': 100}
            },
            'netstat_tcp_error_receive_in_errors': {
                'title': 'TCP Receive Errors',
                'unit': 'count',
                'thresholds': {'critical': 100, 'warning': 10}
            },
            'netstat_tcp_error_rst_sent_out_errors': {
                'title': 'TCP RST Sent',
                'unit': 'count',
                'thresholds': {'critical': 1000, 'warning': 100}
            },
            'netstat_tcp_error_receive_quene_drop': {
                'title': 'TCP Receive Queue Drops',
                'unit': 'count',
                'thresholds': {'critical': 100, 'warning': 10}
            },
            'netstat_tcp_error_out_order_queue': {
                'title': 'TCP Out of Order Queue',
                'unit': 'count',
                'thresholds': {'critical': 1000, 'warning': 100}
            },
            'netstat_tcp_error_tcp_timeouts': {
                'title': 'TCP Timeouts',
                'unit': 'count',
                'thresholds': {'critical': 100, 'warning': 10}
            },
            'node_tcp_sync_cookie_failures': {
                'title': 'TCP SYN Cookie Failures',
                'unit': 'count',
                'thresholds': {'critical': 100, 'warning': 10}
            },
            'node_tcp_sync_cookie_validated': {
                'title': 'TCP SYN Cookie Validated',
                'unit': 'count',
                'thresholds': {'critical': 10000, 'warning': 1000}
            },
            'node_tcp_sync_cookie_sent': {
                'title': 'TCP SYN Cookie Sent',
                'unit': 'count',
                'thresholds': {'critical': 10000, 'warning': 1000}
            },
            'node_netstat_Tcp_MaxConn': {
                'title': 'TCP Max Connections',
                'unit': 'count',
                'thresholds': {'critical': 100000, 'warning': 50000}
            },
            'node_netstat_Tcp_CurrEstab': {
                'title': 'TCP Current Established',
                'unit': 'count',
                'thresholds': {'critical': 50000, 'warning': 20000}
            }
        }
    
    def extract_network_netstat_tcp(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract network TCP netstat information from network_netstat4tcp.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'netstat_tcp_overview': [],
        }
        
        # Initialize all role-based tables for each metric
        metrics_data = actual_data.get('metrics', {})
        
        for metric_name in metrics_data.keys():
            for role in ['controlplane', 'infra', 'worker', 'workload']:
                table_key = f'{metric_name}_{role}'
                structured[table_key] = []
        
        # Extract each metric
        for metric_name, metric_data in metrics_data.items():
            if 'error' in metric_data:
                continue
            
            self._extract_metric_by_roles(metric_name, metric_data, structured)
        
        # Generate overview
        self._generate_overview(actual_data, structured)
        
        return structured

    def _extract_metric_by_roles(self, metric_name: str, metric_data: Dict[str, Any], 
                                 structured: Dict[str, Any]):
        """Extract metric data grouped by roles"""
        
        metric_config = self.metric_configs.get(metric_name, {})
        unit = metric_config.get('unit', 'count')
        thresholds = metric_config.get('thresholds', {})
        
        role_data = metric_data.get('data', {})
        
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            if role not in role_data:
                continue
            
            nodes_list = role_data[role]
            if not nodes_list:
                continue
            
            # Collect all values for top identification
            all_values = [(node['node'], node['avg'], node['max']) for node in nodes_list]
            
            # Find top 1 by max value
            top_max = max((v[2] for v in all_values), default=0) if all_values else 0
            
            table_key = f'{metric_name}_{role}'
            
            for node_name, avg_val, max_val in all_values:
                is_top = (max_val == top_max and max_val > 0)
                
                # Format avg
                avg_display = self.format_and_highlight(avg_val, unit, thresholds, False)
                
                # Format max with top highlight
                if is_top:
                    max_readable = self.format_value_with_unit(max_val, unit)
                    max_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {max_readable}</span>'
                else:
                    max_display = self.format_and_highlight(max_val, unit, thresholds, False)
                
                # Get metric title from config
                metric_title = metric_config.get('title', metric_name)
                
                structured[table_key].append({
                    'Metric Name': metric_title,
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Avg': avg_display,
                    'Max': max_display,
                    'Unit': unit
                })
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate TCP netstat overview"""
        
        metrics_data = data.get('metrics', {})
        total_metrics = len([m for m in metrics_data.values() if 'error' not in m])
        
        role_summary = {}
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            nodes_set = set()
            for metric_name, metric_data in metrics_data.items():
                if 'error' in metric_data:
                    continue
                role_data = metric_data.get('data', {}).get(role, [])
                for node in role_data:
                    nodes_set.add(node['node'])
            
            if nodes_set:
                role_summary[role] = {
                    'nodes': len(nodes_set),
                    'metrics': total_metrics
                }
        
        for role, info in role_summary.items():
            structured['netstat_tcp_overview'].append({
                'Role': role.title(),
                'Nodes': info['nodes'],
                'Metrics Collected': info['metrics'],
                'Status': self.create_status_badge('success', 'Active')
            })
    
    def summarize_network_netstat_tcp(self, data: Dict[str, Any]) -> str:
        """Generate network TCP netstat summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('netstat_tcp_overview', [])
            
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
                "<div class=\"network-netstat-tcp-summary\">"
                "<h4>Network TCP Netstat Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate network TCP netstat summary: {e}")
            return f"Network TCP netstat metrics collected from {len(data)} role groups"
    
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
            logger.error(f"Failed to transform network TCP netstat data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        try:
            # Overview table first
            if 'netstat_tcp_overview' in dataframes and not dataframes['netstat_tcp_overview'].empty:
                html_tables['netstat_tcp_overview'] = self.create_html_table(
                    dataframes['netstat_tcp_overview'], 
                    'Network TCP Netstat Overview'
                )
            
            # Generate tables for each metric and role
            for metric_name, metric_config in self.metric_configs.items():
                metric_title = metric_config['title']
                
                for role in ['controlplane', 'infra', 'worker', 'workload']:
                    table_key = f'{metric_name}_{role}'
                    if table_key in dataframes and not dataframes[table_key].empty:
                        display_name = f"{metric_title} - {role.title()}"
                        html_tables[table_key] = self.create_html_table(
                            dataframes[table_key], 
                            display_name
                        )
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for network TCP netstat: {e}")
        
        return html_tables