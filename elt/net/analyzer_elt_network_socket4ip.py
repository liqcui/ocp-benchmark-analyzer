"""
Extract, Load, Transform module for Network Socket IP Metrics
Handles network socket IP data from tools/net/network_socket4ip.py
ONLY contains network_socket_ip specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class networkSocketIPELT(utilityELT):
    """Extract, Load, Transform class for network socket IP metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'netstat_ip_in_octets': {
                'title': 'IP Incoming Octets',
                'unit': 'bytes_per_second',
                'thresholds': {'critical': 1000000000, 'warning': 500000000}  # 1GB/s, 500MB/s
            },
            'netstat_ip_out_octets': {
                'title': 'IP Outgoing Octets',
                'unit': 'bytes_per_second',
                'thresholds': {'critical': 1000000000, 'warning': 500000000}
            },
            'node_netstat_Icmp_InMsgs': {
                'title': 'ICMP Incoming Messages',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 1000, 'warning': 500}
            },
            'node_netstat_Icmp_OutMsgs': {
                'title': 'ICMP Outgoing Messages',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 1000, 'warning': 500}
            },
            'node_netstat_Icmp_InErrors': {
                'title': 'ICMP Incoming Errors',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 100, 'warning': 50}
            }
        }
    
    def extract_network_socket_ip(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract network socket IP information from network_socket4ip.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'network_socket_ip_overview': [],
        }
        
        # Initialize all role-based tables for each metric
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            for metric_name in self.metric_configs.keys():
                structured[f'{metric_name}_{role}'] = []
        
        metrics = actual_data.get('metrics', {})
        
        # Extract each metric type
        for metric_name, metric_data in metrics.items():
            if 'error' in metric_data:
                continue
            
            if metric_name == 'netstat_ip_in_octets':
                self._extract_ip_in_octets(metric_data, structured)
            elif metric_name == 'netstat_ip_out_octets':
                self._extract_ip_out_octets(metric_data, structured)
            elif metric_name == 'node_netstat_Icmp_InMsgs':
                self._extract_icmp_in_msgs(metric_data, structured)
            elif metric_name == 'node_netstat_Icmp_OutMsgs':
                self._extract_icmp_out_msgs(metric_data, structured)
            elif metric_name == 'node_netstat_Icmp_InErrors':
                self._extract_icmp_in_errors(metric_data, structured)
        
        # Generate overview
        self._generate_overview(actual_data, structured)
        
        return structured

    def _extract_ip_in_octets(self, metric_data: Dict[str, Any], structured: Dict[str, Any]):
        """Extract IP incoming octets for all roles"""
        metric_name = 'netstat_ip_in_octets'
        config = self.metric_configs[metric_name]
        nodes = metric_data.get('nodes', {})
        
        for role in ['controlplane', 'worker', 'infra', 'workload']:
            if role not in nodes:
                continue
            
            table_key = f'{metric_name}_{role}'
            role_nodes = nodes[role]
            
            # Collect all values for top identification
            all_values = [(node_data['node'], node_data['avg'], node_data['max']) 
                         for node_data in role_nodes]
            
            # Find top 1 by max value
            top_max = max((v[2] for v in all_values), default=0) if all_values else 0
            
            # Format rows
            for node_name, avg_val, max_val in all_values:
                is_top = (max_val == top_max and max_val > 0)
                
                # Format avg
                avg_display = self.format_and_highlight(
                    avg_val, config['unit'], config['thresholds'], False
                )
                
                # Format max with top highlight
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
    
    def _extract_ip_out_octets(self, metric_data: Dict[str, Any], structured: Dict[str, Any]):
        """Extract IP outgoing octets for all roles"""
        metric_name = 'netstat_ip_out_octets'
        config = self.metric_configs[metric_name]
        nodes = metric_data.get('nodes', {})
        
        for role in ['controlplane', 'worker', 'infra', 'workload']:
            if role not in nodes:
                continue
            
            table_key = f'{metric_name}_{role}'
            role_nodes = nodes[role]
            
            # Collect all values for top identification
            all_values = [(node_data['node'], node_data['avg'], node_data['max']) 
                         for node_data in role_nodes]
            
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
    
    def _extract_icmp_in_msgs(self, metric_data: Dict[str, Any], structured: Dict[str, Any]):
        """Extract ICMP incoming messages for all roles"""
        metric_name = 'node_netstat_Icmp_InMsgs'
        config = self.metric_configs[metric_name]
        nodes = metric_data.get('nodes', {})
        
        for role in ['controlplane', 'worker', 'infra', 'workload']:
            if role not in nodes:
                continue
            
            table_key = f'{metric_name}_{role}'
            role_nodes = nodes[role]
            
            # Collect all values for top identification
            all_values = [(node_data['node'], node_data['avg'], node_data['max']) 
                         for node_data in role_nodes]
            
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
    
    def _extract_icmp_out_msgs(self, metric_data: Dict[str, Any], structured: Dict[str, Any]):
        """Extract ICMP outgoing messages for all roles"""
        metric_name = 'node_netstat_Icmp_OutMsgs'
        config = self.metric_configs[metric_name]
        nodes = metric_data.get('nodes', {})
        
        for role in ['controlplane', 'worker', 'infra', 'workload']:
            if role not in nodes:
                continue
            
            table_key = f'{metric_name}_{role}'
            role_nodes = nodes[role]
            
            # Collect all values for top identification
            all_values = [(node_data['node'], node_data['avg'], node_data['max']) 
                         for node_data in role_nodes]
            
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
                    'Node': self.truncate_node_name(node_name),
                    'Avg': avg_display,
                    'Max': max_display
                })
    
    def _extract_icmp_in_errors(self, metric_data: Dict[str, Any], structured: Dict[str, Any]):
        """Extract ICMP incoming errors for all roles"""
        metric_name = 'node_netstat_Icmp_InErrors'
        config = self.metric_configs[metric_name]
        nodes = metric_data.get('nodes', {})
        
        for role in ['controlplane', 'worker', 'infra', 'workload']:
            if role not in nodes:
                continue
            
            table_key = f'{metric_name}_{role}'
            role_nodes = nodes[role]
            
            # Collect all values for top identification
            all_values = [(node_data['node'], node_data['avg'], node_data['max']) 
                         for node_data in role_nodes]
            
            # Find top 1 by max value
            top_max = max((v[2] for v in all_values), default=0) if all_values else 0
            
            # Format rows
            for node_name, avg_val, max_val in all_values:
                is_top = (max_val == top_max and max_val > 0)
                
                # Errors should always be highlighted if > 0
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
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate network socket IP overview"""
        node_groups_summary = data.get('node_groups_summary', {})
        metrics = data.get('metrics', {})
        
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            node_count = node_groups_summary.get(role, 0)
            if node_count == 0:
                continue
            
            # Count metrics with data for this role
            metrics_count = 0
            for metric_name in self.metric_configs.keys():
                metric_data = metrics.get(metric_name, {})
                if 'nodes' in metric_data and role in metric_data['nodes']:
                    if metric_data['nodes'][role]:
                        metrics_count += 1
            
            structured['network_socket_ip_overview'].append({
                'Role': role.title(),
                'Nodes': node_count,
                'Metrics Collected': metrics_count,
                'Status': self.create_status_badge('success', 'Active')
            })
    
    def summarize_network_socket_ip(self, data: Dict[str, Any]) -> str:
        """Generate network socket IP summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('network_socket_ip_overview', [])
            
            # Fallback: synthesize overview if missing
            if not overview_data:
                roles = ['controlplane', 'infra', 'worker', 'workload']
                synthesized: List[Dict[str, Any]] = []
                
                for role in roles:
                    role_tables = [f"{metric_name}_{role}" for metric_name in self.metric_configs.keys()]
                    non_empty_tables = [
                        t for t in role_tables
                        if isinstance(data.get(t), list) and len(data.get(t)) > 0
                    ]
                    
                    if not non_empty_tables:
                        continue
                    
                    # Count unique nodes from first available table
                    first_table = data.get(non_empty_tables[0], [])
                    unique_nodes = {str(row.get('Node')) for row in first_table 
                                   if isinstance(row, dict) and 'Node' in row}
                    
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
                    summary_items.append(f"<li>{role}: {nodes} nodes, {metrics} metrics</li>")
            
            return (
                "<div class=\"network-socket-ip-summary\">"
                "<h4>Network Socket IP Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate network socket IP summary: {e}")
            return f"Network Socket IP metrics collected from {len(data)} metric types"
    
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
            logger.error(f"Failed to transform network socket IP data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        try:
            # Overview table first
            if 'network_socket_ip_overview' in dataframes and not dataframes['network_socket_ip_overview'].empty:
                html_tables['network_socket_ip_overview'] = self.create_html_table(
                    dataframes['network_socket_ip_overview'], 
                    'Network Socket IP Overview'
                )
            
            # Generate tables for each metric and role
            for metric_name, config in self.metric_configs.items():
                for role in ['controlplane', 'infra', 'worker', 'workload']:
                    table_key = f'{metric_name}_{role}'
                    if table_key in dataframes and not dataframes[table_key].empty:
                        display_name = f"{config['title']} - {role.title()}"
                        html_tables[table_key] = self.create_html_table(
                            dataframes[table_key], 
                            display_name
                        )
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for network socket IP: {e}")
        
        return html_tables