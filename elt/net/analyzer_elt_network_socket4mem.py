"""
Extract, Load, Transform module for Network Socket Memory Metrics
Handles network socket memory data from tools/net/network_socket4mem.py
ONLY contains network_socket_mem specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class networkSocketMemELT(utilityELT):
    """Extract, Load, Transform class for network socket memory metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'node_sockstat_FRAG_memory': {
                'title': 'Socket FRAG Memory',
                'unit': 'count',
                'thresholds': {'critical': 1000, 'warning': 500}
            },
            'TCP_Kernel_Buffer_Memory_Pages': {
                'title': 'TCP Kernel Buffer Memory',
                'unit': 'count',
                'thresholds': {'critical': 1000, 'warning': 500}
            },
            'UDP_Kernel_Buffer_Memory_Pages': {
                'title': 'UDP Kernel Buffer Memory',
                'unit': 'count',
                'thresholds': {'critical': 1000, 'warning': 500}
            },
            'node_sockstat_TCP_mem_bytes': {
                'title': 'TCP Memory',
                'unit': 'bytes',
                'thresholds': {'critical': 1073741824, 'warning': 536870912}  # 1GB, 512MB
            },
            'node_sockstat_UDP_mem_bytes': {
                'title': 'UDP Memory',
                'unit': 'bytes',
                'thresholds': {'critical': 1073741824, 'warning': 536870912}  # 1GB, 512MB
            }
        }
    
    def extract_network_socket_mem(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract network socket memory information from network_socket4mem.py output"""
        
        # Handle nested data structure - extract actual metrics data
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'network_socket_mem_overview': [],
        }
        
        # Initialize all role-based tables for each metric
        metrics_list = actual_data.get('metrics', [])
        
        # Get unique roles from all metrics
        all_roles = set()
        for metric_item in metrics_list:
            if isinstance(metric_item, dict):
                nodes = metric_item.get('nodes', {})
                all_roles.update(nodes.keys())
        
        # Initialize tables for each metric and role combination
        for metric_item in metrics_list:
            if not isinstance(metric_item, dict):
                continue
            metric_name = metric_item.get('metric', '')
            if not metric_name:
                continue
            
            for role in all_roles:
                table_key = f'{metric_name}_{role}'
                structured[table_key] = []
        
        # Extract each metric
        for metric_item in metrics_list:
            if not isinstance(metric_item, dict):
                continue
            
            metric_name = metric_item.get('metric', '')
            if not metric_name:
                continue
            
            if metric_name == 'node_sockstat_FRAG_memory':
                self._extract_frag_memory(metric_item, structured)
            elif metric_name == 'TCP_Kernel_Buffer_Memory_Pages':
                self._extract_tcp_buffer_pages(metric_item, structured)
            elif metric_name == 'UDP_Kernel_Buffer_Memory_Pages':
                self._extract_udp_buffer_pages(metric_item, structured)
            elif metric_name == 'node_sockstat_TCP_mem_bytes':
                self._extract_tcp_mem_bytes(metric_item, structured)
            elif metric_name == 'node_sockstat_UDP_mem_bytes':
                self._extract_udp_mem_bytes(metric_item, structured)
        
        # Generate overview
        self._generate_overview(actual_data, structured)
        
        return structured

    def _extract_frag_memory(self, metric_data: Dict[str, Any], structured: Dict[str, Any]):
        """Extract socket FRAG memory metric"""
        self._extract_generic_socket_metric(
            metric_data, 
            structured, 
            'node_sockstat_FRAG_memory',
            'count'
        )
    
    def _extract_tcp_buffer_pages(self, metric_data: Dict[str, Any], structured: Dict[str, Any]):
        """Extract TCP kernel buffer memory pages metric"""
        self._extract_generic_socket_metric(
            metric_data, 
            structured, 
            'TCP_Kernel_Buffer_Memory_Pages',
            'count'
        )
    
    def _extract_udp_buffer_pages(self, metric_data: Dict[str, Any], structured: Dict[str, Any]):
        """Extract UDP kernel buffer memory pages metric"""
        self._extract_generic_socket_metric(
            metric_data, 
            structured, 
            'UDP_Kernel_Buffer_Memory_Pages',
            'count'
        )
    
    def _extract_tcp_mem_bytes(self, metric_data: Dict[str, Any], structured: Dict[str, Any]):
        """Extract TCP memory bytes metric"""
        self._extract_generic_socket_metric(
            metric_data, 
            structured, 
            'node_sockstat_TCP_mem_bytes',
            'bytes'
        )
    
    def _extract_udp_mem_bytes(self, metric_data: Dict[str, Any], structured: Dict[str, Any]):
        """Extract UDP memory bytes metric"""
        self._extract_generic_socket_metric(
            metric_data, 
            structured, 
            'node_sockstat_UDP_mem_bytes',
            'bytes'
        )
    
    def _extract_generic_socket_metric(self, metric_data: Dict[str, Any], 
                                      structured: Dict[str, Any], 
                                      metric_name: str,
                                      unit: str):
        """Generic extraction for socket memory metrics"""
        nodes = metric_data.get('nodes', {})
        config = self.metric_configs.get(metric_name, {})
        thresholds = config.get('thresholds', {})
        
        # Collect all values for top identification
        all_values = []
        for role, role_nodes in nodes.items():
            if not isinstance(role_nodes, dict):
                continue
            for node_name, node_stats in role_nodes.items():
                if not isinstance(node_stats, dict):
                    continue
                max_val = float(node_stats.get('max', 0))
                all_values.append((role, node_name, node_stats, max_val))
        
        # Find top 1 by max value
        top_max = max((v[3] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        for role, node_name, node_stats, max_val in all_values:
            table_key = f'{metric_name}_{role}'
            
            avg_val = float(node_stats.get('avg', 0))
            is_top = (max_val == top_max and max_val > 0)
            
            # Format avg
            avg_display = self.format_and_highlight(avg_val, unit, thresholds, False)
            
            # Format max with top highlight
            max_display = self.format_and_highlight(max_val, unit, thresholds, is_top)
            
            # Get metric title from config
            metric_title = config.get('title', metric_name)
            
            structured[table_key].append({
                'Metric Name': metric_title,
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Avg': avg_display,
                'Max': max_display,
                'Unit': unit
            })
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate network socket memory overview"""
        metrics_list = data.get('metrics', [])
        
        # Count metrics per role
        role_metrics = {}
        role_nodes = {}
        
        for metric_item in metrics_list:
            if not isinstance(metric_item, dict):
                continue
            
            metric_name = metric_item.get('metric', '')
            nodes = metric_item.get('nodes', {})
            
            for role, role_nodes_dict in nodes.items():
                if role not in role_metrics:
                    role_metrics[role] = set()
                    role_nodes[role] = set()
                
                role_metrics[role].add(metric_name)
                if isinstance(role_nodes_dict, dict):
                    role_nodes[role].update(role_nodes_dict.keys())
        
        # Generate overview rows
        for role in sorted(role_metrics.keys()):
            structured['network_socket_mem_overview'].append({
                'Role': role.title(),
                'Nodes': len(role_nodes[role]),
                'Metrics Collected': len(role_metrics[role]),
                'Status': self.create_status_badge('success', 'Active')
            })
    
    def summarize_network_socket_mem(self, data: Dict[str, Any]) -> str:
        """Generate network socket memory summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('network_socket_mem_overview', [])
            
            # Fallback: synthesize overview if missing
            if not overview_data:
                # Try to extract from other tables
                roles = set()
                for key in data.keys():
                    if key.endswith(('_controlplane', '_worker', '_infra', '_workload')):
                        role = key.split('_')[-1]
                        roles.add(role)
                
                if roles:
                    synthesized = []
                    for role in roles:
                        role_tables = [k for k in data.keys() if k.endswith(f'_{role}')]
                        non_empty = [t for t in role_tables if isinstance(data.get(t), list) and len(data.get(t)) > 0]
                        
                        if non_empty:
                            first_table = data.get(non_empty[0], [])
                            unique_nodes = {str(row.get('Node')) for row in first_table if isinstance(row, dict) and 'Node' in row}
                            synthesized.append({
                                'Role': role.title(),
                                'Nodes': len(unique_nodes),
                                'Metrics Collected': len(non_empty),
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
                "<div class=\"network-socket-mem-summary\">"
                "<h4>Network Socket Memory Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate network socket memory summary: {e}")
            return f"Network socket memory metrics collected from {len(data)} tables"
    
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
            logger.error(f"Failed to transform network socket memory data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        # Define metric order
        metric_names = [
            'node_sockstat_FRAG_memory',
            'TCP_Kernel_Buffer_Memory_Pages',
            'UDP_Kernel_Buffer_Memory_Pages',
            'node_sockstat_TCP_mem_bytes',
            'node_sockstat_UDP_mem_bytes'
        ]
        
        try:
            # Overview table first
            if 'network_socket_mem_overview' in dataframes and not dataframes['network_socket_mem_overview'].empty:
                html_tables['network_socket_mem_overview'] = self.create_html_table(
                    dataframes['network_socket_mem_overview'], 
                    'Network Socket Memory Overview'
                )
            
            # Generate tables for each metric and role
            for metric_name in metric_names:
                config = self.metric_configs.get(metric_name, {})
                display_name = config.get('title', metric_name)
                
                for role in ['controlplane', 'infra', 'worker', 'workload']:
                    table_key = f'{metric_name}_{role}'
                    if table_key in dataframes and not dataframes[table_key].empty:
                        full_display_name = f"{display_name} - {role.title()}"
                        html_tables[table_key] = self.create_html_table(
                            dataframes[table_key], 
                            full_display_name
                        )
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for network socket memory: {e}")
        
        return html_tables