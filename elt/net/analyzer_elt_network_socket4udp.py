"""
Extract, Load, Transform module for Network Socket UDP Metrics
Handles network socket UDP data from tools/net/network_socket4udp.py
ONLY contains network_socket_udp specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class networkSocketUDPELT(utilityELT):
    """Extract, Load, Transform class for network socket UDP metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'socket_udp_inuse': {
                'title': 'UDP Sockets In Use',
                'unit': 'count',
                'thresholds': {'critical': 1000, 'warning': 500}
            },
            'socket_udp_lite_inuse': {
                'title': 'UDP Lite Sockets In Use',
                'unit': 'count',
                'thresholds': {'critical': 100, 'warning': 50}
            }
        }
    
    def extract_network_socket_udp(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract network socket UDP information from network_socket4udp.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'network_socket_udp_overview': [],
        }
        
        # Initialize all role-based tables for each metric
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            structured[f'socket_udp_inuse_{role}'] = []
            structured[f'socket_udp_lite_inuse_{role}'] = []
        
        metrics = actual_data.get('metrics', [])
        
        # Process each metric
        for metric_item in metrics:
            if not isinstance(metric_item, dict):
                continue
            
            metric_name = metric_item.get('metric', '')
            
            if metric_name == 'socket_udp_inuse':
                self._extract_socket_udp_inuse(metric_item, structured)
            elif metric_name == 'socket_udp_lite_inuse':
                self._extract_socket_udp_lite_inuse(metric_item, structured)
        
        # Generate overview
        self._generate_overview(actual_data, structured)
        
        return structured

    def summarize_network_socket_udp(self, data: Dict[str, Any]) -> str:
        """Generate network socket UDP summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('network_socket_udp_overview', [])
            
            # Fallback: synthesize overview if missing
            if not overview_data:
                roles = ['controlplane', 'infra', 'worker', 'workload']
                metric_prefixes = ['socket_udp_inuse', 'socket_udp_lite_inuse']
                synthesized: List[Dict[str, Any]] = []
                
                for role in roles:
                    role_tables = [f"{p}_{role}" for p in metric_prefixes]
                    non_empty_tables = [
                        t for t in role_tables
                        if isinstance(data.get(t), list) and len(data.get(t)) > 0
                    ]
                    if not non_empty_tables:
                        continue
                    
                    # Derive node count from the first available role table
                    first_table = data.get(non_empty_tables[0], [])
                    unique_nodes = {str(row.get('Node')) for row in first_table if isinstance(row, dict) and 'Node' in row}
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
                "<div class=\"network-socket-udp-summary\">"
                "<h4>Network Socket UDP Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate network socket UDP summary: {e}")
            return f"Network Socket UDP metrics collected from {len(data)} role groups"

    def _extract_socket_udp_inuse(self, metric_item: Dict[str, Any], 
                                   structured: Dict[str, Any]):
        """Extract UDP inuse socket statistics for all roles"""
        metric_data = metric_item.get('data', {})
        unit = metric_item.get('unit', 'count')
        thresholds = self.metric_configs['socket_udp_inuse']['thresholds']
        
        # Collect all values for top identification
        all_values = []
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            nodes = metric_data.get(role, [])
            for node_info in nodes:
                if isinstance(node_info, dict):
                    max_val = float(node_info.get('max', 0))
                    all_values.append((role, node_info, max_val))
        
        # Find top 1 by max value
        top_max = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows for each role
        for role, node_info, max_val in all_values:
            table_key = f'socket_udp_inuse_{role}'
            is_top = (max_val == top_max and max_val > 0)
            
            node_name = node_info.get('node', '')
            avg_val = float(node_info.get('avg', 0))
            
            # Format avg
            avg_display = self.format_and_highlight(avg_val, unit, thresholds, False)
            
            # Format max with top highlight
            max_display = self.format_and_highlight(max_val, unit, thresholds, is_top)
            
            structured[table_key].append({
                'Metric Name': self.metric_configs['socket_udp_inuse']['title'],
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Avg Usage': avg_display,
                'Max Usage': max_display,
                'Unit': unit
            })
    
    def _extract_socket_udp_lite_inuse(self, metric_item: Dict[str, Any], 
                                       structured: Dict[str, Any]):
        """Extract UDP lite inuse socket statistics for all roles"""
        metric_data = metric_item.get('data', {})
        unit = metric_item.get('unit', 'count')
        thresholds = self.metric_configs['socket_udp_lite_inuse']['thresholds']
        
        # Collect all values for top identification
        all_values = []
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            nodes = metric_data.get(role, [])
            for node_info in nodes:
                if isinstance(node_info, dict):
                    max_val = float(node_info.get('max', 0))
                    all_values.append((role, node_info, max_val))
        
        # Find top 1 by max value
        top_max = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows for each role
        for role, node_info, max_val in all_values:
            table_key = f'socket_udp_lite_inuse_{role}'
            is_top = (max_val == top_max and max_val > 0)
            
            node_name = node_info.get('node', '')
            avg_val = float(node_info.get('avg', 0))
            
            # Format avg
            avg_display = self.format_and_highlight(avg_val, unit, thresholds, False)
            
            # Format max with top highlight
            max_display = self.format_and_highlight(max_val, unit, thresholds, is_top)
            
            structured[table_key].append({
                'Metric Name': self.metric_configs['socket_udp_lite_inuse']['title'],
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Avg Usage': avg_display,
                'Max Usage': max_display,
                'Unit': unit
            })
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate network socket UDP overview"""
        metrics = data.get('metrics', [])
        
        # Count metrics and nodes per role
        role_stats = {
            'controlplane': {'nodes': set(), 'metrics': 0},
            'infra': {'nodes': set(), 'metrics': 0},
            'worker': {'nodes': set(), 'metrics': 0},
            'workload': {'nodes': set(), 'metrics': 0}
        }
        
        for metric_item in metrics:
            if not isinstance(metric_item, dict):
                continue
            
            metric_data = metric_item.get('data', {})
            for role in ['controlplane', 'infra', 'worker', 'workload']:
                nodes = metric_data.get(role, [])
                if nodes:
                    role_stats[role]['metrics'] += 1
                    for node_info in nodes:
                        if isinstance(node_info, dict):
                            node_name = node_info.get('node', '')
                            if node_name:
                                role_stats[role]['nodes'].add(node_name)
        
        # Generate overview rows
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            stats = role_stats[role]
            node_count = len(stats['nodes'])
            metrics_count = stats['metrics']
            
            if node_count > 0 or metrics_count > 0:
                structured['network_socket_udp_overview'].append({
                    'Role': role.title(),
                    'Nodes': node_count,
                    'Metrics Collected': metrics_count,
                    'Status': self.create_status_badge('success', 'Active')
                })
      
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
            logger.error(f"Failed to transform network socket UDP data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        # Define metric groups
        metric_groups = {
            'UDP Sockets In Use': 'socket_udp_inuse',
            'UDP Lite Sockets In Use': 'socket_udp_lite_inuse'
        }
        
        try:
            # Overview table first
            if 'network_socket_udp_overview' in dataframes and not dataframes['network_socket_udp_overview'].empty:
                html_tables['network_socket_udp_overview'] = self.create_html_table(
                    dataframes['network_socket_udp_overview'], 
                    'Network Socket UDP Overview'
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
            logger.error(f"Failed to generate HTML tables for network socket UDP: {e}")
        
        return html_tables