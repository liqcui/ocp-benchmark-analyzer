"""
Extract, Load, Transform module for Network Netstat UDP Metrics
Handles network UDP netstat data from tools/net/network_netstat4udp.py
ONLY contains network_netstat_udp specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class networkNetstatUDPELT(utilityELT):
    """Extract, Load, Transform class for network UDP netstat metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'udp_error_rx_in_errors': {
                'title': 'UDP RX In Errors',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 100.0, 'warning': 50.0}
            },
            'udp_error_no_listen': {
                'title': 'UDP No Listen Errors',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 100.0, 'warning': 50.0}
            },
            'udp_error_lite_rx_in_errors': {
                'title': 'UDP Lite RX In Errors',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 50.0, 'warning': 25.0}
            },
            'udp_error_rx_in_buffer_errors': {
                'title': 'UDP RX Buffer Errors',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 100.0, 'warning': 50.0}
            },
            'udp_error_tx_buffer_errors': {
                'title': 'UDP TX Buffer Errors',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 100.0, 'warning': 50.0}
            },
            'nestat_udp_in': {
                'title': 'UDP In',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 10000.0, 'warning': 5000.0}
            },
            'netstat_udp_out': {
                'title': 'UDP Out',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 10000.0, 'warning': 5000.0}
            }
        }
    
    def extract_network_netstat_udp(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract network UDP netstat information from network_netstat4udp.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'network_netstat_udp_overview': [],
        }
        
        # Initialize all role-based tables for each metric
        for metric_name in self.metric_configs.keys():
            for role in ['controlplane', 'infra', 'worker', 'workload']:
                structured[f'{metric_name}_{role}'] = []
        
        metrics_list = actual_data.get('metrics', [])
        
        # Process each metric
        for metric_data in metrics_list:
            if not isinstance(metric_data, dict):
                continue
            
            metric_name = metric_data.get('metric', '')
            
            if metric_name == 'udp_error_rx_in_errors':
                self._extract_udp_error_rx_in_errors(metric_data, structured)
            elif metric_name == 'udp_error_no_listen':
                self._extract_udp_error_no_listen(metric_data, structured)
            elif metric_name == 'udp_error_lite_rx_in_errors':
                self._extract_udp_error_lite_rx_in_errors(metric_data, structured)
            elif metric_name == 'udp_error_rx_in_buffer_errors':
                self._extract_udp_error_rx_in_buffer_errors(metric_data, structured)
            elif metric_name == 'udp_error_tx_buffer_errors':
                self._extract_udp_error_tx_buffer_errors(metric_data, structured)
            elif metric_name == 'nestat_udp_in':
                self._extract_nestat_udp_in(metric_data, structured)
            elif metric_name == 'netstat_udp_out':
                self._extract_netstat_udp_out(metric_data, structured)
        
        # Generate overview
        self._generate_overview(actual_data, structured)
        
        return structured
    
    def _extract_udp_error_rx_in_errors(self, metric_data: Dict[str, Any], 
                                        structured: Dict[str, Any]):
        """Extract UDP RX in errors metric for all roles"""
        self._extract_generic_metric(metric_data, structured, 'udp_error_rx_in_errors')
    
    def _extract_udp_error_no_listen(self, metric_data: Dict[str, Any], 
                                     structured: Dict[str, Any]):
        """Extract UDP no listen errors metric for all roles"""
        self._extract_generic_metric(metric_data, structured, 'udp_error_no_listen')
    
    def _extract_udp_error_lite_rx_in_errors(self, metric_data: Dict[str, Any], 
                                             structured: Dict[str, Any]):
        """Extract UDP Lite RX in errors metric for all roles"""
        self._extract_generic_metric(metric_data, structured, 'udp_error_lite_rx_in_errors')
    
    def _extract_udp_error_rx_in_buffer_errors(self, metric_data: Dict[str, Any], 
                                               structured: Dict[str, Any]):
        """Extract UDP RX buffer errors metric for all roles"""
        self._extract_generic_metric(metric_data, structured, 'udp_error_rx_in_buffer_errors')
    
    def _extract_udp_error_tx_buffer_errors(self, metric_data: Dict[str, Any], 
                                            structured: Dict[str, Any]):
        """Extract UDP TX buffer errors metric for all roles"""
        self._extract_generic_metric(metric_data, structured, 'udp_error_tx_buffer_errors')
    
    def _extract_nestat_udp_in(self, metric_data: Dict[str, Any], 
                              structured: Dict[str, Any]):
        """Extract UDP in metric for all roles"""
        self._extract_generic_metric(metric_data, structured, 'nestat_udp_in')
    
    def _extract_netstat_udp_out(self, metric_data: Dict[str, Any], 
                                 structured: Dict[str, Any]):
        """Extract UDP out metric for all roles"""
        self._extract_generic_metric(metric_data, structured, 'netstat_udp_out')
    
    def _extract_generic_metric(self, metric_data: Dict[str, Any], 
                               structured: Dict[str, Any], 
                               metric_name: str):
        """Generic extraction for UDP netstat metrics"""
        
        unit = metric_data.get('unit', 'packets_per_second')
        groups = metric_data.get('groups', {})
        
        metric_config = self.metric_configs.get(metric_name, {})
        thresholds = metric_config.get('thresholds', {'critical': 100.0, 'warning': 50.0})
        
        # Collect all values to identify top 1
        all_values = []
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            if role not in groups:
                continue
            
            role_data = groups[role]
            nodes = role_data.get('nodes', {})
            
            for node_name, value in nodes.items():
                all_values.append((role, node_name, float(value)))
        
        # Find top 1 value
        top_value = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        for role, node_name, value in all_values:
            table_key = f'{metric_name}_{role}'
            is_top = (value == top_value and value > 0)
            
            # Get role-level stats
            role_data = groups.get(role, {})
            avg = role_data.get('avg', 0)
            max_val = role_data.get('max', 0)
            
            # Format value with highlighting
            value_display = self.format_and_highlight(value, unit, thresholds, is_top)
            avg_display = self.format_value_with_unit(avg, unit)
            max_display = self.format_value_with_unit(max_val, unit)
            
            structured[table_key].append({
                'Metric Name': metric_config.get('title', metric_name),
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Value': value_display,
                'Avg': avg_display,
                'Max': max_display
            })
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate network UDP netstat overview"""
        
        metrics_list = data.get('metrics', [])
        
        # Count metrics and nodes per role
        role_stats = {}
        
        for metric_data in metrics_list:
            if not isinstance(metric_data, dict):
                continue
            
            groups = metric_data.get('groups', {})
            
            for role in ['controlplane', 'infra', 'worker', 'workload']:
                if role not in groups:
                    continue
                
                if role not in role_stats:
                    role_stats[role] = {
                        'nodes': set(),
                        'metrics': set()
                    }
                
                role_data = groups[role]
                nodes = role_data.get('nodes', {})
                
                role_stats[role]['nodes'].update(nodes.keys())
                role_stats[role]['metrics'].add(metric_data.get('metric', ''))
        
        # Generate overview rows
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            if role in role_stats:
                stats = role_stats[role]
                structured['network_netstat_udp_overview'].append({
                    'Role': role.title(),
                    'Nodes': len(stats['nodes']),
                    'Metrics Collected': len(stats['metrics']),
                    'Status': self.create_status_badge('success', 'Active')
                })
    
    def summarize_network_netstat_udp(self, data: Dict[str, Any]) -> str:
        """Generate network UDP netstat summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('network_netstat_udp_overview', [])
            
            # Fallback: synthesize overview if missing
            if not overview_data:
                roles_found = set()
                for key in data.keys():
                    if key.startswith(('udp_error_', 'nestat_udp_', 'netstat_udp_')):
                        parts = key.rsplit('_', 1)
                        if len(parts) == 2:
                            roles_found.add(parts[1])
                
                for role in ['controlplane', 'infra', 'worker', 'workload']:
                    if role not in roles_found:
                        continue
                    
                    # Count metrics for this role
                    metric_count = sum(
                        1 for key in data.keys() 
                        if key.endswith(f'_{role}') and isinstance(data.get(key), list) and len(data.get(key)) > 0
                    )
                    
                    if metric_count > 0:
                        # Get unique nodes
                        unique_nodes = set()
                        for key in data.keys():
                            if key.endswith(f'_{role}'):
                                rows = data.get(key, [])
                                unique_nodes.update(row.get('Node') for row in rows if isinstance(row, dict) and 'Node' in row)
                        
                        overview_data.append({
                            'Role': role.title(),
                            'Nodes': len(unique_nodes),
                            'Metrics Collected': metric_count,
                            'Status': self.create_status_badge('success', 'Active')
                        })
            
            total_nodes = sum(int(item.get('Nodes', 0)) for item in overview_data)
            if total_nodes > 0:
                summary_items.append(f"<li>Total Nodes: {total_nodes}</li>")
            
            # Role breakdown
            for item in overview_data:
                role = item.get('Role', 'Unknown')
                nodes = item.get('Nodes', 0)
                metrics = item.get('Metrics Collected', 0)
                if nodes > 0:
                    summary_items.append(f"<li>{role}: {nodes} nodes, {metrics} UDP netstat metrics</li>")
            
            return (
                "<div class=\"network-netstat-udp-summary\">"
                "<h4>Network UDP Netstat Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate network UDP netstat summary: {e}")
            return f"Network UDP netstat metrics collected from {len(data)} role groups"
    
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
            logger.error(f"Failed to transform network UDP netstat data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        try:
            # Overview table first
            if 'network_netstat_udp_overview' in dataframes and not dataframes['network_netstat_udp_overview'].empty:
                html_tables['network_netstat_udp_overview'] = self.create_html_table(
                    dataframes['network_netstat_udp_overview'], 
                    'Network UDP Netstat Overview'
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
            logger.error(f"Failed to generate HTML tables for network UDP netstat: {e}")
        
        return html_tables