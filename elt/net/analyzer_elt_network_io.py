"""
Extract, Load, Transform module for Network I/O Metrics
Handles network I/O data from tools/net/network_io.py
ONLY contains network_io specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class networkIOELT(utilityELT):
    """Extract, Load, Transform class for network I/O metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'network_io_node_network_rx_utilization': {
                'title': 'Network RX Utilization',
                'unit': 'bits_per_second',
                'thresholds': {'critical': 8000000000, 'warning': 5000000000}  # 8 Gbps, 5 Gbps
            },
            'network_io_node_network_tx_utilization': {
                'title': 'Network TX Utilization',
                'unit': 'bits_per_second',
                'thresholds': {'critical': 8000000000, 'warning': 5000000000}
            },
            'network_io_node_network_rx_package': {
                'title': 'Network RX Packets',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 100000, 'warning': 50000}
            },
            'network_io_node_network_tx_package': {
                'title': 'Network TX Packets',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 100000, 'warning': 50000}
            },
            'network_io_node_network_rx_drop': {
                'title': 'Network RX Drops',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 100, 'warning': 10}
            },
            'network_io_node_network_tx_drop': {
                'title': 'Network TX Drops',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 100, 'warning': 10}
            },
            'network_io_node_nf_conntrack_entries': {
                'title': 'Conntrack Entries',
                'unit': 'count',
                'thresholds': {'critical': 900000, 'warning': 700000}
            },
            'network_io_node_nf_conntrack_entries_limit': {
                'title': 'Conntrack Entries Limit',
                'unit': 'count',
                'thresholds': {}
            },
            'network_io_node_error_rx': {
                'title': 'Network RX Errors',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 100, 'warning': 10}
            },
            'network_io_node_error_tx': {
                'title': 'Network TX Errors',
                'unit': 'packets_per_second',
                'thresholds': {'critical': 100, 'warning': 10}
            },
            'network_io_nodec_receive_fifo_total': {
                'title': 'Receive FIFO Total',
                'unit': 'count',
                'thresholds': {'critical': 1000, 'warning': 100}
            },
            'network_io_node_transit_fifo_total': {
                'title': 'Transmit FIFO Total',
                'unit': 'count',
                'thresholds': {'critical': 1000, 'warning': 100}
            }
        }
    
    def extract_network_io(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract network I/O information from network_io.py output"""
        
        # Accept both top-level and nested 'data' payloads
        payload = data.get('data', data) or {}
        
        structured = {
            'network_io_overview': []
        }
        
        # Initialize all role-based tables for each metric
        for metric_name in self.metric_configs.keys():
            for role in ['controlplane', 'infra', 'worker', 'workload']:
                table_key = f'{metric_name}_{role}'
                structured[table_key] = []
        
        metrics = payload.get('metrics', {})
        
        # Process each metric
        for metric_name, metric_data in metrics.items():
            if 'error' in metric_data or not metric_data:
                continue
            
            config = self.metric_configs.get(metric_name, {})
            if not config:
                continue
            
            # Extract data for each role
            for role in ['controlplane', 'infra', 'worker', 'workload']:
                self._extract_metric_for_role(
                    metric_name, metric_data, structured, role, config
                )
        
        # Generate overview
        self._generate_overview(data, structured)
        
        return structured
    
    def _extract_metric_for_role(self, metric_name: str, metric_data: Dict[str, Any],
                                 structured: Dict[str, Any], role: str,
                                 config: Dict[str, Any]):
        """Extract metric data for a specific role"""
        table_key = f'{metric_name}_{role}'
        unit = config.get('unit', '')
        thresholds = config.get('thresholds', {})
        
        role_data = metric_data.get(role, {})
        
        # Handle controlplane/infra/workload (nodes list)
        if role in ['controlplane', 'infra', 'workload']:
            nodes = role_data.get('nodes', [])
            if not nodes:
                return
            
            # Collect all values for top identification
            all_values = [(node.get('node', ''), 
                          float(node.get('avg', 0)), 
                          float(node.get('max', 0))) 
                         for node in nodes if isinstance(node, dict)]
            
            # Find top 1 by max value
            top_max = max((v[2] for v in all_values), default=0) if all_values else 0
            
            # Format rows
            for node_name, avg_val, max_val in all_values:
                is_top = (max_val == top_max and max_val > 0)
                
                structured[table_key].append({
                    'Metric Name': config.get('title', metric_name),
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Avg': self.format_and_highlight(avg_val, unit, thresholds, False),
                    'Max': self.format_and_highlight(max_val, unit, thresholds, is_top),
                    'Unit': unit
                })
        
        # Handle worker (top3 list)
        elif role == 'worker':
            top3 = role_data.get('top3', [])
            if not top3:
                return
            
            # Collect all values
            all_values = [(node.get('node', ''), 
                          float(node.get('avg', 0)), 
                          float(node.get('max', 0))) 
                         for node in top3 if isinstance(node, dict)]
            
            # Find top 1 by max value
            top_max = max((v[2] for v in all_values), default=0) if all_values else 0
            
            # Format rows
            for node_name, avg_val, max_val in all_values:
                is_top = (max_val == top_max and max_val > 0)
                
                structured[table_key].append({
                    'Metric Name': config.get('title', metric_name),
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Avg': self.format_and_highlight(avg_val, unit, thresholds, False),
                    'Max': self.format_and_highlight(max_val, unit, thresholds, is_top),
                    'Unit': unit
                })
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate network I/O overview"""
        # Support both top-level and nested 'data' payloads
        payload = data.get('data', data) or {}
        summary = payload.get('summary', {})
        
        overview = {
            'Total Metrics': summary.get('total_metrics', 0),
            'Metrics with Data': summary.get('with_data', 0),
            'Empty Metrics': summary.get('empty', 0),
            'Errors': summary.get('errors', 0),
            'Duration': payload.get('duration', data.get('duration', 'N/A')),
            'Status': self.create_status_badge('success', 'Complete') if summary.get('errors', 0) == 0 else self.create_status_badge('warning', 'With Errors')
        }
        
        structured['network_io_overview'].append(overview)
    
    def summarize_network_io(self, data: Dict[str, Any]) -> str:
        """Generate network I/O summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('network_io_overview', [])
            if overview_data:
                overview = overview_data[0]
                
                total_metrics = overview.get('Total Metrics', 0)
                with_data = overview.get('Metrics with Data', 0)
                empty = overview.get('Empty Metrics', 0)
                duration = overview.get('Duration', 'N/A')
                
                summary_items.append(f"<li>Total Metrics: {total_metrics}</li>")
                summary_items.append(f"<li>Metrics with Data: {with_data}</li>")
                if empty > 0:
                    summary_items.append(f"<li>Empty Metrics: {empty}</li>")
                summary_items.append(f"<li>Duration: {duration}</li>")
            
            # Count nodes across all roles
            total_nodes = set()
            for key, value in data.items():
                if isinstance(value, list) and '_controlplane' in key:
                    for row in value:
                        if isinstance(row, dict) and 'Node' in row:
                            total_nodes.add(row['Node'])
                elif isinstance(value, list) and '_worker' in key:
                    for row in value:
                        if isinstance(row, dict) and 'Node' in row:
                            total_nodes.add(row['Node'])
            
            if total_nodes:
                summary_items.append(f"<li>Nodes Monitored: {len(total_nodes)}</li>")
            
            return (
                "<div class=\"network-io-summary\">"
                "<h4>Network I/O Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate network I/O summary: {e}")
            return "Network I/O metrics collected"
    
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
            logger.error(f"Failed to transform network I/O data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        try:
            # Overview table first
            if 'network_io_overview' in dataframes and not dataframes['network_io_overview'].empty:
                html_tables['network_io_overview'] = self.create_html_table(
                    dataframes['network_io_overview'], 
                    'Network I/O Overview'
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
            logger.error(f"Failed to generate HTML tables for network I/O: {e}")
        
        return html_tables