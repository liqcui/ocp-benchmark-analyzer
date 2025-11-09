"""
Network IO ELT module for OpenShift Benchmark Performance Data
Extract, Load, Transform module for network IO metrics data
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class NetworkIOELT(EltUtility):
    """Extract, Load, Transform class for Network IO metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_display_names = {
            'network_io_node_network_rx_utilization': 'Network RX (Receive) Utilization',
            'network_io_node_network_tx_utilization': 'Network TX (Transmit) Utilization',
            'network_io_node_network_rx_package': 'Network RX Packets',
            'network_io_node_network_tx_package': 'Network TX Packets',
            'network_io_node_network_rx_drop': 'Network RX Dropped Packets',
            'network_io_node_network_tx_drop': 'Network TX Dropped Packets',
            'network_io_grpc_active_watch_streams': 'gRPC Active Watch Streams',
            'network_io_node_nf_conntrack_entries': 'Connection Tracking Entries',
            'network_io_node_nf_conntrack_entries_limit': 'Connection Tracking Limit',
            'network_io_node_saturation_rx': 'Network RX Saturation',
            'network_io_node_saturation_tx': 'Network TX Saturation',
            'network_io_node_error_rx': 'Network RX Errors',
            'network_io_node_error_tx': 'Network TX Errors',
            'network_io_node_speed_bytes': 'Network Interface Speed',
            'network_io_nodec_receive_fifo_total': 'Network RX FIFO Total',
            'network_io_node_transit_fifo_total': 'Network TX FIFO Total'
        }
    
    def extract_network_io(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract network IO metrics data from JSON - called by main module"""
        try:
            if 'metrics' not in data:
                return {'error': 'No metrics data found'}
            
            metrics_data = data.get('metrics', {})
            structured = {
                'category': data.get('category', 'network_io'),
                'duration': data.get('duration', 'unknown'),
                'timestamp': data.get('timestamp', 'unknown'),
                'summary': data.get('summary', {}),
                'collection_metadata': data.get('collection_metadata', {}),
                'metrics': {}
            }
            
            # Extract each metric
            for metric_key, metric_data in metrics_data.items():
                metric_name = metric_data.get('metric', metric_key)
                unit = metric_data.get('unit', 'unknown')
                
                structured['metrics'][metric_key] = {
                    'metric_name': metric_name,
                    'display_name': self.metric_display_names.get(metric_key, metric_name),
                    'unit': unit,
                    'controlplane': self._extract_node_group_data(metric_data.get('controlplane', {}), 'controlplane'),
                    'worker': self._extract_node_group_data(metric_data.get('worker', {}), 'worker'),
                    'infra': self._extract_node_group_data(metric_data.get('infra', {}), 'infra'),
                    'workload': self._extract_node_group_data(metric_data.get('workload', {}), 'workload')
                }
            
            return structured
            
        except Exception as e:
            logger.error(f"Failed to extract network IO data: {e}")
            return {'error': str(e)}
    
    def _extract_node_group_data(self, group_data: Dict[str, Any], group_type: str) -> List[Dict[str, Any]]:
        """Extract node group data for a specific metric"""
        extracted_nodes = []
        
        # Handle both 'nodes' list and 'top3' list for workers
        nodes_list = group_data.get('nodes', [])
        if not nodes_list and group_type == 'worker':
            nodes_list = group_data.get('top3', [])
        
        for node_data in nodes_list:
            node_name = node_data.get('node', 'unknown')
            total_avg = node_data.get('total_avg', 0)
            total_max = node_data.get('total_max', 0)
            unit = node_data.get('unit', 'unknown')
            
            # Extract interface details
            interfaces = node_data.get('interfaces', [])
            
            if interfaces:
                # Add each interface as a separate row
                for interface in interfaces:
                    extracted_nodes.append({
                        'node_name': self.truncate_node_name(node_name),
                        'interface': interface.get('interface', 'unknown'),
                        'avg': interface.get('avg', 0),
                        'max': interface.get('max', 0),
                        'unit': unit,
                        'group_type': group_type
                    })
            else:
                # Add node-level data without interface breakdown
                extracted_nodes.append({
                    'node_name': self.truncate_node_name(node_name),
                    'interface': 'total',
                    'avg': total_avg,
                    'max': total_max,
                    'unit': unit,
                    'group_type': group_type
                })
        
        return extracted_nodes
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform network IO data to DataFrames - called by main module"""
        try:
            dataframes = {}
            
            if 'error' in structured_data:
                return dataframes
            
            # Create summary DataFrame
            summary_data = []
            summary_info = structured_data.get('summary', {})
            summary_data.append({'Property': 'Total Metrics', 'Value': summary_info.get('total_metrics', 0)})
            summary_data.append({'Property': 'Metrics with Data', 'Value': summary_info.get('with_data', 0)})
            summary_data.append({'Property': 'Empty Metrics', 'Value': summary_info.get('empty', 0)})
            summary_data.append({'Property': 'Duration', 'Value': structured_data.get('duration', 'unknown')})
            summary_data.append({'Property': 'Timestamp', 'Value': self.format_timestamp(structured_data.get('timestamp', 'unknown'))})
            
            dataframes['network_io_summary'] = pd.DataFrame(summary_data)
            
            # Create DataFrames for each metric grouped by node type
            metrics = structured_data.get('metrics', {})
            
            for metric_key, metric_info in metrics.items():
                display_name = metric_info.get('display_name', metric_key)
                unit = metric_info.get('unit', 'unknown')
                
                # Combine all node groups for this metric
                all_nodes_data = []
                
                for group_type in ['controlplane', 'worker', 'infra', 'workload']:
                    group_data = metric_info.get(group_type, [])
                    if group_data:
                        all_nodes_data.extend(group_data)
                
                if all_nodes_data:
                    df = pd.DataFrame(all_nodes_data)
                    
                    # Format values with proper units
                    df['avg_formatted'] = df.apply(lambda row: self._format_network_value(row['avg'], row['unit']), axis=1)
                    df['max_formatted'] = df.apply(lambda row: self._format_network_value(row['max'], row['unit']), axis=1)
                    
                    # Find top values for highlighting
                    df['is_top'] = False
                    df['is_critical'] = False
                    
                    if len(df) > 0:
                        max_idx = df['max'].idxmax()
                        df.at[max_idx, 'is_top'] = True
                        
                        # Mark critical based on metric type
                        df['is_critical'] = df.apply(
                            lambda row: self._is_critical_network_value(row['max'], row['unit'], metric_key),
                            axis=1
                        )
                    
                    # Reorder columns for display
                    display_columns = ['group_type', 'node_name', 'interface', 'avg_formatted', 'max_formatted']
                    df_display = df[display_columns + ['is_top', 'is_critical']].copy()
                    df_display.columns = ['Node Group', 'Node Name', 'Interface', 'Avg', 'Max', 'is_top', 'is_critical']
                    
                    # Sort by group type and max value
                    group_order = {'controlplane': 0, 'infra': 1, 'worker': 2, 'workload': 3}
                    df_display['group_order'] = df_display['Node Group'].map(group_order)
                    df_display = df_display.sort_values(['group_order', 'Max'], ascending=[True, False])
                    df_display = df_display.drop('group_order', axis=1)
                    
                    dataframes[f'network_io_{metric_key}'] = df_display
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform network IO data: {e}")
            return {}
    
    def _format_network_value(self, value: float, unit: str) -> str:
        """Format network metric value with appropriate unit"""
        try:
            if value == 0:
                return '0'
            
            if unit == 'bits_per_second':
                # Convert to readable format
                if value >= 1_000_000_000:  # Gbps
                    return f"{round(value / 1_000_000_000, 2)} Gbps"
                elif value >= 1_000_000:  # Mbps
                    return f"{round(value / 1_000_000, 2)} Mbps"
                elif value >= 1_000:  # Kbps
                    return f"{round(value / 1_000, 2)} Kbps"
                else:
                    return f"{round(value, 2)} bps"
            
            elif unit == 'packets_per_second':
                if value >= 1_000_000:
                    return f"{round(value / 1_000_000, 2)} M pps"
                elif value >= 1_000:
                    return f"{round(value / 1_000, 2)} K pps"
                else:
                    return f"{round(value, 2)} pps"
            
            elif unit == 'count':
                if value >= 1_000_000:
                    return f"{round(value / 1_000_000, 2)} M"
                elif value >= 1_000:
                    return f"{round(value / 1_000, 2)} K"
                else:
                    return f"{round(value, 2)}"
            
            else:
                return f"{round(value, 2)} {unit}"
                
        except (ValueError, TypeError):
            return str(value)
    
    def _is_critical_network_value(self, value: float, unit: str, metric_key: str) -> bool:
        """Determine if a network metric value is critical"""
        try:
            if unit == 'bits_per_second':
                # Critical if > 800 Mbps (80% of 1Gbps) or > 8 Gbps (80% of 10Gbps)
                return value > 800_000_000
            
            elif unit == 'packets_per_second':
                # Critical if > 100K pps
                return value > 100_000
            
            elif 'drop' in metric_key or 'error' in metric_key:
                # Any drops or errors are concerning
                return value > 0
            
            elif 'saturation' in metric_key:
                # Saturation > 0.8 is critical
                return value > 0.8
            
            return False
            
        except (ValueError, TypeError):
            return False
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables for network IO data with highlighting - called by main module"""
        try:
            html_tables = {}
            
            for table_name, df in dataframes.items():
                if df.empty:
                    continue
                
                # Check if this is a metric table (has highlighting columns)
                has_highlighting = 'is_top' in df.columns and 'is_critical' in df.columns
                
                if has_highlighting:
                    # Create HTML with highlighting
                    df_display = df.drop(['is_top', 'is_critical'], axis=1).copy()
                    
                    html_rows = []
                    for idx, row in df.iterrows():
                        row_class = ''
                        if row['is_critical']:
                            row_class = 'table-danger'
                        elif row['is_top']:
                            row_class = 'table-warning'
                        
                        cells = [f"<td>{self._escape_html(str(val))}</td>" for val in df_display.loc[idx]]
                        html_rows.append(f"<tr class='{row_class}'>{''.join(cells)}</tr>")
                    
                    headers = ''.join([f"<th>{col}</th>" for col in df_display.columns])
                    table_html = f"""
                    <div class="table-responsive">
                        <table class="table table-striped table-bordered table-sm" id="table-{table_name.replace('_', '-')}">
                            <thead><tr>{headers}</tr></thead>
                            <tbody>{''.join(html_rows)}</tbody>
                        </table>
                    </div>
                    """
                    html_tables[table_name] = self.clean_html(table_html)
                else:
                    # Use standard table generation
                    html_tables[table_name] = self.create_html_table(df, table_name)
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Failed to generate HTML tables: {e}")
            return {}
    
    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters"""
        return (str(text)
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&#39;'))
    
    def summarize_network_io(self, structured_data: Dict[str, Any]) -> str:
        """Generate brief summary of network IO data - called by main module"""
        try:
            if 'error' in structured_data:
                return f"Error: {structured_data['error']}"
            
            summary_parts = ["Network IO Metrics Summary:"]
            
            # Basic info
            duration = structured_data.get('duration', 'unknown')
            summary_parts.append(f"• Duration: {duration}")
            
            # Summary stats
            summary = structured_data.get('summary', {})
            total_metrics = summary.get('total_metrics', 0)
            with_data = summary.get('with_data', 0)
            summary_parts.append(f"• Total Metrics: {total_metrics} ({with_data} with data)")
            
            # Identify metrics with data
            metrics = structured_data.get('metrics', {})
            active_metrics = []
            critical_findings = []
            
            for metric_key, metric_info in metrics.items():
                display_name = metric_info.get('display_name', metric_key)
                has_data = False
                max_value = 0
                max_unit = ''
                
                # Check all node groups for data
                for group_type in ['controlplane', 'worker', 'infra', 'workload']:
                    group_data = metric_info.get(group_type, [])
                    if group_data:
                        has_data = True
                        for node in group_data:
                            if node.get('max', 0) > max_value:
                                max_value = node.get('max', 0)
                                max_unit = node.get('unit', '')
                
                if has_data:
                    active_metrics.append(display_name)
                    
                    # Check for critical values
                    if self._is_critical_network_value(max_value, max_unit, metric_key):
                        formatted_value = self._format_network_value(max_value, max_unit)
                        critical_findings.append(f"{display_name}: {formatted_value}")
            
            if active_metrics:
                summary_parts.append(f"• Active Metrics: {', '.join(active_metrics[:3])}")
                if len(active_metrics) > 3:
                    summary_parts.append(f"  and {len(active_metrics) - 3} more")
            
            if critical_findings:
                summary_parts.append(f"• Critical Findings: {len(critical_findings)}")
                for finding in critical_findings[:2]:
                    summary_parts.append(f"  - {finding}")
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to summarize network IO data: {e}")
            return f"Summary generation failed: {str(e)}"