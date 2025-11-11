"""
Extract, Load, Transform module for Network L1 Metrics
Handles network L1 data from tools/net/network_l1.py
ONLY contains network_l1 specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class networkL1ELT(utilityELT):
    """Extract, Load, Transform class for network L1 metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'network_l1_node_network_up': {
                'title': 'Network Up Status',
                'unit': 'status'
            },
            'network_l1_node_traffic_carrier': {
                'title': 'Traffic Carrier Status',
                'unit': 'status'
            },
            'network_l1_node_network_speed_bytes': {
                'title': 'Network Speed',
                'unit': 'bits_per_second',
                'thresholds': {'critical': 40000000000, 'warning': 10000000000}
            },
            'network_l1_node_network_mtu_bytes': {
                'title': 'Network MTU',
                'unit': 'bytes',
                'thresholds': {'critical': 9000, 'warning': 1500}
            },
            'network_l1_node_arp_entries': {
                'title': 'ARP Entries',
                'unit': 'count',
                'thresholds': {'critical': 1000, 'warning': 500}
            }
        }
    
    def extract_network_l1(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract network L1 information from network_l1.py output"""
        
        # Handle nested data structure - extract actual metrics data
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'network_l1_overview': [],
        }
        
        # Initialize all role-based tables
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            structured[f'network_up_status_{role}'] = []
            structured[f'traffic_carrier_{role}'] = []
            structured[f'network_speed_{role}'] = []
            structured[f'network_mtu_{role}'] = []
            structured[f'arp_entries_{role}'] = []
        
        node_groups = actual_data.get('node_groups', {})
        
        # Extract metrics for each role
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            if role not in node_groups:
                continue
            
            role_data = node_groups[role]
            metrics = role_data.get('metrics', {})
            
            # Process each metric type
            for metric_name, metric_data in metrics.items():
                if 'error' in metric_data:
                    continue
                
                if metric_name == 'network_l1_node_network_up':
                    self._extract_network_up_status(metric_data, structured, role)
                elif metric_name == 'network_l1_node_traffic_carrier':
                    self._extract_traffic_carrier(metric_data, structured, role)
                elif metric_name == 'network_l1_node_network_speed_bytes':
                    self._extract_network_speed(metric_data, structured, role)
                elif metric_name == 'network_l1_node_network_mtu_bytes':
                    self._extract_network_mtu(metric_data, structured, role)
                elif metric_name == 'network_l1_node_arp_entries':
                    self._extract_arp_entries(metric_data, structured, role)
        
        # Generate overview
        self._generate_overview(actual_data, structured)
        
        return structured

    def summarize_network_l1(self, data: Dict[str, Any]) -> str:
        """Generate network L1 summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('network_l1_overview', [])
            
            # Fallback: synthesize overview if missing but role tables exist
            if not overview_data:
                roles = ['controlplane', 'infra', 'worker', 'workload']
                metric_prefixes = [
                    'network_up_status', 'traffic_carrier', 'network_speed', 'network_mtu', 'arp_entries'
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
                "<div class=\"network-l1-summary\">"
                "<h4>Network L1 Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate network L1 summary: {e}")
            return f"Network L1 metrics collected from {len(data)} role groups"

    def _extract_network_up_status(self, metric_data: Dict[str, Any], 
                                   structured: Dict[str, Any], role: str):
        """Extract network up status for a role"""
        table_key = f'network_up_status_{role}'
        nodes = metric_data.get('nodes', {})
        
        for node_name, node_data in nodes.items():
            interfaces = node_data.get('interfaces', {})
            for device, status in interfaces.items():
                structured[table_key].append({
                    'Metric Name': self.metric_configs['network_l1_node_network_up']['title'],
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Interface': device,
                    'Status': self.format_status(status)
                })
    
    def _extract_traffic_carrier(self, metric_data: Dict[str, Any], 
                                structured: Dict[str, Any], role: str):
        """Extract traffic carrier status for a role"""
        table_key = f'traffic_carrier_{role}'
        nodes = metric_data.get('nodes', {})
        
        for node_name, node_data in nodes.items():
            interfaces = node_data.get('interfaces', {})
            for device, status in interfaces.items():
                structured[table_key].append({
                    'Metric Name': self.metric_configs['network_l1_node_traffic_carrier']['title'],
                    'Role': role.title(),
                    'Node': self.truncate_node_name(node_name),
                    'Interface': device,
                    'Carrier': self.format_status(status)
                })
    
    def _extract_network_speed(self, metric_data: Dict[str, Any], 
                              structured: Dict[str, Any], role: str):
        """Extract network speed for a role"""
        table_key = f'network_speed_{role}'
        nodes = metric_data.get('nodes', {})
        
        # Collect all speeds for top identification
        all_speeds = []
        for node_name, node_data in nodes.items():
            interfaces = node_data.get('interfaces', {})
            for device, speed in interfaces.items():
                all_speeds.append((node_name, device, float(speed), node_data.get('max_speed', 0)))
        
        # Find top 1 speed
        top_speed = max((s[2] for s in all_speeds), default=0) if all_speeds else 0
        
        # Format rows
        thresholds = self.metric_configs['network_l1_node_network_speed_bytes']['thresholds']
        for node_name, device, speed, max_speed in all_speeds:
            is_top = (speed == top_speed and speed > 0)
            
            speed_readable = self.format_network_speed(speed)
            if is_top:
                speed_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {speed_readable}</span>'
            elif speed >= thresholds['critical']:
                speed_display = f'<span class="text-success font-weight-bold">{speed_readable}</span>'
            elif speed >= thresholds['warning']:
                speed_display = f'<span class="text-info font-weight-bold">{speed_readable}</span>'
            else:
                speed_display = speed_readable
            
            max_speed_readable = self.format_network_speed(max_speed)
            
            structured[table_key].append({
                'Metric Name': self.metric_configs['network_l1_node_network_speed_bytes']['title'],
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Interface': device,
                'Speed': speed_display,
                'Max Speed': max_speed_readable
            })
    
    def _extract_network_mtu(self, metric_data: Dict[str, Any], 
                            structured: Dict[str, Any], role: str):
        """Extract network MTU for a role"""
        table_key = f'network_mtu_{role}'
        nodes = metric_data.get('nodes', {})
        
        # Collect all MTU values
        all_mtus = []
        for node_name, node_data in nodes.items():
            interfaces = node_data.get('interfaces', {})
            for device, mtu in interfaces.items():
                all_mtus.append((node_name, device, float(mtu), node_data.get('max_mtu', 0)))
        
        # Find top 1 MTU
        top_mtu = max((m[2] for m in all_mtus), default=0) if all_mtus else 0
        
        # Format rows
        thresholds = self.metric_configs['network_l1_node_network_mtu_bytes']['thresholds']
        for node_name, device, mtu, max_mtu in all_mtus:
            is_top = (mtu == top_mtu and mtu > 0)
            
            mtu_readable = self.format_mtu_bytes(mtu)
            if is_top:
                mtu_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {mtu_readable}</span>'
            elif mtu >= thresholds['critical']:
                mtu_display = f'<span class="text-success font-weight-bold">{mtu_readable}</span>'
            elif mtu >= thresholds['warning']:
                mtu_display = f'<span class="text-info font-weight-bold">{mtu_readable}</span>'
            else:
                mtu_display = mtu_readable
            
            max_mtu_readable = self.format_mtu_bytes(max_mtu)
            
            structured[table_key].append({
                'Metric Name': self.metric_configs['network_l1_node_network_mtu_bytes']['title'],
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Interface': device,
                'MTU': mtu_display,
                'Max MTU': max_mtu_readable
            })
    
    def _extract_arp_entries(self, metric_data: Dict[str, Any], 
                            structured: Dict[str, Any], role: str):
        """Extract ARP entries for a role"""
        table_key = f'arp_entries_{role}'
        nodes = metric_data.get('nodes', {})
        
        # Collect all ARP entries
        all_arps = []
        for node_name, node_data in nodes.items():
            interfaces = node_data.get('interfaces', {})
            for device, stats in interfaces.items():
                if isinstance(stats, dict):
                    avg_val = float(stats.get('avg', 0))
                    max_val = float(stats.get('max', 0))
                else:
                    avg_val = 0.0
                    max_val = 0.0
                all_arps.append((node_name, device, avg_val, max_val))
        
        # Find top 1 by max value
        top_max = max((a[3] for a in all_arps), default=0) if all_arps else 0
        
        # Format rows
        thresholds = self.metric_configs['network_l1_node_arp_entries']['thresholds']
        for node_name, device, avg_val, max_val in all_arps:
            is_top = (max_val == top_max and max_val > 0)
            
            # Format avg
            if avg_val >= thresholds['critical']:
                avg_display = f'<span class="text-danger font-weight-bold">⚠️ {int(avg_val)}</span>'
            elif avg_val >= thresholds['warning']:
                avg_display = f'<span class="text-warning font-weight-bold">{int(avg_val)}</span>'
            else:
                avg_display = str(int(avg_val))
            
            # Format max with top highlight
            if is_top:
                max_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {int(max_val)}</span>'
            elif max_val >= thresholds['critical']:
                max_display = f'<span class="text-danger font-weight-bold">⚠️ {int(max_val)}</span>'
            elif max_val >= thresholds['warning']:
                max_display = f'<span class="text-warning font-weight-bold">{int(max_val)}</span>'
            else:
                max_display = str(int(max_val))
            
            structured[table_key].append({
                'Metric Name': self.metric_configs['network_l1_node_arp_entries']['title'],
                'Role': role.title(),
                'Node': self.truncate_node_name(node_name),
                'Interface': device,
                'Avg Entries': avg_display,
                'Max Entries': max_display
            })
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate network L1 overview"""
        node_groups = data.get('node_groups', {})
        
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            if role in node_groups:
                node_count = node_groups[role].get('count', 0)
                metrics_count = len([k for k in node_groups[role].get('metrics', {}).keys() 
                                    if 'error' not in node_groups[role]['metrics'][k]])
                
                structured['network_l1_overview'].append({
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
            logger.error(f"Failed to transform network L1 data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        # Define metric groups
        metric_groups = {
            'Network Up Status': 'network_up_status',
            'Traffic Carrier': 'traffic_carrier',
            'Network Speed': 'network_speed',
            'Network MTU': 'network_mtu',
            'ARP Entries': 'arp_entries'
        }
        
        try:
            # Overview table first
            if 'network_l1_overview' in dataframes and not dataframes['network_l1_overview'].empty:
                html_tables['network_l1_overview'] = self.create_html_table(
                    dataframes['network_l1_overview'], 
                    'Network L1 Overview'
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
            logger.error(f"Failed to generate HTML tables for network L1: {e}")
        
        return html_tables