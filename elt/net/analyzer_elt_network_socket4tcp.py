"""
Extract, Load, Transform module for Network Socket TCP Metrics
Handles TCP socket stats collected by tools/net/network_socket4tcp.py
ONLY contains network_socket_tcp specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class networkSocketTCPELT(utilityELT):
    """Extract, Load, Transform class for network socket TCP metrics data"""
    
    def __init__(self):
        super().__init__()
        # Metric configuration: titles, units, thresholds
        self.metric_configs: Dict[str, Dict[str, Any]] = {
            'socket_tcp_allocated': {
                'title': 'TCP Sockets Allocated',
                'unit': 'count',
                'thresholds': {'critical': 20000, 'warning': 10000}
            },
            'socket_tcp_inuse': {
                'title': 'TCP Sockets In Use',
                'unit': 'count',
                'thresholds': {'critical': 10000, 'warning': 5000}
            },
            'socket_tcp_orphan': {
                'title': 'TCP Orphan Sockets',
                'unit': 'count',
                'thresholds': {'critical': 500, 'warning': 100}
            },
            'socket_tcp_tw': {
                'title': 'TCP Time-Wait Sockets',
                'unit': 'count',
                'thresholds': {'critical': 5000, 'warning': 1000}
            },
            'socket_used': {
                'title': 'Sockets Used',
                'unit': 'count',
                'thresholds': {'critical': 10000, 'warning': 5000}
            },
            'socket_frag_inuse': {
                'title': 'FRAG Sockets In Use',
                'unit': 'count',
                'thresholds': {'critical': 500, 'warning': 100}
            },
            'socket_raw_inuse': {
                'title': 'RAW Sockets In Use',
                'unit': 'count',
                'thresholds': {'critical': 500, 'warning': 100}
            },
        }
    
    # =====================================================================
    # MAIN EXTRACTOR
    # =====================================================================
    
    def extract_network_socket_tcp(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract TCP socket metrics into structured tables grouped by role"""
        structured: Dict[str, Any] = {
            'network_socket_tcp_overview': []
        }
        # Initialize all role-based tables for each known metric
        for metric_name in self.metric_configs.keys():
            for role in ['controlplane', 'infra', 'worker', 'workload']:
                structured[f'{metric_name}_{role}'] = []
        
        # Input may be the full collector result with 'metrics' or a single metric payload
        metrics_section = {}
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            metrics_section = data['metrics']
        else:
            # Treat as single metric payload with a 'metric' name
            single_name = data.get('metric') or 'socket_tcp_inuse'
            metrics_section = {single_name: data}
        
        # Extract each metric via dedicated functions (requirement 5.1)
        for metric_name, metric_payload in metrics_section.items():
            if not isinstance(metric_payload, dict):
                continue
            if metric_name == 'socket_tcp_allocated':
                self._extract_socket_tcp_allocated(metric_payload, structured)
            elif metric_name == 'socket_tcp_inuse':
                self._extract_socket_tcp_inuse(metric_payload, structured)
            elif metric_name == 'socket_tcp_orphan':
                self._extract_socket_tcp_orphan(metric_payload, structured)
            elif metric_name == 'socket_tcp_tw':
                self._extract_socket_tcp_tw(metric_payload, structured)
            elif metric_name == 'socket_used':
                self._extract_socket_used(metric_payload, structured)
            elif metric_name == 'socket_frag_inuse':
                self._extract_socket_frag_inuse(metric_payload, structured)
            elif metric_name == 'socket_raw_inuse':
                self._extract_socket_raw_inuse(metric_payload, structured)
            else:
                # Unknown metric: try generic extractor if nodes exist
                self._extract_generic_metric(metric_name, metric_payload, structured)
        
        # Overview
        self._generate_overview(metrics_section, structured)
        return structured
    
    # =====================================================================
    # SUMMARY
    # =====================================================================
    
    def summarize_network_socket_tcp(self, data: Dict[str, Any]) -> str:
        """Generate network socket TCP summary as HTML"""
        try:
            summary_items: List[str] = []
            overview = data.get('network_socket_tcp_overview', [])
            total_roles = len([o for o in overview if int(o.get('Metrics', 0)) > 0])
            total_nodes = sum(int(o.get('Nodes', 0)) for o in overview)
            if total_nodes > 0:
                summary_items.append(f"<li>Total Nodes: {total_nodes}</li>")
            for item in overview:
                role = item.get('Role', 'Unknown')
                nodes = item.get('Nodes', 0)
                metrics = item.get('Metrics', 0)
                if nodes > 0:
                    summary_items.append(f"<li>{role}: {nodes} nodes, {metrics} metrics</li>")
            return (
                "<div class=\"network-socket-tcp-summary\">"
                "<h4>Network Socket TCP Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        except Exception as e:
            logger.error(f"Failed to generate network socket TCP summary: {e}")
            return f"Network socket TCP metrics collected across {len(data)} sections"
    
    # =====================================================================
    # PER-METRIC EXTRACTORS (requirement 5.1)
    # =====================================================================
    
    def _extract_socket_tcp_allocated(self, metric_payload: Dict[str, Any], structured: Dict[str, Any]):
        self._extract_metric_by_role('socket_tcp_allocated', metric_payload, structured)
    
    def _extract_socket_tcp_inuse(self, metric_payload: Dict[str, Any], structured: Dict[str, Any]):
        self._extract_metric_by_role('socket_tcp_inuse', metric_payload, structured)
    
    def _extract_socket_tcp_orphan(self, metric_payload: Dict[str, Any], structured: Dict[str, Any]):
        self._extract_metric_by_role('socket_tcp_orphan', metric_payload, structured)
    
    def _extract_socket_tcp_tw(self, metric_payload: Dict[str, Any], structured: Dict[str, Any]):
        self._extract_metric_by_role('socket_tcp_tw', metric_payload, structured)
    
    def _extract_socket_used(self, metric_payload: Dict[str, Any], structured: Dict[str, Any]):
        self._extract_metric_by_role('socket_used', metric_payload, structured)
    
    def _extract_socket_frag_inuse(self, metric_payload: Dict[str, Any], structured: Dict[str, Any]):
        self._extract_metric_by_role('socket_frag_inuse', metric_payload, structured)
    
    def _extract_socket_raw_inuse(self, metric_payload: Dict[str, Any], structured: Dict[str, Any]):
        self._extract_metric_by_role('socket_raw_inuse', metric_payload, structured)
    
    def _extract_generic_metric(self, metric_name: str, metric_payload: Dict[str, Any], structured: Dict[str, Any]):
        self._extract_metric_by_role(metric_name, metric_payload, structured)
    
    # =====================================================================
    # SHARED EXTRACTION IMPLEMENTATION
    # =====================================================================
    
    def _extract_metric_by_role(self, metric_name: str, metric_payload: Dict[str, Any], structured: Dict[str, Any]):
        """Generic extraction: builds rows grouped by role with highlighting and units"""
        nodes_by_role = metric_payload.get('nodes', {}) if isinstance(metric_payload, dict) else {}
        unit = metric_payload.get('unit', self.metric_configs.get(metric_name, {}).get('unit', 'count'))
        thresholds = self.metric_configs.get(metric_name, {}).get('thresholds', {'critical': float('inf'), 'warning': float('inf')})
        title = self.metric_configs.get(metric_name, {}).get('title', metric_name.replace('_', ' ').title())
        
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            role_nodes = nodes_by_role.get(role, {}) if isinstance(nodes_by_role, dict) else {}
            # Identify top-1 by max
            top_max = 0.0
            try:
                top_max = max((float(v.get('max', 0)) for v in role_nodes.values() if isinstance(v, dict)), default=0.0)
            except Exception:
                top_max = 0.0
            
            table_key = f'{metric_name}_{role}'
            for node_name, stats in role_nodes.items():
                if not isinstance(stats, dict):
                    continue
                avg_val = float(stats.get('avg', 0))
                max_val = float(stats.get('max', 0))
                is_top = (max_val == top_max and max_val > 0)
                
                # Format values with unit (requirement 5.2)
                avg_readable = self.format_value_with_unit(avg_val, unit)
                max_readable = self.format_value_with_unit(max_val, unit)
                
                # Highlight critical and top-1 (requirement 7)
                if is_top:
                    max_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {max_readable}</span>'
                elif max_val >= thresholds.get('critical', float('inf')):
                    max_display = f'<span class="text-danger font-weight-bold">⚠️ {max_readable}</span>'
                elif max_val >= thresholds.get('warning', float('inf')):
                    max_display = f'<span class="text-warning font-weight-bold">{max_readable}</span>'
                else:
                    max_display = max_readable
                
                if avg_val >= thresholds.get('critical', float('inf')):
                    avg_display = f'<span class="text-danger font-weight-bold">⚠️ {avg_readable}</span>'
                elif avg_val >= thresholds.get('warning', float('inf')):
                    avg_display = f'<span class="text-warning font-weight-bold">{avg_readable}</span>'
                else:
                    avg_display = avg_readable
                
                structured[table_key].append({
                    'Metric': title,
                    'Role': role.title(),
                    'Node': node_name,
                    'Avg Usage': avg_display,
                    'Max Usage': max_display,
                    'Unit': unit
                })
    
    # =====================================================================
    # OVERVIEW
    # =====================================================================
    
    def _generate_overview(self, metrics_section: Dict[str, Any], structured: Dict[str, Any]):
        roles = ['controlplane', 'infra', 'worker', 'workload']
        for role in roles:
            # Count nodes by checking any metric's role table
            node_counts = []
            metrics_collected = 0
            for metric_name in self.metric_configs.keys():
                table_key = f'{metric_name}_{role}'
                rows = structured.get(table_key, [])
                if rows:
                    metrics_collected += 1
                    unique_nodes = {row.get('Node') for row in rows if isinstance(row, dict)}
                    node_counts.append(len(unique_nodes))
            node_count = max(node_counts) if node_counts else 0
            structured['network_socket_tcp_overview'].append({
                'Role': role.title(),
                'Nodes': node_count,
                'Metrics': metrics_collected,
                'Status': self.create_status_badge('success', 'Active') if node_count > 0 else self.create_status_badge('warning', 'No Data')
            })
    
    # =====================================================================
    # DATAFRAME + HTML TABLES
    # =====================================================================
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        dataframes: Dict[str, pd.DataFrame] = {}
        try:
            for key, value in structured_data.items():
                if isinstance(value, list) and value:
                    df = pd.DataFrame(value)
                    if not df.empty:
                        for col in df.columns:
                            if df[col].dtype == 'object':
                                df[col] = df[col].astype(str).apply(self.decode_unicode_escapes)
                        dataframes[key] = df
        except Exception as e:
            logger.error(f"Failed to transform network socket TCP data to DataFrames: {e}")
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        html_tables: Dict[str, str] = {}
        try:
            # Overview first
            if 'network_socket_tcp_overview' in dataframes and not dataframes['network_socket_tcp_overview'].empty:
                html_tables['network_socket_tcp_overview'] = self.create_html_table(
                    dataframes['network_socket_tcp_overview'],
                    'Network Socket TCP Overview'
                )
            # Metric tables grouped by role (requirement 5.3)
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
            logger.error(f"Failed to generate HTML tables for network socket TCP: {e}")
        return html_tables


