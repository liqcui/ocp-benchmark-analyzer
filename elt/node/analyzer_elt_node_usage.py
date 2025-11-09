"""
Extract, Load, Transform module for Node Usage Metrics
Handles node usage data from tools/node/node_usage.py
ONLY contains node_usage specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class nodeUsageELT(utilityELT):
    """Extract, Load, Transform class for node usage metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'cpu_usage': {
                'title': 'CPU Usage',
                'unit': 'percent',
                'thresholds': {'critical': 85.0, 'warning': 70.0}
            },
            'memory_used': {
                'title': 'Memory Used',
                'unit': 'GB',
                'thresholds': {'critical': 85.0, 'warning': 70.0}  # For percentage
            },
            'memory_cache_buffer': {
                'title': 'Memory Cache/Buffer',
                'unit': 'GB',
                'thresholds': {'critical': 85.0, 'warning': 70.0}  # For percentage
            },
            'cgroup_cpu_usage': {
                'title': 'Cgroup CPU Usage',
                'unit': 'percent',
                'thresholds': {'critical': 85.0, 'warning': 70.0}
            },
            'cgroup_rss_usage': {
                'title': 'Cgroup RSS Usage',
                'unit': 'GB',
                'thresholds': {'critical': 85.0, 'warning': 70.0}
            }
        }
    
    def extract_node_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract node usage information from node_usage.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'overview': [],
            'timestamp': actual_data.get('timestamp', ''),
            'duration': actual_data.get('duration', ''),
            'time_range': actual_data.get('time_range', {}),
        }
        
        # Extract node_group from query_params if available
        query_params = actual_data.get('query_params', {})
        node_group = query_params.get('node_group', 'unknown')
        
        # Get nodes list with roles
        nodes_list = actual_data.get('nodes', [])
        node_role_map = {node['name']: node.get('role', node_group) for node in nodes_list}
        
        # Initialize role-based tables for each metric
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            structured[f'cpu_usage_{role}'] = []
            structured[f'memory_used_{role}'] = []
            structured[f'memory_cache_buffer_{role}'] = []
            structured[f'cgroup_cpu_usage_{role}'] = []
            structured[f'cgroup_rss_usage_{role}'] = []
        
        metrics = actual_data.get('metrics', {})
        
        # Extract each metric type
        if 'cpu_usage' in metrics:
            self._extract_cpu_usage(metrics['cpu_usage'], structured, node_role_map)
        
        if 'memory_used' in metrics:
            self._extract_memory_used(metrics['memory_used'], structured, node_role_map)
        
        if 'memory_cache_buffer' in metrics:
            self._extract_memory_cache_buffer(metrics['memory_cache_buffer'], structured, node_role_map)
        
        if 'cgroup_cpu_usage' in metrics:
            self._extract_cgroup_cpu_usage(metrics['cgroup_cpu_usage'], structured, node_role_map)
        
        if 'cgroup_rss_usage' in metrics:
            self._extract_cgroup_rss_usage(metrics['cgroup_rss_usage'], structured, node_role_map)
        
        # Generate overview
        self._generate_overview(actual_data, structured, node_role_map)
        
        return structured
    
    def _extract_cpu_usage(self, metric_data: Dict[str, Any], 
                          structured: Dict[str, Any], 
                          node_role_map: Dict[str, str]):
        """Extract CPU usage metrics grouped by role"""
        nodes = metric_data.get('nodes', {})
        
        # Collect all total CPU values for top identification
        all_totals = []
        for node_name, node_data in nodes.items():
            total = node_data.get('total', {})
            if total:
                role = self._get_node_role(node_name, node_role_map)
                all_totals.append((node_name, role, float(total.get('max', 0))))
        
        # Find top 1 max value
        top_max = max((t[2] for t in all_totals), default=0) if all_totals else 0
        
        # Process each node
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            table_key = f'cpu_usage_{role}'
            
            modes = node_data.get('modes', {})
            total = node_data.get('total', {})
            
            # Add mode rows
            for mode, mode_data in modes.items():
                avg_val = float(mode_data.get('avg', 0))
                max_val = float(mode_data.get('max', 0))
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Mode': mode,
                    'Avg': self._format_cpu_value(avg_val, is_top=False),
                    'Max': self._format_cpu_value(max_val, is_top=False)
                })
            
            # Add TOTAL row with highlighting
            if total:
                avg_val = float(total.get('avg', 0))
                max_val = float(total.get('max', 0))
                is_top = (max_val == top_max and max_val > 0)
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Mode': '<strong>TOTAL</strong>',
                    'Avg': f'<strong>{self._format_cpu_value(avg_val, is_top=False)}</strong>',
                    'Max': f'<strong>{self._format_cpu_value(max_val, is_top=is_top)}</strong>'
                })
    
    def _extract_memory_used(self, metric_data: Dict[str, Any], 
                            structured: Dict[str, Any], 
                            node_role_map: Dict[str, str]):
        """Extract memory used metrics grouped by role"""
        nodes = metric_data.get('nodes', {})
        
        # Collect all max values for top identification
        all_values = []
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            
            # Check if we have percentage data
            if 'max_percent' in node_data:
                max_pct = float(node_data.get('max_percent', 0))
                all_values.append((node_name, role, max_pct))
            else:
                max_val = float(node_data.get('max', 0))
                all_values.append((node_name, role, max_val))
        
        # Find top 1
        top_value = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Process each node
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            table_key = f'memory_used_{role}'
            
            avg_gb = float(node_data.get('avg', 0))
            max_gb = float(node_data.get('max', 0))
            
            # Check if we have percentage columns
            if 'avg_percent' in node_data and 'max_percent' in node_data:
                avg_pct = float(node_data.get('avg_percent', 0))
                max_pct = float(node_data.get('max_percent', 0))
                is_top = (max_pct == top_value and max_pct > 0)
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Avg (GB)': f'{avg_gb:.2f}',
                    'RAM Used (%)': self._format_memory_percent(avg_pct, is_top=False),
                    'Max (GB)': f'{max_gb:.2f}',
                    'Max RAM (%)': self._format_memory_percent(max_pct, is_top=is_top)
                })
            else:
                # Fallback without percentages
                is_top = (max_gb == top_value and max_gb > 0)
                thresholds = self.metric_configs['memory_used']['thresholds']
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Avg': self.highlight_critical_values(avg_gb, thresholds, ' GB', is_top=False),
                    'Max': self.highlight_critical_values(max_gb, thresholds, ' GB', is_top=is_top)
                })
    
    def _extract_memory_cache_buffer(self, metric_data: Dict[str, Any], 
                                     structured: Dict[str, Any], 
                                     node_role_map: Dict[str, str]):
        """Extract memory cache/buffer metrics grouped by role"""
        nodes = metric_data.get('nodes', {})
        
        # Collect all max values for top identification
        all_values = []
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            
            # Check if we have percentage data
            if 'max_percent' in node_data:
                max_pct = float(node_data.get('max_percent', 0))
                all_values.append((node_name, role, max_pct))
            else:
                max_val = float(node_data.get('max', 0))
                all_values.append((node_name, role, max_val))
        
        # Find top 1
        top_value = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Process each node
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            table_key = f'memory_cache_buffer_{role}'
            
            avg_gb = float(node_data.get('avg', 0))
            max_gb = float(node_data.get('max', 0))
            
            # Check if we have percentage columns
            if 'avg_percent' in node_data and 'max_percent' in node_data:
                avg_pct = float(node_data.get('avg_percent', 0))
                max_pct = float(node_data.get('max_percent', 0))
                is_top = (max_pct == top_value and max_pct > 0)
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Avg (GB)': f'{avg_gb:.2f}',
                    'Cache/Buffer (%)': self._format_memory_percent(avg_pct, is_top=False),
                    'Max (GB)': f'{max_gb:.2f}',
                    'Max Cache (%)': self._format_memory_percent(max_pct, is_top=is_top)
                })
            else:
                # Fallback without percentages
                is_top = (max_gb == top_value and max_gb > 0)
                thresholds = self.metric_configs['memory_cache_buffer']['thresholds']
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Avg': self.highlight_critical_values(avg_gb, thresholds, ' GB', is_top=False),
                    'Max': self.highlight_critical_values(max_gb, thresholds, ' GB', is_top=is_top)
                })
    
    def _extract_cgroup_cpu_usage(self, metric_data: Dict[str, Any], 
                                  structured: Dict[str, Any], 
                                  node_role_map: Dict[str, str]):
        """Extract cgroup CPU usage metrics grouped by role"""
        nodes = metric_data.get('nodes', {})
        
        # Collect all total max values for top identification
        all_totals = []
        for node_name, node_data in nodes.items():
            total = node_data.get('total', {})
            if total:
                role = self._get_node_role(node_name, node_role_map)
                all_totals.append((node_name, role, float(total.get('max', 0))))
        
        # Find top 1
        top_max = max((t[2] for t in all_totals), default=0) if all_totals else 0
        
        # Process each node
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            table_key = f'cgroup_cpu_usage_{role}'
            
            cgroups = node_data.get('cgroups', {})
            total = node_data.get('total', {})
            
            # Add cgroup rows
            for cgroup_name, cgroup_data in cgroups.items():
                avg_val = float(cgroup_data.get('avg', 0))
                max_val = float(cgroup_data.get('max', 0))
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Cgroup': cgroup_name,
                    'Avg': self._format_cpu_value(avg_val, is_top=False),
                    'Max': self._format_cpu_value(max_val, is_top=False)
                })
            
            # Add TOTAL row
            if total:
                avg_val = float(total.get('avg', 0))
                max_val = float(total.get('max', 0))
                is_top = (max_val == top_max and max_val > 0)
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Cgroup': '<strong>TOTAL</strong>',
                    'Avg': f'<strong>{self._format_cpu_value(avg_val, is_top=False)}</strong>',
                    'Max': f'<strong>{self._format_cpu_value(max_val, is_top=is_top)}</strong>'
                })
    
    def _extract_cgroup_rss_usage(self, metric_data: Dict[str, Any], 
                                  structured: Dict[str, Any], 
                                  node_role_map: Dict[str, str]):
        """Extract cgroup RSS usage metrics grouped by role"""
        nodes = metric_data.get('nodes', {})
        
        # Collect all total max values for top identification
        all_totals = []
        for node_name, node_data in nodes.items():
            total = node_data.get('total', {})
            if total:
                role = self._get_node_role(node_name, node_role_map)
                all_totals.append((node_name, role, float(total.get('max', 0))))
        
        # Find top 1
        top_max = max((t[2] for t in all_totals), default=0) if all_totals else 0
        
        # Process each node
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            table_key = f'cgroup_rss_usage_{role}'
            
            cgroups = node_data.get('cgroups', {})
            total = node_data.get('total', {})
            
            # Add cgroup rows
            for cgroup_name, cgroup_data in cgroups.items():
                avg_val = float(cgroup_data.get('avg', 0))
                max_val = float(cgroup_data.get('max', 0))
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Cgroup': cgroup_name,
                    'Avg': self._format_memory_gb(avg_val, is_top=False),
                    'Max': self._format_memory_gb(max_val, is_top=False)
                })
            
            # Add TOTAL row
            if total:
                avg_val = float(total.get('avg', 0))
                max_val = float(total.get('max', 0))
                is_top = (max_val == top_max and max_val > 0)
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Cgroup': '<strong>TOTAL</strong>',
                    'Avg': f'<strong>{self._format_memory_gb(avg_val, is_top=False)}</strong>',
                    'Max': f'<strong>{self._format_memory_gb(max_val, is_top=is_top)}</strong>'
                })
    
    def _generate_overview(self, data: Dict[str, Any], 
                          structured: Dict[str, Any],
                          node_role_map: Dict[str, str]):
        """Generate node usage overview"""
        # Count nodes by role
        role_counts = {}
        for node_name, role in node_role_map.items():
            role_counts[role] = role_counts.get(role, 0) + 1
        
        metrics = data.get('metrics', {})
        metrics_collected = len([k for k in metrics.keys() if metrics[k].get('status') == 'success'])
        
        for role, count in sorted(role_counts.items()):
            structured['overview'].append({
                'Role': role.title(),
                'Nodes': count,
                'Metrics Collected': metrics_collected,
                'Status': self.create_status_badge('success', 'Active')
            })
    
    def _get_node_role(self, node_name: str, node_role_map: Dict[str, str]) -> str:
        """Get role for a node, defaulting to 'worker' if not found"""
        return node_role_map.get(node_name, 'worker')
    
    def _format_cpu_value(self, value: float, is_top: bool = False) -> str:
        """Format CPU percentage value with thresholds and top highlighting"""
        thresholds = self.metric_configs['cpu_usage']['thresholds']
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {value:.2f}%</span>'
        elif value >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {value:.2f}%</span>'
        elif value >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{value:.2f}%</span>'
        else:
            return f'{value:.2f}%'
    
    def _format_memory_percent(self, value: float, is_top: bool = False) -> str:
        """Format memory percentage value with thresholds and top highlighting"""
        thresholds = self.metric_configs['memory_used']['thresholds']
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {value:.2f}%</span>'
        elif value >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {value:.2f}%</span>'
        elif value >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{value:.2f}%</span>'
        else:
            return f'{value:.2f}%'
    
    def _format_memory_gb(self, value: float, is_top: bool = False) -> str:
        """Format memory GB value with thresholds and top highlighting"""
        thresholds = self.metric_configs['cgroup_rss_usage']['thresholds']
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {value:.2f} GB</span>'
        elif value >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {value:.2f} GB</span>'
        elif value >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{value:.2f} GB</span>'
        else:
            return f'{value:.2f} GB'
    
    def summarize_node_usage(self, data: Dict[str, Any]) -> str:
        """Generate node usage summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('overview', [])
            
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
            
            # Time range
            time_range = data.get('time_range', {})
            if time_range:
                duration = data.get('duration', 'unknown')
                summary_items.append(f"<li>Duration: {duration}</li>")
            
            return (
                "<div class=\"node-usage-summary\">"
                "<h4>Node Usage Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate node usage summary: {e}")
            return "Node usage metrics collected"
    
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
            logger.error(f"Failed to transform node usage data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        # Define metric groups
        metric_groups = {
            'CPU Usage': 'cpu_usage',
            'Memory Used': 'memory_used',
            'Memory Cache/Buffer': 'memory_cache_buffer',
            'Cgroup CPU Usage': 'cgroup_cpu_usage',
            'Cgroup RSS Usage': 'cgroup_rss_usage'
        }
        
        try:
            # Overview table first
            if 'overview' in dataframes and not dataframes['overview'].empty:
                html_tables['overview'] = self.create_html_table(
                    dataframes['overview'], 
                    'Node Usage Overview'
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
            logger.error(f"Failed to generate HTML tables for node usage: {e}")
        
        return html_tables