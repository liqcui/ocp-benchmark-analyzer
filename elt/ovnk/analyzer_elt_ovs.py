"""
Extract, Load, Transform module for OVS Usage Metrics
Handles OVS CPU, Memory, Flow, and Performance metrics from ovnk_ovs_usage.py
ONLY contains OVS usage specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class ovsUsageELT(utilityELT):
    """Extract, Load, Transform class for OVS usage metrics data"""
    
    def __init__(self):
        super().__init__()
        self.node_groups = ['controlplane', 'infra', 'worker', 'workload']
    
    def extract_ovs_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVS usage information from ovnk_ovs_usage.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'ovs_usage_overview': [],
        }
        
        # Initialize tables for each metric and node group
        metrics = actual_data.get('metrics', {})
        
        for metric_name in metrics.keys():
            for group in self.node_groups:
                table_key = f'{metric_name}_{group}'
                structured[table_key] = []
        
        # Extract each metric
        for metric_name, metric_data in metrics.items():
            if metric_data.get('status') != 'success':
                continue
            
            if 'cpu' in metric_name:
                self._extract_cpu_usage(metric_name, metric_data, structured)
            elif 'memory' in metric_name or 'size_bytes' in metric_name:
                self._extract_memory_usage(metric_name, metric_data, structured)
            elif 'flows' in metric_name:
                self._extract_flows(metric_name, metric_data, structured)
            elif 'connections' in metric_name:
                self._extract_connections(metric_name, metric_data, structured)
            elif 'overflow' in metric_name or 'discarded' in metric_name:
                self._extract_error_metrics(metric_name, metric_data, structured)
            elif 'cache' in metric_name:
                self._extract_cache_metrics(metric_name, metric_data, structured)
            elif 'rate' in metric_name:
                self._extract_rate_metrics(metric_name, metric_data, structured)
        
        # Generate overview
        self._generate_overview(actual_data, structured)
        
        return structured
    
    def _extract_cpu_usage(self, metric_name: str, metric_data: Dict[str, Any], 
                        structured: Dict[str, Any]):
        """Extract CPU usage metrics"""
        unit = metric_data.get('unit', 'percent')
        node_mapping = metric_data.get('node_mapping', {})
        metric_values = metric_data.get('metric_data', {})
        
        # Skip if no data
        if not metric_values:
            return
        
        # Collect all values for top identification
        all_values = []
        for key, stats in metric_values.items():
            if '@' in key:
                # Pod-based metric
                pod_name = key.split('@')[0]
                node = node_mapping.get(pod_name, {}).get('node', 'Unknown') if node_mapping else 'Unknown'
                # Infer role from key if node_mapping is empty
                role = self._infer_role_from_name(key)
            else:
                # Node-based metric - key is the node name
                node = key
                pod_name = None
                # Infer role from node name if node_mapping is empty
                role = self._infer_role_from_name(node) if not node_mapping else node_mapping.get(node, {}).get('role', 'unknown')
            
            max_val = float(stats.get('max', 0))
            avg_val = float(stats.get('avg', 0))
            
            all_values.append((key, pod_name, node, role, max_val, avg_val, stats))
        
        # Find top max value
        top_max = max((v[4] for v in all_values), default=0) if all_values else 0
        
        # Group by role and format
        for key, pod_name, node, role, max_val, avg_val, stats in all_values:
            if role not in self.node_groups:
                role = 'worker'  # Default
            
            table_key = f'{metric_name}_{role}'
            is_top = (max_val == top_max and max_val > 0)
            
            row = {
                'Node': self.truncate_node_name(node),
            }
            
            if pod_name:
                row['Pod'] = self.truncate_text(pod_name, 30)
            
            row['Avg (%)'] = self.highlight_ovs_value(avg_val, metric_name, unit, False)
            row['Max (%)'] = self.highlight_ovs_value(max_val, metric_name, unit, is_top)
            row['Min (%)'] = f"{stats.get('min', 0):.2f}"
            
            structured[table_key].append(row)

    def _extract_memory_usage(self, metric_name: str, metric_data: Dict[str, Any], 
                            structured: Dict[str, Any]):
        """Extract memory usage metrics"""
        unit = metric_data.get('unit', 'bytes')
        node_mapping = metric_data.get('node_mapping', {})
        metric_values = metric_data.get('metric_data', {})
        
        # Skip if no data
        if not metric_values:
            return
        
        # Collect all values
        all_values = []
        for key, stats in metric_values.items():
            if '@' in key:
                pod_name = key.split('@')[0]
                node = node_mapping.get(pod_name, {}).get('node', 'Unknown') if node_mapping else 'Unknown'
                role = self._infer_role_from_name(key)
            else:
                node = key
                pod_name = key  # For pod-based metrics, use the key as pod name
                role = self._infer_role_from_name(node) if not node_mapping else node_mapping.get(node, {}).get('role', 'unknown')
            
            # Convert bytes to GB for display
            max_val = float(stats.get('max', 0)) / (1024**3)
            avg_val = float(stats.get('avg', 0)) / (1024**3)
            min_val = float(stats.get('min', 0)) / (1024**3)
            
            all_values.append((key, pod_name, node, role, max_val, avg_val, min_val))
        
        # Find top max value
        top_max = max((v[4] for v in all_values), default=0) if all_values else 0
        
        # Group by role and format
        for key, pod_name, node, role, max_val, avg_val, min_val in all_values:
            if role not in self.node_groups:
                role = 'worker'
            
            table_key = f'{metric_name}_{role}'
            is_top = (max_val == top_max and max_val > 0)
            
            row = {
                'Pod': self.truncate_text(pod_name, 30),
            }
            
            row['Avg (GB)'] = self.highlight_ovs_value(avg_val, metric_name, '', False)
            row['Max (GB)'] = self.highlight_ovs_value(max_val, metric_name, '', is_top)
            row['Min (GB)'] = f"{min_val:.2f}"
            
            structured[table_key].append(row)

    def _extract_flows(self, metric_name: str, metric_data: Dict[str, Any], 
                    structured: Dict[str, Any]):
        """Extract flow metrics"""
        unit = metric_data.get('unit', 'flows')
        node_mapping = metric_data.get('node_mapping', {})
        metric_values = metric_data.get('metric_data', {})
        
        # Skip if no data
        if not metric_values:
            return
        
        # Collect all values
        all_values = []
        for key, stats in metric_values.items():
            # Handle bridge@instance or pod format
            if '@' in key:
                parts = key.split('@')
                bridge = stats.get('bridge', parts[0])
                instance = stats.get('instance', parts[1] if len(parts) > 1 else 'unknown')
                node = node_mapping.get(instance, {}).get('node', instance) if node_mapping else instance
                pod_name = instance
            else:
                bridge = None
                instance = key
                node = key
                pod_name = key
            
            role = self._infer_role_from_name(pod_name if pod_name else node)
            max_val = float(stats.get('max', 0))
            avg_val = float(stats.get('avg', 0))
            
            all_values.append((key, bridge, instance, pod_name, node, role, max_val, avg_val, stats))
        
        # Find top max value
        top_max = max((v[6] for v in all_values), default=0) if all_values else 0
        
        # Group by role and format
        for key, bridge, instance, pod_name, node, role, max_val, avg_val, stats in all_values:
            if role not in self.node_groups:
                role = 'worker'
            
            table_key = f'{metric_name}_{role}'
            is_top = (max_val == top_max and max_val > 0)
            
            row = {
                'Pod': self.truncate_text(pod_name, 30),
            }
            
            if bridge:
                row['Bridge'] = bridge
            
            row['Avg Flows'] = self.highlight_ovs_value(avg_val, metric_name, unit, False)
            row['Max Flows'] = self.highlight_ovs_value(max_val, metric_name, unit, is_top)
            row['Min Flows'] = self.format_flow_count(stats.get('min', 0))
            
            structured[table_key].append(row)
   
    def _extract_connections(self, metric_name: str, metric_data: Dict[str, Any], 
                        structured: Dict[str, Any]):
        """Extract connection metrics"""
        unit = metric_data.get('unit', 'connections')
        node_mapping = metric_data.get('node_mapping', {})
        metric_values = metric_data.get('metric_data', {})
        
        # Skip if no data
        if not metric_values:
            return
        
        # Collect all values
        all_values = []
        for key, stats in metric_values.items():
            if '@' in key:
                pod_name = key.split('@')[0]
                node = node_mapping.get(pod_name, {}).get('node', 'Unknown') if node_mapping else 'Unknown'
            else:
                node = key
                pod_name = key
            
            role = self._infer_role_from_name(pod_name if pod_name else node)
            max_val = float(stats.get('max', 0))
            avg_val = float(stats.get('avg', 0))
            
            all_values.append((key, pod_name, node, role, max_val, avg_val, stats))
        
        # Find top max value
        top_max = max((v[4] for v in all_values), default=0) if all_values else 0
        
        # Group by role and format
        for key, pod_name, node, role, max_val, avg_val, stats in all_values:
            if role not in self.node_groups:
                role = 'worker'
            
            table_key = f'{metric_name}_{role}'
            is_top = (max_val == top_max and max_val > 0)
            
            row = {
                'Pod': self.truncate_text(pod_name, 30),
            }
            
            row['Avg'] = self.highlight_ovs_value(avg_val, metric_name, unit, False)
            row['Max'] = self.highlight_ovs_value(max_val, metric_name, unit, is_top)
            
            structured[table_key].append(row)

    def _extract_error_metrics(self, metric_name: str, metric_data: Dict[str, Any], 
                            structured: Dict[str, Any]):
        """Extract overflow and discarded metrics"""
        unit = metric_data.get('unit', 'count')
        node_mapping = metric_data.get('node_mapping', {})
        metric_values = metric_data.get('metric_data', {})
        
        # Skip if no data
        if not metric_values:
            return
        
        # Similar pattern as connections...
        all_values = []
        for key, stats in metric_values.items():
            if '@' in key:
                pod_name = key.split('@')[0]
                node = node_mapping.get(pod_name, {}).get('node', 'Unknown') if node_mapping else 'Unknown'
            else:
                node = key
                pod_name = key
            
            role = self._infer_role_from_name(pod_name if pod_name else node)
            max_val = float(stats.get('max', 0))
            avg_val = float(stats.get('avg', 0))
            
            all_values.append((key, pod_name, node, role, max_val, avg_val, stats))
        
        # Find top max value
        top_max = max((v[4] for v in all_values), default=0) if all_values else 0
        
        # Group by role and format
        for key, pod_name, node, role, max_val, avg_val, stats in all_values:
            if role not in self.node_groups:
                role = 'worker'
            
            table_key = f'{metric_name}_{role}'
            is_top = (max_val == top_max and max_val > 0)
            
            row = {
                'Pod': self.truncate_text(pod_name, 30),
            }
            
            row['Avg'] = self.highlight_ovs_value(avg_val, metric_name, unit, False)
            row['Max'] = self.highlight_ovs_value(max_val, metric_name, unit, is_top)
            
            structured[table_key].append(row)
  
    def _extract_cache_metrics(self, metric_name: str, metric_data: Dict[str, Any], 
                            structured: Dict[str, Any]):
        """Extract cache hit/miss metrics"""
        unit = metric_data.get('unit', 'count')
        node_mapping = metric_data.get('node_mapping', {})
        metric_values = metric_data.get('metric_data', {})
        
        # Skip if no data
        if not metric_values:
            return
        
        # Collect all values
        all_values = []
        for key, stats in metric_values.items():
            node = key
            role = self._infer_role_from_name(node)
            max_val = float(stats.get('max', 0))
            avg_val = float(stats.get('avg', 0))
            
            all_values.append((node, role, max_val, avg_val, stats))
        
        # Find top max value
        top_max = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Group by role and format
        for node, role, max_val, avg_val, stats in all_values:
            if role not in self.node_groups:
                role = 'worker'
            
            table_key = f'{metric_name}_{role}'
            is_top = (max_val == top_max and max_val > 0)
            
            row = {
                'Node': self.truncate_node_name(node),
                'Avg': self.highlight_ovs_value(avg_val, metric_name, unit, False),
                'Max': self.highlight_ovs_value(max_val, metric_name, unit, is_top),
            }
            
            structured[table_key].append(row)
   
    def _extract_rate_metrics(self, metric_name: str, metric_data: Dict[str, Any], 
                            structured: Dict[str, Any]):
        """Extract rate metrics (packet rate, error rate, bytes rate)"""
        unit = metric_data.get('unit', 'per_second')
        node_mapping = metric_data.get('node_mapping', {})
        metric_values = metric_data.get('metric_data', {})
        
        # Skip if no data
        if not metric_values:
            return
        
        # Collect all values
        all_values = []
        for key, stats in metric_values.items():
            if '@' in key:
                pod_name = key.split('@')[0]
                node = node_mapping.get(pod_name, {}).get('node', 'Unknown') if node_mapping else key
            else:
                node = key
                pod_name = None
            
            role = self._infer_role_from_name(pod_name if pod_name else node)
            max_val = float(stats.get('max', 0))
            avg_val = float(stats.get('avg', 0))
            
            all_values.append((key, pod_name, node, role, max_val, avg_val, stats))
        
        # Find top max value
        top_max = max((v[4] for v in all_values), default=0) if all_values else 0
        
        # Group by role and format
        for key, pod_name, node, role, max_val, avg_val, stats in all_values:
            if role not in self.node_groups:
                role = 'worker'
            
            table_key = f'{metric_name}_{role}'
            is_top = (max_val == top_max and max_val > 0)
            
            row = {
                'Node': self.truncate_node_name(node),
            }
            
            if pod_name:
                row['Pod'] = self.truncate_text(pod_name, 30)
            
            row['Avg'] = self.highlight_ovs_value(avg_val, metric_name, unit, False)
            row['Max'] = self.highlight_ovs_value(max_val, metric_name, unit, is_top)
            
            structured[table_key].append(row)
  
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
            logger.error(f"Failed to transform OVS usage data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        try:
            # Overview table first
            if 'ovs_usage_overview' in dataframes and not dataframes['ovs_usage_overview'].empty:
                html_tables['ovs_usage_overview'] = self.create_html_table(
                    dataframes['ovs_usage_overview'], 
                    'OVS Usage Overview'
                )
            
            # Group tables by metric type
            metric_groups = {}
            for key in dataframes.keys():
                if key == 'ovs_usage_overview':
                    continue
                
                # Extract metric name and role
                parts = key.rsplit('_', 1)
                if len(parts) == 2:
                    metric_name, role = parts
                    if role in self.node_groups:
                        if metric_name not in metric_groups:
                            metric_groups[metric_name] = []
                        metric_groups[metric_name].append(role)
            
            # Generate tables for each metric group and role
            for metric_name, roles in metric_groups.items():
                metric_display = metric_name.replace('_', ' ').title()
                
                for role in self.node_groups:
                    if role not in roles:
                        continue
                    
                    table_key = f'{metric_name}_{role}'
                    if table_key in dataframes and not dataframes[table_key].empty:
                        display_name = f"{metric_display} - {role.title()}"
                        html_tables[table_key] = self.create_html_table(
                            dataframes[table_key], 
                            display_name
                        )
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for OVS usage: {e}")
        
        return html_tables
    
    def summarize_ovs_usage(self, data: Dict[str, Any]) -> str:
        """Generate OVS usage summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('ovs_usage_overview', [])
            
            if overview_data:
                overview = overview_data[0]
                total_metrics = overview.get('Total Metrics', '0')
                successful = overview.get('Successful', '0')
                summary_items.append(f"<li>Metrics: {successful}/{total_metrics} successful</li>")
                
                if 'Total Nodes' in overview:
                    summary_items.append(f"<li>Total Nodes: {overview['Total Nodes']}</li>")
                
                if 'Performance' in overview:
                    perf = overview['Performance']
                    if perf.lower() == 'good':
                        badge = self.create_status_badge('success', perf)
                    elif perf.lower() == 'warning':
                        badge = self.create_status_badge('warning', perf)
                    else:
                        badge = self.create_status_badge('danger', perf)
                    summary_items.append(f"<li>Performance: {badge}</li>")
            
            return (
                "<div class=\"ovs-usage-summary\">"
                "<h4>OVS Usage Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate OVS usage summary: {e}")
            return "OVS usage metrics collected"

    def _infer_role_from_name(self, name: str) -> str:
        """Infer node role from node/pod name"""
        name_lower = str(name).lower()
        
        # Check for common role patterns
        if any(pattern in name_lower for pattern in ['master', 'control', 'cp-']):
            return 'controlplane'
        elif any(pattern in name_lower for pattern in ['infra', 'inf-']):
            return 'infra'
        elif any(pattern in name_lower for pattern in ['work-', 'workload']):
            return 'workload'
        else:
            return 'worker'  # Default to worker

    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate OVS usage overview"""
        metrics = data.get('metrics', {})
        
        # Count successful metrics with data
        successful_with_data = 0
        total_metrics = 0
        
        for metric_name, metric_data in metrics.items():
            if metric_data.get('status') == 'success':
                total_metrics += 1
                if metric_data.get('metric_data') and len(metric_data.get('metric_data', {})) > 0:
                    successful_with_data += 1
        
        # Get cluster health info if available
        cluster_health = data.get('cluster_health', {})
        performance_indicators = data.get('performance_indicators', {})
        
        overview_row = {
            'Total Metrics': str(total_metrics),
            'Metrics with Data': str(successful_with_data),
            'Empty Metrics': str(total_metrics - successful_with_data),
        }
        
        if cluster_health:
            overview_row['Total Nodes'] = str(cluster_health.get('total_nodes', 0))
            perf = cluster_health.get('ovs_performance', 'unknown')
            if perf.lower() == 'good':
                overview_row['Performance'] = self.create_status_badge('success', perf.title())
            elif perf.lower() == 'warning':
                overview_row['Performance'] = self.create_status_badge('warning', perf.title())
            else:
                overview_row['Performance'] = self.create_status_badge('danger', perf.title())
        
        # Add key performance indicators if available
        if performance_indicators:
            if 'ovs_vswitchd_max_cpu_percent' in performance_indicators:
                overview_row['Max vSwitchd CPU (%)'] = f"{performance_indicators['ovs_vswitchd_max_cpu_percent']:.2f}"
            if 'max_datapath_flows' in performance_indicators:
                overview_row['Max Datapath Flows'] = self.format_flow_count(performance_indicators['max_datapath_flows'])
        
        structured['ovs_usage_overview'].append(overview_row)