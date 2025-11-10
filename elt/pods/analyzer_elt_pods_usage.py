"""
Extract, Load, Transform module for Pods Usage Metrics
Handles pod usage data from tools/pods/pods_usage.py
ONLY contains pods_usage specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class podsUsageELT(utilityELT):
    """Extract, Load, Transform class for pod usage metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'container_cpu_usage_percent': {
                'title': 'Container CPU Usage',
                'unit': 'percent',
                'thresholds': {'critical': 80.0, 'warning': 60.0}
            },
            'container_memory_rss_bytes': {
                'title': 'Container Memory RSS',
                'unit': 'bytes',
                'thresholds': {'critical': 1073741824, 'warning': 536870912}  # 1GB, 512MB
            },
            'container_memory_working_set_bytes': {
                'title': 'Container Memory Working Set',
                'unit': 'bytes',
                'thresholds': {'critical': 1073741824, 'warning': 536870912}  # 1GB, 512MB
            }
        }
    
    def extract_pods_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract pod usage information from pods_usage.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'pods_usage_overview': []
        }
        
        # Get namespace from data
        namespace = actual_data.get('namespace', 'unknown')
        duration = actual_data.get('duration', '1h')
        
        # Check for ovnkube_node_containers structure
        container_data = actual_data.get('ovnkube_node_containers', {})
        if not container_data:
            # Fallback: check if metrics are at top level
            container_data = actual_data
        
        metrics = container_data.get('metrics', {})
        
        # Process each metric type
        for metric_name, metric_data in metrics.items():
            if 'error' in metric_data or metric_data.get('status') != 'success':
                continue
            
            if metric_name == 'container_cpu_usage_percent':
                self._extract_cpu_usage(metric_data, structured, namespace)
            elif metric_name == 'container_memory_rss_bytes':
                self._extract_memory_rss(metric_data, structured, namespace)
            elif metric_name == 'container_memory_working_set_bytes':
                self._extract_memory_working_set(metric_data, structured, namespace)
        
        # Generate overview
        self._generate_overview(actual_data, structured, namespace, duration)
        
        return structured

    def summarize_pods_usage(self, data: Dict[str, Any]) -> str:
        """Generate pods usage summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('pods_usage_overview', [])
            
            if overview_data:
                namespace = next((item.get('Namespace') for item in overview_data if 'Namespace' in item), 'Unknown')
                duration = next((item.get('Duration') for item in overview_data if 'Duration' in item), 'N/A')
                
                summary_items.append(f"<li>Namespace: {namespace}</li>")
                summary_items.append(f"<li>Duration: {duration}</li>")
                
                # Count metrics
                metric_count = len([k for k in data.keys() if k != 'pods_usage_overview' and data[k]])
                if metric_count > 0:
                    summary_items.append(f"<li>Metrics Collected: {metric_count}</li>")
                
                # Count total containers
                total_containers = sum(len(data[k]) for k in data.keys() if k != 'pods_usage_overview' and isinstance(data[k], list))
                if total_containers > 0:
                    summary_items.append(f"<li>Total Container Metrics: {total_containers}</li>")
            
            return (
                "<div class=\"pods-usage-summary\">"
                "<h4>Pods Usage Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate pods usage summary: {e}")
            return "Pods usage metrics collected"

    def _extract_cpu_usage(self, metric_data: Dict[str, Any], 
                          structured: Dict[str, Any], namespace: str):
        """Extract CPU usage metrics"""
        table_key = 'cpu_usage'
        structured[table_key] = []
        
        pod_metrics = metric_data.get('pod_container_metrics', {})
        thresholds = self.metric_configs['container_cpu_usage_percent']['thresholds']
        
        # Collect all values for top identification
        all_values = []
        for key, container_data in pod_metrics.items():
            stats = container_data.get('stats', {})
            avg_val = stats.get('avg_percent', 0)
            max_val = stats.get('max_percent', 0)
            all_values.append((key, container_data, avg_val, max_val))
        
        # Find top 1 by max value
        top_max = max((v[3] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        for key, container_data, avg_val, max_val in all_values:
            pod_name = container_data.get('pod_name', 'unknown')
            container_name = container_data.get('container_name', '')
            node_name = container_data.get('node_name', 'unknown')
            stats = container_data.get('stats', {})
            
            is_top = (max_val == top_max and max_val > 0)
            
            # Format values
            avg_display = self.format_and_highlight(avg_val, 'percent', thresholds, False)
            max_display = self.format_and_highlight(max_val, 'percent', thresholds, is_top)
            min_display = self.format_percentage(stats.get('min_percent', 0))
            latest_display = self.format_percentage(stats.get('latest_percent', 0))
            
            structured[table_key].append({
                'Pod': self.truncate_text(pod_name, 30),
                'Container': container_name if container_name else 'N/A',
                'Node': self.truncate_node_name(node_name),
                'Avg %': avg_display,
                'Max %': max_display,
                'Min %': min_display,
                'Latest %': latest_display,
                'Data Points': stats.get('data_points', 0)
            })
    
    def _extract_memory_rss(self, metric_data: Dict[str, Any], 
                           structured: Dict[str, Any], namespace: str):
        """Extract memory RSS metrics"""
        table_key = 'memory_rss'
        structured[table_key] = []
        
        pod_metrics = metric_data.get('pod_container_metrics', {})
        thresholds = self.metric_configs['container_memory_rss_bytes']['thresholds']
        
        # Collect all values for top identification
        all_values = []
        for key, container_data in pod_metrics.items():
            stats = container_data.get('stats', {})
            # Convert to bytes for comparison
            avg_val = self._convert_to_bytes(stats.get('avg_value', 0), stats.get('avg_unit', 'B'))
            max_val = self._convert_to_bytes(stats.get('max_value', 0), stats.get('max_unit', 'B'))
            all_values.append((key, container_data, avg_val, max_val))
        
        # Find top 1 by max value
        top_max = max((v[3] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        for key, container_data, avg_val, max_val in all_values:
            pod_name = container_data.get('pod_name', 'unknown')
            container_name = container_data.get('container_name', '')
            node_name = container_data.get('node_name', 'unknown')
            stats = container_data.get('stats', {})
            
            is_top = (max_val == top_max and max_val > 0)
            
            # Format values with units
            avg_display = self.format_and_highlight(avg_val, 'bytes', thresholds, False)
            max_display = self.format_and_highlight(max_val, 'bytes', thresholds, is_top)
            min_val = self._convert_to_bytes(stats.get('min_value', 0), stats.get('min_unit', 'B'))
            min_display = self.format_value_with_unit(min_val, 'bytes')
            latest_val = self._convert_to_bytes(stats.get('latest_value', 0), stats.get('latest_unit', 'B'))
            latest_display = self.format_value_with_unit(latest_val, 'bytes')
            
            structured[table_key].append({
                'Pod': self.truncate_text(pod_name, 30),
                'Container': container_name if container_name else 'N/A',
                'Node': self.truncate_node_name(node_name),
                'Avg': avg_display,
                'Max': max_display,
                'Min': min_display,
                'Latest': latest_display,
                'Data Points': stats.get('data_points', 0)
            })
    
    def _extract_memory_working_set(self, metric_data: Dict[str, Any], 
                                   structured: Dict[str, Any], namespace: str):
        """Extract memory working set metrics"""
        table_key = 'memory_working_set'
        structured[table_key] = []
        
        pod_metrics = metric_data.get('pod_container_metrics', {})
        thresholds = self.metric_configs['container_memory_working_set_bytes']['thresholds']
        
        # Collect all values for top identification
        all_values = []
        for key, container_data in pod_metrics.items():
            stats = container_data.get('stats', {})
            # Convert to bytes for comparison
            avg_val = self._convert_to_bytes(stats.get('avg_value', 0), stats.get('avg_unit', 'B'))
            max_val = self._convert_to_bytes(stats.get('max_value', 0), stats.get('max_unit', 'B'))
            all_values.append((key, container_data, avg_val, max_val))
        
        # Find top 1 by max value
        top_max = max((v[3] for v in all_values), default=0) if all_values else 0
        
        # Format rows
        for key, container_data, avg_val, max_val in all_values:
            pod_name = container_data.get('pod_name', 'unknown')
            container_name = container_data.get('container_name', '')
            node_name = container_data.get('node_name', 'unknown')
            stats = container_data.get('stats', {})
            
            is_top = (max_val == top_max and max_val > 0)
            
            # Format values with units
            avg_display = self.format_and_highlight(avg_val, 'bytes', thresholds, False)
            max_display = self.format_and_highlight(max_val, 'bytes', thresholds, is_top)
            min_val = self._convert_to_bytes(stats.get('min_value', 0), stats.get('min_unit', 'B'))
            min_display = self.format_value_with_unit(min_val, 'bytes')
            latest_val = self._convert_to_bytes(stats.get('latest_value', 0), stats.get('latest_unit', 'B'))
            latest_display = self.format_value_with_unit(latest_val, 'bytes')
            
            structured[table_key].append({
                'Pod': self.truncate_text(pod_name, 30),
                'Container': container_name if container_name else 'N/A',
                'Node': self.truncate_node_name(node_name),
                'Avg': avg_display,
                'Max': max_display,
                'Min': min_display,
                'Latest': latest_display,
                'Data Points': stats.get('data_points', 0)
            })
    
    def _convert_to_bytes(self, value: float, unit: str) -> float:
        """Convert memory value to bytes"""
        try:
            value = float(value)
            unit = unit.upper()
            
            if unit == 'B':
                return value
            elif unit == 'KB':
                return value * 1024
            elif unit == 'MB':
                return value * 1024 * 1024
            elif unit == 'GB':
                return value * 1024 * 1024 * 1024
            else:
                return value
        except (ValueError, TypeError):
            return 0.0
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any], 
                          namespace: str, duration: str):
        """Generate pods usage overview"""
        container_data = data.get('ovnkube_node_containers', {})
        if not container_data:
            container_data = data
        
        metrics = container_data.get('metrics', {})
        summary = container_data.get('summary', {})
        
        structured['pods_usage_overview'].append({
            'Property': 'Namespace',
            'Value': namespace
        })
        
        structured['pods_usage_overview'].append({
            'Property': 'Duration',
            'Value': duration
        })
        
        structured['pods_usage_overview'].append({
            'Property': 'Total Metrics',
            'Value': summary.get('total_metrics', 0)
        })
        
        structured['pods_usage_overview'].append({
            'Property': 'Successful Metrics',
            'Value': summary.get('successful_metrics', 0)
        })
        
        structured['pods_usage_overview'].append({
            'Property': 'Failed Metrics',
            'Value': summary.get('failed_metrics', 0)
        })
        
        # Add timestamp if available
        timestamp = container_data.get('timestamp', data.get('timestamp', ''))
        if timestamp:
            structured['pods_usage_overview'].append({
                'Property': 'Collection Time',
                'Value': self.format_timestamp(timestamp)
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
            logger.error(f"Failed to transform pods usage data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            # Overview table first
            if 'pods_usage_overview' in dataframes and not dataframes['pods_usage_overview'].empty:
                html_tables['pods_usage_overview'] = self.create_html_table(
                    dataframes['pods_usage_overview'], 
                    'Pods Usage Overview'
                )
            
            # Metric tables
            metric_names = {
                'cpu_usage': 'Container CPU Usage',
                'memory_rss': 'Container Memory RSS',
                'memory_working_set': 'Container Memory Working Set'
            }
            
            for table_key, display_name in metric_names.items():
                if table_key in dataframes and not dataframes[table_key].empty:
                    html_tables[table_key] = self.create_html_table(
                        dataframes[table_key], 
                        display_name
                    )
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for pods usage: {e}")
        
        return html_tables