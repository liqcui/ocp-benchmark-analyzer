"""
Optimized Disk I/O ELT module for ETCD Analyzer
Extract, Load, Transform module for Disk I/O Performance Data
Enhanced to show detailed metrics for each pod/node without column limitations
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from datetime import datetime

from .etcd_analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)

class diskIOELT(utilityELT):
    """Enhanced Extract, Load, Transform class for disk I/O performance data"""
    
    def __init__(self):
        super().__init__()
        self.metric_units = {
            'bytes_per_second': 'MB/s',
            'operations_per_second': 'IOPS'
        }
        # Remove column limitation for detailed metrics
        self.max_columns = None  # Allow unlimited columns for detailed view
    
    def extract_disk_io(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract disk I/O data from tool results with enhanced detail extraction"""
        try:
            # Handle nested data structure
            if 'data' in data and 'metrics' in data['data']:
                metrics_data = data['data']['metrics']
                timestamp = data['data'].get('timestamp', datetime.now().isoformat())
                duration = data['data'].get('duration', '1h')
                status = data['data'].get('status', 'unknown')
            elif 'metrics' in data:
                metrics_data = data['metrics']
                timestamp = data.get('timestamp', datetime.now().isoformat())
                duration = data.get('duration', '1h')
                status = data.get('status', 'unknown')
            else:
                return {'error': 'Invalid disk I/O data structure'}
            
            extracted_data = {
                'timestamp': timestamp,
                'duration': duration,
                'status': status,
                'metrics_summary': [],
                'detailed_node_metrics': {},  # Enhanced detailed metrics
                'overall_stats': {},
                'metric_categories': {}  # Group metrics by category
            }
            
            # Process each metric with enhanced detail
            for metric_name, metric_data in metrics_data.items():
                if metric_data.get('status') != 'success':
                    continue
                
                # Extract metric summary
                metric_summary = {
                    'metric': metric_name,
                    'title': metric_data.get('title', metric_name),
                    'unit': metric_data.get('unit', ''),
                    'display_unit': self.metric_units.get(metric_data.get('unit', ''), metric_data.get('unit', '')),
                    'overall_stats': metric_data.get('overall_stats', {}),
                    'query': metric_data.get('query', 'N/A')  # Include Prometheus query
                }
                extracted_data['metrics_summary'].append(metric_summary)
                
                # Enhanced node data processing with full detail
                nodes_data = metric_data.get('nodes', {})
                metric_category = self._categorize_metric(metric_name)
                
                if metric_category not in extracted_data['metric_categories']:
                    extracted_data['metric_categories'][metric_category] = []
                
                for node_name, node_stats in nodes_data.items():
                    node_metric = {
                        'metric_name': metric_name,
                        'metric_title': metric_data.get('title', metric_name),
                        'node_name': node_name,
                        'short_node_name': self.truncate_node_name(node_name),
                        'unit': metric_data.get('unit', ''),
                        'display_unit': self.metric_units.get(metric_data.get('unit', ''), metric_data.get('unit', '')),
                        'avg_value': node_stats.get('avg', 0),
                        'max_value': node_stats.get('max', 0),
                        'devices': node_stats.get('devices', []),
                        'devices_str': ', '.join(node_stats.get('devices', [])) if node_stats.get('devices') else 'All',
                        'category': metric_category,
                        'query': metric_data.get('query', 'N/A')
                    }
                    
                    # Add to detailed metrics by node
                    if node_name not in extracted_data['detailed_node_metrics']:
                        extracted_data['detailed_node_metrics'][node_name] = []
                    extracted_data['detailed_node_metrics'][node_name].append(node_metric)
                    
                    # Add to category grouping
                    extracted_data['metric_categories'][metric_category].append(node_metric)
            
            return extracted_data
            
        except Exception as e:
            logger.error(f"Failed to extract disk I/O data: {e}")
            return {'error': str(e)}
    
    def _categorize_metric(self, metric_name: str) -> str:
        """Categorize metrics for better organization"""
        metric_lower = metric_name.lower()
        if 'container' in metric_lower:
            return 'Container I/O'
        elif 'throughput' in metric_lower:
            return 'Disk Throughput'
        elif 'iops' in metric_lower:
            return 'Disk IOPS'
        else:
            return 'Other I/O'
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into comprehensive pandas DataFrames"""
        try:
            dataframes = {}
            
            # 1. Comprehensive Metrics Overview (no column limit)
            if 'metrics_summary' in structured_data:
                metrics_overview = []
                for metric in structured_data['metrics_summary']:
                    overall_stats = metric.get('overall_stats', {})
                    
                    # Format values with proper units
                    avg_val = overall_stats.get('avg', 0)
                    max_val = overall_stats.get('max', 0)
                    min_val = overall_stats.get('min', 0)
                    count_val = overall_stats.get('count', 0)
                    latest_val = overall_stats.get('latest', 0)
                    
                    if metric['unit'] == 'bytes_per_second':
                        avg_display = self.format_bytes_per_second(avg_val)
                        max_display = self.format_bytes_per_second(max_val)
                        min_display = self.format_bytes_per_second(min_val)
                        latest_display = self.format_bytes_per_second(latest_val)
                    elif metric['unit'] == 'operations_per_second':
                        avg_display = self.format_operations_per_second(avg_val)
                        max_display = self.format_operations_per_second(max_val)
                        min_display = self.format_operations_per_second(min_val)
                        latest_display = self.format_operations_per_second(latest_val)
                    else:
                        avg_display = f"{avg_val:.2f}"
                        max_display = f"{max_val:.2f}"
                        min_display = f"{min_val:.2f}"
                        latest_display = f"{latest_val:.2f}"
                    
                    metrics_overview.append({
                        'Metric Name': metric['metric'],
                        'Title': metric['title'],
                        'Category': self._categorize_metric(metric['metric']),
                        'Unit': metric['display_unit'],
                        'Avg': avg_display,
                        'Max': max_display,
                        'Min': min_display,
                        'Latest': latest_display,
                        'Data Points': f"{count_val:,}",
                        'Query': metric.get('query', 'N/A')[:80] + ('...' if len(metric.get('query', '')) > 80 else '')
                    })
                
                if metrics_overview:
                    df = pd.DataFrame(metrics_overview)
                    dataframes['comprehensive_metrics_overview'] = df
            
            # 2. Detailed Node Performance (all metrics per node)
            if 'detailed_node_metrics' in structured_data:
                detailed_performance = []
                for node_name, metrics in structured_data['detailed_node_metrics'].items():
                    for metric in metrics:
                        avg_val = metric.get('avg_value', 0)
                        max_val = metric.get('max_value', 0)
                        
                        # Format values with proper units
                        if metric['unit'] == 'bytes_per_second':
                            avg_display = self.format_bytes_per_second(avg_val)
                            max_display = self.format_bytes_per_second(max_val)
                        elif metric['unit'] == 'operations_per_second':
                            avg_display = self.format_operations_per_second(avg_val)
                            max_display = self.format_operations_per_second(max_val)
                        else:
                            avg_display = f"{avg_val:.2f}"
                            max_display = f"{max_val:.2f}"
                        
                        detailed_performance.append({
                            'Node': metric['short_node_name'],
                            'Full Node Name': metric['node_name'],
                            'Metric': metric['metric_name'],
                            'Metric Title': metric['metric_title'],
                            'Category': metric['category'],
                            'Avg Value': avg_display,
                            'Max Value': max_display,
                            'Avg (Numeric)': avg_val,  # For sorting/highlighting
                            'Max (Numeric)': max_val,  # For sorting/highlighting
                            'Unit': metric['display_unit'],
                            'Devices': metric['devices_str'],
                            'Raw Avg': avg_val,
                            'Raw Max': max_val
                        })
                
                if detailed_performance:
                    df = pd.DataFrame(detailed_performance)
                    dataframes['detailed_node_performance'] = df
            
            # 3. Category-specific detailed tables
            if 'metric_categories' in structured_data:
                for category, metrics in structured_data['metric_categories'].items():
                    if not metrics:
                        continue
                    
                    category_data = []
                    for metric in metrics:
                        avg_val = metric.get('avg_value', 0)
                        max_val = metric.get('max_value', 0)
                        
                        # Format values
                        if metric['unit'] == 'bytes_per_second':
                            avg_display = self.format_bytes_per_second(avg_val)
                            max_display = self.format_bytes_per_second(max_val)
                        elif metric['unit'] == 'operations_per_second':
                            avg_display = self.format_operations_per_second(avg_val)
                            max_display = self.format_operations_per_second(max_val)
                        else:
                            avg_display = f"{avg_val:.2f}"
                            max_display = f"{max_val:.2f}"
                        
                        category_data.append({
                            'Node': metric['short_node_name'],
                            'Metric': metric['metric_name'],
                            'Title': metric['metric_title'],
                            'Avg': avg_display,
                            'Max': max_display,
                            'Unit': metric['display_unit'],
                            'Devices': metric['devices_str'],
                            'Avg_Numeric': avg_val,
                            'Max_Numeric': max_val
                        })
                    
                    if category_data:
                        df = pd.DataFrame(category_data)
                        safe_category = category.lower().replace(' ', '_').replace('/', '_')
                        dataframes[f'category_{safe_category}'] = df
            
            # 4. Per-metric detailed analysis
            if 'metrics_summary' in structured_data:
                for metric in structured_data['metrics_summary']:
                    metric_name = metric['metric']
                    
                    # Find all nodes for this specific metric
                    metric_nodes = []
                    for node_name, node_metrics in structured_data.get('detailed_node_metrics', {}).items():
                        for node_metric in node_metrics:
                            if node_metric['metric_name'] == metric_name:
                                avg_val = node_metric.get('avg_value', 0)
                                max_val = node_metric.get('max_value', 0)
                                
                                if node_metric['unit'] == 'bytes_per_second':
                                    avg_display = self.format_bytes_per_second(avg_val)
                                    max_display = self.format_bytes_per_second(max_val)
                                elif node_metric['unit'] == 'operations_per_second':
                                    avg_display = self.format_operations_per_second(avg_val)
                                    max_display = self.format_operations_per_second(max_val)
                                else:
                                    avg_display = f"{avg_val:.2f}"
                                    max_display = f"{max_val:.2f}"
                                
                                metric_nodes.append({
                                    'Node': node_metric['short_node_name'],
                                    'Full Node Name': node_metric['node_name'],
                                    'Avg': avg_display,
                                    'Max': max_display,
                                    'Unit': node_metric['display_unit'],
                                    'Devices': node_metric['devices_str'],
                                    'Category': node_metric['category'],
                                    'Avg_Numeric': avg_val,
                                    'Max_Numeric': max_val
                                })
                    
                    if metric_nodes:
                        df = pd.DataFrame(metric_nodes)
                        # Sort by average value descending
                        df = df.sort_values('Avg_Numeric', ascending=False)
                        safe_metric_name = metric_name.replace('_', '').replace('-', '')
                        dataframes[f'metric_{safe_metric_name}'] = df
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform disk I/O data to DataFrames: {e}")
            return {}
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate comprehensive HTML tables with enhanced highlighting"""
        try:
            html_tables = {}
            
            for table_name, df in dataframes.items():
                if df.empty:
                    continue
                
                # Create a copy for styling
                df_styled = df.copy()
                
                # Apply highlighting based on table type
                if 'comprehensive_metrics_overview' in table_name:
                    df_styled = self._highlight_comprehensive_overview(df_styled)
                elif 'detailed_node_performance' in table_name:
                    df_styled = self._highlight_detailed_performance(df_styled)
                elif table_name.startswith('category_'):
                    df_styled = self._highlight_category_metrics(df_styled)
                elif table_name.startswith('metric_'):
                    df_styled = self._highlight_per_metric_analysis(df_styled)
                
                # Remove numeric columns used for highlighting/sorting
                columns_to_remove = [col for col in df_styled.columns if col.endswith('_Numeric') or col in ['Raw Avg', 'Raw Max']]
                # Remove verbose columns not needed in HTML tables
                for col_name in ['Full Node Name', 'Title', 'Metric Title']:
                    if col_name in df_styled.columns:
                        columns_to_remove.append(col_name)
                if columns_to_remove:
                    df_styled = df_styled.drop(columns=columns_to_remove)
                
                html_tables[table_name] = self.create_html_table(df_styled, table_name)
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Failed to generate HTML tables: {e}")
            return {}
    
    def _highlight_comprehensive_overview(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply highlighting to comprehensive metrics overview"""
        try:
            for idx, row in df.iterrows():
                # Highlight high throughput metrics
                if 'throughput' in row['Metric Name'].lower() or 'writes' in row['Metric Name'].lower():
                    avg_str = row['Avg']
                    max_str = row['Max']
                    
                    # Extract numeric values for comparison
                    try:
                        avg_num = float(avg_str.split()[0])
                        max_num = float(max_str.split()[0])
                        
                        if 'MB/s' in avg_str and avg_num > 100:
                            df.at[idx, 'Avg'] = f'<span class="text-warning font-weight-bold">⚠️ {avg_str}</span>'
                        if 'MB/s' in max_str and max_num > 500:
                            df.at[idx, 'Max'] = f'<span class="text-danger font-weight-bold">🔥 {max_str}</span>'
                    except (ValueError, IndexError):
                        pass
                
                # Highlight IOPS metrics
                elif 'iops' in row['Metric Name'].lower():
                    try:
                        avg_str = row['Avg']
                        max_str = row['Max']
                        avg_num = float(avg_str.split()[0])
                        max_num = float(max_str.split()[0])
                        
                        if avg_num > 50:
                            df.at[idx, 'Avg'] = f'<span class="text-info font-weight-bold">📊 {avg_str}</span>'
                        if max_num > 100:
                            df.at[idx, 'Max'] = f'<span class="text-warning font-weight-bold">⚠️ {max_str}</span>'
                    except (ValueError, IndexError):
                        pass
            
            return df
        except Exception as e:
            logger.error(f"Failed to highlight comprehensive overview: {e}")
            return df
    
    def _highlight_detailed_performance(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply highlighting to detailed node performance"""
        try:
            # Group by metric and highlight top performers
            for metric_name in df['Metric'].unique():
                metric_rows = df[df['Metric'] == metric_name]
                
                if 'Avg (Numeric)' in df.columns:
                    # Find top performer for this metric
                    top_idx = metric_rows['Avg (Numeric)'].idxmax()
                    current_val = df.at[top_idx, 'Avg Value']
                    df.at[top_idx, 'Avg Value'] = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {current_val}</span>'
                    
                    # Highlight concerning values
                    for idx, row in metric_rows.iterrows():
                        avg_val = row['Avg (Numeric)']
                        
                        if 'throughput' in metric_name.lower() and avg_val > 500000000:  # > 500 MB/s
                            current_val = df.at[idx, 'Avg Value']
                            if '🏆' not in str(current_val):
                                df.at[idx, 'Avg Value'] = f'<span class="text-danger font-weight-bold">🔥 {current_val}</span>'
                        elif 'iops' in metric_name.lower() and avg_val > 80:  # > 80 IOPS
                            current_val = df.at[idx, 'Avg Value']
                            if '🏆' not in str(current_val):
                                df.at[idx, 'Avg Value'] = f'<span class="text-warning font-weight-bold">⚠️ {current_val}</span>'
            
            return df
        except Exception as e:
            logger.error(f"Failed to highlight detailed performance: {e}")
            return df
    
    def _highlight_category_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply highlighting to category-specific metrics"""
        try:
            if 'Avg_Numeric' in df.columns:
                # Find overall top performer in category
                top_idx = df['Avg_Numeric'].idxmax()
                current_val = df.at[top_idx, 'Avg']
                df.at[top_idx, 'Avg'] = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {current_val}</span>'
            
            return df
        except Exception as e:
            logger.error(f"Failed to highlight category metrics: {e}")
            return df
    
    def _highlight_per_metric_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply highlighting to per-metric analysis"""
        try:
            if not df.empty and 'Avg_Numeric' in df.columns:
                # Highlight top 3 performers
                top_indices = df.nlargest(3, 'Avg_Numeric').index
                
                for i, idx in enumerate(top_indices):
                    current_val = df.at[idx, 'Avg']
                    if i == 0:
                        df.at[idx, 'Avg'] = f'<span class="text-primary font-weight-bold bg-light px-2">🥇 {current_val}</span>'
                    elif i == 1:
                        df.at[idx, 'Avg'] = f'<span class="text-secondary font-weight-bold bg-light px-2">🥈 {current_val}</span>'
                    elif i == 2:
                        df.at[idx, 'Avg'] = f'<span class="text-info font-weight-bold bg-light px-2">🥉 {current_val}</span>'
            
            return df
        except Exception as e:
            logger.error(f"Failed to highlight per-metric analysis: {e}")
            return df
    
    def summarize_disk_io(self, structured_data: Dict[str, Any]) -> str:
        """Generate enhanced summary for disk I/O data"""
        try:
            summary_parts = []
            
            # Basic info
            timestamp = structured_data.get('timestamp', '')
            duration = structured_data.get('duration', '')
            status = structured_data.get('status', 'unknown')
            
            summary_parts.append(f"<strong>Comprehensive Disk I/O Analysis</strong> ({duration} duration)")
            summary_parts.append(f"Status: <span class='badge badge-{self._get_status_badge_color(status)}'>{status.upper()}</span>")
            
            # Enhanced metrics summary
            metrics_summary = structured_data.get('metrics_summary', [])
            if metrics_summary:
                summary_parts.append(f"<br><strong>Metrics Analyzed:</strong> {len(metrics_summary)} types")
                
                # Detailed analysis of concerning metrics
                high_throughput = []
                high_iops = []
                
                for metric in metrics_summary:
                    overall_stats = metric.get('overall_stats', {})
                    avg_val = overall_stats.get('avg', 0)
                    max_val = overall_stats.get('max', 0)
                    
                    if metric['unit'] == 'bytes_per_second':
                        if avg_val > 100 * 1024 * 1024:  # > 100 MB/s average
                            avg_display = self.format_bytes_per_second(avg_val)
                            high_throughput.append(f"{metric['title']} (avg: {avg_display})")
                        if max_val > 1024 * 1024 * 1024:  # > 1 GB/s peak
                            max_display = self.format_bytes_per_second(max_val)
                            high_throughput.append(f"{metric['title']} (peak: {max_display})")
                    
                    elif metric['unit'] == 'operations_per_second':
                        if avg_val > 50:  # > 50 IOPS average
                            avg_display = self.format_operations_per_second(avg_val)
                            high_iops.append(f"{metric['title']} (avg: {avg_display})")
                        if max_val > 100:  # > 100 IOPS peak
                            max_display = self.format_operations_per_second(max_val)
                            high_iops.append(f"{metric['title']} (peak: {max_display})")
                
                if high_throughput:
                    summary_parts.append(f"<br><span class='text-warning'>⚠️ High Throughput:</span> {', '.join(high_throughput[:2])}")
                    if len(high_throughput) > 2:
                        summary_parts.append(f" and {len(high_throughput) - 2} more")
                
                if high_iops:
                    summary_parts.append(f"<br><span class='text-info'>📊 Notable IOPS:</span> {', '.join(high_iops[:2])}")
                    if len(high_iops) > 2:
                        summary_parts.append(f" and {len(high_iops) - 2} more")
            
            # Node analysis
            detailed_metrics = structured_data.get('detailed_node_metrics', {})
            if detailed_metrics:
                summary_parts.append(f"<br><strong>Nodes Analyzed:</strong> {len(detailed_metrics)}")
                
                # Find most active node
                node_activity = {}
                for node_name, metrics in detailed_metrics.items():
                    total_activity = sum(metric.get('avg_value', 0) for metric in metrics)
                    node_activity[node_name] = total_activity
                
                if node_activity:
                    most_active = max(node_activity.items(), key=lambda x: x[1])
                    short_name = self.truncate_node_name(most_active[0])
                    summary_parts.append(f"<br><span class='text-primary'>🏆 Most Active Node:</span> {short_name}")
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate disk I/O summary: {e}")
            return f"Comprehensive Disk I/O Analysis - Summary generation failed: {str(e)}"
    
    def _get_status_badge_color(self, status: str) -> str:
        """Get badge color for status"""
        status_colors = {
            'success': 'success',
            'warning': 'warning',
            'error': 'danger',
            'failed': 'danger',
            'unknown': 'secondary'
        }
        return status_colors.get(status.lower(), 'secondary')