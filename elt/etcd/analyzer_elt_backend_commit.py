"""
Backend Commit ELT Module for ETCD Analyzer
Extract, Load, Transform module for backend commit duration metrics
"""

import logging
from typing import Dict, Any, List, Union
import pandas as pd

from .etcd_analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)

class backendCommitELT(utilityELT):
    """ELT module for backend commit metrics"""
    
    def __init__(self):
        super().__init__()
    
    def extract_backend_commit(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract backend commit metrics from tool result"""
        try:
            # Handle nested data structure and accept both 'metrics' and 'pods_metrics'
            metrics_data = None
            if isinstance(data.get('data'), dict):
                inner = data['data']
                if 'metrics' in inner and isinstance(inner['metrics'], dict):
                    metrics_data = inner
                elif 'pods_metrics' in inner and isinstance(inner['pods_metrics'], dict):
                    # Shape is equivalent to metrics
                    metrics_data = {
                        'metrics': inner['pods_metrics'],
                        'summary': inner.get('summary', {}),
                        'timestamp': inner.get('timestamp'),
                        'duration': inner.get('duration')
                    }
            elif 'metrics' in data and isinstance(data['metrics'], dict):
                metrics_data = data
            elif 'pods_metrics' in data and isinstance(data['pods_metrics'], dict):
                metrics_data = {
                    'metrics': data['pods_metrics'],
                    'summary': data.get('summary', {}),
                    'timestamp': data.get('timestamp'),
                    'duration': data.get('duration')
                }
            if metrics_data is None:
                return {'error': 'No metrics data found in backend commit result'}
            
            structured = {
                'category': 'disk_backend_commit',
                'timestamp': data.get('timestamp', metrics_data.get('timestamp')),
                'duration': data.get('duration', metrics_data.get('duration', '1h')),
                'summary': metrics_data.get('summary', {}),
                'metrics': {},
                'pod_metrics': {},
                'overall_metrics': {}
            }
            
            # Process each metric
            def normalize_name(name: str) -> str:
                # Keep p99 key as-is
                if name.startswith('disk_backend_commit_duration_seconds_p99'):
                    return name
                # Convert 'disk_backend_commit_*' to 'backend_commit_*'
                if name.startswith('disk_backend_commit_'):
                    return 'backend_commit_' + name[len('disk_backend_commit_'):]
                return name

            for metric_name, metric_data in metrics_data.get('metrics', {}).items():
                if metric_data.get('status') != 'success':
                    continue
                norm_name = normalize_name(metric_name)

                structured['metrics'][norm_name] = {
                    'unit': metric_data.get('unit', ''),
                    'description': metric_data.get('description', ''),
                    'overall': metric_data.get('overall', {}),
                    'pods': metric_data.get('pods', {}),
                    'total_data_points': metric_data.get('total_data_points', 0)
                }
            
            # Create pod-centric view
            all_pods = set()
            for metric_name, metric_data in structured['metrics'].items():
                all_pods.update(metric_data['pods'].keys())
            
            for pod_name in all_pods:
                structured['pod_metrics'][pod_name] = {}
                for metric_name, metric_data in structured['metrics'].items():
                    pod_data = metric_data['pods'].get(pod_name, {})
                    if pod_data:
                        structured['pod_metrics'][pod_name][metric_name] = pod_data
            
            # Create overall metrics summary
            for metric_name, metric_data in structured['metrics'].items():
                overall = metric_data['overall']
                structured['overall_metrics'][metric_name] = {
                    'avg': overall.get('avg'),
                    'max': overall.get('max'),
                    'min': overall.get('min'),
                    'latest': overall.get('latest'),
                    'unit': metric_data.get('unit', '')
                }
            
            return structured
            
        except Exception as e:
            logger.error(f"Error extracting backend commit data: {e}")
            return {'error': str(e)}
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured backend commit data to DataFrames"""
        try:
            dataframes = {}
            
            # 1. Metrics Overview Table
            overview_data = []
            for metric_name, metric_data in structured_data.get('overall_metrics', {}).items():
                metric_display = self._format_metric_name(metric_name)
                unit = metric_data.get('unit', '')
                
                overview_data.append({
                    'Metric': metric_display,
                    'Average': self._format_backend_commit_value(metric_data.get('avg'), unit),
                    'Maximum': self._format_backend_commit_value(metric_data.get('max'), unit),
                    'Minimum': self._format_backend_commit_value(metric_data.get('min'), unit),
                    'Latest': self._format_backend_commit_value(metric_data.get('latest'), unit),
                    'Unit': unit
                })
            
            if overview_data:
                df_overview = pd.DataFrame(overview_data)
                dataframes['metrics_overview'] = self.limit_dataframe_columns(df_overview, table_name='metrics_overview')
            
            # 2. Pod Performance Table
            pod_data = []
            for pod_name, pod_metrics in structured_data.get('pod_metrics', {}).items():
                pod_display = self.truncate_node_name(pod_name, 30)
                
                # Get key metrics for this pod
                p99_data = pod_metrics.get('disk_backend_commit_duration_seconds_p99', {})
                rate_data = pod_metrics.get('backend_commit_duration_count_rate', {})
                
                pod_data.append({
                    'Pod': pod_display,
                    'P99 Latency (avg)': self._format_backend_commit_value(p99_data.get('avg'), 'seconds'),
                    'P99 Latency (max)': self._format_backend_commit_value(p99_data.get('max'), 'seconds'),
                    'Commit Rate (avg)': self._format_backend_commit_value(rate_data.get('avg'), 'ops/sec'),
                    'Commit Rate (max)': self._format_backend_commit_value(rate_data.get('max'), 'ops/sec'),
                    'Data Points': p99_data.get('count', 0)
                })
            
            if pod_data:
                df_pods = pd.DataFrame(pod_data)
                # Identify top performers
                top_latency_indices = self.identify_top_values(pod_data, 'P99 Latency (max)')
                top_rate_indices = self.identify_top_values(pod_data, 'Commit Rate (max)')
                
                # Highlight values
                for i, row in df_pods.iterrows():
                    is_top_latency = i in top_latency_indices
                    is_top_rate = i in top_rate_indices
                    
                    if is_top_latency:
                        df_pods.at[i, 'P99 Latency (max)'] = self.highlight_backend_commit_values(
                            self.extract_numeric_value(row['P99 Latency (max)']), 'latency', 'ms', True)
                    if is_top_rate:
                        df_pods.at[i, 'Commit Rate (max)'] = self.highlight_backend_commit_values(
                            self.extract_numeric_value(row['Commit Rate (max)']), 'rate', 'ops/sec', True)
                
                dataframes['pod_performance'] = self.limit_dataframe_columns(df_pods, table_name='pod_performance')
            
            # 3. Latency Details Table
            latency_data = []
            p99_metric = structured_data.get('metrics', {}).get('disk_backend_commit_duration_seconds_p99', {})
            
            for pod_name, pod_data in p99_metric.get('pods', {}).items():
                pod_display = self.truncate_node_name(pod_name, 30)
                
                latency_data.append({
                    'Pod': pod_display,
                    'Average (ms)': self._format_latency_ms(pod_data.get('avg')),
                    'Maximum (ms)': self._format_latency_ms(pod_data.get('max')),
                    'Minimum (ms)': self._format_latency_ms(pod_data.get('min')),
                    'Latest (ms)': self._format_latency_ms(pod_data.get('latest')),
                    'Sample Count': pod_data.get('count', 0)
                })
            
            if latency_data:
                df_latency = pd.DataFrame(latency_data)
                # Highlight critical latencies (>50ms is critical, >20ms is warning)
                for i, row in df_latency.iterrows():
                    max_val = self.extract_numeric_value(row['Maximum (ms)'])
                    avg_val = self.extract_numeric_value(row['Average (ms)'])
                    
                    df_latency.at[i, 'Maximum (ms)'] = self.highlight_backend_commit_values(
                        max_val, 'latency', 'ms', max_val == df_latency['Maximum (ms)'].apply(self.extract_numeric_value).max())
                    df_latency.at[i, 'Average (ms)'] = self.highlight_backend_commit_values(
                        avg_val, 'latency', 'ms')
                
                dataframes['latency_details'] = self.limit_dataframe_columns(df_latency, table_name='latency_details')
            
            # 4. Operations Rate Table
            ops_data = []
            count_rate_metric = structured_data.get('metrics', {}).get('backend_commit_duration_count_rate', {})
            
            for pod_name, pod_data in count_rate_metric.get('pods', {}).items():
                pod_display = self.truncate_node_name(pod_name, 30)
                
                ops_data.append({
                    'Pod': pod_display,
                    'Avg Rate': self.format_operations_per_second(pod_data.get('avg', 0)),
                    'Max Rate': self.format_operations_per_second(pod_data.get('max', 0)),
                    'Min Rate': self.format_operations_per_second(pod_data.get('min', 0)),
                    'Latest Rate': self.format_operations_per_second(pod_data.get('latest', 0)),
                    'Sample Count': pod_data.get('count', 0)
                })
            
            if ops_data:
                df_ops = pd.DataFrame(ops_data)
                # Highlight top performers
                top_ops_indices = self.identify_top_values(ops_data, 'Max Rate')
                
                for i, row in df_ops.iterrows():
                    is_top = i in top_ops_indices
                    max_val = self.extract_numeric_value(row['Max Rate'])
                    
                    df_ops.at[i, 'Max Rate'] = self.highlight_backend_commit_values(
                        max_val, 'rate', 'ops/sec', is_top)
                
                dataframes['operations_rate'] = self.limit_dataframe_columns(df_ops, table_name='operations_rate')
            
            # 5. Cumulative Metrics Table
            cumulative_data = []
            sum_metric = structured_data.get('metrics', {}).get('backend_commit_duration_sum', {})
            count_metric = structured_data.get('metrics', {}).get('backend_commit_duration_count', {})
            
            for pod_name in structured_data.get('pod_metrics', {}).keys():
                pod_display = self.truncate_node_name(pod_name, 30)
                sum_data = sum_metric.get('pods', {}).get(pod_name, {})
                count_data = count_metric.get('pods', {}).get(pod_name, {})
                
                cumulative_data.append({
                    'Pod': pod_display,
                    'Total Duration (s)': f"{sum_data.get('latest', 0):.2f}",
                    'Total Operations': f"{count_data.get('latest', 0):,}",
                    'Avg Duration (s)': f"{sum_data.get('avg', 0):.2f}",
                    'Avg Operations': f"{count_data.get('avg', 0):,.0f}"
                })
            
            if cumulative_data:
                df_cumulative = pd.DataFrame(cumulative_data)
                dataframes['cumulative_metrics'] = self.limit_dataframe_columns(df_cumulative, table_name='cumulative_metrics')
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Error transforming backend commit data to DataFrames: {e}")
            return {}
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables for backend commit metrics"""
        try:
            html_tables = {}
            
            for table_name, df in dataframes.items():
                if df.empty:
                    continue
                
                html_tables[table_name] = self.create_html_table(df, table_name)
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Error generating HTML tables for backend commit: {e}")
            return {}
    
    def summarize_backend_commit(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary for backend commit metrics"""
        try:
            summary_parts = ["Backend Commit Performance Summary:"]
            
            # Basic info
            total_pods = len(structured_data.get('pod_metrics', {}))
            summary_parts.append(f"• Monitoring {total_pods} etcd pods")
            
            # Key performance indicators
            overall_metrics = structured_data.get('overall_metrics', {})
            
            # P99 latency summary
            p99_data = overall_metrics.get('disk_backend_commit_duration_seconds_p99', {})
            if p99_data:
                avg_latency_ms = (p99_data.get('avg', 0) * 1000)
                max_latency_ms = (p99_data.get('max', 0) * 1000)
                summary_parts.append(f"• Average P99 latency: {avg_latency_ms:.2f}ms (max: {max_latency_ms:.2f}ms)")
            
            # Commit rate summary
            rate_data = overall_metrics.get('backend_commit_duration_count_rate', {})
            if rate_data:
                avg_rate = rate_data.get('avg', 0)
                max_rate = rate_data.get('max', 0)
                summary_parts.append(f"• Average commit rate: {avg_rate:.1f} ops/sec (peak: {max_rate:.1f} ops/sec)")
            
            # Total operations
            count_data = overall_metrics.get('backend_commit_duration_count', {})
            if count_data:
                total_ops = count_data.get('latest', 0)
                summary_parts.append(f"• Total backend commits: {total_ops:,}")
            
            # Performance assessment
            if p99_data:
                avg_latency_ms = (p99_data.get('avg', 0) * 1000)
                if avg_latency_ms > 50:
                    summary_parts.append("⚠️ High backend commit latency detected")
                elif avg_latency_ms > 20:
                    summary_parts.append("⚡ Moderate backend commit latency")
                else:
                    summary_parts.append("✅ Good backend commit performance")
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Error generating backend commit summary: {e}")
            return f"Summary generation failed: {str(e)}"
    
    def _format_metric_name(self, metric_name: str) -> str:
        """Format metric name for display"""
        name_mapping = {
            'disk_backend_commit_duration_seconds_p99': 'P99 Commit Duration',
            'backend_commit_duration_sum_rate': 'Duration Sum Rate',
            'backend_commit_duration_sum': 'Total Duration Sum',
            'backend_commit_duration_count_rate': 'Commit Rate',
            'backend_commit_duration_count': 'Total Commits'
        }
        return name_mapping.get(metric_name, metric_name.replace('_', ' ').title())
    
    def _format_backend_commit_value(self, value: Union[float, int, None], unit: str) -> str:
        """Format backend commit metric value with appropriate units"""
        if value is None:
            return "N/A"
        
        try:
            if unit == 'seconds':
                if value < 0.001:
                    return f"{value*1000000:.0f} μs"
                elif value < 1:
                    return f"{value*1000:.3f} ms"
                else:
                    return f"{value:.6f} s"
            elif unit == 'count' or unit == 'ops/sec':
                if isinstance(value, float) and value < 10:
                    return f"{value:.2f}"
                else:
                    return f"{value:,.0f}"
            else:
                return f"{value:.3f}"
        except (ValueError, TypeError):
            return str(value)
    
    def _format_latency_ms(self, value: Union[float, int, None]) -> str:
        """Format latency value in milliseconds"""
        if value is None:
            return "N/A"
        try:
            return f"{value * 1000:.3f}"
        except (ValueError, TypeError):
            return str(value)
    
    def highlight_backend_commit_values(self, value: Union[float, int], metric_type: str, unit: str = "", is_top: bool = False) -> str:
        """Highlight backend commit values with metric-specific thresholds"""
        try:
            thresholds = self._get_backend_commit_thresholds(metric_type)
            
            if is_top:
                formatted_value = self._format_backend_commit_display_value(float(value), metric_type, unit)
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted_value}</span>'
            
            if thresholds and isinstance(value, (int, float)):
                critical = thresholds.get('critical', float('inf'))
                warning = thresholds.get('warning', float('inf'))
                
                formatted_value = self._format_backend_commit_display_value(float(value), metric_type, unit)
                
                if value >= critical:
                    return f'<span class="text-danger font-weight-bold">⚠️ {formatted_value}</span>'
                elif value >= warning:
                    return f'<span class="text-warning font-weight-bold">{formatted_value}</span>'
                else:
                    return f'<span class="text-success">{formatted_value}</span>'
            else:
                return self._format_backend_commit_display_value(float(value), metric_type, unit)
                
        except (ValueError, TypeError):
            return str(value)
    
    def _get_backend_commit_thresholds(self, metric_type: str) -> Dict[str, float]:
        """Get thresholds for backend commit metrics"""
        thresholds_map = {
            'latency': {'warning': 20, 'critical': 50},  # milliseconds
            'rate': {'warning': 1000, 'critical': 2000}  # ops/sec - high might indicate issues
        }
        
        return thresholds_map.get(metric_type, {})
    
    def _format_backend_commit_display_value(self, value: float, metric_type: str, unit: str) -> str:
        """Format backend commit value for display"""
        if metric_type == 'latency' and unit == 'ms':
            return f"{value:.3f} ms"
        elif metric_type == 'rate' and unit == 'ops/sec':
            return self.format_operations_per_second(value)
        elif metric_type == 'count':
            return self.format_count_value(value)
        else:
            return f"{value:.3f}"
    
    def categorize_backend_commit_metric(self, metric_name: str) -> str:
        """Categorize backend commit metric type"""
        metric_lower = metric_name.lower()
        
        if 'p99' in metric_lower or 'duration' in metric_lower and 'rate' not in metric_lower:
            return 'Latency'
        elif 'sum_rate' in metric_lower or 'rate' in metric_lower:
            return 'Rate/Throughput'  
        elif 'count' in metric_lower and 'rate' not in metric_lower:
            return 'Cumulative'
        elif 'sum' in metric_lower and 'rate' not in metric_lower:
            return 'Cumulative Duration'
        else:
            return 'Other'