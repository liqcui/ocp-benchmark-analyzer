"""
Extract, Load, Transform module for ETCD Analyzer Disk WAL Fsync Performance Data
Optimized version with detailed per-pod metrics tables
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from datetime import datetime

from .etcd_analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)

class diskWalFsyncELT(utilityELT):
    """ELT processor for disk WAL fsync performance data""" 
    def __init__(self):
        super().__init__()
        self.metric_categories = {
            'latency': ['disk_wal_fsync_seconds_duration_p99'],
            'throughput': ['disk_wal_fsync_duration_seconds_sum_rate', 'disk_wal_fsync_duration_sum'],  # Changed metric name
            'operations': ['disk_wal_fsync_duration_seconds_count_rate', 'disk_wal_fsync_duration_seconds_count']  # Changed metric name
        }

    def extract_wal_fsync(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and structure disk WAL fsync data"""
        try:
            # Handle nested data structure
            if 'data' in raw_data and isinstance(raw_data['data'], dict):
                data = raw_data['data']
            else:
                data = raw_data
            
            if data.get('status') != 'success':
                return {'error': f"Data collection failed: {data.get('error', 'Unknown error')}"}
            
            metrics_data = data.get('metrics', {})
            if not metrics_data:
                return {'error': 'No metrics data found'}
            
            structured = {
                'collection_info': {
                    'timestamp': data.get('timestamp'),
                    'duration': data.get('duration', '1h'),
                    'category': data.get('category', 'disk_wal_fsync'),
                    'total_metrics': data.get('summary', {}).get('total_metrics', 0),
                    'successful_metrics': data.get('summary', {}).get('successful_metrics', 0)
                },
                'metrics': {},
                'pod_summary': {},
                'performance_analysis': {}
            }
            
            # Process each metric
            all_pods = set()
            for metric_name, metric_data in metrics_data.items():
                if metric_data.get('status') == 'success':
                    pod_metrics = metric_data.get('pod_metrics', {})
                    all_pods.update(pod_metrics.keys())
                    
                    structured['metrics'][metric_name] = {
                        'title': metric_data.get('title', metric_name),
                        'unit': metric_data.get('unit', ''),
                        'description': metric_data.get('description', ''),
                        'pod_metrics': pod_metrics,
                        'node_mapping': metric_data.get('node_mapping', {}),
                        'total_pods': metric_data.get('total_pods', 0),
                        'overall_stats': metric_data.get('overall_stats', {})
                    }
            
            # Create pod summary
            structured['pod_summary'] = self._create_pod_summary(structured['metrics'], all_pods)
            
            # Performance analysis
            structured['performance_analysis'] = self._analyze_wal_fsync_performance(structured['metrics'])
            
            return structured
            
        except Exception as e:
            logger.error(f"Failed to extract WAL fsync data: {e}")
            return {'error': str(e)}
    
    def _create_pod_summary(self, metrics: Dict[str, Any], all_pods: set) -> Dict[str, Any]:
        """Create summary of all pods across metrics"""
        pod_summary = {}
        
        for pod in all_pods:
            pod_data = {'pod_name': pod, 'metrics': {}}
            
            # Extract key metrics for each pod
            for metric_name, metric_info in metrics.items():
                pod_metrics = metric_info.get('pod_metrics', {})
                if pod in pod_metrics:
                    stats = pod_metrics[pod]
                    pod_data['metrics'][metric_name] = {
                        'avg': stats.get('avg_seconds') or stats.get('avg_rate_seconds') or 
                            stats.get('avg_sum_seconds') or stats.get('avg_ops_per_sec') or 
                            stats.get('avg_count'),
                        'max': stats.get('max_seconds') or stats.get('max_rate_seconds') or 
                            stats.get('max_sum_seconds') or stats.get('max_ops_per_sec') or 
                            stats.get('max_count'),
                        'min': stats.get('min_seconds') or stats.get('min_rate_seconds') or 
                            stats.get('min_sum_seconds') or stats.get('min_ops_per_sec') or 
                            stats.get('min_count'),
                        'latest': stats.get('latest_seconds') or stats.get('latest_rate_seconds') or 
                                stats.get('latest_sum_seconds') or stats.get('latest_ops_per_sec') or 
                                stats.get('latest_count'),
                        'data_points': stats.get('data_points', 0),
                        'unit': metric_info.get('unit', '')
                    }
            
            # Get node mapping
            for metric_info in metrics.values():
                node_mapping = metric_info.get('node_mapping', {})
                if pod in node_mapping:
                    pod_data['node'] = self.truncate_node_name(node_mapping[pod])
                    break
            
            pod_summary[pod] = pod_data
        
        return pod_summary

    def _analyze_wal_fsync_performance(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze WAL fsync performance for issues"""
        analysis = {
            'health_status': 'healthy',
            'issues': [],
            'recommendations': [],
            'critical_pods': [],
            'performance_summary': {}
        }
        
        # Analyze P99 latency
        p99_metric = metrics.get('disk_wal_fsync_seconds_duration_p99')
        if p99_metric:
            pod_metrics = p99_metric.get('pod_metrics', {})
            high_latency_pods = []
            
            for pod, stats in pod_metrics.items():
                max_latency = stats.get('max_seconds', 0)
                avg_latency = stats.get('avg_seconds', 0)
                
                if max_latency > 0.1:  # 100ms threshold
                    analysis['health_status'] = 'critical'
                    high_latency_pods.append({
                        'pod': self.truncate_text(pod, 40),
                        'max_latency_ms': round(max_latency * 1000, 2),
                        'avg_latency_ms': round(avg_latency * 1000, 2)
                    })
                elif max_latency > 0.05:  # 50ms threshold
                    if analysis['health_status'] == 'healthy':
                        analysis['health_status'] = 'warning'
            
            if high_latency_pods:
                analysis['critical_pods'].extend(high_latency_pods)
                analysis['issues'].append(f"High WAL fsync latency detected on {len(high_latency_pods)} pods")
        
        # Analyze operation rates - Updated metric name
        rate_metric = metrics.get('disk_wal_fsync_duration_seconds_count_rate')
        if rate_metric:
            pod_metrics = rate_metric.get('pod_metrics', {})
            total_ops = sum(stats.get('avg_rate_seconds', 0) for stats in pod_metrics.values())  # Updated key name
            analysis['performance_summary']['total_avg_ops_per_sec'] = round(total_ops, 2)
        
        return analysis

    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into DataFrames with detailed per-pod metrics"""
        try:
            if 'error' in structured_data:
                return {}
            
            dataframes = {}
            
            # 1. Metrics Overview (keep existing)
            overview_data = []
            metrics = structured_data.get('metrics', {})
            
            for metric_name, metric_info in metrics.items():
                overall_stats = metric_info.get('overall_stats', {})
                pod_metrics = metric_info.get('pod_metrics', {})
                
                overview_data.append({
                    'Metric': metric_info.get('title', metric_name),
                    'Unit': metric_info.get('unit', ''),
                    'Pods': metric_info.get('total_pods', 0),
                    'Avg Value': self.format_wal_fsync_value(
                        overall_stats.get('avg', 0), 
                        metric_info.get('unit', '')
                    ),
                    'Max Value': self.format_wal_fsync_value(
                        overall_stats.get('max', 0), 
                        metric_info.get('unit', '')
                    ),
                    'Data Points': overall_stats.get('count', 0)
                })
            
            if overview_data:
                dataframes['metrics_overview'] = pd.DataFrame(overview_data)
            
            # 2. Detailed Per-Pod Metrics for each metric
            for metric_name, metric_info in metrics.items():
                pod_metrics = metric_info.get('pod_metrics', {})
                if not pod_metrics:
                    continue
                
                metric_details = []
                metric_title = metric_info.get('title', metric_name)
                metric_unit = metric_info.get('unit', '')
                
                for pod_name, stats in pod_metrics.items():
                    # Extract appropriate statistics based on metric type
                    if 'duration_p99' in metric_name:
                        avg_val = stats.get('avg_seconds', 0)
                        max_val = stats.get('max_seconds', 0)
                        min_val = stats.get('min_seconds', 0)
                        latest_val = stats.get('latest_seconds', 0)
                    elif 'seconds_sum_rate' in metric_name:  # Updated condition
                        avg_val = stats.get('avg_rate_seconds', 0)
                        max_val = stats.get('max_rate_seconds', 0)
                        min_val = stats.get('min_rate_seconds', 0)
                        latest_val = stats.get('latest_rate_seconds', 0)
                    elif 'duration_sum' in metric_name and 'rate' not in metric_name:
                        avg_val = stats.get('avg_sum_seconds', 0)
                        max_val = stats.get('max_sum_seconds', 0)
                        min_val = stats.get('min_sum_seconds', 0)
                        latest_val = stats.get('latest_sum_seconds', 0)
                    elif 'seconds_count_rate' in metric_name:  # Updated condition
                        avg_val = stats.get('avg_rate_seconds', 0)  # Note: using rate_seconds for count_rate
                        max_val = stats.get('max_rate_seconds', 0)
                        min_val = stats.get('min_rate_seconds', 0)
                        latest_val = stats.get('latest_rate_seconds', 0)
                    elif 'seconds_count' in metric_name and 'rate' not in metric_name:  # Updated condition
                        avg_val = stats.get('avg_count', 0)
                        max_val = stats.get('max_count', 0)
                        min_val = stats.get('min_count', 0)
                        latest_val = stats.get('latest_count', 0)
                    else:
                        # Fallback
                        avg_val = list(stats.values())[0] if stats.values() else 0
                        max_val = max(stats.values()) if stats.values() else 0
                        min_val = min(stats.values()) if stats.values() else 0
                        latest_val = avg_val
                    
                    # Format values according to unit
                    if metric_unit == 'seconds' and 'p99' in metric_name:
                        # Convert to milliseconds for latency metrics
                        avg_display = f"{avg_val * 1000:.3f} ms" if avg_val else "0.000 ms"
                        max_display = f"{max_val * 1000:.3f} ms" if max_val else "0.000 ms"
                        min_display = f"{min_val * 1000:.3f} ms" if min_val else "0.000 ms"
                        latest_display = f"{latest_val * 1000:.3f} ms" if latest_val else "0.000 ms"
                    elif metric_unit == 'seconds' and 'rate' in metric_name:  # Updated condition for rate metrics
                        avg_display = f"{avg_val:.6f} s/s"
                        max_display = f"{max_val:.6f} s/s"
                        min_display = f"{min_val:.6f} s/s"
                        latest_display = f"{latest_val:.6f} s/s"
                    elif metric_unit == 'count' and 'rate' in metric_name:  # Updated condition for count rate metrics
                        avg_display = f"{avg_val:.2f} ops/s"
                        max_display = f"{max_val:.2f} ops/s"
                        min_display = f"{min_val:.2f} ops/s"
                        latest_display = f"{latest_val:.2f} ops/s"
                    elif metric_unit == 'count':
                        avg_display = f"{avg_val:,.0f}"
                        max_display = f"{max_val:,.0f}"
                        min_display = f"{min_val:,.0f}"
                        latest_display = f"{latest_val:,.0f}"
                    elif metric_unit == 'seconds':
                        avg_display = f"{avg_val:.6f} s"
                        max_display = f"{max_val:.6f} s"
                        min_display = f"{min_val:.6f} s"
                        latest_display = f"{latest_val:.6f} s"
                    else:
                        avg_display = f"{avg_val:.6f}"
                        max_display = f"{max_val:.6f}"
                        min_display = f"{min_val:.6f}"
                        latest_display = f"{latest_val:.6f}"
                    
                    metric_details.append({
                        'Metric Name': metric_title,
                        'Pod Name': self.truncate_text(pod_name, 45),
                        'Average': avg_display,
                        'Maximum': max_display,
                        'Minimum': min_display,
                        'Latest': latest_display,
                        'Unit': metric_unit,
                        'Data Points': stats.get('data_points', 0)
                    })
                
                if metric_details:
                    # Use a clean table name - Updated to handle new metric names
                    table_name = f"detailed_{metric_name.replace('disk_wal_', '').replace('_seconds', '').replace('_duration', '')}"
                    dataframes[table_name] = pd.DataFrame(metric_details)
            
            # 3. Performance Summary by Pod (enhanced version)
            pod_summary_data = []
            pod_summary = structured_data.get('pod_summary', {})
            
            for pod_name, pod_data in pod_summary.items():
                pod_metrics = pod_data.get('metrics', {})
                
                # Get P99 latency
                p99_data = pod_metrics.get('disk_wal_fsync_seconds_duration_p99', {})
                p99_avg = p99_data.get('avg', 0) * 1000  # Convert to ms
                p99_max = p99_data.get('max', 0) * 1000  # Convert to ms
                
                # Get operation rate - Updated metric name
                rate_data = pod_metrics.get('disk_wal_fsync_duration_seconds_count_rate', {})
                ops_rate = rate_data.get('avg', 0)
                
                # Get total operations - Updated metric name  
                count_data = pod_metrics.get('disk_wal_fsync_duration_seconds_count', {})
                total_ops = count_data.get('latest', 0)
                
                # Get cumulative duration - Updated metric name
                sum_data = pod_metrics.get('disk_wal_fsync_duration_sum', {})
                cum_duration = sum_data.get('latest', 0)
                
                pod_summary_data.append({
                    'Pod Name': self.truncate_text(pod_name, 45),
                    'Node': pod_data.get('node', 'unknown'),
                    'P99 Latency (avg)': f"{p99_avg:.3f} ms",
                    'P99 Latency (max)': f"{p99_max:.3f} ms",
                    'Ops Rate (avg)': f"{ops_rate:.2f} ops/s",
                    'Total Operations': f"{total_ops:,.0f}" if total_ops else "0",
                    'Cumulative Duration': f"{cum_duration:.3f} s" if cum_duration else "0.000 s"
                })
            
            if pod_summary_data:
                dataframes['pod_performance_summary'] = pd.DataFrame(pod_summary_data)
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform WAL fsync data to DataFrames: {e}")
            return {}

    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames with no column limitations"""
        try:
            html_tables = {}
            
            for table_name, df in dataframes.items():
                if df.empty:
                    continue
                
                # Don't limit columns for detailed metrics tables
                if table_name.startswith('detailed_') or table_name == 'pod_performance_summary':
                    df_to_use = df  # Use full dataframe
                else:
                    # Only apply column limiting to overview tables
                    df_to_use = self.limit_dataframe_columns(df, table_name=table_name, max_cols=8)
                
                # Generate HTML table with enhanced styling for readability
                html_table = self._create_enhanced_html_table(df_to_use, table_name)
                html_tables[table_name] = html_table
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for WAL fsync: {e}")
            return {}
    
    def _create_enhanced_html_table(self, df: pd.DataFrame, table_name: str) -> str:
        """Create enhanced HTML table with better formatting for readability"""
        try:
            # Apply styling based on table type
            if table_name.startswith('detailed_'):
                # Enhanced styling for detailed tables
                table_class = "table table-striped table-bordered table-sm"
                style_attrs = 'style="font-size: 0.9em;"'
            else:
                # Standard styling for other tables
                table_class = "table table-striped table-hover"
                style_attrs = ""
            
            html_parts = [f'<table class="{table_class}" {style_attrs}>']
            
            # Table header
            html_parts.append('<thead class="thead-light">')
            html_parts.append('<tr>')
            for col in df.columns:
                # Add title attributes for better UX
                col_title = col.replace('_', ' ').title()
                html_parts.append(f'<th scope="col" title="{col_title}">{col}</th>')
            html_parts.append('</tr>')
            html_parts.append('</thead>')
            
            # Table body
            html_parts.append('<tbody>')
            for idx, row in df.iterrows():
                html_parts.append('<tr>')
                for col_idx, (col, value) in enumerate(row.items()):
                    # Apply conditional formatting for specific columns
                    cell_class = ""
                    if 'latency' in col.lower() and 'ms' in str(value):
                        try:
                            latency_val = float(str(value).replace(' ms', ''))
                            if latency_val > 100:
                                cell_class = ' class="text-danger font-weight-bold"'
                            elif latency_val > 50:
                                cell_class = ' class="text-warning"'
                            elif latency_val < 10:
                                cell_class = ' class="text-success"'
                        except (ValueError, TypeError):
                            pass
                    
                    html_parts.append(f'<td{cell_class}>{value}</td>')
                html_parts.append('</tr>')
            html_parts.append('</tbody>')
            html_parts.append('</table>')
            
            return ''.join(html_parts)
            
        except Exception as e:
            logger.error(f"Failed to create enhanced HTML table: {e}")
            # Fallback to standard table creation
            return self.create_html_table(df, table_name)
    
    def summarize_wal_fsync(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary of WAL fsync performance"""
        try:
            if 'error' in structured_data:
                return f"WAL fsync analysis failed: {structured_data['error']}"
            
            collection_info = structured_data.get('collection_info', {})
            performance_analysis = structured_data.get('performance_analysis', {})
            metrics = structured_data.get('metrics', {})
            
            summary_parts = [
                f"WAL Fsync Performance Analysis",
                f"• Data collected over {collection_info.get('duration', 'unknown')} period",
                f"• {collection_info.get('successful_metrics', 0)} metrics successfully collected",
                f"• Health status: {performance_analysis.get('health_status', 'unknown').upper()}"
            ]
            
            # Add detailed metrics information
            if metrics:
                total_pods = len(set().union(*[m.get('pod_metrics', {}).keys() for m in metrics.values()]))
                summary_parts.append(f"• Total pods monitored: {total_pods}")
            
            # Add performance insights
            perf_summary = performance_analysis.get('performance_summary', {})
            if perf_summary.get('total_avg_ops_per_sec'):
                summary_parts.append(f"• Total average operations: {perf_summary['total_avg_ops_per_sec']}/sec")
            
            # Add latency insights
            p99_metric = metrics.get('disk_wal_fsync_seconds_duration_p99', {})
            if p99_metric:
                overall_stats = p99_metric.get('overall_stats', {})
                avg_latency_ms = overall_stats.get('avg', 0) * 1000
                max_latency_ms = overall_stats.get('max', 0) * 1000
                if avg_latency_ms > 0:
                    summary_parts.append(f"• Average P99 latency: {avg_latency_ms:.3f}ms")
                    summary_parts.append(f"• Maximum P99 latency: {max_latency_ms:.3f}ms")
            
            # Add issues if any
            issues = performance_analysis.get('issues', [])
            if issues:
                summary_parts.extend([f"• {issue}" for issue in issues])
            
            # Add critical pods info
            critical_pods = performance_analysis.get('critical_pods', [])
            if critical_pods:
                summary_parts.append(f"• {len(critical_pods)} pods with performance concerns")
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to summarize WAL fsync: {e}")
            return f"Summary generation failed: {str(e)}"
    
    def format_wal_fsync_value(self, value: float, unit: str) -> str:
        """Format WAL fsync values with appropriate units"""
        try:
            if unit == 'seconds':
                if value < 0.001:
                    return f"{value*1000000:.0f} μs"
                elif value < 1:
                    return f"{value*1000:.3f} ms"
                else:
                    return f"{value:.6f} s"
            elif unit == 'seconds/sec':
                return f"{value:.6f} s/s"
            elif unit == 'operations/sec':
                return self.format_operations_per_second(value)
            elif unit == 'count':
                if value > 1000000:
                    return f"{value/1000000:.1f}M"
                elif value > 1000:
                    return f"{value/1000:.1f}K"
                else:
                    return f"{value:.0f}"
            else:
                return f"{value:.3f}"
        except (ValueError, TypeError):
            return str(value)
    
    def highlight_wal_fsync_latency(self, latency_ms: float) -> str:
        """Highlight WAL fsync latency values based on thresholds"""
        try:
            if latency_ms > 100:  # 100ms critical
                return f'<span class="text-danger font-weight-bold">⚠️ {latency_ms:.2f} ms</span>'
            elif latency_ms > 50:  # 50ms warning
                return f'<span class="text-warning font-weight-bold">{latency_ms:.2f} ms</span>'
            elif latency_ms > 10:  # 10ms caution
                return f'<span class="text-info">{latency_ms:.2f} ms</span>'
            else:
                return f'<span class="text-success">{latency_ms:.2f} ms</span>'
        except (ValueError, TypeError):
            return str(latency_ms)