"""
Deep Drive ELT module for ETCD Analyzer Performance Deep Drive Analysis
Extract, Load, Transform module for deep drive performance data
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from .etcd_analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)

class deepDriveELT(utilityELT):
    """Extract, Load, Transform class for deep drive performance analysis data"""
    
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)
    
    def extract_deep_drive(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract deep drive performance data from JSON result"""
        try:
            structured_data = {
                'test_metadata': {},
                'general_info_metrics': [],
                'wal_fsync_metrics': [],
                'disk_io_metrics': [],
                'network_metrics': [],
                'backend_commit_metrics': [],
                'compact_defrag_metrics': [],
                'node_usage_metrics': [], 
                'analysis_results': {},
                'summary_info': {}
            }
            
            # Extract test metadata
            structured_data['test_metadata'] = {
                'test_id': data.get('test_id', 'unknown'),
                'timestamp': data.get('timestamp', 'unknown'),
                'duration': data.get('duration', 'unknown'),
                'category': data.get('category', 'performance_deep_drive'),
                'status': data.get('status', 'unknown')
            }
            
            # Extract data sections
            data_section = data.get('data', {})
            
            # Extract general info metrics
            general_info_data = data_section.get('general_info_data', [])
            for metric in general_info_data:
                structured_data['general_info_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': metric.get('pod_name', 'unknown'),
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown')
                })
            
            # Extract WAL fsync metrics
            wal_fsync_data = data_section.get('wal_fsync_data', [])
            for metric in wal_fsync_data:
                structured_data['wal_fsync_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': metric.get('pod_name', 'unknown'),
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown')
                })
            
            # Extract disk I/O metrics
            disk_io_data = data_section.get('disk_io_data', [])
            for metric in disk_io_data:
                structured_data['disk_io_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'node_name': metric.get('node_name', 'unknown'),
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown'),
                    'devices': metric.get('devices', [])
                })
            
            # Extract network metrics
            network_data = data_section.get('network_data', {})
            
            # Pod metrics
            pod_metrics = network_data.get('pod_metrics', [])
            for metric in pod_metrics:
                structured_data['network_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': metric.get('pod_name', 'unknown'),
                    'node_name': None,
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown'),
                    'metric_type': 'pod'
                })
            
            # Node metrics
            node_metrics = network_data.get('node_metrics', [])
            for metric in node_metrics:
                structured_data['network_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': None,
                    'node_name': metric.get('node_name', 'unknown'),
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown'),
                    'metric_type': 'node'
                })
            
            # Cluster metrics
            cluster_metrics = network_data.get('cluster_metrics', [])
            for metric in cluster_metrics:
                structured_data['network_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': None,
                    'node_name': None,
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown'),
                    'metric_type': 'cluster'
                })
            
            # Extract backend commit metrics
            backend_commit_data = data_section.get('backend_commit_data', [])
            for metric in backend_commit_data:
                structured_data['backend_commit_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': metric.get('pod_name', 'unknown'),
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown')
                })
            
            # Extract compact defrag metrics
            compact_defrag_data = data_section.get('compact_defrag_data', [])
            for metric in compact_defrag_data:
                structured_data['compact_defrag_metrics'].append({
                    'metric_name': metric.get('metric_name', 'unknown'),
                    'pod_name': metric.get('pod_name', 'unknown'),
                    'avg_value': metric.get('avg', 0),
                    'max_value': metric.get('max', 0),
                    'unit': metric.get('unit', 'unknown')
                })

            node_usage_data = data_section.get('node_usage_data', {})
            if node_usage_data.get('status') == 'success':
                metrics = node_usage_data.get('metrics', {})
                node_capacities = node_usage_data.get('node_capacities', {})
                
                # Extract CPU usage metrics
                cpu_usage = metrics.get('cpu_usage', {})
                if cpu_usage.get('status') == 'success':
                    nodes = cpu_usage.get('nodes', {})
                    for node_name, node_data in nodes.items():
                        total = node_data.get('total', {})
                        modes = node_data.get('modes', {})
                        
                        structured_data['node_usage_metrics'].append({
                            'metric_name': 'node_cpu_usage_total',
                            'node_name': node_name,
                            'avg_value': total.get('avg', 0),
                            'max_value': total.get('max', 0),
                            'unit': 'percent',
                            'metric_type': 'cpu',
                            'details': modes
                        })
                
                # Extract memory used metrics
                memory_used = metrics.get('memory_used', {})
                if memory_used.get('status') == 'success':
                    nodes = memory_used.get('nodes', {})
                    for node_name, node_data in nodes.items():
                        structured_data['node_usage_metrics'].append({
                            'metric_name': 'node_memory_used',
                            'node_name': node_name,
                            'avg_value': node_data.get('avg', 0),
                            'max_value': node_data.get('max', 0),
                            'unit': node_data.get('unit', 'GB'),
                            'metric_type': 'memory',
                            'total_capacity': node_data.get('total_capacity', 0)
                        })
                
                # Extract memory cache/buffer metrics
                memory_cache = metrics.get('memory_cache_buffer', {})
                if memory_cache.get('status') == 'success':
                    nodes = memory_cache.get('nodes', {})
                    for node_name, node_data in nodes.items():
                        structured_data['node_usage_metrics'].append({
                            'metric_name': 'node_memory_cache_buffer',
                            'node_name': node_name,
                            'avg_value': node_data.get('avg', 0),
                            'max_value': node_data.get('max', 0),
                            'unit': node_data.get('unit', 'GB'),
                            'metric_type': 'memory',
                            'total_capacity': node_data.get('total_capacity', 0)
                        })
                
                # Extract cgroup CPU usage metrics
                cgroup_cpu = metrics.get('cgroup_cpu_usage', {})
                if cgroup_cpu.get('status') == 'success':
                    nodes = cgroup_cpu.get('nodes', {})
                    for node_name, node_data in nodes.items():
                        total = node_data.get('total', {})
                        cgroups = node_data.get('cgroups', {})
                        
                        structured_data['node_usage_metrics'].append({
                            'metric_name': 'cgroup_cpu_usage_total',
                            'node_name': node_name,
                            'avg_value': total.get('avg', 0),
                            'max_value': total.get('max', 0),
                            'unit': 'percent',
                            'metric_type': 'cgroup_cpu',
                            'details': cgroups
                        })
                
                # Extract cgroup RSS usage metrics
                cgroup_rss = metrics.get('cgroup_rss_usage', {})
                if cgroup_rss.get('status') == 'success':
                    nodes = cgroup_rss.get('nodes', {})
                    for node_name, node_data in nodes.items():
                        total = node_data.get('total', {})
                        cgroups = node_data.get('cgroups', {})
                        
                        structured_data['node_usage_metrics'].append({
                            'metric_name': 'cgroup_rss_usage_total',
                            'node_name': node_name,
                            'avg_value': total.get('avg', 0),
                            'max_value': total.get('max', 0),
                            'unit': 'GB',
                            'metric_type': 'cgroup_memory',
                            'details': cgroups
                        })

            # Extract analysis results
            analysis_section = data.get('analysis', {})
            structured_data['analysis_results'] = {
                'potential_bottlenecks': analysis_section.get('potential_bottlenecks', []),
                'latency_analysis': analysis_section.get('latency_analysis', {}),
                'recommendations': analysis_section.get('recommendations', [])
            }
            
            # Extract summary information
            summary_section = data.get('summary', {})
            structured_data['summary_info'] = {
                'total_metrics_collected': summary_section.get('total_metrics_collected', 0),
                'categories': summary_section.get('categories', {}),
                'overall_health': summary_section.get('overall_health', 'unknown'),
                'timestamp': summary_section.get('timestamp', 'unknown')
            }
            
            return structured_data
            
        except Exception as e:
            self.logger.error(f"Error extracting deep drive data: {e}")
            return {'error': str(e)}
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        try:
            dataframes = {}
            
            # Transform test metadata
            if structured_data.get('test_metadata'):
                metadata_df = pd.DataFrame([structured_data['test_metadata']])
                dataframes['test_overview'] = self.limit_dataframe_columns(metadata_df, table_name='test_overview')
            
            # Transform general info metrics
            if structured_data.get('general_info_metrics'):
                general_df = pd.DataFrame(structured_data['general_info_metrics'])
                general_df = self._format_deep_drive_metrics_dataframe(general_df, 'general_info')
                dataframes['general_info'] = self.limit_dataframe_columns(general_df, table_name='general_info')
            
            # Transform WAL fsync metrics
            if structured_data.get('wal_fsync_metrics'):
                wal_df = pd.DataFrame(structured_data['wal_fsync_metrics'])
                wal_df = self._format_deep_drive_metrics_dataframe(wal_df, 'wal_fsync')
                dataframes['wal_fsync'] = self.limit_dataframe_columns(wal_df, table_name='wal_fsync')
            
            # Transform disk I/O metrics
            if structured_data.get('disk_io_metrics'):
                disk_df = pd.DataFrame(structured_data['disk_io_metrics'])
                disk_df = self._format_deep_drive_metrics_dataframe(disk_df, 'disk_io')
                dataframes['disk_io'] = self.limit_dataframe_columns(disk_df, table_name='disk_io')
            
            # Transform network metrics
            if structured_data.get('network_metrics'):
                network_df = pd.DataFrame(structured_data['network_metrics'])
                network_df = self._format_deep_drive_metrics_dataframe(network_df, 'network_io')
                dataframes['network_io'] = self.limit_dataframe_columns(network_df, table_name='network_io')
            
            # Transform backend commit metrics
            if structured_data.get('backend_commit_metrics'):
                backend_df = pd.DataFrame(structured_data['backend_commit_metrics'])
                backend_df = self._format_deep_drive_metrics_dataframe(backend_df, 'backend_commit')
                dataframes['backend_commit'] = self.limit_dataframe_columns(backend_df, table_name='backend_commit')
            
            # Transform compact defrag metrics
            if structured_data.get('compact_defrag_metrics'):
                compact_df = pd.DataFrame(structured_data['compact_defrag_metrics'])
                compact_df = self._format_deep_drive_metrics_dataframe(compact_df, 'compact_defrag')
                dataframes['compact_defrag'] = self.limit_dataframe_columns(compact_df, table_name='compact_defrag')

            # Removed generation of generic Node Resource Usage table

            # Build specialized node memory tables (match screenshot layout)
            memory_tables = self._build_node_memory_tables_dataframes(structured_data)
            for key, df in memory_tables.items():
                dataframes[key] = self.limit_dataframe_columns(df, table_name=key)

            # Transform analysis summary
            if structured_data.get('analysis_results') or structured_data.get('summary_info'):
                analysis_data = []
                
                # Add latency analysis
                latency_analysis = structured_data.get('analysis_results', {}).get('latency_analysis', {})
                for metric_type, analysis in latency_analysis.items():
                    if isinstance(analysis, dict):
                        analysis_data.append({
                            'Analysis Type': 'Latency Analysis',
                            'Metric': metric_type.replace('_', ' ').title(),
                            'Average (ms)': analysis.get('avg_ms', 'N/A'),
                            'Status': analysis.get('status', 'Unknown')
                        })

            node_resource_util = structured_data.get('analysis_results', {}).get('node_resource_utilization', {})
            if node_resource_util:
                cpu_util = node_resource_util.get('cpu_utilization', {})
                for node_name, util_data in cpu_util.items():
                    analysis_data.append({
                        'Analysis Type': 'Node CPU Utilization',
                        'Metric': node_name,
                        'Average (%)': util_data.get('avg_percent', 'N/A'),
                        'Status': util_data.get('status', 'Unknown')
                    })
                
                memory_util = node_resource_util.get('memory_utilization', {})
                for node_name, util_data in memory_util.items():
                    analysis_data.append({
                        'Analysis Type': 'Node Memory Utilization',
                        'Metric': node_name,
                        'Average (%)': util_data.get('avg_percent', 'N/A'),
                        'Status': util_data.get('status', 'Unknown')
                    })

                # Add summary info
                summary_info = structured_data.get('summary_info', {})
                if summary_info:
                    analysis_data.append({
                        'Analysis Type': 'Summary',
                        'Metric': 'Total Metrics',
                        'Average (ms)': summary_info.get('total_metrics_collected', 0),
                        'Status': summary_info.get('overall_health', 'Unknown')
                    })
                
                if analysis_data:
                    analysis_df = pd.DataFrame(analysis_data)
                    dataframes['analysis_summary'] = self.limit_dataframe_columns(analysis_df, table_name='analysis_summary')
            
            return dataframes
            
        except Exception as e:
            self.logger.error(f"Error transforming deep drive data to DataFrames: {e}")
            return {}
    
    def _format_deep_drive_metrics_dataframe(self, df: pd.DataFrame, metric_type: str) -> pd.DataFrame:
        """Format metrics DataFrame with readable units and highlighting"""
        try:
            if df.empty:
                return df
            
            df_copy = df.copy()
            
            # Identify top performers for highlighting
            if 'avg_value' in df_copy.columns:
                top_indices = self.identify_top_values(df_copy.to_dict('records'), 'avg_value')
            else:
                top_indices = []
            
            # Format values based on metric type and unit
            for idx, row in df_copy.iterrows():
                unit = str(row.get('unit', ''))
                avg_val = row.get('avg_value', 0)
                max_val = row.get('max_value', 0)
                
                is_top = idx in top_indices
                
                # Format based on metric type
                if metric_type == 'general_info':
                    metric_name = str(row.get('metric_name', ''))
                    df_copy.at[idx, 'Avg Usage'] = self.highlight_general_info_values(avg_val, metric_name, unit, is_top)
                    df_copy.at[idx, 'Max Usage'] = self.highlight_general_info_values(max_val, metric_name, unit, False)
                
                elif metric_type == 'wal_fsync':
                    df_copy.at[idx, 'Avg Usage'] = self.highlight_wal_fsync_values(avg_val, 'latency', unit, is_top)
                    df_copy.at[idx, 'Max Usage'] = self.highlight_wal_fsync_values(max_val, 'latency', unit, False)
                
                elif metric_type == 'disk_io':
                    df_copy.at[idx, 'Avg Usage'] = self.highlight_disk_io_values(avg_val, unit, is_top)
                    df_copy.at[idx, 'Max Usage'] = self.highlight_disk_io_values(max_val, unit, False)
                
                elif metric_type == 'network_io':
                    df_copy.at[idx, 'Avg Usage'] = self.highlight_network_io_values(avg_val, 'network', unit, is_top)
                    df_copy.at[idx, 'Max Usage'] = self.highlight_network_io_values(max_val, 'network', unit, False)
                
                elif metric_type == 'backend_commit':
                    df_copy.at[idx, 'Avg Usage'] = self.highlight_backend_commit_values(avg_val, 'latency', unit, is_top)
                    df_copy.at[idx, 'Max Usage'] = self.highlight_backend_commit_values(max_val, 'latency', unit, False)
                
                elif metric_type == 'compact_defrag':
                    df_copy.at[idx, 'Avg Usage'] = self.highlight_compact_defrag_values(avg_val, 'duration', unit, is_top)
                    df_copy.at[idx, 'Max Usage'] = self.highlight_compact_defrag_values(max_val, 'duration', unit, False)
            
            # Rename columns for better display
            column_mapping = {
                'metric_name': 'Metric Name',
                'pod_name': 'Pod Name',
                'node_name': 'Node Name',
                'avg_value': 'Avg Usage',
                'max_value': 'Max Usage',
                'unit': 'Unit'
            }
            
            df_copy = df_copy.rename(columns=column_mapping)
            
            # Select relevant columns based on available data
            display_columns = ['Metric Name']
            if 'Pod Name' in df_copy.columns and df_copy['Pod Name'].notna().any():
                display_columns.append('Pod Name')
            if 'Node Name' in df_copy.columns and df_copy['Node Name'].notna().any():
                display_columns.append('Node Name')
            display_columns.extend(['Avg Usage', 'Max Usage'])
            
            # Filter to only existing columns
            available_columns = [col for col in display_columns if col in df_copy.columns]
            df_copy = df_copy[available_columns]
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"Error formatting deep drive metrics DataFrame: {e}")
            return df
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            for table_name, df in dataframes.items():
                if not df.empty:
                    # Use custom renderer for node memory tables to match screenshot
                    if table_name in {'node_memory_used', 'node_memory_cache_buffer'}:
                        percent_col = 'RAM Used (%)' if table_name == 'node_memory_used' else 'Cache/Buffer (%)'
                        max_percent_col = 'Max RAM (%)' if table_name == 'node_memory_used' else 'Max Cache (%)'
                        html_tables[table_name] = self._create_memory_table_html(df, percent_col, max_percent_col)
                    else:
                        html_tables[table_name] = self.create_html_table(df, table_name)
            
        except Exception as e:
            self.logger.error(f"Error generating HTML tables for deep drive: {e}")
        
        return html_tables
    
    def summarize_deep_drive(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary for deep drive analysis"""
        try:
            summary_parts = []
            
            # Test metadata summary
            metadata = structured_data.get('test_metadata', {})
            test_id = metadata.get('test_id', 'unknown')
            duration = metadata.get('duration', 'unknown')
            status = metadata.get('status', 'unknown')
            
            summary_parts.append(f"<strong>Deep Drive Performance Analysis</strong> (Test ID: {test_id})")
            summary_parts.append(f"Duration: {duration}, Status: {status}")
            
            # Metrics count summary
            metrics_counts = []
            for metric_type in ['general_info_metrics', 'wal_fsync_metrics', 'disk_io_metrics', 
                            'network_metrics', 'backend_commit_metrics', 'compact_defrag_metrics', 
                            'node_usage_metrics']:  # Added node_usage_metrics
                count = len(structured_data.get(metric_type, []))
                if count > 0:
                    display_name = metric_type.replace('_metrics', '').replace('_', ' ').title()
                    metrics_counts.append(f"{display_name}: {count}")


            if metrics_counts:
                summary_parts.append(f"Metrics collected: {', '.join(metrics_counts)}")

            # Analysis results summary
            analysis = structured_data.get('analysis_results', {})
            latency_analysis = analysis.get('latency_analysis', {})
            node_usage_metrics = structured_data.get('node_usage_metrics', [])

            if node_usage_metrics:
                node_count = len(set([m['node_name'] for m in node_usage_metrics]))
                summary_parts.append(f"📊 Node Usage: {node_count} master nodes monitored")
                
                # Check for high resource utilization
                high_cpu_nodes = []
                high_mem_nodes = []
                
                for metric in node_usage_metrics:
                    if 'cpu' in metric.get('metric_type', ''):
                        avg_val = metric.get('avg_value', 0)
                        if avg_val > 70:  # Assuming percentage
                            high_cpu_nodes.append(metric['node_name'])
                    elif metric.get('metric_name') == 'node_memory_used':
                        total_capacity = metric.get('total_capacity', 0)
                        avg_val = metric.get('avg_value', 0)
                        if total_capacity > 0:
                            usage_percent = (avg_val / total_capacity) * 100
                            if usage_percent > 70:
                                high_mem_nodes.append(metric['node_name'])
                
                if high_cpu_nodes:
                    summary_parts.append(f"⚠️ High CPU usage on: {', '.join(set(high_cpu_nodes))}")
                if high_mem_nodes:
                    summary_parts.append(f"⚠️ High memory usage on: {', '.join(set(high_mem_nodes))}")

            if latency_analysis:
                latency_summary = []
                for metric_type, data in latency_analysis.items():
                    if isinstance(data, dict) and 'status' in data:
                        status = data['status']
                        metric_display = metric_type.replace('_', ' ').title()
                        if status == 'excellent':
                            latency_summary.append(f"✅ {metric_display}: {status}")
                        elif status == 'good':
                            latency_summary.append(f"✅ {metric_display}: {status}")
                        else:
                            latency_summary.append(f"⚠️ {metric_display}: {status}")
                
                if latency_summary:
                    summary_parts.append("Latency Status: " + ", ".join(latency_summary))
            
            # Overall health
            summary_info = structured_data.get('summary_info', {})
            overall_health = summary_info.get('overall_health', 'unknown')
            total_metrics = summary_info.get('total_metrics_collected', 0)
            
            summary_parts.append(f"Overall Health: {overall_health.title()}, Total Metrics: {total_metrics}")
            
            # Recommendations
            recommendations = analysis.get('recommendations', [])
            if recommendations:
                if len(recommendations) == 1 and recommendations[0] == "No significant performance bottlenecks detected":
                    summary_parts.append("✅ No performance issues detected")
                else:
                    summary_parts.append(f"📋 {len(recommendations)} recommendations available")
            
            return "<br>".join(summary_parts)
            
        except Exception as e:
            self.logger.error(f"Error generating deep drive summary: {e}")
            return f"Deep Drive Analysis Summary (Error: {str(e)})"

    def _format_node_usage_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format node usage metrics DataFrame with readable units and highlighting"""
        try:
            if df.empty:
                return df
            
            df_copy = df.copy()
            
            # Identify top performers for highlighting
            if 'avg_value' in df_copy.columns:
                top_indices = self.identify_top_values(df_copy.to_dict('records'), 'avg_value')
            else:
                top_indices = []
            
            # Format values based on metric type
            for idx, row in df_copy.iterrows():
                metric_type = str(row.get('metric_type', ''))
                metric_name = str(row.get('metric_name', ''))
                unit = str(row.get('unit', ''))
                avg_val = row.get('avg_value', 0)
                max_val = row.get('max_value', 0)
                total_capacity = row.get('total_capacity', 0)
                
                is_top = idx in top_indices
                
                # Format CPU metrics
                if metric_type == 'cpu' or 'cpu' in metric_name.lower():
                    df_copy.at[idx, 'Avg Usage'] = self.highlight_node_usage_values(
                        avg_val, 'cpu', unit, is_top
                    )
                    df_copy.at[idx, 'Max Usage'] = self.highlight_node_usage_values(
                        max_val, 'cpu', unit, False
                    )
                
                # Format memory metrics
                elif metric_type == 'memory' or 'memory' in metric_name.lower():
                    if total_capacity > 0:
                        avg_percent = (avg_val / total_capacity) * 100
                        max_percent = (max_val / total_capacity) * 100
                        
                        df_copy.at[idx, 'Avg Usage'] = self.highlight_node_usage_values(
                            avg_val, 'memory', unit, is_top, 
                            extra_info=f" ({avg_percent:.1f}%)"
                        )
                        df_copy.at[idx, 'Max Usage'] = self.highlight_node_usage_values(
                            max_val, 'memory', unit, False,
                            extra_info=f" ({max_percent:.1f}%)"
                        )
                        df_copy.at[idx, 'Capacity'] = f"{total_capacity} {unit}"
                    else:
                        df_copy.at[idx, 'Avg Usage'] = self.highlight_node_usage_values(
                            avg_val, 'memory', unit, is_top
                        )
                        df_copy.at[idx, 'Max Usage'] = self.highlight_node_usage_values(
                            max_val, 'memory', unit, False
                        )
                
                # Format cgroup metrics
                elif 'cgroup' in metric_type:
                    df_copy.at[idx, 'Avg Usage'] = f"{avg_val:.2f} {unit}"
                    df_copy.at[idx, 'Max Usage'] = f"{max_val:.2f} {unit}"
            
            # Rename columns
            column_mapping = {
                'metric_name': 'Metric Name',
                'node_name': 'Node Name',
                'avg_value': 'Avg Usage',
                'max_value': 'Max Usage',
                'unit': 'Unit',
                'metric_type': 'Type'
            }
            
            df_copy = df_copy.rename(columns=column_mapping)
            
            # Select display columns
            display_columns = ['Metric Name', 'Node Name', 'Avg Usage', 'Max Usage']
            if 'Capacity' in df_copy.columns:
                display_columns.append('Capacity')
            
            available_columns = [col for col in display_columns if col in df_copy.columns]
            df_copy = df_copy[available_columns]
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"Error formatting node usage DataFrame: {e}")
            return df


    def _build_node_memory_tables_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Create DataFrames for Node Memory Used and Cache/Buffer with percent columns.

        Output columns:
        - Node, Avg (GB), <Percent>, Max (GB), <Max Percent>
        """
        tables: Dict[str, pd.DataFrame] = {}
        try:
            node_usage_metrics: List[Dict[str, Any]] = structured_data.get('node_usage_metrics', [])
            if not node_usage_metrics:
                return tables

            used_rows: List[Dict[str, Union[str, float]]] = []
            cache_rows: List[Dict[str, Union[str, float]]] = []

            for m in node_usage_metrics:
                metric_name = m.get('metric_name')
                if metric_name not in {'node_memory_used', 'node_memory_cache_buffer'}:
                    continue

                node_name = m.get('node_name')
                avg_gb = float(m.get('avg_value', 0) or 0)
                max_gb = float(m.get('max_value', 0) or 0)
                capacity_gb = float(m.get('total_capacity', 0) or 0)

                avg_pct = (avg_gb / capacity_gb * 100) if capacity_gb > 0 else None
                max_pct = (max_gb / capacity_gb * 100) if capacity_gb > 0 else None

                row = {
                    'Node': str(node_name),
                    'Avg (GB)': round(avg_gb, 2),
                    # percent columns filled below depending on table
                    'Max (GB)': round(max_gb, 2),
                }

                if metric_name == 'node_memory_used':
                    row_used = dict(row)
                    row_used['RAM Used (%)'] = round(avg_pct, 2) if avg_pct is not None else None
                    row_used['Max RAM (%)'] = round(max_pct, 2) if max_pct is not None else None
                    used_rows.append(row_used)
                else:
                    row_cache = dict(row)
                    row_cache['Cache/Buffer (%)'] = round(avg_pct, 2) if avg_pct is not None else None
                    row_cache['Max Cache (%)'] = round(max_pct, 2) if max_pct is not None else None
                    cache_rows.append(row_cache)

            if used_rows:
                used_df = pd.DataFrame(used_rows, columns=['Node', 'Avg (GB)', 'RAM Used (%)', 'Max (GB)', 'Max RAM (%)'])
                tables['node_memory_used'] = used_df
            if cache_rows:
                cache_df = pd.DataFrame(cache_rows, columns=['Node', 'Avg (GB)', 'Cache/Buffer (%)', 'Max (GB)', 'Max Cache (%)'])
                tables['node_memory_cache_buffer'] = cache_df

            return tables
        except Exception as e:
            self.logger.error(f"Error building node memory DataFrames: {e}")
            return tables


    def _create_memory_table_html(self, df: pd.DataFrame, percent_col: str, max_percent_col: str) -> str:
        """Render a compact HTML table for node memory metrics with a trophy on max percent row."""
        try:
            if df.empty:
                return ""

            # Determine the row with highest max percent for trophy icon
            max_idx = None
            try:
                max_idx = df[max_percent_col].astype(float).idxmax()
            except Exception:
                max_idx = None

            # Inline styles to roughly match the screenshot
            table_style = (
                "width:100%; border-collapse:separate; border-spacing:0; font-family:system-ui, -apple-system, Segoe UI, Roboto, Arial;"
            )
            th_style = (
                "text-align:left; padding:10px 12px; background:#e9f3fb; color:#333; font-weight:600;"
            )
            td_style = "padding:10px 12px; border-top:1px solid #e6eef5; color:#222;"
            right_td_style = td_style + " text-align:right;"

            # Build header
            headers = list(df.columns)

            html = [f'<table style="{table_style}">']
            html.append('<thead><tr>')
            for h in headers:
                html.append(f'<th style="{th_style}">{h}</th>')
            html.append('</tr></thead>')

            # Build rows
            html.append('<tbody>')
            for idx, row in df.iterrows():
                html.append('<tr>')
                # Node (left)
                html.append(f'<td style="{td_style}">{row.get("Node", "-")}</td>')

                # Avg (GB) right aligned
                avg_gb = row.get('Avg (GB)')
                avg_gb_str = '-' if pd.isna(avg_gb) else f"{float(avg_gb):.2f}"
                html.append(f'<td style="{right_td_style}">{avg_gb_str}</td>')

                # Percent (with %)
                pct = row.get(percent_col)
                pct_str = '-' if pd.isna(pct) else f"{float(pct):.2f}%"
                html.append(f'<td style="{right_td_style}">{pct_str}</td>')

                # Max (GB)
                max_gb = row.get('Max (GB)')
                max_gb_str = '-' if pd.isna(max_gb) else f"{float(max_gb):.2f}"
                html.append(f'<td style="{right_td_style}">{max_gb_str}</td>')

                # Max percent with trophy for the top row
                mp = row.get(max_percent_col)
                mp_str_core = '-' if pd.isna(mp) else f"{float(mp):.2f}%"
                trophy = ' 🏆' if max_idx is not None and idx == max_idx and not pd.isna(mp) else ''
                html.append(f'<td style="{right_td_style}">{mp_str_core}{trophy}</td>')

                html.append('</tr>')
            html.append('</tbody></table>')

            return ''.join(html)
        except Exception as e:
            self.logger.error(f"Error creating memory table HTML: {e}")
            return self.create_html_table(df, 'memory_table')
