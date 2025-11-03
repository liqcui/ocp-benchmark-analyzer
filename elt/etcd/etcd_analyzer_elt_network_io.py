"""
Network I/O ELT module for ETCD Analyzer - Optimized Version
Extract, Load, Transform module for Network I/O Performance Data
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from datetime import datetime
from .etcd_analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)

class networkIOELT(utilityELT):
    """Extract, Load, Transform class for Network I/O data - Optimized"""
    
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)
    
    def extract_network_io(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract network I/O metrics data"""
        try:
            # Handle deeply nested data structure
            metrics_data = data
            
            # Navigate through the nested structure
            if 'data' in data and isinstance(data['data'], dict):
                if 'data' in data['data'] and isinstance(data['data']['data'], dict):
                    # Double nested: data -> data -> data
                    metrics_data = data['data']['data']
                else:
                    # Single nested: data -> data
                    metrics_data = data['data']
            
            # Extract different levels of metrics
            pods_metrics = metrics_data.get('pods_metrics', {})
            container_metrics = metrics_data.get('container_metrics', {})
            node_metrics = metrics_data.get('node_metrics', {})
            cluster_metrics = metrics_data.get('cluster_metrics', {})
            
            # Prefer pods_metrics over container_metrics, but use both if available
            if not pods_metrics and container_metrics:
                pods_metrics = container_metrics
            
            # Extract pod-level metrics
            pod_data = []
            for metric_name, metric_info in pods_metrics.items():
                if metric_info.get('status') == 'success' and metric_info.get('pods'):
                    unit = metric_info.get('unit', 'unknown')
                    for pod_name, pod_stats in metric_info.get('pods', {}).items():
                        avg_value = pod_stats.get('avg', 0) or 0
                        max_value = pod_stats.get('max', 0) or 0
                        
                        pod_data.append({
                            'metric_name': metric_name,
                            'pod_name': pod_name,
                            'avg_value': float(avg_value),
                            'max_value': float(max_value),
                            'unit': unit
                        })
            
            # Extract node-level metrics
            node_data = []
            for metric_name, metric_info in node_metrics.items():
                if metric_info.get('status') == 'success' and metric_info.get('nodes'):
                    unit = metric_info.get('unit', 'unknown')
                    for node_name, node_stats in metric_info.get('nodes', {}).items():
                        avg_value = node_stats.get('avg', 0) or 0
                        max_value = node_stats.get('max', 0) or 0
                        
                        node_data.append({
                            'metric_name': metric_name,
                            'node_name': node_name,
                            'avg_value': float(avg_value),
                            'max_value': float(max_value),
                            'unit': unit
                        })
            
            # Extract cluster-level metrics
            cluster_data = []
            for metric_name, metric_info in cluster_metrics.items():
                if metric_info.get('status') == 'success':
                    unit = metric_info.get('unit', 'unknown')
                    avg_value = metric_info.get('avg', 0) or 0
                    max_value = metric_info.get('max', 0) or 0
                    latest_value = metric_info.get('latest', 0) or 0
                    
                    cluster_data.append({
                        'metric_name': metric_name,
                        'avg_value': float(avg_value),
                        'max_value': float(max_value),
                        'latest_value': float(latest_value),
                        'unit': unit
                    })
            
            # Create network overview with top metrics
            overview_data = self._create_network_overview(pod_data, node_data, cluster_data)
            
            # Get timestamp and duration
            timestamp = (data.get('timestamp') or 
                        data.get('data', {}).get('timestamp') or 
                        metrics_data.get('timestamp') or 
                        datetime.now().isoformat())
            
            duration = (data.get('duration') or 
                       data.get('data', {}).get('duration') or 
                       metrics_data.get('duration') or 
                       'unknown')
            
            result = {
                'pod_metrics': pod_data,
                'node_metrics': node_data,
                'cluster_metrics': cluster_data,
                'overview': overview_data,
                'timestamp': timestamp,
                'duration': duration
            }
            
            self.logger.info(f"Network I/O extraction results: {len(pod_data)} pods, {len(node_data)} nodes, {len(cluster_data)} cluster metrics")
            return result
            
        except Exception as e:
            logger.error(f"Failed to extract network I/O data: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {'error': str(e)}
    
    def _create_network_overview(self, pod_data: List[Dict[str, Any]], node_data: List[Dict[str, Any]], 
                                cluster_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create network overview with top metrics summary"""
        overview_data = []
        
        # Combine all metrics for top analysis
        all_metrics = {}
        
        # Process pod metrics
        for item in pod_data:
            metric_name = item['metric_name']
            if metric_name not in all_metrics:
                all_metrics[metric_name] = {
                    'avg_values': [],
                    'max_values': [],
                    'unit': item['unit'],
                    'category': 'Pod Network'
                }
            all_metrics[metric_name]['avg_values'].append(item['avg_value'])
            all_metrics[metric_name]['max_values'].append(item['max_value'])
        
        # Process node metrics
        for item in node_data:
            metric_name = item['metric_name']
            if metric_name not in all_metrics:
                all_metrics[metric_name] = {
                    'avg_values': [],
                    'max_values': [],
                    'unit': item['unit'],
                    'category': 'Node Network'
                }
            all_metrics[metric_name]['avg_values'].append(item['avg_value'])
            all_metrics[metric_name]['max_values'].append(item['max_value'])
        
        # Process cluster metrics
        for item in cluster_data:
            metric_name = item['metric_name']
            all_metrics[metric_name] = {
                'avg_values': [item['avg_value']],
                'max_values': [item['max_value']],
                'unit': item['unit'],
                'category': 'Cluster'
            }
        
        # Create overview entries for each metric
        for metric_name, metric_data in all_metrics.items():
            avg_values = [v for v in metric_data['avg_values'] if v is not None and v > 0]
            max_values = [v for v in metric_data['max_values'] if v is not None and v > 0]
            
            if avg_values or max_values:
                # Calculate aggregated values
                total_avg = sum(avg_values) if avg_values else 0
                peak_max = max(max_values) if max_values else 0
                
                # Clean metric name for display
                display_name = self._clean_metric_name_for_display(metric_name)
                
                overview_data.append({
                    'metric_name': display_name,
                    'category': metric_data['category'],
                    'avg_usage': total_avg,
                    'max_usage': peak_max,
                    'unit': metric_data['unit'],
                    'raw_metric_name': metric_name  # Keep for threshold checking
                })
        
        # Sort by average usage (descending) to show top metrics first
        overview_data.sort(key=lambda x: x['avg_usage'], reverse=True)
        
        return overview_data

    def _clean_metric_name_for_display(self, metric_name: str) -> str:
        """Clean metric name for display"""
        display_name = metric_name.replace('network_io_', '').replace('_', ' ').title()
        
        # Apply specific transformations
        replacements = {
            'Container Network Rx': 'Container RX Bytes',
            'Container Network Tx': 'Container TX Bytes', 
            'Network Client Grpc Received Bytes': 'gRPC RX Bytes',
            'Network Client Grpc Sent Bytes': 'gRPC TX Bytes',
            'Network Peer Received Bytes': 'Peer RX Bytes',
            'Network Peer Sent Bytes': 'Peer TX Bytes',
            'Peer2Peer Latency P99': 'Peer Latency P99',
            'Node Network Rx Utilization': 'Node RX Utilization',
            'Node Network Tx Utilization': 'Node TX Utilization',
            'Node Network Rx Package': 'Node RX Packets',
            'Node Network Tx Package': 'Node TX Packets',
            'Node Network Rx Drop': 'Node RX Drops',
            'Node Network Tx Drop': 'Node TX Drops',
            'Grpc Active Watch Streams': 'Active Watch Streams',
            'Grpc Active Lease Streams': 'Active Lease Streams'
        }
        
        for old, new in replacements.items():
            if display_name == old:
                display_name = new
                break
        
        return display_name

    def _format_network_value(self, value: float, unit: str) -> str:
        """Format network values with appropriate units"""
        try:
            if unit == 'bytes_per_second':
                return self._format_bytes_per_second(value)
            elif unit == 'bits_per_second':
                return self._format_bits_per_second(value)
            elif unit == 'packets_per_second':
                return self._format_packets_per_second(value)
            elif unit == 'count':
                return self._format_count(value)
            else:
                return f"{value:.2f}"
        except (ValueError, TypeError):
            return str(value)

    def _format_bytes_per_second(self, bytes_per_sec: float) -> str:
        """Format bytes per second to readable units"""
        if bytes_per_sec == 0:
            return "0 B/s"
        elif bytes_per_sec < 1024:
            return f"{bytes_per_sec:.1f} B/s"
        elif bytes_per_sec < 1024**2:
            return f"{bytes_per_sec/1024:.1f} KB/s"
        elif bytes_per_sec < 1024**3:
            return f"{bytes_per_sec/(1024**2):.1f} MB/s"
        else:
            return f"{bytes_per_sec/(1024**3):.2f} GB/s"

    def _format_bits_per_second(self, bits_per_sec: float) -> str:
        """Format bits per second to readable units"""
        if bits_per_sec == 0:
            return "0 bps"
        elif bits_per_sec < 1000:
            return f"{bits_per_sec:.1f} bps"
        elif bits_per_sec < 1000**2:
            return f"{bits_per_sec/1000:.1f} Kbps"
        elif bits_per_sec < 1000**3:
            return f"{bits_per_sec/(1000**2):.1f} Mbps"
        else:
            return f"{bits_per_sec/(1000**3):.2f} Gbps"

    def _format_packets_per_second(self, packets_per_sec: float) -> str:
        """Format packets per second to readable units"""
        if packets_per_sec == 0:
            return "0 pps"
        elif packets_per_sec < 1:
            return f"{packets_per_sec:.3f} pps"
        elif packets_per_sec < 1000:
            return f"{packets_per_sec:.1f} pps"
        else:
            return f"{packets_per_sec/1000:.1f} Kpps"

    def _format_count(self, count: float) -> str:
        """Format count values"""
        if count == 0:
            return "0"
        elif count < 1000:
            return f"{int(count)}"
        else:
            return f"{count/1000:.1f}K"

    def _highlight_value(self, value: float, unit: str, is_top: bool = False) -> str:
        """Add highlighting to formatted values"""
        formatted = self._format_network_value(value, unit)
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
        elif self._is_high_value(value, unit):
            return f'<span class="text-warning font-weight-bold">{formatted}</span>'
        else:
            return f'<span class="text-success">{formatted}</span>'

    def _is_high_value(self, value: float, unit: str) -> bool:
        """Determine if value is high based on unit type"""
        if unit == 'bytes_per_second':
            return value > 100 * 1024 * 1024  # > 100 MB/s
        elif unit == 'bits_per_second':
            return value > 100 * 1000 * 1000  # > 100 Mbps
        elif unit == 'packets_per_second':
            return value > 1000  # > 1K pps
        elif unit == 'count':
            return value > 500  # > 500 streams
        return False
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames with proper formatting"""
        dataframes = {}
        
        try:
            # Network overview table
            if structured_data.get('overview'):
                overview_data = structured_data['overview']
                if overview_data:
                    overview_df = pd.DataFrame(overview_data)
                    if not overview_df.empty:
                        # Apply formatting and highlighting
                        overview_df['avg_formatted'] = overview_df.apply(
                            lambda row: self._highlight_value(row['avg_usage'], row['unit']), axis=1
                        )
                        overview_df['max_formatted'] = overview_df.apply(
                            lambda row: self._highlight_value(row['max_usage'], row['unit']), axis=1
                        )
                        
                        # Highlight top 3 performers
                        for idx in range(min(3, len(overview_df))):
                            overview_df.at[idx, 'avg_formatted'] = self._highlight_value(
                                overview_df.at[idx, 'avg_usage'], overview_df.at[idx, 'unit'], is_top=True
                            )
                        
                        dataframes['metrics_overview'] = overview_df

            # Pod metrics table (renamed from container_metrics)
            pod_metrics_list = structured_data.get('pod_metrics', [])
            if pod_metrics_list:
                pod_df = pd.DataFrame(pod_metrics_list)
                if not pod_df.empty:
                    # Apply formatting
                    pod_df['avg_formatted'] = pod_df.apply(
                        lambda row: self._highlight_value(row['avg_value'], row['unit']), axis=1
                    )
                    pod_df['max_formatted'] = pod_df.apply(
                        lambda row: self._highlight_value(row['max_value'], row['unit']), axis=1
                    )
                    
                    # Highlight top performers
                    top_indices = self.identify_top_values(pod_metrics_list, 'avg_value')
                    for idx in top_indices:
                        if idx < len(pod_df):
                            pod_df.at[idx, 'avg_formatted'] = self._highlight_value(
                                pod_df.at[idx, 'avg_value'], pod_df.at[idx, 'unit'], is_top=True
                            )
                    
                    dataframes['pod_metrics'] = pod_df

            # Node metrics table
            if structured_data.get('node_metrics'):
                node_metrics_list = structured_data['node_metrics']
                if node_metrics_list:
                    node_df = pd.DataFrame(node_metrics_list)
                    if not node_df.empty:
                        # Apply formatting
                        node_df['avg_formatted'] = node_df.apply(
                            lambda row: self._highlight_value(row['avg_value'], row['unit']), axis=1
                        )
                        node_df['max_formatted'] = node_df.apply(
                            lambda row: self._highlight_value(row['max_value'], row['unit']), axis=1
                        )
                        
                        # Highlight top performers
                        top_indices = self.identify_top_values(node_metrics_list, 'avg_value')
                        for idx in top_indices:
                            if idx < len(node_df):
                                node_df.at[idx, 'avg_formatted'] = self._highlight_value(
                                    node_df.at[idx, 'avg_value'], node_df.at[idx, 'unit'], is_top=True
                                )
                        
                        dataframes['node_metrics'] = node_df

            # Cluster metrics table
            if structured_data.get('cluster_metrics'):
                cluster_metrics_list = structured_data['cluster_metrics']
                if cluster_metrics_list:
                    cluster_df = pd.DataFrame(cluster_metrics_list)
                    if not cluster_df.empty:
                        # Apply formatting
                        cluster_df['avg_formatted'] = cluster_df.apply(
                            lambda row: self._highlight_value(row['avg_value'], row['unit']), axis=1
                        )
                        cluster_df['max_formatted'] = cluster_df.apply(
                            lambda row: self._highlight_value(row['max_value'], row['unit']), axis=1
                        )
                        cluster_df['latest_formatted'] = cluster_df.apply(
                            lambda row: self._highlight_value(row['latest_value'], row['unit']), axis=1
                        )
                        
                        dataframes['cluster_metrics'] = cluster_df

            # Create fallback if no dataframes
            if not dataframes:
                fallback_data = [{
                    'Status': 'No Network I/O Data Available',
                    'Details': 'No metrics were found in the provided data',
                    'Suggestion': 'Verify that network I/O metrics are being collected'
                }]
                dataframes['status'] = pd.DataFrame(fallback_data)
                
        except Exception as e:
            self.logger.error(f"Failed to create DataFrames: {e}")
            error_data = [{
                'Error': 'DataFrame Creation Failed',
                'Message': str(e),
                'Action': 'Check the input data structure and format'
            }]
            dataframes['error_info'] = pd.DataFrame(error_data)
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames with optimized formatting"""
        html_tables = {}
        
        try:
            # Network Usage Overview
            if 'metrics_overview' in dataframes:
                df = dataframes['metrics_overview'].copy()
                if not df.empty:
                    df_display = df[['metric_name', 'category', 'avg_formatted', 'max_formatted']].copy()
                    df_display.columns = ['Metric', 'Category', 'Average Usage', 'Peak Usage']
                    html_tables['metrics_overview'] = self.create_html_table(df_display, 'Network Usage Overview')
            
            # Pods Network Usage (removed node_name column)
            if 'pod_metrics' in dataframes:
                df = dataframes['pod_metrics'].copy()
                if not df.empty:
                    df_display = df[['pod_name', 'avg_formatted', 'max_formatted']].copy()
                    df_display.columns = ['Pod Name', 'Average Usage', 'Peak Usage']
                    html_tables['container_metrics'] = self.create_html_table(df_display, 'Pods Network Usage')
            
            # Node Network Usage
            if 'node_metrics' in dataframes:
                df = dataframes['node_metrics'].copy()
                if not df.empty:
                    df_display = df[['node_name', 'avg_formatted', 'max_formatted']].copy()
                    df_display.columns = ['Node Name', 'Average Usage', 'Peak Usage']
                    html_tables['node_performance'] = self.create_html_table(df_display, 'Node Network Usage')
            
            # gRPC Active Stream
            if 'cluster_metrics' in dataframes:
                df = dataframes['cluster_metrics'].copy()
                if not df.empty:
                    # Clean up metric names for display
                    df['clean_metric_name'] = df['metric_name'].str.replace('network_io_', '').str.replace('_', ' ').str.title()
                    df_display = df[['clean_metric_name', 'latest_formatted', 'avg_formatted', 'max_formatted']].copy()
                    df_display.columns = ['Stream Type', 'Current Count', 'Average Count', 'Peak Count']
                    html_tables['grpc_streams'] = self.create_html_table(df_display, 'gRPC Active Stream')
            
            # Handle fallback tables
            if 'status' in dataframes:
                df = dataframes['status'].copy()
                html_tables['status'] = self.create_html_table(df, 'Network I/O Status')
            
            if 'error_info' in dataframes:
                df = dataframes['error_info'].copy()
                html_tables['error_info'] = self.create_html_table(df, 'Processing Error')
            
        except Exception as e:
            logger.error(f"Failed to generate HTML tables: {e}")
            return {}
        
        return html_tables
    
    def summarize_network_io(self, structured_data: Dict[str, Any]) -> str:
        """Generate a brief summary of network I/O performance"""
        try:
            pod_metrics = structured_data.get('pod_metrics', [])
            node_metrics = structured_data.get('node_metrics', [])
            cluster_metrics = structured_data.get('cluster_metrics', [])
            
            summary_parts = ["Network I/O Performance Analysis:"]
            
            if pod_metrics:
                total_pods = len(set(item['pod_name'] for item in pod_metrics))
                avg_throughput = self._calculate_pod_throughput_avg(pod_metrics)
                summary_parts.append(f"• {total_pods} etcd pods monitored with avg throughput {avg_throughput:.1f} Mbps")
                
                # Check for high throughput pods
                high_throughput = [item for item in pod_metrics if item['avg_value'] > 100*1024*1024]  # >100MB/s
                if high_throughput:
                    summary_parts.append(f"⚠️ {len(high_throughput)} pods with high network usage (>100MB/s)")
            
            if node_metrics:
                total_nodes = len(set(item['node_name'] for item in node_metrics))
                avg_utilization = self._calculate_node_utilization_avg(node_metrics)
                summary_parts.append(f"• {total_nodes} master nodes with avg network utilization {avg_utilization:.1f} Mbps")
            
            if cluster_metrics:
                active_streams = self._get_active_streams_count(cluster_metrics)
                stream_health = self._assess_stream_health(cluster_metrics)
                summary_parts.append(f"• {active_streams} active gRPC streams ({stream_health} load)")
            
            # Overall health assessment
            health_status = self._assess_network_health(pod_metrics + node_metrics)
            if health_status == 'critical':
                summary_parts.append("⚠️ Network performance issues detected")
            elif health_status == 'degraded':
                summary_parts.append("⚠️ Some network metrics elevated")
            else:
                summary_parts.append("✓ Network I/O performance appears healthy")
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate network I/O summary: {e}")
            return f"Network I/O summary generation failed: {str(e)}"

    def _calculate_pod_throughput_avg(self, pod_metrics: List[Dict[str, Any]]) -> float:
        """Calculate average pod throughput in Mbps"""
        try:
            throughput_metrics = [item for item in pod_metrics 
                                if 'bytes' in item['metric_name'].lower()]
            if not throughput_metrics:
                return 0.0
            
            # Convert bytes/second to Mbps
            total_throughput = sum(item['avg_value'] for item in throughput_metrics)
            return (total_throughput * 8) / (1024 * 1024)  # Convert bytes/s to Mbps
        except Exception:
            return 0.0

    def _calculate_node_utilization_avg(self, node_metrics: List[Dict[str, Any]]) -> float:
        """Calculate average node network utilization in Mbps"""
        try:
            utilization_metrics = [item for item in node_metrics 
                                 if 'utilization' in item['metric_name'].lower()]
            if not utilization_metrics:
                return 0.0
            
            # Convert bits/second to Mbps
            total_utilization = sum(item['avg_value'] for item in utilization_metrics)
            return total_utilization / (1000 * 1000)  # Convert bits/s to Mbps
        except Exception:
            return 0.0

    def _get_active_streams_count(self, cluster_metrics: List[Dict[str, Any]]) -> int:
        """Get current active gRPC streams count"""
        try:
            stream_metrics = [item for item in cluster_metrics 
                            if 'watch_streams' in item['metric_name'].lower()]
            if stream_metrics:
                return int(stream_metrics[0]['latest_value'])
            return 0
        except Exception:
            return 0

    def _assess_stream_health(self, cluster_metrics: List[Dict[str, Any]]) -> str:
        """Assess gRPC stream health based on count"""
        try:
            active_streams = self._get_active_streams_count(cluster_metrics)
            if active_streams == 0:
                return "no"
            elif active_streams < 100:
                return "low"
            elif active_streams < 500:
                return "normal"
            elif active_streams < 1000:
                return "high"
            else:
                return "critical"
        except Exception:
            return "unknown"

    def _assess_network_health(self, metrics_data: List[Dict[str, Any]]) -> str:
        """Assess overall network health status"""
        try:
            if not metrics_data:
                return "unknown"
            
            high_usage_count = 0
            total_metrics = len(metrics_data)
            
            for metric in metrics_data:
                if metric.get('avg_value', 0) > 0:
                    if self._is_high_value(metric['avg_value'], metric.get('unit', '')):
                        high_usage_count += 1
            
            if high_usage_count > total_metrics * 0.5:  # More than 50% high usage
                return "critical"
            elif high_usage_count > total_metrics * 0.3:  # More than 30% high usage
                return "degraded"
            else:
                return "healthy"
                
        except Exception as e:
            logger.error(f"Failed to assess network health: {e}")
            return "unknown"