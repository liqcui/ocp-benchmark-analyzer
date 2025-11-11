"""
Extract, Load, Transform module for ETCD Analyzer Disk WAL Fsync Performance Data
Follows network_l1 pattern with node role grouping and metric extraction
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from datetime import datetime

from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class diskWalFsyncELT(utilityELT):
    """ELT processor for disk WAL fsync performance data"""
    
    def __init__(self):
        super().__init__()
        self.metric_names = [
            'disk_wal_fsync_seconds_duration_p99',
            'disk_wal_fsync_duration_seconds_sum_rate',
            'disk_wal_fsync_duration_sum',
            'disk_wal_fsync_duration_seconds_count_rate',
            'disk_wal_fsync_duration_seconds_count'
        ]

    def extract_wal_fsync(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and structure disk WAL fsync data grouped by node roles"""
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
                    'total_metrics': len(metrics_data),
                    'successful_metrics': sum(1 for m in metrics_data.values() if m.get('status') == 'success')
                },
                'node_groups': {
                    'controlplane': {'nodes': [], 'metrics': {}},
                    'infra': {'nodes': [], 'metrics': {}},
                    'worker': {'nodes': [], 'metrics': {}},
                    'workload': {'nodes': [], 'metrics': {}}
                }
            }
            
            # Extract metrics with node role grouping
            for metric_name in self.metric_names:
                if metric_name in metrics_data:
                    metric_data = metrics_data[metric_name]
                    if metric_data.get('status') == 'success':
                        # Extract metric for each node role group
                        for role in structured['node_groups'].keys():
                            extracted = self._extract_metric_by_role(metric_name, metric_data, role)
                            if extracted:
                                structured['node_groups'][role]['metrics'][metric_name] = extracted
                                # Collect unique nodes
                                for node in extracted.get('nodes', []):
                                    if node not in structured['node_groups'][role]['nodes']:
                                        structured['node_groups'][role]['nodes'].append(node)
            
            return structured
            
        except Exception as e:
            logger.error(f"Failed to extract WAL fsync data: {e}")
            return {'error': str(e)}
    
    def _extract_metric_by_role(self, metric_name: str, metric_data: Dict[str, Any], role: str) -> Dict[str, Any]:
        """Extract metric data filtered by node role"""
        try:
            pod_metrics = metric_data.get('pod_metrics', {})
            node_mapping = metric_data.get('node_mapping', {})
            
            if not pod_metrics:
                return None
            
            # Filter pods/nodes by role
            role_pods = {}
            role_nodes = []
            
            for pod, node in node_mapping.items():
                # etcd runs on control plane nodes - check if pod name starts with 'etcd-'
                if role == 'controlplane' and pod.startswith('etcd-'):
                    role_pods[pod] = pod_metrics.get(pod)
                    if node not in role_nodes:
                        role_nodes.append(node)
            
            if not role_pods:
                return None
            
            # Extract based on metric type
            if metric_name == 'disk_wal_fsync_seconds_duration_p99':
                return self._extract_p99_latency(role_pods, role_nodes, metric_data)
            elif metric_name == 'disk_wal_fsync_duration_seconds_sum_rate':
                return self._extract_duration_sum_rate(role_pods, role_nodes, metric_data)
            elif metric_name == 'disk_wal_fsync_duration_sum':
                return self._extract_duration_sum(role_pods, role_nodes, metric_data)
            elif metric_name == 'disk_wal_fsync_duration_seconds_count_rate':
                return self._extract_count_rate(role_pods, role_nodes, metric_data)
            elif metric_name == 'disk_wal_fsync_duration_seconds_count':
                return self._extract_count(role_pods, role_nodes, metric_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to extract metric {metric_name} for role {role}: {e}")
            return None

    def _extract_p99_latency(self, pod_metrics: Dict[str, Any], nodes: List[str], metric_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract P99 latency metric"""
        try:
            pod_data = []
            all_values = []
            
            for pod, stats in pod_metrics.items():
                if not stats:
                    continue
                
                avg_val = stats.get('avg_seconds', 0)
                max_val = stats.get('max_seconds', 0)
                min_val = stats.get('min_seconds', 0)
                latest_val = stats.get('latest_seconds', 0)
                
                all_values.append(max_val)
                
                pod_data.append({
                    'pod': pod,
                    'avg_seconds': avg_val,
                    'max_seconds': max_val,
                    'min_seconds': min_val,
                    'latest_seconds': latest_val,
                    'data_points': stats.get('data_points', 0)
                })
            
            return {
                'title': metric_data.get('title', 'P99 WAL Fsync Latency'),
                'unit': 'seconds',
                'nodes': nodes,
                'pod_data': pod_data,
                'overall_stats': metric_data.get('overall_stats', {}),
                'top_values': self.identify_top_values(pod_data, 'max_seconds')
            }
            
        except Exception as e:
            logger.error(f"Failed to extract P99 latency: {e}")
            return None

    def _extract_duration_sum_rate(self, pod_metrics: Dict[str, Any], nodes: List[str], metric_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract duration sum rate metric"""
        try:
            pod_data = []
            all_values = []
            
            for pod, stats in pod_metrics.items():
                if not stats:
                    continue
                
                avg_val = stats.get('avg_rate_seconds', 0)
                max_val = stats.get('max_rate_seconds', 0)
                min_val = stats.get('min_rate_seconds', 0)
                latest_val = stats.get('latest_rate_seconds', 0)
                
                all_values.append(max_val)
                
                pod_data.append({
                    'pod': pod,
                    'avg_rate_seconds': avg_val,
                    'max_rate_seconds': max_val,
                    'min_rate_seconds': min_val,
                    'latest_rate_seconds': latest_val,
                    'data_points': stats.get('data_points', 0)
                })
            
            return {
                'title': metric_data.get('title', 'WAL Fsync Duration Sum Rate'),
                'unit': 'seconds',
                'nodes': nodes,
                'pod_data': pod_data,
                'overall_stats': metric_data.get('overall_stats', {}),
                'top_values': self.identify_top_values(pod_data, 'max_rate_seconds')
            }
            
        except Exception as e:
            logger.error(f"Failed to extract duration sum rate: {e}")
            return None

    def _extract_duration_sum(self, pod_metrics: Dict[str, Any], nodes: List[str], metric_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cumulative duration sum metric"""
        try:
            pod_data = []
            all_values = []
            
            for pod, stats in pod_metrics.items():
                if not stats:
                    continue
                
                avg_val = stats.get('avg_sum_seconds', 0)
                max_val = stats.get('max_sum_seconds', 0)
                min_val = stats.get('min_sum_seconds', 0)
                latest_val = stats.get('latest_sum_seconds', 0)
                
                all_values.append(max_val)
                
                pod_data.append({
                    'pod': pod,
                    'avg_sum_seconds': avg_val,
                    'max_sum_seconds': max_val,
                    'min_sum_seconds': min_val,
                    'latest_sum_seconds': latest_val,
                    'data_points': stats.get('data_points', 0)
                })
            
            return {
                'title': metric_data.get('title', 'WAL Fsync Duration Sum'),
                'unit': 'seconds',
                'nodes': nodes,
                'pod_data': pod_data,
                'overall_stats': metric_data.get('overall_stats', {}),
                'top_values': self.identify_top_values(pod_data, 'max_sum_seconds')
            }
            
        except Exception as e:
            logger.error(f"Failed to extract duration sum: {e}")
            return None

    def _extract_count_rate(self, pod_metrics: Dict[str, Any], nodes: List[str], metric_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract operations count rate metric"""
        try:
            pod_data = []
            all_values = []
            
            for pod, stats in pod_metrics.items():
                if not stats:
                    continue
                
                # The actual data uses avg_rate_seconds, max_rate_seconds, etc.
                avg_val = stats.get('avg_rate_seconds', 0) or stats.get('avg_ops_per_sec', 0)
                max_val = stats.get('max_rate_seconds', 0) or stats.get('max_ops_per_sec', 0)
                min_val = stats.get('min_rate_seconds', 0) or stats.get('min_ops_per_sec', 0)
                latest_val = stats.get('latest_rate_seconds', 0) or stats.get('latest_ops_per_sec', 0)
                
                all_values.append(max_val)
                
                pod_data.append({
                    'pod': pod,
                    'avg_ops_per_sec': avg_val,
                    'max_ops_per_sec': max_val,
                    'min_ops_per_sec': min_val,
                    'latest_ops_per_sec': latest_val,
                    'data_points': stats.get('data_points', 0)
                })
            
            return {
                'title': metric_data.get('title', 'WAL Fsync Operations Rate'),
                'unit': 'ops/sec',
                'nodes': nodes,
                'pod_data': pod_data,
                'overall_stats': metric_data.get('overall_stats', {}),
                'top_values': self.identify_top_values(pod_data, 'max_ops_per_sec')
            }
            
        except Exception as e:
            logger.error(f"Failed to extract count rate: {e}")
            return None

    def _extract_count(self, pod_metrics: Dict[str, Any], nodes: List[str], metric_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract total operations count metric"""
        try:
            pod_data = []
            all_values = []
            
            for pod, stats in pod_metrics.items():
                if not stats:
                    continue
                
                avg_val = stats.get('avg_count', 0)
                max_val = stats.get('max_count', 0)
                min_val = stats.get('min_count', 0)
                latest_val = stats.get('latest_count', 0)
                
                all_values.append(max_val)
                
                pod_data.append({
                    'pod': pod,
                    'avg_count': avg_val,
                    'max_count': max_val,
                    'min_count': min_val,
                    'latest_count': latest_val,
                    'data_points': stats.get('data_points', 0)
                })
            
            return {
                'title': metric_data.get('title', 'WAL Fsync Operations Count'),
                'unit': 'count',
                'nodes': nodes,
                'pod_data': pod_data,
                'overall_stats': metric_data.get('overall_stats', {}),
                'top_values': self.identify_top_values(pod_data, 'max_count')
            }
            
        except Exception as e:
            logger.error(f"Failed to extract count: {e}")
            return None

    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into DataFrames grouped by node roles"""
        try:
            if 'error' in structured_data:
                return {}
            
            dataframes = {}
            node_groups = structured_data.get('node_groups', {})
            
            # Process each node role group
            for role, role_data in node_groups.items():
                metrics = role_data.get('metrics', {})
                if not metrics:
                    continue
                
                # Create dataframes for each metric in this role
                for metric_name, metric_info in metrics.items():
                    pod_data = metric_info.get('pod_data', [])
                    if not pod_data:
                        continue
                    
                    # Build dataframe based on metric type
                    df_data = []
                    top_indices = metric_info.get('top_values', [])
                    metric_title = metric_info.get('title', metric_name.replace('_', ' ').title())
                    
                    for idx, pod_stat in enumerate(pod_data):
                        is_top = idx in top_indices
                        
                        if metric_name == 'disk_wal_fsync_seconds_duration_p99':
                            df_data.append(self._format_p99_row(pod_stat, is_top, metric_title, role))
                        elif metric_name == 'disk_wal_fsync_duration_seconds_sum_rate':
                            df_data.append(self._format_sum_rate_row(pod_stat, is_top, metric_title, role))
                        elif metric_name == 'disk_wal_fsync_duration_sum':
                            df_data.append(self._format_sum_row(pod_stat, is_top, metric_title, role))
                        elif metric_name == 'disk_wal_fsync_duration_seconds_count_rate':
                            df_data.append(self._format_count_rate_row(pod_stat, is_top, metric_title, role))
                        elif metric_name == 'disk_wal_fsync_duration_seconds_count':
                            df_data.append(self._format_count_row(pod_stat, is_top, metric_title, role))
                    
                    if df_data:
                        table_name = f"{role}_{metric_name}"
                        dataframes[table_name] = pd.DataFrame(df_data)
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform WAL fsync data to DataFrames: {e}")
            return {}

    def _format_p99_row(self, pod_stat: Dict[str, Any], is_top: bool, metric_title: str, role: str) -> Dict[str, str]:
        """Format P99 latency row with highlighting"""
        thresholds = {'critical': 100, 'warning': 50}  # in milliseconds
        
        return {
            'Metric Name': metric_title,
            'Role': role.title(),
            'Pod': self.truncate_text(pod_stat['pod'], 45),
            'Avg Latency': self.highlight_latency_value(pod_stat['avg_seconds'] * 1000, is_top),
            'Max Latency': self.highlight_latency_value(pod_stat['max_seconds'] * 1000, is_top),
            'Min Latency': self.format_latency_ms(pod_stat['min_seconds']),
            'Latest Latency': self.format_latency_ms(pod_stat['latest_seconds']),
            'Data Points': str(pod_stat['data_points'])
        }

    def _format_sum_rate_row(self, pod_stat: Dict[str, Any], is_top: bool, metric_title: str, role: str) -> Dict[str, str]:
        """Format duration sum rate row"""
        return {
            'Metric Name': metric_title,
            'Role': role.title(),
            'Pod': self.truncate_text(pod_stat['pod'], 45),
            'Avg Rate': f"{pod_stat['avg_rate_seconds']:.6f} s/s",
            'Max Rate': f"{pod_stat['max_rate_seconds']:.6f} s/s" + (' 🏆' if is_top else ''),
            'Min Rate': f"{pod_stat['min_rate_seconds']:.6f} s/s",
            'Latest Rate': f"{pod_stat['latest_rate_seconds']:.6f} s/s",
            'Data Points': str(pod_stat['data_points'])
        }

    def _format_sum_row(self, pod_stat: Dict[str, Any], is_top: bool, metric_title: str, role: str) -> Dict[str, str]:
        """Format cumulative duration sum row"""
        return {
            'Metric Name': metric_title,
            'Role': role.title(),
            'Pod': self.truncate_text(pod_stat['pod'], 45),
            'Avg Sum': f"{pod_stat['avg_sum_seconds']:.6f} s",
            'Max Sum': f"{pod_stat['max_sum_seconds']:.6f} s" + (' 🏆' if is_top else ''),
            'Min Sum': f"{pod_stat['min_sum_seconds']:.6f} s",
            'Latest Sum': f"{pod_stat['latest_sum_seconds']:.6f} s",
            'Data Points': str(pod_stat['data_points'])
        }

    def _format_count_rate_row(self, pod_stat: Dict[str, Any], is_top: bool, metric_title: str, role: str) -> Dict[str, str]:
        """Format operations rate row"""
        return {
            'Metric Name': metric_title,
            'Role': role.title(),
            'Pod': self.truncate_text(pod_stat['pod'], 45),
            'Avg Ops/sec': f"{pod_stat['avg_ops_per_sec']:.3f}",
            'Max Ops/sec': f"{pod_stat['max_ops_per_sec']:.3f}" + (' 🏆' if is_top else ''),
            'Min Ops/sec': f"{pod_stat['min_ops_per_sec']:.3f}",
            'Latest Ops/sec': f"{pod_stat['latest_ops_per_sec']:.3f}",
            'Data Points': str(pod_stat['data_points'])
        }

    def _format_count_row(self, pod_stat: Dict[str, Any], is_top: bool, metric_title: str, role: str) -> Dict[str, str]:
        """Format total operations count row"""
        return {
            'Metric Name': metric_title,
            'Role': role.title(),
            'Pod': self.truncate_text(pod_stat['pod'], 45),
            'Avg Count': self.format_count_value(pod_stat['avg_count']),
            'Max Count': self.format_count_value(pod_stat['max_count']) + (' 🏆' if is_top else ''),
            'Min Count': self.format_count_value(pod_stat['min_count']),
            'Latest Count': self.format_count_value(pod_stat['latest_count']),
            'Data Points': str(pod_stat['data_points'])
        }

    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by node roles"""
        try:
            html_tables = {}
            
            # Group tables by role
            for table_name, df in dataframes.items():
                if df.empty:
                    continue
                
                html_table = self.create_html_table(df, table_name)
                html_tables[table_name] = html_table
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for WAL fsync: {e}")
            return {}

    def summarize_wal_fsync(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary of WAL fsync performance"""
        try:
            if 'error' in structured_data:
                return f"<div class='alert alert-danger'>WAL fsync analysis failed: {structured_data['error']}</div>"
            
            collection_info = structured_data.get('collection_info', {})
            node_groups = structured_data.get('node_groups', {})
            
            summary_parts = []
            summary_parts.append(f"<h5>WAL Fsync Performance Analysis</h5>")
            summary_parts.append(f"<p><strong>Collection Period:</strong> {collection_info.get('duration', 'unknown')}</p>")
            summary_parts.append(f"<p><strong>Total Metrics:</strong> {collection_info.get('successful_metrics', 0)}/{collection_info.get('total_metrics', 0)}</p>")
            
            # Summary by node role
            for role, role_data in node_groups.items():
                metrics = role_data.get('metrics', {})
                if not metrics:
                    continue
                
                nodes = role_data.get('nodes', [])
                summary_parts.append(f"<p><strong>{role.title()} Nodes:</strong> {len(nodes)} nodes monitored</p>")
            
            return ''.join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to summarize WAL fsync: {e}")
            return f"<div class='alert alert-danger'>Summary generation failed: {str(e)}</div>"