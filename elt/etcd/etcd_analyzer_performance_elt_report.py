"""
etcd Analyzer Performance Report ELT Module
Converts performance analysis results into readable HTML tables
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Union
from datetime import datetime
import json

# Import the utility class for consistent formatting
from .etcd_analyzer_elt_utility import utilityELT


class etcdReportELT:
    """ELT module for converting performance report analysis to HTML tables"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.utility = utilityELT()
    
    def process_performance_report(self, analysis_results: Dict[str, Any]) -> Dict[str, str]:
        """
        Main method to process performance report analysis results into HTML tables
        
        Args:
            analysis_results: Dictionary containing the complete analysis results
            
        Returns:
            Dictionary with HTML table strings for each section
        """
        try:
            html_tables = {}
            
            # Get the nested analysis results - handle both direct and nested structures
            nested_results = analysis_results.get('analysis_results', analysis_results)
            
            # Process executive summary
            html_tables['executive_summary'] = self._create_executive_summary_table(analysis_results)
            
            # Process critical metrics analysis
            critical_analysis = nested_results.get('critical_metrics_analysis', {})
            if critical_analysis:
                html_tables['wal_fsync_analysis'] = self._create_wal_fsync_table(critical_analysis.get('wal_fsync_analysis', {}))
                html_tables['backend_commit_analysis'] = self._create_backend_commit_table(critical_analysis.get('backend_commit_analysis', {}))
            
            # Process performance summary
            performance_summary = nested_results.get('performance_summary', {})
            if performance_summary:
                html_tables['cpu_analysis'] = self._create_cpu_analysis_table(performance_summary.get('cpu_analysis', {}))
                html_tables['memory_analysis'] = self._create_memory_analysis_table(performance_summary.get('memory_analysis', {}))
                html_tables['network_analysis'] = self._create_network_analysis_table(performance_summary.get('network_analysis', {}))
                html_tables['disk_io_analysis'] = self._create_disk_io_table(performance_summary.get('disk_io_analysis', {}))
            
            # Process baseline comparison
            baseline_comparison = nested_results.get('baseline_comparison', {})
            if baseline_comparison:
                html_tables['baseline_comparison'] = self._create_baseline_comparison_table(baseline_comparison)
            
            # Process alerts
            alerts = nested_results.get('alerts', [])
            if alerts:
                html_tables['alerts'] = self._create_alerts_table(alerts)
            
            # Process recommendations
            recommendations = nested_results.get('recommendations', [])
            if recommendations:
                html_tables['recommendations'] = self._create_recommendations_table(recommendations)
            
            return html_tables
            
        except Exception as e:
            self.logger.error(f"Error processing performance report: {e}")
            return {'error': f'<div class="alert alert-danger">Error processing performance report: {str(e)}</div>'}
    
    def _create_executive_summary_table(self, analysis_results: Dict[str, Any]) -> str:
        """Create executive summary table"""
        try:
            # Get the nested analysis results
            nested_results = analysis_results.get('analysis_results', analysis_results)
            
            summary_data = []
            
            # Basic report info
            summary_data.append({
                'Property': 'Test ID',
                'Value': nested_results.get('test_id', analysis_results.get('test_id', 'Unknown'))
            })
            
            summary_data.append({
                'Property': 'Analysis Duration',
                'Value': nested_results.get('duration', analysis_results.get('duration', 'Unknown'))
            })
            
            summary_data.append({
                'Property': 'Report Generated',
                'Value': nested_results.get('timestamp', analysis_results.get('timestamp', 'Unknown'))
            })
            
            summary_data.append({
                'Property': 'Analysis Status',
                'Value': self._create_status_badge(nested_results.get('status', analysis_results.get('status', 'unknown')))
            })
            
            # Critical health indicators
            critical_analysis = nested_results.get('critical_metrics_analysis', {})
            overall_disk_health = critical_analysis.get('overall_disk_health', 'unknown')
            summary_data.append({
                'Property': 'Overall Disk Health',
                'Value': self._create_health_badge(overall_disk_health)
            })
            
            # Performance grade
            baseline_comparison = nested_results.get('baseline_comparison', {})
            performance_grade = baseline_comparison.get('performance_grade', 'unknown')
            summary_data.append({
                'Property': 'Performance Grade',
                'Value': self._create_performance_badge(performance_grade)
            })
            
            # Alert count
            alerts = nested_results.get('alerts', [])
            alert_count = len(alerts)
            critical_alerts = len([a for a in alerts if a.get('severity', '').lower() == 'critical'])
            
            summary_data.append({
                'Property': 'Active Alerts',
                'Value': self._create_alert_badge(alert_count, critical_alerts)
            })
            
            df = pd.DataFrame(summary_data)
            return self.utility.create_html_table(df, 'executive_summary')
            
        except Exception as e:
            self.logger.error(f"Error creating executive summary table: {e}")
            return f'<div class="alert alert-danger">Error creating executive summary: {str(e)}</div>'
    
    def _create_wal_fsync_table(self, wal_analysis: Dict[str, Any]) -> str:
        """Create WAL fsync analysis table"""
        try:
            if not wal_analysis:
                return '<div class="alert alert-info">No WAL fsync analysis data available</div>'
            
            pod_results = wal_analysis.get('pod_results', [])
            if not pod_results:
                return '<div class="alert alert-info">No WAL fsync pod data available</div>'
            
            table_data = []
            for pod in pod_results:
                # Truncate pod name for better display
                pod_name = self.utility.truncate_text(pod.get('pod_name', 'Unknown'), 40)
                
                # Format latency values with highlighting
                avg_ms = pod.get('avg_ms', 0)
                max_ms = pod.get('max_ms', 0)
                status = pod.get('status', 'unknown')
                
                avg_formatted = self._highlight_latency_value(avg_ms, 10.0, 'ms')
                max_formatted = self._highlight_latency_value(max_ms, 10.0, 'ms')
                
                table_data.append({
                    'Pod Name': pod_name,
                    'Average Latency': avg_formatted,
                    'Maximum Latency': max_formatted,
                    'Threshold Exceeded': self._create_boolean_badge(pod.get('avg_exceeds_threshold', False)),
                    'Status': self._create_status_badge(status)
                })
            
            # Add cluster summary row
            cluster_summary = wal_analysis.get('cluster_summary', {})
            if cluster_summary:
                table_data.append({
                    'Pod Name': '<strong>CLUSTER AVERAGE</strong>',
                    'Average Latency': f"<strong>{cluster_summary.get('avg_latency_ms', 0):.3f} ms</strong>",
                    'Maximum Latency': f"<strong>{cluster_summary.get('max_latency_ms', 0):.3f} ms</strong>",
                    'Threshold Exceeded': f"<strong>{cluster_summary.get('pods_with_issues', 0)}/{cluster_summary.get('total_pods', 0)} pods</strong>",
                    'Status': f"<strong>{self._create_health_badge(wal_analysis.get('health_status', 'unknown'))}</strong>"
                })
            
            df = pd.DataFrame(table_data)
            
            # Add threshold information as a header
            threshold = wal_analysis.get('threshold_ms', 10.0)
            header = f'<div class="alert alert-info mb-2"><strong>WAL Fsync P99 Latency Analysis</strong><br>Threshold: {threshold} ms</div>'
            
            return header + self.utility.create_html_table(df, 'wal_fsync_analysis')
            
        except Exception as e:
            self.logger.error(f"Error creating WAL fsync table: {e}")
            return f'<div class="alert alert-danger">Error creating WAL fsync table: {str(e)}</div>'
    
    def _create_backend_commit_table(self, backend_analysis: Dict[str, Any]) -> str:
        """Create backend commit analysis table"""
        try:
            if not backend_analysis:
                return '<div class="alert alert-info">No backend commit analysis data available</div>'
            
            pod_results = backend_analysis.get('pod_results', [])
            if not pod_results:
                return '<div class="alert alert-info">No backend commit pod data available</div>'
            
            table_data = []
            for pod in pod_results:
                pod_name = self.utility.truncate_text(pod.get('pod_name', 'Unknown'), 40)
                
                avg_ms = pod.get('avg_ms', 0)
                max_ms = pod.get('max_ms', 0)
                status = pod.get('status', 'unknown')
                
                avg_formatted = self._highlight_latency_value(avg_ms, 25.0, 'ms')
                max_formatted = self._highlight_latency_value(max_ms, 25.0, 'ms')
                
                table_data.append({
                    'Pod Name': pod_name,
                    'Average Latency': avg_formatted,
                    'Maximum Latency': max_formatted,
                    'Threshold Exceeded': self._create_boolean_badge(pod.get('avg_exceeds_threshold', False)),
                    'Status': self._create_status_badge(status)
                })
            
            # Add cluster summary
            cluster_summary = backend_analysis.get('cluster_summary', {})
            if cluster_summary:
                table_data.append({
                    'Pod Name': '<strong>CLUSTER AVERAGE</strong>',
                    'Average Latency': f"<strong>{cluster_summary.get('avg_latency_ms', 0):.3f} ms</strong>",
                    'Maximum Latency': f"<strong>{cluster_summary.get('max_latency_ms', 0):.3f} ms</strong>",
                    'Threshold Exceeded': f"<strong>{cluster_summary.get('pods_with_issues', 0)}/{cluster_summary.get('total_pods', 0)} pods</strong>",
                    'Status': f"<strong>{self._create_health_badge(backend_analysis.get('health_status', 'unknown'))}</strong>"
                })
            
            df = pd.DataFrame(table_data)
            
            threshold = backend_analysis.get('threshold_ms', 25.0)
            header = f'<div class="alert alert-info mb-2"><strong>Backend Commit P99 Latency Analysis</strong><br>Threshold: {threshold} ms</div>'
            
            return header + self.utility.create_html_table(df, 'backend_commit_analysis')
            
        except Exception as e:
            self.logger.error(f"Error creating backend commit table: {e}")
            return f'<div class="alert alert-danger">Error creating backend commit table: {str(e)}</div>'
    
    def _create_cpu_analysis_table(self, cpu_analysis: Dict[str, Any]) -> str:
        """Create CPU analysis table"""
        try:
            if not cpu_analysis:
                return '<div class="alert alert-info">No CPU analysis data available</div>'
            
            pod_results = cpu_analysis.get('pod_results', [])
            if not pod_results:
                return '<div class="alert alert-info">No CPU pod data available</div>'
            
            table_data = []
            for pod in pod_results:
                pod_name = self.utility.truncate_text(pod.get('pod_name', 'Unknown'), 40)
                
                avg_usage = pod.get('avg_usage', 0)
                max_usage = pod.get('max_usage', 0)
                status = pod.get('status', 'unknown')
                
                avg_formatted = self._highlight_percentage_value(avg_usage, 70.0, 85.0)
                max_formatted = self._highlight_percentage_value(max_usage, 70.0, 85.0)
                
                table_data.append({
                    'Pod Name': pod_name,
                    'Average Usage': avg_formatted,
                    'Maximum Usage': max_formatted,
                    'Status': self._create_status_badge(status)
                })
            
            # Add cluster summary
            cluster_summary = cpu_analysis.get('cluster_summary', {})
            if cluster_summary:
                table_data.append({
                    'Pod Name': '<strong>CLUSTER AVERAGE</strong>',
                    'Average Usage': f"<strong>{cluster_summary.get('avg_usage', 0):.2f}%</strong>",
                    'Maximum Usage': f"<strong>{cluster_summary.get('max_usage', 0):.2f}%</strong>",
                    'Status': f"<strong>{self._create_health_badge(cpu_analysis.get('health_status', 'unknown'))}</strong>"
                })
            
            df = pd.DataFrame(table_data)
            
            warning_threshold = cpu_analysis.get('warning_threshold', 70.0)
            critical_threshold = cpu_analysis.get('critical_threshold', 85.0)
            header = f'<div class="alert alert-info mb-2"><strong>CPU Usage Analysis</strong><br>Warning: {warning_threshold}% | Critical: {critical_threshold}%</div>'
            
            return header + self.utility.create_html_table(df, 'cpu_analysis')
            
        except Exception as e:
            self.logger.error(f"Error creating CPU analysis table: {e}")
            return f'<div class="alert alert-danger">Error creating CPU analysis table: {str(e)}</div>'
    
    def _create_memory_analysis_table(self, memory_analysis: Dict[str, Any]) -> str:
        """Create memory analysis table"""
        try:
            if not memory_analysis:
                return '<div class="alert alert-info">No memory analysis data available</div>'
            
            pod_results = memory_analysis.get('pod_results', [])
            if not pod_results:
                return '<div class="alert alert-info">No memory pod data available</div>'
            
            table_data = []
            for pod in pod_results:
                pod_name = self.utility.truncate_text(pod.get('pod_name', 'Unknown'), 40)
                
                avg_usage = pod.get('avg_usage', 0)
                max_usage = pod.get('max_usage', 0)
                unit = pod.get('unit', 'percent')
                status = pod.get('status', 'unknown')
                
                # Format based on unit
                if unit == 'percent':
                    avg_formatted = self._highlight_percentage_value(avg_usage, 70.0, 85.0)
                    max_formatted = self._highlight_percentage_value(max_usage, 70.0, 85.0)
                else:
                    # Assume MB and format accordingly
                    avg_formatted = self.utility.format_memory_display(f"{avg_usage}MB")
                    max_formatted = self.utility.format_memory_display(f"{max_usage}MB")
                
                table_data.append({
                    'Pod Name': pod_name,
                    'Average Usage': avg_formatted,
                    'Maximum Usage': max_formatted,
                    'Status': self._create_status_badge(status)
                })
            
            # Add cluster summary
            cluster_summary = memory_analysis.get('cluster_summary', {})
            if cluster_summary:
                avg_cluster = cluster_summary.get('avg_usage', 0)
                max_cluster = cluster_summary.get('max_usage', 0)
                
                if unit == 'percent':
                    avg_display = f"{avg_cluster:.2f}%"
                    max_display = f"{max_cluster:.2f}%"
                else:
                    avg_display = self.utility.format_memory_display(f"{avg_cluster}MB")
                    max_display = self.utility.format_memory_display(f"{max_cluster}MB")
                
                table_data.append({
                    'Pod Name': '<strong>CLUSTER AVERAGE</strong>',
                    'Average Usage': f"<strong>{avg_display}</strong>",
                    'Maximum Usage': f"<strong>{max_display}</strong>",
                    'Status': f"<strong>{self._create_health_badge(memory_analysis.get('health_status', 'unknown'))}</strong>"
                })
            
            df = pd.DataFrame(table_data)
            
            warning_threshold = memory_analysis.get('warning_threshold', 70.0)
            critical_threshold = memory_analysis.get('critical_threshold', 85.0)
            header = f'<div class="alert alert-info mb-2"><strong>Memory Usage Analysis</strong><br>Warning: {warning_threshold}% | Critical: {critical_threshold}%</div>'
            
            return header + self.utility.create_html_table(df, 'memory_analysis')
            
        except Exception as e:
            self.logger.error(f"Error creating memory analysis table: {e}")
            return f'<div class="alert alert-danger">Error creating memory analysis table: {str(e)}</div>'
    
    def _create_network_analysis_table(self, network_analysis: Dict[str, Any]) -> str:
        """Create network analysis table"""
        try:
            if not network_analysis:
                return '<div class="alert alert-info">No network analysis data available</div>'
            
            html_sections = []
            
            # Peer latency analysis
            peer_latency = network_analysis.get('peer_latency_analysis', {})
            if peer_latency:
                html_sections.append(self._create_peer_latency_table(peer_latency))
            
            # Network utilization analysis
            network_util = network_analysis.get('network_utilization_analysis', {})
            if network_util:
                html_sections.append(self._create_network_utilization_table(network_util))
            
            # Packet drop analysis
            packet_drops = network_analysis.get('packet_drop_analysis', {})
            if packet_drops:
                html_sections.append(self._create_packet_drop_table(packet_drops))
            
            # Overall network health summary
            overall_health = network_analysis.get('health_status', 'unknown')
            header = f'<div class="alert alert-info mb-2"><strong>Network Performance Analysis</strong><br>Overall Health: {self._create_health_badge(overall_health)}</div>'
            
            return header + ''.join(html_sections)
            
        except Exception as e:
            self.logger.error(f"Error creating network analysis table: {e}")
            return f'<div class="alert alert-danger">Error creating network analysis table: {str(e)}</div>'
    
    def _create_peer_latency_table(self, peer_latency: Dict[str, Any]) -> str:
        """Create peer latency table"""
        try:
            pod_results = peer_latency.get('pod_results', [])
            if not pod_results:
                return ''
            
            table_data = []
            for pod in pod_results:
                pod_name = self.utility.truncate_text(pod.get('pod_name', 'Unknown'), 40)
                
                avg_ms = pod.get('avg_ms', 0)
                max_ms = pod.get('max_ms', 0)
                status = pod.get('status', 'unknown')
                
                avg_formatted = self._highlight_latency_value(avg_ms, 50.0, 'ms')
                max_formatted = self._highlight_latency_value(max_ms, 100.0, 'ms')
                
                table_data.append({
                    'Pod Name': pod_name,
                    'Average Latency': avg_formatted,
                    'Maximum Latency': max_formatted,
                    'Status': self._create_status_badge(status)
                })
            
            # Add cluster summary
            cluster_summary = peer_latency.get('cluster_summary', {})
            if cluster_summary:
                table_data.append({
                    'Pod Name': '<strong>CLUSTER AVERAGE</strong>',
                    'Average Latency': f"<strong>{cluster_summary.get('avg_latency_ms', 0):.3f} ms</strong>",
                    'Maximum Latency': f"<strong>{cluster_summary.get('max_latency_ms', 0):.3f} ms</strong>",
                    'Status': f"<strong>{self._create_health_badge(peer_latency.get('health_status', 'unknown'))}</strong>"
                })
            
            df = pd.DataFrame(table_data)
            
            threshold = peer_latency.get('threshold_ms', 100.0)
            subheader = f'<h5>Peer-to-Peer Latency (Threshold: {threshold} ms)</h5>'
            
            return subheader + self.utility.create_html_table(df, 'peer_latency')
            
        except Exception as e:
            self.logger.error(f"Error creating peer latency table: {e}")
            return f'<div class="alert alert-warning">Error creating peer latency table: {str(e)}</div>'
    
    def _create_network_utilization_table(self, network_util: Dict[str, Any]) -> str:
        """Create network utilization table"""
        try:
            node_results = network_util.get('node_results', [])
            if not node_results:
                return ''
            
            table_data = []
            for node in node_results:
                node_name = self.utility.truncate_text(node.get('node_name', 'Unknown'), 40)
                
                avg_util = node.get('avg_utilization_percent', 0)
                max_util = node.get('max_utilization_percent', 0)
                status = node.get('status', 'unknown')
                
                avg_formatted = self._highlight_percentage_value(avg_util, 70.0, 85.0)
                max_formatted = self._highlight_percentage_value(max_util, 70.0, 85.0)
                
                table_data.append({
                    'Node Name': node_name,
                    'Average Utilization': avg_formatted,
                    'Maximum Utilization': max_formatted,
                    'Status': self._create_status_badge(status)
                })
            
            df = pd.DataFrame(table_data)
            
            subheader = '<h5>Network Utilization Analysis</h5>'
            return subheader + self.utility.create_html_table(df, 'network_utilization')
            
        except Exception as e:
            self.logger.error(f"Error creating network utilization table: {e}")
            return f'<div class="alert alert-warning">Error creating network utilization table: {str(e)}</div>'
    
    def _create_packet_drop_table(self, packet_drops: Dict[str, Any]) -> str:
        """Create packet drop table"""
        try:
            node_results = packet_drops.get('node_results', [])
            if not node_results:
                return ''
            
            table_data = []
            for node in node_results:
                node_name = self.utility.truncate_text(node.get('node_name', 'Unknown'), 40)
                
                avg_drops = node.get('avg_drops_per_sec', 0)
                max_drops = node.get('max_drops_per_sec', 0)
                status = node.get('status', 'unknown')
                
                avg_formatted = f"{avg_drops:.6f}/s" if avg_drops > 0 else "0/s"
                max_formatted = f"{max_drops:.6f}/s" if max_drops > 0 else "0/s"
                
                # Highlight if there are drops
                if avg_drops > 0:
                    avg_formatted = f'<span class="text-warning">{avg_formatted}</span>'
                if max_drops > 0:
                    max_formatted = f'<span class="text-warning">{max_formatted}</span>'
                
                table_data.append({
                    'Node Name': node_name,
                    'Average Drops': avg_formatted,
                    'Maximum Drops': max_formatted,
                    'Status': self._create_status_badge(status)
                })
            
            df = pd.DataFrame(table_data)
            
            subheader = '<h5>Packet Drop Analysis</h5>'
            return subheader + self.utility.create_html_table(df, 'packet_drops')
            
        except Exception as e:
            self.logger.error(f"Error creating packet drop table: {e}")
            return f'<div class="alert alert-warning">Error creating packet drop table: {str(e)}</div>'
    
    def _create_disk_io_table(self, disk_io_analysis: Dict[str, Any]) -> str:
        """Create disk I/O analysis table"""
        try:
            if not disk_io_analysis:
                return '<div class="alert alert-info">No disk I/O analysis data available</div>'
            
            html_sections = []
            
            # Throughput analysis
            throughput_analysis = disk_io_analysis.get('throughput_analysis', [])
            if throughput_analysis:
                html_sections.append(self._create_disk_throughput_table(throughput_analysis))
            
            # IOPS analysis
            iops_analysis = disk_io_analysis.get('iops_analysis', [])
            if iops_analysis:
                html_sections.append(self._create_disk_iops_table(iops_analysis))
            
            # Overall health
            health_status = disk_io_analysis.get('health_status', 'unknown')
            header = f'<div class="alert alert-info mb-2"><strong>Disk I/O Performance Analysis</strong><br>Health Status: {self._create_health_badge(health_status)}</div>'
            
            return header + ''.join(html_sections)
            
        except Exception as e:
            self.logger.error(f"Error creating disk I/O table: {e}")
            return f'<div class="alert alert-danger">Error creating disk I/O table: {str(e)}</div>'
    
    def _create_disk_throughput_table(self, throughput_data: List[Dict[str, Any]]) -> str:
        """Create disk throughput table"""
        try:
            if not throughput_data:
                return ''
            
            table_data = []
            for entry in throughput_data:
                node_name = self.utility.truncate_text(entry.get('node_name', 'Unknown'), 40)
                metric_type = entry.get('metric_type', 'unknown')
                avg_mb_s = entry.get('avg_mb_per_sec', 0)
                max_mb_s = entry.get('max_mb_per_sec', 0)
                devices = ', '.join(entry.get('devices', []))
                
                # Determine if this is read or write
                operation = 'Write' if 'write' in metric_type.lower() else 'Read'
                
                # Highlight low throughput values
                avg_formatted = self._highlight_throughput_value(avg_mb_s)
                max_formatted = self._highlight_throughput_value(max_mb_s)
                
                table_data.append({
                    'Node Name': node_name,
                    'Operation': operation,
                    'Avg Throughput': avg_formatted,
                    'Max Throughput': max_formatted,
                    'Devices': devices
                })
            
            df = pd.DataFrame(table_data)
            
            subheader = '<h5>Disk Throughput Analysis</h5>'
            return subheader + self.utility.create_html_table(df, 'disk_throughput')
            
        except Exception as e:
            self.logger.error(f"Error creating disk throughput table: {e}")
            return f'<div class="alert alert-warning">Error creating disk throughput table: {str(e)}</div>'
    
    def _create_disk_iops_table(self, iops_data: List[Dict[str, Any]]) -> str:
        """Create disk IOPS table"""
        try:
            if not iops_data:
                return ''
            
            table_data = []
            for entry in iops_data:
                node_name = self.utility.truncate_text(entry.get('node_name', 'Unknown'), 40)
                metric_type = entry.get('metric_type', 'unknown')
                avg_iops = entry.get('avg_iops', 0)
                max_iops = entry.get('max_iops', 0)
                devices = ', '.join(entry.get('devices', []))
                
                # Determine if this is read or write
                operation = 'Write' if 'write' in metric_type.lower() else 'Read'
                
                # Highlight low IOPS values (especially for writes)
                avg_formatted = self._highlight_iops_value(avg_iops, operation)
                max_formatted = self._highlight_iops_value(max_iops, operation)
                
                table_data.append({
                    'Node Name': node_name,
                    'Operation': operation,
                    'Avg IOPS': avg_formatted,
                    'Max IOPS': max_formatted,
                    'Devices': devices
                })
            
            df = pd.DataFrame(table_data)
            
            subheader = '<h5>Disk IOPS Analysis</h5>'
            return subheader + self.utility.create_html_table(df, 'disk_iops')
            
        except Exception as e:
            self.logger.error(f"Error creating disk IOPS table: {e}")
            return f'<div class="alert alert-warning">Error creating disk IOPS table: {str(e)}</div>'
    
    def _create_baseline_comparison_table(self, baseline_comparison: Dict[str, Any]) -> str:
        """Create baseline comparison table"""
        try:
            current_vs_baseline = baseline_comparison.get('current_vs_baseline', {})
            if not current_vs_baseline:
                return '<div class="alert alert-info">No baseline comparison data available</div>'
            
            table_data = []
            for metric, comparison in current_vs_baseline.items():
                # Format metric name for display
                display_name = metric.replace('_', ' ').title()
                
                current = comparison.get('current', 'N/A')
                target = comparison.get('target', 'N/A')
                within_target = comparison.get('within_target', False)
                
                # Format current and target values
                if isinstance(current, (int, float)):
                    if 'latency' in metric or 'ms' in metric:
                        current_formatted = f"{current:.3f} ms"
                        target_formatted = f"{target:.1f} ms"
                    elif 'percent' in metric:
                        current_formatted = f"{current:.2f}%"
                        target_formatted = f"{target:.1f}%"
                    else:
                        current_formatted = f"{current:.2f}"
                        target_formatted = f"{target:.1f}"
                else:
                    current_formatted = str(current)
                    target_formatted = str(target)
                
                # Create status badge
                status_badge = self._create_boolean_badge(within_target, 'PASS', 'FAIL')
                
                # Highlight values that exceed targets
                if not within_target and isinstance(current, (int, float)):
                    current_formatted = f'<span class="text-danger font-weight-bold">{current_formatted}</span>'
                
                table_data.append({
                    'Metric': display_name,
                    'Current Value': current_formatted,
                    'Target Value': target_formatted,
                    'Status': status_badge
                })
            
            df = pd.DataFrame(table_data)
            
            # Add performance grade
            performance_grade = baseline_comparison.get('performance_grade', 'unknown')
            benchmark_standards = baseline_comparison.get('benchmark_standards', {})
            
            header = f'<div class="alert alert-info mb-2"><strong>Baseline Comparison Analysis</strong><br>'
            header += f'Overall Performance Grade: {self._create_performance_badge(performance_grade)}'
            
            if benchmark_standards:
                header += '<br><small>Industry Standard Thresholds: '
                for key, value in benchmark_standards.items():
                    name = key.replace('_', ' ').title()
                    if 'ms' in key:
                        header += f'{name}: {value}ms | '
                    elif 'percent' in key:
                        header += f'{name}: {value}% | '
                    else:
                        header += f'{name}: {value} | '
                header = header.rstrip(' | ') + '</small>'
            
            header += '</div>'
            
            return header + self.utility.create_html_table(df, 'baseline_comparison')
            
        except Exception as e:
            self.logger.error(f"Error creating baseline comparison table: {e}")
            return f'<div class="alert alert-danger">Error creating baseline comparison table: {str(e)}</div>'
    
    def _create_alerts_table(self, alerts: List[Dict[str, Any]]) -> str:
        """Create alerts table"""
        try:
            if not alerts:
                return '<div class="alert alert-success">No active alerts</div>'
            
            table_data = []
            for alert in alerts:
                severity = alert.get('severity', 'unknown')
                category = alert.get('category', 'unknown')
                message = alert.get('message', 'No message')
                impact = alert.get('impact', 'Unknown impact')
                action_required = alert.get('action_required', 'No action specified')
                
                # Truncate long messages for table display
                message_short = self.utility.truncate_text(message, 60)
                impact_short = self.utility.truncate_text(impact, 50)
                action_short = self.utility.truncate_text(action_required, 50)
                
                table_data.append({
                    'Severity': self._create_severity_badge(severity),
                    'Category': category.replace('_', ' ').title(),
                    'Alert Message': message_short,
                    'Impact': impact_short,
                    'Action Required': action_short
                })
            
            df = pd.DataFrame(table_data)
            
            # Count alerts by severity
            critical_count = len([a for a in alerts if a.get('severity', '').lower() == 'critical'])
            warning_count = len([a for a in alerts if a.get('severity', '').lower() == 'warning'])
            
            header = f'<div class="alert alert-warning mb-2"><strong>Active Performance Alerts</strong><br>'
            header += f'Total: {len(alerts)} | Critical: {critical_count} | Warning: {warning_count}</div>'
            
            return header + self.utility.create_html_table(df, 'alerts')
            
        except Exception as e:
            self.logger.error(f"Error creating alerts table: {e}")
            return f'<div class="alert alert-danger">Error creating alerts table: {str(e)}</div>'
    
    def _create_recommendations_table(self, recommendations: List[Dict[str, Any]]) -> str:
        """Create recommendations table"""
        try:
            if not recommendations:
                return '<div class="alert alert-info">No recommendations available</div>'
            
            # Group recommendations by priority
            high_priority = [r for r in recommendations if r.get('priority', '').lower() == 'high']
            medium_priority = [r for r in recommendations if r.get('priority', '').lower() == 'medium']
            low_priority = [r for r in recommendations if r.get('priority', '').lower() == 'low']
            
            html_sections = []
            
            # Create table for each priority level
            for priority_name, priority_recs in [('High Priority', high_priority), 
                                               ('Medium Priority', medium_priority), 
                                               ('Low Priority', low_priority)]:
                if not priority_recs:
                    continue
                
                table_data = []
                for i, rec in enumerate(priority_recs, 1):
                    category = rec.get('category', 'unknown')
                    issue = rec.get('issue', 'No issue specified')
                    recommendation = rec.get('recommendation', 'No recommendation')
                    rationale = rec.get('rationale', 'No rationale provided')
                    
                    # Truncate for display
                    issue_short = self.utility.truncate_text(issue, 50)
                    recommendation_short = self.utility.truncate_text(recommendation, 60)
                    rationale_short = self.utility.truncate_text(rationale, 50)
                    
                    table_data.append({
                        '#': i,
                        'Category': category.replace('_', ' ').title(),
                        'Issue': issue_short,
                        'Recommendation': recommendation_short,
                        'Rationale': rationale_short
                    })
                
                df = pd.DataFrame(table_data)
                
                priority_color = 'danger' if priority_name == 'High Priority' else ('warning' if priority_name == 'Medium Priority' else 'info')
                subheader = f'<h5><span class="badge badge-{priority_color}">{priority_name} ({len(priority_recs)} items)</span></h5>'
                
                html_sections.append(subheader + self.utility.create_html_table(df, f'recommendations_{priority_name.lower().replace(" ", "_")}'))
            
            header = f'<div class="alert alert-info mb-2"><strong>Performance Optimization Recommendations</strong><br>'
            header += f'Total: {len(recommendations)} | High: {len(high_priority)} | Medium: {len(medium_priority)} | Low: {len(low_priority)}</div>'
            
            return header + ''.join(html_sections)
            
        except Exception as e:
            self.logger.error(f"Error creating recommendations table: {e}")
            return f'<div class="alert alert-danger">Error creating recommendations table: {str(e)}</div>'
    
    # Helper methods for formatting and styling
    
    def _create_status_badge(self, status: str) -> str:
        """Create status badge with appropriate color"""
        status_lower = status.lower()
        
        if status_lower in ['good', 'excellent', 'healthy']:
            return f'<span class="badge badge-success">{status.title()}</span>'
        elif status_lower in ['warning', 'degraded']:
            return f'<span class="badge badge-warning">{status.title()}</span>'
        elif status_lower in ['critical', 'unhealthy', 'failed']:
            return f'<span class="badge badge-danger">{status.title()}</span>'
        else:
            return f'<span class="badge badge-secondary">{status.title()}</span>'
    
    def _create_health_badge(self, health_status: str) -> str:
        """Create health status badge"""
        health_lower = health_status.lower()
        
        if health_lower in ['excellent', 'good', 'healthy']:
            return f'<span class="badge badge-success">✓ {health_status.title()}</span>'
        elif health_lower in ['warning', 'degraded']:
            return f'<span class="badge badge-warning">⚠ {health_status.title()}</span>'
        elif health_lower in ['critical', 'unhealthy']:
            return f'<span class="badge badge-danger">✗ {health_status.title()}</span>'
        else:
            return f'<span class="badge badge-secondary">{health_status.title()}</span>'
    
    def _create_performance_badge(self, grade: str) -> str:
        """Create performance grade badge"""
        grade_lower = grade.lower()
        
        if grade_lower == 'excellent':
            return f'<span class="badge badge-success">A+ {grade.title()}</span>'
        elif grade_lower == 'good':
            return f'<span class="badge badge-success">B+ {grade.title()}</span>'
        elif grade_lower == 'fair':
            return f'<span class="badge badge-warning">C {grade.title()}</span>'
        elif grade_lower == 'poor':
            return f'<span class="badge badge-danger">D {grade.title()}</span>'
        else:
            return f'<span class="badge badge-secondary">{grade.title()}</span>'
    
    def _create_alert_badge(self, total_count: int, critical_count: int) -> str:
        """Create alert count badge"""
        if total_count == 0:
            return '<span class="badge badge-success">✓ No Alerts</span>'
        elif critical_count > 0:
            return f'<span class="badge badge-danger">🔥 {total_count} ({critical_count} Critical)</span>'
        else:
            return f'<span class="badge badge-warning">⚠ {total_count} Alerts</span>'
    
    def _create_boolean_badge(self, condition: bool, true_text: str = 'Yes', false_text: str = 'No') -> str:
        """Create boolean badge"""
        if condition:
            return f'<span class="badge badge-success">{true_text}</span>'
        else:
            return f'<span class="badge badge-danger">{false_text}</span>'
    
    def _create_severity_badge(self, severity: str) -> str:
        """Create severity badge"""
        severity_lower = severity.lower()
        
        if severity_lower == 'critical':
            return f'<span class="badge badge-danger">🔥 CRITICAL</span>'
        elif severity_lower == 'warning':
            return f'<span class="badge badge-warning">⚠ WARNING</span>'
        elif severity_lower == 'info':
            return f'<span class="badge badge-info">ℹ INFO</span>'
        else:
            return f'<span class="badge badge-secondary">{severity.upper()}</span>'
    
    def _highlight_latency_value(self, value: float, threshold: float, unit: str) -> str:
        """Highlight latency values based on threshold"""
        formatted_value = f"{value:.3f} {unit}"
        
        if value > threshold * 1.5:  # 150% of threshold
            return f'<span class="text-danger font-weight-bold">🔥 {formatted_value}</span>'
        elif value > threshold:
            return f'<span class="text-warning font-weight-bold">⚠ {formatted_value}</span>'
        else:
            return f'<span class="text-success">{formatted_value}</span>'
    
    def _highlight_percentage_value(self, value: float, warning_threshold: float, critical_threshold: float) -> str:
        """Highlight percentage values based on thresholds"""
        formatted_value = f"{value:.2f}%"
        
        if value >= critical_threshold:
            return f'<span class="text-danger font-weight-bold">🔥 {formatted_value}</span>'
        elif value >= warning_threshold:
            return f'<span class="text-warning font-weight-bold">⚠ {formatted_value}</span>'
        else:
            return f'<span class="text-success">{formatted_value}</span>'
    
    def _highlight_throughput_value(self, value: float) -> str:
        """Highlight throughput values (MB/s)"""
        formatted_value = f"{value:.2f} MB/s"
        
        if value < 20:  # Very low throughput
            return f'<span class="text-danger font-weight-bold">⚠ {formatted_value}</span>'
        elif value < 50:  # Low throughput
            return f'<span class="text-warning">{formatted_value}</span>'
        else:
            return f'<span class="text-success">{formatted_value}</span>'
    
    def _highlight_iops_value(self, value: float, operation: str) -> str:
        """Highlight IOPS values"""
        formatted_value = f"{value:.1f} IOPS"
        
        # etcd typically needs 1000+ write IOPS
        if operation.lower() == 'write':
            if value < 500:
                return f'<span class="text-danger font-weight-bold">🔥 {formatted_value}</span>'
            elif value < 1000:
                return f'<span class="text-warning font-weight-bold">⚠ {formatted_value}</span>'
            else:
                return f'<span class="text-success">{formatted_value}</span>'
        else:
            # Read IOPS thresholds are generally lower
            if value < 100:
                return f'<span class="text-warning">{formatted_value}</span>'
            else:
                return f'<span class="text-success">{formatted_value}</span>'


# Example usage and testing function
def main():
    """Test function for the etcdReportELT class"""
    
    # Sample data structure matching the actual JSON output structure from the document
    sample_analysis_results = {
        "status": "success",
        "analysis_results": {
            "test_id": "perf-report-20250925-063124",
            "timestamp": "2025-09-25T06:31:39.163680+00:00",
            "duration": "1h",
            "status": "success",
            "critical_metrics_analysis": {
                "wal_fsync_analysis": {
                    "metric_type": "wal_fsync_p99",
                    "threshold_ms": 10.0,
                    "pod_results": [
                        {
                            "pod_name": "etcd-ci-op-b3nspgj9-0af14-twjwv-master-0",
                            "avg_ms": 7.997,
                            "max_ms": 9.539,
                            "avg_exceeds_threshold": False,
                            "max_exceeds_threshold": False,
                            "status": "good"
                        },
                        {
                            "pod_name": "etcd-ci-op-b3nspgj9-0af14-twjwv-master-1",
                            "avg_ms": 9.719,
                            "max_ms": 13.11,
                            "avg_exceeds_threshold": False,
                            "max_exceeds_threshold": True,
                            "status": "warning"
                        },
                        {
                            "pod_name": "etcd-ci-op-b3nspgj9-0af14-twjwv-master-2",
                            "avg_ms": 12.991,
                            "max_ms": 15.599,
                            "avg_exceeds_threshold": True,
                            "max_exceeds_threshold": True,
                            "status": "critical"
                        }
                    ],
                    "cluster_summary": {
                        "avg_latency_ms": 10.236,
                        "max_latency_ms": 15.599,
                        "pods_with_issues": 2,
                        "total_pods": 3,
                        "threshold_exceeded": True
                    },
                    "health_status": "critical"
                },
                "backend_commit_analysis": {
                    "metric_type": "backend_commit_p99",
                    "threshold_ms": 25.0,
                    "pod_results": [
                        {
                            "pod_name": "etcd-ci-op-b3nspgj9-0af14-twjwv-master-0",
                            "avg_ms": 14.058,
                            "max_ms": 15.246,
                            "avg_exceeds_threshold": False,
                            "max_exceeds_threshold": False,
                            "status": "good"
                        }
                    ],
                    "cluster_summary": {
                        "avg_latency_ms": 15.174,
                        "max_latency_ms": 25.942,
                        "pods_with_issues": 1,
                        "total_pods": 3,
                        "threshold_exceeded": True
                    },
                    "health_status": "warning"
                },
                "overall_disk_health": "critical"
            },
            "performance_summary": {
                "cpu_analysis": {
                    "resource_type": "cpu",
                    "warning_threshold": 70.0,
                    "critical_threshold": 85.0,
                    "pod_results": [
                        {
                            "pod_name": "etcd-ci-op-b3nspgj9-0af14-twjwv-master-0",
                            "avg_usage": 6.5,
                            "max_usage": 10.06,
                            "unit": "percent",
                            "status": "good"
                        }
                    ],
                    "cluster_summary": {
                        "avg_usage": 5.66,
                        "max_usage": 10.06,
                        "critical_pods": 0,
                        "warning_pods": 0,
                        "total_pods": 3
                    },
                    "health_status": "good"
                },
                "network_analysis": {
                    "peer_latency_analysis": {
                        "metric_type": "peer_latency",
                        "threshold_ms": 100.0,
                        "pod_results": [
                            {
                                "pod_name": "etcd-ci-op-b3nspgj9-0af14-twjwv-master-0",
                                "avg_ms": 12.672,
                                "max_ms": 12.672,
                                "avg_exceeds_threshold": False,
                                "max_exceeds_threshold": False,
                                "status": "good"
                            }
                        ],
                        "cluster_summary": {
                            "avg_latency_ms": 12.671,
                            "max_latency_ms": 12.672,
                            "pods_with_issues": 0,
                            "total_pods": 3,
                            "threshold_exceeded": False
                        },
                        "health_status": "excellent"
                    },
                    "health_status": "good"
                }
            },
            "baseline_comparison": {
                "current_vs_baseline": {
                    "wal_fsync_p99_ms": {
                        "current": 10.236,
                        "target": 10.0,
                        "within_target": False
                    },
                    "cpu_usage_percent": {
                        "current": 5.66,
                        "target": 70.0,
                        "within_target": True
                    }
                },
                "performance_grade": "good"
            },
            "alerts": [
                {
                    "severity": "critical",
                    "category": "disk_performance",
                    "message": "WAL fsync latency exceeds critical threshold (>10ms)",
                    "impact": "High write latency affecting cluster stability",
                    "action_required": "Immediate storage performance investigation required"
                }
            ],
            "recommendations": [
                {
                    "category": "disk_performance",
                    "priority": "high",
                    "issue": "High WAL fsync latency detected",
                    "recommendation": "Upgrade to high-performance NVMe SSDs with low latency",
                    "rationale": "Ensure etcd gets adequate CPU resources over other processes"
                }
            ]
        },
        "timestamp": "2025-09-25T06:31:39.163680+00:00",
        "duration": "1h",
        "test_id": "perf-report-20250925-063124"
    }
    
    # Test the ELT processor
    elt_processor = etcdReportELT()
    html_tables = elt_processor.process_performance_report(sample_analysis_results)
    
    # Display sample outputs
    print("etcd Performance Report ELT - Sample HTML Tables")
    print("=" * 60)
    
    for section_name, html_content in html_tables.items():
        print(f"\n--- {section_name.upper()} ---")
        print(html_content[:500] + "..." if len(html_content) > 500 else html_content)
    
    return html_tables


if __name__ == "__main__":
    main()