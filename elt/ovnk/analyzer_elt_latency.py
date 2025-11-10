"""
Extract, Load, Transform module for OVN-Kubernetes Latency Metrics
Handles latency data from tools/ovnk/ovnk_latency.py
ONLY contains OVN latency specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class ovnkLatencyELT(utilityELT):
    """Extract, Load, Transform class for OVN-Kubernetes latency metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'cni_request_add_latency_p99': {
                'title': 'CNI ADD Request Latency (P99)',
                'unit': 'seconds',
                'description': '99th percentile latency for CNI ADD requests',
                'thresholds': {'critical': 0.1, 'warning': 0.05}  # 100ms, 50ms
            },
            'cni_request_del_latency_p99': {
                'title': 'CNI DEL Request Latency (P99)',
                'unit': 'seconds',
                'description': '99th percentile latency for CNI DEL requests',
                'thresholds': {'critical': 0.1, 'warning': 0.05}
            },
            'pod_annotation_latency_p99': {
                'title': 'Pod Annotation Latency (P99)',
                'unit': 'seconds',
                'description': '99th percentile latency for pod annotation processing',
                'thresholds': {'critical': 0.2, 'warning': 0.1}
            },
            'pod_first_seen_lsp_created_p99': {
                'title': 'Pod First Seen to LSP Created (P99)',
                'unit': 'seconds',
                'description': '99th percentile latency from pod first seen to LSP created',
                'thresholds': {'critical': 0.05, 'warning': 0.02}
            },
            'pod_lsp_created_p99': {
                'title': 'Pod LSP Creation to Port Binding (P99)',
                'unit': 'seconds',
                'description': '99th percentile latency for pod LSP creation to port binding',
                'thresholds': {'critical': 0.02, 'warning': 0.01}
            },
            'pod_port_binding_p99': {
                'title': 'Pod Port Binding to Chassis Binding (P99)',
                'unit': 'seconds',
                'description': '99th percentile latency for pod port binding to chassis binding',
                'thresholds': {'critical': 1.0, 'warning': 0.5}
            },
            'pod_port_binding_up_p99': {
                'title': 'Pod Chassis Binding to Port Up (P99)',
                'unit': 'seconds',
                'description': '99th percentile latency for pod chassis binding to port up',
                'thresholds': {'critical': 0.1, 'warning': 0.05}
            },
            'sync_service_latency': {
                'title': 'Service Sync Latency (Avg)',
                'unit': 'seconds',
                'description': 'Average service synchronization latency',
                'thresholds': {'critical': 0.2, 'warning': 0.1}
            },
            'sync_service_latency_p99': {
                'title': 'Service Sync Latency (P99)',
                'unit': 'seconds',
                'description': '99th percentile service synchronization latency',
                'thresholds': {'critical': 0.5, 'warning': 0.2}
            },
            'apply_network_config_pod_duration_p99': {
                'title': 'Apply Network Config to Pod (P99)',
                'unit': 'seconds',
                'description': '99th percentile latency for applying network configuration to pods',
                'thresholds': {'critical': 0.5, 'warning': 0.2}
            },
            'apply_network_config_service_duration_p99': {
                'title': 'Apply Network Config to Service (P99)',
                'unit': 'seconds',
                'description': '99th percentile latency for applying network configuration to services',
                'thresholds': {'critical': 0.5, 'warning': 0.2}
            },
            'ovnkube_controller_ready_duration_seconds': {
                'title': 'OVNKube Controller Ready Duration',
                'unit': 'seconds',
                'description': 'Time taken for ovnkube-controller to become ready',
                'thresholds': {'critical': 5.0, 'warning': 2.0}
            },
            'ovnkube_node_ready_duration_seconds': {
                'title': 'OVNKube Node Ready Duration',
                'unit': 'seconds',
                'description': 'Time taken for ovnkube-node to become ready',
                'thresholds': {'critical': 5.0, 'warning': 3.0}
            },
            'ovnkube_controller_sync_duration_seconds': {
                'title': 'OVNKube Controller Sync Duration',
                'unit': 'seconds',
                'description': 'Duration of controller sync operations',
                'thresholds': {'critical': 1.0, 'warning': 0.5}
            },
            'ovnkube_controller_sync_duration_p95': {
                'title': 'OVNKube Controller Sync Duration (P95)',
                'unit': 'seconds',
                'description': '95th percentile of controller sync duration',
                'thresholds': {'critical': 1.0, 'warning': 0.5}
            }
        }
    
    def extract_ovn_latency(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVN latency information from ovnk_latency.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'ovn_latency_overview': [],
        }
        
        # Extract metadata
        metadata = {
            'timestamp': actual_data.get('timestamp', 'Unknown'),
            'duration': actual_data.get('duration', 'Unknown'),
            'category': actual_data.get('category', 'latency'),
            'timezone': actual_data.get('timezone', 'UTC')
        }
        structured['metadata'] = [metadata]
        
        # Extract summary if present
        summary = actual_data.get('summary', {})
        if summary:
            structured['summary'] = [{
                'Total Metrics': summary.get('total_metrics', 0),
                'Successful': summary.get('successful_metrics', 0),
                'Failed': summary.get('failed_metrics', 0)
            }]
        
        # Get metrics data
        metrics = actual_data.get('metrics', {})
        
        # Extract each metric
        for metric_name, metric_data in metrics.items():
            if metric_data.get('status') != 'success':
                continue
            
            # Create table for this metric
            table_key = f'latency_{metric_name}'
            structured[table_key] = []
            
            # Extract metric-specific data
            if metric_name == 'cni_request_add_latency_p99':
                self._extract_cni_add_latency(metric_data, structured, table_key)
            elif metric_name == 'cni_request_del_latency_p99':
                self._extract_cni_del_latency(metric_data, structured, table_key)
            elif metric_name == 'pod_annotation_latency_p99':
                self._extract_pod_annotation_latency(metric_data, structured, table_key)
            elif metric_name == 'pod_first_seen_lsp_created_p99':
                self._extract_pod_first_seen_lsp(metric_data, structured, table_key)
            elif metric_name == 'pod_lsp_created_p99':
                self._extract_pod_lsp_created(metric_data, structured, table_key)
            elif metric_name == 'pod_port_binding_p99':
                self._extract_pod_port_binding(metric_data, structured, table_key)
            elif metric_name == 'pod_port_binding_up_p99':
                self._extract_pod_port_binding_up(metric_data, structured, table_key)
            elif metric_name == 'sync_service_latency':
                self._extract_sync_service_latency(metric_data, structured, table_key)
            elif metric_name == 'sync_service_latency_p99':
                self._extract_sync_service_latency_p99(metric_data, structured, table_key)
            elif metric_name == 'apply_network_config_pod_duration_p99':
                self._extract_network_config_pod(metric_data, structured, table_key)
            elif metric_name == 'apply_network_config_service_duration_p99':
                self._extract_network_config_service(metric_data, structured, table_key)
            elif metric_name == 'ovnkube_controller_ready_duration_seconds':
                self._extract_controller_ready(metric_data, structured, table_key)
            elif metric_name == 'ovnkube_node_ready_duration_seconds':
                self._extract_node_ready(metric_data, structured, table_key)
            elif metric_name == 'ovnkube_controller_sync_duration_seconds':
                self._extract_controller_sync(metric_data, structured, table_key)
            elif metric_name == 'ovnkube_controller_sync_duration_p95':
                self._extract_controller_sync_p95(metric_data, structured, table_key)
        
        # Generate overview
        self._generate_overview(actual_data, structured)
        
        return structured
    
    def _extract_cni_add_latency(self, metric_data: Dict[str, Any], 
                                 structured: Dict[str, Any], table_key: str):
        """Extract CNI ADD request latency data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'cni_request_add_latency_p99')
    
    def _extract_cni_del_latency(self, metric_data: Dict[str, Any], 
                                 structured: Dict[str, Any], table_key: str):
        """Extract CNI DEL request latency data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'cni_request_del_latency_p99')
    
    def _extract_pod_annotation_latency(self, metric_data: Dict[str, Any], 
                                        structured: Dict[str, Any], table_key: str):
        """Extract pod annotation latency data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'pod_annotation_latency_p99')
    
    def _extract_pod_first_seen_lsp(self, metric_data: Dict[str, Any], 
                                     structured: Dict[str, Any], table_key: str):
        """Extract pod first seen to LSP created latency data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'pod_first_seen_lsp_created_p99')
    
    def _extract_pod_lsp_created(self, metric_data: Dict[str, Any], 
                                 structured: Dict[str, Any], table_key: str):
        """Extract pod LSP created latency data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'pod_lsp_created_p99')
    
    def _extract_pod_port_binding(self, metric_data: Dict[str, Any], 
                                  structured: Dict[str, Any], table_key: str):
        """Extract pod port binding latency data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'pod_port_binding_p99')
    
    def _extract_pod_port_binding_up(self, metric_data: Dict[str, Any], 
                                     structured: Dict[str, Any], table_key: str):
        """Extract pod port binding up latency data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'pod_port_binding_up_p99')
    
    def _extract_sync_service_latency(self, metric_data: Dict[str, Any], 
                                      structured: Dict[str, Any], table_key: str):
        """Extract service sync latency data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'sync_service_latency')
    
    def _extract_sync_service_latency_p99(self, metric_data: Dict[str, Any], 
                                         structured: Dict[str, Any], table_key: str):
        """Extract service sync latency P99 data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'sync_service_latency_p99')
    
    def _extract_network_config_pod(self, metric_data: Dict[str, Any], 
                                    structured: Dict[str, Any], table_key: str):
        """Extract network config pod duration data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'apply_network_config_pod_duration_p99')
    
    def _extract_network_config_service(self, metric_data: Dict[str, Any], 
                                        structured: Dict[str, Any], table_key: str):
        """Extract network config service duration data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'apply_network_config_service_duration_p99')
    
    def _extract_controller_ready(self, metric_data: Dict[str, Any], 
                                  structured: Dict[str, Any], table_key: str):
        """Extract controller ready duration data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'ovnkube_controller_ready_duration_seconds')
    
    def _extract_node_ready(self, metric_data: Dict[str, Any], 
                           structured: Dict[str, Any], table_key: str):
        """Extract node ready duration data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'ovnkube_node_ready_duration_seconds')
    
    def _extract_controller_sync(self, metric_data: Dict[str, Any], 
                                 structured: Dict[str, Any], table_key: str):
        """Extract controller sync duration data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'ovnkube_controller_sync_duration_seconds')
    
    def _extract_controller_sync_p95(self, metric_data: Dict[str, Any], 
                                     structured: Dict[str, Any], table_key: str):
        """Extract controller sync duration P95 data"""
        self._extract_generic_pod_metric(metric_data, structured, table_key, 'ovnkube_controller_sync_duration_p95')
    
    def _extract_generic_pod_metric(self, metric_data: Dict[str, Any], 
                                    structured: Dict[str, Any], 
                                    table_key: str,
                                    metric_name: str):
        """Generic extraction for pod-based metrics"""
        pod_metrics = metric_data.get('pod_metrics', {})
        
        if not pod_metrics:
            # No data available
            return
        
        # Collect all values for top identification
        all_values = []
        for pod_name, pod_data in pod_metrics.items():
            max_val = pod_data.get('max_value')
            if max_val is not None:
                all_values.append((pod_name, pod_data, float(max_val)))
        
        # Find top 1 by max value
        top_max = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Get thresholds
        config = self.metric_configs.get(metric_name, {})
        thresholds = config.get('thresholds', {'critical': 0.1, 'warning': 0.05})
        
        # Format rows
        for pod_name, pod_data, max_val in all_values:
            is_top = (max_val == top_max and max_val > 0)
            
            avg_val = pod_data.get('avg_value')
            latest_val = pod_data.get('latest_value')
            
            # Format values with highlighting
            avg_display = self.highlight_latency_value(avg_val, False) if avg_val is not None else 'N/A'
            max_display = self.highlight_latency_value(max_val, is_top)
            latest_display = self.highlight_latency_value(latest_val, False) if latest_val is not None else 'N/A'
            
            structured[table_key].append({
                'Pod Name': self.truncate_text(pod_name, 30),
                'Node Name': self.truncate_node_name(pod_data.get('node_name', 'Unknown')),
                'Avg Latency': avg_display,
                'Max Latency': max_display,
                'Latest': latest_display,
                'Data Points': pod_data.get('data_points', 0)
            })
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate OVN latency overview"""
        metrics = data.get('metrics', {})
        summary = data.get('summary', {})
        
        overview_data = {
            'Total Metrics': summary.get('total_metrics', 0),
            'Successful': summary.get('successful_metrics', 0),
            'Failed': summary.get('failed_metrics', 0),
            'Duration': data.get('duration', 'Unknown'),
            'Status': self.create_status_badge('success', 'Active') if summary.get('successful_metrics', 0) > 0 else self.create_status_badge('danger', 'No Data')
        }
        
        structured['ovn_latency_overview'].append(overview_data)
    
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
            logger.error(f"Failed to transform OVN latency data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            # Overview table first
            if 'ovn_latency_overview' in dataframes and not dataframes['ovn_latency_overview'].empty:
                html_tables['ovn_latency_overview'] = self.create_html_table(
                    dataframes['ovn_latency_overview'], 
                    'OVN Latency Overview'
                )
            
            # Metadata table
            if 'metadata' in dataframes and not dataframes['metadata'].empty:
                html_tables['metadata'] = self.create_html_table(
                    dataframes['metadata'], 
                    'Collection Metadata'
                )
            
            # Summary table
            if 'summary' in dataframes and not dataframes['summary'].empty:
                html_tables['summary'] = self.create_html_table(
                    dataframes['summary'], 
                    'Metrics Summary'
                )
            
            # Generate tables for each metric
            for table_key, df in dataframes.items():
                if table_key.startswith('latency_') and not df.empty:
                    # Extract metric name from table key
                    metric_name = table_key.replace('latency_', '')
                    config = self.metric_configs.get(metric_name, {})
                    display_title = config.get('title', metric_name.replace('_', ' ').title())
                    
                    html_tables[table_key] = self.create_html_table(df, display_title)
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for OVN latency: {e}")
        
        return html_tables
    
    def summarize_ovn_latency(self, data: Dict[str, Any]) -> str:
        """Generate OVN latency summary as HTML"""
        try:
            summary_items: List[str] = []
            
            # Get overview data
            overview_data = data.get('ovn_latency_overview', [])
            if overview_data and len(overview_data) > 0:
                overview = overview_data[0]
                total = overview.get('Total Metrics', 0)
                successful = overview.get('Successful', 0)
                failed = overview.get('Failed', 0)
                duration = overview.get('Duration', 'Unknown')
                
                summary_items.append(f"<li>Collected {successful}/{total} latency metrics over {duration}</li>")
                if failed > 0:
                    summary_items.append(f"<li>⚠️ {failed} metrics failed to collect</li>")
            
            # Count metrics with data
            metrics_with_data = 0
            total_pods = 0
            for key, value in data.items():
                if key.startswith('latency_') and isinstance(value, list) and len(value) > 0:
                    metrics_with_data += 1
                    total_pods += len(value)
            
            if metrics_with_data > 0:
                summary_items.append(f"<li>Metrics with data: {metrics_with_data}</li>")
                summary_items.append(f"<li>Total pod measurements: {total_pods}</li>")
            
            return (
                "<div class=\"ovn-latency-summary\">"
                "<h4>OVN-Kubernetes Latency Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate OVN latency summary: {e}")
            return "OVN-Kubernetes latency metrics collected"