"""
Extract, Load, Transform module for Kubernetes API Server Metrics
Handles API server metrics data from tools/ocp/cluster_apistats.py
ONLY contains API server specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class apiStatsELT(utilityELT):
    """Extract, Load, Transform class for API server metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'avg_ro_apicalls_latency': {
                'title': 'Average Read-Only API Calls Latency',
                'unit': 'seconds',
                'thresholds': {'critical': 5.0, 'warning': 1.0}
            },
            'max_ro_apicalls_latency': {
                'title': 'Maximum Read-Only API Calls Latency',
                'unit': 'seconds',
                'thresholds': {'critical': 10.0, 'warning': 5.0}
            },
            'avg_mutating_apicalls_latency': {
                'title': 'Average Mutating API Calls Latency',
                'unit': 'seconds',
                'thresholds': {'critical': 10.0, 'warning': 5.0}
            },
            'max_mutating_apicalls_latency': {
                'title': 'Maximum Mutating API Calls Latency',
                'unit': 'seconds',
                'thresholds': {'critical': 30.0, 'warning': 10.0}
            },
            'api_request_rate': {
                'title': 'API Request Rate',
                'unit': 'requests_per_second',
                'thresholds': {'critical': 2000.0, 'warning': 1000.0}
            },
            'api_request_errors': {
                'title': 'API Request Errors',
                'unit': 'requests_per_second',
                'thresholds': {'critical': 10.0, 'warning': 1.0}
            },
            'api_server_current_inflight_requests': {
                'title': 'Current Inflight Requests',
                'unit': 'count',
                'thresholds': {'critical': 1000.0, 'warning': 500.0}
            },
            'etcd_request_duration': {
                'title': 'etcd Request Duration (P99)',
                'unit': 'seconds',
                'thresholds': {'critical': 0.1, 'warning': 0.05}
            },
            'request_latency_p99_by_verb': {
                'title': 'Request Latency P99 by Verb',
                'unit': 'seconds',
                'thresholds': {'critical': 5.0, 'warning': 2.0}
            },
            'request_duration_p99_by_resource': {
                'title': 'Request Duration P99 by Resource',
                'unit': 'seconds',
                'thresholds': {'critical': 5.0, 'warning': 2.0}
            },
            'pf_request_wait_duration_p99': {
                'title': 'Priority & Fairness Wait Duration P99',
                'unit': 'seconds',
                'thresholds': {'critical': 1.0, 'warning': 0.5}
            },
            'pf_request_execution_duration_p99': {
                'title': 'Priority & Fairness Execution Duration P99',
                'unit': 'seconds',
                'thresholds': {'critical': 5.0, 'warning': 2.0}
            },
            'pf_request_dispatch_rate': {
                'title': 'Priority & Fairness Dispatch Rate',
                'unit': 'requests_per_second',
                'thresholds': {'critical': 2000.0, 'warning': 1000.0}
            },
            'pf_request_in_queue': {
                'title': 'Priority & Fairness Requests in Queue',
                'unit': 'count',
                'thresholds': {'critical': 100.0, 'warning': 50.0}
            }
        }
    
    def extract_api_stats(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract API server metrics from cluster_apistats.py output"""
        
        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'api_stats_overview': [],
        }
        
        # Get metrics from actual_data
        metrics = actual_data.get('metrics', {})
        
        # Process each metric type
        for metric_name, metric_data in metrics.items():
            if metric_data.get('status') != 'success':
                continue
            
            metric_config = self.metric_configs.get(metric_name, {})
            if not metric_config:
                continue
            
            # Extract metric based on its type
            if 'latency' in metric_name or 'duration' in metric_name:
                self._extract_latency_metric(metric_name, metric_data, structured, metric_config)
            elif 'rate' in metric_name or 'errors' in metric_name:
                self._extract_rate_metric(metric_name, metric_data, structured, metric_config)
            elif 'inflight' in metric_name or 'in_queue' in metric_name:
                self._extract_count_metric(metric_name, metric_data, structured, metric_config)
            else:
                self._extract_generic_metric(metric_name, metric_data, structured, metric_config)
        
        # Generate overview
        self._generate_overview(actual_data, structured)
        
        return structured
    
    def _extract_latency_metric(self, metric_name: str, metric_data: Dict[str, Any], 
                                structured: Dict[str, Any], config: Dict[str, Any]):
        """Extract latency-type metrics"""
        table_key = metric_name
        structured[table_key] = []
        
        # Check for top_3 first, then fall back to top_5
        top_items = metric_data.get('top_3', metric_data.get('top_5', []))
        if not top_items:
            return
        
        # Find top 1 value for max_seconds
        max_values = [s.get('max_seconds', 0) for s in top_items if s.get('max_seconds') is not None]
        top_value = max(max_values) if max_values else 0
        thresholds = config.get('thresholds', {'critical': 5.0, 'warning': 1.0})
        
        for series_info in top_items:
            max_val = series_info.get('max_seconds')
            if max_val is None:
                continue
                
            is_top = (max_val == top_value and top_value > 0)
            
            # Extract labels
            resource = series_info.get('resource', '')
            verb = series_info.get('verb', '')
            scope = series_info.get('scope', '')
            node_name = series_info.get('node_name', '')
            operation = series_info.get('operation', '')
            flow_schema = series_info.get('flow_schema', '')
            priority_level = series_info.get('priority_level', '')
            
            # Format values
            avg_val = series_info.get('avg_seconds', 0)
            
            avg_display = self.highlight_latency_value(avg_val * 1000, False)  # Convert to ms
            max_display = self.highlight_latency_value(max_val * 1000, is_top)
            
            row = {
                'Avg Latency': avg_display,
                'Max Latency': max_display,
                'Data Points': series_info.get('data_points', 0)
            }
            
            # Add resource/verb/scope/operation if present
            if resource:
                row['Resource'] = self.truncate_text(resource, 30)
            if verb:
                row['Verb'] = verb
            if scope:
                row['Scope'] = scope
            if operation:
                row['Operation'] = self.truncate_text(operation, 25)
            if flow_schema:
                row['Flow Schema'] = self.truncate_text(flow_schema, 25)
            if priority_level:
                row['Priority Level'] = self.truncate_text(priority_level, 25)
            if node_name:
                row['Node'] = self.truncate_node_name(node_name)
            
            structured[table_key].append(row)
    
    def _extract_rate_metric(self, metric_name: str, metric_data: Dict[str, Any],
                            structured: Dict[str, Any], config: Dict[str, Any]):
        """Extract rate-type metrics (requests/sec, errors/sec)"""
        table_key = metric_name
        structured[table_key] = []
        
        # Check for top_3 first, then fall back to top_5
        top_items = metric_data.get('top_3', metric_data.get('top_5', []))
        if not top_items:
            return
        
        # Find top 1 value
        max_values = [s.get('max_requests_per_sec', 0) for s in top_items if s.get('max_requests_per_sec') is not None]
        top_value = max(max_values) if max_values else 0
        
        for series_info in top_items:
            max_val = series_info.get('max_requests_per_sec')
            if max_val is None:
                continue
                
            is_top = (max_val == top_value and top_value > 0)
            
            # Extract labels
            resource = series_info.get('resource', '')
            verb = series_info.get('verb', '')
            code = series_info.get('code', '')
            node_name = series_info.get('node_name', '')
            
            # Format values
            avg_val = series_info.get('avg_requests_per_sec', 0)
            
            avg_display = self.highlight_rate_value(avg_val, "req/sec", False)
            max_display = self.highlight_rate_value(max_val, "req/sec", is_top)
            
            row = {
                'Avg Rate': avg_display,
                'Max Rate': max_display,
                'Data Points': series_info.get('data_points', 0)
            }
            
            if resource:
                row['Resource'] = self.truncate_text(resource, 30)
            if verb:
                row['Verb'] = verb
            if code:
                row['Code'] = code
            if node_name:
                row['Node'] = self.truncate_node_name(node_name)
            
            structured[table_key].append(row)
    
    def _extract_count_metric(self, metric_name: str, metric_data: Dict[str, Any],
                             structured: Dict[str, Any], config: Dict[str, Any]):
        """Extract count-type metrics (inflight requests, queue depth)"""
        table_key = metric_name
        structured[table_key] = []
        
        # Check for top_3 first, then fall back to top_5
        top_items = metric_data.get('top_3', metric_data.get('top_5', []))
        if not top_items:
            return
        
        # Find top 1 value
        max_values = [s.get('max_count', 0) for s in top_items if s.get('max_count') is not None]
        top_value = max(max_values) if max_values else 0
        thresholds = config.get('thresholds', {'critical': 1000.0, 'warning': 500.0})
        
        for series_info in top_items:
            max_val = series_info.get('max_count')
            if max_val is None:
                continue
                
            is_top = (max_val == top_value and top_value > 0)
            
            # Extract labels
            request_kind = series_info.get('request_kind', '')
            flow_schema = series_info.get('flow_schema', '')
            priority_level = series_info.get('priority_level', '')
            node_name = series_info.get('node_name', '')
            
            # Format values
            avg_val = series_info.get('avg_count', 0)
            
            # Highlight based on thresholds
            if is_top:
                avg_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {avg_val:.0f}</span>'
            elif avg_val >= thresholds['critical']:
                avg_display = f'<span class="text-danger font-weight-bold">⚠️ {avg_val:.0f}</span>'
            elif avg_val >= thresholds['warning']:
                avg_display = f'<span class="text-warning font-weight-bold">{avg_val:.0f}</span>'
            else:
                avg_display = f'{avg_val:.0f}'
            
            if is_top:
                max_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {max_val:.0f}</span>'
            elif max_val >= thresholds['critical']:
                max_display = f'<span class="text-danger font-weight-bold">⚠️ {max_val:.0f}</span>'
            elif max_val >= thresholds['warning']:
                max_display = f'<span class="text-warning font-weight-bold">{max_val:.0f}</span>'
            else:
                max_display = f'{max_val:.0f}'
            
            row = {
                'Avg Count': avg_display,
                'Max Count': max_display,
                'Data Points': series_info.get('data_points', 0)
            }
            
            if request_kind:
                row['Request Kind'] = request_kind
            if flow_schema:
                row['Flow Schema'] = self.truncate_text(flow_schema, 25)
            if priority_level:
                row['Priority Level'] = self.truncate_text(priority_level, 25)
            if node_name:
                row['Node'] = self.truncate_node_name(node_name)
            
            structured[table_key].append(row)
    
    def _extract_generic_metric(self, metric_name: str, metric_data: Dict[str, Any],
                               structured: Dict[str, Any], config: Dict[str, Any]):
        """Extract generic metrics that don't fit other categories"""
        table_key = metric_name
        structured[table_key] = []
        
        # Check for top_3 first, then fall back to top_5
        top_items = metric_data.get('top_3', metric_data.get('top_5', []))
        if not top_items:
            return
        
        unit = config.get('unit', 'value')
        
        # Determine which field to use for values
        value_key = None
        avg_key = None
        
        # Check first item to determine structure
        first_item = top_items[0]
        if 'max_value' in first_item:
            value_key = 'max_value'
            avg_key = 'avg_value'
        elif 'max_seconds' in first_item:
            value_key = 'max_seconds'
            avg_key = 'avg_seconds'
        elif 'max_count' in first_item:
            value_key = 'max_count'
            avg_key = 'avg_count'
        else:
            return
        
        # Find top 1 value
        max_values = [s.get(value_key, 0) for s in top_items if s.get(value_key) is not None]
        top_value = max(max_values) if max_values else 0
        
        for series_info in top_items:
            max_val = series_info.get(value_key)
            if max_val is None:
                continue
                
            is_top = (max_val == top_value and top_value > 0)
            
            node_name = series_info.get('node_name', '')
            labels = series_info.get('labels', '')
            
            avg_val = series_info.get(avg_key, 0)
            
            # Format with unit
            avg_display = self.format_and_highlight(avg_val, unit, config.get('thresholds', {}), False)
            max_display = self.format_and_highlight(max_val, unit, config.get('thresholds', {}), is_top)
            
            row = {
                'Labels': self.truncate_text(labels, 40),
                'Avg Value': avg_display,
                'Max Value': max_display,
                'Data Points': series_info.get('data_points', 0)
            }
            
            if node_name:
                row['Node'] = self.truncate_node_name(node_name)
            
            structured[table_key].append(row)
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate API stats overview"""
        summary = data.get('summary', {})
        cluster_summary = data.get('cluster_summary', {})
        
        total_metrics = summary.get('total_metrics', 0)
        successful_metrics = summary.get('successful_metrics', 0)
        failed_metrics = summary.get('failed_metrics', 0)
        
        health = cluster_summary.get('cluster_health', {})
        overall_score = health.get('overall_score', 'unknown')
        
        structured['api_stats_overview'].append({
            'Property': 'Total Metrics',
            'Value': total_metrics
        })
        
        structured['api_stats_overview'].append({
            'Property': 'Successful Metrics',
            'Value': successful_metrics
        })
        
        structured['api_stats_overview'].append({
            'Property': 'Failed Metrics',
            'Value': failed_metrics
        })
        
        # Add status badge
        status_badge = self.create_status_badge(overall_score, overall_score.upper())
        structured['api_stats_overview'].append({
            'Property': 'Overall Health',
            'Value': status_badge
        })
        
        # Add performance indicators
        perf_indicators = cluster_summary.get('performance_indicators', {})
        
        if 'read_only_latency' in perf_indicators:
            ro_latency = perf_indicators['read_only_latency']
            max_latency = ro_latency.get('max_seconds', 0)
            structured['api_stats_overview'].append({
                'Property': 'Max RO Latency',
                'Value': self.format_latency_seconds(max_latency)
            })
        
        if 'mutating_latency' in perf_indicators:
            mut_latency = perf_indicators['mutating_latency']
            max_latency = mut_latency.get('max_seconds', 0)
            structured['api_stats_overview'].append({
                'Property': 'Max Mutating Latency',
                'Value': self.format_latency_seconds(max_latency)
            })
        
        if 'error_rate' in perf_indicators:
            error_rate = perf_indicators['error_rate']
            avg_errors = error_rate.get('avg_errors_per_sec', 0)
            structured['api_stats_overview'].append({
                'Property': 'Avg Error Rate',
                'Value': f"{avg_errors:.3f} errors/sec"
            })
        
        if 'request_rate' in perf_indicators:
            request_rate = perf_indicators['request_rate']
            avg_rate = request_rate.get('avg_requests_per_sec', 0)
            structured['api_stats_overview'].append({
                'Property': 'Avg Request Rate',
                'Value': f"{avg_rate:.3f} req/sec"
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
            logger.error(f"Failed to transform API stats data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            # Overview table first
            if 'api_stats_overview' in dataframes and not dataframes['api_stats_overview'].empty:
                html_tables['api_stats_overview'] = self.create_html_table(
                    dataframes['api_stats_overview'],
                    'API Server Metrics Overview'
                )
            
            # Generate tables for each metric in logical order
            metric_order = [
                'avg_ro_apicalls_latency',
                'max_ro_apicalls_latency',
                'avg_mutating_apicalls_latency',
                'max_mutating_apicalls_latency',
                'api_request_rate',
                'api_request_errors',
                'api_server_current_inflight_requests',
                'etcd_request_duration',
                'request_latency_p99_by_verb',
                'request_duration_p99_by_resource',
                'pf_request_wait_duration_p99',
                'pf_request_execution_duration_p99',
                'pf_request_dispatch_rate',
                'pf_request_in_queue'
            ]
            
            for metric_name in metric_order:
                if metric_name in dataframes and not dataframes[metric_name].empty:
                    config = self.metric_configs.get(metric_name, {})
                    display_name = config.get('title', metric_name.replace('_', ' ').title())
                    html_tables[metric_name] = self.create_html_table(
                        dataframes[metric_name],
                        display_name
                    )
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for API stats: {e}")
        
        return html_tables
    
    def summarize_api_stats(self, data: Dict[str, Any]) -> str:
        """Generate API stats summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('api_stats_overview', [])
            
            # Extract key metrics from overview
            for item in overview_data:
                if isinstance(item, dict):
                    prop = item.get('Property', '')
                    value = item.get('Value', '')
                    
                    if prop == 'Overall Health':
                        summary_items.append(f"<li><strong>Health:</strong> {value}</li>")
                    elif prop == 'Total Metrics':
                        summary_items.append(f"<li><strong>Total Metrics:</strong> {value}</li>")
                    elif prop == 'Max RO Latency':
                        summary_items.append(f"<li><strong>Max Read-Only Latency:</strong> {value}</li>")
                    elif prop == 'Max Mutating Latency':
                        summary_items.append(f"<li><strong>Max Mutating Latency:</strong> {value}</li>")
                    elif prop == 'Avg Error Rate':
                        summary_items.append(f"<li><strong>Avg Error Rate:</strong> {value}</li>")
            
            return (
                "<div class=\"api-stats-summary\">"
                "<h4>API Server Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate API stats summary: {e}")
            return "API server metrics collected"