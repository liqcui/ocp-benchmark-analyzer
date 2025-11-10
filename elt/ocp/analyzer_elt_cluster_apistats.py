"""
Extract, Load, Transform module for Kubernetes API Server Metrics
Processes Kube API metrics data from ovnk_benchmark_prometheus_kubeapi.py
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class kubeAPIELT(EltUtility):
    """Extract, Load, Transform for Kubernetes API Server metrics"""
    
    def __init__(self):
        super().__init__()
    
    def extract_kube_api_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract Kubernetes API server metrics data"""
        try:
            structured_data = {
                'metadata': {
                    'timestamp': data.get('timestamp', ''),
                    'duration': data.get('duration', ''),
                    'analyzer_type': 'kube_api_metrics',
                    'total_metrics': len(data.get('metrics', {}))
                },
                'health_summary': data.get('summary', {}),
                'latency_metrics': {},
                'activity_metrics': {},
                'component_metrics': {},
                'errors': data.get('errors', [])
            }
            
            metrics = data.get('metrics', {})
            
            # Extract latency metrics
            readonly_latency = metrics.get('readonly_latency', {})
            mutating_latency = metrics.get('mutating_latency', {})
            
            structured_data['latency_metrics'] = {
                'readonly': self._extract_latency_data(readonly_latency, 'readonly'),
                'mutating': self._extract_latency_data(mutating_latency, 'mutating')
            }
            
            # Extract activity metrics
            structured_data['activity_metrics'] = {
                'watch_events': self._extract_activity_data(metrics.get('watch_events', {})),
                'cache_list': self._extract_activity_data(metrics.get('cache_list', {})),
                'watch_cache_received': self._extract_activity_data(metrics.get('watch_cache_received', {})),
                'watch_cache_dispatched': self._extract_activity_data(metrics.get('watch_cache_dispatched', {}))
            }
            
            # Extract component metrics
            structured_data['component_metrics'] = {
                'rest_client': self._extract_activity_data(metrics.get('rest_client', {})),
                'ovnkube_controller': self._extract_activity_data(metrics.get('ovnkube_controller', {})),
                'etcd_requests': self._extract_activity_data(metrics.get('etcd_requests', {}))
            }
            
            return structured_data
            
        except Exception as e:
            logger.error(f"Failed to extract Kube API data: {e}")
            return {'error': str(e)}
    
    def _extract_latency_data(self, latency_data: Dict[str, Any], latency_type: str) -> Dict[str, Any]:
        """Extract latency metrics data"""
        if not latency_data or 'error' in latency_data:
            return {'error': latency_data.get('error', 'No data available')}
        
        return {
            'metric_type': latency_type,
            'unit': latency_data.get('unit', 'seconds'),
            'top5_avg': latency_data.get('top5_avg', {}),
            'top5_max': latency_data.get('top5_max', {})
        }
    
    def _extract_activity_data(self, activity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract activity metrics data"""
        if not activity_data or 'error' in activity_data:
            return {'error': activity_data.get('error', 'No data available')}
        
        return {
            'metric_type': activity_data.get('metric_type', 'unknown'),
            'unit': activity_data.get('unit', 'count'),
            'query': activity_data.get('query', ''),
            'top5_avg': activity_data.get('top5_avg', []),
            'top5_max': activity_data.get('top5_max', [])
        }
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data to DataFrames"""
        try:
            dataframes = {}
            
            # Create metadata summary
            metadata = structured_data.get('metadata', {})
            metadata_df_data = [
                {'Property': 'Duration', 'Value': metadata.get('duration', 'N/A')},
                {'Property': 'Total Metrics', 'Value': metadata.get('total_metrics', 0)},
                {'Property': 'Timestamp', 'Value': self.format_timestamp(metadata.get('timestamp', ''))}
            ]
            dataframes['api_metadata'] = pd.DataFrame(metadata_df_data)
            
            # Create health summary
            health_summary = structured_data.get('health_summary', {})
            if health_summary and 'error' not in health_summary:
                health_df_data = [
                    {'Property': 'Overall Health', 'Value': f"{health_summary.get('overall_health', 0)}%"},
                    {'Property': 'Overall Status', 'Value': health_summary.get('overall_status', 'unknown').title()},
                    {'Property': 'Total Issues', 'Value': len(health_summary.get('top_issues', []))},
                    {'Property': 'Metric Types', 'Value': health_summary.get('total_metric_types', 0)}
                ]
                dataframes['health_summary'] = pd.DataFrame(health_df_data)
            
            # Process latency metrics
            latency_metrics = structured_data.get('latency_metrics', {})
            
            # Readonly latency
            readonly_data = latency_metrics.get('readonly', {})
            if readonly_data and 'error' not in readonly_data:
                readonly_df = self._create_latency_dataframe(readonly_data, 'Readonly API')
                if not readonly_df.empty:
                    dataframes['readonly_latency'] = readonly_df
            
            # Mutating latency  
            mutating_data = latency_metrics.get('mutating', {})
            if mutating_data and 'error' not in mutating_data:
                mutating_df = self._create_latency_dataframe(mutating_data, 'Mutating API')
                if not mutating_df.empty:
                    dataframes['mutating_latency'] = mutating_df
            
            # Process activity metrics
            activity_metrics = structured_data.get('activity_metrics', {})
            for activity_name, activity_data in activity_metrics.items():
                if activity_data and 'error' not in activity_data:
                    activity_df = self._create_activity_dataframe(activity_data, activity_name)
                    if not activity_df.empty:
                        dataframes[f'{activity_name}_top'] = activity_df
            
            # Process component metrics
            component_metrics = structured_data.get('component_metrics', {})
            for component_name, component_data in component_metrics.items():
                if component_data and 'error' not in component_data:
                    component_df = self._create_activity_dataframe(component_data, component_name)
                    if not component_df.empty:
                        dataframes[f'{component_name}_metrics'] = component_df
            
            # Create top issues summary if available
            if health_summary.get('top_issues'):
                issues_df_data = []
                for i, issue in enumerate(health_summary['top_issues'][:5], 1):
                    issues_df_data.append({
                        'Rank': i,
                        'Issue': issue[:60] + '...' if len(issue) > 60 else issue
                    })
                dataframes['top_issues'] = pd.DataFrame(issues_df_data)
            
            # Apply column limits
            for table_name, df in dataframes.items():
                dataframes[table_name] = self.limit_dataframe_columns(df, table_name=table_name)
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform Kube API data to DataFrames: {e}")
            return {}
    
    def _create_latency_dataframe(self, latency_data: Dict[str, Any], latency_type: str) -> pd.DataFrame:
        """Create DataFrame for latency metrics"""
        try:
            rows = []
            unit = latency_data.get('unit', 'seconds')
            
            # Process avg latency data
            top5_avg = latency_data.get('top5_avg', {})
            if top5_avg:
                for query_name, metrics_list in top5_avg.items():
                    if isinstance(metrics_list, list):
                        for i, metric in enumerate(metrics_list[:5], 1):
                            rows.append({
                                'Rank': i,
                                'Resource': self._format_resource_label(metric.get('label', 'unknown')),
                                'Avg P99': f"{metric.get('value', 0):.4f}",
                                'Type': 'Average'
                            })
            
            # Process max latency data
            top5_max = latency_data.get('top5_max', {})
            if top5_max:
                for query_name, metrics_list in top5_max.items():
                    if isinstance(metrics_list, list):
                        for i, metric in enumerate(metrics_list[:3], 1):  # Limit to top 3 max to save space
                            rows.append({
                                'Rank': i,
                                'Resource': self._format_resource_label(metric.get('label', 'unknown')),
                                'Max P99': f"{metric.get('value', 0):.4f}",
                                'Type': 'Maximum'
                            })
            
            return pd.DataFrame(rows) if rows else pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Failed to create latency DataFrame: {e}")
            return pd.DataFrame()
    
    def _create_activity_dataframe(self, activity_data: Dict[str, Any], activity_name: str) -> pd.DataFrame:
        """Create DataFrame for activity metrics"""
        try:
            rows = []
            unit = activity_data.get('unit', 'count')
            
            # Use top5_max data as primary source
            top5_data = activity_data.get('top5_max', [])
            if not top5_data:
                top5_data = activity_data.get('top5_avg', [])
            
            if isinstance(top5_data, list):
                for i, metric in enumerate(top5_data[:5], 1):
                    rows.append({
                        'Rank': i,
                        'Resource': self._format_activity_label(metric.get('label', 'unknown'), activity_name),
                        'Value': f"{metric.get('value', 0):.3f}",
                        'Unit': unit.split('/')[1] if '/' in unit else unit
                    })
            
            return pd.DataFrame(rows) if rows else pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Failed to create activity DataFrame for {activity_name}: {e}")
            return pd.DataFrame()
    
    def _format_resource_label(self, label: str) -> str:
        """Format resource label for display"""
        if ':' in label:
            parts = label.split(':')
            if len(parts) >= 2:
                return f"{parts[0]}:{parts[1]}"
        return self.truncate_text(label, 25)
    
    def _format_activity_label(self, label: str, activity_type: str) -> str:
        """Format activity label for display based on activity type"""
        if ':' in label:
            parts = label.split(':')
            if activity_type == 'watch_events' and len(parts) >= 2:
                return f"{parts[1]}:{parts[2]}" if len(parts) >= 3 else f"{parts[0]}:{parts[1]}"
            elif len(parts) >= 2:
                return f"{parts[0]}:{parts[1]}"
        return self.truncate_text(label, 30)
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        try:
            html_tables = {}
            
            # Define table order for better presentation
            table_order = [
                'health_summary', 'api_metadata', 'top_issues',
                'readonly_latency', 'mutating_latency',
                'watch_events_top', 'etcd_requests_metrics',
                'rest_client_metrics', 'ovnkube_controller_metrics'
            ]
            
            # Generate tables in order
            for table_name in table_order:
                if table_name in dataframes and not dataframes[table_name].empty:
                    html_tables[table_name] = self.create_html_table(dataframes[table_name], table_name)
            
            # Add any remaining tables not in the order
            for table_name, df in dataframes.items():
                if table_name not in html_tables and not df.empty:
                    html_tables[table_name] = self.create_html_table(df, table_name)
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for Kube API metrics: {e}")
            return {}
    
    def summarize_kube_api_data(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary for Kubernetes API metrics"""
        try:
            summary_parts = ["Kubernetes API Server Metrics:"]
            
            # Health summary
            health_summary = structured_data.get('health_summary', {})
            if health_summary and 'error' not in health_summary:
                overall_health = health_summary.get('overall_health', 0)
                overall_status = health_summary.get('overall_status', 'unknown')
                summary_parts.append(f"Overall health: {overall_health}% ({overall_status})")
                
                if health_summary.get('top_issues'):
                    summary_parts.append(f"Issues identified: {len(health_summary['top_issues'])}")
            
            # Latency summary
            latency_metrics = structured_data.get('latency_metrics', {})
            readonly_data = latency_metrics.get('readonly', {})
            mutating_data = latency_metrics.get('mutating', {})
            
            if readonly_data and 'error' not in readonly_data:
                top5_max = readonly_data.get('top5_max', {})
                if top5_max:
                    for metrics_list in top5_max.values():
                        if isinstance(metrics_list, list) and metrics_list:
                            highest_readonly = metrics_list[0].get('value', 0)
                            summary_parts.append(f"Highest readonly latency: {highest_readonly:.3f}s")
                            break
            
            if mutating_data and 'error' not in mutating_data:
                top5_max = mutating_data.get('top5_max', {})
                if top5_max:
                    for metrics_list in top5_max.values():
                        if isinstance(metrics_list, list) and metrics_list:
                            highest_mutating = metrics_list[0].get('value', 0)
                            summary_parts.append(f"Highest mutating latency: {highest_mutating:.3f}s")
                            break
            
            # Metadata summary
            metadata = structured_data.get('metadata', {})
            total_metrics = metadata.get('total_metrics', 0)
            duration = metadata.get('duration', '')
            
            summary_parts.append(f"Analyzed {total_metrics} metric types over {duration}")
            
            return " • ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate Kube API summary: {e}")
            return f"Kubernetes API metrics analysis completed with errors: {str(e)}"