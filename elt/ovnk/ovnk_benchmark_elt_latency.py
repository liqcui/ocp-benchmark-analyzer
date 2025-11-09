"""
ELT module for OVN-Kubernetes Latency Metrics
Handles extraction, transformation and HTML generation for latency data
Optimized version with reduced duplication and improved organization
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class latencyELT(EltUtility):
    """ELT module for OVN-Kubernetes latency metrics"""
    
    def __init__(self):
        super().__init__()
        self.max_columns = 6  # Allow more columns for latency data
        
        # Define metric categories and their extraction patterns
        self.metric_categories = {
            'ready_duration_metrics': ['ready_duration'],
            'sync_duration_metrics': ['sync_duration', 'sync'],
            'cni_latency_metrics': ['cni'],
            'pod_annotation_metrics': ['pod_annotation', 'annotation'],
            'pod_creation_metrics': ['pod_creation', 'creation'],
            'service_latency_metrics': ['service'],
            'network_config_metrics': ['network', 'config', 'programming']
        }
    
    def extract_ovn_latency_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVN latency data from collected metrics"""
        try:
            structured = {
                'collection_info': self._extract_collection_info(data),
                'latency_summary': self._extract_latency_summary(data)
            }
            
            # Extract metrics by category using unified method
            for category in self.metric_categories:
                structured[category] = self._extract_metrics_by_category(data, category)
            
            # Derive special datasets for HTML tables
            self._derive_special_datasets(data, structured)
            
            return structured
            
        except Exception as e:
            logger.error(f"Failed to extract latency data: {e}")
            return {'error': str(e)}
    
    def _extract_collection_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract collection metadata"""
        return {
            'timestamp': data.get('collection_timestamp', 'Unknown'),
            'query_type': data.get('query_type', 'Unknown'),
            'duration': data.get('query_parameters', {}).get('duration', 'N/A'),
            'total_metrics': data.get('summary', {}).get('total_metrics', 0),
            'successful_metrics': data.get('summary', {}).get('successful_metrics', 0),
            'failed_metrics': data.get('summary', {}).get('failed_metrics', 0)
        }
    
    def _extract_metrics_by_category(self, data: Dict[str, Any], category: str) -> List[Dict[str, Any]]:
        """Unified method to extract metrics by category"""
        metrics = []
        category_data = data.get(category, {})
        
        for metric_key, metric_info in category_data.items():
            if not self._has_valid_statistics(metric_info):
                continue
            
            stats = metric_info['statistics']
            metric_entry = self._create_base_metric_entry(metric_info, stats)
            
            # Add category-specific fields
            self._add_category_specific_fields(metric_entry, stats, category)
            
            metrics.append(metric_entry)
        
        return metrics
    
    def _has_valid_statistics(self, metric_info: Dict[str, Any]) -> bool:
        """Check if metric has valid statistics"""
        return ('statistics' in metric_info and 
                metric_info['statistics'].get('count', 0) > 0)
    
    def _create_base_metric_entry(self, metric_info: Dict[str, Any], stats: Dict[str, Any]) -> Dict[str, Any]:
        """Create base metric entry with common fields"""
        return {
            'metric_name': self.truncate_metric_name(metric_info.get('metric_name', 'Unknown')),
            'component': metric_info.get('component', 'Unknown'),
            'max_value': self.format_latency_value(stats.get('max_value', 0)),
            'avg_value': self.format_latency_value(stats.get('avg_value', 0)),
            'data_points': stats.get('count', 0),
            'severity': self.categorize_latency_severity(stats.get('max_value', 0))
        }
    
    def _add_category_specific_fields(self, metric_entry: Dict[str, Any], stats: Dict[str, Any], category: str):
        """Add category-specific fields to metric entry"""
        # Get top entries based on category
        if category == 'sync_duration_metrics':
            top_entries = stats.get('top_20', stats.get('top_5', []))
        else:
            top_entries = stats.get('top_5', [])
        
        if top_entries:
            top_pod = self._find_first_valid_entry(top_entries)
            if top_pod:
                metric_entry['top_pod'] = top_pod.get('pod_name', 'N/A')
                metric_entry['top_node'] = self.truncate_node_name(top_pod.get('node_name', 'N/A'))
                
                # Add resource name for sync metrics
                if category == 'sync_duration_metrics' and 'resource_name' in top_pod:
                    metric_entry['top_resource'] = top_pod.get('resource_name', 'N/A')
                
                # Add service name for network config metrics
                if category == 'network_config_metrics' and 'service_name' in top_pod:
                    metric_entry['top_service'] = top_pod.get('service_name', 'N/A')
    
    def _find_first_valid_entry(self, entries: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find first entry with non-null value"""
        for entry in entries:
            if entry.get('value') is not None:
                return entry
        return None
    
    def _extract_latency_summary(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract overall latency summary"""
        summary = data.get('summary', {})
        
        summary_data = {
            'total_metrics': summary.get('total_metrics', 0),
            'successful_metrics': summary.get('successful_metrics', 0),
            'failed_metrics': summary.get('failed_metrics', 0),
            'controller_metrics': summary.get('component_breakdown', {}).get('controller', 0),
            'node_metrics': summary.get('component_breakdown', {}).get('node', 0),
            'overall_max_latency': 'N/A',
            'overall_avg_latency': 'N/A',
            'critical_metric': 'N/A'
        }
        
        # Extract overall latency info with error handling
        self._extract_overall_latency_info(summary, summary_data)
        
        return summary_data
    
    def _extract_overall_latency_info(self, summary: Dict[str, Any], summary_data: Dict[str, Any]):
        """Extract overall latency information safely"""
        if 'overall_max_latency' in summary:
            max_info = summary['overall_max_latency']
            readable = max_info.get('readable', {})
            summary_data['overall_max_latency'] = f"{readable.get('value', 0)} {readable.get('unit', '')}"
            summary_data['critical_metric'] = max_info.get('metric', 'N/A')
        
        if 'overall_avg_latency' in summary:
            avg_info = summary['overall_avg_latency']
            readable = avg_info.get('readable', {})
            summary_data['overall_avg_latency'] = f"{readable.get('value', 0)} {readable.get('unit', '')}"
    
    def _derive_special_datasets(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Derive special datasets for HTML table generation"""
        try:
            # Derive controller sync top20 for HTML table
            self._derive_controller_sync_top20(data, structured)
            
            # Derive all metrics top5 detailed for HTML table
            self._derive_all_metrics_top5_detailed(data, structured)
            
            # Derive top latencies ranking
            self._derive_top_latencies_ranking(data, structured)
            
            # Group metrics by category for organized display
            structured['metrics_by_category'] = self._group_metrics_by_category(structured)
            
            # Identify critical findings
            structured['critical_findings'] = self._identify_critical_findings(data)
            
        except Exception as e:
            logger.warning(f"Failed to derive special datasets: {e}")
    
    def _derive_controller_sync_top20(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Derive controller sync top20 detailed data"""
        try:
            sync_metrics = data.get('sync_duration_metrics', {}) or {}
            for metric_key, metric_info in sync_metrics.items():
                if metric_info.get('metric_name') == 'ovnkube_controller_sync_duration_seconds':
                    stats = metric_info.get('statistics', {}) or {}
                    top_entries = stats.get('top_20', []) or stats.get('top_5', [])
                    
                    detailed = []
                    for idx, entry in enumerate(top_entries[:20], 1):
                        rv = entry.get('readable_value', {}) or {}
                        detailed.append({
                            'rank': idx,
                            'pod_name': entry.get('pod_name', 'N/A'),
                            'node_name': entry.get('node_name', 'N/A'),
                            'resource_name': entry.get('resource_name', 'N/A'),
                            'value': entry.get('value', 0),
                            'formatted_value': f"{rv.get('value', 0)} {rv.get('unit', '')}"
                        })
                    
                    if detailed:
                        structured['top_20_detailed'] = detailed
                    break
        except Exception:
            pass
    
    def _derive_all_metrics_top5_detailed(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Derive all metrics top5 detailed data"""
        try:
            metrics_details = {}
            
            for category in self.metric_categories:
                cat_data = data.get(category, {}) or {}
                for metric_key, metric_info in cat_data.items():
                    stats = metric_info.get('statistics', {}) or {}
                    if stats.get('count', 0) <= 0:
                        continue
                    
                    top_entries = stats.get('top_5', []) or stats.get('top_20', [])
                    top_details = self._create_top5_details(top_entries, metric_info)
                    
                    if top_details:
                        metrics_details[metric_key] = {
                            'metric_name': metric_info.get('metric_name', metric_key),
                            'component': metric_info.get('component', 'Unknown'),
                            'unit': metric_info.get('unit', 'seconds'),
                            'category': category.replace('_metrics', ''),
                            'max_value': stats.get('max_value', 0),
                            'avg_value': stats.get('avg_value', 0),
                            'readable_max': stats.get('readable_max', {}),
                            'readable_avg': stats.get('readable_avg', {}),
                            'data_points': stats.get('count', 0),
                            'top_5_detailed': top_details
                        }
            
            if metrics_details:
                structured['metrics_details'] = metrics_details
        except Exception:
            pass
    
    def _create_top5_details(self, top_entries: List[Dict[str, Any]], metric_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create top 5 details for a metric"""
        top_details = []
        for i, entry in enumerate(top_entries[:5]):
            if entry.get('value') is None:
                continue
            
            rv = entry.get('readable_value', {}) or {}
            detail = {
                'rank': i + 1,
                'pod_name': entry.get('pod_name', 'N/A'),
                'node_name': entry.get('node_name', 'N/A'),
                'value': entry.get('value', 0),
                'unit': metric_info.get('unit', 'seconds'),
                'formatted_value': f"{rv.get('value', 0)} {rv.get('unit', '')}"
            }
            
            # Add optional fields
            for field in ['resource_name', 'service_name']:
                if field in entry:
                    detail[field] = entry.get(field, 'N/A')
            
            top_details.append(detail)
        
        return top_details
    
    def _derive_top_latencies_ranking(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Derive top latencies ranking for comprehensive view"""
        try:
            top_latencies = []
            summary = data.get('summary', {})
            
            for metric in summary.get('top_latencies', [])[:10]:
                latency_entry = {
                    'rank': len(top_latencies) + 1,
                    'metric_name': self.truncate_metric_name(metric.get('metric_name', 'Unknown')),
                    'component': metric.get('component', 'Unknown'),
                    'max_latency': f"{metric.get('readable_max', {}).get('value', 0)} {metric.get('readable_max', {}).get('unit', '')}",
                    'avg_latency': f"{metric.get('readable_avg', {}).get('value', 0)} {metric.get('readable_avg', {}).get('unit', '')}",
                    'data_points': metric.get('data_points', 0),
                    'severity': self.categorize_latency_severity(metric.get('max_value', 0)),
                    'readable_max': metric.get('readable_max', {}),
                    'readable_avg': metric.get('readable_avg', {})
                }
                top_latencies.append(latency_entry)
            
            if top_latencies:
                structured['top_latencies_ranking'] = top_latencies
        except Exception:
            pass
    
    def _group_metrics_by_category(self, structured: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Group metrics by category for organized display"""
        grouped = {}
        
        for category in self.metric_categories:
            if category in structured and structured[category]:
                category_name = category.replace('_metrics', '').replace('_', ' ').title()
                
                # Add readable format for each metric
                formatted_metrics = []
                for metric in structured[category]:
                    formatted_metric = dict(metric)
                    # Add readable max/avg if not present
                    if 'readable_max' not in formatted_metric:
                        formatted_metric['readable_max'] = self.format_latency_for_summary(
                            float(str(metric.get('max_value', '0')).split()[0]) if metric.get('max_value', '0') != 'N/A' else 0
                        )
                    if 'readable_avg' not in formatted_metric:
                        formatted_metric['readable_avg'] = self.format_latency_for_summary(
                            float(str(metric.get('avg_value', '0')).split()[0]) if metric.get('avg_value', '0') != 'N/A' else 0
                        )
                    formatted_metrics.append(formatted_metric)
                
                grouped[category_name] = formatted_metrics
        
        return grouped
    
    def _identify_critical_findings(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify critical findings in the latency data"""
        findings = []
        
        try:
            summary = data.get('summary', {})
            
            # Check for overall high latency
            if 'overall_max_latency' in summary:
                max_latency = summary['overall_max_latency']
                if max_latency.get('value', 0) > 5:  # > 5 seconds
                    findings.append({
                        'type': 'high_latency',
                        'description': f"Critical latency detected: {max_latency.get('readable', {}).get('value', 0)} {max_latency.get('readable', {}).get('unit', '')}",
                        'metric': max_latency.get('metric', 'N/A')
                    })
            
            # Check for high failure rate
            total_metrics = summary.get('total_metrics', 0)
            failed_metrics = summary.get('failed_metrics', 0)
            if total_metrics > 0 and (failed_metrics / total_metrics) > 0.1:  # > 10% failure rate
                findings.append({
                    'type': 'high_failure_rate',
                    'description': f"High metric collection failure rate: {failed_metrics}/{total_metrics}",
                    'count': failed_metrics
                })
            
            # Check for critical metrics in top latencies
            for metric in summary.get('top_latencies', [])[:3]:
                if metric.get('max_value', 0) > 10:  # > 10 seconds
                    findings.append({
                        'type': 'critical_metric_performance',
                        'description': f"Critical performance issue in {metric.get('metric_name', 'Unknown')}",
                        'metric': metric.get('metric_name', 'Unknown')
                    })
        
        except Exception:
            pass
        
        return findings
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured latency data to DataFrames with enhanced formatting"""
        dataframes = {}
        
        try:
            # Collection info
            if 'collection_info' in structured_data:
                collection_data = self._create_collection_info_data(structured_data['collection_info'])
                dataframes['latencyelt_collection_info'] = pd.DataFrame(collection_data)

            # Essential results summary
            if 'latency_summary' in structured_data:
                summary_data = self._create_summary_data(structured_data['latency_summary'])
                if summary_data:
                    dataframes['latencyelt_essential_summary'] = pd.DataFrame(summary_data)

            # Top latencies ranking
            if 'top_latencies_ranking' in structured_data:
                df_data = self._create_top_latencies_ranking_data(structured_data['top_latencies_ranking'])
                if df_data:
                    dataframes['latencyelt_top_latencies_ranking'] = pd.DataFrame(df_data)

            # Controller sync top 20
            if 'top_20_detailed' in structured_data:
                df_data = self._create_controller_sync_top20_data(structured_data['top_20_detailed'])
                if df_data:
                    dataframes['latencyelt_controller_sync_top20'] = pd.DataFrame(df_data)

            # All metrics top 5 detailed
            if 'metrics_details' in structured_data:
                df_data = self._create_all_metrics_top5_data(structured_data['metrics_details'])
                if df_data:
                    dataframes['latencyelt_all_metrics_top5'] = pd.DataFrame(df_data)

            # Metrics by category
            if 'metrics_by_category' in structured_data:
                self._create_category_dataframes(structured_data['metrics_by_category'], dataframes)

            # Critical findings
            if 'critical_findings' in structured_data and structured_data['critical_findings']:
                df_data = self._create_critical_findings_data(structured_data['critical_findings'])
                if df_data:
                    dataframes['latencyelt_critical_findings'] = pd.DataFrame(df_data)

            # Individual metric category dataframes
            for category in self.metric_categories:
                if structured_data.get(category):
                    df = pd.DataFrame(structured_data[category])
                    if not df.empty:
                        table_name = f"latencyelt_{category.replace('_metrics', '')}"
                        dataframes[table_name] = df
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to create latency DataFrames: {e}")
            return {}
    
    def _create_collection_info_data(self, collection_info: Dict[str, Any]) -> List[Dict[str, str]]:
        """Create collection info data for DataFrame"""
        return [
            {'Property': 'Collection Time', 'Value': collection_info['timestamp'][:19]},
            {'Property': 'Query Type', 'Value': collection_info['query_type'].title()},
            {'Property': 'Total Metrics', 'Value': str(collection_info['total_metrics'])},
            {'Property': 'Success Rate', 'Value': f"{collection_info['successful_metrics']}/{collection_info['total_metrics']}"},
            {'Property': 'Failed Metrics', 'Value': str(collection_info['failed_metrics'])}
        ]
    
    def _create_summary_data(self, summary: Dict[str, Any]) -> List[Dict[str, str]]:
        """Create summary data for DataFrame"""
        summary_data = []
        
        if summary.get('overall_max_latency') != 'N/A':
            summary_data.append({
                'Property': 'Maximum Latency', 
                'Value': summary['overall_max_latency']
            })
        
        if summary.get('overall_avg_latency') != 'N/A':
            summary_data.append({
                'Property': 'Average Latency', 
                'Value': summary['overall_avg_latency']
            })
        
        if summary.get('critical_metric') != 'N/A':
            summary_data.append({
                'Property': 'Critical Metric', 
                'Value': self.truncate_metric_name(summary['critical_metric'], 40)
            })
        
        summary_data.extend([
            {'Property': 'Controller Metrics', 'Value': str(summary.get('controller_metrics', 0))},
            {'Property': 'Node Metrics', 'Value': str(summary.get('node_metrics', 0))}
        ])
        
        return summary_data
    
    def _create_top_latencies_ranking_data(self, ranking: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create top latencies ranking data for DataFrame"""
        df_data = []
        for entry in ranking:
            df_data.append({
                'Rank': entry['rank'],
                'Metric Name': self.truncate_metric_name(entry['metric_name'], 35),
                'Component': entry['component'].title(),
                'Max Latency': entry['max_latency'],
                'Severity': entry['severity'],
                'Data Points': entry['data_points'],
                'is_top1': entry.get('rank') == 1,
                'is_critical': entry['severity'] in ['critical', 'high']
            })
        return df_data
    
    def _create_controller_sync_top20_data(self, top20: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create controller sync top20 data for DataFrame"""
        df_data = []
        for entry in top20:
            df_data.append({
                'Rank': entry['rank'],
                'Pod Name': self.truncate_latencyelt_pod_name(entry['pod_name'], 25),
                'Node Name': self.truncate_node_name(entry['node_name'], 20),
                'Resource': entry.get('resource_name', 'N/A'),
                'Latency': entry['formatted_value'],
                'Value (s)': entry['value'],
                'is_top1': entry['rank'] == 1,
                'is_top3': entry['rank'] <= 3
            })
        return df_data
    
    def _create_all_metrics_top5_data(self, metrics_details: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create all metrics top5 data for DataFrame"""
        all_top5_data = []
        
        for metric_key, metric_info in metrics_details.items():
            for pod_entry in metric_info['top_5_detailed']:
                all_top5_data.append({
                    'Metric Name': self.truncate_metric_name(metric_info['metric_name'], 30),
                    'Component': metric_info['component'].title(),
                    'Category': metric_info['category'].replace('_', ' ').title(),
                    'Rank': pod_entry['rank'],
                    'Pod Name': self.truncate_latencyelt_pod_name(pod_entry['pod_name'], 25),
                    'Node Name': self.truncate_node_name(pod_entry['node_name'], 20),
                    'Latency': pod_entry['formatted_value'],
                    'Value': pod_entry['value'],
                    'Unit': pod_entry['unit'],
                    'is_top1_in_metric': pod_entry['rank'] == 1
                })
        
        return all_top5_data
    
    def _create_category_dataframes(self, metrics_by_category: Dict[str, List[Dict[str, Any]]], dataframes: Dict[str, pd.DataFrame]):
        """Create category-specific dataframes"""
        for category_name, metrics in metrics_by_category.items():
            if metrics:
                category_data = []
                for metric in metrics:
                    category_data.append({
                        'Metric Name': self.truncate_metric_name(metric['metric_name'], 30),
                        'Component': metric['component'].title(),
                        'Max Latency': f"{metric['readable_max'].get('value', 0)} {metric['readable_max'].get('unit', '')}",
                        'Avg Latency': f"{metric['readable_avg'].get('value', 0)} {metric['readable_avg'].get('unit', '')}",
                        'Severity': metric['severity'],
                        'Data Points': metric['data_points']
                    })
                
                table_name = f"latencyelt_{category_name.lower().replace(' ', '_')}"
                dataframes[table_name] = pd.DataFrame(category_data)
    
    def _create_critical_findings_data(self, findings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create critical findings data for DataFrame"""
        findings_data = []
        for finding in findings:
            findings_data.append({
                'Finding Type': finding['type'].replace('_', ' ').title(),
                'Description': finding['description'],
                'Details': finding.get('metric', finding.get('count', 'N/A'))
            })
        return findings_data

    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables with enhanced latency-specific styling and highlighting"""
        html_tables = {}
        
        try:
            for table_name, df in dataframes.items():
                if df.empty:
                    continue
                
                # Apply column limiting based on table type
                df_limited = self.limit_dataframe_columns(df, table_name=table_name)
                df_formatted = df_limited.copy()
                
                # Apply enhanced formatting using helper methods
                self._apply_table_formatting(df_formatted, table_name)
                
                # Create HTML table with appropriate styling
                css_class = self._get_table_css_class(table_name)
                
                html = df_formatted.to_html(
                    index=False,
                    classes=css_class,
                    escape=False,
                    table_id=f"table-{table_name.replace('_', '-')}",
                    border=1
                )
                
                # Clean up HTML and add wrapper
                html = self.clean_html(html)
                html = self._wrap_table_html(html, table_name)
                
                html_tables[table_name] = html
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Failed to generate latency HTML tables: {e}")
            return {}
    
    def _apply_table_formatting(self, df_formatted: pd.DataFrame, table_name: str):
        """Apply specific formatting based on table type"""
        if table_name == 'latencyelt_top_latencies_ranking':
            self._format_top_latencies_ranking(df_formatted)
        elif table_name == 'latencyelt_controller_sync_top20':
            self._format_controller_sync_top20(df_formatted)
        elif table_name == 'latencyelt_all_metrics_top5':
            self._format_all_metrics_top5(df_formatted)
        elif table_name == 'latencyelt_critical_findings':
            self._format_critical_findings(df_formatted)
        elif 'severity' in df_formatted.columns:
            self._format_severity_based_table(df_formatted)
    
    def _format_top_latencies_ranking(self, df: pd.DataFrame):
        """Format top latencies ranking table"""
        if 'Rank' in df.columns:
            df['Rank'] = df.apply(
                lambda row: f'<span class="badge badge-danger badge-lg pulse-animation"><i class="fas fa-exclamation-triangle"></i> #{row["Rank"]}</span>' if row.get('is_top1', False)
                else f'<span class="badge badge-warning font-weight-bold">#{row["Rank"]}</span>' if row.get('is_critical', False)
                else f'<span class="badge badge-info">#{row["Rank"]}</span>',
                axis=1
            )
        
        if 'Max Latency' in df.columns:
            df['Max Latency'] = df.apply(
                lambda row: f'<span class="text-danger font-weight-bold blink-text">{row["Max Latency"]}</span>' if row.get('is_top1', False)
                else f'<span class="text-warning font-weight-bold">{row["Max Latency"]}</span>' if row.get('is_critical', False)
                else row['Max Latency'],
                axis=1
            )
        
        if 'Metric Name' in df.columns:
            df['Metric Name'] = df.apply(
                lambda row: f'<span class="text-danger font-weight-bold">{row["Metric Name"]}</span> <span class="badge badge-danger">CRITICAL</span>' if row.get('is_top1', False)
                else f'<span class="text-warning font-weight-bold">{row["Metric Name"]}</span>' if row.get('is_critical', False)
                else row['Metric Name'],
                axis=1
            )
        
        # Remove helper columns
        df.drop(columns=['is_top1', 'is_critical'], errors='ignore', inplace=True)
    
    def _format_controller_sync_top20(self, df: pd.DataFrame):
        """Format controller sync top20 table"""
        if 'Rank' in df.columns:
            df['Rank'] = df.apply(
                lambda row: f'<span class="badge badge-danger badge-lg"><i class="fas fa-exclamation-triangle"></i> #{row["Rank"]}</span>' if row.get('is_top1', False)
                else f'<span class="badge badge-warning font-weight-bold">#{row["Rank"]}</span>' if row.get('is_top3', False)
                else f'<span class="badge badge-info">#{row["Rank"]}</span>',
                axis=1
            )
        
        if 'Latency' in df.columns:
            df['Latency'] = df.apply(
                lambda row: f'<span class="text-danger font-weight-bold">{row["Latency"]}</span>' if row.get('is_top1', False)
                else f'<span class="text-warning">{row["Latency"]}</span>' if row.get('is_top3', False)
                else row['Latency'],
                axis=1
            )
        
        # Remove helper columns
        df.drop(columns=['is_top1', 'is_top3'], errors='ignore', inplace=True)
    
    def _format_all_metrics_top5(self, df: pd.DataFrame):
        """Format all metrics top5 table"""
        if 'Rank' in df.columns:
            df['Rank'] = df.apply(
                lambda row: f'<span class="badge badge-danger">#{row["Rank"]}</span>' if row.get('is_top1_in_metric', False)
                else f'<span class="badge badge-secondary">#{row["Rank"]}</span>',
                axis=1
            )
        
        if 'Latency' in df.columns:
            df['Latency'] = df.apply(
                lambda row: f'<span class="text-danger font-weight-bold">{row["Latency"]}</span>' if row.get('is_top1_in_metric', False)
                else row['Latency'],
                axis=1
            )
        
        # Remove helper columns
        df.drop(columns=['is_top1_in_metric'], errors='ignore', inplace=True)
    
    def _format_critical_findings(self, df: pd.DataFrame):
        """Format critical findings table"""
        if 'Finding Type' in df.columns:
            df['Finding Type'] = df['Finding Type'].apply(
                lambda x: f'<span class="badge badge-danger">{x}</span>'
            )
    
    def _format_severity_based_table(self, df: pd.DataFrame):
        """Format tables with severity columns"""
        if 'max_value' in df.columns and 'severity' in df.columns:
            df['max_value'] = df.apply(
                lambda row: self._create_latency_badge_html(row['max_value'], row.get('severity', 'unknown')), 
                axis=1
            )
        
        if 'component' in df.columns:
            df['component'] = df['component'].apply(
                lambda x: f'<span class="badge badge-primary">{x}</span>' if x.lower() == 'controller'
                else f'<span class="badge badge-info">{x}</span>' if x.lower() == 'node'
                else f'<span class="badge badge-secondary">{x}</span>'
            )
    
    def _get_table_css_class(self, table_name: str) -> str:
        """Get appropriate CSS class for table type"""
        base_class = 'table table-striped table-bordered table-sm'
        
        if 'top_latencies_ranking' in table_name or 'critical' in table_name:
            return base_class + ' table-hover alert-table'
        elif 'summary' in table_name or 'collection_info' in table_name:
            return base_class + ' table-info'
        else:
            return base_class
    
    def _wrap_table_html(self, html: str, table_name: str) -> str:
        """Wrap table HTML with appropriate container"""
        if 'top_latencies_ranking' in table_name or 'critical' in table_name:
            return f'<div class="table-responsive critical-table-wrapper">{html}</div>'
        else:
            return f'<div class="table-responsive">{html}</div>'
    
    def _create_latency_badge_html(self, value: str, severity: str) -> str:
        """Create HTML badge for latency value with severity color"""
        badge_colors = {
            'critical': 'danger',
            'high': 'warning', 
            'medium': 'info',
            'low': 'success',
            'unknown': 'secondary'
        }
        
        color = badge_colors.get(severity, 'secondary')
        return f'<span class="badge badge-{color}">{value}</span>'
    
    def summarize_ovn_latency_data(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary text for OVN latency data"""
        try:
            summary_parts = []
            
            # Collection info
            if 'collection_info' in structured_data:
                info = structured_data['collection_info']
                summary_parts.append(f"Collected {info['successful_metrics']}/{info['total_metrics']} latency metrics")
                if info['duration'] != 'N/A':
                    summary_parts.append(f"over {info['duration']}")
            
            # Overall latency summary
            if 'latency_summary' in structured_data:
                summary = structured_data['latency_summary']
                if summary['overall_max_latency'] != 'N/A':
                    summary_parts.append(f"• Maximum latency: {summary['overall_max_latency']}")
                if summary['critical_metric'] != 'N/A':
                    metric_name = self.truncate_metric_name(summary['critical_metric'], 25)
                    summary_parts.append(f"• Critical metric: {metric_name}")
                
                summary_parts.append(f"• Controller metrics: {summary['controller_metrics']}")
                summary_parts.append(f"• Node metrics: {summary['node_metrics']}")
            
            # Component breakdown
            component_counts = {}
            for category in self.metric_categories:
                if category in structured_data and structured_data[category]:
                    count = len(structured_data[category])
                    category_name = category.replace('_metrics', '').replace('_', ' ').title()
                    component_counts[category_name] = count
            
            if component_counts:
                summary_parts.append("• Metrics by category: " + 
                                   ", ".join([f"{k}: {v}" for k, v in component_counts.items()]))
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate latency summary: {e}")
            return f"Latency summary generation failed: {str(e)}"


# Async functions for data collection and processing
async def latencyelt_get_controller_sync_top20(prometheus_client, time: str = None, duration: str = None, end_time: str = None):
    """Get top 20 controller sync duration metrics with detailed pod/node/resource info"""
    try:
        from tools.ovnk_benchmark_prometheus_ovnk_latency import OVNLatencyCollector
        
        collector = OVNLatencyCollector(prometheus_client)
        result = await collector.collect_controller_sync_duration(time, duration, end_time)
        
        # Extract top 20 detailed entries for ovnkube_controller_sync_duration_seconds only
        if ('metric_name' in result and 
            result['metric_name'] == 'ovnkube_controller_sync_duration_seconds' and
            'statistics' in result and 'top_20' in result['statistics']):
            
            top_20_entries = []
            for i, entry in enumerate(result['statistics']['top_20'][:20]):
                detailed_entry = {
                    'rank': i + 1,
                    'pod_name': entry.get('pod_name', 'N/A'),
                    'node_name': entry.get('node_name', 'N/A'),
                    'resource_name': entry.get('resource_name', 'N/A'),
                    'value': entry.get('value', 0),
                    'unit': 'seconds',
                    'readable_value': entry.get('readable_value', {}),
                    'formatted_value': f"{entry.get('readable_value', {}).get('value', 0)} {entry.get('readable_value', {}).get('unit', 'ms')}"
                }
                top_20_entries.append(detailed_entry)
            
            return {
                'metric_name': 'ovnkube_controller_sync_duration_seconds',
                'component': 'controller',
                'total_entries': len(top_20_entries),
                'max_value': result['statistics'].get('max_value', 0),
                'avg_value': result['statistics'].get('avg_value', 0),
                'readable_max': result['statistics'].get('readable_max', {}),
                'readable_avg': result['statistics'].get('readable_avg', {}),
                'top_20_detailed': top_20_entries,
                'collection_timestamp': datetime.now().isoformat(),
                'query_type': 'controller_sync_top20'
            }
        
        return {'error': 'No controller sync duration data available for ovnkube_controller_sync_duration_seconds'}
    
    except Exception as e:
        logger.error(f"Failed to get controller sync top20: {e}")
        return {'error': str(e)}


async def latencyelt_get_all_metrics_top5_detailed(prometheus_client, time: str = None, duration: str = None, end_time: str = None):
    """Get detailed top 5 entries for all latency metrics with pod/node/value/unit info"""
    try:
        comprehensive_data = await latencyelt_get_comprehensive_metrics(prometheus_client, time, duration, end_time)
        
        if 'error' in comprehensive_data:
            return comprehensive_data
        
        metrics_top5_detailed = {}
        
        # Process all metric categories
        categories = [
            'ready_duration_metrics', 'sync_duration_metrics', 'cni_latency_metrics',
            'pod_annotation_metrics', 'pod_creation_metrics', 'service_latency_metrics', 
            'network_config_metrics'
        ]
        
        for category in categories:
            if category in comprehensive_data:
                category_data = comprehensive_data[category]
                
                for metric_key, metric_info in category_data.items():
                    if 'statistics' in metric_info and metric_info['statistics'].get('count', 0) > 0:
                        stats = metric_info['statistics']
                        
                        # Extract detailed top 5 pod info
                        top_5_details = []
                        top_entries = stats.get('top_5', stats.get('top_20', [])[:5])
                        
                        for i, entry in enumerate(top_entries[:5]):
                            if entry.get('value') is not None:
                                detail = {
                                    'rank': i + 1,
                                    'pod_name': entry.get('pod_name', 'N/A'),
                                    'node_name': entry.get('node_name', 'N/A'),
                                    'value': entry.get('value', 0),
                                    'unit': metric_info.get('unit', 'seconds'),
                                    'readable_value': entry.get('readable_value', {}),
                                    'formatted_value': f"{entry.get('readable_value', {}).get('value', 0)} {entry.get('readable_value', {}).get('unit', 'ms')}"
                                }
                                
                                # Add additional fields if present
                                if 'resource_name' in entry:
                                    detail['resource_name'] = entry.get('resource_name', 'N/A')
                                if 'service_name' in entry:
                                    detail['service_name'] = entry.get('service_name', 'N/A')
                                
                                top_5_details.append(detail)
                        
                        if top_5_details:
                            metrics_top5_detailed[metric_key] = {
                                'metric_name': metric_info.get('metric_name', metric_key),
                                'component': metric_info.get('component', 'Unknown'),
                                'unit': metric_info.get('unit', 'seconds'),
                                'category': category.replace('_metrics', ''),
                                'max_value': stats.get('max_value', 0),
                                'avg_value': stats.get('avg_value', 0),
                                'readable_max': stats.get('readable_max', {}),
                                'readable_avg': stats.get('readable_avg', {}),
                                'data_points': stats.get('count', 0),
                                'top_5_detailed': top_5_details
                            }
        
        return {
            'collection_timestamp': comprehensive_data.get('collection_timestamp', ''),
            'query_type': 'all_metrics_top5_detailed',
            'total_metrics': len(metrics_top5_detailed),
            'metrics_details': metrics_top5_detailed,
            'summary': comprehensive_data.get('summary', {})
        }
    
    except Exception as e:
        logger.error(f"Failed to get all metrics top5 detailed: {e}")
        return {'error': str(e)}


async def latencyelt_get_comprehensive_metrics(prometheus_client, time: str = None, duration: str = None, end_time: str = None):
    """Get comprehensive latency metrics for all components"""
    try:
        from tools.ovnk_benchmark_prometheus_ovnk_latency import collect_enhanced_ovn_latency_metrics
        
        return await collect_enhanced_ovn_latency_metrics(
            prometheus_client, time, duration, end_time,
            include_controller_metrics=True,
            include_node_metrics=True, 
            include_extended_metrics=True
        )
    
    except Exception as e:
        logger.error(f"Failed to get comprehensive metrics: {e}")
        return {'error': str(e)}


def latencyelt_extract_essential_results(data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract all essential results from latency.json format data"""
    try:
        # Initialize ELT processor
        elt = latencyELT()
        
        # Use existing extract method and enhance with essential results
        base_data = elt.extract_ovn_latency_data(data)
        
        # Add the essential results structure that was previously separate
        base_data.update({
            'overall_summary': base_data.get('latency_summary', {}),
            'top_latencies_ranking': base_data.get('top_latencies_ranking', []),
            'metrics_by_category': base_data.get('metrics_by_category', {}),
            'critical_findings': base_data.get('critical_findings', [])
        })
        
        return base_data
        
    except Exception as e:
        logger.error(f"Failed to extract essential results: {e}")
        return {'error': str(e)}


async def latencyelt_convert_json_to_html_tables(latency_json_data: Dict[str, Any]) -> str:
    """Convert latency.json format data to comprehensive HTML tables"""
    try:
        # Extract essential results
        essential_data = latencyelt_extract_essential_results(latency_json_data)
        
        if 'error' in essential_data:
            return f"<div class='alert alert-danger'>Error: {essential_data['error']}</div>"
        
        # Initialize processor and create tables
        elt = latencyELT()
        dataframes = elt.transform_to_dataframes(essential_data)
        html_tables = elt.generate_html_tables(dataframes)
        
        # Generate organized HTML output
        output_parts = []
        
        # Add header with critical alert if needed
        if essential_data.get('critical_findings'):
            output_parts.append(f"""
            <div class="alert alert-danger alert-dismissible fade show critical-alert" role="alert">
                <h4 class="alert-heading"><i class="fas fa-exclamation-triangle"></i> Critical Latency Issues Detected!</h4>
                <p>High-impact latency problems have been identified in your OVN-Kubernetes cluster.</p>
                <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                    <span aria-hidden="true">&times;</span>
                </button>
            </div>
            """)
        
        # Add title and metadata
        output_parts.append(f"""
        <div class="mb-4">
            <h3><span class="badge badge-primary">OVN-Kubernetes Latency Analysis</span></h3>
            <small class="text-muted">Generated from latency metrics data</small>
        </div>
        """)
        
        # Define table display order for logical presentation
        table_order = [
            ('latencyelt_essential_summary', 'Overall Summary'),
            ('latencyelt_collection_info', 'Collection Information'),
            ('latencyelt_critical_findings', 'Critical Findings'),
            ('latencyelt_top_latencies_ranking', 'Top Latency Metrics (Critical First)'),
            ('latencyelt_controller_sync_top20', 'Controller Sync Duration Top 20'),
            ('latencyelt_all_metrics_top5', 'All Metrics Top 5 Details'),
            ('latencyelt_ready_duration', 'Ready Duration Metrics'),
            ('latencyelt_sync_duration', 'Sync Duration Metrics'),
            ('latencyelt_cni_latency', 'CNI Latency Metrics'),
            ('latencyelt_pod_annotation', 'Pod Annotation Metrics'),
            ('latencyelt_pod_creation', 'Pod Creation Metrics'),
            ('latencyelt_service_latency', 'Service Latency Metrics'),
            ('latencyelt_network_config', 'Network Configuration Metrics')
        ]
        
        # Add tables in logical order
        for table_name, display_title in table_order:
            if table_name in html_tables:
                if table_name in ['latencyelt_critical_findings', 'latencyelt_top_latencies_ranking']:
                    output_parts.append(f"""
                    <div class="critical-section mt-4">
                        <h4 class="text-danger"><i class="fas fa-exclamation-circle"></i> {display_title}</h4>
                        {html_tables[table_name]}
                    </div>
                    """)
                else:
                    output_parts.append(f"<h5 class='mt-4'>{display_title}</h5>")
                    output_parts.append(html_tables[table_name])
        
        # Add any remaining tables not in the predefined order
        for table_name, html_table in html_tables.items():
            if not any(table_name == order_item[0] for order_item in table_order):
                table_title = table_name.replace('latencyelt_', '').replace('_', ' ').title()
                output_parts.append(f"<h5 class='mt-4'>{table_title}</h5>")
                output_parts.append(html_table)
        
        # Add custom CSS for enhanced styling
        custom_css = """
        <style>
            .critical-alert { 
                border-left: 5px solid #dc3545; 
                animation: pulse 2s infinite; 
            }
            .critical-table-wrapper { 
                border: 2px solid #dc3545; 
                border-radius: 5px; 
                margin: 10px 0; 
            }
            .critical-section { 
                background-color: #f8f9fa; 
                border: 1px solid #dee2e6; 
                border-radius: 5px; 
                padding: 15px; 
                margin: 10px 0; 
            }
            .pulse-animation { 
                animation: pulse 1.5s infinite; 
            }
            .blink-text { 
                animation: blink 2s infinite; 
            }
            .alert-table { 
                background-color: #fff3cd; 
            }
            @keyframes pulse {
                0% { opacity: 1; }
                50% { opacity: 0.5; }
                100% { opacity: 1; }
            }
            @keyframes blink {
                0%, 50% { opacity: 1; }
                51%, 100% { opacity: 0.3; }
            }
            .badge-lg { 
                font-size: 0.9em; 
                padding: 0.5em 0.8em; 
            }
        </style>
        """
        
        return custom_css + ' '.join(output_parts)
        
    except Exception as e:
        return f"<div class='alert alert-danger'>Error processing latency data: {str(e)}</div>"


async def latencyelt_process_and_convert(latency_data: Union[Dict[str, Any], str], output_format: str = "html") -> Dict[str, Any]:
    """Process latency data and convert to requested format with enhanced features"""
    try:
        # Parse input data
        if isinstance(latency_data, str):
            try:
                import json
                data = json.loads(latency_data)
            except json.JSONDecodeError as e:
                return {'error': f"Invalid JSON: {str(e)}"}
        else:
            data = latency_data
        
        # Extract essential results
        essential_data = latencyelt_extract_essential_results(data)
        
        if 'error' in essential_data:
            return essential_data
        
        # Initialize processor
        elt = latencyELT()
        
        # Create summary
        summary = elt.summarize_ovn_latency_data(essential_data)
        
        result = {
            'data_type': 'latencyelt_metrics',
            'summary': summary,
            'essential_data': essential_data,
            'timestamp': data.get('collection_timestamp', datetime.now().isoformat()),
            'processing_successful': True,
            'critical_issues_found': len(essential_data.get('critical_findings', [])) > 0,
            'total_metrics': essential_data.get('collection_info', {}).get('total_metrics', 0)
        }
        
        if output_format in ["html", "both"]:
            html_output = await latencyelt_convert_json_to_html_tables(data)
            result['html'] = html_output
        
        if output_format in ["dataframes", "both"]:
            dataframes = elt.transform_to_dataframes(essential_data)
            result['dataframes'] = dataframes
        
        if output_format in ["tabular", "both"]:
            from tabulate import tabulate
            dataframes = elt.transform_to_dataframes(essential_data)
            tabular_tables = {}
            
            for table_name, df in dataframes.items():
                if not df.empty:
                    try:
                        formatted_table = tabulate(
                            df.values.tolist(),
                            headers=df.columns.tolist(),
                            tablefmt="grid",
                            stralign="left",
                            maxcolwidths=[30] * len(df.columns)
                        )
                        tabular_tables[table_name] = {
                            'raw_data': [df.columns.tolist()] + df.values.tolist(),
                            'formatted_string': formatted_table
                        }
                    except Exception as e:
                        tabular_tables[table_name] = {
                            'raw_data': [df.columns.tolist()] + df.values.tolist(),
                            'formatted_string': str(df)
                        }
            
            result['tabular'] = tabular_tables
        
        return result
        
    except Exception as e:
        return {'error': f"Processing failed: {str(e)}", 'processing_successful': False}