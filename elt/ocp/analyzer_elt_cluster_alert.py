"""
Extract, Load, Transform module for Cluster Alert Metrics
Handles alert data from tools/ocp/cluster_alert.py
ONLY contains cluster_alert specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class clusterAlertELT(utilityELT):
    """Extract, Load, Transform class for cluster alert metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'top_alerts': {
                'title': 'Top Alerts',
                'unit': 'count',
                'thresholds': {'critical': 10.0, 'warning': 5.0}
            }
        }
    
    def extract_cluster_alert(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cluster alert information from cluster_alert.py output"""
        
        # Handle nested data structure - extract actual metrics data
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'cluster_alert_overview': [],
        }
        
        # Extract metrics from the data
        metrics = actual_data.get('metrics', {})
        
        # Process top_alerts metric
        if 'top_alerts' in metrics:
            self._extract_top_alerts(metrics['top_alerts'], structured)
        
        # Generate overview
        self._generate_overview(actual_data, structured)
        
        return structured
    
    def _extract_top_alerts(self, metric_data: Dict[str, Any], 
                           structured: Dict[str, Any]):
        """Extract top alerts information"""
        
        if metric_data.get('status') != 'success':
            return
        
        alerts = metric_data.get('alerts', [])
        alertname_statistics = metric_data.get('alertname_statistics', {})
        
        # Group alerts by severity
        severity_groups = {
            'critical': [],
            'warning': [],
            'info': [],
            'unknown': []
        }
        
        for alert in alerts:
            alert_name = alert.get('alert_name', 'unknown')
            severity = alert.get('severity', 'unknown').lower()
            count = float(alert.get('count', 0))
            
            # Get statistics for this alertname
            stats = alertname_statistics.get(alert_name, {})
            avg_count = stats.get('avg_count', count)
            max_count = stats.get('max_count', count)
            
            alert_entry = {
                'Alert Name': alert_name,
                'Severity': severity.title(),
                'Avg Count': avg_count,
                'Max Count': max_count
            }
            
            if severity in severity_groups:
                severity_groups[severity].append(alert_entry)
            else:
                severity_groups['unknown'].append(alert_entry)
        
        # Find top 1 by max_count for highlighting
        all_alerts_for_top = []
        for severity, alert_list in severity_groups.items():
            all_alerts_for_top.extend(alert_list)
        
        top_max_count = max((a['Max Count'] for a in all_alerts_for_top), default=0) if all_alerts_for_top else 0
        
        # Format and store alerts by severity
        thresholds = self.metric_configs['top_alerts']['thresholds']
        
        for severity in ['critical', 'warning', 'info', 'unknown']:
            table_key = f'top_alerts_{severity}'
            structured[table_key] = []
            
            for alert_entry in severity_groups[severity]:
                avg_count = alert_entry['Avg Count']
                max_count = alert_entry['Max Count']
                
                # Format avg count
                if avg_count >= thresholds['critical']:
                    avg_display = f'<span class="text-danger font-weight-bold">⚠️ {avg_count:.1f}</span>'
                elif avg_count >= thresholds['warning']:
                    avg_display = f'<span class="text-warning font-weight-bold">{avg_count:.1f}</span>'
                else:
                    avg_display = f'{avg_count:.1f}'
                
                # Format max count with top 1 highlighting
                is_top = (max_count == top_max_count and max_count > 0)
                if is_top:
                    max_display = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {max_count:.1f}</span>'
                elif max_count >= thresholds['critical']:
                    max_display = f'<span class="text-danger font-weight-bold">⚠️ {max_count:.1f}</span>'
                elif max_count >= thresholds['warning']:
                    max_display = f'<span class="text-warning font-weight-bold">{max_count:.1f}</span>'
                else:
                    max_display = f'{max_count:.1f}'
                
                # Format severity badge
                severity_badge = self._create_severity_badge(alert_entry['Severity'])
                
                structured[table_key].append({
                    'Alert Name': alert_entry['Alert Name'],
                    'Severity': severity_badge,
                    'Avg Count': avg_display,
                    'Max Count': max_display
                })
    
    def _create_severity_badge(self, severity: str) -> str:
        """Create HTML badge for severity"""
        severity_lower = severity.lower()
        badge_map = {
            'critical': 'danger',
            'warning': 'warning',
            'info': 'info',
            'unknown': 'secondary'
        }
        badge_color = badge_map.get(severity_lower, 'secondary')
        return f'<span class="badge badge-{badge_color}">{severity.upper()}</span>'
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate cluster alert overview"""
        
        metrics = data.get('metrics', {})
        top_alerts = metrics.get('top_alerts', {})
        
        if top_alerts.get('status') != 'success':
            structured['cluster_alert_overview'].append({
                'Property': 'Status',
                'Value': self.create_status_badge('danger', 'Error')
            })
            return
        
        total_alert_types = top_alerts.get('total_alert_types', 0)
        alerts = top_alerts.get('alerts', [])
        
        # Count by severity
        severity_counts = {'critical': 0, 'warning': 0, 'info': 0, 'unknown': 0}
        for alert in alerts:
            severity = alert.get('severity', 'unknown').lower()
            if severity in severity_counts:
                severity_counts[severity] += 1
            else:
                severity_counts['unknown'] += 1
        
        time_range = top_alerts.get('time_range', {})
        
        structured['cluster_alert_overview'].append({
            'Property': 'Total Alert Types',
            'Value': str(total_alert_types)
        })
        
        structured['cluster_alert_overview'].append({
            'Property': 'Critical Alerts',
            'Value': self._format_severity_count(severity_counts['critical'], 'critical')
        })
        
        structured['cluster_alert_overview'].append({
            'Property': 'Warning Alerts',
            'Value': self._format_severity_count(severity_counts['warning'], 'warning')
        })
        
        structured['cluster_alert_overview'].append({
            'Property': 'Info Alerts',
            'Value': self._format_severity_count(severity_counts['info'], 'info')
        })
        
        if time_range:
            start_time = time_range.get('start', 'N/A')
            end_time = time_range.get('end', 'N/A')
            structured['cluster_alert_overview'].append({
                'Property': 'Time Range',
                'Value': f"{self.format_timestamp(start_time)} to {self.format_timestamp(end_time)}"
            })
    
    def _format_severity_count(self, count: int, severity: str) -> str:
        """Format severity count with badge"""
        if count == 0:
            return '0'
        
        badge_map = {
            'critical': 'danger',
            'warning': 'warning',
            'info': 'info'
        }
        badge_color = badge_map.get(severity, 'secondary')
        return f'<span class="badge badge-{badge_color}">{count}</span>'
    
    def summarize_cluster_alert(self, data: Dict[str, Any]) -> str:
        """Generate cluster alert summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('cluster_alert_overview', [])
            
            # Extract key metrics
            total_alerts = 0
            critical_count = 0
            warning_count = 0
            
            for item in overview_data:
                prop = item.get('Property', '')
                value = item.get('Value', '')
                
                if prop == 'Total Alert Types':
                    try:
                        total_alerts = int(value)
                    except (ValueError, TypeError):
                        pass
                elif prop == 'Critical Alerts':
                    # Extract number from badge HTML
                    import re
                    match = re.search(r'>(\d+)<', str(value))
                    if match:
                        critical_count = int(match.group(1))
                elif prop == 'Warning Alerts':
                    import re
                    match = re.search(r'>(\d+)<', str(value))
                    if match:
                        warning_count = int(match.group(1))
            
            if total_alerts > 0:
                summary_items.append(f"<li>Total Alert Types: {total_alerts}</li>")
            
            if critical_count > 0:
                summary_items.append(f"<li>Critical Alerts: <span class=\"text-danger font-weight-bold\">{critical_count}</span></li>")
            
            if warning_count > 0:
                summary_items.append(f"<li>Warning Alerts: <span class=\"text-warning font-weight-bold\">{warning_count}</span></li>")
            
            # Check for specific alert tables
            for severity in ['critical', 'warning', 'info']:
                table_key = f'top_alerts_{severity}'
                if table_key in data and data[table_key]:
                    count = len(data[table_key])
                    if count > 0:
                        summary_items.append(f"<li>{severity.title()} Alerts: {count}</li>")
            
            return (
                "<div class=\"cluster-alert-summary\">"
                "<h4>Cluster Alert Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate cluster alert summary: {e}")
            return "Cluster alert metrics collected"
    
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
            logger.error(f"Failed to transform cluster alert data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by severity"""
        html_tables = {}
        
        try:
            # Overview table first
            if 'cluster_alert_overview' in dataframes and not dataframes['cluster_alert_overview'].empty:
                html_tables['cluster_alert_overview'] = self.create_html_table(
                    dataframes['cluster_alert_overview'], 
                    'Cluster Alert Overview'
                )
            
            # Generate tables for each severity level
            for severity in ['critical', 'warning', 'info', 'unknown']:
                table_key = f'top_alerts_{severity}'
                if table_key in dataframes and not dataframes[table_key].empty:
                    display_name = f"Top Alerts - {severity.title()}"
                    html_tables[table_key] = self.create_html_table(
                        dataframes[table_key], 
                        display_name
                    )
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for cluster alerts: {e}")
        
        return html_tables