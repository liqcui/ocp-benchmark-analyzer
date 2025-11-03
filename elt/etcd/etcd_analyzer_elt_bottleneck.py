"""
ELT module for etcd bottleneck analysis data
Extract, Load, Transform module for processing bottleneck analysis results
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from datetime import datetime
from .etcd_analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)

class bottleneckELT(utilityELT):
    """Extract, Load, Transform class for bottleneck analysis data"""
    
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)
    
    def extract_bottleneck_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract bottleneck analysis data"""
        try:
            if not isinstance(data, dict):
                return {'error': 'Input must be a dictionary'}
            
            # Check for nested structure
            if 'bottleneck_analysis' in data:
                analysis_data = data['bottleneck_analysis']
            else:
                analysis_data = data
            
            extracted = {
                'test_info': {
                    'test_id': data.get('test_id', 'unknown'),
                    'timestamp': data.get('timestamp', datetime.now().isoformat()),
                    'duration': data.get('duration', 'unknown'),
                    'status': data.get('status', 'unknown')
                },
                'disk_io_bottlenecks': analysis_data.get('disk_io_bottlenecks', []),
                'network_bottlenecks': analysis_data.get('network_bottlenecks', []),
                'memory_bottlenecks': analysis_data.get('memory_bottlenecks', []),
                'consensus_bottlenecks': analysis_data.get('consensus_bottlenecks', []),
                'root_cause_analysis': data.get('root_cause_analysis', []),
                'performance_recommendations': data.get('performance_recommendations', [])
            }
            
            return extracted
            
        except Exception as e:
            logger.error(f"Failed to extract bottleneck analysis data: {e}")
            return {'error': str(e)}
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        try:
            dataframes = {}
            
            # Test overview
            test_info = structured_data.get('test_info', {})
            if test_info:
                test_overview_data = [
                    {'Property': 'Test ID', 'Value': test_info.get('test_id', 'N/A')},
                    {'Property': 'Analysis Time', 'Value': self.format_timestamp(test_info.get('timestamp', ''), 19)},
                    {'Property': 'Duration', 'Value': test_info.get('duration', 'N/A')},
                    {'Property': 'Status', 'Value': test_info.get('status', 'N/A')}
                ]
                dataframes['test_overview'] = pd.DataFrame(test_overview_data)
            
            # Bottleneck summary
            summary_data = []
            disk_count = len(structured_data.get('disk_io_bottlenecks', []))
            network_count = len(structured_data.get('network_bottlenecks', []))
            memory_count = len(structured_data.get('memory_bottlenecks', []))
            consensus_count = len(structured_data.get('consensus_bottlenecks', []))
            
            summary_data.extend([
                {'Category': 'Disk I/O Bottlenecks', 'Count': disk_count, 'Severity': self._assess_category_severity(structured_data.get('disk_io_bottlenecks', []))},
                {'Category': 'Network Bottlenecks', 'Count': network_count, 'Severity': self._assess_category_severity(structured_data.get('network_bottlenecks', []))},
                {'Category': 'Memory Bottlenecks', 'Count': memory_count, 'Severity': self._assess_category_severity(structured_data.get('memory_bottlenecks', []))},
                {'Category': 'Consensus Bottlenecks', 'Count': consensus_count, 'Severity': self._assess_category_severity(structured_data.get('consensus_bottlenecks', []))}
            ])
            
            if summary_data:
                dataframes['bottleneck_summary'] = pd.DataFrame(summary_data)
            
            # Process each bottleneck category
            for category in ['disk_io_bottlenecks', 'network_bottlenecks', 'memory_bottlenecks', 'consensus_bottlenecks']:
                bottlenecks = structured_data.get(category, [])
                if bottlenecks:
                    df_data = []
                    top_values_indices = self._identify_top_bottlenecks(bottlenecks)
                    
                    for idx, bottleneck in enumerate(bottlenecks):
                        is_top = idx in top_values_indices
                        
                        # Format the value with proper units
                        formatted_value = self._format_bottleneck_value(
                            bottleneck.get('value', 0),
                            bottleneck.get('unit', ''),
                            bottleneck.get('type', ''),
                            is_top
                        )
                        
                        df_data.append({
                            'Type': bottleneck.get('type', 'Unknown').replace('_', ' ').title(),
                            'Node/Pod': bottleneck.get('node', bottleneck.get('pod', 'N/A')),
                            'Value': formatted_value,
                            'Severity': self.create_severity_badge(bottleneck.get('severity', 'unknown')),
                            'Description': bottleneck.get('description', 'No description')
                        })
                    
                    if df_data:
                        df = pd.DataFrame(df_data)
                        dataframes[category.replace('_bottlenecks', '_details')] = self.limit_dataframe_columns(df, 5)
            
            # Root cause analysis
            root_causes = structured_data.get('root_cause_analysis', [])
            if root_causes:
                root_cause_data = []
                for cause in root_causes:
                    root_cause_data.append({
                        'Category': cause.get('category', 'Unknown').replace('_', ' ').title(),
                        'Root Cause': cause.get('root_cause', 'Not specified'),
                        'Evidence': cause.get('evidence', 'No evidence'),
                        'Impact': cause.get('impact', 'Unknown impact'),
                        'Likelihood': self.create_likelihood_badge(cause.get('likelihood', 'unknown'))
                    })
                
                if root_cause_data:
                    dataframes['root_cause_analysis'] = pd.DataFrame(root_cause_data)
            
            # Performance recommendations
            recommendations = structured_data.get('performance_recommendations', [])
            if recommendations:
                rec_data = []
                for rec in recommendations:
                    rec_data.append({
                        'Category': rec.get('category', 'Unknown').replace('_', ' ').title(),
                        'Priority': self.create_priority_badge(rec.get('priority', 'unknown')),
                        'Recommendation': rec.get('recommendation', 'No recommendation'),
                        'Rationale': rec.get('rationale', 'No rationale provided')
                    })
                
                if rec_data:
                    dataframes['performance_recommendations'] = pd.DataFrame(rec_data)
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform bottleneck data to DataFrames: {e}")
            return {}
    
    def _identify_top_bottlenecks(self, bottlenecks: List[Dict[str, Any]]) -> List[int]:
        """Identify top severity bottlenecks for highlighting"""
        try:
            severity_order = {'high': 3, 'critical': 3, 'medium': 2, 'low': 1, 'unknown': 0}
            
            # Sort by severity and value
            indexed_bottlenecks = []
            for idx, bottleneck in enumerate(bottlenecks):
                severity_score = severity_order.get(bottleneck.get('severity', 'unknown').lower(), 0)
                value = float(bottleneck.get('value', 0)) if bottleneck.get('value') is not None else 0
                indexed_bottlenecks.append((idx, severity_score, value))
            
            # Sort by severity first, then by value
            indexed_bottlenecks.sort(key=lambda x: (x[1], x[2]), reverse=True)
            
            # Return indices of top bottlenecks (highest severity)
            if indexed_bottlenecks:
                top_severity = indexed_bottlenecks[0][1]
                return [idx for idx, severity, value in indexed_bottlenecks if severity == top_severity][:3]
            
            return []
        except Exception as e:
            logger.error(f"Error identifying top bottlenecks: {e}")
            return []
    
    def _format_bottleneck_value(self, value: Union[float, int], unit: str, bottleneck_type: str, is_top: bool = False) -> str:
        """Format bottleneck value with appropriate units and highlighting"""
        try:
            if value is None or value == 0:
                formatted = "0"
            else:
                unit_lower = unit.lower()
                type_lower = bottleneck_type.lower()
                
                # Network throughput/utilization
                if 'bytes_per_second' in unit_lower:
                    formatted = self.format_network_bytes_per_second(float(value))
                elif 'bits_per_second' in unit_lower:
                    formatted = self.format_network_bits_per_second(float(value))
                elif 'packets_per_second' in unit_lower:
                    formatted = self.format_network_packets_per_second(float(value))
                
                # Time/latency values
                elif 'seconds' in unit_lower and ('latency' in type_lower or 'duration' in type_lower):
                    formatted = self.format_network_latency_seconds(float(value))
                
                # Memory values
                elif 'percent' in unit_lower or 'percentage' in unit_lower:
                    formatted = self.format_percentage(float(value))
                elif 'mb' in unit_lower or 'gb' in unit_lower:
                    if float(value) > 1024 and 'mb' in unit_lower:
                        formatted = f"{float(value)/1024:.1f} GB"
                    else:
                        formatted = f"{float(value):.1f} {unit_lower.upper()}"
                
                # Count values
                elif 'count' in unit_lower or type_lower in ['proposal', 'operations']:
                    formatted = self.format_count_value(float(value))
                
                # Default formatting
                else:
                    if float(value) < 0.001:
                        formatted = f"{float(value):.6f}"
                    elif float(value) < 1:
                        formatted = f"{float(value):.3f}"
                    else:
                        formatted = f"{float(value):.2f}"
                    
                    if unit and unit != 'unknown':
                        formatted += f" {unit}"
            
            # Apply highlighting for top/critical values
            if is_top:
                return f'<span class="text-danger font-weight-bold bg-warning px-1">⚠️ {formatted}</span>'
            else:
                return formatted
                
        except (ValueError, TypeError) as e:
            logger.error(f"Error formatting bottleneck value: {e}")
            return str(value)
    
    def _assess_category_severity(self, bottlenecks: List[Dict[str, Any]]) -> str:
        """Assess overall severity for a category of bottlenecks"""
        if not bottlenecks:
            return '<span class="badge badge-success">None</span>'
        
        severities = [b.get('severity', '').lower() for b in bottlenecks]
        
        if 'critical' in severities or 'high' in severities:
            return '<span class="badge badge-danger">Critical</span>'
        elif 'medium' in severities:
            return '<span class="badge badge-warning">Medium</span>'
        elif 'low' in severities:
            return '<span class="badge badge-info">Low</span>'
        else:
            return '<span class="badge badge-secondary">Unknown</span>'
    
    def create_severity_badge(self, severity: str) -> str:
        """Create HTML badge for severity level"""
        severity_lower = severity.lower()
        
        if severity_lower in ['critical', 'high']:
            return f'<span class="badge badge-danger">⚠️ {severity.title()}</span>'
        elif severity_lower == 'medium':
            return f'<span class="badge badge-warning">{severity.title()}</span>'
        elif severity_lower == 'low':
            return f'<span class="badge badge-info">{severity.title()}</span>'
        else:
            return f'<span class="badge badge-secondary">{severity.title()}</span>'
    
    def create_likelihood_badge(self, likelihood: str) -> str:
        """Create HTML badge for likelihood level"""
        likelihood_lower = likelihood.lower()
        
        if likelihood_lower == 'high':
            return f'<span class="badge badge-danger">{likelihood.title()}</span>'
        elif likelihood_lower == 'medium':
            return f'<span class="badge badge-warning">{likelihood.title()}</span>'
        elif likelihood_lower == 'low':
            return f'<span class="badge badge-success">{likelihood.title()}</span>'
        else:
            return f'<span class="badge badge-secondary">{likelihood.title()}</span>'
    
    def create_priority_badge(self, priority: str) -> str:
        """Create HTML badge for priority level"""
        priority_lower = priority.lower()
        
        if priority_lower == 'high':
            return f'<span class="badge badge-danger">🔥 {priority.title()}</span>'
        elif priority_lower == 'medium':
            return f'<span class="badge badge-warning">{priority.title()}</span>'
        elif priority_lower == 'low':
            return f'<span class="badge badge-info">{priority.title()}</span>'
        else:
            return f'<span class="badge badge-secondary">{priority.title()}</span>'
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        try:
            html_tables = {}
            
            for table_name, df in dataframes.items():
                if df.empty:
                    continue
                
                html_tables[table_name] = self.create_html_table(df, table_name)
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Failed to generate HTML tables: {e}")
            return {}
    
    def summarize_bottleneck_analysis(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary of bottleneck analysis"""
        try:
            test_info = structured_data.get('test_info', {})
            
            # Count bottlenecks by category
            disk_count = len(structured_data.get('disk_io_bottlenecks', []))
            network_count = len(structured_data.get('network_bottlenecks', []))
            memory_count = len(structured_data.get('memory_bottlenecks', []))
            consensus_count = len(structured_data.get('consensus_bottlenecks', []))
            total_bottlenecks = disk_count + network_count + memory_count + consensus_count
            
            # Count critical issues
            all_bottlenecks = []
            all_bottlenecks.extend(structured_data.get('disk_io_bottlenecks', []))
            all_bottlenecks.extend(structured_data.get('network_bottlenecks', []))
            all_bottlenecks.extend(structured_data.get('memory_bottlenecks', []))
            all_bottlenecks.extend(structured_data.get('consensus_bottlenecks', []))
            
            critical_count = sum(1 for b in all_bottlenecks if b.get('severity', '').lower() in ['critical', 'high'])
            
            # Generate summary
            summary_parts = []
            
            summary_parts.append(f"<strong>Bottleneck Analysis Summary</strong>")
            summary_parts.append(f"Test ID: {test_info.get('test_id', 'N/A')}")
            
            if total_bottlenecks == 0:
                summary_parts.append("✅ No significant bottlenecks detected in the etcd cluster")
            else:
                summary_parts.append(f"⚠️ {total_bottlenecks} bottlenecks identified ({critical_count} critical/high severity)")
                
                # Break down by category
                if disk_count > 0:
                    summary_parts.append(f"• Disk I/O: {disk_count} issues")
                if network_count > 0:
                    summary_parts.append(f"• Network: {network_count} issues")
                if memory_count > 0:
                    summary_parts.append(f"• Memory: {memory_count} issues")
                if consensus_count > 0:
                    summary_parts.append(f"• Consensus: {consensus_count} issues")
            
            # Add root causes count
            root_causes = len(structured_data.get('root_cause_analysis', []))
            if root_causes > 0:
                summary_parts.append(f"🔍 {root_causes} root causes identified")
            
            # Add recommendations count
            recommendations = len(structured_data.get('performance_recommendations', []))
            if recommendations > 0:
                summary_parts.append(f"💡 {recommendations} performance recommendations provided")
            
            return "<br>".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate bottleneck summary: {e}")
            return f"Summary generation failed: {str(e)}"