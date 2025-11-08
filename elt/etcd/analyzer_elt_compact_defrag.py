"""
ELT module for etcd compact and defrag data processing
Specialized module for extracting, transforming and loading compact/defrag metrics
"""

import logging
from typing import Dict, Any, List, Union
import pandas as pd
from .etcd_analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)

class compactDefragELT(utilityELT):
    """Extract, Load, Transform class for compact defrag data"""
    
    def __init__(self):
        super().__init__()
        self.metric_categories = {
            'compaction': ['debugging_mvcc_db_compaction_duration_sum_delta', 'debugging_mvcc_db_compaction_duration_sum'],
            'defragmentation': ['disk_backend_defrag_duration_sum_rate', 'disk_backend_defrag_duration_sum'],
            'page_faults': ['vmstat_pgmajfault_rate', 'vmstat_pgmajfault_total']
        }
    
    def extract_compact_defrag(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract compact defrag data into structured format"""
        try:
            # Handle nested data structures and both 'metrics' and 'pods_metrics' layouts
            metrics_data = None
            master_nodes = []
            summary_data = {}
            category = data.get('category', 'disk_compact_defrag')
            timestamp = data.get('timestamp', '')
            duration = data.get('duration', '1h')

            # Handle the actual JSON structure from your data
            if isinstance(data.get('data'), dict):
                inner = data['data']
                if 'pods_metrics' in inner and isinstance(inner['pods_metrics'], dict):
                    metrics_data = inner['pods_metrics']
                    master_nodes = inner.get('master_nodes', [])
                    summary_data = inner.get('summary', {})
                elif 'metrics' in inner and isinstance(inner['metrics'], dict):
                    metrics_data = inner['metrics']
                    master_nodes = inner.get('master_nodes', [])
                    summary_data = inner.get('summary', {})
            elif 'pods_metrics' in data and isinstance(data['pods_metrics'], dict):
                metrics_data = data['pods_metrics']
                master_nodes = data.get('master_nodes', [])
                summary_data = data.get('summary', {})
            elif 'metrics' in data and isinstance(data['metrics'], dict):
                metrics_data = data['metrics']
                master_nodes = data.get('master_nodes', [])
                summary_data = data.get('summary', {})
            else:
                return {'error': 'No pods_metrics or metrics data found in compact defrag results'}
            
            structured = {
                'timestamp': timestamp,
                'duration': duration,
                'category': category,
                'master_nodes': master_nodes,
                'summary': summary_data,
                'metrics_overview': [],
                'compaction_metrics': [],
                'defragmentation_metrics': [],
                'page_fault_metrics': [],
                'snapshot_metrics': [],
                'pod_performance': [],
                'node_performance': []
            }
            
            # Process each metric
            for metric_name, metric_info in metrics_data.items():
                if metric_info.get('status') != 'success':
                    continue
                
                # Add to metrics overview (prefer 'overall' if present)
                overall = metric_info.get('overall', {}) or metric_info.get('statistics', {})
                # statistics may use nested {'avg': {'raw': ...}} in summaries; handle gracefully
                def get_stat(key):
                    val = overall.get(key, 0)
                    if isinstance(val, dict):
                        return val.get('raw', 0)
                    return val
                
                # Get raw numeric values for processing
                avg_raw = get_stat('avg')
                max_raw = get_stat('max') 
                latest_raw = get_stat('latest')
                
                overview_item = {
                    'Metric': self.format_metric_name(metric_name),
                    'Category': self.categorize_compact_defrag_metric(metric_name),
                    'Unit': metric_info.get('unit', ''),
                    'Average': avg_raw,  # Store raw numeric value
                    'Maximum': max_raw,  # Store raw numeric value  
                    'Latest': latest_raw,  # Store raw numeric value
                    'avg_raw': avg_raw,   # Keep raw values for highlighting
                    'max_raw': max_raw,
                    'latest_raw': latest_raw
                }
                structured['metrics_overview'].append(overview_item)
                
                # Process pod/instance data
                pod_data = metric_info.get('data', {})
                
                # Handle pod-based metrics (compaction, defragmentation)
                if 'pods' in pod_data:
                    for pod_name, pod_info in pod_data['pods'].items():
                        # Skip 'unknown' pods unless they have meaningful data
                        if pod_name == 'unknown' and pod_info.get('avg', 0) == 0:
                            continue
                            
                        avg_val = pod_info.get('avg', 0)
                        max_val = pod_info.get('max', 0)
                        latest_val = pod_info.get('latest', 0)
                        
                        pod_item = {
                            'Pod': self.truncate_node_name(pod_name, 30),
                            'Node': self.truncate_node_name(pod_info.get('node', 'unknown'), 25),
                            'Metric': self.format_metric_name(metric_name),
                            'Average': avg_val,  # Store raw numeric value
                            'Maximum': max_val,  # Store raw numeric value
                            'Latest': latest_val,  # Store raw numeric value
                            'avg_raw': avg_val,   # Keep for highlighting
                            'max_raw': max_val,
                            'latest_raw': latest_val,
                            'Unit': metric_info.get('unit', '')  # Store unit in separate column
                        }
                        structured['pod_performance'].append(pod_item)
                
                # Handle instance-based metrics (vmstat)
                elif 'instances' in pod_data:
                    for instance_name, instance_info in pod_data['instances'].items():
                        avg_val = instance_info.get('avg', 0)
                        max_val = instance_info.get('max', 0)
                        latest_val = instance_info.get('latest', 0)
                        
                        instance_item = {
                            'Instance': self.truncate_node_name(instance_name, 30),
                            'Node': self.truncate_node_name(instance_info.get('node', 'unknown'), 25),
                            'Metric': self.format_metric_name(metric_name),
                            'Average': avg_val,  # Store raw numeric value
                            'Maximum': max_val,  # Store raw numeric value
                            'Latest': latest_val,  # Store raw numeric value
                            'avg_raw': avg_val,   # Keep for highlighting
                            'max_raw': max_val,
                            'latest_raw': latest_val,
                            'unit': metric_info.get('unit', '')
                        }
                        structured['node_performance'].append(instance_item)
                
                # Categorize metrics
                category = self.categorize_compact_defrag_metric(metric_name)
                if category == 'Compaction':
                    structured['compaction_metrics'].append(overview_item)
                elif category == 'Defragmentation':
                    structured['defragmentation_metrics'].append(overview_item)
                elif category == 'Page Faults':
                    structured['page_fault_metrics'].append(overview_item)
                elif category == 'Snapshot':
                    structured['snapshot_metrics'].append(overview_item)
            
            return structured
            
        except Exception as e:
            logger.error(f"Error extracting compact defrag data: {e}")
            return {'error': str(e)}
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        try:
            dataframes = {}
            
            # Create DataFrames from lists
            for key, data_list in structured_data.items():
                if isinstance(data_list, list) and data_list and key != 'master_nodes':
                    df = pd.DataFrame(data_list)
                    if not df.empty:
                        # Apply column limiting
                        df = self.limit_dataframe_columns(df, table_name=key)
                        dataframes[key] = df
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Error transforming compact defrag data: {e}")
            return {}
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables with highlighting for compact defrag data"""
        try:
            html_tables = {}
            
            for table_name, df in dataframes.items():
                if df.empty:
                    continue
                
                # Apply highlighting based on table type
                df_styled = df.copy()
                
                if table_name == 'metrics_overview':
                    # Highlight top performers in overview
                    records = df.to_dict('records')
                    top_avg_indices = self.identify_top_values(records, 'avg_raw') if 'avg_raw' in df.columns else []
                    top_max_indices = self.identify_top_values(records, 'max_raw') if 'max_raw' in df.columns else []
                    
                    for idx, row in df_styled.iterrows():
                        unit = row.get('Unit', '')
                        
                        # Format and highlight average values
                        avg_raw = row.get('avg_raw', row.get('Average', 0))
                        if idx in top_avg_indices:
                            df_styled.loc[idx, 'Average'] = self.highlight_compact_defrag_values(
                                avg_raw, 'average', unit, is_top=True
                            )
                        else:
                            df_styled.loc[idx, 'Average'] = self.highlight_compact_defrag_values(
                                avg_raw, 'average', unit
                            )
                        
                        # Format and highlight maximum values
                        max_raw = row.get('max_raw', row.get('Maximum', 0))
                        if idx in top_max_indices:
                            df_styled.loc[idx, 'Maximum'] = self.highlight_compact_defrag_values(
                                max_raw, 'maximum', unit, is_top=True
                            )
                        else:
                            df_styled.loc[idx, 'Maximum'] = self.highlight_compact_defrag_values(
                                max_raw, 'maximum', unit
                            )
                        
                        # Format latest values
                        latest_raw = row.get('latest_raw', row.get('Latest', 0))
                        df_styled.loc[idx, 'Latest'] = self.highlight_compact_defrag_values(
                            latest_raw, 'latest', unit
                        )
                
                elif table_name in ['pod_performance', 'node_performance']:
                    # Create a copy to avoid dtype warnings
                    df_copy = df_styled.copy()
                    
                    # Convert numeric columns to object type to allow string assignment
                    for col in ['Average', 'Maximum', 'Latest']:
                        if col in df_copy.columns:
                            df_copy[col] = df_copy[col].astype('object')
                    
                    records = df.to_dict('records')
                    top_avg_indices = self.identify_top_values(records, 'avg_raw') if 'avg_raw' in df.columns else []
                    
                    for idx, row in df_copy.iterrows():
                        unit = row.get('Unit', '')
                        
                        # Format values with proper highlighting and units
                        avg_raw = row.get('avg_raw', row.get('Average', 0))
                        if idx in top_avg_indices:
                            formatted_avg = self.format_compact_defrag_value(avg_raw, unit)
                            df_copy.loc[idx, 'Average'] = f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted_avg}</span>'
                        else:
                            df_copy.loc[idx, 'Average'] = self.highlight_compact_defrag_values(avg_raw, 'performance', unit)
                        
                        max_raw = row.get('max_raw', row.get('Maximum', 0))
                        latest_raw = row.get('latest_raw', row.get('Latest', 0))
                        
                        df_copy.loc[idx, 'Maximum'] = self.highlight_compact_defrag_values(max_raw, 'performance', unit)
                        df_copy.loc[idx, 'Latest'] = self.highlight_compact_defrag_values(latest_raw, 'performance', unit)
                    
                    df_styled = df_copy

                # Remove helper columns before HTML generation
                columns_to_drop = ['avg_raw', 'max_raw', 'latest_raw', 'Unit']
                for col in columns_to_drop:
                    if col in df_styled.columns:
                        df_styled = df_styled.drop(columns=[col])
                                
                # Remove "Node" column from Pod Performance table for cleaner display
                if table_name == 'pod_performance' and 'Node' in df_styled.columns:
                    try:
                        df_styled = df_styled.drop(columns=['Node'])
                    except Exception:
                        pass

                # Generate HTML table
                html_tables[table_name] = self.create_html_table(df_styled, table_name)
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Error generating HTML tables: {e}")
            return {}
    
    def summarize_compact_defrag(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary for compact defrag data"""
        try:
            summary_parts = []
            
            # Basic info
            duration = structured_data.get('duration', '1h')
            master_nodes_count = len(structured_data.get('master_nodes', []))
            total_metrics = len(structured_data.get('metrics_overview', []))
            
            summary_parts.append(f"Compact & Defrag Analysis ({duration})")
            summary_parts.append(f"• {total_metrics} metrics collected across {master_nodes_count} master nodes")
            
            # Compaction summary
            compaction_metrics = structured_data.get('compaction_metrics', [])
            if compaction_metrics:
                avg_compaction_time = sum([m.get('avg_raw', m.get('Average', 0)) for m in compaction_metrics]) / len(compaction_metrics)
                summary_parts.append(f"• Average compaction duration: {self.format_compact_defrag_value(avg_compaction_time, 'milliseconds')}")
            
            # Defragmentation summary
            defrag_metrics = structured_data.get('defragmentation_metrics', [])
            if defrag_metrics:
                total_defrag_time = sum([m.get('avg_raw', m.get('Average', 0)) for m in defrag_metrics])
                summary_parts.append(f"• Total defragmentation activity: {self.format_compact_defrag_value(total_defrag_time, 'seconds')}")
            
            # Page faults summary
            page_fault_metrics = structured_data.get('page_fault_metrics', [])
            if page_fault_metrics:
                avg_page_faults = sum([m.get('avg_raw', m.get('Average', 0)) for m in page_fault_metrics]) / len(page_fault_metrics)
                summary_parts.append(f"• Average page fault rate: {self.format_compact_defrag_value(avg_page_faults, 'faults/s')}")
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return f"Summary generation failed: {str(e)}"
    
    def format_metric_name(self, metric_name: str) -> str:
        """Format metric name for display"""
        name_mapping = {
            'debugging_mvcc_db_compacted_keys': 'DB Compacted Keys',
            'debugging_mvcc_db_compaction_duration_sum_delta': 'DB Compaction Rate',
            'debugging_mvcc_db_compaction_duration_sum': 'DB Compaction Duration',
            'debugging_snapshot_duration': 'Snapshot Duration',
            'disk_backend_defrag_duration_sum_rate': 'Defrag Rate',
            'disk_backend_defrag_duration_sum': 'Defrag Duration',
            'vmstat_pgmajfault_rate': 'Page Fault Rate',
            'vmstat_pgmajfault_total': 'Total Page Faults'
        }
        return name_mapping.get(metric_name, metric_name.replace('_', ' ').title())
    
    def categorize_compact_defrag_metric(self, metric_name: str) -> str:
        """Categorize compact defrag metric type"""
        metric_lower = metric_name.lower()
        
        if 'compaction' in metric_lower or 'compacted' in metric_lower:
            return 'Compaction'
        elif 'defrag' in metric_lower:
            return 'Defragmentation'
        elif 'snapshot' in metric_lower:
            return 'Snapshot'
        elif 'pgmajfault' in metric_lower or 'page' in metric_lower:
            return 'Page Faults'
        else:
            return 'Other'
    
    def format_compact_defrag_value(self, value: Union[float, int], unit: str) -> str:
        """Format compact defrag values with appropriate units"""
        try:
            # Convert to float for processing
            if isinstance(value, str):
                # Try to extract numeric value from string
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    # If can't convert, try to extract first number
                    import re
                    numbers = re.findall(r'[\d.]+', str(value))
                    if numbers:
                        value = float(numbers[0])
                    else:
                        return str(value)
            
            value = float(value)
            
            if unit == 'count':
                # Handle count values (like compacted keys)
                if value == 0:
                    return "0"
                elif value >= 1000000:
                    return f"{value/1000000:.1f}M"
                elif value >= 1000:
                    return f"{value/1000:.1f}K"
                else:
                    return f"{value:,.0f}"
            elif unit == 'milliseconds' or unit == 'ms':
                if value == 0:
                    return "0 ms"
                elif value < 1:
                    return f"{value*1000:.0f} μs"
                elif value < 1000:
                    return f"{value:.1f} ms"
                else:
                    return f"{value/1000:.2f} s"
            elif unit == 'seconds' or unit == 's':
                if value == 0:
                    return "0 s"
                elif abs(value) < 1e-6:  # Very small values (like snapshot duration)
                    return f"{value*1000000:.3f} μs"
                elif abs(value) < 1e-3:
                    return f"{value*1000:.3f} ms"
                elif value < 1:
                    return f"{value*1000:.1f} ms"
                elif value < 60:
                    return f"{value:.3f} s"
                else:
                    return f"{value/60:.1f} min"
            elif unit in ['faults/s', 'faults']:
                if value == 0:
                    return "0 faults/s" if unit == 'faults/s' else "0 faults"
                elif value < 0.001:
                    return f"{value*1000:.2f} mfaults/s"
                elif value < 1:
                    return f"{value:.3f} faults/s"
                else:
                    return f"{value:.1f} faults/s"
            else:
                if value == 0:
                    return "0"
                else:
                    return f"{value:.2f}"
                    
        except (ValueError, TypeError):
            return str(value)   

    def highlight_compact_defrag_values(self, value: Union[float, int], metric_type: str, unit: str = "", is_top: bool = False) -> str:
        """Highlight compact defrag values with metric-specific thresholds"""
        try:
            # Convert value to float for processing
            if isinstance(value, str):
                try:
                    # Try to extract numeric value from formatted string
                    import re
                    numbers = re.findall(r'[\d.]+', value)
                    if numbers:
                        numeric_value = float(numbers[0])
                    else:
                        # If no numbers found, return the original string
                        return str(value)
                except (ValueError, TypeError):
                    return str(value)
            else:
                numeric_value = float(value)
            
            thresholds = self._get_compact_defrag_thresholds(metric_type)
            
            # Format the value first
            formatted_value = self.format_compact_defrag_value(numeric_value, unit)
            
            if is_top:
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted_value}</span>'
            
            if thresholds and isinstance(numeric_value, (int, float)):
                critical = thresholds.get('critical', float('inf'))
                warning = thresholds.get('warning', float('inf'))
                
                if numeric_value >= critical:
                    return f'<span class="text-danger font-weight-bold">⚠️ {formatted_value}</span>'
                elif numeric_value >= warning:
                    return f'<span class="text-warning font-weight-bold">{formatted_value}</span>'
                else:
                    return f'<span class="text-success">{formatted_value}</span>'
            else:
                return formatted_value
                
        except (ValueError, TypeError):
            return str(value)
    
    def _get_compact_defrag_thresholds(self, metric_type: str) -> Dict[str, float]:
        """Get thresholds for compact defrag metrics"""
        thresholds_map = {
            'compaction_duration': {'warning': 50, 'critical': 100},  # milliseconds
            'defrag_duration': {'warning': 1, 'critical': 5},         # seconds
            'page_fault_rate': {'warning': 0.1, 'critical': 1.0},    # faults/s
            'page_fault_total': {'warning': 20000, 'critical': 50000} # total faults
        }
        
        for key, threshold in thresholds_map.items():
            if key in metric_type.lower():
                return threshold
        
        return {}