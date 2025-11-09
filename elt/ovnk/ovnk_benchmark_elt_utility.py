"""
Utility functions for OpenShift Benchmark ELT modules
Common functions used across multiple ELT modules
Updated to support optimized latency ELT
"""

import logging
import re
from typing import Dict, Any, List, Union, Tuple
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

class EltUtility:
    """Common utility functions for ELT modules"""
    
    def __init__(self):
        self.max_columns = 6
    
    def truncate_text(self, text: str, max_length: int = 30, suffix: str = '...') -> str:
        """Truncate text for display"""
        if len(text) <= max_length:
            return text
        return text[:max_length-len(suffix)] + suffix
    
    def truncate_url(self, url: str, max_length: int = 50) -> str:
        """Truncate URL for display"""
        return self.truncate_text(url, max_length)
    
    def truncate_node_name(self, name: str, max_length: int = 25) -> str:
        """Truncate node name for display"""
        return self.truncate_text(name, max_length)
    
    def truncate_kernel_version(self, kernel_ver: str, max_length: int = 30) -> str:
        """Truncate kernel version for display"""
        return self.truncate_text(kernel_ver, max_length)
    
    def truncate_runtime(self, runtime: str, max_length: int = 25) -> str:
        """Truncate container runtime for display"""
        if len(runtime) <= max_length:
            return runtime
        # Try to keep the version part
        if '://' in runtime:
            protocol, version = runtime.split('://', 1)
            return f"{protocol}://{version[:max_length-len(protocol)-6]}..."
        return runtime[:max_length-3] + '...'
    
    def truncate_os_image(self, os_image: str, max_length: int = 35) -> str:
        """Truncate OS image for display"""
        return self.truncate_text(os_image, max_length)
    
    def parse_cpu_capacity(self, cpu_str: str) -> int:
        """Parse CPU capacity string to integer"""
        try:
            # Handle formats like "32", "32000m"
            if cpu_str.endswith('m'):
                return int(cpu_str[:-1]) // 1000
            return int(cpu_str)
        except (ValueError, TypeError):
            return 0
    
    def parse_memory_capacity(self, memory_str: str) -> float:
        """Parse memory capacity string to GB"""
        try:
            if memory_str.endswith('Ki'):
                # Convert KiB to GB
                kib = int(memory_str[:-2])
                return kib / (1024 * 1024)  # KiB to GB
            elif memory_str.endswith('Mi'):
                # Convert MiB to GB  
                mib = int(memory_str[:-2])
                return mib / 1024  # MiB to GB
            elif memory_str.endswith('Gi'):
                # Already in GiB, close enough to GB
                return float(memory_str[:-2])
            else:
                # Assume it's already in bytes, convert to GB
                return int(memory_str) / (1024**3)
        except (ValueError, TypeError):
            return 0.0
    
    def format_memory_display(self, memory_str: str) -> str:
        """Format memory for display"""
        try:
            gb_value = self.parse_memory_capacity(memory_str)
            if gb_value >= 1:
                return f"{gb_value:.0f} GB"
            else:
                # Show in MB for small values
                return f"{gb_value * 1024:.0f} MB"
        except:
            return memory_str
    
    def categorize_resource_type(self, resource_name: str) -> str:
        """Categorize resource type for better organization"""
        resource_lower = resource_name.lower()
        
        if any(keyword in resource_lower for keyword in ['network', 'policy', 'egress', 'udn']):
            return 'Network & Security'
        elif any(keyword in resource_lower for keyword in ['config', 'secret']):
            return 'Configuration'
        elif any(keyword in resource_lower for keyword in ['pod', 'service']):
            return 'Workloads'
        elif any(keyword in resource_lower for keyword in ['namespace']):
            return 'Organization'
        else:
            return 'Other'
    
    def format_timestamp(self, timestamp_str: str, length: int = 19) -> str:
        """Format timestamp for display"""
        if not timestamp_str:
            return 'Unknown'
        return timestamp_str[:length]
    
    def clean_html(self, html: str) -> str:
        """Clean up HTML (remove newlines and extra whitespace)"""
        html = re.sub(r'\s+', ' ', html.replace('\n', ' ').replace('\r', ''))
        return html.strip()
    
    def limit_dataframe_columns(self, df: pd.DataFrame, max_cols: int = None, table_name: str = None) -> pd.DataFrame:
        """Limit DataFrame columns to maximum number - Updated with enhanced latency support"""
        if max_cols is None:
            max_cols = self.max_columns

        # Enhanced handling for latencyELT tables
        if table_name and table_name.startswith('latencyelt_'):
            if table_name in ['latencyelt_collection_info', 'latencyelt_essential_summary']:
                max_cols = 2  # Property-value format
            elif table_name == 'latencyelt_top_latencies_ranking':
                max_cols = 6  # Show rank, metric, component, latency, severity, data points
            elif table_name == 'latencyelt_controller_sync_top20':
                max_cols = 5  # Show rank, pod, node, resource, latency
            elif table_name == 'latencyelt_all_metrics_top5':
                max_cols = 7  # Show metric, component, category, rank, pod, node, latency
            elif table_name == 'latencyelt_critical_findings':
                max_cols = 3  # Show type, description, details
            elif table_name in ['latencyelt_ready_duration', 'latencyelt_sync_duration', 
                            'latencyelt_cni_latency', 'latencyelt_pod_annotation',
                            'latencyelt_pod_creation', 'latencyelt_service_latency', 
                            'latencyelt_network_config']:
                max_cols = 6  # Show more columns for detailed latency metrics

        if len(df.columns) <= max_cols:
            return df
        
        # Enhanced priority columns for latencyELT tables
        if table_name and table_name.startswith('latencyelt_'):
            if 'ranking' in table_name or 'top20' in table_name:
                priority_cols = ['rank', 'metric name', 'metric_name', 'pod name', 'pod_name', 'node name', 'node_name', 'latency', 'max latency', 'component', 'severity']
            elif 'top5' in table_name or 'all_metrics' in table_name:
                priority_cols = ['metric name', 'metric_name', 'component', 'category', 'rank', 'pod name', 'pod_name', 'node name', 'node_name', 'latency', 'value']
            else:
                priority_cols = ['property', 'value', 'metric', 'component', 'max_value', 'avg_value', 'severity', 'rank']
        else:
            # Default priority columns for other tables
            priority_cols = ['name', 'status', 'value', 'count', 'property', 'rank', 'node', 'type', 'ready', 'cpu', 'memory', 'metric']
        
        # Find priority columns that exist
        keep_cols = []
        for col in df.columns:
            col_lower = col.lower()
            if any(priority in col_lower for priority in priority_cols):
                keep_cols.append(col)
        
        # Add remaining columns up to limit
        remaining_cols = [col for col in df.columns if col not in keep_cols]
        while len(keep_cols) < max_cols and remaining_cols:
            keep_cols.append(remaining_cols.pop(0))
        
        return df[keep_cols[:max_cols]]

    def create_property_value_table(self, data: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """Create a property-value table format"""
        return [{'Property': item.get('Property', ''), 'Value': str(item.get('Value', ''))} for item in data]
    
    def calculate_totals_from_nodes(self, nodes: List[Dict[str, Any]]) -> Dict[str, Union[int, float]]:
        """Calculate total CPU and memory from node list"""
        total_cpu = sum(self.parse_cpu_capacity(node.get('cpu_capacity', '0')) for node in nodes)
        total_memory_gb = sum(self.parse_memory_capacity(node.get('memory_capacity', '0Ki')) for node in nodes)
        ready_count = sum(1 for node in nodes if 'Ready' in node.get('ready_status', ''))
        schedulable_count = sum(1 for node in nodes if node.get('schedulable', False))
        
        return {
            'total_cpu': total_cpu,
            'total_memory_gb': total_memory_gb,
            'ready_count': ready_count,
            'schedulable_count': schedulable_count,
            'count': len(nodes)
        }

    def format_latency_value(self, value: Union[float, int], unit: str = 'seconds') -> str:
        """Format latency value with appropriate unit for display"""
        try:
            if unit == 'seconds' and isinstance(value, (int, float)):
                if value == 0:
                    return '0 ms'
                elif value < 1:
                    return f"{round(value * 1000, 2)} ms"
                elif value < 60:
                    return f"{round(value, 3)} s"
                elif value < 3600:
                    return f"{round(value / 60, 2)} min"
                else:
                    return f"{round(value / 3600, 2)} h"
            else:
                return f"{round(float(value), 4)} {unit}"
        except (ValueError, TypeError):
            return str(value)

    def format_latency_for_summary(self, value_seconds: float) -> Dict[str, Union[str, float]]:
        """Format latency for summary blocks as a dict with value and unit."""
        try:
            if value_seconds < 1:
                return {'value': round(value_seconds * 1000, 2), 'unit': 'ms'}
            elif value_seconds < 60:
                return {'value': round(value_seconds, 3), 'unit': 's'}
            elif value_seconds < 3600:
                return {'value': round(value_seconds / 60, 2), 'unit': 'min'}
            else:
                return {'value': round(value_seconds / 3600, 2), 'unit': 'h'}
        except Exception:
            return {'value': value_seconds, 'unit': 'seconds'}

    def categorize_latency_severity(self, value: float, unit: str = 'seconds') -> str:
        """Categorize latency severity for color coding"""
        try:
            if unit == 'seconds':
                if value < 0.1:  # Less than 100ms
                    return 'low'
                elif value < 1.0:  # Less than 1 second
                    return 'medium'
                elif value < 5.0:  # Less than 5 seconds
                    return 'high'
                else:  # 5+ seconds
                    return 'critical'
            else:
                # For other units, assume already in appropriate scale
                return 'medium'
        except (ValueError, TypeError):
            return 'unknown'

    def truncate_metric_name(self, metric_name: str, max_length: int = 30) -> str:
        """Truncate metric name for display while preserving key information"""
        if len(metric_name) <= max_length:
            return metric_name
        
        # Try to preserve important parts like percentile info
        if 'p99' in metric_name:
            base_name = metric_name.replace('_p99', '').replace('p99', '')
            truncated = self.truncate_text(base_name, max_length - 4)
            return f"{truncated} p99"
        elif 'p95' in metric_name:
            base_name = metric_name.replace('_p95', '').replace('p95', '')
            truncated = self.truncate_text(base_name, max_length - 4)
            return f"{truncated} p95"
        else:
            return self.truncate_text(metric_name, max_length)

    def extract_component_from_metric(self, metric_name: str) -> str:
        """Extract component type from metric name"""
        metric_lower = metric_name.lower()
        
        if any(keyword in metric_lower for keyword in ['controller', 'ovnkube_controller']):
            return 'Controller'
        elif any(keyword in metric_lower for keyword in ['node', 'ovnkube_node']):
            return 'Node'
        elif any(keyword in metric_lower for keyword in ['cni']):
            return 'CNI'
        elif any(keyword in metric_lower for keyword in ['pod']):
            return 'Pod'
        elif any(keyword in metric_lower for keyword in ['service']):
            return 'Service'
        elif any(keyword in metric_lower for keyword in ['network']):
            return 'Network'
        else:
            return 'Unknown'

    def create_latency_severity_badge(self, value: float, unit: str = 'seconds') -> str:
        """Create HTML badge for latency severity"""
        severity = self.categorize_latency_severity(value, unit)
        formatted_value = self.format_latency_value(value, unit)
        
        badge_colors = {
            'low': 'success',
            'medium': 'warning', 
            'high': 'danger',
            'critical': 'dark',
            'unknown': 'secondary'
        }
        
        color = badge_colors.get(severity, 'secondary')
        return f'<span class="badge badge-{color}">{formatted_value}</span>'

    def resolve_pod_and_node_names(self, entry: Dict[str, Any]) -> Tuple[str, str]:
        """Resolve pod_name and node_name from multiple possible keys (including nested dicts)."""
        def first_non_empty(d: Dict[str, Any], keys: List[str], default: str) -> str:
            for k in keys:
                v = d.get(k)
                if isinstance(v, str) and v.strip():
                    return v
            # Also check nested 'labels' and 'metadata' if present
            labels = d.get('labels') if isinstance(d.get('labels'), dict) else {}
            for k in keys:
                v = labels.get(k)
                if isinstance(v, str) and v.strip():
                    return v
            meta = d.get('metadata') if isinstance(d.get('metadata'), dict) else {}
            for k in keys:
                v = meta.get(k)
                if isinstance(v, str) and v.strip():
                    return v
            return default

        pod_name = first_non_empty(entry, ['pod_name', 'pod', 'podname', 'podName'], 'N/A')
        node_name = first_non_empty(entry, ['node_name', 'node', 'instance', 'nodename', 'nodeName', 'host'], 'unknown')
        return pod_name, node_name

    def create_status_badge(self, status: str, value: str = None) -> str:
        """Create HTML badge for status with optional value"""
        badge_colors = {
            'success': 'success',
            'warning': 'warning',
            'danger': 'danger',
            'info': 'info',
            'critical': 'danger',
            'high': 'warning',
            'medium': 'info',
            'low': 'success',
            'normal': 'success'
        }
        
        color = badge_colors.get(status.lower(), 'secondary')
        display_text = value if value else status.title()
        return f'<span class="badge badge-{color}">{display_text}</span>'

    def highlight_critical_values(self, value: float, thresholds: Dict[str, float], unit: str = "") -> str:
        """Highlight critical values with color coding"""
        critical = thresholds.get('critical', 90)
        warning = thresholds.get('warning', 70)
        
        if value >= critical:
            return f'<span class="text-danger font-weight-bold">{value}{unit}</span>'
        elif value >= warning:
            return f'<span class="text-warning font-weight-bold">{value}{unit}</span>'
        else:
            return f'{value}{unit}'

    # Enhanced latencyELT utility functions
    def truncate_latencyelt_pod_name(self, pod_name: str, max_length: int = 25) -> str:
        """Truncate pod name for latencyELT tables with intelligent shortening"""
        if len(pod_name) <= max_length:
            return pod_name
        
        # For OVN pods, keep the important parts
        if 'ovnkube' in pod_name:
            if 'controller' in pod_name:
                return pod_name.replace('ovnkube-controller-', 'ovn-ctrl-')[:max_length]
            elif 'node' in pod_name:
                parts = pod_name.split('-')
                if len(parts) >= 3:
                    return f"ovn-node-{parts[-1]}"
            elif 'master' in pod_name:
                return pod_name.replace('ovnkube-master-', 'ovn-mstr-')[:max_length]
        
        return self.truncate_text(pod_name, max_length)

    def create_latencyelt_component_badge(self, component: str) -> str:
        """Create component badge for latencyELT with appropriate styling"""
        component_colors = {
            'controller': 'primary',
            'node': 'info',
            'cni': 'success',
            'unknown': 'secondary'
        }
        
        color = component_colors.get(component.lower(), 'secondary')
        return f'<span class="badge badge-{color}">{component.title()}</span>'

    def categorize_latencyelt_severity_enhanced(self, max_value: float, avg_value: float, data_points: int, unit: str = 'seconds') -> Dict[str, str]:
        """Enhanced severity categorization for latencyELT metrics"""
        try:
            if unit == 'seconds':
                # Enhanced thresholds based on metric impact
                if max_value > 300:  # > 5 minutes - system critical
                    max_severity = 'critical'
                    impact = 'system_critical'
                elif max_value > 60:  # > 1 minute - service critical
                    max_severity = 'critical' 
                    impact = 'service_critical'
                elif max_value > 10:  # > 10 seconds - performance critical
                    max_severity = 'high'
                    impact = 'performance_critical'
                elif max_value > 2:   # > 2 seconds - noticeable delay
                    max_severity = 'medium'
                    impact = 'noticeable'
                elif max_value > 0.5: # > 500ms - minor impact
                    max_severity = 'low'
                    impact = 'minor'
                else:
                    max_severity = 'excellent'
                    impact = 'minimal'
                
                # Average value assessment
                if avg_value > 30:
                    avg_severity = 'critical'
                elif avg_value > 5:
                    avg_severity = 'high' 
                elif avg_value > 1:
                    avg_severity = 'medium'
                elif avg_value > 0.2:
                    avg_severity = 'low'
                else:
                    avg_severity = 'excellent'
                
                # Data reliability factor
                reliability = 'high' if data_points > 10 else 'medium' if data_points > 3 else 'low'
                
                return {
                    'max_severity': max_severity,
                    'avg_severity': avg_severity,
                    'overall': max_severity if max_severity in ['critical'] else avg_severity,
                    'impact_level': impact,
                    'data_reliability': reliability,
                    'is_critical': max_severity == 'critical' or avg_severity == 'critical'
                }
            else:
                return {
                    'max_severity': 'unknown',
                    'avg_severity': 'unknown', 
                    'overall': 'unknown',
                    'impact_level': 'unknown',
                    'data_reliability': 'unknown',
                    'is_critical': False
                }
        except (ValueError, TypeError):
            return {
                'max_severity': 'unknown',
                'avg_severity': 'unknown',
                'overall': 'unknown', 
                'impact_level': 'unknown',
                'data_reliability': 'unknown',
                'is_critical': False
            }

    def decode_unicode_escapes(self, text: str) -> str:
        """Decode Unicode escape sequences to actual Unicode characters"""
        try:
            original = text
            # Handle common Unicode escape patterns
            # First decode standard Unicode escapes like \u26a0
            text = text.encode('utf-8').decode('unicode_escape')
            
            # Additional specific replacements for common sequences
            replacements = {
                "\\u26a0\\ufe0f": "⚠️",  # warning sign with variation selector
                "\\u26a0": "⚠",         # warning sign
                "\\u2022": "•",         # bullet point
                "\\u2713": "✓",         # check mark
                "\\u2717": "✗",         # cross mark
                "\\u27a1": "➡",         # right arrow
                "\\u2b06": "⬆",         # up arrow
                "\\u2b07": "⬇",         # down arrow
                "\\u1f3c6": "🏆",       # trophy
                "\\u1f4ca": "📊",       # bar chart
                "\\u1f4c8": "📈",       # chart increasing
                "\\u1f4c9": "📉",       # chart decreasing
                "\\u1f534": "🔴",       # red circle
                "\\u1f7e1": "🟡",       # yellow circle
                "\\u1f7e2": "🟢",       # green circle
                "\\ufe0f": "",          # variation selector (remove if standalone)
            }
            
            for escape_seq, replacement in replacements.items():
                text = text.replace(escape_seq, replacement)
            
            # Fix common UTF-8 mojibake sequences (double-encoded as Latin-1)
            try:
                if any(marker in text for marker in ["Ã", "â€", "â„¢", "œ"]):
                    text = text.encode('latin1', errors='ignore').decode('utf-8', errors='ignore')
            except Exception:
                pass
            
            return text
            
        except (UnicodeDecodeError, UnicodeError) as e:
            logger.warning(f"Failed to decode Unicode escapes in text: {e}")
            # Fallback: just replace the specific sequences we know about
            fallback_replacements = {
                "\\u26a0\\ufe0f": "⚠️",
                "\\u26a0": "⚠",
                "\\u2022": "•",
                "\\u2713": "✓",
                "\\u2717": "✗",
                "\\u1f3c6": "🏆",
            }
            for escape_seq, replacement in fallback_replacements.items():
                text = text.replace(escape_seq, replacement)
            return text

    def create_html_table(self, df: pd.DataFrame, table_name: str) -> str:
        """Generate HTML table from DataFrame with improved styling and Unicode handling"""
        try:
            if df.empty:
                return ""
            
            # Create a copy to avoid modifying the original DataFrame
            df_copy = df.copy()
            
            # If columns are MultiIndex, flatten them to single string names
            try:
                if isinstance(df_copy.columns, pd.MultiIndex):
                    df_copy.columns = [
                        "_".join([str(part) for part in col if part is not None])
                        for col in df_copy.columns.values
                    ]
                else:
                    # Ensure all column names are strings
                    df_copy.columns = [str(c) for c in df_copy.columns]
            except Exception:
                # Best-effort fallback: cast column names to strings
                df_copy.columns = [str(c) for c in df_copy.columns]

            # Apply Unicode decoding to all object (string-like) columns safely
            try:
                object_columns = list(df_copy.select_dtypes(include=['object']).columns)
                for col in object_columns:
                    df_copy[col] = df_copy[col].astype(str).apply(self.decode_unicode_escapes)
            except Exception:
                pass
            
            # Create styled HTML table
            html = df_copy.to_html(
                index=False,
                classes='table table-striped table-bordered table-sm',
                escape=False,
                table_id=f"table-{table_name.replace('_', '-')}",
                border=1
            )
            
            # Additional cleanup for any remaining Unicode issues
            html = self.decode_unicode_escapes(html)
            
            # Clean up HTML
            html = self.clean_html(html)
            
            # Add responsive wrapper
            html = f'<div class="table-responsive">{html}</div>'
            
            return html
            
        except Exception as e:
            logger.error(f"Failed to generate HTML table for {table_name}: {e}")
            return f'<div class="alert alert-danger">Error generating table: {str(e)}</div>'            