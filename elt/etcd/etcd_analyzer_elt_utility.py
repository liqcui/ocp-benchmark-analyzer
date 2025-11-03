"""
Utility functions for ETCD Analyzer ELT modules
Common functions used across multiple ELT modules
"""

import logging
import re
from typing import Dict, Any, List, Union, Tuple
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

class utilityELT:
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
    
    def format_timestamp(self, timestamp_str: str, length: int = 19) -> str:
        """Format timestamp for display"""
        if not timestamp_str:
            return 'Unknown'
        return timestamp_str[:length]
    
    def clean_html(self, html: str) -> str:
        """Clean up HTML (remove newlines and extra whitespace)"""
        html = re.sub(r'\s+', ' ', html.replace('\n', ' ').replace('\r', ''))
        return html.strip()
    
    def decode_unicode_escapes(self, text: str) -> str:
        """Decode Unicode escape sequences to actual Unicode characters"""
        try:
            original = text
            # Handle common Unicode escape patterns
            # First decode standard Unicode escapes like \u26a0
            text = text.encode('utf-8').decode('unicode_escape')
            
            # Additional specific replacements for common sequences that might not decode properly
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
            # e.g., "\u00C3\u0083\u00C2\u00A2..." → "⚠️" or proper symbols
            try:
                # If text contains typical mojibake markers after unicode unescape
                if any(marker in text for marker in ["Ã", "Â", "â", "€", "™", "", "ï", "¸", ""]):
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
            
            # If columns are MultiIndex, flatten them to single string names to avoid Series/DataFrame ambiguity
            try:
                if isinstance(df_copy.columns, pd.MultiIndex):
                    df_copy.columns = [
                        "_".join([str(part) for part in col if part is not None])
                        for col in df_copy.columns.values
                    ]
                else:
                    # Ensure all column names are strings for consistent handling
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
 
    def limit_dataframe_columns(self, df: pd.DataFrame, max_cols: int = None, table_name: str = None) -> pd.DataFrame:
        """Limit DataFrame columns to maximum number"""
        if max_cols is None:
            max_cols = self.max_columns

        if len(df.columns) <= max_cols:
            return df
        
        # Priority columns for cluster info tables
        if table_name and 'cluster' in table_name.lower():
            priority_cols = ['property', 'value', 'name', 'status', 'count', 'cpu', 'memory', 'type', 'ready', 'schedulable']
        else:
            # Default priority columns
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

    def highlight_critical_values(self, value: Union[float, int], thresholds: Dict[str, float], unit: str = "", is_top: bool = False) -> str:
        """Highlight critical values with color coding"""
        critical = thresholds.get('critical', 90)
        warning = thresholds.get('warning', 70)
        
        # Add top 1 highlighting
        if is_top:
            # For CPU/Memory columns in node detail tables we avoid the trophy icon
            return f'<span class="text-primary font-weight-bold bg-light px-1">{value}{unit}</span>'
        elif value >= critical:
            return f'<span class="text-danger font-weight-bold">⚠️ {value}{unit}</span>'
        elif value >= warning:
            return f'<span class="text-warning font-weight-bold">{value}{unit}</span>'
        else:
            return f'{value}{unit}'
    
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

    def identify_top_values(self, data: List[Dict[str, Any]], value_key: str) -> List[int]:
        """Identify indices of top values for highlighting"""
        try:
            values = [(i, float(item.get(value_key, 0))) for i, item in enumerate(data)]
            values.sort(key=lambda x: x[1], reverse=True)
            return [values[0][0]] if values else []  # Return index of top 1
        except (ValueError, TypeError):
            return []

    def parse_db_size(self, size_str: str) -> float:
        """Parse database size string to MB"""
        try:
            size_str = size_str.upper().strip()
            if 'MB' in size_str:
                return float(size_str.replace('MB', '').strip())
            elif 'GB' in size_str:
                return float(size_str.replace('GB', '').strip()) * 1024
            elif 'KB' in size_str:
                return float(size_str.replace('KB', '').strip()) / 1024
            else:
                return 0.0
        except (ValueError, AttributeError):
            return 0.0

    def format_db_size_display(self, size_mb: float) -> str:
        """Format database size with appropriate units"""
        if size_mb == 0:
            return "0 MB"
        elif size_mb >= 1024:
            return f"{size_mb / 1024:.1f} GB"
        else:
            return f"{size_mb:.0f} MB"

    def create_leader_badge(self, is_leader: bool) -> str:
        """Create HTML badge for leader status"""
        if is_leader:
            return '<span class="badge badge-success">LEADER</span>'
        else:
            return '<span class="text-muted">false</span>'

    def categorize_etcd_resource_type(self, resource_name: str) -> str:
        """Categorize etcd resource type for better organization"""
        resource_lower = resource_name.lower()
        
        if any(keyword in resource_lower for keyword in ['endpoint', 'status']):
            return 'Endpoint Status'
        elif any(keyword in resource_lower for keyword in ['health', 'cluster_health']):
            return 'Health Monitoring'
        elif any(keyword in resource_lower for keyword in ['member', 'leader']):
            return 'Membership & Leadership'
        elif any(keyword in resource_lower for keyword in ['metrics', 'db_size']):
            return 'Performance Metrics'
        else:
            return 'Cluster Configuration'

    def format_bytes_per_second(self, bytes_per_sec: float) -> str:
        """Format bytes per second to readable units"""
        try:
            if bytes_per_sec == 0:
                return "0 B/s"
            elif bytes_per_sec < 1024:
                return f"{bytes_per_sec:.0f} B/s"
            elif bytes_per_sec < 1024**2:
                return f"{bytes_per_sec/1024:.1f} KB/s"
            elif bytes_per_sec < 1024**3:
                return f"{bytes_per_sec/(1024**2):.1f} MB/s"
            else:
                return f"{bytes_per_sec/(1024**3):.2f} GB/s"
        except (ValueError, TypeError):
            return str(bytes_per_sec)

    def format_operations_per_second(self, ops_per_sec: float) -> str:
        """Format operations per second to readable units"""
        try:
            if ops_per_sec == 0:
                return "0 IOPS"
            elif ops_per_sec < 1:
                return f"{ops_per_sec:.3f} IOPS"
            elif ops_per_sec < 1000:
                return f"{ops_per_sec:.1f} IOPS"
            else:
                return f"{ops_per_sec/1000:.1f}K IOPS"
        except (ValueError, TypeError):
            return str(ops_per_sec)

    def format_percentage(self, value: float, precision: int = 1) -> str:
        """Format percentage values"""
        try:
            return f"{value:.{precision}f}%"
        except (ValueError, TypeError):
            return str(value)

    def extract_numeric_value(self, value_str: str) -> float:
        """Extract numeric value from formatted string"""
        try:
            if isinstance(value_str, (int, float)):
                return float(value_str)
            
            # Remove HTML tags first
            import re
            clean_str = re.sub(r'<[^>]+>', '', str(value_str))
            
            # Extract first number from string
            numbers = re.findall(r'[\d.]+', clean_str)
            if numbers:
                return float(numbers[0])
            return 0.0
        except (ValueError, TypeError):
            return 0.0

    def categorize_disk_io_metric(self, metric_name: str) -> str:
        """Categorize disk I/O metric type"""
        metric_lower = metric_name.lower()
        
        if 'throughput' in metric_lower or 'bytes' in metric_lower:
            return 'Throughput'
        elif 'iops' in metric_lower or 'operations' in metric_lower:
            return 'IOPS'
        elif 'container' in metric_lower:
            return 'Container I/O'
        elif 'node' in metric_lower:
            return 'Node I/O'
        else:
            return 'Other I/O'        

    def format_general_info_metric(self, value: float, unit: str) -> str:
        """Format general info metric value with appropriate units"""
        try:
            if unit == 'percent':
                return f"{value:.2f}%"
            elif unit == 'MB':
                if value > 1024:
                    return f"{value/1024:.1f} GB"
                return f"{value:.1f} MB"
            elif unit == 'seconds':
                if value < 0.001:
                    return f"{value*1000000:.0f} μs"
                elif value < 1:
                    return f"{value*1000:.1f} ms"
                else:
                    return f"{value:.3f} s"
            elif unit == 'per_second':
                if value < 0.01:
                    return f"{value*1000:.1f} m/s"
                return f"{value:.2f}/s"
            elif unit == 'count':
                if value > 1000000:
                    return f"{value/1000000:.1f}M"
                elif value > 1000:
                    return f"{value/1000:.1f}K"
                else:
                    return f"{value:.0f}"
            elif unit == 'per_day':
                return f"{value:.1f}/day"
            elif unit == 'boolean':
                return "Healthy" if value == 1 else "Unhealthy"
            else:
                return f"{value:.2f}"
        except (ValueError, TypeError):
            return str(value)

    def highlight_general_info_values(self, value: Union[float, int], metric_name: str, unit: str = "", is_top: bool = False) -> str:
        """Highlight general info values with metric-specific thresholds"""
        try:
            # Define metric-specific thresholds
            thresholds = self._get_general_info_thresholds(metric_name)
            
            if is_top:
                formatted_value = self.format_general_info_metric(float(value), unit)
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted_value}</span>'
            
            if thresholds and isinstance(value, (int, float)):
                critical = thresholds.get('critical', float('inf'))
                warning = thresholds.get('warning', float('inf'))
                
                formatted_value = self.format_general_info_metric(float(value), unit)
                
                if value >= critical:
                    return f'<span class="text-danger font-weight-bold">⚠️ {formatted_value}</span>'
                elif value >= warning:
                    return f'<span class="text-warning font-weight-bold">{formatted_value}</span>'
                else:
                    return f'<span class="text-success">{formatted_value}</span>'
            else:
                return self.format_general_info_metric(float(value), unit)
                
        except (ValueError, TypeError):
            return str(value)

    def _get_general_info_thresholds(self, metric_name: str) -> Dict[str, float]:
        """Get thresholds for general info metrics"""
        metric_lower = metric_name.lower()
        
        thresholds_map = {
            'cpu': {'warning': 60, 'critical': 80},
            'memory': {'warning': 70, 'critical': 85},
            'db_space': {'warning': 60, 'critical': 80},
            'proposal_failure': {'warning': 0.01, 'critical': 0.1},
            'slow_applies': {'warning': 0.05, 'critical': 0.1},
            'slow_read': {'warning': 0.05, 'critical': 0.1},
            'leader_changes': {'warning': 0.1, 'critical': 1.0},
            'health_failures': {'warning': 0.01, 'critical': 0.1}
        }
        
        for key, threshold in thresholds_map.items():
            if key in metric_lower:
                return threshold
        
        return {}

    def categorize_general_info_metric(self, metric_name: str) -> str:
        """Categorize general info metric type"""
        metric_lower = metric_name.lower()
        
        if any(keyword in metric_lower for keyword in ['cpu', 'memory']):
            return 'Resource Usage'
        elif any(keyword in metric_lower for keyword in ['db', 'database', 'space']):
            return 'Database Metrics'
        elif any(keyword in metric_lower for keyword in ['proposal', 'commit', 'apply']):
            return 'Proposal Metrics'
        elif any(keyword in metric_lower for keyword in ['leader', 'election']):
            return 'Leadership'
        elif any(keyword in metric_lower for keyword in ['slow', 'latency', 'duration']):
            return 'Performance'
        elif any(keyword in metric_lower for keyword in ['put', 'delete', 'operations']):
            return 'Operations'
        elif any(keyword in metric_lower for keyword in ['failure', 'health', 'error']):
            return 'Health & Errors'
        elif any(keyword in metric_lower for keyword in ['key', 'total']):
            return 'Data Statistics'
        else:
            return 'Other'

    def format_wal_fsync_latency(self, latency_seconds: float) -> str:
        """Format WAL fsync latency with appropriate units"""
        try:
            if latency_seconds == 0:
                return "0 ms"
            elif latency_seconds < 0.001:
                return f"{latency_seconds*1000000:.0f} μs"
            elif latency_seconds < 1:
                return f"{latency_seconds*1000:.3f} ms"
            else:
                return f"{latency_seconds:.6f} s"
        except (ValueError, TypeError):
            return str(latency_seconds)

    def format_wal_fsync_rate(self, rate: float, unit: str) -> str:
        """Format WAL fsync rate values"""
        try:
            if unit == 'seconds/sec' or unit == 's/s':
                return f"{rate:.6f} s/s"
            elif unit == 'operations/sec':
                return self.format_operations_per_second(rate)
            else:
                return f"{rate:.3f}"
        except (ValueError, TypeError):
            return str(rate)

    def highlight_wal_fsync_values(self, value: Union[float, int], metric_type: str, unit: str = "", is_top: bool = False) -> str:
        """Highlight WAL fsync values with metric-specific thresholds"""
        try:
            thresholds = self._get_wal_fsync_thresholds(metric_type)
            
            if is_top:
                formatted_value = self._format_wal_fsync_by_type(float(value), metric_type, unit)
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted_value}</span>'
            
            if thresholds and isinstance(value, (int, float)):
                critical = thresholds.get('critical', float('inf'))
                warning = thresholds.get('warning', float('inf'))
                
                formatted_value = self._format_wal_fsync_by_type(float(value), metric_type, unit)
                
                if value >= critical:
                    return f'<span class="text-danger font-weight-bold">⚠️ {formatted_value}</span>'
                elif value >= warning:
                    return f'<span class="text-warning font-weight-bold">{formatted_value}</span>'
                else:
                    return f'<span class="text-success">{formatted_value}</span>'
            else:
                return self._format_wal_fsync_by_type(float(value), metric_type, unit)
                
        except (ValueError, TypeError):
            return str(value)

    def _get_wal_fsync_thresholds(self, metric_type: str) -> Dict[str, float]:
        """Get thresholds for WAL fsync metrics"""
        thresholds_map = {
            'p99_latency_ms': {'warning': 50, 'critical': 100},
            'avg_latency_ms': {'warning': 30, 'critical': 70},
            'ops_per_sec': {'warning': 1000, 'critical': 2000},  # High values might indicate issues
            'fsync_rate': {'warning': 0.01, 'critical': 0.05}    # High duration rates
        }
        
        for key, threshold in thresholds_map.items():
            if key in metric_type.lower():
                return threshold
        
        return {}

    def _format_wal_fsync_by_type(self, value: float, metric_type: str, unit: str) -> str:
        """Format WAL fsync value by metric type"""
        if 'latency' in metric_type and 'ms' in unit:
            return f"{value:.3f} ms"
        elif 'ops' in metric_type or 'operations' in metric_type:
            return self.format_operations_per_second(value)
        elif 'rate' in metric_type and 's/s' in unit:
            return f"{value:.6f} s/s"
        elif 'count' in metric_type:
            return self.format_count_value(value)
        else:
            return f"{value:.3f}"

    def format_count_value(self, count: Union[int, float]) -> str:
        """Format count values with appropriate scaling"""
        try:
            count = int(count)
            if count > 1000000:
                return f"{count/1000000:.1f}M"
            elif count > 1000:
                return f"{count/1000:.1f}K"
            else:
                return f"{count:,}"
        except (ValueError, TypeError):
            return str(count)

    def categorize_wal_fsync_metric(self, metric_name: str) -> str:
        """Categorize WAL fsync metric type"""
        metric_lower = metric_name.lower()
        
        if 'p99' in metric_lower or 'duration' in metric_lower and 'rate' not in metric_lower:
            return 'Latency'
        elif 'sum_rate' in metric_lower or 'rate' in metric_lower:
            return 'Rate/Throughput'
        elif 'count' in metric_lower and 'rate' not in metric_lower:
            return 'Cumulative'
        elif 'operations' in metric_lower or 'ops' in metric_lower:
            return 'Operations'
        else:
            return 'Other' 

# Add these methods to the utilityELT class in etcd_analyzer_elt_utility.py
    def format_backend_commit_latency(self, latency_seconds: float) -> str:
        """Format backend commit latency with appropriate units"""
        try:
            if latency_seconds == 0:
                return "0 ms"
            elif latency_seconds < 0.001:
                return f"{latency_seconds*1000000:.0f} μs"
            elif latency_seconds < 1:
                return f"{latency_seconds*1000:.3f} ms"
            else:
                return f"{latency_seconds:.6f} s"
        except (ValueError, TypeError):
            return str(latency_seconds)

    def format_backend_commit_rate(self, rate: float, unit: str) -> str:
        """Format backend commit rate values"""
        try:
            if unit == 'seconds/sec' or unit == 's/s':
                return f"{rate:.6f} s/s"
            elif unit == 'operations/sec':
                return self.format_operations_per_second(rate)
            elif unit == 'count':
                return self.format_count_value(rate)
            else:
                return f"{rate:.3f}"
        except (ValueError, TypeError):
            return str(rate)

    def highlight_backend_commit_values(self, value: Union[float, int], metric_type: str, unit: str = "", is_top: bool = False) -> str:
        """Highlight backend commit values with metric-specific thresholds"""
        try:
            thresholds = self._get_backend_commit_thresholds(metric_type)
            
            if is_top:
                formatted_value = self._format_backend_commit_by_type(float(value), metric_type, unit)
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted_value}</span>'
            
            if thresholds and isinstance(value, (int, float)):
                critical = thresholds.get('critical', float('inf'))
                warning = thresholds.get('warning', float('inf'))
                
                formatted_value = self._format_backend_commit_by_type(float(value), metric_type, unit)
                
                if value >= critical:
                    return f'<span class="text-danger font-weight-bold">⚠️ {formatted_value}</span>'
                elif value >= warning:
                    return f'<span class="text-warning font-weight-bold">{formatted_value}</span>'
                else:
                    return f'<span class="text-success">{formatted_value}</span>'
            else:
                return self._format_backend_commit_by_type(float(value), metric_type, unit)
                
        except (ValueError, TypeError):
            return str(value)

    def _get_backend_commit_thresholds(self, metric_type: str) -> Dict[str, float]:
        """Get thresholds for backend commit metrics"""
        thresholds_map = {
            'p99_latency_ms': {'warning': 20, 'critical': 50},
            'avg_latency_ms': {'warning': 10, 'critical': 30},
            'ops_per_sec': {'warning': 1000, 'critical': 2000},  # High values might indicate issues
            'commit_rate': {'warning': 0.01, 'critical': 0.05}    # High duration rates
        }
        
        for key, threshold in thresholds_map.items():
            if key in metric_type.lower():
                return threshold
        
        return {}

    def _format_backend_commit_by_type(self, value: float, metric_type: str, unit: str) -> str:
        """Format backend commit value by metric type"""
        if 'latency' in metric_type and 'ms' in unit:
            return f"{value:.3f} ms"
        elif 'ops' in metric_type or 'operations' in metric_type:
            return self.format_operations_per_second(value)
        elif 'rate' in metric_type and 's/s' in unit:
            return f"{value:.6f} s/s"
        elif 'count' in metric_type:
            return self.format_count_value(value)
        else:
            return f"{value:.3f}"

    def categorize_backend_commit_metric(self, metric_name: str) -> str:
        """Categorize backend commit metric type"""
        metric_lower = metric_name.lower()
        
        if 'p99' in metric_lower or 'duration' in metric_lower and 'rate' not in metric_lower:
            return 'Latency'
        elif 'sum_rate' in metric_lower or 'rate' in metric_lower:
            return 'Rate/Throughput'
        elif 'count' in metric_lower and 'rate' not in metric_lower:
            return 'Cumulative'
        elif 'operations' in metric_lower or 'ops' in metric_lower:
            return 'Operations'
        else:
            return 'Other'

    def format_compact_defrag_duration(self, duration_value: float, unit: str) -> str:
        """Format compact defrag duration with appropriate units"""
        try:
            if duration_value == 0:
                return "0"
            
            if unit == 'milliseconds':
                if duration_value < 1:
                    return f"{duration_value*1000:.0f} μs"
                elif duration_value < 1000:
                    return f"{duration_value:.1f} ms"
                else:
                    return f"{duration_value/1000:.2f} s"
            elif unit == 'seconds':
                if duration_value < 1:
                    return f"{duration_value*1000:.1f} ms"
                elif duration_value < 60:
                    return f"{duration_value:.3f} s"
                else:
                    return f"{duration_value/60:.1f} min"
            else:
                return f"{duration_value:.2f}"
        except (ValueError, TypeError):
            return str(duration_value)

    def format_page_fault_rate(self, rate: float, unit: str) -> str:
        """Format page fault rate values"""
        try:
            if unit == 'faults/s':
                if rate == 0:
                    return "0 faults/s"
                elif rate < 0.001:
                    return f"{rate*1000:.2f} mfaults/s"
                elif rate < 1:
                    return f"{rate:.3f} faults/s"
                else:
                    return f"{rate:.1f} faults/s"
            elif unit == 'faults':
                return self.format_count_value(rate)
            else:
                return f"{rate:.3f}"
        except (ValueError, TypeError):
            return str(rate)

    def highlight_compact_defrag_values(self, value: Union[float, int], metric_type: str, unit: str = "", is_top: bool = False) -> str:
        """Highlight compact defrag values with metric-specific thresholds"""
        try:
            thresholds = self._get_compact_defrag_thresholds(metric_type)
            
            if is_top:
                formatted_value = self._format_compact_defrag_by_type(float(value), metric_type, unit)
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted_value}</span>'
            
            if thresholds and isinstance(value, (int, float)):
                critical = thresholds.get('critical', float('inf'))
                warning = thresholds.get('warning', float('inf'))
                
                formatted_value = self._format_compact_defrag_by_type(float(value), metric_type, unit)
                
                if value >= critical:
                    return f'<span class="text-danger font-weight-bold">⚠️ {formatted_value}</span>'
                elif value >= warning:
                    return f'<span class="text-warning font-weight-bold">{formatted_value}</span>'
                else:
                    return f'<span class="text-success">{formatted_value}</span>'
            else:
                return self._format_compact_defrag_by_type(float(value), metric_type, unit)
                
        except (ValueError, TypeError):
            return str(value)

    def _get_compact_defrag_thresholds(self, metric_type: str) -> Dict[str, float]:
        """Get thresholds for compact defrag metrics"""
        thresholds_map = {
            'compaction_duration': {'warning': 50, 'critical': 100},    # milliseconds
            'compaction_rate': {'warning': 10, 'critical': 50},        # ms rate
            'defrag_duration': {'warning': 1, 'critical': 5},          # seconds
            'defrag_rate': {'warning': 0.5, 'critical': 2.0},          # seconds rate
            'page_fault_rate': {'warning': 0.1, 'critical': 1.0},     # faults/s
            'page_fault_total': {'warning': 20000, 'critical': 50000} # total faults
        }
        
        for key, threshold in thresholds_map.items():
            if key in metric_type.lower():
                return threshold
        
        return {}

    def _format_compact_defrag_by_type(self, value: float, metric_type: str, unit: str) -> str:
        """Format compact defrag value by metric type"""
        if 'compaction' in metric_type and 'milliseconds' in unit:
            return self.format_compact_defrag_duration(value, 'milliseconds')
        elif 'defrag' in metric_type and 'seconds' in unit:
            return self.format_compact_defrag_duration(value, 'seconds')
        elif 'fault' in metric_type:
            return self.format_page_fault_rate(value, unit)
        else:
            return f"{value:.3f}"

    def categorize_compact_defrag_metric(self, metric_name: str) -> str:
        """Categorize compact defrag metric type"""
        metric_lower = metric_name.lower()
        
        if 'compaction' in metric_lower:
            return 'Compaction'
        elif 'defrag' in metric_lower:
            return 'Defragmentation'  
        elif 'pgmajfault' in metric_lower or 'page' in metric_lower:
            return 'Page Faults'
        else:
            return 'Other'      

    def format_network_bytes_per_second(self, bytes_per_sec: float) -> str:
        """Format network bytes per second to readable units"""
        try:
            if bytes_per_sec == 0:
                return "0 B/s"
            elif bytes_per_sec < 1024:
                return f"{bytes_per_sec:.0f} B/s"
            elif bytes_per_sec < 1024**2:
                return f"{bytes_per_sec/1024:.1f} KB/s"
            elif bytes_per_sec < 1024**3:
                return f"{bytes_per_sec/(1024**2):.1f} MB/s"
            else:
                return f"{bytes_per_sec/(1024**3):.2f} GB/s"
        except (ValueError, TypeError):
            return str(bytes_per_sec)

    def format_network_bits_per_second(self, bits_per_sec: float) -> str:
        """Format network bits per second to readable units"""
        try:
            if bits_per_sec == 0:
                return "0 bps"
            elif bits_per_sec < 1000:
                return f"{bits_per_sec:.0f} bps"
            elif bits_per_sec < 1000**2:
                return f"{bits_per_sec/1000:.1f} Kbps"
            elif bits_per_sec < 1000**3:
                return f"{bits_per_sec/(1000**2):.1f} Mbps"
            else:
                return f"{bits_per_sec/(1000**3):.2f} Gbps"
        except (ValueError, TypeError):
            return str(bits_per_sec)

    def format_network_packets_per_second(self, packets_per_sec: float) -> str:
        """Format network packets per second to readable units"""
        try:
            if packets_per_sec == 0:
                return "0 pps"
            elif packets_per_sec < 1:
                return f"{packets_per_sec:.3f} pps"
            elif packets_per_sec < 1000:
                return f"{packets_per_sec:.1f} pps"
            else:
                return f"{packets_per_sec/1000:.1f}K pps"
        except (ValueError, TypeError):
            return str(packets_per_sec)

    def format_network_latency_seconds(self, latency_seconds: float) -> str:
        """Format network latency with appropriate units"""
        try:
            if latency_seconds == 0:
                return "0 ms"
            elif latency_seconds < 0.001:
                return f"{latency_seconds*1000000:.0f} μs"
            elif latency_seconds < 1:
                return f"{latency_seconds*1000:.3f} ms"
            else:
                return f"{latency_seconds:.6f} s"
        except (ValueError, TypeError):
            return str(latency_seconds)

    def highlight_network_io_values(self, value: Union[float, int], metric_type: str, unit: str = "", is_top: bool = False) -> str:
        """Highlight network I/O values with metric-specific thresholds"""
        try:
            thresholds = self._get_network_io_thresholds(metric_type)
            
            if is_top:
                formatted_value = self._format_network_io_by_type(float(value), metric_type, unit)
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted_value}</span>'
            
            if thresholds and isinstance(value, (int, float)):
                critical = thresholds.get('critical', float('inf'))
                warning = thresholds.get('warning', float('inf'))
                
                formatted_value = self._format_network_io_by_type(float(value), metric_type, unit)
                
                if value >= critical:
                    return f'<span class="text-danger font-weight-bold">⚠️ {formatted_value}</span>'
                elif value >= warning:
                    return f'<span class="text-warning font-weight-bold">{formatted_value}</span>'
                else:
                    return f'<span class="text-success">{formatted_value}</span>'
            else:
                return self._format_network_io_by_type(float(value), metric_type, unit)
                
        except (ValueError, TypeError):
            return str(value)

    def _get_network_io_thresholds(self, metric_type: str) -> Dict[str, float]:
        """Get thresholds for network I/O metrics"""
        thresholds_map = {
            'rx': {'warning': 100000000, 'critical': 500000000},  # 100MB/s, 500MB/s
            'tx': {'warning': 100000000, 'critical': 500000000},  # 100MB/s, 500MB/s
            'round_trip_time': {'warning': 0.01, 'critical': 0.05},  # 10ms, 50ms
            'packets': {'warning': 1000, 'critical': 5000},  # 1K pps, 5K pps
            'watch_streams': {'warning': 500, 'critical': 1000},  # 500, 1000 streams
            'utilization': {'warning': 100000000, 'critical': 500000000}  # bits per second
        }
        
        metric_lower = metric_type.lower()
        for key, threshold in thresholds_map.items():
            if key in metric_lower:
                return threshold
        
        return {}

    def _format_network_io_by_type(self, value: float, metric_type: str, unit: str) -> str:
        """Format network I/O value by metric type"""
        if 'bytes_per_second' in unit:
            return self.format_network_bytes_per_second(value)
        elif 'bits_per_second' in unit:
            return self.format_network_bits_per_second(value)
        elif 'packets_per_second' in unit:
            return self.format_network_packets_per_second(value)
        elif 'seconds' in unit:
            return self.format_network_latency_seconds(value)
        elif 'count' in unit:
            return self.format_count_value(value)
        else:
            return f"{value:.3f}"

    def categorize_network_io_metric(self, metric_name: str) -> str:
        """Categorize network I/O metric type"""
        metric_lower = metric_name.lower()
        
        if any(keyword in metric_lower for keyword in ['container', 'pod']):
            return 'Container Network'
        elif any(keyword in metric_lower for keyword in ['node', 'utilization']):
            return 'Node Network'
        elif any(keyword in metric_lower for keyword in ['peer', 'round_trip']):
            return 'Peer Network'
        elif any(keyword in metric_lower for keyword in ['grpc', 'watch', 'lease']):
            return 'gRPC Streams'
        elif 'snapshot' in metric_lower:
            return 'Cluster Operations'
        else:
            return 'Other Network'

    def format_network_stream_count(self, count: Union[int, float]) -> str:
        """Format network stream count values"""
        try:
            count = int(count)
            if count == 0:
                return "0 streams"
            elif count == 1:
                return "1 stream"
            elif count < 1000:
                return f"{count:,} streams"
            else:
                return f"{count/1000:.1f}K streams"
        except (ValueError, TypeError):
            return str(count)

    def assess_network_io_health(self, metrics_data: List[Dict[str, Any]]) -> str:
        """Assess overall network I/O health status"""
        try:
            if not metrics_data:
                return "unknown"
            
            warning_count = 0
            critical_count = 0
            total_metrics = 0
            
            for metric in metrics_data:
                if metric.get('avg_value') is not None:
                    total_metrics += 1
                    thresholds = self._get_network_io_thresholds(metric.get('metric_name', ''))
                    
                    if thresholds:
                        value = metric['avg_value']
                        if value >= thresholds.get('critical', float('inf')):
                            critical_count += 1
                        elif value >= thresholds.get('warning', float('inf')):
                            warning_count += 1
            
            if critical_count > 0:
                return "critical"
            elif warning_count > total_metrics * 0.3:  # More than 30% in warning
                return "degraded"
            else:
                return "healthy"
                
        except Exception as e:
            logger.error(f"Failed to assess network I/O health: {e}")
            return "unknown"            

    def highlight_disk_io_values(self, value: Union[float, int], unit: str = "", is_top: bool = False) -> str:
        """Highlight disk I/O values with unit conversion and thresholds"""
        try:
            thresholds = self._get_disk_io_thresholds(unit)
            
            if is_top:
                formatted_value = self._format_disk_io_by_unit(float(value), unit)
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted_value}</span>'
            
            if thresholds and isinstance(value, (int, float)):
                critical = thresholds.get('critical', float('inf'))
                warning = thresholds.get('warning', float('inf'))
                
                formatted_value = self._format_disk_io_by_unit(float(value), unit)
                
                if value >= critical:
                    return f'<span class="text-danger font-weight-bold">⚠️ {formatted_value}</span>'
                elif value >= warning:
                    return f'<span class="text-warning font-weight-bold">{formatted_value}</span>'
                else:
                    return f'<span class="text-success">{formatted_value}</span>'
            else:
                return self._format_disk_io_by_unit(float(value), unit)
                
        except (ValueError, TypeError):
            return str(value)

    def _get_disk_io_thresholds(self, unit: str) -> Dict[str, float]:
        """Get thresholds for disk I/O metrics based on unit"""
        unit_lower = unit.lower()
        
        if 'bytes_per_second' in unit_lower:
            # Thresholds for disk throughput (100MB/s warning, 500MB/s critical)
            return {'warning': 100000000, 'critical': 500000000}
        elif 'operations_per_second' in unit_lower:
            # Thresholds for IOPS (1000 warning, 5000 critical)
            return {'warning': 1000, 'critical': 5000}
        else:
            return {}

    def _format_disk_io_by_unit(self, value: float, unit: str) -> str:
        """Format disk I/O value based on unit"""
        unit_lower = unit.lower()
        
        if 'bytes_per_second' in unit_lower:
            return self.format_bytes_per_second(value)
        elif 'operations_per_second' in unit_lower:
            return self.format_operations_per_second(value)
        elif 'bytes' in unit_lower:
            return self.format_bytes_per_second(value)
        else:
            return f"{value:.2f}"

    def categorize_deep_drive_metric(self, metric_name: str) -> str:
        """Categorize deep drive metric type"""
        metric_lower = metric_name.lower()
        
        if any(keyword in metric_lower for keyword in ['cpu', 'memory']):
            return 'Resource Usage'
        elif any(keyword in metric_lower for keyword in ['wal', 'fsync']):
            return 'WAL Operations'
        elif any(keyword in metric_lower for keyword in ['disk', 'io', 'throughput', 'iops']):
            return 'Disk I/O'
        elif any(keyword in metric_lower for keyword in ['network', 'grpc', 'peer']):
            return 'Network I/O'
        elif any(keyword in metric_lower for keyword in ['backend', 'commit']):
            return 'Backend Operations'
        elif any(keyword in metric_lower for keyword in ['compact', 'defrag']):
            return 'Maintenance Operations'
        elif any(keyword in metric_lower for keyword in ['proposal', 'leader']):
            return 'Consensus'
        else:
            return 'Other'

    def format_deep_drive_duration(self, value: float, unit: str) -> str:
        """Format duration values for deep drive metrics"""
        try:
            unit_lower = unit.lower()
            
            if unit_lower == 'seconds':
                if value < 0.001:
                    return f"{value*1000000:.0f} μs"
                elif value < 1:
                    return f"{value*1000:.3f} ms"
                elif value < 60:
                    return f"{value:.3f} s"
                else:
                    return f"{value/60:.1f} min"
            elif unit_lower == 'milliseconds':
                if value < 1:
                    return f"{value*1000:.0f} μs"
                elif value < 1000:
                    return f"{value:.1f} ms"
                else:
                    return f"{value/1000:.2f} s"
            else:
                return f"{value:.3f}"
        except (ValueError, TypeError):
            return str(value)

    def assess_deep_drive_health(self, metrics_data: List[Dict[str, Any]], metric_type: str) -> str:
        """Assess overall health status for deep drive metrics"""
        try:
            if not metrics_data:
                return "unknown"
            
            warning_count = 0
            critical_count = 0
            total_metrics = 0
            
            for metric in metrics_data:
                avg_value = metric.get('avg_value')
                unit = metric.get('unit', '')
                
                if avg_value is not None:
                    total_metrics += 1
                    thresholds = self._get_metric_thresholds_by_type(metric_type, unit)
                    
                    if thresholds:
                        if avg_value >= thresholds.get('critical', float('inf')):
                            critical_count += 1
                        elif avg_value >= thresholds.get('warning', float('inf')):
                            warning_count += 1
            
            if critical_count > 0:
                return "critical"
            elif warning_count > total_metrics * 0.3:  # More than 30% in warning
                return "degraded"
            else:
                return "healthy"
                
        except Exception as e:
            logger.error(f"Failed to assess deep drive health: {e}")
            return "unknown"

    def _get_metric_thresholds_by_type(self, metric_type: str, unit: str) -> Dict[str, float]:
        """Get thresholds based on metric type and unit"""
        if metric_type == 'general_info':
            return self._get_general_info_thresholds(unit)
        elif metric_type == 'wal_fsync':
            return self._get_wal_fsync_thresholds(unit)
        elif metric_type == 'disk_io':
            return self._get_disk_io_thresholds(unit)
        elif metric_type == 'network_io':
            return self._get_network_io_thresholds(unit)
        elif metric_type == 'backend_commit':
            return self._get_backend_commit_thresholds(unit)
        elif metric_type == 'compact_defrag':
            return self._get_compact_defrag_thresholds(unit)
        else:
            return {}

    def format_deep_drive_metric_value(self, value: float, metric_name: str, unit: str) -> str:
        """Format deep drive metric values with appropriate units"""
        try:
            metric_lower = metric_name.lower()
            unit_lower = unit.lower()
            
            # CPU usage
            if 'cpu' in metric_lower and 'percent' in unit_lower:
                return f"{value:.2f}%"
            
            # Memory usage
            elif 'memory' in metric_lower:
                if 'mb' in unit_lower:
                    if value > 1024:
                        return f"{value/1024:.1f} GB"
                    return f"{value:.0f} MB"
                elif 'gb' in unit_lower:
                    return f"{value:.1f} GB"
            
            # Latency metrics
            elif any(keyword in metric_lower for keyword in ['fsync', 'commit', 'latency', 'duration']):
                return self.format_deep_drive_duration(value, unit)
            
            # Network metrics
            elif any(keyword in metric_lower for keyword in ['network', 'grpc']):
                if 'bytes_per_second' in unit_lower:
                    return self.format_network_bytes_per_second(value)
                elif 'packets_per_second' in unit_lower:
                    return self.format_network_packets_per_second(value)
                elif 'seconds' in unit_lower:
                    return self.format_network_latency_seconds(value)
            
            # Disk I/O metrics
            elif any(keyword in metric_lower for keyword in ['disk', 'io']):
                return self._format_disk_io_by_unit(value, unit)
            
            # Count metrics
            elif 'count' in unit_lower:
                return self.format_count_value(value)
            
            # Rate metrics
            elif 'per_second' in unit_lower:
                if value < 0.01:
                    return f"{value*1000:.1f} m/s"
                return f"{value:.2f}/s"
            
            # Default formatting
            else:
                if value == 0:
                    return "0"
                elif abs(value) < 0.001:
                    return f"{value:.6f}"
                elif abs(value) < 1:
                    return f"{value:.3f}"
                else:
                    return f"{value:.2f}"
                    
        except (ValueError, TypeError):
            return str(value)

    def create_deep_drive_status_badge(self, status: str, metric_type: str = "") -> str:
        """Create status badge for deep drive analysis"""
        status_lower = status.lower()
        
        if status_lower in ['excellent', 'healthy', 'good']:
            color = 'success'
            icon = 'OK'
        elif status_lower in ['warning', 'degraded', 'elevated']:
            color = 'warning'
            icon = 'WARN'
        elif status_lower in ['critical', 'unhealthy', 'failed']:
            color = 'danger'
            icon = 'CRIT'
        else:
            color = 'secondary'
            icon = status.upper()
        
        return f'<span class="badge badge-{color}">{icon} {status.title()}</span>'

    def format_bottleneck_throughput(self, value: float, unit: str) -> str:
        """Format bottleneck throughput values with appropriate units"""
        try:
            if value == 0:
                return "0 B/s"
            
            unit_lower = unit.lower()
            
            if 'bytes_per_second' in unit_lower:
                return self.format_network_bytes_per_second(value)
            elif 'bits_per_second' in unit_lower:
                return self.format_network_bits_per_second(value)
            else:
                # Default byte formatting
                if value < 1024:
                    return f"{value:.0f} B/s"
                elif value < 1024**2:
                    return f"{value/1024:.1f} KB/s"
                elif value < 1024**3:
                    return f"{value/(1024**2):.1f} MB/s"
                else:
                    return f"{value/(1024**3):.2f} GB/s"
        except (ValueError, TypeError):
            return str(value)

    def assess_bottleneck_severity(self, bottlenecks_list: List[Dict[str, Any]]) -> str:
        """Assess overall severity of a list of bottlenecks"""
        if not bottlenecks_list:
            return "none"
        
        severities = [b.get('severity', '').lower() for b in bottlenecks_list]
        
        if 'critical' in severities or 'high' in severities:
            return "critical"
        elif 'medium' in severities:
            return "medium" 
        elif 'low' in severities:
            return "low"
        else:
            return "unknown"

    def create_bottleneck_status_indicator(self, category: str, count: int, severity: str) -> str:
        """Create status indicator for bottleneck categories"""
        if count == 0:
            return f'<span class="badge badge-success">✓ {category}: None</span>'
        
        severity_lower = severity.lower()
        icon_map = {
            'critical': '🔥',
            'high': '⚠️',
            'medium': '⚡',
            'low': 'ℹ️'
        }
        
        color_map = {
            'critical': 'danger',
            'high': 'danger', 
            'medium': 'warning',
            'low': 'info'
        }
        
        icon = icon_map.get(severity_lower, '⚠️')
        color = color_map.get(severity_lower, 'warning')
        
        return f'<span class="badge badge-{color}">{icon} {category}: {count}</span>'

    def format_bottleneck_metric_by_type(self, value: Union[float, int], bottleneck_type: str, unit: str) -> str:
        """Format bottleneck metric values based on type and unit"""
        try:
            type_lower = bottleneck_type.lower()
            unit_lower = unit.lower()
            
            # Network throughput bottlenecks
            if 'throughput' in type_lower or 'utilization' in type_lower:
                if 'bytes_per_second' in unit_lower:
                    return self.format_network_bytes_per_second(float(value))
                elif 'bits_per_second' in unit_lower:
                    return self.format_network_bits_per_second(float(value))
            
            # Network packet bottlenecks
            elif 'packet' in type_lower:
                if 'packets_per_second' in unit_lower:
                    return self.format_network_packets_per_second(float(value))
                else:
                    return self.format_count_value(float(value))
            
            # Latency bottlenecks
            elif 'latency' in type_lower or 'duration' in type_lower:
                if 'seconds' in unit_lower:
                    return self.format_network_latency_seconds(float(value))
                elif 'milliseconds' in unit_lower:
                    return f"{float(value):.3f} ms"
            
            # Memory bottlenecks
            elif 'memory' in type_lower:
                if 'percent' in unit_lower:
                    return self.format_percentage(float(value))
                elif 'mb' in unit_lower:
                    if float(value) > 1024:
                        return f"{float(value)/1024:.1f} GB"
                    return f"{float(value):.0f} MB"
                elif 'gb' in unit_lower:
                    return f"{float(value):.1f} GB"
            
            # CPU bottlenecks
            elif 'cpu' in type_lower:
                if 'percent' in unit_lower:
                    return self.format_percentage(float(value))
            
            # Disk bottlenecks
            elif 'disk' in type_lower:
                return self.format_bottleneck_throughput(float(value), unit)
            
            # Default formatting
            else:
                if isinstance(value, (int, float)):
                    if float(value) == 0:
                        return "0"
                    elif abs(float(value)) < 0.001:
                        return f"{float(value):.6f}"
                    elif abs(float(value)) < 1:
                        return f"{float(value):.3f}"
                    else:
                        return f"{float(value):.2f}"
                else:
                    return str(value)
                    
        except (ValueError, TypeError):
            return str(value)

    def highlight_bottleneck_severity(self, value: str, severity: str, is_top: bool = False) -> str:
        """Highlight bottleneck values based on severity"""
        try:
            severity_lower = severity.lower()
            
            if is_top:
                return f'<span class="text-danger font-weight-bold bg-warning px-1">🔥 {value}</span>'
            elif severity_lower in ['critical', 'high']:
                return f'<span class="text-danger font-weight-bold">⚠️ {value}</span>'
            elif severity_lower == 'medium':
                return f'<span class="text-warning font-weight-bold">{value}</span>'
            elif severity_lower == 'low':
                return f'<span class="text-info">{value}</span>'
            else:
                return str(value)
                
        except Exception as e:
            logger.error(f"Error highlighting bottleneck severity: {e}")
            return str(value)

    def categorize_bottleneck_type(self, bottleneck_type: str) -> str:
        """Categorize bottleneck type for better organization"""
        type_lower = bottleneck_type.lower()
        
        if any(keyword in type_lower for keyword in ['disk', 'storage', 'throughput', 'iops']):
            return 'Storage Performance'
        elif any(keyword in type_lower for keyword in ['network', 'latency', 'packet', 'utilization']):
            return 'Network Performance'
        elif any(keyword in type_lower for keyword in ['memory', 'ram', 'heap']):
            return 'Memory Resources'
        elif any(keyword in type_lower for keyword in ['cpu', 'processor', 'compute']):
            return 'CPU Resources'
        elif any(keyword in type_lower for keyword in ['consensus', 'proposal', 'leader', 'apply']):
            return 'Consensus Mechanism'
        else:
            return 'Other Performance'

    def format_root_cause_likelihood(self, likelihood: str) -> str:
        """Format root cause likelihood with appropriate styling"""
        likelihood_lower = likelihood.lower()
        
        if likelihood_lower == 'high':
            return f'<span class="badge badge-danger">High Likelihood</span>'
        elif likelihood_lower == 'medium':
            return f'<span class="badge badge-warning">Medium Likelihood</span>'
        elif likelihood_lower == 'low':
            return f'<span class="badge badge-info">Low Likelihood</span>'
        else:
            return f'<span class="badge badge-secondary">{likelihood.title()}</span>'

    def format_recommendation_priority(self, priority: str) -> str:
        """Format recommendation priority with appropriate styling"""
        priority_lower = priority.lower()
        
        if priority_lower == 'high':
            return f'<span class="badge badge-danger">🔥 High Priority</span>'
        elif priority_lower == 'medium':
            return f'<span class="badge badge-warning">⚡ Medium Priority</span>'
        elif priority_lower == 'low':
            return f'<span class="badge badge-info">ℹ️ Low Priority</span>'
        else:
            return f'<span class="badge badge-secondary">{priority.title()}</span>'

    def assess_overall_bottleneck_health(self, all_bottlenecks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Assess overall health status based on all bottlenecks"""
        try:
            if not all_bottlenecks:
                return {
                    'status': 'healthy',
                    'score': 100,
                    'description': 'No bottlenecks detected'
                }
            
            total_bottlenecks = len(all_bottlenecks)
            critical_count = sum(1 for b in all_bottlenecks if b.get('severity', '').lower() in ['critical', 'high'])
            medium_count = sum(1 for b in all_bottlenecks if b.get('severity', '').lower() == 'medium')
            
            # Calculate health score (0-100)
            score = 100 - (critical_count * 25) - (medium_count * 10)
            score = max(0, score)  # Don't go below 0
            
            if critical_count > 0:
                status = 'critical'
                description = f'{critical_count} critical bottlenecks require immediate attention'
            elif medium_count > total_bottlenecks * 0.5:  # More than 50% medium severity
                status = 'degraded'
                description = f'{medium_count} medium severity bottlenecks detected'
            elif total_bottlenecks > 5:
                status = 'degraded'
                description = f'{total_bottlenecks} bottlenecks detected across multiple categories'
            else:
                status = 'healthy'
                description = f'{total_bottlenecks} minor bottlenecks detected'
            
            return {
                'status': status,
                'score': score,
                'description': description,
                'total_bottlenecks': total_bottlenecks,
                'critical_count': critical_count,
                'medium_count': medium_count
            }
            
        except Exception as e:
            logger.error(f"Error assessing bottleneck health: {e}")
            return {
                'status': 'unknown',
                'score': 0,
                'description': 'Unable to assess health status'
            }

    def highlight_node_usage_values(self, value: float, metric_type: str, unit: str, is_top: bool = False, extra_info: str = "") -> str:
        """Highlight node usage values with thresholds"""
        try:
            if is_top:
                formatted_value = self._format_node_usage_value(value, unit) + extra_info
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted_value}</span>'
            
            thresholds = self._get_node_usage_thresholds(metric_type)
            
            if thresholds and isinstance(value, (int, float)):
                critical = thresholds.get('critical', float('inf'))
                warning = thresholds.get('warning', float('inf'))
                
                formatted_value = self._format_node_usage_value(value, unit) + extra_info
                
                if value >= critical:
                    return f'<span class="text-danger font-weight-bold">⚠️ {formatted_value}</span>'
                elif value >= warning:
                    return f'<span class="text-warning font-weight-bold">{formatted_value}</span>'
                else:
                    return f'<span class="text-success">{formatted_value}</span>'
            else:
                return self._format_node_usage_value(value, unit) + extra_info
                
        except (ValueError, TypeError):
            return str(value)

    def _get_node_usage_thresholds(self, metric_type: str) -> Dict[str, float]:
        """Get thresholds for node usage metrics"""
        thresholds_map = {
            'cpu': {'warning': 70.0, 'critical': 85.0},
            'memory': {'warning': 70.0, 'critical': 85.0},
        }
        return thresholds_map.get(metric_type.lower(), {})

    def _format_node_usage_value(self, value: float, unit: str) -> str:
        """Format node usage value with appropriate unit"""
        try:
            unit_lower = unit.lower()
            
            if unit_lower == 'percent':
                return f"{value:.2f}%"
            elif unit_lower == 'gb':
                return f"{value:.2f} GB"
            elif unit_lower == 'mb':
                if value > 1024:
                    return f"{value/1024:.2f} GB"
                return f"{value:.1f} MB"
            else:
                return f"{value:.2f}"
        except (ValueError, TypeError):
            return str(value) 
