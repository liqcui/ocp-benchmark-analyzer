"""
Utility functions for ETCD Analyzer ELT modules
Pure utility functions for formatting, HTML generation, and data processing
NO metric-specific logic - only reusable utilities
"""

import logging
import re
import subprocess
import json
from typing import Dict, Any, List, Union, Tuple, Optional
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


class utilityELT:
    """Pure utility functions for ELT modules - no metric-specific logic"""
    
    def __init__(self):
        self.max_columns = 6
        self._node_role_cache: Dict[str, str] = {}
        self._node_labels_cache: Dict[str, Dict[str, str]] = {}
    
    # ============================================================================
    # TEXT FORMATTING UTILITIES
    # ============================================================================
    
    def truncate_text(self, text: str, max_length: int = 30, suffix: str = '...') -> str:
        """Truncate text for display"""
        if not text:
            return text
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
        if not runtime or len(runtime) <= max_length:
            return runtime
        # Try to keep the version part
        if '://' in runtime:
            protocol, version = runtime.split('://', 1)
            return f"{protocol}://{version[:max_length-len(protocol)-6]}..."
        return runtime[:max_length-3] + '...'
    
    # ============================================================================
    # NODE ROLE QUERY UTILITIES
    # ============================================================================
    
    def get_node_role_from_labels(self, node_name: str, node_labels: Optional[Dict[str, str]] = None) -> str:
        """Get node role from node labels based on node-role.kubernetes.io/master and other role labels.
        
        Args:
            node_name: Name of the node
            node_labels: Optional dictionary of node labels. If not provided, will query via oc CLI.
        
        Returns:
            Node role: 'controlplane', 'infra', 'worker', or 'workload'
        """
        # Check cache first
        if node_name in self._node_role_cache:
            return self._node_role_cache[node_name]
        
        # If labels not provided, try to get them
        if node_labels is None:
            node_labels = self.get_node_labels(node_name)
        
        if not node_labels:
            # Fallback: try to infer from node name
            return self._infer_role_from_name(node_name)
        
        # Check standard Kubernetes node role labels in priority order
        # node-role.kubernetes.io/control-plane (newer label)
        if 'node-role.kubernetes.io/control-plane' in node_labels:
            role = 'controlplane'
        # node-role.kubernetes.io/master (older label, still used in OpenShift)
        elif 'node-role.kubernetes.io/master' in node_labels:
            role = 'controlplane'
        # node-role.kubernetes.io/infra
        elif 'node-role.kubernetes.io/infra' in node_labels:
            role = 'infra'
        # node-role.kubernetes.io/worker
        elif 'node-role.kubernetes.io/worker' in node_labels:
            role = 'worker'
        else:
            # Fallback: infer from node name
            role = self._infer_role_from_name(node_name)
        
        # Cache the result
        self._node_role_cache[node_name] = role
        return role
    
    def get_node_labels(self, node_name: str) -> Dict[str, str]:
        """Get node labels for a specific node using oc CLI.
        
        Args:
            node_name: Name of the node
        
        Returns:
            Dictionary of node labels, or empty dict if query fails
        """
        # Check cache first
        if node_name in self._node_labels_cache:
            return self._node_labels_cache[node_name]
        
        # Try to get labels via oc CLI using JSON output
        try:
            # Try with full node name first
            result = subprocess.run(
                ['oc', 'get', 'node', node_name, '-o', 'json'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0 and result.stdout:
                node_data = json.loads(result.stdout)
                labels = node_data.get('metadata', {}).get('labels', {})
                
                if labels:
                    # Cache the result
                    self._node_labels_cache[node_name] = labels
                    return labels
        except (subprocess.TimeoutExpired, subprocess.SubprocessError, FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            logger.debug(f"Failed to query node labels via oc CLI for {node_name}: {e}")
        
        # If full name failed and node name contains '.', try short name
        if '.' in node_name:
            short_name = node_name.split('.')[0]
            try:
                result = subprocess.run(
                    ['oc', 'get', 'node', short_name, '-o', 'json'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                if result.returncode == 0 and result.stdout:
                    node_data = json.loads(result.stdout)
                    labels = node_data.get('metadata', {}).get('labels', {})
                    
                    if labels:
                        # Cache for both names
                        self._node_labels_cache[node_name] = labels
                        self._node_labels_cache[short_name] = labels
                        return labels
            except (subprocess.TimeoutExpired, subprocess.SubprocessError, FileNotFoundError, json.JSONDecodeError, KeyError) as e:
                logger.debug(f"Failed to query node labels via oc CLI for {short_name}: {e}")
        
        return {}
    
    def _infer_role_from_name(self, node_name: str) -> str:
        """Infer node role from node name as fallback.
        
        Args:
            node_name: Name of the node
        
        Returns:
            Inferred role: 'controlplane', 'infra', 'worker', or 'workload'
        """
        name_lower = (node_name or '').lower()
        
        # etcd pods run on control plane nodes
        if 'etcd' in name_lower:
            return 'controlplane'
        elif 'master' in name_lower or 'control' in name_lower:
            return 'controlplane'
        elif 'infra' in name_lower:
            return 'infra'
        elif 'workload' in name_lower:
            return 'workload'
        else:
            return 'worker'
    
    # ============================================================================
    # CAPACITY PARSING UTILITIES
    # ============================================================================
    
    def parse_cpu_capacity(self, cpu_str: str) -> int:
        """Parse CPU capacity string to integer cores"""
        try:
            if not cpu_str:
                return 0
            cpu_str = str(cpu_str).strip()
            # Handle formats like "32", "32000m"
            if cpu_str.endswith('m'):
                return int(cpu_str[:-1]) // 1000
            return int(float(cpu_str))
        except (ValueError, TypeError, AttributeError):
            return 0
    
    def parse_memory_capacity(self, memory_str: str) -> float:
        """Parse memory capacity string to GB"""
        try:
            if not memory_str:
                return 0.0
            memory_str = str(memory_str).strip()
            
            if memory_str.endswith('Ki'):
                # Convert KiB to GB
                kib = int(memory_str[:-2])
                return kib / (1024 * 1024)
            elif memory_str.endswith('Mi'):
                # Convert MiB to GB  
                mib = int(memory_str[:-2])
                return mib / 1024
            elif memory_str.endswith('Gi'):
                # Already in GiB, close enough to GB
                return float(memory_str[:-2])
            elif memory_str.endswith('G') or memory_str.endswith('GB'):
                return float(memory_str.rstrip('GB').strip())
            elif memory_str.endswith('M') or memory_str.endswith('MB'):
                return float(memory_str.rstrip('MB').strip()) / 1024
            else:
                # Assume it's in bytes, convert to GB
                return int(float(memory_str)) / (1024**3)
        except (ValueError, TypeError, AttributeError):
            return 0.0
    
    def parse_db_size(self, size_str: str) -> float:
        """Parse database size string to MB"""
        try:
            if not size_str:
                return 0.0
            size_str = str(size_str).upper().strip()
            
            if 'MB' in size_str:
                return float(size_str.replace('MB', '').strip())
            elif 'GB' in size_str:
                return float(size_str.replace('GB', '').strip()) * 1024
            elif 'KB' in size_str:
                return float(size_str.replace('KB', '').strip()) / 1024
            else:
                # Try to parse as number (assume MB)
                return float(size_str)
        except (ValueError, AttributeError, TypeError):
            return 0.0
    
    # ============================================================================
    # TIMESTAMP FORMATTING
    # ============================================================================
    
    def format_timestamp(self, timestamp_str: str, length: int = 19) -> str:
        """Format timestamp for display"""
        if not timestamp_str:
            return 'Unknown'
        timestamp_str = str(timestamp_str)
        return timestamp_str[:length]
    
    # ============================================================================
    # HTML UTILITIES
    # ============================================================================
    
    def clean_html(self, html: str) -> str:
        """Clean up HTML (remove newlines and extra whitespace)"""
        if not html:
            return html
        html = re.sub(r'\s+', ' ', html.replace('\n', ' ').replace('\r', ''))
        return html.strip()
    
    def decode_unicode_escapes(self, text: str) -> str:
        """Decode Unicode escape sequences to actual Unicode characters"""
        try:
            if not text or not isinstance(text, str):
                return str(text) if text is not None else ''
            
            original = text
            
            # Handle standard Unicode escapes like \u26a0
            try:
                text = text.encode('utf-8').decode('unicode_escape')
            except (UnicodeDecodeError, UnicodeError):
                pass
            
            # Common Unicode replacements
            replacements = {
                "\\u26a0\\ufe0f": "⚠️",
                "\\u26a0": "⚠",
                "\\u2022": "•",
                "\\u2713": "✓",
                "\\u2717": "✗",
                "\\u27a1": "➡",
                "\\u2b06": "⬆",
                "\\u2b07": "⬇",
                "\\u1f3c6": "🏆",
                "\\u1f4ca": "📊",
                "\\u1f4c8": "📈",
                "\\u1f4c9": "📉",
                "\\u1f534": "🔴",
                "\\u1f7e1": "🟡",
                "\\u1f7e2": "🟢",
                "\\ufe0f": "",
            }
            
            for escape_seq, replacement in replacements.items():
                text = text.replace(escape_seq, replacement)
            
            # Fix common UTF-8 mojibake sequences
            try:
                if any(marker in text for marker in ["Ã", "â€", "Â"]):
                    text = text.encode('latin1', errors='ignore').decode('utf-8', errors='ignore')
            except (UnicodeDecodeError, UnicodeError):
                pass
            
            return text
            
        except Exception as e:
            logger.warning(f"Failed to decode Unicode escapes: {e}")
            return str(text) if text is not None else ''
    
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
        
        color = badge_colors.get(str(status).lower(), 'secondary')
        display_text = value if value else str(status).title()
        return f'<span class="badge badge-{color}">{display_text}</span>'
    
    def create_leader_badge(self, is_leader: bool) -> str:
        """Create HTML badge for leader status"""
        if is_leader:
            return '<span class="badge badge-success">LEADER</span>'
        else:
            return '<span class="text-muted">false</span>'
    
    # ============================================================================
    # VALUE HIGHLIGHTING WITH TOP 1 AND CRITICAL THRESHOLDS
    # ============================================================================
    
    def highlight_critical_values(self, value: Union[float, int], 
                                  thresholds: Dict[str, float], 
                                  unit: str = "", 
                                  is_top: bool = False) -> str:
        """
        Highlight critical values with color coding and top 1 indicator
        
        Args:
            value: The numeric value to format
            thresholds: Dict with 'critical' and 'warning' keys
            unit: Unit to append (e.g., ' GB', ' MB/s')
            is_top: True if this is the top 1 value in the dataset
            
        Returns:
            HTML formatted string with appropriate highlighting
        """
        try:
            value = float(value) if value is not None else 0
        except (ValueError, TypeError):
            return str(value) + unit
        
        critical = thresholds.get('critical', float('inf'))
        warning = thresholds.get('warning', float('inf'))
        
        # Top 1 highlighting - blue background with trophy
        if is_top and value > 0:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {value}{unit}</span>'
        
        # Critical highlighting - red with warning
        elif value >= critical:
            return f'<span class="text-danger font-weight-bold">⚠️ {value}{unit}</span>'
        
        # Warning highlighting - orange/yellow
        elif value >= warning:
            return f'<span class="text-warning font-weight-bold">{value}{unit}</span>'
        
        # Normal value
        else:
            return f'{value}{unit}'
    
    def extract_numeric_value(self, value_str: str) -> float:
        """Extract numeric value from formatted string (e.g., remove HTML tags)"""
        try:
            if isinstance(value_str, (int, float)):
                return float(value_str)
            
            if not value_str:
                return 0.0
            
            # Remove HTML tags first
            clean_str = re.sub(r'<[^>]+>', '', str(value_str))
            
            # Remove emoji and special characters
            clean_str = re.sub(r'[🏆⚠️✓✗➡⬆⬇]', '', clean_str)
            
            # Extract first number from string
            numbers = re.findall(r'[\d.]+', clean_str)
            if numbers:
                return float(numbers[0])
            return 0.0
        except (ValueError, TypeError):
            return 0.0
    
    # ============================================================================
    # DATAFRAME UTILITIES
    # ============================================================================
    
    def limit_dataframe_columns(self, df: pd.DataFrame, max_cols: int = None, 
                               table_name: str = None) -> pd.DataFrame:
        """Limit DataFrame columns to maximum number with intelligent selection"""
        if max_cols is None:
            max_cols = self.max_columns

        if len(df.columns) <= max_cols:
            return df
        
        # Priority columns - always keep if present
        priority_cols = [
            'name', 'status', 'value', 'count', 'property', 
            'rank', 'node', 'type', 'ready', 'cpu', 'memory', 
            'metric', 'pod', 'namespace', 'health'
        ]
        
        # Find priority columns that exist in DataFrame
        keep_cols = []
        for col in df.columns:
            col_lower = str(col).lower()
            if any(priority in col_lower for priority in priority_cols):
                keep_cols.append(col)
                if len(keep_cols) >= max_cols:
                    break
        
        # Add remaining columns up to limit
        remaining_cols = [col for col in df.columns if col not in keep_cols]
        while len(keep_cols) < max_cols and remaining_cols:
            keep_cols.append(remaining_cols.pop(0))
        
        return df[keep_cols[:max_cols]]
    
    def create_html_table(self, df: pd.DataFrame, table_name: str) -> str:
        """Generate HTML table from DataFrame with Unicode handling and styling"""
        try:
            if df is None or df.empty:
                return ""
            
            # Create a copy to avoid modifying original
            df_copy = df.copy()
            
            # Flatten MultiIndex columns if present
            try:
                if isinstance(df_copy.columns, pd.MultiIndex):
                    df_copy.columns = [
                        "_".join([str(part) for part in col if part is not None])
                        for col in df_copy.columns.values
                    ]
                else:
                    df_copy.columns = [str(c) for c in df_copy.columns]
            except Exception as e:
                logger.warning(f"Error flattening columns: {e}")
                df_copy.columns = [str(c) for c in df_copy.columns]

            # Apply Unicode decoding to object columns
            try:
                object_columns = list(df_copy.select_dtypes(include=['object']).columns)
                for col in object_columns:
                    df_copy[col] = df_copy[col].astype(str).apply(self.decode_unicode_escapes)
            except Exception as e:
                logger.warning(f"Error decoding unicode: {e}")
            
            # Create styled HTML table
            html = df_copy.to_html(
                index=False,
                classes='table table-striped table-bordered table-sm',
                escape=False,  # Don't escape HTML in cells (for badges, icons)
                table_id=f"table-{table_name.replace('_', '-')}",
                border=1
            )
            
            # Additional cleanup
            html = self.decode_unicode_escapes(html)
            html = self.clean_html(html)
            
            # Add responsive wrapper
            html = f'<div class="table-responsive">{html}</div>'
            
            return html
            
        except Exception as e:
            logger.error(f"Failed to generate HTML table for {table_name}: {e}")
            return f'<div class="alert alert-danger">Error generating table: {str(e)}</div>'
    
    # ============================================================================
    # CALCULATION UTILITIES
    # ============================================================================
    
    def calculate_totals_from_nodes(self, nodes: List[Dict[str, Any]]) -> Dict[str, Union[int, float]]:
        """Calculate total CPU and memory from node list"""
        if not nodes or not isinstance(nodes, list):
            return {
                'total_cpu': 0,
                'total_memory_gb': 0.0,
                'ready_count': 0,
                'schedulable_count': 0,
                'count': 0
            }
        
        total_cpu = sum(
            self.parse_cpu_capacity(node.get('cpu_capacity', '0')) 
            for node in nodes if isinstance(node, dict)
        )
        
        total_memory_gb = sum(
            self.parse_memory_capacity(node.get('memory_capacity', '0Ki')) 
            for node in nodes if isinstance(node, dict)
        )
        
        ready_count = sum(
            1 for node in nodes 
            if isinstance(node, dict) and 'Ready' in str(node.get('ready_status', ''))
        )
        
        schedulable_count = sum(
            1 for node in nodes 
            if isinstance(node, dict) and node.get('schedulable', False)
        )
        
        return {
            'total_cpu': total_cpu,
            'total_memory_gb': total_memory_gb,
            'ready_count': ready_count,
            'schedulable_count': schedulable_count,
            'count': len([n for n in nodes if isinstance(n, dict)])
        }
    
    def identify_top_values(self, data: List[Dict[str, Any]], value_key: str) -> List[int]:
        """
        Identify indices of top values for highlighting
        Returns list with index of top 1 value
        """
        try:
            if not data or not isinstance(data, list):
                return []
            
            values = []
            for i, item in enumerate(data):
                if isinstance(item, dict):
                    try:
                        val = float(item.get(value_key, 0))
                        values.append((i, val))
                    except (ValueError, TypeError):
                        continue
            
            if not values:
                return []
            
            values.sort(key=lambda x: x[1], reverse=True)
            
            # Return index of top 1
            return [values[0][0]] if values and values[0][1] > 0 else []
            
        except (ValueError, TypeError, AttributeError):
            return []
    
    # ============================================================================
    # CATEGORIZATION UTILITIES
    # ============================================================================
    
    def categorize_resource_type(self, resource_name: str) -> str:
        """Categorize resource type for better organization"""
        if not resource_name:
            return 'Other'
        
        resource_lower = str(resource_name).lower()
        
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
    
    # ============================================================================
    # SAFE VALUE GETTERS
    # ============================================================================
    
    def _safe_int_get(self, data: Dict[str, Any], key: str, default: int = 0) -> int:
        """Safely get integer value from dictionary"""
        try:
            if not isinstance(data, dict):
                return default
            value = data.get(key, default)
            if value is None:
                return default
            return int(float(value))
        except (ValueError, TypeError, AttributeError):
            return default
    
    def _safe_float_get(self, data: Dict[str, Any], key: str, default: float = 0.0) -> float:
        """Safely get float value from dictionary"""
        try:
            if not isinstance(data, dict):
                return default
            value = data.get(key, default)
            if value is None:
                return default
            return float(value)
        except (ValueError, TypeError, AttributeError):
            return default
    
    def _safe_str_get(self, data: Dict[str, Any], key: str, default: str = '') -> str:
        """Safely get string value from dictionary"""
        try:
            if not isinstance(data, dict):
                return default
            value = data.get(key, default)
            return str(value) if value is not None else default
        except (ValueError, TypeError, AttributeError):
            return default
    
    # ============================================================================
    # DISPLAY FORMATTING
    # ============================================================================
    
    def format_memory_display(self, memory_str: str) -> str:
        """Format memory for display"""
        try:
            gb_value = self.parse_memory_capacity(memory_str)
            if gb_value >= 1:
                return f"{gb_value:.0f} GB"
            else:
                return f"{gb_value * 1024:.0f} MB"
        except:
            return str(memory_str) if memory_str else "0 MB"
    
    def format_count_value(self, count: Union[int, float]) -> str:
        """Format count values with appropriate scaling (K, M)"""
        try:
            count = int(float(count))
            if count > 1000000:
                return f"{count/1000000:.1f}M"
            elif count > 1000:
                return f"{count/1000:.1f}K"
            else:
                return f"{count:,}"
        except (ValueError, TypeError):
            return str(count)
    
    def format_percentage(self, value: float, precision: int = 1) -> str:
        """Format percentage values"""
        try:
            return f"{float(value):.{precision}f}%"
        except (ValueError, TypeError):
            return str(value)
    
    def format_bytes_per_second(self, bytes_per_sec: float) -> str:
        """Format bytes per second to readable units"""
        try:
            bytes_per_sec = float(bytes_per_sec)
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
            ops_per_sec = float(ops_per_sec)
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
    
    def format_duration_seconds(self, seconds: float) -> str:
        """Format duration in seconds to readable format"""
        try:
            seconds = float(seconds)
            if seconds < 0.001:
                return f"{seconds*1000000:.0f} μs"
            elif seconds < 1:
                return f"{seconds*1000:.3f} ms"
            elif seconds < 60:
                return f"{seconds:.3f} s"
            else:
                return f"{seconds/60:.1f} min"
        except (ValueError, TypeError):
            return str(seconds)
    
    # ============================================================================
    # PROPERTY-VALUE TABLE UTILITIES
    # ============================================================================
    
    def create_property_value_table(self, data: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """Create a property-value table format"""
        if not data or not isinstance(data, list):
            return []
        
        result = []
        for item in data:
            if isinstance(item, dict):
                result.append({
                    'Property': str(item.get('Property', '')),
                    'Value': str(item.get('Value', ''))
                })
        return result
    
    def flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
        """Flatten nested dictionary for display"""
        items = []
        
        if not isinstance(d, dict):
            return {parent_key: d} if parent_key else {}
        
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                # Only flatten small dicts
                if len(str(v)) < 200:
                    items.extend(self.flatten_dict(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, f"Dict({len(v)} keys)"))
            elif isinstance(v, list):
                # Handle small lists
                if len(v) < 10 and all(isinstance(item, (str, int, float)) for item in v):
                    items.append((new_key, ', '.join(map(str, v))))
                else:
                    items.append((new_key, f"List({len(v)} items)"))
            else:
                items.append((new_key, v))
        
        return dict(items)
    
    # ============================================================================
    # THRESHOLD UTILITIES
    # ============================================================================
    
    def get_default_thresholds(self, metric_name: str) -> Dict[str, float]:
        """Get default thresholds for common metrics"""
        metric_lower = str(metric_name).lower()
        
        # CPU thresholds
        if 'cpu' in metric_lower:
            return {'warning': 70.0, 'critical': 85.0}
        
        # Memory thresholds
        elif 'memory' in metric_lower or 'ram' in metric_lower:
            return {'warning': 70.0, 'critical': 85.0}
        
        # Disk space thresholds
        elif 'disk' in metric_lower and 'space' in metric_lower:
            return {'warning': 70.0, 'critical': 85.0}
        
        # Latency thresholds (ms)
        elif 'latency' in metric_lower or 'duration' in metric_lower:
            return {'warning': 50.0, 'critical': 100.0}
        
        # IOPS thresholds
        elif 'iops' in metric_lower:
            return {'warning': 1000.0, 'critical': 5000.0}
        
        # Throughput thresholds (MB/s)
        elif 'throughput' in metric_lower:
            return {'warning': 100.0, 'critical': 500.0}
        
        # Default thresholds
        else:
            return {'warning': 70.0, 'critical': 90.0}
    
    # ============================================================================
    # ERROR HANDLING UTILITIES
    # ============================================================================
    
    def safe_divide(self, numerator: Union[int, float], 
                   denominator: Union[int, float], 
                   default: float = 0.0) -> float:
        """Safely divide two numbers, returning default on error"""
        try:
            num = float(numerator)
            den = float(denominator)
            if den == 0:
                return default
            return num / den
        except (ValueError, TypeError, ZeroDivisionError):
            return default
    
    def safe_percentage(self, part: Union[int, float], 
                       total: Union[int, float], 
                       precision: int = 1) -> str:
        """Safely calculate percentage"""
        try:
            if float(total) == 0:
                return "0%"
            percentage = (float(part) / float(total)) * 100
            return f"{percentage:.{precision}f}%"
        except (ValueError, TypeError, ZeroDivisionError):
            return "N/A"

    def format_network_speed(self, bits_per_sec: float) -> str:
        """Format network speed to readable units"""
        try:
            bits_per_sec = float(bits_per_sec)
            if bits_per_sec == 0:
                return "0 bps"
            elif bits_per_sec < 1000:
                return f"{bits_per_sec:.0f} bps"
            elif bits_per_sec < 1000000:
                return f"{bits_per_sec/1000:.1f} Kbps"
            elif bits_per_sec < 1000000000:
                return f"{bits_per_sec/1000000:.1f} Mbps"
            else:
                return f"{bits_per_sec/1000000000:.2f} Gbps"
        except (ValueError, TypeError):
            return str(bits_per_sec)

    def format_mtu_bytes(self, bytes_val: float) -> str:
        """Format MTU bytes to readable format"""
        try:
            bytes_val = float(bytes_val)
            return f"{int(bytes_val)} bytes"
        except (ValueError, TypeError):
            return str(bytes_val)

    def format_status(self, status: str) -> str:
        """Format Yes/No status with badge"""
        if str(status).lower() in ['yes', '1', 'true']:
            return self.create_status_badge('success', 'Yes')
        else:
            return self.create_status_badge('danger', 'No')            

    # ============================================================================
    # GENERIC VALUE + UNIT FORMATTERS (REUSABLE ACROSS METRICS)
    # ============================================================================
    def format_value_with_unit(self, value: float, unit: str) -> str:
        """Format a numeric value with a unit into a readable string.
        Supports common units like count, bytes, bits_per_second, bytes_per_second, percent, iops, packets_per_second.
        """
        try:
            unit_lower = (unit or '').strip().lower()
            if unit_lower in ['', 'count', 'connections', 'sockets']:
                return self.format_count_value(value)
            elif unit_lower in ['bytes', 'byte']:
                # Reuse bytes/s formatter scale logic but without /s suffix
                val = float(value)
                if val < 1024:
                    return f"{val:.0f} B"
                elif val < 1024**2:
                    return f"{val/1024:.1f} KB"
                elif val < 1024**3:
                    return f"{val/(1024**2):.1f} MB"
                else:
                    return f"{val/(1024**3):.2f} GB"
            elif unit_lower in ['bytes_per_second', 'b/s', 'byte/s']:
                return self.format_bytes_per_second(value)
            elif unit_lower in ['bits_per_second', 'bps']:
                return self.format_network_speed(value)
            elif unit_lower in ['percent', '%']:
                return self.format_percentage(value)
            elif unit_lower in ['iops', 'ops', 'ops_per_second']:
                return self.format_operations_per_second(value)
            elif unit_lower in ['packets_per_second', 'pps', 'packets/s']:  # NEW LINE
                return self.format_packets_per_second(value)                # NEW LINE
            elif unit_lower in ['seconds', 'sec', 's']:
                return self.format_duration_seconds(value)
            else:
                # Fallback: plain value with unit suffix
                try:
                    val = float(value)
                    return f"{val:g} {unit}".strip()
                except (ValueError, TypeError):
                    return f"{value} {unit}".strip()
        except Exception:
            return f"{value} {unit}".strip()

    def format_and_highlight(self, value: float, unit: str, 
                            thresholds: Dict[str, float], 
                            is_top: bool = False) -> str:
        """Format a value with unit and apply highlight thresholds"""
        try:
            value = float(value) if value is not None else 0
        except (ValueError, TypeError):
            return str(value)
        
        # Format the value with unit
        readable = self.format_value_with_unit(value, unit)
        
        critical = thresholds.get('critical', float('inf'))
        warning = thresholds.get('warning', float('inf'))
        
        # Top 1 highlighting
        if is_top and value > 0:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {readable}</span>'
        # Critical highlighting
        elif value >= critical:
            return f'<span class="text-danger font-weight-bold">⚠️ {readable}</span>'
        # Warning highlighting
        elif value >= warning:
            return f'<span class="text-warning font-weight-bold">{readable}</span>'
        else:
            return readable

    def format_packets_per_second(self, packets_per_sec: float) -> str:
        """Format packets per second to readable units"""
        try:
            packets_per_sec = float(packets_per_sec)
            if packets_per_sec == 0:
                return "0 pps"
            elif packets_per_sec < 1:
                return f"{packets_per_sec:.3f} pps"
            elif packets_per_sec < 1000:
                return f"{packets_per_sec:.1f} pps"
            elif packets_per_sec < 1000000:
                return f"{packets_per_sec/1000:.1f}K pps"
            else:
                return f"{packets_per_sec/1000000:.2f}M pps"
        except (ValueError, TypeError):
            return str(packets_per_sec)      

    def format_latency_ms(self, seconds: float) -> str:
        """Format latency from seconds to milliseconds with appropriate precision"""
        try:
            seconds = float(seconds)
            ms = seconds * 1000
            if ms < 0.001:
                return f"{ms * 1000:.2f} Î¼s"
            elif ms < 1:
                return f"{ms:.3f} ms"
            elif ms < 10:
                return f"{ms:.2f} ms"
            elif ms < 100:
                return f"{ms:.1f} ms"
            else:
                return f"{ms:.0f} ms"
        except (ValueError, TypeError):
            return str(seconds)

    def format_latency_seconds(self, seconds: float) -> str:
        """Format latency in seconds with appropriate unit"""
        try:
            seconds = float(seconds)
            if seconds < 0.000001:
                return f"{seconds * 1000000:.2f} Î¼s"
            elif seconds < 0.001:
                return f"{seconds * 1000:.3f} ms"
            elif seconds < 1:
                return f"{seconds * 1000:.2f} ms"
            else:
                return f"{seconds:.3f} s"
        except (ValueError, TypeError):
            return str(seconds)

    def get_latency_thresholds_ms(self) -> Dict[str, float]:
        """Get standard latency thresholds in milliseconds"""
        return {
            'critical': 50.0,  # >50ms is critical
            'warning': 20.0     # >20ms is warning
        }

    def get_rate_thresholds(self) -> Dict[str, float]:
        """Get standard rate/throughput thresholds"""
        return {
            'critical': 2000.0,  # Very high rate might indicate issues
            'warning': 1000.0
        }

    def highlight_latency_value(self, value_ms: float, is_top: bool = False) -> str:
        """Highlight latency value with color coding"""
        try:
            thresholds = self.get_latency_thresholds_ms()
            formatted = self.format_latency_ms(value_ms / 1000)  # Convert back to seconds for formatting
            
            if is_top:
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
            elif value_ms >= thresholds['critical']:
                return f'<span class="text-danger font-weight-bold">⚠️ {formatted}</span>'
            elif value_ms >= thresholds['warning']:
                return f'<span class="text-warning font-weight-bold">{formatted}</span>'
            else:
                return f'<span class="text-success">{formatted}</span>'
        except (ValueError, TypeError):
            return str(value_ms)

    def highlight_rate_value(self, value: float, unit: str = "ops/sec", is_top: bool = False) -> str:
        """Highlight rate/throughput value with color coding"""
        try:
            formatted = self.format_operations_per_second(value)
            
            if is_top:
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
            else:
                return formatted
        except (ValueError, TypeError):
            return str(value)

    def highlight_general_info_values(self, value: float, metric_name: str, 
                                    unit: str = "", is_top: bool = False) -> str:
        """
        Highlight general info metric values with color coding and top 1 indicator
        
        Args:
            value: The numeric value to format
            metric_name: Name of the metric for threshold lookup
            unit: Unit to append
            is_top: True if this is the top 1 value
            
        Returns:
            HTML formatted string with appropriate highlighting
        """
        try:
            value = float(value) if value is not None else 0
        except (ValueError, TypeError):
            return str(value) + unit
        
        # Define thresholds for different metric types
        thresholds = {
            'cpu_usage': {'critical': 80.0, 'warning': 60.0},
            'memory_usage': {'critical': 85.0, 'warning': 70.0},
            'db_space_used_percent': {'critical': 80.0, 'warning': 60.0},
            'db_physical_size': {'critical': 8000.0, 'warning': 6000.0},  # MB
            'db_logical_size': {'critical': 4000.0, 'warning': 3000.0},  # MB
            'proposal_failure_rate': {'critical': 0.1, 'warning': 0.01},
            'proposal_pending_total': {'critical': 10.0, 'warning': 5.0},
            'leader_changes_rate': {'critical': 1.0, 'warning': 0.1},
            'slow_applies': {'critical': 1.0, 'warning': 0.1},
            'slow_read_indexes': {'critical': 1.0, 'warning': 0.1},
            'cpu_io_utilization_iowait': {'critical': 10.0, 'warning': 5.0},
            'vmstat_pgmajfault_rate': {'critical': 1.0, 'warning': 0.5}
        }
        
        metric_thresholds = thresholds.get(metric_name, {'critical': 80.0, 'warning': 60.0})
        
        # Format value with unit
        formatted_value = self.format_value_with_unit(value, unit)
        
        # Top 1 highlighting
        if is_top and value > 0:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted_value}</span>'
        
        # Critical highlighting
        elif value >= metric_thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {formatted_value}</span>'
        
        # Warning highlighting
        elif value >= metric_thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{formatted_value}</span>'
        
        # Normal value
        else:
            return formatted_value

    def format_latency_seconds(self, seconds: float) -> str:
        """Format latency in seconds with appropriate unit"""
        try:
            seconds = float(seconds)
            if seconds < 0.000001:
                return f"{seconds * 1000000:.2f} μs"
            elif seconds < 0.001:
                return f"{seconds * 1000:.3f} ms"
            elif seconds < 1:
                return f"{seconds * 1000:.2f} ms"
            else:
                return f"{seconds:.3f} s"
        except (ValueError, TypeError):
            return str(seconds)

    def get_latency_thresholds_ms(self) -> Dict[str, float]:
        """Get standard latency thresholds in milliseconds"""
        return {
            'critical': 100.0,  # >100ms is critical
            'warning': 50.0     # >50ms is warning
        }

    def highlight_latency_value(self, value_seconds: float, is_top: bool = False) -> str:
        """Highlight latency value with color coding"""
        try:
            value_ms = float(value_seconds) * 1000  # Convert to ms for threshold comparison
            thresholds = self.get_latency_thresholds_ms()
            formatted = self.format_latency_seconds(value_seconds)
            
            if is_top:
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
            elif value_ms >= thresholds['critical']:
                return f'<span class="text-danger font-weight-bold">⚠️ {formatted}</span>'
            elif value_ms >= thresholds['warning']:
                return f'<span class="text-warning font-weight-bold">{formatted}</span>'
            else:
                return f'<span class="text-success">{formatted}</span>'
        except (ValueError, TypeError):
            return str(value_seconds)  

    def format_flow_count(self, flows: Union[int, float]) -> str:
        """Format flow counts to readable units"""
        try:
            flows = int(float(flows))
            if flows == 0:
                return "0"
            elif flows < 1000:
                return f"{flows:,}"
            elif flows < 1000000:
                return f"{flows/1000:.1f}K"
            else:
                return f"{flows/1000000:.2f}M"
        except (ValueError, TypeError):
            return str(flows)

    def get_ovs_metric_thresholds(self, metric_name: str) -> Dict[str, float]:
        """Get thresholds for OVS metrics"""
        metric_lower = str(metric_name).lower()
        
        if 'cpu' in metric_lower:
            return {'critical': 80.0, 'warning': 60.0}
        elif 'memory' in metric_lower or 'size_bytes' in metric_lower:
            # Memory in GB
            return {'critical': 2.0, 'warning': 1.5}
        elif 'flows' in metric_lower:
            return {'critical': 100000.0, 'warning': 50000.0}
        elif 'connections' in metric_lower:
            return {'critical': 1000.0, 'warning': 500.0}
        elif 'overflow' in metric_lower or 'discarded' in metric_lower:
            return {'critical': 100.0, 'warning': 10.0}
        elif 'cache_hits' in metric_lower or 'cache_misses' in metric_lower:
            return {'critical': 1000000.0, 'warning': 500000.0}
        elif 'packet_rate' in metric_lower:
            return {'critical': 100000.0, 'warning': 50000.0}
        elif 'error_rate' in metric_lower:
            return {'critical': 100.0, 'warning': 10.0}
        else:
            return {'critical': 80.0, 'warning': 60.0}

    def highlight_ovs_value(self, value: float, metric_name: str, 
                        unit: str = "", is_top: bool = False) -> str:
        """Highlight OVS metric values with color coding"""
        try:
            value = float(value) if value is not None else 0
        except (ValueError, TypeError):
            return str(value) + unit
        
        thresholds = self.get_ovs_metric_thresholds(metric_name)
        
        # Format value based on unit
        if unit == 'percent':
            formatted = self.format_percentage(value)
        elif unit == 'bytes':
            formatted = self.format_memory_display(f"{value}B")
        elif unit == 'flows':
            formatted = self.format_flow_count(value)
        elif unit == 'bytes_per_second':
            formatted = self.format_bytes_per_second(value)
        elif unit == 'packets_per_second':
            formatted = self.format_packets_per_second(value)
        else:
            formatted = f"{value:g} {unit}".strip()
        
        # Apply highlighting
        if is_top and value > 0:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
        elif value >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {formatted}</span>'
        elif value >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{formatted}</span>'
        else:
            return formatted

    def format_iops(self, iops: float) -> str:
        """Format IOPS values to readable units"""
        try:
            iops = float(iops)
            if iops == 0:
                return "0 IOPS"
            elif iops < 1:
                return f"{iops:.3f} IOPS"
            elif iops < 1000:
                return f"{iops:.2f} IOPS"
            else:
                return f"{iops/1000:.2f}K IOPS"
        except (ValueError, TypeError):
            return str(iops)

    def format_threads(self, threads: float) -> str:
        """Format thread count"""
        try:
            threads = int(float(threads))
            if threads < 1000:
                return f"{threads}"
            else:
                return f"{threads/1000:.1f}K"
        except (ValueError, TypeError):
            return str(threads)

    def get_iops_thresholds(self) -> Dict[str, float]:
        """Get standard IOPS thresholds"""
        return {
            'critical': 50.0,  # >50 IOPS might indicate issues
            'warning': 30.0
        }

    def get_thread_thresholds(self) -> Dict[str, float]:
        """Get standard thread count thresholds"""
        return {
            'critical': 2000.0,  # >2000 threads
            'warning': 1500.0
        }

    def highlight_iops_value(self, value: float, is_top: bool = False) -> str:
        """Highlight IOPS value with color coding"""
        try:
            thresholds = self.get_iops_thresholds()
            formatted = self.format_iops(value)
            
            if is_top:
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
            elif value >= thresholds['critical']:
                return f'<span class="text-danger font-weight-bold">⚠️ {formatted}</span>'
            elif value >= thresholds['warning']:
                return f'<span class="text-warning font-weight-bold">{formatted}</span>'
            else:
                return formatted
        except (ValueError, TypeError):
            return str(value)

    def highlight_thread_value(self, value: float, is_top: bool = False) -> str:
        """Highlight thread count with color coding"""
        try:
            thresholds = self.get_thread_thresholds()
            formatted = self.format_threads(value)
            
            if is_top:
                return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {formatted}</span>'
            elif value >= thresholds['critical']:
                return f'<span class="text-danger font-weight-bold">⚠️ {formatted}</span>'
            elif value >= thresholds['warning']:
                return f'<span class="text-warning font-weight-bold">{formatted}</span>'
            else:
                return formatted
        except (ValueError, TypeError):
            return str(value)