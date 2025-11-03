"""
Complete updated implementation for etcd_analyzer_elt_node_usage.py
Adds percentage columns to memory tables with automatic capacity detection
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from .etcd_analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class nodeUsageELT(utilityELT):
    """ELT processor for Node Usage metrics"""
    
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        # Store node capacities if available
        self.node_capacities = {}
    
    def _add_blank_row_between_nodes(self, df: pd.DataFrame, node_col: str = 'Node') -> pd.DataFrame:
        """Insert a blank spacer row between each node group for better HTML readability."""
        try:
            if df.empty or node_col not in df.columns:
                return df
            parts = []
            for _, group in df.groupby(node_col, sort=False):
                parts.append(group)
                spacer = {col: '&nbsp;' for col in df.columns}
                parts.append(pd.DataFrame([spacer]))
            if parts:
                parts = parts[:-1]
            combined = pd.concat(parts, ignore_index=True) if parts else df
            return combined
        except Exception as e:
            self.logger.warning(f"Failed to insert blank rows between nodes: {e}")
            return df

    def extract_node_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract node usage metrics from JSON data"""
        try:
            # Handle nested structure
            if 'data' in data and isinstance(data.get('data'), dict):
                metrics_data = data['data']
            else:
                metrics_data = data
            
            if metrics_data.get('status') != 'success':
                return {'error': 'Node usage data collection failed'}
            
            structured = {
                'timestamp': metrics_data.get('timestamp', ''),
                'duration': metrics_data.get('duration', ''),
                'time_range': metrics_data.get('time_range', {}),
                'node_group': metrics_data.get('node_group', 'unknown'),
                'total_nodes': metrics_data.get('total_nodes', 0),
                'metrics': {}
            }
            
            # Extract node capacities if available (for percentage calculation)
            if 'node_capacities' in metrics_data:
                self.node_capacities = metrics_data['node_capacities']
            
            metrics = metrics_data.get('metrics', {})
            
            # Extract CPU usage
            if 'cpu_usage' in metrics:
                structured['metrics']['cpu_usage'] = self._extract_cpu_usage(metrics['cpu_usage'])
            
            # Extract memory used (with percentage support)
            if 'memory_used' in metrics:
                structured['metrics']['memory_used'] = self._extract_memory_used(
                    metrics['memory_used']
                )
            
            # Extract memory cache/buffer (with percentage support)
            if 'memory_cache_buffer' in metrics:
                structured['metrics']['memory_cache_buffer'] = self._extract_memory_cache_buffer(
                    metrics['memory_cache_buffer']
                )
            
            # Extract cgroup CPU usage
            if 'cgroup_cpu_usage' in metrics:
                structured['metrics']['cgroup_cpu_usage'] = self._extract_cgroup_cpu_usage(
                    metrics['cgroup_cpu_usage']
                )
            
            # Extract cgroup RSS usage
            if 'cgroup_rss_usage' in metrics:
                structured['metrics']['cgroup_rss_usage'] = self._extract_cgroup_rss_usage(
                    metrics['cgroup_rss_usage']
                )
            
            return structured
            
        except Exception as e:
            self.logger.error(f"Error extracting node usage data: {e}", exc_info=True)
            return {'error': str(e)}
    
    def _extract_memory_used(self, memory_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract memory used metrics per node with automatic percentage calculation"""
        result = {
            'metric': memory_data.get('metric', 'node_memory_used'),
            'description': memory_data.get('description', ''),
            'nodes': []
        }
        
        nodes_data = memory_data.get('nodes', {})
        for node_name, node_metrics in nodes_data.items():
            node_info = {
                'node': self._extract_node_short_name(node_name),
                'node_fqdn': node_name,
                'avg': node_metrics.get('avg', 0.0),
                'max': node_metrics.get('max', 0.0),
                'unit': node_metrics.get('unit', 'GB')
            }
            
            # Try to get total capacity from multiple sources
            total_capacity = None
            
            # 1. Check if it's in the node_metrics
            if 'total_capacity' in node_metrics:
                total_capacity = node_metrics['total_capacity']
            
            # 2. Check if we stored it from node_capacities
            elif node_name in self.node_capacities:
                total_capacity = self.node_capacities[node_name].get('memory', 0.0)
            
            # 3. Try short name
            elif node_info['node'] in self.node_capacities:
                total_capacity = self.node_capacities[node_info['node']].get('memory', 0.0)
            
            # Calculate percentages if we have capacity
            if total_capacity and total_capacity > 0:
                node_info['total_capacity'] = total_capacity
                node_info['avg_percent'] = (node_info['avg'] / total_capacity) * 100
                node_info['max_percent'] = (node_info['max'] / total_capacity) * 100
            
            result['nodes'].append(node_info)
        
        return result
    
    def _extract_memory_cache_buffer(self, cache_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract memory cache/buffer metrics per node with automatic percentage calculation"""
        result = {
            'metric': cache_data.get('metric', 'node_memory_cache_buffer'),
            'description': cache_data.get('description', ''),
            'nodes': []
        }
        
        nodes_data = cache_data.get('nodes', {})
        for node_name, node_metrics in nodes_data.items():
            node_info = {
                'node': self._extract_node_short_name(node_name),
                'node_fqdn': node_name,
                'avg': node_metrics.get('avg', 0.0),
                'max': node_metrics.get('max', 0.0),
                'unit': node_metrics.get('unit', 'GB')
            }
            
            # Try to get total capacity from multiple sources
            total_capacity = None
            
            # 1. Check if it's in the node_metrics
            if 'total_capacity' in node_metrics:
                total_capacity = node_metrics['total_capacity']
            
            # 2. Check if we stored it from node_capacities
            elif node_name in self.node_capacities:
                total_capacity = self.node_capacities[node_name].get('memory', 0.0)
            
            # 3. Try short name
            elif node_info['node'] in self.node_capacities:
                total_capacity = self.node_capacities[node_info['node']].get('memory', 0.0)
            
            # Calculate percentages if we have capacity
            if total_capacity and total_capacity > 0:
                node_info['total_capacity'] = total_capacity
                node_info['avg_percent'] = (node_info['avg'] / total_capacity) * 100
                node_info['max_percent'] = (node_info['max'] / total_capacity) * 100
            
            result['nodes'].append(node_info)
        
        return result
    
    def _extract_cpu_usage(self, cpu_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract CPU usage metrics per node and mode"""
        result = {
            'metric': cpu_data.get('metric', 'node_cpu_usage'),
            'description': cpu_data.get('description', ''),
            'nodes': []
        }
        
        nodes_data = cpu_data.get('nodes', {})
        for node_name, node_metrics in nodes_data.items():
            modes_data = node_metrics.get('modes', {})
            
            for mode, mode_metrics in modes_data.items():
                result['nodes'].append({
                    'node': self._extract_node_short_name(node_name),
                    'node_fqdn': node_name,
                    'mode': mode,
                    'avg': mode_metrics.get('avg', 0.0),
                    'max': mode_metrics.get('max', 0.0),
                    'unit': mode_metrics.get('unit', 'percent')
                })
            
            # Add total row
            total = node_metrics.get('total', {})
            if total:
                result['nodes'].append({
                    'node': self._extract_node_short_name(node_name),
                    'node_fqdn': node_name,
                    'mode': 'TOTAL',
                    'avg': total.get('avg', 0.0),
                    'max': total.get('max', 0.0),
                    'unit': total.get('unit', 'percent')
                })
        
        return result
    
    def _extract_cgroup_cpu_usage(self, cgroup_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cgroup CPU usage metrics per node and cgroup"""
        result = {
            'metric': cgroup_data.get('metric', 'cgroup_cpu_usage'),
            'description': cgroup_data.get('description', ''),
            'nodes': []
        }
        
        nodes_data = cgroup_data.get('nodes', {})
        for node_name, node_metrics in nodes_data.items():
            cgroups_data = node_metrics.get('cgroups', {})
            
            for cgroup, cgroup_metrics in cgroups_data.items():
                result['nodes'].append({
                    'node': self._extract_node_short_name(node_name),
                    'node_fqdn': node_name,
                    'cgroup': cgroup,
                    'avg': cgroup_metrics.get('avg', 0.0),
                    'max': cgroup_metrics.get('max', 0.0),
                    'unit': cgroup_metrics.get('unit', 'percent')
                })
            
            # Add total row
            total = node_metrics.get('total', {})
            if total:
                result['nodes'].append({
                    'node': self._extract_node_short_name(node_name),
                    'node_fqdn': node_name,
                    'cgroup': 'TOTAL',
                    'avg': total.get('avg', 0.0),
                    'max': total.get('max', 0.0),
                    'unit': total.get('unit', 'percent')
                })
        
        return result
    
    def _extract_cgroup_rss_usage(self, rss_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cgroup RSS memory usage metrics per node and cgroup"""
        result = {
            'metric': rss_data.get('metric', 'cgroup_rss_usage'),
            'description': rss_data.get('description', ''),
            'nodes': []
        }
        
        nodes_data = rss_data.get('nodes', {})
        for node_name, node_metrics in nodes_data.items():
            cgroups_data = node_metrics.get('cgroups', {})
            
            for cgroup, cgroup_metrics in cgroups_data.items():
                result['nodes'].append({
                    'node': self._extract_node_short_name(node_name),
                    'node_fqdn': node_name,
                    'cgroup': cgroup,
                    'avg': cgroup_metrics.get('avg', 0.0),
                    'max': cgroup_metrics.get('max', 0.0),
                    'unit': cgroup_metrics.get('unit', 'GB')
                })
            
            # Add total row
            total = node_metrics.get('total', {})
            if total:
                result['nodes'].append({
                    'node': self._extract_node_short_name(node_name),
                    'node_fqdn': node_name,
                    'cgroup': 'TOTAL',
                    'avg': total.get('avg', 0.0),
                    'max': total.get('max', 0.0),
                    'unit': total.get('unit', 'GB')
                })
        
        return result
    
    def _extract_node_short_name(self, fqdn: str) -> str:
        """Extract short hostname from FQDN"""
        return fqdn.split('.')[0] if '.' in fqdn else fqdn
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        try:
            dataframes = {}
            metrics = structured_data.get('metrics', {})
            
            # CPU Usage DataFrame
            if 'cpu_usage' in metrics:
                cpu_nodes = metrics['cpu_usage'].get('nodes', [])
                if cpu_nodes:
                    df = pd.DataFrame(cpu_nodes)
                    df = df[['node', 'mode', 'avg', 'max', 'unit']]
                    df = df[df['mode'] != 'TOTAL']
                    df.columns = ['Node', 'Mode', 'Avg', 'Max', 'Unit']
                    dataframes['cpu_usage'] = df
            
            # Memory Used DataFrame - with percentage columns if available
            if 'memory_used' in metrics:
                memory_nodes = metrics['memory_used'].get('nodes', [])
                if memory_nodes:
                    df = pd.DataFrame(memory_nodes)
                    
                    # Check if we have percentage data
                    if 'avg_percent' in df.columns and 'max_percent' in df.columns:
                        # New format with percentages
                        df = df[['node', 'avg', 'avg_percent', 'max', 'max_percent']]
                        df.columns = ['Node', 'Avg (GB)', 'RAM Used (%)', 'Max (GB)', 'Max RAM (%)']
                        self.logger.info("Created memory_used DataFrame WITH percentage columns")
                    else:
                        # Fallback format without percentages
                        df = df[['node', 'avg', 'max', 'unit']]
                        df.columns = ['Node', 'Avg', 'Max', 'Unit']
                        self.logger.warning("Created memory_used DataFrame WITHOUT percentage columns - total_capacity not available")
                    
                    dataframes['memory_used'] = df
            
            # Memory Cache/Buffer DataFrame - with percentage columns if available
            if 'memory_cache_buffer' in metrics:
                cache_nodes = metrics['memory_cache_buffer'].get('nodes', [])
                if cache_nodes:
                    df = pd.DataFrame(cache_nodes)
                    
                    # Check if we have percentage data
                    if 'avg_percent' in df.columns and 'max_percent' in df.columns:
                        # New format with percentages
                        df = df[['node', 'avg', 'avg_percent', 'max', 'max_percent']]
                        df.columns = ['Node', 'Avg (GB)', 'Cache/Buffer (%)', 'Max (GB)', 'Max Cache (%)']
                        self.logger.info("Created memory_cache_buffer DataFrame WITH percentage columns")
                    else:
                        # Fallback format without percentages
                        df = df[['node', 'avg', 'max', 'unit']]
                        df.columns = ['Node', 'Avg', 'Max', 'Unit']
                        self.logger.warning("Created memory_cache_buffer DataFrame WITHOUT percentage columns - total_capacity not available")
                    
                    dataframes['memory_cache_buffer'] = df
            
            # Cgroup CPU Usage DataFrame
            if 'cgroup_cpu_usage' in metrics:
                cgroup_cpu_nodes = metrics['cgroup_cpu_usage'].get('nodes', [])
                if cgroup_cpu_nodes:
                    df = pd.DataFrame(cgroup_cpu_nodes)
                    df = df[['node', 'cgroup', 'avg', 'max', 'unit']]
                    df = df[df['cgroup'] != 'TOTAL']
                    df.columns = ['Node', 'Cgroup', 'Avg', 'Max', 'Unit']
                    dataframes['cgroup_cpu_usage'] = df
            
            # Cgroup RSS Usage DataFrame
            if 'cgroup_rss_usage' in metrics:
                cgroup_rss_nodes = metrics['cgroup_rss_usage'].get('nodes', [])
                if cgroup_rss_nodes:
                    df = pd.DataFrame(cgroup_rss_nodes)
                    df = df[['node', 'cgroup', 'avg', 'max', 'unit']]
                    df = df[df['cgroup'] != 'TOTAL']
                    df.columns = ['Node', 'Cgroup', 'Avg', 'Max', 'Unit']
                    dataframes['cgroup_rss_usage'] = df
            
            return dataframes
            
        except Exception as e:
            self.logger.error(f"Error transforming to DataFrames: {e}", exc_info=True)
            return {}
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables with highlighting"""
        try:
            html_tables = {}
            
            # CPU Usage Table
            if 'cpu_usage' in dataframes:
                df = dataframes['cpu_usage'].copy()
                df = self._format_cpu_usage_table(df)
                df = self._add_blank_row_between_nodes(df, 'Node')
                html_tables['cpu_usage'] = self.create_html_table(df, 'cpu_usage')
            
            # Memory Used Table
            if 'memory_used' in dataframes:
                df = dataframes['memory_used'].copy()
                df = self._format_memory_table(df, 'memory')
                df = self._add_blank_row_between_nodes(df, 'Node')
                html_tables['memory_used'] = self.create_html_table(df, 'memory_used')
            
            # Memory Cache/Buffer Table
            if 'memory_cache_buffer' in dataframes:
                df = dataframes['memory_cache_buffer'].copy()
                df = self._format_memory_table(df, 'cache')
                df = self._add_blank_row_between_nodes(df, 'Node')
                html_tables['memory_cache_buffer'] = self.create_html_table(df, 'memory_cache_buffer')
            
            # Cgroup CPU Usage Table
            if 'cgroup_cpu_usage' in dataframes:
                df = dataframes['cgroup_cpu_usage'].copy()
                df = self._format_cgroup_cpu_table(df)
                df = self._add_blank_row_between_nodes(df, 'Node')
                html_tables['cgroup_cpu_usage'] = self.create_html_table(df, 'cgroup_cpu_usage')
            
            # Cgroup RSS Usage Table
            if 'cgroup_rss_usage' in dataframes:
                df = dataframes['cgroup_rss_usage'].copy()
                df = self._format_cgroup_rss_table(df)
                df = self._add_blank_row_between_nodes(df, 'Node')
                html_tables['cgroup_rss_usage'] = self.create_html_table(df, 'cgroup_rss_usage')
            
            return html_tables
            
        except Exception as e:
            self.logger.error(f"Error generating HTML tables: {e}", exc_info=True)
            return {}
    
    def _format_memory_table(self, df: pd.DataFrame, metric_type: str) -> pd.DataFrame:
        """Format memory usage table with highlighting"""
        # Check if we have percentage columns
        has_percentages = any('(%)' in str(col) for col in df.columns)
        
        self.logger.info(f"Formatting memory table - has_percentages: {has_percentages}, columns: {df.columns.tolist()}")
        
        if has_percentages:
            # NEW FORMAT: With percentage columns
            # Find the percentage columns dynamically
            pct_cols = [col for col in df.columns if '(%)' in str(col)]
            
            if len(pct_cols) >= 2:
                avg_pct_col = pct_cols[0]  # First percentage column (Avg)
                max_pct_col = pct_cols[1]  # Second percentage column (Max)
                
                # Identify top value based on Max percentage
                max_idx = df[max_pct_col].idxmax()
                
                # Format values
                for idx, row in df.iterrows():
                    is_top = idx == max_idx
                    
                    # Format Avg (GB) - plain formatting
                    if 'Avg (GB)' in df.columns:
                        avg_gb = row['Avg (GB)']
                        df.at[idx, 'Avg (GB)'] = f"{avg_gb:.2f}"
                    
                    # Format percentage column with highlighting
                    avg_pct = row[avg_pct_col]
                    df.at[idx, avg_pct_col] = self.highlight_node_usage_values(
                        avg_pct, 'memory', 'percent', is_top=False
                    )
                    
                    # Format Max (GB) - plain formatting
                    if 'Max (GB)' in df.columns:
                        max_gb = row['Max (GB)']
                        df.at[idx, 'Max (GB)'] = f"{max_gb:.2f}"
                    
                    # Format Max percentage with highlighting and top marker
                    max_pct = row[max_pct_col]
                    df.at[idx, max_pct_col] = self.highlight_node_usage_values(
                        max_pct, 'memory', 'percent', is_top=is_top
                    )
                
                return df
        
        # FALLBACK FORMAT: Without percentages
        self.logger.info("Using fallback format without percentages")
        max_idx = df['Max'].idxmax()
        
        for idx, row in df.iterrows():
            unit = row['Unit']
            is_top = idx == max_idx
            
            # Format Avg
            avg_val = row['Avg']
            thresholds = self._get_memory_thresholds()
            df.at[idx, 'Avg'] = self.highlight_node_usage_values(
                avg_val, 'memory', unit, is_top=False
            )
            
            # Format Max
            max_val = row['Max']
            df.at[idx, 'Max'] = self.highlight_node_usage_values(
                max_val, 'memory', unit, is_top=is_top
            )
        
        # Remove Unit column
        df = df.drop('Unit', axis=1)
        
        return df
    
    def _format_cpu_usage_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format CPU usage table with highlighting"""
        # Identify top values per mode
        top_indices = {}
        for mode in df['Mode'].unique():
            mode_df = df[df['Mode'] == mode]
            if not mode_df.empty and mode != 'TOTAL':
                max_idx = mode_df['Max'].idxmax()
                top_indices[max_idx] = True
        
        # Format values with highlighting
        for idx, row in df.iterrows():
            unit = row['Unit']
            is_top = idx in top_indices
            is_total = row['Mode'] == 'TOTAL'
            
            # Format Avg
            avg_val = row['Avg']
            if is_total:
                df.at[idx, 'Avg'] = f'<strong>{avg_val:.2f} {unit}</strong>'
            else:
                thresholds = self._get_cpu_thresholds()
                df.at[idx, 'Avg'] = self.highlight_node_usage_values(
                    avg_val, 'cpu', unit, is_top=False
                )
            
            # Format Max
            max_val = row['Max']
            if is_total:
                df.at[idx, 'Max'] = f'<strong>{max_val:.2f} {unit}</strong>'
            else:
                thresholds = self._get_cpu_thresholds()
                df.at[idx, 'Max'] = self.highlight_node_usage_values(
                    max_val, 'cpu', unit, is_top=is_top
                )
            
            # Bold mode name for TOTAL rows
            if is_total:
                df.at[idx, 'Mode'] = f'<strong>{row["Mode"]}</strong>'
        
        # Remove Unit column
        df = df.drop('Unit', axis=1)
        return df
    
    def _format_cgroup_cpu_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format cgroup CPU usage table with highlighting"""
        # Identify top values per cgroup
        top_indices = {}
        for cgroup in df['Cgroup'].unique():
            cgroup_df = df[df['Cgroup'] == cgroup]
            if not cgroup_df.empty and cgroup != 'TOTAL':
                max_idx = cgroup_df['Max'].idxmax()
                top_indices[max_idx] = True
        
        # Format values
        for idx, row in df.iterrows():
            unit = row['Unit']
            is_top = idx in top_indices
            is_total = row['Cgroup'] == 'TOTAL'
            
            # Format Avg
            avg_val = row['Avg']
            if is_total:
                df.at[idx, 'Avg'] = f'<strong>{avg_val:.2f} {unit}</strong>'
            else:
                df.at[idx, 'Avg'] = self.highlight_node_usage_values(
                    avg_val, 'cpu', unit, is_top=False
                )
            
            # Format Max
            max_val = row['Max']
            if is_total:
                df.at[idx, 'Max'] = f'<strong>{max_val:.2f} {unit}</strong>'
            else:
                df.at[idx, 'Max'] = self.highlight_node_usage_values(
                    max_val, 'cpu', unit, is_top=is_top
                )
            
            # Bold cgroup name for TOTAL rows
            if is_total:
                df.at[idx, 'Cgroup'] = f'<strong>{row["Cgroup"]}</strong>'
        
        # Remove Unit column
        df = df.drop('Unit', axis=1)
        return df
    
    def _format_cgroup_rss_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format cgroup RSS usage table with highlighting"""
        # Identify top values per cgroup
        top_indices = {}
        for cgroup in df['Cgroup'].unique():
            cgroup_df = df[df['Cgroup'] == cgroup]
            if not cgroup_df.empty and cgroup != 'TOTAL':
                max_idx = cgroup_df['Max'].idxmax()
                top_indices[max_idx] = True
        
        # Format values
        for idx, row in df.iterrows():
            unit = row['Unit']
            is_top = idx in top_indices
            is_total = row['Cgroup'] == 'TOTAL'
            
            # Format Avg
            avg_val = row['Avg']
            if is_total:
                df.at[idx, 'Avg'] = f'<strong>{avg_val:.2f} {unit}</strong>'
            else:
                df.at[idx, 'Avg'] = self.highlight_node_usage_values(
                    avg_val, 'memory', unit, is_top=False
                )
            
            # Format Max
            max_val = row['Max']
            if is_total:
                df.at[idx, 'Max'] = f'<strong>{max_val:.2f} {unit}</strong>'
            else:
                df.at[idx, 'Max'] = self.highlight_node_usage_values(
                    max_val, 'memory', unit, is_top=is_top
                )
            
            # Bold cgroup name for TOTAL rows
            if is_total:
                df.at[idx, 'Cgroup'] = f'<strong>{row["Cgroup"]}</strong>'
        
        # Remove Unit column
        df = df.drop('Unit', axis=1)
        return df
    
    def _get_cpu_thresholds(self) -> Dict[str, float]:
        """Get CPU usage thresholds"""
        return {'warning': 70.0, 'critical': 85.0}
    
    def _get_memory_thresholds(self) -> Dict[str, float]:
        """Get memory usage thresholds"""
        return {'warning': 70.0, 'critical': 85.0}
    
    def summarize_node_usage(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary for node usage metrics"""
        try:
            summary_parts = []
            
            # Basic info
            duration = structured_data.get('duration', 'unknown')
            total_nodes = structured_data.get('total_nodes', 0)
            node_group = structured_data.get('node_group', 'unknown')
            
            summary_parts.append(
                f"<strong>Node Usage Analysis</strong>: {node_group.title()} nodes over {duration}"
            )
            summary_parts.append(f"Total nodes: {total_nodes}")
            
            metrics = structured_data.get('metrics', {})
            
            # CPU summary
            if 'cpu_usage' in metrics:
                cpu_nodes = metrics['cpu_usage'].get('nodes', [])
                total_rows = [n for n in cpu_nodes if n.get('mode') == 'TOTAL']
                if total_rows:
                    max_cpu = max(total_rows, key=lambda x: x['max'])
                    summary_parts.append(
                        f"Max CPU usage: {max_cpu['max']:.1f}% on {max_cpu['node']}"
                    )
            
            # Memory summary
            if 'memory_used' in metrics:
                mem_nodes = metrics['memory_used'].get('nodes', [])
                if mem_nodes:
                    max_mem = max(mem_nodes, key=lambda x: x['max'])
                    summary_parts.append(
                        f"Max memory used: {max_mem['max']:.1f} GB on {max_mem['node']}"
                    )
            
            return " • ".join(summary_parts)
            
        except Exception as e:
            self.logger.error(f"Error generating summary: {e}", exc_info=True)
            return "Node usage metrics collected"