"""
Extract, Load, Transform module for Node Usage Metrics
Processes node usage data from ovnk_benchmark_prometheus_nodes_usage.py
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)


class NodesUsageELT(EltUtility):
    """ELT processor for node usage metrics"""
    
    def __init__(self):
        super().__init__()
        self.role_display_order = ['controlplane', 'infra', 'worker', 'workload']
        self.max_columns = 5  # Limit to 5 columns
    
    def extract_node_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract node usage data from raw metrics"""
        try:
            structured = {
                'collection_info': self._extract_collection_info(data),
                'metrics': {}
            }
            
            # Extract each metric type
            if 'metrics' in data:
                metrics = data['metrics']
                
                if 'cpu_usage' in metrics:
                    structured['metrics']['cpu_usage'] = self._extract_cpu_usage(metrics['cpu_usage'])
                
                if 'cgroup_cpu' in metrics:
                    structured['metrics']['cgroup_cpu'] = self._extract_cgroup_cpu(metrics['cgroup_cpu'])
                
                if 'memory_used' in metrics:
                    structured['metrics']['memory_used'] = self._extract_memory_used(metrics['memory_used'])
                
                if 'cache_buffer' in metrics:
                    structured['metrics']['cache_buffer'] = self._extract_cache_buffer(metrics['cache_buffer'])
                
                if 'cgroup_rss' in metrics:
                    structured['metrics']['cgroup_rss'] = self._extract_cgroup_rss(metrics['cgroup_rss'])
            
            return structured
            
        except Exception as e:
            logger.error(f"Failed to extract node usage data: {e}")
            return {'error': str(e)}
    
    def _extract_collection_info(self, data: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract collection metadata"""
        info = []
        
        if 'collection_timestamp_utc' in data:
            info.append({
                'Property': 'Collection Time',
                'Value': self.format_timestamp(data['collection_timestamp_utc'])
            })
        
        if 'time_range' in data:
            tr = data['time_range']
            info.append({
                'Property': 'Time Range',
                'Value': f"{self.format_timestamp(tr.get('start', ''), 16)} to {self.format_timestamp(tr.get('end', ''), 16)}"
            })
            info.append({
                'Property': 'Step',
                'Value': tr.get('step', 'N/A')
            })
        
        if 'node_groups_summary' in data:
            total_nodes = sum(data['node_groups_summary'].values())
            info.append({
                'Property': 'Total Nodes',
                'Value': str(total_nodes)
            })
            for role, count in data['node_groups_summary'].items():
                info.append({
                    'Property': f'{role.title()} Nodes',
                    'Value': str(count)
                })
        
        return info
    
    def _extract_cpu_usage(self, cpu_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract CPU usage metrics by role"""
        extracted = {
            'metric': cpu_data.get('metric', 'node_cpu_usage'),
            'unit': cpu_data.get('unit', 'percent'),
            'groups': {}
        }
        
        if 'groups' in cpu_data:
            for role, group_data in cpu_data['groups'].items():
                if 'error' in group_data:
                    extracted['groups'][role] = {'error': group_data['error']}
                    continue
                
                role_metrics = {
                    'group_summary': {
                        'group_avg': group_data.get('group_avg', 0),
                        'group_max': group_data.get('group_max', 0),
                        'unit': group_data.get('unit', 'percent')
                    },
                    'nodes': []
                }
                
                # Extract node-level data
                if 'nodes' in group_data:
                    for node_name, node_data in group_data['nodes'].items():
                        node_entry = {
                            'node': node_name,
                            'total_avg': node_data.get('total', {}).get('avg', 0),
                            'total_max': node_data.get('total', {}).get('max', 0),
                            'modes': []
                        }
                        
                        # Extract CPU modes (include all modes, sorted by max desc)
                        if 'modes' in node_data:
                            modes_list = []
                            for mode, mode_data in node_data['modes'].items():
                                modes_list.append({
                                    'mode': mode,
                                    'avg': mode_data.get('avg', 0),
                                    'max': mode_data.get('max', 0)
                                })
                            # Sort by max
                            modes_list.sort(key=lambda x: x['max'], reverse=True)
                            node_entry['modes'] = modes_list
                        
                        role_metrics['nodes'].append(node_entry)
                
                # Sort nodes by total_max descending
                role_metrics['nodes'].sort(key=lambda x: x['total_max'], reverse=True)
                
                extracted['groups'][role] = role_metrics
        
        return extracted
    
    def _extract_cgroup_cpu(self, cgroup_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cgroup CPU usage metrics by role"""
        extracted = {
            'metric': cgroup_data.get('metric', 'cgroup_cpu_usage'),
            'unit': cgroup_data.get('unit', 'percent'),
            'groups': {}
        }
        
        if 'groups' in cgroup_data:
            for role, group_data in cgroup_data['groups'].items():
                if 'error' in group_data:
                    extracted['groups'][role] = {'error': group_data['error']}
                    continue
                
                role_metrics = {
                    'group_summary': {
                        'group_avg': group_data.get('group_avg', 0),
                        'group_max': group_data.get('group_max', 0),
                        'unit': group_data.get('unit', 'percent')
                    },
                    'nodes': []
                }
                
                if 'nodes' in group_data:
                    for node_name, node_data in group_data['nodes'].items():
                        node_entry = {
                            'node': node_name,
                            'total_avg': node_data.get('total', {}).get('avg', 0),
                            'total_max': node_data.get('total', {}).get('max', 0),
                            'cgroups': []
                        }
                        
                        if 'cgroups' in node_data:
                            cgroups_list = []
                            for cgroup_id, cgroup_vals in node_data['cgroups'].items():
                                # Shorten cgroup ID for display
                                display_id = self._format_cgroup_id(cgroup_id)
                                cgroups_list.append({
                                    'cgroup': display_id,
                                    'full_cgroup_id': cgroup_id,
                                    'avg': cgroup_vals.get('avg', 0),
                                    'max': cgroup_vals.get('max', 0)
                                })
                            # Sort by max
                            cgroups_list.sort(key=lambda x: x['max'], reverse=True)
                            node_entry['cgroups'] = cgroups_list
                        
                        role_metrics['nodes'].append(node_entry)
                
                role_metrics['nodes'].sort(key=lambda x: x['total_max'], reverse=True)
                
                extracted['groups'][role] = role_metrics
        
        return extracted
    
    def _extract_memory_used(self, mem_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract memory used metrics by role"""
        extracted = {
            'metric': mem_data.get('metric', 'node_memory_used'),
            'unit': mem_data.get('unit', 'bytes'),
            'groups': {}
        }
        
        if 'groups' in mem_data:
            for role, group_data in mem_data['groups'].items():
                if 'error' in group_data:
                    extracted['groups'][role] = {'error': group_data['error']}
                    continue
                
                role_metrics = {
                    'group_summary': {
                        'group_avg': group_data.get('group_avg', 0),
                        'group_max': group_data.get('group_max', 0),
                        'unit': group_data.get('unit', 'bytes')
                    },
                    'nodes': []
                }
                
                if 'nodes' in group_data:
                    for node_name, node_data in group_data['nodes'].items():
                        role_metrics['nodes'].append({
                            'node': node_name,
                            'avg': node_data.get('avg', 0),
                            'max': node_data.get('max', 0)
                        })
                
                role_metrics['nodes'].sort(key=lambda x: x['max'], reverse=True)
                
                extracted['groups'][role] = role_metrics
        
        return extracted
    
    def _extract_cache_buffer(self, cache_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cache/buffer metrics by role"""
        return self._extract_memory_used(cache_data)  # Same structure
    
    def _extract_cgroup_rss(self, rss_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cgroup RSS metrics by role"""
        extracted = {
            'metric': rss_data.get('metric', 'cgroup_rss_usage'),
            'unit': rss_data.get('unit', 'bytes'),
            'groups': {}
        }
        
        if 'groups' in rss_data:
            for role, group_data in rss_data['groups'].items():
                if 'error' in group_data:
                    extracted['groups'][role] = {'error': group_data['error']}
                    continue
                
                role_metrics = {
                    'group_summary': {
                        'group_avg': group_data.get('group_avg', 0),
                        'group_max': group_data.get('group_max', 0),
                        'unit': group_data.get('unit', 'bytes')
                    },
                    'nodes': []
                }
                
                if 'nodes' in group_data:
                    for node_name, node_data in group_data['nodes'].items():
                        node_entry = {
                            'node': node_name,
                            'total_avg': node_data.get('total', {}).get('avg', 0),
                            'total_max': node_data.get('total', {}).get('max', 0),
                            'cgroups': []
                        }
                        
                        if 'cgroups' in node_data:
                            cgroups_list = []
                            for cgroup_id, cgroup_vals in node_data['cgroups'].items():
                                display_id = self._format_cgroup_id(cgroup_id)
                                cgroups_list.append({
                                    'cgroup': display_id,
                                    'avg': cgroup_vals.get('avg', 0),
                                    'max': cgroup_vals.get('max', 0)
                                })
                            cgroups_list.sort(key=lambda x: x['max'], reverse=True)
                            node_entry['cgroups'] = cgroups_list
                        
                        role_metrics['nodes'].append(node_entry)
                
                role_metrics['nodes'].sort(key=lambda x: x['total_max'], reverse=True)
                
                extracted['groups'][role] = role_metrics
        
        return extracted
    
    def _format_cgroup_id(self, cgroup_id: str) -> str:
        """Format cgroup ID for display"""
        if cgroup_id.startswith('/system.slice/'):
            return cgroup_id.replace('/system.slice/', '')
        elif cgroup_id == '/kubepods.slice':
            return 'kubepods'
        elif cgroup_id == '/system.slice':
            return 'system'
        return cgroup_id
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data to DataFrames"""
        dataframes = {}
        
        try:
            # Collection info - limit to 2 columns
            if 'collection_info' in structured_data:
                df = pd.DataFrame(structured_data['collection_info'])
                if not df.empty:
                    df = self.limit_dataframe_columns(df, max_cols=2, table_name='collection_info')
                    dataframes['collection_info'] = df
            
            # Process each metric
            if 'metrics' in structured_data:
                metrics = structured_data['metrics']
                
                if 'cpu_usage' in metrics:
                    dfs = self._transform_cpu_usage(metrics['cpu_usage'])
                    dataframes.update(dfs)
                
                if 'cgroup_cpu' in metrics:
                    dfs = self._transform_cgroup_cpu(metrics['cgroup_cpu'])
                    dataframes.update(dfs)
                
                if 'memory_used' in metrics:
                    dfs = self._transform_memory_metric(metrics['memory_used'], 'memory_used')
                    dataframes.update(dfs)
                
                if 'cache_buffer' in metrics:
                    dfs = self._transform_memory_metric(metrics['cache_buffer'], 'cache_buffer')
                    dataframes.update(dfs)
                
                if 'cgroup_rss' in metrics:
                    dfs = self._transform_cgroup_rss(metrics['cgroup_rss'])
                    dataframes.update(dfs)
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform node usage data: {e}")
            return {}

    def _transform_cpu_usage(self, cpu_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform CPU usage to DataFrames by role"""
        dfs = {}
        
        if 'groups' not in cpu_data:
            return dfs
        
        for role in self.role_display_order:
            if role not in cpu_data['groups']:
                continue
            
            group_data = cpu_data['groups'][role]
            if 'error' in group_data or 'nodes' not in group_data:
                continue
            
            rows = []
            max_values_for_ranking = []
            
            for node in group_data['nodes']:
                node_short = self.truncate_node_name(node['node'], 25)  # Reduced from 30
                total_max = round(node['total_max'], 2)
                max_values_for_ranking.append((node['node'], total_max))
                
                # Add rows for all modes
                for mode_data in node['modes']:
                    rows.append({
                        'node': node_short,
                        'full_node': node['node'],
                        'mode': mode_data['mode'],
                        'avg': round(mode_data['avg'], 2),
                        'max': round(mode_data['max'], 2)
                    })
            
            if rows:
                df = pd.DataFrame(rows)
                
                # Create ranking based on TOTAL max values
                max_values_for_ranking.sort(key=lambda x: x[1], reverse=True)
                rank_map = {node: idx + 1 for idx, (node, _) in enumerate(max_values_for_ranking)}
                
                # Add rank column
                # Assign rank to all rows based on node total max
                df['rank'] = df.apply(lambda row: rank_map.get(row['full_node'], 999), axis=1)
                
                # Format display columns
                df['Rank'] = df['rank']
                df['Node'] = df['node']
                df['Mode'] = df['mode']
                df['Avg (%)'] = df['avg']
                df['Max (%)'] = df['max']
                
                # Select final columns - ensure exactly 5 columns
                df = df[['Rank', 'Node', 'Mode', 'Avg (%)', 'Max (%)']]
                
                dfs[f'cpu_usage_{role}'] = df
        
        return dfs

    def _transform_cgroup_cpu(self, cgroup_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform cgroup CPU to DataFrames by role"""
        dfs = {}
        
        if 'groups' not in cgroup_data:
            return dfs
        
        for role in self.role_display_order:
            if role not in cgroup_data['groups']:
                continue
            
            group_data = cgroup_data['groups'][role]
            if 'error' in group_data or 'nodes' not in group_data:
                continue
            
            rows = []
            max_values_for_ranking = []
            
            for node in group_data['nodes']:
                node_short = self.truncate_node_name(node['node'], 25)  # Reduced from 30
                total_max = round(node['total_max'], 2)
                max_values_for_ranking.append((node['node'], total_max))
                
                # Add rows for all cgroups
                for cg in node['cgroups']:
                    rows.append({
                        'node': node_short,
                        'full_node': node['node'],
                        'cgroup': cg['cgroup'],
                        'avg': round(cg['avg'], 2),
                        'max': round(cg['max'], 2)
                    })
            
            if rows:
                df = pd.DataFrame(rows)
                
                # Create ranking
                max_values_for_ranking.sort(key=lambda x: x[1], reverse=True)
                rank_map = {node: idx + 1 for idx, (node, _) in enumerate(max_values_for_ranking)}
                
                # Assign rank to all rows based on node total max
                df['rank'] = df.apply(lambda row: rank_map.get(row['full_node'], 999), axis=1)
                
                df['Rank'] = df['rank']
                df['Node'] = df['node']
                df['Cgroup'] = df['cgroup']
                df['Avg (%)'] = df['avg']
                df['Max (%)'] = df['max']
                
                # Select final columns - ensure exactly 5 columns
                df = df[['Rank', 'Node', 'Cgroup', 'Avg (%)', 'Max (%)']]
                
                dfs[f'cgroup_cpu_{role}'] = df
        
        return dfs

    def _transform_memory_metric(self, mem_data: Dict[str, Any], metric_name: str) -> Dict[str, pd.DataFrame]:
        """Transform memory metrics to DataFrames by role"""
        dfs = {}
        
        if 'groups' not in mem_data:
            return dfs
        
        for role in self.role_display_order:
            if role not in mem_data['groups']:
                continue
            
            group_data = mem_data['groups'][role]
            if 'error' in group_data or 'nodes' not in group_data:
                continue
            
            rows = []
            for node in group_data['nodes']:
                rows.append({
                    'node': self.truncate_node_name(node['node'], 25),  # Reduced from 30
                    'full_node': node['node'],
                    'avg': round(node['avg'] / (1024**3), 2),
                    'max': round(node['max'] / (1024**3), 2)
                })
            
            if rows:
                df = pd.DataFrame(rows)
                df = df.sort_values('max', ascending=False)
                df['rank'] = range(1, len(df) + 1)
                
                df['Rank'] = df['rank']
                df['Node'] = df['node']
                df['Avg (GB)'] = df['avg']
                df['Max (GB)'] = df['max']
                
                # Select final columns - ensure exactly 4 columns (memory doesn't need 5)
                df = df[['Rank', 'Node', 'Avg (GB)', 'Max (GB)']]
                
                dfs[f'{metric_name}_{role}'] = df
        
        return dfs

    def _transform_cgroup_rss(self, rss_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform cgroup RSS to DataFrames by role"""
        dfs = {}
        
        if 'groups' not in rss_data:
            return dfs
        
        for role in self.role_display_order:
            if role not in rss_data['groups']:
                continue
            
            group_data = rss_data['groups'][role]
            if 'error' in group_data or 'nodes' not in group_data:
                continue
            
            rows = []
            max_values_for_ranking = []
            
            for node in group_data['nodes']:
                node_short = self.truncate_node_name(node['node'], 25)  # Reduced from 30
                total_max = round(node['total_max'] / (1024**3), 2)
                max_values_for_ranking.append((node['node'], total_max))
                
                # Add rows for all cgroups
                for cg in node['cgroups']:
                    rows.append({
                        'node': node_short,
                        'full_node': node['node'],
                        'cgroup': cg['cgroup'],
                        'avg': round(cg['avg'] / (1024**3), 2),
                        'max': round(cg['max'] / (1024**3), 2)
                    })
            
            if rows:
                df = pd.DataFrame(rows)
                
                max_values_for_ranking.sort(key=lambda x: x[1], reverse=True)
                rank_map = {node: idx + 1 for idx, (node, _) in enumerate(max_values_for_ranking)}
                
                # Assign rank to all rows based on node total max
                df['rank'] = df.apply(lambda row: rank_map.get(row['full_node'], 999), axis=1)
                
                df['Rank'] = df['rank']
                df['Node'] = df['node']
                df['Cgroup'] = df['cgroup']
                df['Avg (GB)'] = df['avg']
                df['Max (GB)'] = df['max']
                
                # Select final columns - ensure exactly 5 columns
                df = df[['Rank', 'Node', 'Cgroup', 'Avg (GB)', 'Max (GB)']]
                
                dfs[f'cgroup_rss_{role}'] = df
        
        return dfs 

        # --- Consolidated, role-grouped flat tables ---
        try:
            metrics = structured_data.get('metrics', {})
            for role in self.role_display_order:
                rows: List[Dict[str, Any]] = []

                # CPU usage (modes and total)
                cpu = metrics.get('cpu_usage', {})
                cpu_groups = cpu.get('groups', {}).get(role, {})
                if 'nodes' in cpu_groups:
                    for node_name, node_data in cpu_groups['nodes'].items():
                        # Modes
                        for mode, mode_vals in (node_data.get('modes') or {}).items():
                            rows.append({
                                'Metric': 'node_cpu_usage',
                                'Node': node_name,
                                'Device/Interface': mode,
                                'Avg': round(float(mode_vals.get('avg', 0)), 2),
                                'Max': round(float(mode_vals.get('max', 0)), 2),
                                'Unit': cpu.get('unit', 'percent')
                            })
                        # Total
                        if 'total' in node_data:
                            rows.append({
                                'Metric': 'node_cpu_usage',
                                'Node': node_name,
                                'Device/Interface': 'TOTAL',
                                'Avg': round(float(node_data['total'].get('avg', 0)), 2),
                                'Max': round(float(node_data['total'].get('max', 0)), 2),
                                'Unit': cpu.get('unit', 'percent')
                            })

                # Cgroup CPU
                cgroup_cpu = metrics.get('cgroup_cpu', {})
                cg_groups = cgroup_cpu.get('groups', {}).get(role, {})
                if 'nodes' in cg_groups:
                    for node_name, node_data in cg_groups['nodes'].items():
                        for cgroup_id, vals in (node_data.get('cgroups') or {}).items():
                            rows.append({
                                'Metric': 'cgroup_cpu_usage',
                                'Node': node_name,
                                'Device/Interface': self._format_cgroup_id(cgroup_id),
                                'Avg': round(float(vals.get('avg', 0)), 2),
                                'Max': round(float(vals.get('max', 0)), 2),
                                'Unit': cgroup_cpu.get('unit', 'percent')
                            })

                # Memory used
                mem_used = metrics.get('memory_used', {})
                mem_groups = mem_used.get('groups', {}).get(role, {})
                if 'nodes' in mem_groups:
                    for node_name, node_vals in mem_groups['nodes'].items():
                        rows.append({
                            'Metric': 'node_memory_used',
                            'Node': node_name,
                            'Device/Interface': '',
                            'Avg': float(node_vals.get('avg', 0)),
                            'Max': float(node_vals.get('max', 0)),
                            'Unit': mem_used.get('unit', 'bytes')
                        })

                # Cache/Buffer
                cache_buf = metrics.get('cache_buffer', {})
                cb_groups = cache_buf.get('groups', {}).get(role, {})
                if 'nodes' in cb_groups:
                    for node_name, node_vals in cb_groups['nodes'].items():
                        rows.append({
                            'Metric': 'node_memory_cache_buffer',
                            'Node': node_name,
                            'Device/Interface': '',
                            'Avg': float(node_vals.get('avg', 0)),
                            'Max': float(node_vals.get('max', 0)),
                            'Unit': cache_buf.get('unit', 'bytes')
                        })

                # Cgroup RSS
                cgroup_rss = metrics.get('cgroup_rss', {})
                rss_groups = cgroup_rss.get('groups', {}).get(role, {})
                if 'nodes' in rss_groups:
                    for node_name, node_data in rss_groups['nodes'].items():
                        for cgroup_id, vals in (node_data.get('cgroups') or {}).items():
                            rows.append({
                                'Metric': 'cgroup_rss_usage',
                                'Node': node_name,
                                'Device/Interface': self._format_cgroup_id(cgroup_id),
                                'Avg': float(vals.get('avg', 0)),
                                'Max': float(vals.get('max', 0)),
                                'Unit': cgroup_rss.get('unit', 'bytes')
                            })

                if rows:
                    # Convert to DataFrame and format units
                    df_flat = pd.DataFrame(rows)
                    # Human-friendly unit formatting for bytes
                    def _fmt_value(val: float, unit: str) -> str:
                        try:
                            if unit in ['bytes', 'byte', 'B']:
                                v = float(val)
                                for suffix in ['B', 'KB', 'MB', 'GB', 'TB']:
                                    if v < 1024.0 or suffix == 'TB':
                                        return f"{v:.2f} {suffix}"
                                    v /= 1024.0
                                return f"{v:.2f} B"
                            if unit in ['percent', '%']:
                                return f"{float(val):.2f} %"
                            return f"{val} {unit}"
                        except Exception:
                            return str(val)

                    df_flat['Avg'] = df_flat.apply(lambda r: _fmt_value(r['Avg'], r['Unit']), axis=1)
                    df_flat['Max'] = df_flat.apply(lambda r: _fmt_value(r['Max'], r['Unit']), axis=1)

                    # Order columns
                    df_flat = df_flat[['Metric', 'Node', 'Device/Interface', 'Avg', 'Max', 'Unit']]
                    # Sort by Metric then Node
                    df_flat = df_flat.sort_values(['Metric', 'Node']).reset_index(drop=True)
                    dataframes[f'node_usage_flat_{role}'] = df_flat
        except Exception:
            pass

    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables with highlighting"""
        html_tables = {}
        
        for table_name, df in dataframes.items():
            if df.empty:
                continue
            
            # Create a copy for HTML generation
            df_html = df.copy()
            
            # Apply highlighting based on table type
            if 'cpu_usage' in table_name or 'cgroup_cpu' in table_name:
                df_html = self._apply_cpu_highlighting(df_html)
            elif 'memory' in table_name or 'cache_buffer' in table_name or 'rss' in table_name:
                df_html = self._apply_memory_highlighting(df_html)
            
            # Use base class method to create HTML table
            html_tables[table_name] = self.create_html_table(df_html, table_name)
        
        return html_tables
    
    def _apply_cpu_highlighting(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply highlighting to CPU usage tables"""
        df = df.copy()
        
        # Highlight Max (%) column
        if 'Max (%)' in df.columns and 'Rank' in df.columns:
            df['Max (%)'] = df.apply(
                lambda row: self._format_cpu_cell(row['Max (%)'], row['Rank']),
                axis=1
            )
        
        # Highlight Avg (%) column (without rank consideration)
        if 'Avg (%)' in df.columns:
            df['Avg (%)'] = df['Avg (%)'].apply(
                lambda x: self._format_cpu_cell(x, '')
            )
        
        return df
    
    def _apply_memory_highlighting(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply highlighting to memory usage tables"""
        df = df.copy()
        
        # Highlight Max column
        if 'Max (GB)' in df.columns and 'Rank' in df.columns:
            df['Max (GB)'] = df.apply(
                lambda row: self._format_memory_cell(row['Max (GB)'], row['Rank']),
                axis=1
            )
        
        # Highlight Avg column
        if 'Avg (GB)' in df.columns:
            df['Avg (GB)'] = df['Avg (GB)'].apply(
                lambda x: self._format_memory_cell(x, '')
            )
        
        return df
    
    def _format_cpu_cell(self, value: float, rank: Any) -> str:
        """Format CPU cell with highlighting"""
        try:
            if rank == 1:
                # Top 1 - critical red badge
                return f'<span class="badge badge-danger font-weight-bold">{value}%</span>'
            elif value >= 80:
                # Critical threshold
                return f'<span class="text-danger font-weight-bold">{value}%</span>'
            elif value >= 60:
                # Warning threshold
                return f'<span class="text-warning">{value}%</span>'
            else:
                return f'{value}%'
        except:
            return str(value)
    
    def _format_memory_cell(self, value: float, rank: Any) -> str:
        """Format memory cell with highlighting"""
        try:
            if rank == 1:
                # Top 1 - critical red badge
                return f'<span class="badge badge-danger font-weight-bold">{value} GB</span>'
            elif value >= 100:
                # Critical threshold (100GB+)
                return f'<span class="text-danger font-weight-bold">{value} GB</span>'
            elif value >= 50:
                # Warning threshold (50GB+)
                return f'<span class="text-warning">{value} GB</span>'
            else:
                return f'{value} GB'
        except:
            return str(value)
    
    def summarize_node_usage(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary text for node usage"""
        summary_parts = []
        summary_parts.append("📊 Node Usage Metrics Summary:")
        
        if 'collection_info' in structured_data:
            info = structured_data['collection_info']
            for item in info[:3]:
                summary_parts.append(f"• {item['Property']}: {item['Value']}")
        
        if 'metrics' in structured_data:
            metrics = structured_data['metrics']
            
            # CPU Usage summary
            if 'cpu_usage' in metrics and 'groups' in metrics['cpu_usage']:
                summary_parts.append("\n🔹 CPU Usage by Role:")
                for role in self.role_display_order:
                    if role in metrics['cpu_usage']['groups']:
                        data = metrics['cpu_usage']['groups'][role]
                        if 'group_summary' in data:
                            gs = data['group_summary']
                            summary_parts.append(
                                f"  • {role.title()}: Avg {gs['group_avg']:.1f}%, Max {gs['group_max']:.1f}%"
                            )
            
            # Memory summary
            if 'memory_used' in metrics and 'groups' in metrics['memory_used']:
                summary_parts.append("\n🔹 Memory Usage by Role:")
                for role in self.role_display_order:
                    if role in metrics['memory_used']['groups']:
                        data = metrics['memory_used']['groups'][role]
                        if 'group_summary' in data:
                            gs = data['group_summary']
                            avg_gb = gs['group_avg'] / (1024**3)
                            max_gb = gs['group_max'] / (1024**3)
                            summary_parts.append(
                                f"  • {role.title()}: Avg {avg_gb:.1f} GB, Max {max_gb:.1f} GB"
                            )
        
        return "\n".join(summary_parts)

       