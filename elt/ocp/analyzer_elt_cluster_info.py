"""
Extract, Load, Transform module for ETCD Analyzer Cluster Information
Handles cluster info data from tools/ocp_cluster_info.py
ONLY contains cluster_info specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class clusterInfoELT(utilityELT):
    """Extract, Load, Transform class for cluster information data"""
    
    def __init__(self):
        super().__init__()
    
    def extract_cluster_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cluster information from ocp_cluster_info.py output"""
        
        # Handle nested result structure
        cluster_data = data
        if 'result' in data and 'data' in data['result']:
            cluster_data = data['result']['data']
        elif 'data' in data and isinstance(data['data'], dict):
            cluster_data = data['data']
        
        structured = {
            'cluster_overview': [],
            'resource_summary': [],
            'resource_metrics': [],
            'node_distribution': [],
            'node_capacity_metrics': [],
            'master_nodes_detail': [],
            'worker_nodes_detail': [],
            'infra_nodes_detail': [],
            'cluster_health_status': [],
            'mcp_status_detail': [],
            'unavailable_operators_detail': []
        }
        
        # Extract cluster overview
        self._extract_cluster_overview(cluster_data, structured)
        
        # Extract resource metrics
        self._extract_resource_metrics(cluster_data, structured)
        
        # Extract node information
        self._extract_node_information(cluster_data, structured)
        
        # Extract health status
        self._extract_health_status(cluster_data, structured)
        
        return structured
    
    def _extract_cluster_overview(self, cluster_data: Dict[str, Any], 
                                  structured: Dict[str, Any]):
        """Extract basic cluster overview information"""
        structured['cluster_overview'] = [
            {'Property': 'Cluster Name', 
             'Value': cluster_data.get('cluster_name', 'Unknown')},
            {'Property': 'Version', 
             'Value': cluster_data.get('cluster_version', 'Unknown')},
            {'Property': 'Platform', 
             'Value': cluster_data.get('platform', 'Unknown')},
            {'Property': 'Total Nodes', 
             'Value': cluster_data.get('total_nodes', 0)},
            {'Property': 'API Server', 
             'Value': self.truncate_url(cluster_data.get('api_server_url', 'Unknown'))},
            {'Property': 'Collection Time', 
             'Value': self.format_timestamp(cluster_data.get('collection_timestamp', 'Unknown'))}
        ]
    
    def _extract_resource_metrics(self, cluster_data: Dict[str, Any], 
                                  structured: Dict[str, Any]):
        """Extract and process resource metrics"""
        resource_items = [
            ('namespaces_count', 'Namespaces'),
            ('pods_count', 'Pods'),
            ('services_count', 'Services'),
            ('secrets_count', 'Secrets'),
            ('configmaps_count', 'Config Maps'),
            ('networkpolicies_count', 'Network Policies'),
        ]
        
        # Collect resource counts
        resource_counts = []
        for field, label in resource_items:
            count_value = cluster_data.get(field, 0)
            try:
                count_int = int(count_value) if count_value is not None else 0
            except (ValueError, TypeError):
                count_int = 0
            resource_counts.append((field, label, count_int))
        
        resource_counts.sort(key=lambda x: x[2], reverse=True)
        
        # Create resource metrics table with highlighting
        for i, (field, label, count) in enumerate(resource_counts):
            category = self.categorize_resource_type(label)
            is_top = i == 0 and count > 0  # Top 1 resource
            
            # Thresholds
            thresholds = {'critical': 1000, 'warning': 500} if 'pod' in label.lower() else {'critical': 200, 'warning': 100}
            formatted_count = self.highlight_critical_values(count, thresholds, is_top=is_top)
            
            structured['resource_metrics'].append({
                'Resource Type': label,
                'Count': formatted_count,
                'Category': category,
                'Status': self.create_status_badge('success' if count >= 0 else 'danger', 
                                                   'Available' if count >= 0 else 'Error')
            })
        
        # Resource summary
        total_resources = sum(count for _, _, count in resource_counts)
        network_resources = sum(count for field, _, count in resource_counts 
                              if 'network' in field.lower() or 'egress' in field.lower())
        
        pods_count = self._safe_int_get(cluster_data, 'pods_count', 0)
        services_count = self._safe_int_get(cluster_data, 'services_count', 0)
        secrets_count = self._safe_int_get(cluster_data, 'secrets_count', 0)
        configmaps_count = self._safe_int_get(cluster_data, 'configmaps_count', 0)
        
        structured['resource_summary'] = [
            {'Metric': 'Total Resources', 'Value': total_resources},
            {'Metric': 'Network-Related Resources', 'Value': network_resources},
            {'Metric': 'Core Resources (Pods+Services)', 'Value': pods_count + services_count},
            {'Metric': 'Config Resources (Secrets+ConfigMaps)', 'Value': secrets_count + configmaps_count}
        ]
    
    def _extract_node_information(self, cluster_data: Dict[str, Any], 
                                  structured: Dict[str, Any]):
        """Extract node distribution and capacity information"""
        node_types = [
            ('master_nodes', 'Master'),
            ('worker_nodes', 'Worker'),
            ('infra_nodes', 'Infra')
        ]
        
        all_totals = []
        for field, role in node_types:
            nodes = cluster_data.get(field, [])
            if nodes and isinstance(nodes, list):
                totals = self.calculate_totals_from_nodes(nodes)
                all_totals.append((role, totals))
                
                structured['node_distribution'].append({
                    'Node Type': role,
                    'Count': totals['count'],
                    'Ready': totals['ready_count'],
                    'Schedulable': totals['schedulable_count'],
                    'Total CPU (cores)': totals['total_cpu'],
                    'Total Memory (GB)': f"{totals['total_memory_gb']:.0f}",
                    'Health Ratio': f"{totals['ready_count']}/{totals['count']}",
                    'Avg CPU per Node': f"{totals['total_cpu']/totals['count']:.0f}" if totals['count'] > 0 else "0"
                })
            else:
                all_totals.append((role, {'total_cpu': 0, 'total_memory_gb': 0, 'count': 0, 
                                          'ready_count': 0, 'schedulable_count': 0}))
                structured['node_distribution'].append({
                    'Node Type': role,
                    'Count': 0,
                    'Ready': 0,
                    'Schedulable': 0,
                    'Total CPU (cores)': 0,
                    'Total Memory (GB)': '0',
                    'Health Ratio': '0/0',
                    'Avg CPU per Node': '0'
                })
        
        # Node capacity metrics with top highlighting
        if all_totals:
            max_cpu_total = max((totals['total_cpu'] for _, totals in all_totals), default=0)
            max_memory_total = max((totals['total_memory_gb'] for _, totals in all_totals), default=0)
            max_cpu_type = next((role for role, totals in all_totals if totals['total_cpu'] == max_cpu_total), None)
            max_memory_type = next((role for role, totals in all_totals if totals['total_memory_gb'] == max_memory_total), None)
            
            for role, totals in all_totals:
                if totals['count'] > 0:
                    is_top_cpu = role == max_cpu_type and totals['total_cpu'] > 0
                    is_top_memory = role == max_memory_type and totals['total_memory_gb'] > 0
                    
                    cpu_display = self.highlight_critical_values(
                        totals['total_cpu'], 
                        {'critical': 100, 'warning': 50}, 
                        ' cores',
                        is_top=is_top_cpu
                    )
                    memory_display = self.highlight_critical_values(
                        int(totals['total_memory_gb']), 
                        {'critical': 1000, 'warning': 500}, 
                        ' GB',
                        is_top=is_top_memory
                    )
                    
                    structured['node_capacity_metrics'].append({
                        'Node Type': role,
                        'Node Count': totals['count'],
                        'Total CPU': cpu_display,
                        'Total Memory': memory_display,
                        'Avg CPU/Node': f"{totals['total_cpu']/totals['count']:.1f} cores",
                        'Avg Memory/Node': f"{totals['total_memory_gb']/totals['count']:.1f} GB"
                    })
        
        # Extract detailed node information
        self._extract_node_details(cluster_data, 'master_nodes', structured, 'master_nodes_detail')
        self._extract_node_details(cluster_data, 'worker_nodes', structured, 'worker_nodes_detail')
        if cluster_data.get('infra_nodes'):
            self._extract_node_details(cluster_data, 'infra_nodes', structured, 'infra_nodes_detail')
    
    def _extract_health_status(self, cluster_data: Dict[str, Any], 
                              structured: Dict[str, Any]):
        """Extract cluster health status information"""
        unavailable_ops = cluster_data.get('unavailable_cluster_operators', [])
        mcp_status = cluster_data.get('mcp_status', {})
        total_nodes = self._safe_int_get(cluster_data, 'total_nodes', 0)
        
        node_types = [('master_nodes', 'Master'), ('worker_nodes', 'Worker'), ('infra_nodes', 'Infra')]
        
        health_items = [
            ('Unavailable Operators', len(unavailable_ops)),
            ('Total MCP Pools', len(mcp_status)),
            ('MCP Updated Pools', sum(1 for status in mcp_status.values() if status == 'Updated')),
            ('MCP Degraded Pools', sum(1 for status in mcp_status.values() if status == 'Degraded')),
            ('Overall Cluster Health', 'Healthy' if len(unavailable_ops) == 0 and 
             all(status in ['Updated'] for status in mcp_status.values()) else 'Issues Detected'),
            ('Node Health Score', f"{sum(1 for field, _ in node_types for node in cluster_data.get(field, []) if isinstance(node, dict) and 'Ready' in str(node.get('ready_status', '')))}/{total_nodes}")
        ]
        
        for metric, value in health_items:
            # Highlight critical health issues
            if metric == 'Unavailable Operators' and isinstance(value, int) and value > 0:
                display_value = self.create_status_badge('danger', str(value))
            elif metric == 'MCP Degraded Pools' and isinstance(value, int) and value > 0:
                display_value = self.create_status_badge('danger', str(value))
            elif metric == 'Overall Cluster Health':
                display_value = self.create_status_badge('success' if value == 'Healthy' else 'warning', value)
            else:
                display_value = str(value)
            
            structured['cluster_health_status'].append({
                'Health Metric': metric,
                'Value': display_value
            })
        
        # MCP Status Detail
        for pool_name, status in mcp_status.items():
            status_badge = self.create_status_badge(
                'success' if status == 'Updated' else 'danger' if status == 'Degraded' else 'warning',
                status
            )
            structured['mcp_status_detail'].append({
                'Machine Config Pool': pool_name.title(),
                'Status': status_badge
            })
        
        # Unavailable Operators Detail
        if unavailable_ops:
            for i, op in enumerate(unavailable_ops, 1):
                structured['unavailable_operators_detail'].append({
                    'Operator #': i,
                    'Operator Name': op,
                    'Status': self.create_status_badge('danger', 'Unavailable')
                })
        else:
            structured['unavailable_operators_detail'].append({
                'Status': self.create_status_badge('success', 'All operators available'),
                'Message': 'No unavailable operators detected'
            })
    
    def _extract_node_details(self, cluster_data: Dict[str, Any], node_field: str, 
                             structured: Dict[str, Any], output_key: str):
        """Extract detailed node information with formatted metrics"""
        nodes = cluster_data.get(node_field, [])
        
        if not nodes or not isinstance(nodes, list):
            return
        
        # Find top CPU/memory nodes for highlighting
        cpu_values = [(i, self.parse_cpu_capacity(node.get('cpu_capacity', '0'))) 
                     for i, node in enumerate(nodes) if isinstance(node, dict)]
        memory_values = [(i, self.parse_memory_capacity(node.get('memory_capacity', '0Ki'))) 
                        for i, node in enumerate(nodes) if isinstance(node, dict)]
        
        top_cpu_idx = max(cpu_values, key=lambda x: x[1])[0] if cpu_values and max(cpu_values, key=lambda x: x[1])[1] > 0 else -1
        top_memory_idx = max(memory_values, key=lambda x: x[1])[0] if memory_values and max(memory_values, key=lambda x: x[1])[1] > 0 else -1
        
        for i, node in enumerate(nodes):
            if not isinstance(node, dict):
                continue
            
            cpu_cores = self.parse_cpu_capacity(node.get('cpu_capacity', '0'))
            memory_gb = self.parse_memory_capacity(node.get('memory_capacity', '0Ki'))
            
            # Highlight top resources
            cpu_display = self.highlight_critical_values(
                cpu_cores, 
                {'critical': 64, 'warning': 32}, 
                '',
                is_top=(i == top_cpu_idx)
            )
            memory_display = self.highlight_critical_values(
                int(memory_gb), 
                {'critical': 256, 'warning': 128}, 
                ' GB',
                is_top=(i == top_memory_idx)
            )
            
            # Status badges
            ready_status = str(node.get('ready_status', 'Unknown'))
            status_badge = self.create_status_badge(
                'success' if 'Ready' in ready_status else 'danger',
                ready_status
            )
            
            schedulable = node.get('schedulable', False)
            schedulable_badge = self.create_status_badge(
                'success' if schedulable else 'warning', 
                'Yes' if schedulable else 'No'
            )
            
            structured[output_key].append({
                'Name': self.truncate_node_name(node.get('name', 'unknown')),
                'CPU Cores': cpu_display,
                'Memory': memory_display,
                'Architecture': node.get('architecture', 'Unknown'),
                'Kernel Version': self.truncate_kernel_version(node.get('kernel_version', 'Unknown')),
                'Kubelet Version': node.get('kubelet_version', 'Unknown').replace('v', ''),
                'Container Runtime': self.truncate_runtime(node.get('container_runtime', 'Unknown')),
                'Status': status_badge,
                'Schedulable': schedulable_badge,
                'Creation Time': node.get('creation_timestamp', 'Unknown')[:10] if node.get('creation_timestamp') else 'Unknown'
            })
    
    def summarize_cluster_info(self, data: Dict[str, Any]) -> str:
        """Generate cluster info summary as HTML list"""
        try:
            summary_items: List[str] = []
            
            # Basic cluster info
            cluster_overview = data.get('cluster_overview', [])
            cluster_name = next((item['Value'] for item in cluster_overview 
                               if item.get('Property') == 'Cluster Name'), 'Unknown')
            version = next((item['Value'] for item in cluster_overview 
                          if item.get('Property') == 'Version'), 'Unknown')
            
            if cluster_name != 'Unknown':
                summary_items.append(f"<li>Cluster: {cluster_name}</li>")
            if version != 'Unknown':
                summary_items.append(f"<li>Version: {version}</li>")
            
            # Node summary
            node_distribution = data.get('node_distribution', [])
            if node_distribution:
                total_nodes = sum(int(item.get('Count', 0)) for item in node_distribution)
                total_ready = sum(int(item.get('Ready', 0)) for item in node_distribution)
                total_cpu = sum(int(item.get('Total CPU (cores)', 0)) for item in node_distribution)
                total_memory = sum(float(str(item.get('Total Memory (GB)', '0')).replace(' GB', '').replace('GB', '')) 
                                 for item in node_distribution)
                
                if total_nodes > 0:
                    summary_items.append(f"<li>Nodes: {total_ready}/{total_nodes} ready</li>")
                    summary_items.append(f"<li>Total Resources: {total_cpu} CPU cores, {total_memory:.0f}GB RAM</li>")
            
            # Health warnings
            cluster_health = data.get('cluster_health_status', [])
            for item in cluster_health:
                metric = item.get('Health Metric', '')
                if metric == 'Unavailable Operators':
                    value = item.get('Value', '0')
                    if '>' in str(value):
                        import re
                        match = re.search(r'>(\d+)<', str(value))
                        if match and int(match.group(1)) > 0:
                            summary_items.append(f"<li>⚠ {match.group(1)} operators unavailable</li>")
            
            return (
                "<div class=\"cluster-summary\">"
                "<h4>Cluster Information Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate cluster summary: {e}")
            return f"Cluster summary available with {len(data)} data sections"
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        dataframes = {}
        
        try:
            for key, value in structured_data.items():
                if isinstance(value, list) and value:
                    df = pd.DataFrame(value)
                    if not df.empty:
                        # Apply column limiting for most tables
                        if 'detail' not in key and key not in ['node_distribution', 'node_capacity_metrics', 'resource_metrics']:
                            df = self.limit_dataframe_columns(df, table_name=key)
                        
                        # Decode unicode in object columns
                        for col in df.columns:
                            if df[col].dtype == 'object':
                                df[col] = df[col].astype(str).apply(self.decode_unicode_escapes)
                        
                        dataframes[key] = df
        
        except Exception as e:
            logger.error(f"Failed to transform cluster info to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            for table_name, df in dataframes.items():
                if not df.empty:
                    html_tables[table_name] = self.create_html_table(df, table_name)
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for cluster info: {e}")
        
        return html_tables