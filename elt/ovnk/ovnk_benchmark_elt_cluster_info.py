"""
Extract, Load, Transform module for OpenShift Cluster Information
Handles cluster info data from ocp_cluster_info.py
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)

class ClusterInfoELT(EltUtility):
    """Extract, Load, Transform class for cluster information data"""
    
    def __init__(self):
        super().__init__()
        
    def extract_cluster_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
            """Extract cluster information from ocp_cluster_info.py output"""
            structured = {
                'cluster_overview': [],
                'resource_summary': [],
                'all_resource_counts': [],
                'collection_metadata': [],
                'node_distribution': [],
                'master_nodes_detail': [],
                'worker_nodes_detail': [],
                'infra_nodes_detail': [],
                'cluster_health_status': [],
                'mcp_status_detail': [],
                'unavailable_operators_detail': []
            }
            
            # Cluster overview (2-column format for basic info)
            structured['cluster_overview'] = [
                {'Property': 'Cluster Name', 'Value': data.get('cluster_name', 'Unknown')},
                {'Property': 'Version', 'Value': data.get('cluster_version', 'Unknown')},
                {'Property': 'Platform', 'Value': data.get('platform', 'Unknown')},
                {'Property': 'Total Nodes', 'Value': data.get('total_nodes', 0)},
                {'Property': 'API Server', 'Value': self.truncate_url(data.get('api_server_url', 'Unknown'))},
                {'Property': 'Collection Time', 'Value': self.format_timestamp(data.get('collection_timestamp', 'Unknown'))}
            ]
            
            # Enhanced resource counts table - includes all available resource types from the JSON
            resource_items = [
                ('namespaces_count', 'Namespaces'),
                ('pods_count', 'Pods'), 
                ('services_count', 'Services'),
                ('secrets_count', 'Secrets'),
                ('configmaps_count', 'Config Maps'),
                ('networkpolicies_count', 'Network Policies'),
                ('adminnetworkpolicies_count', 'Admin Network Policies'),
                ('baselineadminnetworkpolicies_count', 'Baseline Admin Network Policies'),
                ('egressfirewalls_count', 'Egress Firewalls'),
                ('egressips_count', 'Egress IPs'),
                ('clusteruserdefinednetworks_count', 'Cluster User Defined Networks'),
                ('userdefinednetworks_count', 'User Defined Networks')
            ]
            
            # Create comprehensive resource table
            for field, label in resource_items:
                count = data.get(field, 0)
                category = self.categorize_resource_type(label)
                structured['all_resource_counts'].append({
                    'Resource Type': label,
                    'Count': count,
                    'Category': category,
                    'Status': 'Available' if count >= 0 else 'Error'
                })
            
            # Collection metadata table (if available)
            metadata = data.get('collection_metadata', {})
            if metadata:
                structured['collection_metadata'] = [
                    {'Metadata Field': 'Tool Name', 'Value': metadata.get('tool_name', 'Unknown')},
                    {'Metadata Field': 'Collection Duration (s)', 'Value': metadata.get('collection_duration_seconds', 'N/A')},
                    {'Metadata Field': 'Data Freshness', 'Value': self.format_timestamp(metadata.get('data_freshness', 'Unknown'))},
                    {'Metadata Field': 'Total Fields Collected', 'Value': metadata.get('total_fields_collected', 0)},
                    {'Metadata Field': 'Node Details Included', 'Value': 'Yes' if metadata.get('parameters_applied', {}).get('include_node_details', False) else 'No'},
                    {'Metadata Field': 'Resource Counts Included', 'Value': 'Yes' if metadata.get('parameters_applied', {}).get('include_resource_counts', False) else 'No'},
                    {'Metadata Field': 'Network Policies Included', 'Value': 'Yes' if metadata.get('parameters_applied', {}).get('include_network_policies', False) else 'No'},
                    {'Metadata Field': 'Operator Status Included', 'Value': 'Yes' if metadata.get('parameters_applied', {}).get('include_operator_status', False) else 'No'},
                    {'Metadata Field': 'MCP Status Included', 'Value': 'Yes' if metadata.get('parameters_applied', {}).get('include_mcp_status', False) else 'No'}
                ]
            
            # Enhanced resource summary with totals and categories 
            total_resources = sum(data.get(field, 0) for field, _ in resource_items)
            network_resources = sum(data.get(field, 0) for field, _ in resource_items if 'network' in field.lower() or 'egress' in field.lower() or 'udn' in field.lower())
            policy_resources = sum(data.get(field, 0) for field, _ in resource_items if 'policy' in field.lower() or 'policies' in field.lower())
            
            structured['resource_summary'] = [
                {'Metric': 'Total Resources', 'Value': total_resources},
                {'Metric': 'Network-Related Resources', 'Value': network_resources},
                {'Metric': 'Policy Resources', 'Value': policy_resources},
                {'Metric': 'Core Resources (Pods+Services)', 'Value': data.get('pods_count', 0) + data.get('services_count', 0)},
                {'Metric': 'Config Resources (Secrets+ConfigMaps)', 'Value': data.get('secrets_count', 0) + data.get('configmaps_count', 0)}
            ]
            
            # ENHANCED NODE DISTRIBUTION SUMMARY with Total CPU and Memory
            node_types = [
                ('master_nodes', 'Master'),
                ('worker_nodes', 'Worker'),
                ('infra_nodes', 'Infra')
            ]
            
            for field, role in node_types:
                nodes = data.get(field, [])
                if nodes:
                    # Calculate resource totals for this node type using utility functions
                    totals = self.calculate_totals_from_nodes(nodes)
                    
                    # Create the comprehensive node distribution entry with ALL fields
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
                    # Handle case where no nodes exist for this type
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
            
            # Master nodes detail (comprehensive node info) - REMOVED OS Image column
            master_nodes = data.get('master_nodes', [])
            for node in master_nodes:
                structured['master_nodes_detail'].append({
                    'Name': self.truncate_node_name(node.get('name', 'unknown')),
                    'CPU Cores': node.get('cpu_capacity', 'Unknown'),
                    'Memory': self.format_memory_display(node.get('memory_capacity', '0Ki')),
                    'Architecture': node.get('architecture', 'Unknown'),
                    'Kernel Version': self.truncate_kernel_version(node.get('kernel_version', 'Unknown')),
                    'Kubelet Version': node.get('kubelet_version', 'Unknown').replace('v', ''),
                    'Container Runtime': self.truncate_runtime(node.get('container_runtime', 'Unknown')),
                    'Status': node.get('ready_status', 'Unknown'),
                    'Schedulable': 'Yes' if node.get('schedulable', False) else 'No',
                    'Creation Time': node.get('creation_timestamp', 'Unknown')[:10] if node.get('creation_timestamp') else 'Unknown'
                })
            
            # Worker nodes detail (comprehensive node info) - REMOVED OS Image column
            worker_nodes = data.get('worker_nodes', [])
            for node in worker_nodes:
                structured['worker_nodes_detail'].append({
                    'Name': self.truncate_node_name(node.get('name', 'unknown')),
                    'CPU Cores': node.get('cpu_capacity', 'Unknown'),
                    'Memory': self.format_memory_display(node.get('memory_capacity', '0Ki')),
                    'Architecture': node.get('architecture', 'Unknown'),
                    'Kernel Version': self.truncate_kernel_version(node.get('kernel_version', 'Unknown')),
                    'Kubelet Version': node.get('kubelet_version', 'Unknown').replace('v', ''),
                    'Container Runtime': self.truncate_runtime(node.get('container_runtime', 'Unknown')),
                    'Status': node.get('ready_status', 'Unknown'),
                    'Schedulable': 'Yes' if node.get('schedulable', False) else 'No',
                    'Creation Time': node.get('creation_timestamp', 'Unknown')[:10] if node.get('creation_timestamp') else 'Unknown'
                })
            
            # Infra nodes detail (if any exist) - REMOVED OS Image column
            infra_nodes = data.get('infra_nodes', [])
            if infra_nodes:
                for node in infra_nodes:
                    structured['infra_nodes_detail'].append({
                        'Name': self.truncate_node_name(node.get('name', 'unknown')),
                        'CPU Cores': node.get('cpu_capacity', 'Unknown'),
                        'Memory': self.format_memory_display(node.get('memory_capacity', '0Ki')),
                        'Architecture': node.get('architecture', 'Unknown'),
                        'Kernel Version': self.truncate_kernel_version(node.get('kernel_version', 'Unknown')),
                        'Kubelet Version': node.get('kubelet_version', 'Unknown').replace('v', ''),
                        'Container Runtime': self.truncate_runtime(node.get('container_runtime', 'Unknown')),
                        'Status': node.get('ready_status', 'Unknown'),
                        'Schedulable': 'Yes' if node.get('schedulable', False) else 'No',
                        'Creation Time': node.get('creation_timestamp', 'Unknown')[:10] if node.get('creation_timestamp') else 'Unknown'
                    })
            
            # Cluster health status (enhanced with more metrics)
            unavailable_ops = data.get('unavailable_cluster_operators', [])
            mcp_status = data.get('mcp_status', {})
            
            health_items = [
                ('Unavailable Operators', len(unavailable_ops)),
                ('Total MCP Pools', len(mcp_status)),
                ('MCP Updated Pools', sum(1 for status in mcp_status.values() if status == 'Updated')),
                ('MCP Degraded Pools', sum(1 for status in mcp_status.values() if status == 'Degraded')),
                ('MCP Updating Pools', sum(1 for status in mcp_status.values() if status == 'Updating')),
                ('Overall Cluster Health', 'Healthy' if len(unavailable_ops) == 0 and all(status in ['Updated'] for status in mcp_status.values()) else 'Issues Detected'),
                ('Node Health Score', f"{sum(1 for field, _ in node_types for node in data.get(field, []) if 'Ready' in node.get('ready_status', ''))}/{data.get('total_nodes', 0)}")
            ]
            
            for metric, value in health_items:
                structured['cluster_health_status'].append({
                    'Health Metric': metric,
                    'Value': value
                })
            
            # MCP Status Detail
            for pool_name, status in mcp_status.items():
                structured['mcp_status_detail'].append({
                    'Machine Config Pool': pool_name.title(),
                    'Status': status
                })
            
            # Unavailable Operators Detail
            if unavailable_ops:
                for i, op in enumerate(unavailable_ops, 1):
                    structured['unavailable_operators_detail'].append({
                        'Operator #': i,
                        'Operator Name': op
                    })
            else:
                structured['unavailable_operators_detail'].append({
                    'Status': 'All operators are available',
                    'Message': 'No unavailable operators detected'
                })
            
            return structured

    def summarize_cluster_info(self, data: Dict[str, Any]) -> str:
        """Generate cluster info summary with total CPU/memory"""
        summary = ["Cluster Information Summary:"]
        
        # Basic cluster info
        cluster_name = data.get('cluster_overview', [{}])[0].get('Value', 'Unknown')
        if cluster_name != 'Unknown':
            summary.append(f"• Cluster: {cluster_name}")
        
        version_info = next((item for item in data.get('cluster_overview', []) if item.get('Property') == 'Version'), {})
        if version_info.get('Value', 'Unknown') != 'Unknown':
            summary.append(f"• Version: {version_info['Value']}")
        
        platform_info = next((item for item in data.get('cluster_overview', []) if item.get('Property') == 'Platform'), {})
        if platform_info.get('Value', 'Unknown') != 'Unknown':
            summary.append(f"• Platform: {platform_info['Value']}")
        
        # Node summary with total CPU and memory
        if 'node_distribution' in data:
            total_nodes = sum(item.get('Count', 0) for item in data['node_distribution'])
            total_ready = sum(item.get('Ready', 0) for item in data['node_distribution'])
            total_cpu = sum(item.get('Total CPU (cores)', 0) for item in data['node_distribution'])
            total_memory = sum(float(item.get('Total Memory (GB)', '0').replace(' GB', '').replace('GB', '')) for item in data['node_distribution'])
            
            summary.append(f"• Nodes: {total_ready}/{total_nodes} ready")
            summary.append(f"• Total Resources: {total_cpu} CPU cores, {total_memory:.0f}GB RAM")
            
            # Details by type
            for item in data['node_distribution']:
                if item.get('Count', 0) > 0:
                    node_type = item['Node Type']
                    count = item['Count'] 
                    ready = item['Ready']
                    cpu = item['Total CPU (cores)']
                    memory = item['Total Memory (GB)']
                    summary.append(f"• {node_type}: {ready}/{count} ready ({cpu} cores, {memory}GB)")
        
        # Resource highlights
        if 'resource_summary' in data:
            total_resources = next((item.get('Value') for item in data['resource_summary'] if item.get('Metric') == 'Total Resources'), 0)
            if total_resources > 0:
                summary.append(f"• Total Resources: {total_resources}")
            
            core_resources = next((item.get('Value') for item in data['resource_summary'] if item.get('Metric') == 'Core Resources (Pods+Services)'), 0)
            if core_resources > 0:
                summary.append(f"• Core Resources: {core_resources} (Pods+Services)")
        
        # Health status
        if 'cluster_health_status' in data:
            unavailable_ops = next((item for item in data['cluster_health_status'] 
                                if item.get('Health Metric') == 'Unavailable Operators'), {})
            if unavailable_ops.get('Value', 0) > 0:
                summary.append(f"⚠ {unavailable_ops['Value']} operators unavailable")
            
            degraded_mcp = next((item for item in data['cluster_health_status'] 
                            if item.get('Health Metric') == 'MCP Degraded Pools'), {})
            if degraded_mcp.get('Value', 0) > 0:
                summary.append(f"⚠ {degraded_mcp['Value']} MCP pools degraded")
        
        return " ".join(summary)
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        dataframes = {}
        
        try:
            for key, value in structured_data.items():
                if isinstance(value, list) and value:
                    df = pd.DataFrame(value)
                    if not df.empty:
                        # Apply column limiting for most tables, but not for node detail tables or node_distribution
                        if 'detail' not in key and key != 'node_distribution':
                            df = self.limit_dataframe_columns(df)
                        dataframes[key] = df
                        
        except Exception as e:
            logger.error(f"Failed to transform cluster info to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            for name, df in dataframes.items():
                # Skip rendering collection metadata as an HTML table
                if name == 'collection_metadata':
                    continue
                if not df.empty:
                    html_tables[name] = self.create_html_table(df, name)
        except Exception as e:
            logger.error(f"Failed to generate cluster info HTML tables: {e}")
        
        return html_tables