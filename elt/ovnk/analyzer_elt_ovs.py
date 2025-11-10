"""
Extract, Load, Transform module for OVS Usage Data
Handles OVS CPU, Memory, Flows, and Connection metrics
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Union
from datetime import datetime

from .ovnk_benchmark_elt_utility import EltUtility

logger = logging.getLogger(__name__)


class OvsELT(EltUtility):
    """Extract, Load, Transform class for OVS metrics data"""
    
    def __init__(self):
        super().__init__()
    
    def extract_ovs_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract OVS metrics from JSON data"""
        try:
            extracted = {
                'timestamp': data.get('timestamp', datetime.now().isoformat()),
                'collection_type': data.get('collection_type', 'unknown'),
                'cpu_usage': data.get('cpu_usage', {}),
                'memory_usage': data.get('memory_usage', {}),
                'dp_flows': data.get('dp_flows', {}),
                'bridge_flows': data.get('bridge_flows', {}),
                'connection_metrics': data.get('connection_metrics', {}),
                'metadata': {
                    'analyzer_type': 'ovs_metrics',
                    'collection_type': data.get('collection_type', 'unknown'),
                    'query_type': data.get('cpu_usage', {}).get('query_type', 'unknown')
                }
            }
            
            return extracted
            
        except Exception as e:
            logger.error(f"Failed to extract OVS data: {e}")
            return {'error': str(e)}
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform OVS data into pandas DataFrames"""
        dataframes = {}
        
        try:
            # Metadata table
            metadata_data = []
            metadata = structured_data.get('metadata', {})
            metadata_data.append({'Property': 'Collection Type', 'Value': structured_data.get('collection_type', 'Unknown')})
            metadata_data.append({'Property': 'Query Type', 'Value': metadata.get('query_type', 'Unknown')})
            metadata_data.append({'Property': 'Timestamp', 'Value': self.format_timestamp(structured_data.get('timestamp', ''))})
            
            if metadata_data:
                df = pd.DataFrame(metadata_data)
                dataframes['metadata'] = self.limit_dataframe_columns(df, 2, 'metadata')
            
            # CPU Usage Summary - Top performers
            cpu_usage = structured_data.get('cpu_usage', {})
            if cpu_usage and 'summary' in cpu_usage:
                cpu_summary_data = []
                
                # OVS-vSwitchd top performers
                vswitchd_top = cpu_usage['summary'].get('ovs_vswitchd_top10', [])[:5]
                for item in vswitchd_top:
                    node_name = self.truncate_node_name(item.get('node_name', ''), 20)
                    cpu_summary_data.append({
                        'Component': 'ovs-vswitchd',
                        'Node': node_name,
                        'Max CPU %': f"{item.get('max', 0):.2f}",
                        'Avg CPU %': f"{item.get('avg', 0):.2f}"
                    })
                
                # OVSDB Server top performers
                ovsdb_top = cpu_usage['summary'].get('ovsdb_server_top10', [])[:3]
                for item in ovsdb_top:
                    node_name = self.truncate_node_name(item.get('node_name', ''), 20)
                    cpu_summary_data.append({
                        'Component': 'ovsdb-server',
                        'Node': node_name,
                        'Max CPU %': f"{item.get('max', 0):.2f}",
                        'Avg CPU %': f"{item.get('avg', 0):.2f}"
                    })
                
                if cpu_summary_data:
                    df = pd.DataFrame(cpu_summary_data)
                    dataframes['cpu_usage_summary'] = self.limit_dataframe_columns(df, 4, 'cpu_usage_summary')
            
            # Memory Usage Summary
            memory_usage = structured_data.get('memory_usage', {})
            if memory_usage and 'summary' in memory_usage:
                memory_summary_data = []
                
                # OVS DB Memory top performers
                db_top = memory_usage['summary'].get('ovs_db_top10', [])[:3]
                for item in db_top:
                    pod_name = self.truncate_text(item.get('pod_name', ''), 25)
                    memory_summary_data.append({
                        'Component': 'OVS-DB',
                        'Pod': pod_name,
                        'Max Memory': f"{item.get('max', 0):.1f} {item.get('unit', 'MB')}",
                        'Avg Memory': f"{item.get('avg', 0):.1f} {item.get('unit', 'MB')}"
                    })
                
                # OVS vSwitchd Memory top performers
                vswitchd_top = memory_usage['summary'].get('ovs_vswitchd_top10', [])[:3]
                for item in vswitchd_top:
                    pod_name = self.truncate_text(item.get('pod_name', ''), 25)
                    memory_summary_data.append({
                        'Component': 'OVS-vSwitchd',
                        'Pod': pod_name,
                        'Max Memory': f"{item.get('max', 0):.1f} {item.get('unit', 'MB')}",
                        'Avg Memory': f"{item.get('avg', 0):.1f} {item.get('unit', 'MB')}"
                    })
                
                if memory_summary_data:
                    df = pd.DataFrame(memory_summary_data)
                    dataframes['memory_usage_summary'] = self.limit_dataframe_columns(df, 4, 'memory_usage_summary')
            
            # DP Flows Analysis
            dp_flows = structured_data.get('dp_flows', {})
            if dp_flows and 'top_10' in dp_flows:
                dp_flows_data = []
                for item in dp_flows['top_10'][:5]:
                    instance = self.truncate_text(item.get('instance', ''), 20)
                    dp_flows_data.append({
                        'Instance': instance,
                        'Max Flows': f"{item.get('max', 0):,}",
                        'Avg Flows': f"{item.get('avg', 0):.0f}",
                        'Min Flows': f"{item.get('min', 0):,}"
                    })
                
                if dp_flows_data:
                    df = pd.DataFrame(dp_flows_data)
                    dataframes['dp_flows_top'] = self.limit_dataframe_columns(df, 4, 'dp_flows_top')
            
            # Bridge Flows Analysis
            bridge_flows = structured_data.get('bridge_flows', {})
            if bridge_flows and 'top_10' in bridge_flows:
                bridge_flows_data = []
                
                # BR-INT flows
                br_int_top = bridge_flows['top_10'].get('br_int', [])[:3]
                for item in br_int_top:
                    instance = self.truncate_text(item.get('instance', ''), 20)
                    bridge_flows_data.append({
                        'Bridge': 'br-int',
                        'Instance': instance,
                        'Max Flows': f"{item.get('max', 0):,}",
                        'Avg Flows': f"{item.get('avg', 0):.0f}"
                    })
                
                # BR-EX flows
                br_ex_top = bridge_flows['top_10'].get('br_ex', [])[:2]
                for item in br_ex_top:
                    instance = self.truncate_text(item.get('instance', ''), 20)
                    bridge_flows_data.append({
                        'Bridge': 'br-ex',
                        'Instance': instance,
                        'Max Flows': f"{item.get('max', 0):,}",
                        'Avg Flows': f"{item.get('avg', 0):.0f}"
                    })
                
                if bridge_flows_data:
                    df = pd.DataFrame(bridge_flows_data)
                    dataframes['bridge_flows_summary'] = self.limit_dataframe_columns(df, 4, 'bridge_flows_summary')
            
            # Connection Metrics
            connection_metrics = structured_data.get('connection_metrics', {})
            if connection_metrics and 'connection_metrics' in connection_metrics:
                conn_data = []
                metrics = connection_metrics['connection_metrics']
                
                for metric_name, values in metrics.items():
                    if 'error' not in values:
                        metric_display = metric_name.replace('_', ' ').title()
                        conn_data.append({
                            'Metric': metric_display,
                            'Value': f"{values.get('max', 0):.0f} {values.get('unit', '')}"
                        })
                
                if conn_data:
                    df = pd.DataFrame(conn_data)
                    dataframes['connection_metrics'] = self.limit_dataframe_columns(df, 2, 'connection_metrics')
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform OVS data to DataFrames: {e}")
            return {}
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables for OVS data"""
        html_tables = {}
        
        # Define table order and titles
        table_configs = {
            'metadata': 'Collection Information',
            'cpu_usage_summary': 'CPU Usage - Top Performers',
            'memory_usage_summary': 'Memory Usage - Top Performers', 
            'dp_flows_top': 'Datapath Flows - Top Instances',
            'bridge_flows_summary': 'Bridge Flows Summary',
            'connection_metrics': 'Connection Metrics'
        }
        
        for table_name, title in table_configs.items():
            if table_name in dataframes and not dataframes[table_name].empty:
                html_table = self.create_html_table(dataframes[table_name], table_name)
                if html_table:
                    html_tables[table_name] = html_table
        
        return html_tables
    
    def summarize_ovs_data(self, structured_data: Dict[str, Any]) -> str:
        """Generate a brief summary of OVS metrics"""
        try:
            summary_parts = ["OVS Metrics Analysis:"]
            
            # Collection info
            collection_type = structured_data.get('collection_type', 'unknown')
            summary_parts.append(f"Collection type: {collection_type}")
            
            # CPU Usage Summary
            cpu_usage = structured_data.get('cpu_usage', {})
            if cpu_usage and 'summary' in cpu_usage:
                vswitchd_top = cpu_usage['summary'].get('ovs_vswitchd_top10', [])
                ovsdb_top = cpu_usage['summary'].get('ovsdb_server_top10', [])
                
                if vswitchd_top:
                    max_cpu = vswitchd_top[0].get('max', 0)
                    summary_parts.append(f"Peak OVS-vSwitchd CPU: {max_cpu:.2f}%")
                
                if ovsdb_top:
                    max_ovsdb = ovsdb_top[0].get('max', 0)
                    summary_parts.append(f"Peak OVSDB CPU: {max_ovsdb:.2f}%")
            
            # Memory Usage Summary
            memory_usage = structured_data.get('memory_usage', {})
            if memory_usage and 'summary' in memory_usage:
                vswitchd_mem = memory_usage['summary'].get('ovs_vswitchd_top10', [])
                if vswitchd_mem:
                    max_mem = vswitchd_mem[0].get('max', 0)
                    unit = vswitchd_mem[0].get('unit', 'MB')
                    summary_parts.append(f"Peak vSwitchd Memory: {max_mem:.1f} {unit}")
            
            # Flow Statistics
            dp_flows = structured_data.get('dp_flows', {})
            if dp_flows and 'top_10' in dp_flows:
                top_flows = dp_flows['top_10']
                if top_flows:
                    max_flows = top_flows[0].get('max', 0)
                    summary_parts.append(f"Peak DP flows: {max_flows:,}")
            
            bridge_flows = structured_data.get('bridge_flows', {})
            if bridge_flows and 'top_10' in bridge_flows:
                br_int_top = bridge_flows['top_10'].get('br_int', [])
                if br_int_top:
                    max_br_int = br_int_top[0].get('max', 0)
                    summary_parts.append(f"Peak br-int flows: {max_br_int:,}")
            
            return " • ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to summarize OVS data: {e}")
            return f"Summary generation failed: {str(e)}"