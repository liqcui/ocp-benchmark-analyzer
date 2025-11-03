"""
Extract, Load, Transform module for OpenShift Benchmark Performance Data
Main module for converting JSON outputs to table format and generating brief results
Updated to use optimized latencyELT module
"""

import logging
from typing import Dict, Any, List, Optional, Union, Tuple
import json
import pandas as pd
from datetime import datetime
import re
from tabulate import tabulate

# Import specialized ELT modules
from .ovnk_benchmark_elt_cluster_info import ClusterInfoELT
from .ovnk_benchmark_elt_nodes_usage import NodesUsageELT
from .ovnk_benchmark_elt_pods_usage import PodsUsageELT
from .ovnk_benchmark_elt_utility import EltUtility
from .ovnk_benchmark_elt_cluster_stat import ClusterStatELT
from .ovnk_benchmark_elt_ovs import OvsELT
from .ovnk_benchmark_elt_latency import latencyELT
from .ovnk_benchmark_elt_kubeapi import kubeAPIELT
from .ovnk_benchmark_elt_deepdrive import deepDriveELT

logger = logging.getLogger(__name__)

class PerformanceDataELT(EltUtility):
    """Main Extract, Load, Transform class for performance data"""
    
    def __init__(self):
        super().__init__()
        self.processed_data = {}
        # Initialize specialized ELT modules
        self.cluster_info_elt = ClusterInfoELT()
        self.node_usage_elt = NodesUsageELT()
        self.pods_usage_elt = PodsUsageELT()
        self.cluster_stat_elt = ClusterStatELT() 
        self.ovs_elt = OvsELT()
        self.kube_api_elt = kubeAPIELT()
        self.latencyelt = latencyELT() 
        self.deepdrive_elt = deepDriveELT()  

    def extract_json_data(self, mcp_results: Union[Dict[str, Any], str]) -> Dict[str, Any]:
        """Extract relevant data from MCP tool results"""
        try:
            # Normalize input to a dictionary
            if isinstance(mcp_results, str):
                try:
                    mcp_results = json.loads(mcp_results)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON string in extract_json_data: {e}")
                    return {'error': f"Invalid JSON string: {str(e)}", 'raw_data': mcp_results}
            
            if not isinstance(mcp_results, dict):
                return {'error': 'Input must be a dictionary or valid JSON string', 'raw_data': mcp_results}

            extracted = {
                'timestamp': mcp_results.get('timestamp', datetime.now().isoformat()),
                'data_type': self._identify_data_type(mcp_results),
                'raw_data': mcp_results,
                'structured_data': {}
            }
            
            # Extract structured data using specialized modules
            if extracted['data_type'] == 'cluster_info':
                extracted['structured_data'] = self.cluster_info_elt.extract_cluster_info(mcp_results)
            elif extracted['data_type'] == 'node_usage':
                extracted['structured_data'] = self.node_usage_elt.extract_node_usage(mcp_results)
            elif extracted['data_type'] == 'pod_usage':
                extracted['structured_data'] = self.pods_usage_elt.extract_pod_usage(mcp_results)
            elif extracted['data_type'] == 'cluster_status':
                extracted['structured_data'] = self.cluster_stat_elt.extract_cluster_stat(mcp_results)
            elif extracted['data_type'] == 'ovs_metrics':
                extracted['structured_data'] = self.ovs_elt.extract_ovs_data(mcp_results) 
            elif extracted['data_type'] in ['latencyelt_metrics', 'ovn_latency_metrics']:
                # Use optimized latencyELT for all latency-related data
                extracted['structured_data'] = self.latencyelt.extract_ovn_latency_data(mcp_results)                
            elif extracted['data_type'] == 'kube_api_metrics':
                extracted['structured_data'] = self.kube_api_elt.extract_kube_api_data(mcp_results)   
            elif extracted['data_type'] == 'ovn_deep_drive':
                extracted['structured_data'] = self.deepdrive_elt.extract_deepdrive_data(mcp_results)                             
            else:
                extracted['structured_data'] = self._extract_generic_data(mcp_results)
            
            return extracted
            
        except Exception as e:
            logger.error(f"Failed to extract JSON data: {e}")
            return {'error': str(e), 'raw_data': mcp_results}
    
    def _identify_data_type(self, data: Dict[str, Any]) -> str:
        """Identify the type of data from MCP results"""

        # Enhanced detection for optimized latencyELT data types
        # Check for controller sync top20 data
        if ('metric_name' in data and data.get('metric_name') == 'ovnkube_controller_sync_duration_seconds' and
            'top_20_detailed' in data and 'query_type' in data and 
            data.get('query_type') == 'controller_sync_top20'):
            return 'latencyelt_metrics'
        
        # Check for all metrics top5 detailed data
        if ('metrics_details' in data and 'query_type' in data and 
            data.get('query_type') == 'all_metrics_top5_detailed' and
            'total_metrics' in data):
            return 'latencyelt_metrics'
        
        # Check for essential results data (comprehensive latency analysis)
        if ('overall_summary' in data and 'top_latencies_ranking' in data and
            'metrics_by_category' in data and 'critical_findings' in data):
            return 'latencyelt_metrics'

        # Check for standard latencyELT structure
        if ('collection_timestamp' in data and 'query_type' in data and 'summary' in data and
            any(key in data for key in ['ready_duration_metrics', 'sync_duration_metrics'])):
            return 'latencyelt_metrics'

        # Check for OVN Deep Drive analysis data
        if ('analysis_type' in data and data.get('analysis_type') == 'comprehensive_deep_drive' and
            'basic_info' in data and 'ovnkube_pods_cpu' in data and 'ovs_metrics' in data and
            'performance_analysis' in data):
            return 'ovn_deep_drive'

        # Check for OVN latency metrics data (comprehensive enhanced metrics)
        if ('overall_summary' in data and 'collection_type' in data and 
            data.get('collection_type') in ['enhanced_comprehensive', 'comprehensive'] and
            any(key in data for key in ['ready_duration_metrics', 'sync_duration_metrics', 
                                    'percentile_latency_metrics', 'pod_latency_metrics', 
                                    'cni_latency_metrics', 'service_latency_metrics', 
                                    'network_programming_metrics'])):
            return 'latencyelt_metrics'  # Use optimized latencyELT
        
        # Check for single OVN latency metric results
        if ('metric_name' in data and 'statistics' in data and 'component' in data and
            'unit' in data and any(component in data.get('component', '') for component in ['controller', 'node'])):
            return 'latencyelt_metrics'  # Use optimized latencyELT

        # Check for Kube API metrics
        if ('metrics' in data and 'summary' in data and 
            'timestamp' in data and 'duration' in data):
            metrics = data.get('metrics', {})
            if ('readonly_latency' in metrics and 'mutating_latency' in metrics and 
                'basic_api' in metrics):
                return 'kube_api_metrics'

        # Check for OVS metrics data
        if ('cpu_usage' in data and 'memory_usage' in data and 
            'dp_flows' in data and 'bridge_flows' in data and 
            'connection_metrics' in data):
            return 'ovs_metrics'

        # Check for cluster status analysis data
        if ('cluster_health' in data and 'node_groups' in data and 
            'metadata' in data and data.get('metadata', {}).get('analyzer_type') == 'cluster_status'):
            return 'cluster_status'

        # Check for pod usage metrics
        if ('top_5_cpu_usage' in data and 'top_5_memory_usage' in data and 
            'collection_type' in data and 'total_analyzed' in data):
            return 'pod_usage'

        # Check for cluster info
        if 'cluster_name' in data and 'cluster_version' in data and 'master_nodes' in data:
            return 'cluster_info'
        
        # Check for node usage data
        if 'groups' in data and 'metadata' in data and 'top_usage' in data:
            return 'node_usage'

        # Legacy checks
        if 'version' in data and 'identity' in data:
            return 'cluster_info'
        elif 'nodes_by_role' in data or 'total_nodes' in data:
            return 'node_info'
        elif 'api_server_latency' in data or 'latency_metrics' in data:
            return 'api_metrics'
        elif 'result' in data and 'query' in data:
            return 'prometheus_query'
        elif 'analysis_type' in data and data.get('analysis_type') == 'cluster_status_analysis':
            return 'cluster_status'
        elif 'node_status' in data and 'operator_status' in data:
            return 'cluster_status'
        else:
            return 'generic'
    
    def _extract_generic_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract generic data with smart column limiting"""
        structured = {'key_value_pairs': []}
        
        def extract_important_fields(d: Dict[str, Any], max_fields: int = 20) -> List[Tuple[str, Any]]:
            """Extract most important fields from a dictionary"""
            fields = []
            
            # Priority fields (always include if present)
            priority_keys = [
                'name', 'status', 'version', 'timestamp', 'count', 'total',
                'health', 'error', 'message', 'value', 'metric', 'result'
            ]
            
            # Add priority fields first
            for key in priority_keys:
                if key in d:
                    fields.append((key, d[key]))
            
            # Add remaining fields up to limit
            remaining_keys = [k for k in d.keys() if k not in priority_keys]
            for key in remaining_keys:
                if len(fields) < max_fields:
                    fields.append((key, d[key]))
                else:
                    break
            
            return fields
        
        important_fields = extract_important_fields(data)
        
        for key, value in important_fields:
            # Format value for display
            if isinstance(value, (dict, list)):
                if isinstance(value, dict):
                    display_value = f"Dict({len(value)} keys)"
                else:
                    display_value = f"List({len(value)} items)"
            else:
                value_str = str(value)
                display_value = value_str[:100] + '...' if len(value_str) > 100 else value_str
            
            structured['key_value_pairs'].append({
                'Property': key.replace('_', ' ').title(),
                'Value': display_value
            })
        
        return structured
       
    def generate_brief_summary(self, structured_data: Dict[str, Any], data_type: str) -> str:
        """Generate a brief textual summary of the data using specialized modules"""
        try:
            if data_type == 'cluster_info':
                return self.cluster_info_elt.summarize_cluster_info(structured_data)
            elif data_type == 'node_usage':
                return self.node_usage_elt.summarize_node_usage(structured_data)
            elif data_type == 'pod_usage':
                return self.pods_usage_elt.summarize_pod_usage(structured_data)
            elif data_type == 'cluster_status':
                return self.cluster_stat_elt.summarize_cluster_stat(structured_data)
            elif data_type == 'ovs_metrics':
                return self.ovs_elt.summarize_ovs_data(structured_data) 
            elif data_type in ['latencyelt_metrics', 'ovn_latency_metrics']:
                # Use optimized latencyELT for all latency data
                return self.latencyelt.summarize_ovn_latency_data(structured_data)                
            elif data_type == 'kube_api_metrics':
                return self.kube_api_elt.summarize_kube_api_data(structured_data)
            elif data_type == 'ovn_deep_drive':
                return self.deepdrive_elt.summarize_deepdrive_data(structured_data)                
            else:
                return self._summarize_generic(structured_data)
        
        except Exception as e:
            logger.error(f"Failed to generate summary: {e}")
            return f"Summary generation failed: {str(e)}"
    
    def _summarize_generic(self, data: Dict[str, Any]) -> str:
        """Generate generic summary"""
        summary = ["Data Summary:"]
        if 'key_value_pairs' in data:
            summary.append(f"• Total properties: {len(data['key_value_pairs'])}")
            # Show first few important properties
            for item in data['key_value_pairs'][:3]:
                value_preview = str(item['Value'])[:30] + "..." if len(str(item['Value'])) > 30 else str(item['Value'])
                summary.append(f"• {item['Property']}: {value_preview}")
        return " ".join(summary)

    def transform_to_dataframes(self, structured_data: Dict[str, Any], data_type: str) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames using specialized modules"""
        try:
            if data_type == 'cluster_info':
                return self.cluster_info_elt.transform_to_dataframes(structured_data)
            elif data_type == 'node_usage':
                return self.node_usage_elt.transform_to_dataframes(structured_data)
            elif data_type == 'pod_usage':
                return self.pods_usage_elt.transform_to_dataframes(structured_data)
            elif data_type == 'cluster_status':
                return self.cluster_stat_elt.transform_to_dataframes(structured_data)
            elif data_type == 'ovs_metrics':
                return self.ovs_elt.transform_to_dataframes(structured_data)
            elif data_type in ['latencyelt_metrics', 'ovn_latency_metrics']:
                # Use optimized latencyELT for all latency data
                return self.latencyelt.transform_to_dataframes(structured_data)                
            elif data_type == 'kube_api_metrics':
                return self.kube_api_elt.transform_to_dataframes(structured_data)
            elif data_type == 'ovn_deep_drive':
                return self.deepdrive_elt.transform_to_dataframes(structured_data)                                                          
            else:
                # Default transformation for other data types
                dataframes = {}
                for key, value in structured_data.items():
                    if isinstance(value, list) and value:
                        df = pd.DataFrame(value)
                        if not df.empty:
                            df = self.limit_dataframe_columns(df, table_name=key)
                            dataframes[key] = df
                return dataframes

        except Exception as e:
            logger.error(f"Failed to transform to DataFrames: {e}")
            return {}

    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame], data_type: str) -> Dict[str, str]:
        """Generate HTML tables using specialized modules"""
        try:
            if data_type == 'cluster_info':
                return self.cluster_info_elt.generate_html_tables(dataframes)
            elif data_type == 'node_usage':
                return self.node_usage_elt.generate_html_tables(dataframes)
            elif data_type == 'pod_usage':
                return self.pods_usage_elt.generate_html_tables(dataframes)
            elif data_type == 'cluster_status':
                return self.cluster_stat_elt.generate_html_tables(dataframes)
            elif data_type == 'ovs_metrics':
                return self.ovs_elt.generate_html_tables(dataframes)
            elif data_type in ['latencyelt_metrics', 'ovn_latency_metrics']:
                # Use optimized latencyELT for all latency data
                return self.latencyelt.generate_html_tables(dataframes)                     
            elif data_type == 'kube_api_metrics':
                return self.kube_api_elt.generate_html_tables(dataframes)   
            elif data_type == 'ovn_deep_drive':
                return self.deepdrive_elt.generate_html_tables(dataframes)                                                       
            else:
                # Default HTML table generation
                html_tables = {}
                for name, df in dataframes.items():
                    if not df.empty:
                        html_tables[name] = self.create_html_table(df, name)
                return html_tables
        except Exception as e:
            logger.error(f"Failed to generate HTML tables: {e}")
            return {}


# Enhanced module functions using the optimized latencyELT structure
def extract_and_transform_mcp_results(mcp_results: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and transform MCP results into tables and summaries"""
    try:
        elt = PerformanceDataELT()
        
        # Extract data
        extracted = elt.extract_json_data(mcp_results)
        
        if 'error' in extracted:
            return extracted
        
        # Create DataFrames using specialized modules
        dataframes = elt.transform_to_dataframes(extracted['structured_data'], extracted['data_type'])
        
        # Generate HTML tables using specialized modules
        html_tables = elt.generate_html_tables(dataframes, extracted['data_type'])
        
        # Generate summary using specialized modules
        summary = elt.generate_brief_summary(extracted['structured_data'], extracted['data_type'])
        
        return {
            'data_type': extracted['data_type'],
            'summary': summary,
            'html_tables': html_tables,
            'dataframes': dataframes,
            'structured_data': extracted['structured_data'],
            'timestamp': extracted['timestamp']
        }
        
    except Exception as e:
        logger.error(f"Failed to extract and transform MCP results: {e}")
        return {'error': str(e)}


# JSON to HTML format functions (main entry points)
def json_to_html_table(json_data: Union[Dict[str, Any], str], compact: bool = True) -> str:
    """Convert JSON data to HTML table format"""
    result = convert_json_to_tables(json_data, "html", compact)
    
    if 'error' in result:
        return f"<div class='alert alert-danger'>Error: {result['error']}</div>"
    
    html_tables = result.get('html', {})
    if not html_tables:
        return "<div class='alert alert-warning'>No tables generated</div>"
    
    # Generate organized output
    output_parts = []
    
    # Add data type info
    data_type = result.get('metadata', {}).get('data_type', 'unknown')
    output_parts.append(f"<div class='mb-3'><span class='badge badge-info'>{data_type.replace('_', ' ').title()}</span></div>")
    
    # Add summary if available
    if result.get('summary'):
        output_parts.append(f"<div class='alert alert-light'>{result['summary']}</div>")
    
    # Add tables
    for table_name, html_table in html_tables.items():
        table_title = table_name.replace('_', ' ').title()
        output_parts.append(f"<h5 class='mt-3'>{table_title}</h5>")
        output_parts.append(html_table)
    
    return ' '.join(output_parts)


def convert_json_to_tables(json_data: Union[Dict[str, Any], str], 
                          table_format: str = "both",
                          compact: bool = True) -> Dict[str, Union[str, List[List]]]:
    """Convert JSON/dictionary data to table formats"""
    try:
        # Parse JSON string if needed
        if isinstance(json_data, str):
            try:
                data = json.loads(json_data)
            except json.JSONDecodeError as e:
                return {
                    'error': f"Invalid JSON string: {str(e)}",
                    'metadata': {'conversion_failed': True}
                }
        else:
            data = json_data
        
        if not isinstance(data, dict):
            return {
                'error': "Input data must be a dictionary or JSON object",
                'metadata': {'conversion_failed': True}
            }
        
        # Use the main ELT processor
        transformed = extract_and_transform_mcp_results(data)
        
        if 'error' in transformed:
            return {
                'error': f"Data transformation failed: {transformed['error']}",
                'metadata': {'conversion_failed': True}
            }
        
        result = {
            'metadata': {
                'data_type': transformed['data_type'],
                'timestamp': transformed.get('timestamp'),
                'tables_generated': len(transformed['html_tables']),
                'table_names': list(transformed['html_tables'].keys()),
                'conversion_successful': True,
                'compact_mode': compact
            },
            'summary': transformed['summary']
        }
        
        # Add requested formats
        if table_format in ["html", "both"]:
            result['html'] = transformed['html_tables']
        
        if table_format in ["tabular", "both"]:
            # Convert DataFrames to tabular format
            tabular_tables = {}
            for table_name, df in transformed['dataframes'].items():
                if not df.empty:
                    raw_data = [df.columns.tolist()] + df.values.tolist()
                    try:
                        formatted_table = tabulate(
                            df.values.tolist(), 
                            headers=df.columns.tolist(),
                            tablefmt="grid",
                            stralign="left",
                            maxcolwidths=[30] * len(df.columns)
                        )
                        tabular_tables[table_name] = {
                            'raw_data': raw_data,
                            'formatted_string': formatted_table
                        }
                    except Exception as e:
                        logger.warning(f"Failed to format table {table_name}: {e}")
                        tabular_tables[table_name] = {
                            'raw_data': raw_data,
                            'formatted_string': str(df)
                        }
            result['tabular'] = tabular_tables
        
        return result
        
    except Exception as e:
        logger.error(f"Error converting JSON to tables: {e}")
        return {
            'error': str(e),
            'metadata': {'conversion_failed': True}
        }


# Optimized latencyELT convenience functions
async def latencyelt_convert_to_html_tables(latency_data: Dict[str, Any]) -> str:
    """Convert latencyELT data to HTML table format using optimized module"""
    try:
        # Import optimized functions from the latencyELT module
        from .ovnk_benchmark_elt_latency import latencyelt_convert_json_to_html_tables
        
        return await latencyelt_convert_json_to_html_tables(latency_data)
        
    except Exception as e:
        return f"<div class='alert alert-danger'>Error processing latency data: {str(e)}</div>"


async def latencyelt_process_and_convert_data(latency_data: Union[Dict[str, Any], str], output_format: str = "html") -> Dict[str, Any]:
    """Process and convert latency data using optimized latencyELT module"""
    try:
        # Import optimized function from the latencyELT module
        from .ovnk_benchmark_elt_latency import latencyelt_process_and_convert
        
        return await latencyelt_process_and_convert(latency_data, output_format)
        
    except Exception as e:
        return {'error': f"Processing failed: {str(e)}", 'processing_successful': False}


async def latencyelt_get_comprehensive_data(prometheus_client, time: str = None, duration: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get comprehensive latency data using optimized latencyELT module"""
    try:
        from .ovnk_benchmark_elt_latency import latencyelt_get_comprehensive_metrics
        
        return await latencyelt_get_comprehensive_metrics(prometheus_client, time, duration, end_time)
        
    except Exception as e:
        logger.error(f"Failed to get comprehensive latency data: {e}")
        return {'error': str(e)}


async def latencyelt_get_controller_sync_top20_data(prometheus_client, time: str = None, duration: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get detailed top 20 controller sync duration metrics using optimized latencyELT module"""
    try:
        from .ovnk_benchmark_elt_latency import latencyelt_get_controller_sync_top20
        
        return await latencyelt_get_controller_sync_top20(prometheus_client, time, duration, end_time)
        
    except Exception as e:
        logger.error(f"Failed to get controller sync top20 data: {e}")
        return {'error': str(e)}


async def latencyelt_get_all_metrics_top5_data(prometheus_client, time: str = None, duration: str = None, end_time: str = None) -> Dict[str, Any]:
    """Get detailed top 5 entries for all latency metrics using optimized latencyELT module"""
    try:
        from .ovnk_benchmark_elt_latency import latencyelt_get_all_metrics_top5_detailed
        
        return await latencyelt_get_all_metrics_top5_detailed(prometheus_client, time, duration, end_time)
        
    except Exception as e:
        logger.error(f"Failed to get all metrics top5 data: {e}")
        return {'error': str(e)}


# Export main functions for external use
__all__ = [
    'PerformanceDataELT',
    'extract_and_transform_mcp_results',
    'json_to_html_table',
    'convert_json_to_tables',
    'latencyelt_convert_to_html_tables',
    'latencyelt_process_and_convert_data',
    'latencyelt_get_comprehensive_data',
    'latencyelt_get_controller_sync_top20_data', 
    'latencyelt_get_all_metrics_top5_data'    
]