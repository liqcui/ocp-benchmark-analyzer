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
from .ovnk_benchmark_elt_ovs import OvsELT
from .ovnk_benchmark_elt_latency import latencyELT
from .ovnk_benchmark_elt_kubeapi import kubeAPIELT
from .ovnk_benchmark_elt_deepdrive import deepDriveELT
from .ovnk_benchmark_elt_network_io import NetworkIOELT

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
        self.network_io_elt = NetworkIOELT()
        self.logger=logging.getLogger(__name__)

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
            elif extracted['data_type'] == 'network_io':
                extracted['structured_data'] = self.network_io_elt.extract_network_io(mcp_results)
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

        if ('category' in data and data.get('category') == 'network_io' and
            'metrics' in data and 'summary' in data and 
            'collection_metadata' in data):
            return 'network_io'

        # Check for node usage data
        # Support both legacy structure (groups/metadata/top_usage) and new structure
        # with collection timestamp/time range/node_groups_summary and metrics map
        if (
            (
                isinstance(data.get('metrics'), dict)
                and any(k in data['metrics'] for k in ['cpu_usage', 'cgroup_cpu', 'memory_used', 'cache_buffer', 'cgroup_rss'])
                and any(k in data for k in ['collection_timestamp_utc', 'time_range', 'node_groups_summary'])
            )
            or ('groups' in data and 'metadata' in data and 'top_usage' in data)
        ):
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
        """Extract generic data for unknown types.
        Keep nested values intact so HTML renderer can expand them instead of placeholder strings.
        """
        structured = {'key_value_pairs': []}
        
        def extract_fields(d: Dict[str, Any], max_fields: int = 50) -> List[Tuple[str, Any]]:
            fields: List[Tuple[str, Any]] = []
            preferred = ['collection_timestamp_utc', 'time_range', 'node_groups_summary', 'metrics', 'summary', 'status']
            for k in preferred:
                if k in d:
                    fields.append((k, d[k]))
            for k, v in d.items():
                if k not in [f[0] for f in fields]:
                    fields.append((k, v))
                if len(fields) >= max_fields:
                    break
            return fields

        for key, value in extract_fields(data):
            if isinstance(value, (dict, list)):
                final_value = value
            else:
                s = str(value)
                final_value = s if len(s) <= 500 else s[:500] + '...'
            structured['key_value_pairs'].append({
                'Property': key.replace('_', ' ').title(),
                'Value': final_value
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
            elif data_type == 'network_io':
                return self.network_io_elt.summarize_network_io(structured_data)
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
            elif data_type == 'network_io':
                return self.network_io_elt.transform_to_dataframes(structured_data)
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
            elif data_type == 'network_io':
                return self.network_io_elt.generate_html_tables(dataframes)                                                                      
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

def json_to_html_table(json_data: Union[Dict[str, Any], str], compact: bool = True, title: Optional[str] = None) -> str:
    """Convert JSON data to HTML table format with proper styling"""
    try:
        # Parse JSON string if needed
        if isinstance(json_data, str):
            try:
                json_data = json.loads(json_data)
            except json.JSONDecodeError as e:
                return f"<div class='alert alert-danger'>Invalid JSON: {str(e)}</div>"
        
        if not isinstance(json_data, dict):
            return "<div class='alert alert-warning'>Input must be a dictionary or JSON object</div>"

        # Try to use specialized ELT processor first
        result = convert_json_to_tables(json_data, "html", compact)
        
        if 'error' in result:
            # Fallback to generic processing
            logger.warning(f"Conversion error: {result['error']}, using generic processing")
            return _json_to_html_table_generic(json_data, title)
        
        html_tables = result.get('html', {})
        if not html_tables:
            return "<div class='alert alert-warning'>No tables generated</div>"
        
        # Generate organized output with metrics highlighting
        output_parts = []

        data_type = result.get('metadata', {}).get('data_type', 'unknown')
        
        # Add title if provided
        if title:
            output_parts.append(f"<h4 class='mt-3'>{title}</h4>")
        
        # Add data type badge
        type_display = data_type.replace('_', ' ').title()
        if data_type == 'node_usage':
            type_display = 'Node Usage Metrics'
        elif data_type == 'pod_usage':
            type_display = 'Pod Usage Metrics'
        elif data_type == 'cluster_info':
            type_display = 'Cluster Information'
        elif data_type == 'cluster_status':
            type_display = 'Cluster Status Analysis'
        elif data_type == 'ovs_metrics':
            type_display = 'OVS Performance Metrics'
        elif data_type in ['latencyelt_metrics', 'ovn_latency_metrics']:
            type_display = 'OVN Latency Analysis'
        elif data_type == 'kube_api_metrics':
            type_display = 'Kubernetes API Metrics'
        elif data_type == 'ovn_deep_drive':
            type_display = 'OVN Deep Drive Analysis'
            
        output_parts.append(
            f"<div class='mb-3'><span class='badge badge-info' style='font-size: 1.0rem; font-weight: 600'>{type_display}</span></div>"
        )

        # Add summary if available
        if result.get('summary'):
            summary_val = result['summary']
            # Get utility instance for Unicode decoding
            util = EltUtility()
            
            # If summary is HTML, pass through; otherwise format
            if isinstance(summary_val, str) and summary_val.strip().startswith('<'):
                try:
                    decoded_summary = util.decode_unicode_escapes(summary_val)
                except Exception:
                    decoded_summary = summary_val
                # Normalize bullet symbols to HTML entity to avoid mojibake in some renderers
                decoded_summary = decoded_summary.replace('•', '&bull;')
                output_parts.append(f"<div class='alert alert-light'>{decoded_summary}</div>")
            else:
                # Format summary text
                try:
                    summary_text = util.decode_unicode_escapes(str(summary_val))
                except Exception:
                    summary_text = str(summary_val)

                def _escape_html(s: str) -> str:
                    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

                # Special formatting for performance summaries
                performance_keywords = ['Performance', 'Analysis', 'Metrics', 'Usage', 'Summary']
                if any(keyword in summary_text for keyword in performance_keywords) or '•' in summary_text or '\n' in summary_text:
                    parts = [p.strip() for p in summary_text.split('\n') if p.strip()]
                    
                    if parts:
                        first_line = _escape_html(parts[0])
                        # Bold performance analysis titles
                        for keyword in performance_keywords:
                            if keyword in first_line:
                                first_line = first_line.replace(keyword, f'<strong>{keyword}</strong>')
                        
                        remaining_parts = [_escape_html(p) for p in parts[1:] if p]
                        formatted_summary = first_line
                        if remaining_parts:
                            formatted_summary += '<br>' + '<br>'.join(remaining_parts)
                        # Normalize bullet symbols
                        formatted_summary = formatted_summary.replace('•', '&bull;')
                        output_parts.append(f"<div class='alert alert-light'>{formatted_summary}</div>")
                    else:
                        safe_summary = _escape_html(summary_text)
                        safe_summary = safe_summary.replace('•', '&bull;')
                        output_parts.append(f"<div class='alert alert-light'>{safe_summary}</div>")
                else:
                    safe_summary = _escape_html(summary_text)
                    safe_summary = safe_summary.replace('•', '&bull;')
                    output_parts.append(f"<div class='alert alert-light'>{safe_summary}</div>")
        
        # Add tables with priority ordering based on data type
        table_priority = []
        
        if data_type == 'node_usage':
            table_priority = [
                'collection_info',
                'cpu_usage_controlplane', 'cpu_usage_infra', 'cpu_usage_worker', 'cpu_usage_workload',
                'cgroup_cpu_controlplane', 'cgroup_cpu_infra', 'cgroup_cpu_worker', 'cgroup_cpu_workload',
                'memory_used_controlplane', 'memory_used_infra', 'memory_used_worker', 'memory_used_workload',
                'cache_buffer_controlplane', 'cache_buffer_infra', 'cache_buffer_worker', 'cache_buffer_workload',
                'cgroup_rss_controlplane', 'cgroup_rss_infra', 'cgroup_rss_worker', 'cgroup_rss_workload'
            ]
        elif data_type == 'pod_usage':
            table_priority = [
                'collection_info', 'top_cpu_usage', 'top_memory_usage',
                'cpu_usage_by_namespace', 'memory_usage_by_namespace'
            ]
        elif data_type == 'cluster_info':
            table_priority = [
                'cluster_overview', 'node_summary', 'master_nodes', 'worker_nodes'
            ]
        elif data_type == 'cluster_status':
            table_priority = [
                'cluster_overview', 'node_status', 'operator_status', 'health_summary'
            ]
        elif data_type == 'ovs_metrics':
            table_priority = [
                'metrics_overview', 'cpu_usage', 'memory_usage', 'flow_metrics', 'connection_metrics'
            ]
        elif data_type in ['latencyelt_metrics', 'ovn_latency_metrics']:
            table_priority = [
                'latencyelt_collection_info', 'latencyelt_essential_summary',
                'latencyelt_top_latencies_ranking', 'latencyelt_controller_sync_top20',
                'latencyelt_all_metrics_top5', 'latencyelt_critical_findings'
            ]
        elif data_type == 'kube_api_metrics':
            table_priority = [
                'summary', 'readonly_latency', 'mutating_latency', 'basic_api'
            ]
        elif data_type == 'ovn_deep_drive':
            table_priority = [
                'overview', 'cpu_analysis', 'memory_analysis', 'ovs_analysis', 'performance_summary'
            ]
        else:
            table_priority = []
        
        # Add priority tables first
        added_tables = set()
        for priority_table in table_priority:
            if priority_table in html_tables:
                table_title = priority_table.replace('_', ' ').title()
                
                # Special titles for different data types
                if data_type == 'node_usage':
                    if priority_table == 'collection_info':
                        table_title = 'Collection Information'
                    else:
                        parts = priority_table.split('_')
                        if len(parts) >= 2:
                            metric = ' '.join(parts[:-1]).title()
                            role = parts[-1].title()
                            table_title = f'{metric} - {role} Nodes'
                elif data_type == 'pod_usage':
                    title_mapping = {
                        'collection_info': 'Collection Information',
                        'top_cpu_usage': 'Top CPU Usage (Pods)',
                        'top_memory_usage': 'Top Memory Usage (Pods)',
                        'cpu_usage_by_namespace': 'CPU Usage by Namespace',
                        'memory_usage_by_namespace': 'Memory Usage by Namespace'
                    }
                    table_title = title_mapping.get(priority_table, table_title)
                elif data_type in ['latencyelt_metrics', 'ovn_latency_metrics']:
                    title_mapping = {
                        'latencyelt_collection_info': 'Collection Information',
                        'latencyelt_essential_summary': 'Essential Metrics Summary',
                        'latencyelt_top_latencies_ranking': 'Top Latencies Ranking',
                        'latencyelt_controller_sync_top20': 'Controller Sync Top 20 Details',
                        'latencyelt_all_metrics_top5': 'All Metrics Top 5 Details',
                        'latencyelt_critical_findings': 'Critical Findings'
                    }
                    table_title = title_mapping.get(priority_table, table_title)
                
                output_parts.append(f"<h5 class='mt-3'>{table_title}</h5>")
                output_parts.append(html_tables[priority_table])
                added_tables.add(priority_table)
        
        # Add remaining tables
        for table_name, html_table in html_tables.items():
            if table_name not in added_tables:
                table_title = table_name.replace('_', ' ').title()
                output_parts.append(f"<h5 class='mt-3'>{table_title}</h5>")
                output_parts.append(html_table)
        
        # Join all parts into final HTML
        final_html = ''.join(output_parts)
        
        # Decode any lingering unicode escapes in the final HTML
        try:
            util = EltUtility()
            final_html = util.decode_unicode_escapes(final_html)
        except Exception:
            pass
        
        # Return as plain string (not JSON encoded)
        return final_html
        
    except Exception as e:
        logger.error(f"Error in json_to_html_table: {e}")
        import traceback
        traceback.print_exc()
        return f"<div class='alert alert-danger'>Error generating HTML table: {str(e)}</div>"

def _json_to_html_table_generic(data: Dict[str, Any], title: Optional[str] = None) -> str:
    """Generic JSON to HTML table converter as fallback"""
    try:
        from html import escape
        
        def _render(value: Any) -> str:
            if isinstance(value, dict):
                rows = []
                for k, v in value.items():
                    rows.append(
                        f"<tr><th style='white-space:nowrap;text-align:left;padding:4px 8px;background:#f7f7f7'>{escape(str(k))}</th>"
                        f"<td style='padding:4px 8px'>{_render(v)}</td></tr>"
                    )
                return (
                    "<table class='table table-sm table-bordered' style='width:100%;border-collapse:collapse'>"
                    + "".join(rows)
                    + "</table>"
                )

            if isinstance(value, list):
                if not value:
                    return "<i>empty</i>"
                # List of dicts -> tabular with headers
                if all(isinstance(item, dict) for item in value):
                    # Collect headers from union of keys
                    headers: List[str] = []
                    seen = set()
                    for item in value:
                        for key in item.keys():
                            if key not in seen:
                                seen.add(key)
                                headers.append(key)
                    thead = "<thead><tr>" + "".join(
                        f"<th style='text-align:left;padding:4px 8px;background:#f0f0f0'>{escape(str(h))}</th>" for h in headers
                    ) + "</tr></thead>"
                    body_rows = []
                    for item in value:
                        cells = []
                        for h in headers:
                            cells.append(
                                f"<td style='padding:4px 8px;vertical-align:top'>{_render(item.get(h)) if h in item else ''}</td>"
                            )
                        body_rows.append("<tr>" + "".join(cells) + "</tr>")
                    tbody = "<tbody>" + "".join(body_rows) + "</tbody>"
                    return (
                        "<table class='table table-sm table-striped table-bordered' style='width:100%;border-collapse:collapse'>"
                        + thead + tbody + "</table>"
                    )
                # List of scalars -> single-column table
                items = "".join(
                    f"<tr><td style='padding:4px 8px'>{escape(str(i))}</td></tr>" for i in value
                )
                return (
                    "<table class='table table-sm table-bordered' style='width:auto;border-collapse:collapse'>"
                    + items + "</table>"
                )

            # Scalars
            if value is None:
                return "<i>null</i>"
            if isinstance(value, (int, float)):
                return escape(str(value))
            return escape(str(value))

        header = f"<h4 style='margin:8px 0'>{escape(title)}</h4>" if title else ""
        return header + _render(data)
    except Exception as e:
        logger.error(f"_json_to_html_table_generic failed: {e}")
        # Fallback: preformatted JSON-like dump
        try:
            import json
            pretty = json.dumps(data, indent=2, default=str)
        except Exception:
            pretty = str(data)
        from html import escape
        return f"<pre>{escape(pretty)}</pre>"
        
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



