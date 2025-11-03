"""
ELT module for etcd cluster status data
Extract, Load, Transform module for etcd cluster status information
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from datetime import datetime
from .etcd_analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)

class etcdClusterStatusELT(utilityELT):
    """ELT module for etcd cluster status data"""
    
    def __init__(self):
        super().__init__()
    
    def extract_cluster_status(self, status_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract etcd cluster status data"""
        try:
            # Handle nested data structure
            data = status_data.get('data', status_data)
            
            extracted = {
                'timestamp': data.get('timestamp', datetime.now().isoformat()),
                'etcd_pod': data.get('etcd_pod', 'Unknown'),
                'cluster_health': self._extract_health_info(data.get('cluster_health', {})),
                'endpoint_status': self._extract_endpoint_status(data.get('endpoint_status', {})),
                'member_status': self._extract_member_status(data.get('member_status', {})),
                'leader_info': self._extract_leader_info(data.get('leader_info', {})),
                'cluster_metrics': self._extract_cluster_metrics(data.get('cluster_metrics', {}))
            }
            
            return extracted
            
        except Exception as e:
            logger.error(f"Failed to extract cluster status: {e}")
            return {'error': str(e)}
    
    def _extract_health_info(self, health_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract health information"""
        return {
            'status': health_data.get('status', 'unknown'),
            'healthy_endpoints': health_data.get('healthy_endpoints', []),
            'unhealthy_endpoints': health_data.get('unhealthy_endpoints', []),
            'total_endpoints': health_data.get('total_endpoints', 0),
            'health_percentage': health_data.get('health_percentage', 0)
        }
    
    def _extract_endpoint_status(self, endpoint_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract endpoint status information"""
        endpoints = endpoint_data.get('endpoints', [])
        
        # Process endpoint data with metrics highlighting
        processed_endpoints = []
        db_sizes = []
        
        for endpoint in endpoints:
            # Parse DB size for comparison
            db_size_raw = endpoint.get('db_size', '0 MB')
            db_size_mb = self._parse_db_size(db_size_raw)
            db_sizes.append(db_size_mb)
            
            processed_endpoints.append({
                'endpoint': endpoint.get('endpoint', ''),
                'id': endpoint.get('id', ''),
                'version': endpoint.get('version', ''),
                'db_size': db_size_raw,
                'db_size_mb': db_size_mb,
                'is_leader': endpoint.get('is_leader', False),
                'raft_term': endpoint.get('raft_term', ''),
                'raft_index': endpoint.get('raft_index', '')
            })
        
        # Identify top DB size for highlighting
        top_db_indices = self.identify_top_values(
            [{'db_size_mb': ep['db_size_mb']} for ep in processed_endpoints], 
            'db_size_mb'
        )
        
        return {
            'status': endpoint_data.get('status', 'unknown'),
            'endpoints': processed_endpoints,
            'total_endpoints': len(processed_endpoints),
            'leader_endpoint': endpoint_data.get('leader_endpoint'),
            'top_db_indices': top_db_indices
        }
    
    def _extract_member_status(self, member_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract member status information"""
        return {
            'status': member_data.get('status', 'unknown'),
            'total_members': member_data.get('total_members', 0),
            'active_members': member_data.get('active_members', 0),
            'learner_members': member_data.get('learner_members', 0),
            'members': member_data.get('members', [])
        }
    
    def _extract_leader_info(self, leader_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract leader information"""
        leader_info = leader_data.get('leader_info', {})
        return {
            'status': leader_data.get('status', 'unknown'),
            'has_leader': leader_info.get('has_leader', False),
            'leader_endpoint': leader_info.get('leader_endpoint'),
            'leader_id': leader_info.get('leader_id'),
            'term': leader_info.get('term')
        }
    
    def _extract_cluster_metrics(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cluster metrics"""
        metrics = metrics_data.get('metrics', {})
        return {
            'status': metrics_data.get('status', 'unknown'),
            'namespace': metrics.get('namespace', ''),
            'etcd_pod': metrics.get('etcd_pod', ''),
            'total_endpoints': metrics.get('total_endpoints', 0),
            'leader_count': metrics.get('leader_count', 0),
            'total_db_size_mb': metrics.get('estimated_total_db_size_mb', 0),
            'endpoints_summary': metrics.get('endpoints_summary', [])
        }
    
    def _parse_db_size(self, size_str: str) -> float:
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
    
    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        try:
            dataframes = {}
            
            # Cluster Overview
            overview_data = [
                {'Property': 'Timestamp', 'Value': self.format_timestamp(structured_data.get('timestamp', ''))},
                {'Property': 'etcd Pod', 'Value': structured_data.get('etcd_pod', 'Unknown')},
                {'Property': 'Health Status', 'Value': structured_data.get('cluster_health', {}).get('status', 'Unknown')},
                {'Property': 'Health Percentage', 'Value': f"{structured_data.get('cluster_health', {}).get('health_percentage', 0)}%"},
                {'Property': 'Total Endpoints', 'Value': structured_data.get('endpoint_status', {}).get('total_endpoints', 0)},
                {'Property': 'Leader Present', 'Value': 'Yes' if structured_data.get('leader_info', {}).get('has_leader') else 'No'},
                {'Property': 'Total DB Size', 'Value': self._format_db_size(structured_data.get('cluster_metrics', {}).get('total_db_size_mb', 0))}
            ]
            dataframes['cluster_overview'] = pd.DataFrame(overview_data)
            
            # Endpoint Status Table (main table similar to etcdctl output)
            endpoint_data = structured_data.get('endpoint_status', {})
            endpoints = endpoint_data.get('endpoints', [])
            top_db_indices = endpoint_data.get('top_db_indices', [])
            
            if endpoints:
                endpoint_rows = []
                for i, endpoint in enumerate(endpoints):
                    # Highlight top DB size and leader
                    db_size = endpoint.get('db_size', '')
                    if i in top_db_indices:
                        db_size = self.highlight_critical_values(
                            endpoint.get('db_size_mb', 0), 
                            {'critical': float('inf'), 'warning': float('inf')}, 
                            ' MB', 
                            is_top=True
                        )
                    
                    leader_status = endpoint.get('is_leader', False)
                    leader_display = self.create_status_badge('success', 'LEADER') if leader_status else 'false'
                    
                    endpoint_rows.append({
                        'Endpoint': self.truncate_url(endpoint.get('endpoint', ''), 40),
                        'ID': endpoint.get('id', '')[:16],  # Truncate ID
                        'Version': endpoint.get('version', ''),
                        'DB Size': db_size,
                        'Is Leader': leader_display,
                        'Raft Term': endpoint.get('raft_term', ''),
                        'Raft Index': endpoint.get('raft_index', '')
                    })
                
                dataframes['endpoint_status'] = pd.DataFrame(endpoint_rows)
            
            # Cluster Health Details
            health_data = structured_data.get('cluster_health', {})
            if health_data.get('healthy_endpoints') or health_data.get('unhealthy_endpoints'):
                health_rows = []
                
                for endpoint in health_data.get('healthy_endpoints', []):
                    health_rows.append({
                        'Endpoint': self.truncate_url(endpoint, 40),
                        'Status': self.create_status_badge('success', 'Healthy'),
                        'Type': 'Healthy'
                    })
                
                for endpoint in health_data.get('unhealthy_endpoints', []):
                    health_rows.append({
                        'Endpoint': self.truncate_url(endpoint, 40),
                        'Status': self.create_status_badge('danger', 'Unhealthy'),
                        'Type': 'Unhealthy'
                    })
                
                dataframes['health_status'] = pd.DataFrame(health_rows)
            
            # Member Information
            member_data = structured_data.get('member_status', {})
            members = member_data.get('members', [])
            if members:
                member_rows = []
                for member in members:
                    member_type = 'Learner' if member.get('is_learner', False) else 'Active'
                    member_rows.append({
                        'Name': self.truncate_node_name(member.get('name', ''), 35),
                        'ID': str(member.get('id', ''))[:16],
                        'Type': self.create_status_badge('info' if member_type == 'Active' else 'warning', member_type),
                        'Client URLs': len(member.get('client_urls', [])),
                        'Peer URLs': len(member.get('peer_urls', []))
                    })
                
                dataframes['member_details'] = pd.DataFrame(member_rows)
            
            # Cluster Metrics Summary
            metrics_data = structured_data.get('cluster_metrics', {})
            if metrics_data.get('status') == 'success':
                metrics_rows = [
                    {'Metric': 'Namespace', 'Value': metrics_data.get('namespace', '')},
                    {'Metric': 'Total Endpoints', 'Value': metrics_data.get('total_endpoints', 0)},
                    {'Metric': 'Leader Count', 'Value': metrics_data.get('leader_count', 0)},
                    {'Metric': 'Total DB Size', 'Value': self._format_db_size(metrics_data.get('total_db_size_mb', 0))}
                ]
                dataframes['metrics_summary'] = pd.DataFrame(metrics_rows)
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform cluster status to DataFrames: {e}")
            return {}
    
    def _format_db_size(self, size_mb: float) -> str:
        """Format database size with appropriate units"""
        if size_mb == 0:
            return "0 MB"
        elif size_mb >= 1024:
            return f"{size_mb / 1024:.1f} GB"
        else:
            return f"{size_mb:.0f} MB"
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables with etcd-specific formatting"""
        try:
            html_tables = {}
            
            # Table generation priority for etcd data
            table_priority = [
                'cluster_overview',
                'endpoint_status', 
                'health_status',
                'member_details',
                'metrics_summary'
            ]
            
            for table_name in table_priority:
                if table_name in dataframes and not dataframes[table_name].empty:
                    df = dataframes[table_name]
                    
                    # Limit columns if needed (except for main endpoint status table)
                    if table_name != 'endpoint_status':
                        df = self.limit_dataframe_columns(df, table_name=table_name)
                    
                    html_tables[table_name] = self.create_html_table(df, table_name)
            
            return html_tables
            
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for cluster status: {e}")
            return {}
    
    def summarize_cluster_status(self, structured_data: Dict[str, Any]) -> str:
        """Generate summary of etcd cluster status"""
        try:
            summary_parts = []
            
            # Basic cluster info
            etcd_pod = structured_data.get('etcd_pod', 'Unknown')
            summary_parts.append(f"<strong>etcd Cluster Status</strong> (Pod: {etcd_pod})")
            
            # Health status
            health_data = structured_data.get('cluster_health', {})
            health_status = health_data.get('status', 'unknown')
            health_pct = health_data.get('health_percentage', 0)
            
            if health_status == 'healthy':
                summary_parts.append(f"✅ <span class='text-success'>Cluster Health: {health_status.title()} ({health_pct}%)</span>")
            else:
                summary_parts.append(f"⚠️ <span class='text-warning'>Cluster Health: {health_status.title()} ({health_pct}%)</span>")
            
            # Endpoint and leader info
            endpoint_data = structured_data.get('endpoint_status', {})
            total_endpoints = endpoint_data.get('total_endpoints', 0)
            leader_endpoint = endpoint_data.get('leader_endpoint', 'None')
            
            summary_parts.append(f"• <strong>Endpoints:</strong> {total_endpoints}")
            
            if leader_endpoint != 'None':
                short_leader = leader_endpoint.split('//')[1].split(':')[0] if '//' in leader_endpoint else leader_endpoint
                summary_parts.append(f"• <strong>Leader:</strong> {short_leader}")
            
            # Database size
            metrics_data = structured_data.get('cluster_metrics', {})
            total_db_size = metrics_data.get('total_db_size_mb', 0)
            if total_db_size > 0:
                summary_parts.append(f"• <strong>Total DB Size:</strong> {self._format_db_size(total_db_size)}")
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate cluster status summary: {e}")
            return f"etcd Cluster Status Summary (Error: {str(e)})"