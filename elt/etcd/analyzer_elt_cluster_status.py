"""
ELT module for etcd cluster status data
Extract, Load, Transform module for etcd cluster status information
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from datetime import datetime
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)

class etcdClusterStatusELT(utilityELT):
    """ELT module for etcd cluster status data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'db_size': {
                'title': 'Database Size',
                'unit': 'MB',
                'thresholds': {'critical': 8000, 'warning': 4000}
            },
            'raft_term': {
                'title': 'Raft Term',
                'unit': 'count',
                'thresholds': {'critical': float('inf'), 'warning': float('inf')}
            },
            'raft_index': {
                'title': 'Raft Index',
                'unit': 'count',
                'thresholds': {'critical': float('inf'), 'warning': float('inf')}
            }
        }
    
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
                'cluster_metrics': self._extract_cluster_metrics(data.get('cluster_metrics', {})),
                'prometheus_metrics': self._extract_prometheus_metrics(data.get('prometheus_etcd_metrics', {}))
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
            db_size_mb = self.parse_db_size(db_size_raw)
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
    
    def _extract_prometheus_metrics(self, prom_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract Prometheus metrics grouped by node role"""
        try:
            if prom_data.get('status') != 'success':
                return {'status': 'error', 'error': prom_data.get('error', 'Unknown error')}
            
            metrics = prom_data.get('metrics', {})
            role_groups = {
                'controlplane': {},
                'infra': {},
                'worker': {},
                'workload': {}
            }
            
            # Process each metric
            for metric_name, metric_data in metrics.items():
                if metric_data.get('status') != 'success':
                    continue
                
                nodes = metric_data.get('nodes', {})
                
                # Group nodes by role (infer from node name)
                for node_name, node_stats in nodes.items():
                    role = self._infer_node_role(node_name)
                    if role not in role_groups:
                        role = 'worker'  # default
                    
                    if metric_name not in role_groups[role]:
                        role_groups[role][metric_name] = {}
                    
                    role_groups[role][metric_name][node_name] = node_stats
            
            return {
                'status': 'success',
                'duration': prom_data.get('duration', '1h'),
                'category': prom_data.get('category', 'general_info'),
                'role_groups': role_groups,
                'raw_metrics': metrics
            }
            
        except Exception as e:
            logger.error(f"Failed to extract Prometheus metrics: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def _infer_node_role(self, node_name: str) -> str:
        """Infer node role from node name"""
        node_lower = node_name.lower()
        if 'master' in node_lower or 'control' in node_lower:
            return 'controlplane'
        elif 'infra' in node_lower:
            return 'infra'
        elif 'workload' in node_lower:
            return 'workload'
        else:
            return 'worker'
    
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
            df_overview = pd.DataFrame(overview_data)
            if not df_overview.empty:
                for col in df_overview.columns:
                    if df_overview[col].dtype == 'object':
                        df_overview[col] = df_overview[col].astype(str).apply(self.decode_unicode_escapes)
            dataframes['cluster_overview'] = df_overview
            
            # Endpoint Status Table
            endpoint_data = structured_data.get('endpoint_status', {})
            endpoints = endpoint_data.get('endpoints', [])
            top_db_indices = endpoint_data.get('top_db_indices', [])
            
            if endpoints:
                endpoint_rows = []
                for i, endpoint in enumerate(endpoints):
                    db_size = endpoint.get('db_size', '')
                    db_size_mb = endpoint.get('db_size_mb', 0)
                    
                    # Highlight top DB size and critical values
                    if i in top_db_indices:
                        db_size_display = self.highlight_critical_values(
                            db_size_mb, 
                            self.metric_configs['db_size']['thresholds'], 
                            ' MB', 
                            is_top=True
                        )
                    else:
                        db_size_display = self.highlight_critical_values(
                            db_size_mb,
                            self.metric_configs['db_size']['thresholds'],
                            ' MB',
                            is_top=False
                        )
                    
                    leader_status = endpoint.get('is_leader', False)
                    leader_display = self.create_status_badge('success', 'LEADER') if leader_status else 'false'
                    
                    endpoint_rows.append({
                        'Endpoint': self.truncate_url(endpoint.get('endpoint', ''), 40),
                        'ID': endpoint.get('id', '')[:16],
                        'Version': endpoint.get('version', ''),
                        'DB Size': db_size_display,
                        'Is Leader': leader_display,
                        'Raft Term': endpoint.get('raft_term', ''),
                        'Raft Index': endpoint.get('raft_index', '')
                    })
                
                df_endpoints = pd.DataFrame(endpoint_rows)
                if not df_endpoints.empty:
                    for col in df_endpoints.columns:
                        if df_endpoints[col].dtype == 'object':
                            df_endpoints[col] = df_endpoints[col].astype(str).apply(self.decode_unicode_escapes)
                dataframes['endpoint_status'] = df_endpoints
            
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
                
                df_health = pd.DataFrame(health_rows)
                if not df_health.empty:
                    for col in df_health.columns:
                        if df_health[col].dtype == 'object':
                            df_health[col] = df_health[col].astype(str).apply(self.decode_unicode_escapes)
                dataframes['health_status'] = df_health
            
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
                
                df_members = pd.DataFrame(member_rows)
                if not df_members.empty:
                    for col in df_members.columns:
                        if df_members[col].dtype == 'object':
                            df_members[col] = df_members[col].astype(str).apply(self.decode_unicode_escapes)
                dataframes['member_details'] = df_members
            
            # Cluster Metrics Summary
            metrics_data = structured_data.get('cluster_metrics', {})
            if metrics_data.get('status') == 'success':
                metrics_rows = [
                    {'Metric': 'Namespace', 'Value': metrics_data.get('namespace', '')},
                    {'Metric': 'Total Endpoints', 'Value': metrics_data.get('total_endpoints', 0)},
                    {'Metric': 'Leader Count', 'Value': metrics_data.get('leader_count', 0)},
                    {'Metric': 'Total DB Size', 'Value': self._format_db_size(metrics_data.get('total_db_size_mb', 0))}
                ]
                df_metrics = pd.DataFrame(metrics_rows)
                if not df_metrics.empty:
                    for col in df_metrics.columns:
                        if df_metrics[col].dtype == 'object':
                            df_metrics[col] = df_metrics[col].astype(str).apply(self.decode_unicode_escapes)
                dataframes['metrics_summary'] = df_metrics
            
            # Prometheus Metrics by Role
            prom_data = structured_data.get('prometheus_metrics', {})
            if prom_data.get('status') == 'success':
                self._transform_prometheus_metrics(prom_data, dataframes)
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to transform cluster status to DataFrames: {e}")
            return {}
    
    def _transform_prometheus_metrics(self, prom_data: Dict[str, Any], dataframes: Dict[str, pd.DataFrame]):
        """Transform Prometheus metrics into role-grouped DataFrames"""
        try:
            role_groups = prom_data.get('role_groups', {})
            raw_metrics = prom_data.get('raw_metrics', {})
            
            for role in ['controlplane', 'infra', 'worker', 'workload']:
                role_metrics = role_groups.get(role, {})
                if not role_metrics:
                    continue
                
                # Process each metric type for this role
                for metric_name, nodes_data in role_metrics.items():
                    if not nodes_data:
                        continue
                    
                    # Get metric metadata
                    metric_info = raw_metrics.get(metric_name, {})
                    title = metric_info.get('title', metric_name)
                    unit = metric_info.get('unit', '')
                    
                    # Collect all values for top identification
                    all_values = []
                    for node_name, stats in nodes_data.items():
                        avg_val = stats.get('avg', 0)
                        max_val = stats.get('max', 0)
                        all_values.append((node_name, avg_val, max_val))
                    
                    # Find top values
                    top_avg = max((v[1] for v in all_values), default=0) if all_values else 0
                    top_max = max((v[2] for v in all_values), default=0) if all_values else 0
                    
                    # Build rows
                    rows = []
                    thresholds = self.get_default_thresholds(metric_name)
                    
                    for node_name, avg_val, max_val in all_values:
                        # Format values with highlighting
                        avg_display = self.format_and_highlight(
                            avg_val, unit, thresholds, is_top=(avg_val == top_avg and avg_val > 0)
                        )
                        max_display = self.format_and_highlight(
                            max_val, unit, thresholds, is_top=(max_val == top_max and max_val > 0)
                        )
                        
                        rows.append({
                            'Node': self.truncate_node_name(node_name),
                            'Avg': avg_display,
                            'Max': max_display
                        })
                    
                    if rows:
                        df = pd.DataFrame(rows)
                        if not df.empty:
                            for col in df.columns:
                                if df[col].dtype == 'object':
                                    df[col] = df[col].astype(str).apply(self.decode_unicode_escapes)
                            
                            table_key = f'{metric_name}_{role}'
                            dataframes[table_key] = df
        
        except Exception as e:
            logger.error(f"Failed to transform Prometheus metrics: {e}")
    
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
            
            # Core cluster status tables (not role-grouped)
            core_tables = [
                'cluster_overview',
                'endpoint_status', 
                'health_status',
                'member_details',
                'metrics_summary'
            ]
            
            for table_name in core_tables:
                if table_name in dataframes and not dataframes[table_name].empty:
                    df = dataframes[table_name]
                    if table_name != 'endpoint_status':
                        df = self.limit_dataframe_columns(df, table_name=table_name)
                    html_tables[table_name] = self.create_html_table(df, table_name)
            
            # Prometheus metrics tables grouped by role
            metric_prefixes = []
            for key in dataframes.keys():
                if key not in core_tables and '_controlplane' in key:
                    prefix = key.replace('_controlplane', '')
                    if prefix not in metric_prefixes:
                        metric_prefixes.append(prefix)
            
            # Generate tables for each metric and role
            for metric_prefix in metric_prefixes:
                for role in ['controlplane', 'infra', 'worker', 'workload']:
                    table_key = f'{metric_prefix}_{role}'
                    if table_key in dataframes and not dataframes[table_key].empty:
                        html_tables[table_key] = self.create_html_table(
                            dataframes[table_key], 
                            table_key
                        )
            
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
            
            # Prometheus metrics summary
            prom_data = structured_data.get('prometheus_metrics', {})
            if prom_data.get('status') == 'success':
                role_groups = prom_data.get('role_groups', {})
                metrics_count = sum(len(metrics) for metrics in role_groups.values())
                if metrics_count > 0:
                    summary_parts.append(f"• <strong>Prometheus Metrics:</strong> {metrics_count} collected")
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate cluster status summary: {e}")
            return f"etcd Cluster Status Summary (Error: {str(e)})"