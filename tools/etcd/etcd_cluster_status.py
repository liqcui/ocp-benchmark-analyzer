"""
etcd Cluster Status Collector
Collects etcd cluster status and health information using oc rsh commands
"""

import asyncio
import json
import logging
import subprocess
from typing import Dict, Any, List, Optional
from datetime import datetime
import pytz
import os
import sys

# Ensure project root on sys.path for utils imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from config.metrics_config_reader import Config


class ClusterStatCollector:
    """Collector for etcd cluster status information"""
    
    def __init__(self, ocp_auth=None, metrics_file_path: str = None):
        self.ocp_auth = ocp_auth
        self.logger = logging.getLogger(__name__)
        self.timezone = pytz.UTC
        self.etcd_namespace = "openshift-etcd"
        self.duration = "1h"
        self.utility = mcpToolsUtility(ocp_auth)
        
        # Load metrics configuration
        if metrics_file_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metrics_file_path = os.path.join(project_root, 'config', 'metrics-etcd.yml')
        
        self.config = Config()
        load_result = self.config.load_metrics_file(metrics_file_path)
        
        # Get category-specific metrics (cluster status uses general_info)
        self.category = "general_info"
        cluster_metrics = self.config.get_metrics_by_category(self.category)
        
        # Log metrics loading
        if load_result.get('success'):
            metrics_count = len(cluster_metrics)
            self.logger.info(f"✅ Loaded {metrics_count} metrics from {self.category} category (ClusterStatCollector)")
        else:
            self.logger.warning(f"⚠️  No {self.category} metrics found in configuration")
    
    async def _get_etcd_pod_name(self) -> Dict[str, Any]:
        """Get the first available etcd pod name"""
        try:
            cmd = [
                "oc", "get", "pod", 
                "-n", self.etcd_namespace, 
                "-l", "app=etcd", 
                "-o", "jsonpath={.items[0].metadata.name}"
            ]
            
            result = await self._run_command(cmd)
            
            if result['returncode'] != 0:
                return {
                    'status': 'error',
                    'error': f"Failed to get etcd pod: {result['stderr']}"
                }
            
            pod_name = result['stdout'].strip()
            if not pod_name:
                return {
                    'status': 'error',
                    'error': "No etcd pods found"
                }
            
            return {
                'status': 'success',
                'pod_name': pod_name
            }
            
        except Exception as e:
            self.logger.error(f"Error getting etcd pod name: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def _run_command(self, cmd: List[str]) -> Dict[str, Any]:
        """Run a shell command asynchronously"""
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            return {
                'returncode': process.returncode,
                'stdout': stdout.decode('utf-8'),
                'stderr': stderr.decode('utf-8')
            }
            
        except Exception as e:
            return {
                'returncode': -1,
                'stdout': '',
                'stderr': str(e)
            }
    
    async def _execute_etcd_command(self, etcd_cmd: str, pod_name: str) -> Dict[str, Any]:
        """Execute etcdctl command in the etcd pod using oc rsh"""
        try:
            cmd = [
                "oc", "rsh", 
                "-n", self.etcd_namespace, 
                "-c", "etcd", 
                pod_name,
                "sh", "-c", 
                f"unset ETCDCTL_ENDPOINTS; {etcd_cmd}"
            ]
            
            result = await self._run_command(cmd)
            
            if result['returncode'] != 0:
                return {
                    'status': 'error',
                    'error': result['stderr'],
                    'output': result['stdout']
                }
            
            return {
                'status': 'success',
                'output': result['stdout']
            }
            
        except Exception as e:
            self.logger.error(f"Error executing etcd command: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def get_cluster_status(self) -> Dict[str, Any]:
        """Get comprehensive etcd cluster status"""
        try:
            # Get etcd pod name
            pod_result = await self._get_etcd_pod_name()
            if pod_result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': f"Failed to get etcd pod: {pod_result.get('error')}",
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
            
            pod_name = pod_result['pod_name']
            
            cluster_info = {
                'timestamp': datetime.now(self.timezone).isoformat(),
                'etcd_pod': pod_name,
                'cluster_health': await self._get_cluster_health(pod_name),
                'member_status': await self._get_member_status(pod_name),
                'endpoint_status': await self._get_endpoint_status(pod_name),
                'leader_info': await self._get_leader_info(pod_name),
                'cluster_metrics': await self._get_basic_metrics(pod_name),
                'prometheus_etcd_metrics': await self._collect_etcd_metrics()
            }
            
            return {
                'status': 'success',
                'data': cluster_info
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting cluster status: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def _get_cluster_health(self, pod_name: str) -> Dict[str, Any]:
        """Get cluster health information"""
        try:
            health_result = await self._execute_etcd_command(
                "etcdctl endpoint health --cluster",
                pod_name
            )
            
            if health_result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': health_result.get('error')
                }
            
            # Parse health output
            health_lines = health_result.get('output', '').strip().split('\n')
            healthy_endpoints = []
            unhealthy_endpoints = []
            
            for line in health_lines:
                if line.strip():
                    if 'is healthy' in line.lower():
                        endpoint = line.split()[0]
                        healthy_endpoints.append(endpoint)
                    elif 'is unhealthy' in line.lower() or 'unhealthy' in line.lower():
                        endpoint = line.split()[0] if line.split() else 'unknown'
                        unhealthy_endpoints.append(endpoint)
            
            total_endpoints = len(healthy_endpoints) + len(unhealthy_endpoints)
            health_percentage = (len(healthy_endpoints) / max(total_endpoints, 1)) * 100
            
            return {
                'status': 'healthy' if len(unhealthy_endpoints) == 0 else 'degraded',
                'healthy_endpoints': healthy_endpoints,
                'unhealthy_endpoints': unhealthy_endpoints,
                'total_endpoints': total_endpoints,
                'health_percentage': round(health_percentage, 2),
                'raw_output': health_result.get('output')
            }
            
        except Exception as e:
            self.logger.error(f"Error getting cluster health: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def _get_member_status(self, pod_name: str) -> Dict[str, Any]:
        """Get member status information"""
        try:
            member_result = await self._execute_etcd_command(
                "etcdctl member list -w json",
                pod_name
            )
            
            if member_result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': member_result.get('error')
                }
            
            # Parse JSON output
            output = member_result.get('output', '').strip()
            if not output:
                return {'status': 'no_data', 'members': []}
            
            try:
                members_data = json.loads(output)
                members = members_data.get('members', [])
                
                active_members = [m for m in members if not m.get('isLearner', False)]
                learner_members = [m for m in members if m.get('isLearner', False)]
                
                return {
                    'status': 'success',
                    'total_members': len(members),
                    'active_members': len(active_members),
                    'learner_members': len(learner_members),
                    'members': [
                        {
                            'id': member.get('ID'),
                            'name': member.get('name'),
                            'peer_urls': member.get('peerURLs', []),
                            'client_urls': member.get('clientURLs', []),
                            'is_learner': member.get('isLearner', False)
                        }
                        for member in members
                    ]
                }
                
            except json.JSONDecodeError as e:
                return {
                    'status': 'parse_error',
                    'error': f'Failed to parse member list JSON: {e}',
                    'raw_output': output
                }
            
        except Exception as e:
            self.logger.error(f"Error getting member status: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def _get_endpoint_status(self, pod_name: str) -> Dict[str, Any]:
        """Get endpoint status information using the exact command you specified"""
        try:
            status_result = await self._execute_etcd_command(
                "etcdctl endpoint status -w table --cluster",
                pod_name
            )
            
            if status_result['status'] != 'success':
                return {
                    'status': 'error',
                    'error': status_result.get('error')
                }
            
            # Parse table output
            output = status_result.get('output', '').strip()
            lines = output.split('\n')
            
            endpoints_info = []
            
            # Look for table data (skip header and separator lines)
            for line in lines:
                line = line.strip()
                if line and '|' in line and not line.startswith('+') and 'ENDPOINT' not in line.upper():
                    parts = [p.strip() for p in line.split('|') if p.strip()]
                    if len(parts) >= 4:
                        endpoints_info.append({
                            'endpoint': parts[0],
                            'id': parts[1],
                            'version': parts[2] if len(parts) > 2 else 'unknown',
                            'db_size': parts[3] if len(parts) > 3 else 'unknown',
                            'is_leader': parts[4].lower() == 'true' if len(parts) > 4 else False,
                            'raft_term': parts[5] if len(parts) > 5 else 'unknown',
                            'raft_index': parts[6] if len(parts) > 6 else 'unknown'
                        })
            
            # Find leader
            leader_endpoint = None
            for endpoint in endpoints_info:
                if endpoint.get('is_leader'):
                    leader_endpoint = endpoint['endpoint']
                    break
            
            return {
                'status': 'success',
                'endpoints': endpoints_info,
                'total_endpoints': len(endpoints_info),
                'leader_endpoint': leader_endpoint,
                'raw_output': output
            }
            
        except Exception as e:
            self.logger.error(f"Error getting endpoint status: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def _get_leader_info(self, pod_name: str) -> Dict[str, Any]:
        """Get leader information"""
        try:
            endpoint_status = await self._get_endpoint_status(pod_name)
            
            if endpoint_status['status'] != 'success':
                return endpoint_status
            
            leader_info = {
                'has_leader': False,
                'leader_endpoint': None,
                'term': None,
                'leader_id': None
            }
            
            for endpoint in endpoint_status.get('endpoints', []):
                if endpoint.get('is_leader'):
                    leader_info.update({
                        'has_leader': True,
                        'leader_endpoint': endpoint['endpoint'],
                        'term': endpoint.get('raft_term'),
                        'leader_id': endpoint.get('id')
                    })
                    break
            
            return {
                'status': 'success',
                'leader_info': leader_info
            }
            
        except Exception as e:
            self.logger.error(f"Error getting leader info: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def _get_basic_metrics(self, pod_name: str) -> Dict[str, Any]:
        """Get basic cluster metrics"""
        try:
            # Get endpoint status for basic metrics
            endpoint_status = await self._get_endpoint_status(pod_name)
            
            if endpoint_status['status'] != 'success':
                return endpoint_status
            
            endpoints = endpoint_status.get('endpoints', [])
            total_db_size = 0
            
            # Try to calculate total DB size (if format allows)
            for endpoint in endpoints:
                db_size_str = endpoint.get('db_size', '0')
                try:
                    # Extract numeric part (assuming format like "25 MB" or "1.2 GB")
                    if 'MB' in db_size_str.upper():
                        size_mb = float(db_size_str.upper().replace('MB', '').strip())
                        total_db_size += size_mb
                    elif 'GB' in db_size_str.upper():
                        size_gb = float(db_size_str.upper().replace('GB', '').strip())
                        total_db_size += size_gb * 1024
                    elif 'KB' in db_size_str.upper():
                        size_kb = float(db_size_str.upper().replace('KB', '').strip())
                        total_db_size += size_kb / 1024
                except (ValueError, AttributeError):
                    pass
            
            metrics = {
                'namespace': self.etcd_namespace,
                'etcd_pod': pod_name,
                'total_endpoints': len(endpoints),
                'leader_count': sum(1 for e in endpoints if e.get('is_leader')),
                'estimated_total_db_size_mb': round(total_db_size, 2) if total_db_size > 0 else None,
                'endpoints_summary': [
                    {
                        'endpoint': e.get('endpoint'),
                        'is_leader': e.get('is_leader'),
                        'version': e.get('version'),
                        'db_size': e.get('db_size')
                    }
                    for e in endpoints
                ]
            }
            
            return {
                'status': 'success',
                'metrics': metrics
            }
            
        except Exception as e:
            self.logger.error(f"Error getting basic metrics: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def get_etcd_cluster_table_status(self) -> Dict[str, Any]:
        """Get etcd cluster status in table format (equivalent to the exact command you specified)"""
        try:
            # Get etcd pod name
            pod_result = await self._get_etcd_pod_name()
            if pod_result['status'] != 'success':
                return pod_result
            
            pod_name = pod_result['pod_name']
            
            # Execute the exact command you specified
            status_result = await self._execute_etcd_command(
                "etcdctl endpoint status -w table --cluster",
                pod_name
            )
            
            if status_result['status'] != 'success':
                return status_result
            
            # Parse the table output
            output = status_result.get('output', '').strip()
            lines = output.split('\n')
            
            table_data = []
            headers = []
            
            # Extract headers and data
            for line in lines:
                line = line.strip()
                if line and '|' in line:
                    if 'ENDPOINT' in line.upper():
                        # This is the header line
                        headers = [h.strip() for h in line.split('|') if h.strip()]
                    elif not line.startswith('+') and 'ENDPOINT' not in line.upper():
                        # This is a data line
                        row = [r.strip() for r in line.split('|') if r.strip()]
                        if row:  # Only add non-empty rows
                            table_data.append(row)
            
            # If no headers were found, use default ones
            if not headers:
                headers = ['ENDPOINT', 'ID', 'VERSION', 'DB SIZE', 'IS LEADER', 'RAFT TERM', 'RAFT INDEX']
            
            return {
                'status': 'success',
                'command_used': f"oc rsh -n {self.etcd_namespace} -c etcd {pod_name} sh -c 'unset ETCDCTL_ENDPOINTS; etcdctl endpoint status -w table --cluster'",
                'etcd_pod': pod_name,
                'table_format': {
                    'headers': headers,
                    'rows': table_data
                },
                'raw_output': output,
                'summary': {
                    'total_endpoints': len(table_data),
                    'leader_count': sum(1 for row in table_data if len(row) > 4 and row[4].lower() == 'true')
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting table status: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }

    async def quick_status_check(self) -> Dict[str, Any]:
        """Quick status check using the exact command format you provided"""
        return await self.get_etcd_cluster_table_status()

    async def _collect_etcd_metrics(self) -> Dict[str, Any]:
        """Collect selected etcd Prometheus metrics defined in metrics-etcd.yml"""
        try:
            if not self.ocp_auth:
                return {'status': 'error', 'error': 'ocp_auth not initialized'}

            prom_cfg = self.ocp_auth.get_prometheus_config()
            if not prom_cfg or not prom_cfg.get('url'):
                return {'status': 'error', 'error': 'Prometheus config missing'}

            # Choose a representative etcd category (general_info) for cluster status
            metrics = self.config.get_metrics_by_category('general_info')
            if not metrics:
                return {'status': 'no_metrics', 'metrics': {}}

            results: Dict[str, Any] = {}

            async with PrometheusBaseQuery(prom_cfg.get('url'), prom_cfg.get('token')) as prom_client:
                # Test connection
                if not await prom_client.test_connection():
                    return {'status': 'error', 'error': 'Prometheus connection failed'}

                # Collect each metric
                for metric in metrics:
                    name = metric.get('name')
                    expr = metric.get('expr')
                    unit = metric.get('unit')
                    title = metric.get('title', name)

                    try:
                        query_result = await self._query_with_stats(prom_client, expr, self.duration)
                        if query_result.get('status') != 'success':
                            results[name] = {
                                'status': 'error',
                                'error': query_result.get('error'),
                                'title': title,
                                'unit': unit,
                            }
                            continue

                        # Summarize by node when instance label exists
                        node_stats: Dict[str, Dict[str, Any]] = {}
                        for series in query_result.get('series_data', []):
                            labels = series.get('labels', {})
                            stats = series.get('statistics', {})
                            instance = labels.get('instance') or labels.get('node') or labels.get('kubernetes_node')
                            node_name = (instance.split(':')[0] if instance else labels.get('pod') or 'unknown')

                            if node_name not in node_stats:
                                node_stats[node_name] = {'avg': 0, 'max': 0, 'count': 0}
                            if 'avg' in stats:
                                node_stats[node_name]['avg'] += stats['avg']
                                node_stats[node_name]['count'] += 1
                            if 'max' in stats:
                                node_stats[node_name]['max'] = max(node_stats[node_name]['max'], stats['max'])

                        # finalize avg
                        for n in list(node_stats.keys()):
                            if node_stats[n]['count'] > 0:
                                node_stats[n]['avg'] = node_stats[n]['avg'] / node_stats[n]['count']
                            node_stats[n].pop('count', None)

                        results[name] = {
                            'status': 'success',
                            'title': title,
                            'unit': unit,
                            'overall_stats': query_result.get('overall_statistics', {}),
                            'nodes': node_stats
                        }
                    except Exception as e:
                        results[name] = {'status': 'error', 'error': str(e)}

            return {
                'status': 'success',
                'duration': self.duration,
                'category': 'general_info',
                'metrics': results
            }
        except Exception as e:
            self.logger.error(f"Error collecting etcd metrics: {e}")
            return {'status': 'error', 'error': str(e)}

    async def _query_with_stats(self, prom_client: PrometheusBaseQuery, query: str, duration: str) -> Dict[str, Any]:
        """Execute a range query and compute basic per-series and overall stats."""
        try:
            start, end = prom_client.get_time_range_from_duration(duration)
            data = await prom_client.query_range(query, start, end, step='15s')

            series_data: List[Dict[str, Any]] = []
            all_values: List[float] = []

            for item in data.get('result', []) if isinstance(data, dict) else []:
                metric_labels = item.get('metric', {})
                values = []
                for ts, val in item.get('values', []) or []:
                    try:
                        v = float(val)
                    except (ValueError, TypeError):
                        continue
                    if v != float('inf') and v != float('-inf'):
                        values.append(v)

                stats: Dict[str, Any] = {}
                if values:
                    avg_v = sum(values) / len(values)
                    max_v = max(values)
                    min_v = min(values)
                    stats = {
                        'avg': avg_v,
                        'max': max_v,
                        'min': min_v,
                        'count': len(values),
                        'latest': values[-1]
                    }
                    all_values.extend(values)

                series_data.append({'labels': metric_labels, 'statistics': stats})

            overall_statistics: Dict[str, Any] = {}
            if all_values:
                overall_statistics = {
                    'avg': sum(all_values) / len(all_values),
                    'max': max(all_values),
                    'min': min(all_values),
                    'count': len(all_values)
                }

            return {
                'status': 'success',
                'series_data': series_data,
                'overall_statistics': overall_statistics
            }
        except Exception as e:
            return {'status': 'error', 'error': str(e)}