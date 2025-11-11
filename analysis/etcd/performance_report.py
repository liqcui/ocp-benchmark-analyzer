"""
etcd Analyzer Performance Report Module
Comprehensive performance analysis and reporting for etcd clusters with node usage
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import pytz
from analysis.utils.analysis_utility import etcdAnalyzerUtility


class etcdReportAnalyzer:
    """Performance report analyzer for etcd cluster metrics"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.utility = etcdAnalyzerUtility()
        self.timezone = pytz.UTC
        
        # Performance thresholds (based on etcd best practices)
        self.thresholds = {
            'wal_fsync_p99_ms': 10.0,
            'backend_commit_p99_ms': 25.0,
            'cpu_usage_warning': 70.0,
            'cpu_usage_critical': 85.0,
            'memory_usage_warning': 70.0,
            'memory_usage_critical': 85.0,
            'peer_latency_warning_ms': 50.0,
            'peer_latency_critical_ms': 100.0,
            'network_utilization_warning': 70.0,
            'network_utilization_critical': 85.0,
        }

    def analyze_performance_metrics(self, metrics_data: Dict[str, Any], test_id: str, 
                                   node_usage_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Analyze comprehensive performance metrics including node usage"""
        try:
            analysis_results = {
                'test_id': test_id,
                'timestamp': self.utility.format_timestamp(),
                'duration': metrics_data.get('duration', '1h'),
                'status': 'success',
                'critical_metrics_analysis': {},
                'performance_summary': {},
                'node_usage_analysis': {},
                'baseline_comparison': {},
                'recommendations': [],
                'alerts': [],
                'metric_tables': {}
            }
            
            data = metrics_data.get('data', {})
            
            # Analyze critical metrics (WAL fsync and backend commit)
            analysis_results['critical_metrics_analysis'] = self._analyze_critical_metrics(data)
            
            # Analyze supporting metrics (CPU, memory, network, disk)
            analysis_results['performance_summary'] = self._analyze_supporting_metrics(data)
            
            # Analyze node usage if provided
            if node_usage_data and node_usage_data.get('status') == 'success':
                analysis_results['node_usage_analysis'] = self._analyze_node_usage(node_usage_data)
                self.logger.info("Node usage analysis completed")
            
            # Generate baseline comparison
            analysis_results['baseline_comparison'] = self._generate_baseline_comparison(
                data, node_usage_data
            )
            
            # Generate recommendations based on all analyses
            analysis_results['recommendations'] = self._generate_recommendations(
                analysis_results['critical_metrics_analysis'],
                analysis_results['performance_summary'],
                analysis_results['node_usage_analysis']
            )
            
            # Generate alerts for critical issues
            analysis_results['alerts'] = self._generate_alerts(
                analysis_results['critical_metrics_analysis'],
                analysis_results['performance_summary'],
                analysis_results['node_usage_analysis']
            )
            
            # Create formatted metric tables
            analysis_results['metric_tables'] = self._create_metric_tables(data, node_usage_data)
            
            return analysis_results
            
        except Exception as e:
            self.logger.error(f"Error analyzing performance metrics: {e}")
            return {
                'test_id': test_id,
                'timestamp': self.utility.format_timestamp(),
                'status': 'error',
                'error': str(e)
            }
    
    def _analyze_node_usage(self, node_usage_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze node usage metrics"""
        analysis = {
            'cpu_analysis': {},
            'memory_analysis': {},
            'cgroup_analysis': {},
            'health_status': 'unknown',
            'resource_issues': []
        }
        
        try:
            usage_data = node_usage_data.get('data', {})
            metrics = usage_data.get('metrics', {})
            node_capacities = usage_data.get('node_capacities', {})
            
            # Analyze CPU usage
            cpu_usage = metrics.get('cpu_usage', {})
            if cpu_usage.get('status') == 'success':
                analysis['cpu_analysis'] = self._analyze_node_cpu_usage(
                    cpu_usage.get('nodes', {})
                )
            
            # Analyze memory usage
            memory_used = metrics.get('memory_used', {})
            if memory_used.get('status') == 'success':
                analysis['memory_analysis'] = self._analyze_node_memory_usage(
                    memory_used.get('nodes', {}),
                    node_capacities
                )
            
            # Analyze cgroup usage
            cgroup_cpu = metrics.get('cgroup_cpu_usage', {})
            cgroup_rss = metrics.get('cgroup_rss_usage', {})
            if cgroup_cpu.get('status') == 'success' or cgroup_rss.get('status') == 'success':
                analysis['cgroup_analysis'] = self._analyze_cgroup_usage(
                    cgroup_cpu.get('nodes', {}),
                    cgroup_rss.get('nodes', {})
                )
            
            # Determine overall health
            analysis['health_status'] = self._determine_node_health(analysis)
            
            # Identify resource issues
            analysis['resource_issues'] = self._identify_resource_issues(analysis)
            
        except Exception as e:
            self.logger.error(f"Error analyzing node usage: {e}")
            analysis['error'] = str(e)
        
        return analysis
    
    def _analyze_node_cpu_usage(self, nodes_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze node CPU usage"""
        cpu_analysis = {
            'nodes': {},
            'cluster_summary': {},
            'issues': []
        }
        
        try:
            total_nodes = len(nodes_data)
            cluster_utilization = []
            high_usage_nodes = 0
            critical_usage_nodes = 0
            
            for node_name, node_data in nodes_data.items():
                total = node_data.get('total', {})
                modes = node_data.get('modes', {})
                
                # Calculate actual CPU utilization
                # CPU reported as total % across all cores
                idle_max = modes.get('idle', {}).get('max', 0)
                estimated_cores = int(idle_max / 100) if idle_max > 0 else 40
                
                raw_avg = total.get('avg', 0)
                raw_max = total.get('max', 0)
                
                avg_utilization = (raw_avg / estimated_cores) if estimated_cores > 0 else 0
                max_utilization = (raw_max / estimated_cores) if estimated_cores > 0 else 0
                
                # Determine status
                status = 'good'
                if max_utilization > self.thresholds['cpu_usage_critical']:
                    status = 'critical'
                    critical_usage_nodes += 1
                elif avg_utilization > self.thresholds['cpu_usage_warning']:
                    status = 'warning'
                    high_usage_nodes += 1
                
                cpu_analysis['nodes'][node_name] = {
                    'estimated_cores': estimated_cores,
                    'avg_utilization_percent': round(avg_utilization, 2),
                    'max_utilization_percent': round(max_utilization, 2),
                    'status': status,
                    'modes': modes
                }
                
                cluster_utilization.append(avg_utilization)
                
                # Track issues
                if status in ['warning', 'critical']:
                    cpu_analysis['issues'].append({
                        'node': node_name,
                        'severity': status,
                        'avg_utilization': round(avg_utilization, 2),
                        'max_utilization': round(max_utilization, 2)
                    })
            
            # Cluster summary
            if cluster_utilization:
                cpu_analysis['cluster_summary'] = {
                    'avg_utilization_percent': round(sum(cluster_utilization) / len(cluster_utilization), 2),
                    'total_nodes': total_nodes,
                    'high_usage_nodes': high_usage_nodes,
                    'critical_usage_nodes': critical_usage_nodes
                }
        
        except Exception as e:
            self.logger.error(f"Error analyzing node CPU usage: {e}")
            cpu_analysis['error'] = str(e)
        
        return cpu_analysis
    
    def _analyze_node_memory_usage(self, nodes_data: Dict[str, Any], 
                                   node_capacities: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze node memory usage"""
        memory_analysis = {
            'nodes': {},
            'cluster_summary': {},
            'issues': []
        }
        
        try:
            total_nodes = len(nodes_data)
            cluster_utilization = []
            high_usage_nodes = 0
            critical_usage_nodes = 0
            
            for node_name, node_data in nodes_data.items():
                avg_used = node_data.get('avg', 0)
                max_used = node_data.get('max', 0)
                total_capacity = node_data.get('total_capacity', 0)
                
                # Calculate utilization percentage
                avg_percent = (avg_used / total_capacity * 100) if total_capacity > 0 else 0
                max_percent = (max_used / total_capacity * 100) if total_capacity > 0 else 0
                
                # Determine status
                status = 'good'
                if max_percent > self.thresholds['memory_usage_critical']:
                    status = 'critical'
                    critical_usage_nodes += 1
                elif avg_percent > self.thresholds['memory_usage_warning']:
                    status = 'warning'
                    high_usage_nodes += 1
                
                memory_analysis['nodes'][node_name] = {
                    'total_capacity_gb': total_capacity,
                    'avg_used_gb': round(avg_used, 2),
                    'max_used_gb': round(max_used, 2),
                    'avg_utilization_percent': round(avg_percent, 2),
                    'max_utilization_percent': round(max_percent, 2),
                    'status': status
                }
                
                cluster_utilization.append(avg_percent)
                
                # Track issues
                if status in ['warning', 'critical']:
                    memory_analysis['issues'].append({
                        'node': node_name,
                        'severity': status,
                        'avg_utilization': round(avg_percent, 2),
                        'max_utilization': round(max_percent, 2)
                    })
            
            # Cluster summary
            if cluster_utilization:
                memory_analysis['cluster_summary'] = {
                    'avg_utilization_percent': round(sum(cluster_utilization) / len(cluster_utilization), 2),
                    'total_nodes': total_nodes,
                    'high_usage_nodes': high_usage_nodes,
                    'critical_usage_nodes': critical_usage_nodes
                }
        
        except Exception as e:
            self.logger.error(f"Error analyzing node memory usage: {e}")
            memory_analysis['error'] = str(e)
        
        return memory_analysis
    
    def _analyze_cgroup_usage(self, cgroup_cpu_nodes: Dict[str, Any], 
                             cgroup_rss_nodes: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze cgroup CPU and memory usage"""
        cgroup_analysis = {
            'nodes': {},
            'top_consumers': {
                'cpu': [],
                'memory': []
            }
        }
        
        try:
            # Analyze CPU usage by cgroup
            for node_name, node_data in cgroup_cpu_nodes.items():
                if node_name not in cgroup_analysis['nodes']:
                    cgroup_analysis['nodes'][node_name] = {}
                
                cgroups = node_data.get('cgroups', {})
                cgroup_analysis['nodes'][node_name]['cpu'] = {}
                
                for cgroup_name, cgroup_data in cgroups.items():
                    avg_cpu = cgroup_data.get('avg', 0)
                    max_cpu = cgroup_data.get('max', 0)
                    
                    cgroup_analysis['nodes'][node_name]['cpu'][cgroup_name] = {
                        'avg_percent': round(avg_cpu, 2),
                        'max_percent': round(max_cpu, 2)
                    }
                    
                    # Track top consumers
                    cgroup_analysis['top_consumers']['cpu'].append({
                        'node': node_name,
                        'cgroup': cgroup_name,
                        'avg_percent': round(avg_cpu, 2)
                    })
            
            # Analyze memory usage by cgroup
            for node_name, node_data in cgroup_rss_nodes.items():
                if node_name not in cgroup_analysis['nodes']:
                    cgroup_analysis['nodes'][node_name] = {}
                
                cgroups = node_data.get('cgroups', {})
                cgroup_analysis['nodes'][node_name]['memory'] = {}
                
                for cgroup_name, cgroup_data in cgroups.items():
                    avg_mem = cgroup_data.get('avg', 0)
                    max_mem = cgroup_data.get('max', 0)
                    
                    cgroup_analysis['nodes'][node_name]['memory'][cgroup_name] = {
                        'avg_gb': round(avg_mem, 2),
                        'max_gb': round(max_mem, 2)
                    }
                    
                    # Track top consumers
                    cgroup_analysis['top_consumers']['memory'].append({
                        'node': node_name,
                        'cgroup': cgroup_name,
                        'avg_gb': round(avg_mem, 2)
                    })
            
            # Sort top consumers
            cgroup_analysis['top_consumers']['cpu'].sort(
                key=lambda x: x['avg_percent'], reverse=True
            )
            cgroup_analysis['top_consumers']['memory'].sort(
                key=lambda x: x['avg_gb'], reverse=True
            )
            
            # Keep only top 10
            cgroup_analysis['top_consumers']['cpu'] = cgroup_analysis['top_consumers']['cpu'][:10]
            cgroup_analysis['top_consumers']['memory'] = cgroup_analysis['top_consumers']['memory'][:10]
        
        except Exception as e:
            self.logger.error(f"Error analyzing cgroup usage: {e}")
            cgroup_analysis['error'] = str(e)
        
        return cgroup_analysis
    
    def _determine_node_health(self, analysis: Dict[str, Any]) -> str:
        """Determine overall node health status"""
        try:
            cpu_issues = len(analysis.get('cpu_analysis', {}).get('issues', []))
            memory_issues = len(analysis.get('memory_analysis', {}).get('issues', []))
            
            cpu_summary = analysis.get('cpu_analysis', {}).get('cluster_summary', {})
            memory_summary = analysis.get('memory_analysis', {}).get('cluster_summary', {})
            
            critical_cpu = cpu_summary.get('critical_usage_nodes', 0)
            critical_memory = memory_summary.get('critical_usage_nodes', 0)
            
            if critical_cpu > 0 or critical_memory > 0:
                return 'critical'
            elif cpu_issues > 0 or memory_issues > 0:
                return 'warning'
            else:
                return 'good'
        
        except Exception as e:
            self.logger.error(f"Error determining node health: {e}")
            return 'unknown'
    
    def _identify_resource_issues(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify resource issues from node analysis"""
        issues = []
        
        try:
            # CPU issues
            cpu_issues = analysis.get('cpu_analysis', {}).get('issues', [])
            for issue in cpu_issues:
                issues.append({
                    'type': 'cpu',
                    'node': issue['node'],
                    'severity': issue['severity'],
                    'description': f"Node {issue['node']} CPU utilization at {issue['avg_utilization']}% avg, {issue['max_utilization']}% max"
                })
            
            # Memory issues
            memory_issues = analysis.get('memory_analysis', {}).get('issues', [])
            for issue in memory_issues:
                issues.append({
                    'type': 'memory',
                    'node': issue['node'],
                    'severity': issue['severity'],
                    'description': f"Node {issue['node']} memory utilization at {issue['avg_utilization']}% avg, {issue['max_utilization']}% max"
                })
        
        except Exception as e:
            self.logger.error(f"Error identifying resource issues: {e}")
        
        return issues
    
    def _analyze_critical_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze critical performance metrics (WAL fsync and backend commit)"""
        critical_analysis = {
            'wal_fsync_analysis': {},
            'backend_commit_analysis': {},
            'overall_disk_health': 'unknown'
        }
        
        try:
            # Analyze WAL fsync metrics
            wal_data = data.get('wal_fsync_data', [])
            wal_p99_metrics = [m for m in wal_data if 'p99' in m.get('metric_name', '')]
            
            if wal_p99_metrics:
                wal_analysis = self._analyze_latency_metrics(
                    wal_p99_metrics, 
                    'wal_fsync_p99', 
                    self.thresholds['wal_fsync_p99_ms']
                )
                critical_analysis['wal_fsync_analysis'] = wal_analysis
            
            # Analyze backend commit metrics
            backend_data = data.get('backend_commit_data', [])
            backend_p99_metrics = [m for m in backend_data if 'p99' in m.get('metric_name', '')]
            
            if backend_p99_metrics:
                backend_analysis = self._analyze_latency_metrics(
                    backend_p99_metrics, 
                    'backend_commit_p99', 
                    self.thresholds['backend_commit_p99_ms']
                )
                critical_analysis['backend_commit_analysis'] = backend_analysis
            
            # Determine overall disk health
            critical_analysis['overall_disk_health'] = self._determine_disk_health(
                critical_analysis['wal_fsync_analysis'],
                critical_analysis['backend_commit_analysis']
            )
            
        except Exception as e:
            self.logger.error(f"Error analyzing critical metrics: {e}")
            critical_analysis['error'] = str(e)
            
        return critical_analysis
    
    def _analyze_latency_metrics(self, metrics: List[Dict[str, Any]], metric_type: str, threshold_ms: float) -> Dict[str, Any]:
        """Analyze latency metrics against thresholds"""
        analysis = {
            'metric_type': metric_type,
            'threshold_ms': threshold_ms,
            'pod_results': [],
            'cluster_summary': {},
            'health_status': 'unknown'
        }
        
        try:
            avg_latencies = []
            max_latencies = []
            issues_found = 0
            
            for metric in metrics:
                pod_name = metric.get('pod_name', 'unknown')
                avg_value = metric.get('avg', 0)
                max_value = metric.get('max', 0)
                unit = metric.get('unit', 'seconds')
                
                # Convert to milliseconds if needed
                if unit == 'seconds':
                    avg_ms = avg_value * 1000
                    max_ms = max_value * 1000
                else:
                    avg_ms = avg_value
                    max_ms = max_value
                
                # Analyze against threshold
                avg_exceeds = avg_ms > threshold_ms
                max_exceeds = max_ms > threshold_ms
                
                if avg_exceeds or max_exceeds:
                    issues_found += 1
                
                pod_result = {
                    'pod_name': pod_name,
                    'avg_ms': round(avg_ms, 3),
                    'max_ms': round(max_ms, 3),
                    'avg_exceeds_threshold': avg_exceeds,
                    'max_exceeds_threshold': max_exceeds,
                    'status': 'critical' if avg_exceeds else ('warning' if max_exceeds else 'good')
                }
                
                analysis['pod_results'].append(pod_result)
                avg_latencies.append(avg_ms)
                max_latencies.append(max_ms)
            
            # Cluster summary
            if avg_latencies:
                analysis['cluster_summary'] = {
                    'avg_latency_ms': round(sum(avg_latencies) / len(avg_latencies), 3),
                    'max_latency_ms': round(max(max_latencies), 3),
                    'pods_with_issues': issues_found,
                    'total_pods': len(metrics),
                    'threshold_exceeded': issues_found > 0
                }
                
                # Determine health status
                if issues_found == 0:
                    analysis['health_status'] = 'excellent'
                elif issues_found <= len(metrics) / 2:
                    analysis['health_status'] = 'warning'
                else:
                    analysis['health_status'] = 'critical'
            
        except Exception as e:
            self.logger.error(f"Error analyzing latency metrics: {e}")
            analysis['error'] = str(e)
            
        return analysis
    
    def _analyze_supporting_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze supporting performance metrics (CPU, memory, network, disk I/O)"""
        supporting_analysis = {
            'cpu_analysis': {},
            'memory_analysis': {},
            'network_analysis': {},
            'disk_io_analysis': {}
        }
        
        try:
            # Analyze CPU usage
            general_data = data.get('general_info_data', [])
            cpu_metrics = [m for m in general_data if 'cpu_usage' in m.get('metric_name', '')]
            supporting_analysis['cpu_analysis'] = self._analyze_resource_metrics(
                cpu_metrics, 'cpu', self.thresholds['cpu_usage_warning'], self.thresholds['cpu_usage_critical']
            )
            
            # Analyze memory usage
            memory_metrics = [m for m in general_data if 'memory_usage' in m.get('metric_name', '')]
            supporting_analysis['memory_analysis'] = self._analyze_resource_metrics(
                memory_metrics, 'memory', self.thresholds['memory_usage_warning'], self.thresholds['memory_usage_critical']
            )
            
            # Analyze network performance
            network_data = data.get('network_data', {})
            supporting_analysis['network_analysis'] = self._analyze_network_metrics(network_data)
            
            # Analyze disk I/O
            disk_data = data.get('disk_io_data', [])
            supporting_analysis['disk_io_analysis'] = self._analyze_disk_io_metrics(disk_data)
            
        except Exception as e:
            self.logger.error(f"Error analyzing supporting metrics: {e}")
            supporting_analysis['error'] = str(e)
            
        return supporting_analysis
    
    def _analyze_resource_metrics(self, metrics: List[Dict[str, Any]], resource_type: str, 
                                warning_threshold: float, critical_threshold: float) -> Dict[str, Any]:
        """Analyze resource utilization metrics (CPU/Memory)"""
        analysis = {
            'resource_type': resource_type,
            'warning_threshold': warning_threshold,
            'critical_threshold': critical_threshold,
            'pod_results': [],
            'cluster_summary': {},
            'health_status': 'unknown'
        }
        
        try:
            if not metrics:
                analysis['health_status'] = 'no_data'
                return analysis
            
            usage_values = []
            critical_pods = 0
            warning_pods = 0
            
            for metric in metrics:
                pod_name = metric.get('pod_name', 'unknown')
                avg_usage = metric.get('avg', 0)
                max_usage = metric.get('max', 0)
                unit = metric.get('unit', 'percent')
                
                # For memory metrics, convert to percentage of node capacity
                if resource_type == 'memory' and unit == 'MB':
                    node_memory_gb = 16.0
                    avg_percent = (avg_usage / 1024) / node_memory_gb * 100
                    max_percent = (max_usage / 1024) / node_memory_gb * 100
                else:
                    avg_percent = avg_usage
                    max_percent = max_usage
                
                # Determine status
                status = 'good'
                if max_percent >= critical_threshold:
                    status = 'critical'
                    critical_pods += 1
                elif max_percent >= warning_threshold or avg_percent >= warning_threshold:
                    status = 'warning'
                    warning_pods += 1
                
                pod_result = {
                    'pod_name': pod_name,
                    'avg_usage': round(avg_percent, 2),
                    'max_usage': round(max_percent, 2),
                    'unit': 'percent' if resource_type == 'memory' and unit == 'MB' else unit,
                    'status': status
                }
                
                analysis['pod_results'].append(pod_result)
                usage_values.append(avg_percent)
            
            # Cluster summary
            analysis['cluster_summary'] = {
                'avg_usage': round(sum(usage_values) / len(usage_values), 2),
                'max_usage': round(max([m['max_usage'] for m in analysis['pod_results']]), 2),
                'critical_pods': critical_pods,
                'warning_pods': warning_pods,
                'total_pods': len(metrics)
            }
            
            # Health status
            if critical_pods > 0:
                analysis['health_status'] = 'critical'
            elif warning_pods > 0:
                analysis['health_status'] = 'warning'
            else:
                analysis['health_status'] = 'good'
                
        except Exception as e:
            self.logger.error(f"Error analyzing {resource_type} metrics: {e}")
            analysis['error'] = str(e)
            
        return analysis
    """
    Continuation of etcd_analyzer_performance_report.py
    Includes remaining analysis methods
    """

    def _analyze_network_metrics(self, network_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze network performance metrics"""
        analysis = {
            'peer_latency_analysis': {},
            'network_utilization_analysis': {},
            'packet_drop_analysis': {},
            'health_status': 'unknown'
        }
        
        try:
            # Analyze peer-to-peer latency
            pod_metrics = network_data.get('pod_metrics', [])
            peer_latency_metrics = [m for m in pod_metrics if 'peer2peer_latency' in m.get('metric_name', '')]
            
            if peer_latency_metrics:
                analysis['peer_latency_analysis'] = self._analyze_latency_metrics(
                    peer_latency_metrics, 
                    'peer_latency', 
                    self.thresholds['peer_latency_critical_ms']
                )
            
            # Analyze network utilization
            node_metrics = network_data.get('node_metrics', [])
            utilization_metrics = [m for m in node_metrics if 'utilization' in m.get('metric_name', '')]
            analysis['network_utilization_analysis'] = self._analyze_network_utilization(utilization_metrics)
            
            # Analyze packet drops
            drop_metrics = [m for m in node_metrics if 'drop' in m.get('metric_name', '')]
            analysis['packet_drop_analysis'] = self._analyze_packet_drops(drop_metrics)
            
            # Overall network health
            latency_health = analysis['peer_latency_analysis'].get('health_status', 'unknown')
            utilization_health = analysis['network_utilization_analysis'].get('health_status', 'unknown')
            drop_health = analysis['packet_drop_analysis'].get('health_status', 'unknown')
            
            health_scores = {'excellent': 4, 'good': 3, 'warning': 2, 'critical': 1, 'unknown': 0}
            avg_score = sum(health_scores.get(h, 0) for h in [latency_health, utilization_health, drop_health]) / 3
            
            if avg_score >= 3.5:
                analysis['health_status'] = 'excellent'
            elif avg_score >= 2.5:
                analysis['health_status'] = 'good'
            elif avg_score >= 1.5:
                analysis['health_status'] = 'warning'
            else:
                analysis['health_status'] = 'critical'
            
        except Exception as e:
            self.logger.error(f"Error analyzing network metrics: {e}")
            analysis['error'] = str(e)
            
        return analysis
    
    def _analyze_network_utilization(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze network utilization metrics"""
        analysis = {
            'node_results': [],
            'health_status': 'unknown'
        }
        
        try:
            if not metrics:
                analysis['health_status'] = 'no_data'
                return analysis
                
            high_util_nodes = 0
            
            for metric in metrics:
                node_name = metric.get('node_name', 'unknown')
                avg_util = metric.get('avg', 0)
                max_util = metric.get('max', 0)
                
                if metric.get('unit') == 'bits_per_second':
                    avg_percent = (avg_util / 1000000000) * 100
                    max_percent = (max_util / 1000000000) * 100
                else:
                    avg_percent = avg_util
                    max_percent = max_util
                
                status = 'good'
                if max_percent > self.thresholds['network_utilization_critical']:
                    status = 'critical'
                    high_util_nodes += 1
                elif avg_percent > self.thresholds['network_utilization_warning']:
                    status = 'warning'
                    high_util_nodes += 1
                
                analysis['node_results'].append({
                    'node_name': node_name,
                    'avg_utilization_percent': round(avg_percent, 2),
                    'max_utilization_percent': round(max_percent, 2),
                    'status': status
                })
            
            if high_util_nodes == 0:
                analysis['health_status'] = 'good'
            elif high_util_nodes <= len(metrics) / 2:
                analysis['health_status'] = 'warning'
            else:
                analysis['health_status'] = 'critical'
                
        except Exception as e:
            self.logger.error(f"Error analyzing network utilization: {e}")
            analysis['error'] = str(e)
            
        return analysis
    
    def _analyze_packet_drops(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze packet drop metrics"""
        analysis = {
            'node_results': [],
            'health_status': 'unknown'
        }
        
        try:
            if not metrics:
                analysis['health_status'] = 'no_data'
                return analysis
                
            nodes_with_drops = 0
            
            for metric in metrics:
                node_name = metric.get('node_name', 'unknown')
                avg_drops = metric.get('avg', 0)
                max_drops = metric.get('max', 0)
                
                status = 'good'
                if avg_drops > 0 or max_drops > 0:
                    status = 'warning' if avg_drops < 1 else 'critical'
                    nodes_with_drops += 1
                
                analysis['node_results'].append({
                    'node_name': node_name,
                    'avg_drops_per_sec': round(avg_drops, 6),
                    'max_drops_per_sec': round(max_drops, 6),
                    'status': status
                })
            
            if nodes_with_drops == 0:
                analysis['health_status'] = 'good'
            else:
                analysis['health_status'] = 'warning'
                
        except Exception as e:
            self.logger.error(f"Error analyzing packet drops: {e}")
            analysis['error'] = str(e)
            
        return analysis
    
    def _analyze_disk_io_metrics(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze disk I/O performance metrics"""
        analysis = {
            'throughput_analysis': [],
            'iops_analysis': [],
            'health_status': 'unknown'
        }
        
        try:
            if not metrics:
                analysis['health_status'] = 'no_data'
                return analysis
            
            throughput_metrics = [m for m in metrics if 'throughput' in m.get('metric_name', '')]
            iops_metrics = [m for m in metrics if 'iops' in m.get('metric_name', '')]
            
            for metric in throughput_metrics:
                node_name = metric.get('node_name', 'unknown')
                avg_throughput = metric.get('avg', 0)
                max_throughput = metric.get('max', 0)
                unit = metric.get('unit', 'bytes_per_second')
                
                if unit == 'bytes_per_second':
                    avg_mb_s = avg_throughput / (1024 * 1024)
                    max_mb_s = max_throughput / (1024 * 1024)
                else:
                    avg_mb_s = avg_throughput
                    max_mb_s = max_throughput
                
                analysis['throughput_analysis'].append({
                    'node_name': node_name,
                    'metric_type': metric.get('metric_name', 'unknown'),
                    'avg_mb_per_sec': round(avg_mb_s, 2),
                    'max_mb_per_sec': round(max_mb_s, 2),
                    'devices': metric.get('devices', [])
                })
            
            for metric in iops_metrics:
                node_name = metric.get('node_name', 'unknown')
                avg_iops = metric.get('avg', 0)
                max_iops = metric.get('max', 0)
                
                analysis['iops_analysis'].append({
                    'node_name': node_name,
                    'metric_type': metric.get('metric_name', 'unknown'),
                    'avg_iops': round(avg_iops, 2),
                    'max_iops': round(max_iops, 2),
                    'devices': metric.get('devices', [])
                })
            
            analysis['health_status'] = 'good' if (throughput_metrics or iops_metrics) else 'no_data'
            
        except Exception as e:
            self.logger.error(f"Error analyzing disk I/O metrics: {e}")
            analysis['error'] = str(e)
            
        return analysis
    
    def _determine_disk_health(self, wal_analysis: Dict[str, Any], backend_analysis: Dict[str, Any]) -> str:
        """Determine overall disk health based on WAL fsync and backend commit analysis"""
        try:
            wal_health = wal_analysis.get('health_status', 'unknown')
            backend_health = backend_analysis.get('health_status', 'unknown')
            
            health_priorities = {'critical': 4, 'warning': 3, 'good': 2, 'excellent': 1, 'unknown': 0}
            
            wal_priority = health_priorities.get(wal_health, 0)
            backend_priority = health_priorities.get(backend_health, 0)
            
            max_priority = max(wal_priority, backend_priority)
            
            for health, priority in health_priorities.items():
                if priority == max_priority:
                    return health
                    
            return 'unknown'
            
        except Exception as e:
            self.logger.error(f"Error determining disk health: {e}")
            return 'unknown'
    
    def _generate_baseline_comparison(self, data: Dict[str, Any], 
                                     node_usage_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Generate baseline comparison analysis including node usage"""
        comparison = {
            'benchmark_standards': {
                'wal_fsync_p99_target_ms': self.thresholds['wal_fsync_p99_ms'],
                'backend_commit_p99_target_ms': self.thresholds['backend_commit_p99_ms'],
                'cpu_usage_target_percent': 70.0,
                'memory_usage_target_percent': 70.0,
                'peer_latency_target_ms': self.thresholds['peer_latency_warning_ms'],
                'node_cpu_target_percent': 70.0,
                'node_memory_target_percent': 70.0
            },
            'current_vs_baseline': {},
            'performance_grade': 'unknown'
        }
        
        try:
            # Compare WAL fsync
            wal_data = data.get('wal_fsync_data', [])
            wal_p99_metrics = [m for m in wal_data if 'p99' in m.get('metric_name', '')]
            
            if wal_p99_metrics:
                avg_wal_latency = sum(m.get('avg', 0) * 1000 for m in wal_p99_metrics) / len(wal_p99_metrics)
                comparison['current_vs_baseline']['wal_fsync_p99_ms'] = {
                    'current': round(avg_wal_latency, 3),
                    'target': self.thresholds['wal_fsync_p99_ms'],
                    'within_target': avg_wal_latency <= self.thresholds['wal_fsync_p99_ms']
                }
            
            # Compare backend commit
            backend_data = data.get('backend_commit_data', [])
            backend_p99_metrics = [m for m in backend_data if 'p99' in m.get('metric_name', '')]
            
            if backend_p99_metrics:
                avg_backend_latency = sum(m.get('avg', 0) * 1000 for m in backend_p99_metrics) / len(backend_p99_metrics)
                comparison['current_vs_baseline']['backend_commit_p99_ms'] = {
                    'current': round(avg_backend_latency, 3),
                    'target': self.thresholds['backend_commit_p99_ms'],
                    'within_target': avg_backend_latency <= self.thresholds['backend_commit_p99_ms']
                }
            
            # Compare CPU usage (etcd pods)
            general_data = data.get('general_info_data', [])
            cpu_metrics = [m for m in general_data if 'cpu_usage' in m.get('metric_name', '')]
            
            if cpu_metrics:
                avg_cpu_usage = sum(m.get('avg', 0) for m in cpu_metrics) / len(cpu_metrics)
                comparison['current_vs_baseline']['etcd_cpu_usage_percent'] = {
                    'current': round(avg_cpu_usage, 2),
                    'target': 70.0,
                    'within_target': avg_cpu_usage <= 70.0
                }
            
            # Compare node usage if available
            if node_usage_data and node_usage_data.get('status') == 'success':
                usage_data = node_usage_data.get('data', {})
                metrics = usage_data.get('metrics', {})
                
                # Node CPU usage
                cpu_usage = metrics.get('cpu_usage', {})
                if cpu_usage.get('status') == 'success':
                    nodes = cpu_usage.get('nodes', {})
                    cpu_utilizations = []
                    
                    for node_name, node_data in nodes.items():
                        total = node_data.get('total', {})
                        modes = node_data.get('modes', {})
                        idle_max = modes.get('idle', {}).get('max', 0)
                        estimated_cores = int(idle_max / 100) if idle_max > 0 else 40
                        
                        raw_avg = total.get('avg', 0)
                        avg_utilization = (raw_avg / estimated_cores) if estimated_cores > 0 else 0
                        cpu_utilizations.append(avg_utilization)
                    
                    if cpu_utilizations:
                        avg_node_cpu = sum(cpu_utilizations) / len(cpu_utilizations)
                        comparison['current_vs_baseline']['node_cpu_usage_percent'] = {
                            'current': round(avg_node_cpu, 2),
                            'target': 70.0,
                            'within_target': avg_node_cpu <= 70.0
                        }
                
                # Node memory usage
                memory_used = metrics.get('memory_used', {})
                if memory_used.get('status') == 'success':
                    nodes = memory_used.get('nodes', {})
                    memory_utilizations = []
                    
                    for node_name, node_data in nodes.items():
                        avg_used = node_data.get('avg', 0)
                        total_capacity = node_data.get('total_capacity', 0)
                        
                        if total_capacity > 0:
                            avg_percent = (avg_used / total_capacity) * 100
                            memory_utilizations.append(avg_percent)
                    
                    if memory_utilizations:
                        avg_node_memory = sum(memory_utilizations) / len(memory_utilizations)
                        comparison['current_vs_baseline']['node_memory_usage_percent'] = {
                            'current': round(avg_node_memory, 2),
                            'target': 70.0,
                            'within_target': avg_node_memory <= 70.0
                        }
            
            # Calculate performance grade
            comparison['performance_grade'] = self._calculate_performance_grade(comparison['current_vs_baseline'])
            
        except Exception as e:
            self.logger.error(f"Error generating baseline comparison: {e}")
            comparison['error'] = str(e)
            
        return comparison
    
    def _calculate_performance_grade(self, baseline_comparison: Dict[str, Any]) -> str:
        """Calculate overall performance grade"""
        try:
            within_target_count = 0
            total_metrics = len(baseline_comparison)
            
            if total_metrics == 0:
                return 'insufficient_data'
            
            for metric, comparison in baseline_comparison.items():
                if comparison.get('within_target', False):
                    within_target_count += 1
            
            percentage = (within_target_count / total_metrics) * 100
            
            if percentage >= 90:
                return 'excellent'
            elif percentage >= 75:
                return 'good'
            elif percentage >= 50:
                return 'fair'
            else:
                return 'poor'
                
        except Exception as e:
            self.logger.error(f"Error calculating performance grade: {e}")
            return 'unknown'
    
    def _generate_recommendations(self, critical_analysis: Dict[str, Any], 
                                performance_analysis: Dict[str, Any],
                                node_usage_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate performance optimization recommendations including node usage"""
        recommendations = []
        
        try:
            # Check WAL fsync performance
            wal_analysis = critical_analysis.get('wal_fsync_analysis', {})
            if wal_analysis.get('health_status') in ['critical', 'warning']:
                recommendations.append({
                    'category': 'disk_performance',
                    'priority': 'high',
                    'issue': 'High WAL fsync latency detected',
                    'recommendation': 'Upgrade to high-performance NVMe SSDs with low latency',
                    'rationale': 'WAL fsync latency directly impacts etcd write performance'
                })
            
            # Check network performance
            network_analysis = performance_analysis.get('network_analysis', {})
            peer_latency_health = network_analysis.get('peer_latency_analysis', {}).get('health_status')
            if peer_latency_health in ['critical', 'warning']:
                recommendations.append({
                    'category': 'network_optimization',
                    'priority': 'high',
                    'issue': 'High peer-to-peer network latency',
                    'recommendation': 'Optimize network topology and reduce network hops between etcd members',
                    'rationale': 'High network latency affects cluster consensus and performance'
                })
            
            # Check node resource usage
            if node_usage_analysis:
                node_health = node_usage_analysis.get('health_status')
                resource_issues = node_usage_analysis.get('resource_issues', [])
                
                cpu_issues = [i for i in resource_issues if i['type'] == 'cpu']
                memory_issues = [i for i in resource_issues if i['type'] == 'memory']
                
                if cpu_issues:
                    recommendations.append({
                        'category': 'node_resources',
                        'priority': 'high' if node_health == 'critical' else 'medium',
                        'issue': f'High CPU utilization on {len(cpu_issues)} master node(s)',
                        'recommendation': 'Increase CPU allocation for master nodes or redistribute workload',
                        'rationale': 'High CPU utilization can impact etcd and control plane performance'
                    })
                
                if memory_issues:
                    recommendations.append({
                        'category': 'node_resources',
                        'priority': 'high' if node_health == 'critical' else 'medium',
                        'issue': f'High memory utilization on {len(memory_issues)} master node(s)',
                        'recommendation': 'Increase memory allocation for master nodes',
                        'rationale': 'Memory pressure can lead to performance degradation and instability'
                    })
            
            # General recommendations if no issues
            if not recommendations:
                recommendations.extend([
                    {
                        'category': 'monitoring',
                        'priority': 'low',
                        'issue': 'Ongoing performance monitoring',
                        'recommendation': 'Continue regular performance monitoring and analysis',
                        'rationale': 'No critical issues detected, maintain current monitoring practices'
                    },
                    {
                        'category': 'preventive_maintenance',
                        'priority': 'low',
                        'issue': 'Preventive maintenance',
                        'recommendation': 'Schedule regular database compaction and system maintenance',
                        'rationale': 'Preventive maintenance helps maintain optimal performance'
                    }
                ])
            
        except Exception as e:
            self.logger.error(f"Error generating recommendations: {e}")
            recommendations.append({
                'category': 'error',
                'priority': 'high',
                'issue': 'Failed to generate recommendations',
                'recommendation': f'Review analysis error: {str(e)}',
                'rationale': 'Error occurred during recommendation generation'
            })
            
        return recommendations
    
    def _generate_alerts(self, critical_analysis: Dict[str, Any], 
                        performance_analysis: Dict[str, Any],
                        node_usage_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate alerts for critical performance issues including node usage"""
        alerts = []
        
        try:
            # Critical disk performance alerts
            wal_health = critical_analysis.get('wal_fsync_analysis', {}).get('health_status')
            if wal_health == 'critical':
                alerts.append({
                    'severity': 'critical',
                    'category': 'disk_performance',
                    'message': 'WAL fsync latency exceeds critical threshold (>10ms)',
                    'impact': 'High write latency affecting cluster stability',
                    'action_required': 'Immediate storage performance investigation required'
                })
            
            backend_health = critical_analysis.get('backend_commit_analysis', {}).get('health_status')
            if backend_health == 'critical':
                alerts.append({
                    'severity': 'critical',
                    'category': 'disk_performance',
                    'message': 'Backend commit latency exceeds critical threshold (>25ms)',
                    'impact': 'Database write performance significantly degraded',
                    'action_required': 'Immediate storage optimization required'
                })
            
            # Node resource alerts
            if node_usage_analysis:
                node_health = node_usage_analysis.get('health_status')
                
                if node_health == 'critical':
                    resource_issues = node_usage_analysis.get('resource_issues', [])
                    critical_issues = [i for i in resource_issues if i['severity'] == 'critical']
                    
                    if critical_issues:
                        alerts.append({
                            'severity': 'critical',
                            'category': 'node_resources',
                            'message': f'Critical resource utilization on {len(critical_issues)} master node(s)',
                            'impact': 'Control plane and etcd performance at risk',
                            'action_required': 'Immediate resource scaling or workload redistribution required'
                        })
            
            # CPU starvation alerts
            cpu_health = performance_analysis.get('cpu_analysis', {}).get('health_status')
            if cpu_health == 'critical':
                alerts.append({
                    'severity': 'critical',
                    'category': 'resource_starvation',
                    'message': 'Critical CPU utilization detected on etcd pods',
                    'impact': 'Potential CPU starvation affecting etcd performance',
                    'action_required': 'Increase CPU resources or reduce load immediately'
                })
            
            # Network performance alerts
            network_health = performance_analysis.get('network_analysis', {}).get('health_status')
            if network_health == 'critical':
                alerts.append({
                    'severity': 'warning',
                    'category': 'network_performance',
                    'message': 'Network performance issues detected',
                    'impact': 'Cluster communication may be affected',
                    'action_required': 'Investigate network connectivity and bandwidth'
                })
            
        except Exception as e:
            self.logger.error(f"Error generating alerts: {e}")
            alerts.append({
                'severity': 'error',
                'category': 'system_error',
                'message': f'Alert generation failed: {str(e)}',
                'impact': 'Unable to assess critical alerts',
                'action_required': 'Review system logs for alert generation errors'
            })
            
        return alerts
    
    def _create_metric_tables(self, data: Dict[str, Any], 
                             node_usage_data: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """Create formatted metric tables for display including node usage"""
        tables = {}
        
        try:
            # WAL fsync table
            wal_data = data.get('wal_fsync_data', [])
            wal_p99_metrics = [m for m in wal_data if 'p99' in m.get('metric_name', '')]
            if wal_p99_metrics:
                tables['wal_fsync_p99'] = self._format_latency_table(
                    wal_p99_metrics, 'WAL Fsync P99 Latency', self.thresholds['wal_fsync_p99_ms']
                )
            
            # Backend commit table
            backend_data = data.get('backend_commit_data', [])
            backend_p99_metrics = [m for m in backend_data if 'p99' in m.get('metric_name', '')]
            if backend_p99_metrics:
                tables['backend_commit_p99'] = self._format_latency_table(
                    backend_p99_metrics, 'Backend Commit P99 Latency', self.thresholds['backend_commit_p99_ms']
                )
            
            # CPU usage table
            general_data = data.get('general_info_data', [])
            cpu_metrics = [m for m in general_data if 'cpu_usage' in m.get('metric_name', '')]
            if cpu_metrics:
                tables['etcd_cpu_usage'] = self._format_resource_table(cpu_metrics, 'etcd Pod CPU Usage')
            
            # Memory usage table
            memory_metrics = [m for m in general_data if 'memory_usage' in m.get('metric_name', '')]
            if memory_metrics:
                tables['etcd_memory_usage'] = self._format_memory_table(memory_metrics, 'etcd Pod Memory Usage')
            
            # Node usage tables
            if node_usage_data and node_usage_data.get('status') == 'success':
                usage_data = node_usage_data.get('data', {})
                metrics = usage_data.get('metrics', {})
                
                # Node CPU table
                cpu_usage = metrics.get('cpu_usage', {})
                if cpu_usage.get('status') == 'success':
                    tables['node_cpu_usage'] = self._format_node_cpu_table(
                        cpu_usage.get('nodes', {})
                    )
                
                # Node memory table
                memory_used = metrics.get('memory_used', {})
                if memory_used.get('status') == 'success':
                    tables['node_memory_usage'] = self._format_node_memory_table(
                        memory_used.get('nodes', {})
                    )
                
                # Top cgroup consumers
                cgroup_cpu = metrics.get('cgroup_cpu_usage', {})
                cgroup_rss = metrics.get('cgroup_rss_usage', {})
                if cgroup_cpu.get('status') == 'success' or cgroup_rss.get('status') == 'success':
                    tables['cgroup_usage'] = self._format_cgroup_table(
                        cgroup_cpu.get('nodes', {}),
                        cgroup_rss.get('nodes', {})
                    )
            
        except Exception as e:
            self.logger.error(f"Error creating metric tables: {e}")
            tables['error'] = f"Error creating tables: {str(e)}"
            
        return tables
    
    def _format_node_cpu_table(self, nodes_data: Dict[str, Any]) -> str:
        """Format node CPU usage table"""
        try:
            table_lines = []
            table_lines.append("\nNode CPU Usage")
            table_lines.append("=" * 100)
            table_lines.append(f"{'Node Name':<50} {'Cores':<10} {'Avg (%)':<12} {'Max (%)':<12} {'Status':<10}")
            table_lines.append("-" * 100)
            
            for node_name, node_data in nodes_data.items():
                total = node_data.get('total', {})
                modes = node_data.get('modes', {})
                
                idle_max = modes.get('idle', {}).get('max', 0)
                estimated_cores = int(idle_max / 100) if idle_max > 0 else 40
                
                raw_avg = total.get('avg', 0)
                raw_max = total.get('max', 0)
                
                avg_utilization = (raw_avg / estimated_cores) if estimated_cores > 0 else 0
                max_utilization = (raw_max / estimated_cores) if estimated_cores > 0 else 0
                
                if max_utilization > self.thresholds['cpu_usage_critical']:
                    status = "CRITICAL"
                elif avg_utilization > self.thresholds['cpu_usage_warning']:
                    status = "WARNING"
                else:
                    status = "GOOD"
                
                table_lines.append(f"{node_name:<50} {estimated_cores:<10} {avg_utilization:<12.2f} {max_utilization:<12.2f} {status:<10}")
            
            table_lines.append(f"\nThresholds: Warning {self.thresholds['cpu_usage_warning']}%, Critical {self.thresholds['cpu_usage_critical']}%")
            return "\n".join(table_lines)
            
        except Exception as e:
            return f"Error formatting node CPU table: {str(e)}"
    
    def _format_node_memory_table(self, nodes_data: Dict[str, Any]) -> str:
        """Format node memory usage table"""
        try:
            table_lines = []
            table_lines.append("\nNode Memory Usage")
            table_lines.append("=" * 110)
            table_lines.append(f"{'Node Name':<50} {'Capacity (GB)':<15} {'Used (GB)':<12} {'Util (%)':<12} {'Status':<10}")
            table_lines.append("-" * 110)
            
            for node_name, node_data in nodes_data.items():
                avg_used = node_data.get('avg', 0)
                max_used = node_data.get('max', 0)
                total_capacity = node_data.get('total_capacity', 0)
                
                avg_percent = (avg_used / total_capacity * 100) if total_capacity > 0 else 0
                max_percent = (max_used / total_capacity * 100) if total_capacity > 0 else 0
                
                if max_percent > self.thresholds['memory_usage_critical']:
                    status = "CRITICAL"
                elif avg_percent > self.thresholds['memory_usage_warning']:
                    status = "WARNING"
                else:
                    status = "GOOD"
                
                table_lines.append(f"{node_name:<50} {total_capacity:<15.2f} {avg_used:<12.2f} {avg_percent:<12.2f} {status:<10}")
            
            table_lines.append(f"\nThresholds: Warning {self.thresholds['memory_usage_warning']}%, Critical {self.thresholds['memory_usage_critical']}%")
            return "\n".join(table_lines)
            
        except Exception as e:
            return f"Error formatting node memory table: {str(e)}"
    
    def _format_cgroup_table(self, cgroup_cpu_nodes: Dict[str, Any], 
                            cgroup_rss_nodes: Dict[str, Any]) -> str:
        """Format cgroup usage table"""
        try:
            table_lines = []
            table_lines.append("\nTop Cgroup Resource Consumers")
            table_lines.append("=" * 100)
            table_lines.append(f"{'Node':<40} {'Cgroup':<30} {'CPU (%)':<15} {'Memory (GB)':<15}")
            table_lines.append("-" * 100)
            
            # Combine CPU and memory data
            combined_data = {}
            
            for node_name, node_data in cgroup_cpu_nodes.items():
                if node_name not in combined_data:
                    combined_data[node_name] = {}
                
                cgroups = node_data.get('cgroups', {})
                for cgroup_name, cgroup_data in cgroups.items():
                    if cgroup_name not in combined_data[node_name]:
                        combined_data[node_name][cgroup_name] = {}
                    combined_data[node_name][cgroup_name]['cpu'] = cgroup_data.get('avg', 0)
            
            for node_name, node_data in cgroup_rss_nodes.items():
                if node_name not in combined_data:
                    combined_data[node_name] = {}
                
                cgroups = node_data.get('cgroups', {})
                for cgroup_name, cgroup_data in cgroups.items():
                    if cgroup_name not in combined_data[node_name]:
                        combined_data[node_name][cgroup_name] = {}
                    combined_data[node_name][cgroup_name]['memory'] = cgroup_data.get('avg', 0)
            
            # Display top consumers per node
            for node_name, cgroups in combined_data.items():
                # Sort by CPU usage
                sorted_cgroups = sorted(cgroups.items(), 
                                      key=lambda x: x[1].get('cpu', 0), 
                                      reverse=True)[:5]
                
                for cgroup_name, data in sorted_cgroups:
                    cpu = data.get('cpu', 0)
                    memory = data.get('memory', 0)
                    
                    # Only show significant consumers
                    if cpu > 1 or memory > 0.1:
                        table_lines.append(f"{node_name:<40} {cgroup_name:<30} {cpu:<15.2f} {memory:<15.2f}")
            
            return "\n".join(table_lines)
            
        except Exception as e:
            return f"Error formatting cgroup table: {str(e)}"
    """
    Final part of etcd_analyzer_performance_report.py
    Includes report generation and utility methods
    """

    def _format_latency_table(self, metrics: List[Dict[str, Any]], title: str, threshold_ms: float) -> str:
        """Format latency metrics into a table"""
        try:
            table_lines = []
            table_lines.append(f"\n{title}")
            table_lines.append("=" * len(title))
            table_lines.append(f"{'Pod Name':<50} {'Avg (ms)':<12} {'Max (ms)':<12} {'Status':<10}")
            table_lines.append("-" * 84)
            
            for metric in metrics:
                pod_name = metric.get('pod_name', 'unknown')
                avg_value = metric.get('avg', 0)
                max_value = metric.get('max', 0)
                unit = metric.get('unit', 'seconds')
                
                if unit == 'seconds':
                    avg_ms = avg_value * 1000
                    max_ms = max_value * 1000
                else:
                    avg_ms = avg_value
                    max_ms = max_value
                
                if avg_ms > threshold_ms:
                    status = "CRITICAL"
                elif max_ms > threshold_ms:
                    status = "WARNING"
                else:
                    status = "GOOD"
                
                table_lines.append(f"{pod_name:<50} {avg_ms:<12.3f} {max_ms:<12.3f} {status:<10}")
            
            table_lines.append(f"\nThreshold: {threshold_ms} ms")
            return "\n".join(table_lines)
            
        except Exception as e:
            return f"Error formatting latency table: {str(e)}"
    
    def _format_resource_table(self, metrics: List[Dict[str, Any]], title: str) -> str:
        """Format resource metrics into a table"""
        try:
            table_lines = []
            table_lines.append(f"\n{title}")
            table_lines.append("=" * len(title))
            table_lines.append(f"{'Pod Name':<50} {'Avg (%)':<12} {'Max (%)':<12} {'Status':<10}")
            table_lines.append("-" * 84)
            
            for metric in metrics:
                pod_name = metric.get('pod_name', 'unknown')
                avg_usage = metric.get('avg', 0)
                max_usage = metric.get('max', 0)
                
                if max_usage > self.thresholds['cpu_usage_critical']:
                    status = "CRITICAL"
                elif avg_usage > self.thresholds['cpu_usage_warning']:
                    status = "WARNING"
                else:
                    status = "GOOD"
                
                table_lines.append(f"{pod_name:<50} {avg_usage:<12.2f} {max_usage:<12.2f} {status:<10}")
            
            return "\n".join(table_lines)
            
        except Exception as e:
            return f"Error formatting resource table: {str(e)}"
    
    def _format_memory_table(self, metrics: List[Dict[str, Any]], title: str) -> str:
        """Format memory metrics into a table"""
        try:
            table_lines = []
            table_lines.append(f"\n{title}")
            table_lines.append("=" * len(title))
            table_lines.append(f"{'Pod Name':<50} {'Avg (MB)':<12} {'Max (MB)':<12} {'Status':<10}")
            table_lines.append("-" * 84)
            
            for metric in metrics:
                pod_name = metric.get('pod_name', 'unknown')
                avg_usage = metric.get('avg', 0)
                max_usage = metric.get('max', 0)
                
                memory_percent = (avg_usage / 1024) * 100
                if memory_percent > 85:
                    status = "CRITICAL"
                elif memory_percent > 70:
                    status = "WARNING"
                else:
                    status = "GOOD"
                
                table_lines.append(f"{pod_name:<50} {avg_usage:<12.2f} {max_usage:<12.2f} {status:<10}")
            
            return "\n".join(table_lines)
            
        except Exception as e:
            return f"Error formatting memory table: {str(e)}"
    
    def generate_performance_report(self, analysis_results: Dict[str, Any], 
                                  test_id: str, duration: str) -> str:
        """Generate comprehensive performance report including node usage"""
        try:
            report_lines = []
            
            # Header
            report_lines.extend([
                "=" * 100,
                "ETCD PERFORMANCE ANALYSIS REPORT",
                "=" * 100,
                f"Test ID: {test_id}",
                f"Analysis Duration: {duration}",
                f"Report Generated: {datetime.now(self.timezone).strftime('%Y-%m-%d %H:%M:%S UTC')}",
                f"Analysis Status: {analysis_results.get('status', 'unknown').upper()}",
                ""
            ])
            
            # Executive Summary
            report_lines.extend([
                "EXECUTIVE SUMMARY",
                "=" * 50,
                ""
            ])
            
            # Critical metrics summary
            critical_analysis = analysis_results.get('critical_metrics_analysis', {})
            overall_disk_health = critical_analysis.get('overall_disk_health', 'unknown')
            
            report_lines.append(f"Overall Disk Performance Health: {overall_disk_health.upper()}")
            
            # Node usage summary
            node_usage_analysis = analysis_results.get('node_usage_analysis', {})
            if node_usage_analysis:
                node_health = node_usage_analysis.get('health_status', 'unknown')
                report_lines.append(f"Master Node Resource Health: {node_health.upper()}")
            
            # Baseline comparison
            baseline_comparison = analysis_results.get('baseline_comparison', {})
            performance_grade = baseline_comparison.get('performance_grade', 'unknown')
            report_lines.append(f"Performance Grade: {performance_grade.upper()}")
            report_lines.append("")
            
            # Alerts section
            alerts = analysis_results.get('alerts', [])
            if alerts:
                report_lines.extend([
                    "CRITICAL ALERTS",
                    "=" * 50,
                    ""
                ])
                
                for alert in alerts:
                    severity = alert.get('severity', 'unknown').upper()
                    message = alert.get('message', 'No message')
                    impact = alert.get('impact', 'Unknown impact')
                    action = alert.get('action_required', 'No action specified')
                    
                    report_lines.extend([
                        f"[{severity}] {message}",
                        f"Impact: {impact}",
                        f"Action Required: {action}",
                        ""
                    ])
            else:
                report_lines.extend([
                    "CRITICAL ALERTS",
                    "=" * 50,
                    "No critical alerts detected.",
                    ""
                ])
            
            # Critical Metrics Analysis
            report_lines.extend([
                "CRITICAL METRICS ANALYSIS",
                "=" * 50,
                ""
            ])
            
            # WAL fsync analysis
            wal_analysis = critical_analysis.get('wal_fsync_analysis', {})
            if wal_analysis:
                cluster_summary = wal_analysis.get('cluster_summary', {})
                health_status = wal_analysis.get('health_status', 'unknown')
                
                report_lines.extend([
                    "WAL Fsync Performance:",
                    f"  Health Status: {health_status.upper()}",
                    f"  Average Latency: {cluster_summary.get('avg_latency_ms', 'N/A')} ms",
                    f"  Maximum Latency: {cluster_summary.get('max_latency_ms', 'N/A')} ms",
                    f"  Threshold: {self.thresholds['wal_fsync_p99_ms']} ms",
                    f"  Pods with Issues: {cluster_summary.get('pods_with_issues', 0)}/{cluster_summary.get('total_pods', 0)}",
                    ""
                ])
            
            # Backend commit analysis
            backend_analysis = critical_analysis.get('backend_commit_analysis', {})
            if backend_analysis:
                cluster_summary = backend_analysis.get('cluster_summary', {})
                health_status = backend_analysis.get('health_status', 'unknown')
                
                report_lines.extend([
                    "Backend Commit Performance:",
                    f"  Health Status: {health_status.upper()}",
                    f"  Average Latency: {cluster_summary.get('avg_latency_ms', 'N/A')} ms",
                    f"  Maximum Latency: {cluster_summary.get('max_latency_ms', 'N/A')} ms",
                    f"  Threshold: {self.thresholds['backend_commit_p99_ms']} ms",
                    f"  Pods with Issues: {cluster_summary.get('pods_with_issues', 0)}/{cluster_summary.get('total_pods', 0)}",
                    ""
                ])
            
            # Node Usage Analysis
            if node_usage_analysis:
                report_lines.extend([
                    "MASTER NODE RESOURCE USAGE",
                    "=" * 50,
                    ""
                ])
                
                # CPU Analysis
                cpu_analysis = node_usage_analysis.get('cpu_analysis', {})
                if cpu_analysis.get('cluster_summary'):
                    cpu_summary = cpu_analysis['cluster_summary']
                    report_lines.extend([
                        "Node CPU Usage:",
                        f"  Average Utilization: {cpu_summary.get('avg_utilization_percent', 'N/A')}%",
                        f"  Total Nodes: {cpu_summary.get('total_nodes', 0)}",
                        f"  High Usage Nodes: {cpu_summary.get('high_usage_nodes', 0)}",
                        f"  Critical Usage Nodes: {cpu_summary.get('critical_usage_nodes', 0)}",
                        ""
                    ])
                
                # Memory Analysis
                memory_analysis = node_usage_analysis.get('memory_analysis', {})
                if memory_analysis.get('cluster_summary'):
                    memory_summary = memory_analysis['cluster_summary']
                    report_lines.extend([
                        "Node Memory Usage:",
                        f"  Average Utilization: {memory_summary.get('avg_utilization_percent', 'N/A')}%",
                        f"  Total Nodes: {memory_summary.get('total_nodes', 0)}",
                        f"  High Usage Nodes: {memory_summary.get('high_usage_nodes', 0)}",
                        f"  Critical Usage Nodes: {memory_summary.get('critical_usage_nodes', 0)}",
                        ""
                    ])
                
                # Cgroup Analysis
                cgroup_analysis = node_usage_analysis.get('cgroup_analysis', {})
                if cgroup_analysis.get('top_consumers'):
                    top_cpu = cgroup_analysis['top_consumers'].get('cpu', [])[:3]
                    top_memory = cgroup_analysis['top_consumers'].get('memory', [])[:3]
                    
                    if top_cpu:
                        report_lines.extend(["Top CPU Consuming Cgroups:"])
                        for consumer in top_cpu:
                            report_lines.append(
                                f"  - {consumer['cgroup']} on {consumer['node']}: {consumer['avg_percent']:.2f}%"
                            )
                        report_lines.append("")
                    
                    if top_memory:
                        report_lines.extend(["Top Memory Consuming Cgroups:"])
                        for consumer in top_memory:
                            report_lines.append(
                                f"  - {consumer['cgroup']} on {consumer['node']}: {consumer['avg_gb']:.2f} GB"
                            )
                        report_lines.append("")
            
            # Supporting metrics summary
            performance_summary = analysis_results.get('performance_summary', {})
            
            # Network Analysis
            network_analysis = performance_summary.get('network_analysis', {})
            if network_analysis:
                report_lines.extend([
                    "NETWORK PERFORMANCE",
                    "=" * 50,
                    f"Overall Health: {network_analysis.get('health_status', 'unknown').upper()}",
                ])
                
                peer_latency = network_analysis.get('peer_latency_analysis', {})
                if peer_latency.get('cluster_summary'):
                    peer_summary = peer_latency['cluster_summary']
                    report_lines.extend([
                        f"Peer Latency: {peer_summary.get('avg_latency_ms', 'N/A')} ms avg, {peer_summary.get('max_latency_ms', 'N/A')} ms max",
                        f"Pods with Latency Issues: {peer_summary.get('pods_with_issues', 0)}/{peer_summary.get('total_pods', 0)}"
                    ])
                
                report_lines.append("")
            
            # Detailed Metric Tables
            report_lines.extend([
                "DETAILED METRICS",
                "=" * 50
            ])
            
            metric_tables = analysis_results.get('metric_tables', {})
            for table_name, table_content in metric_tables.items():
                if table_content and 'error' not in table_name.lower():
                    report_lines.append(table_content)
                    report_lines.append("")
            
            # Baseline Comparison
            report_lines.extend([
                "BASELINE COMPARISON",
                "=" * 50,
                ""
            ])
            
            current_vs_baseline = baseline_comparison.get('current_vs_baseline', {})
            
            if current_vs_baseline:
                report_lines.append(f"{'Metric':<40} {'Current':<15} {'Target':<15} {'Status':<10}")
                report_lines.append("-" * 80)
                
                for metric, comparison in current_vs_baseline.items():
                    current = comparison.get('current', 'N/A')
                    target = comparison.get('target', 'N/A')
                    within_target = comparison.get('within_target', False)
                    status = "PASS" if within_target else "FAIL"
                    
                    display_name = metric.replace('_', ' ').title()
                    
                    if isinstance(current, (int, float)):
                        current_str = f"{current:.2f}"
                    else:
                        current_str = str(current)
                    
                    if isinstance(target, (int, float)):
                        target_str = f"{target:.2f}"
                    else:
                        target_str = str(target)
                    
                    report_lines.append(f"{display_name:<40} {current_str:<15} {target_str:<15} {status:<10}")
                
                report_lines.extend(["", f"Overall Performance Grade: {performance_grade.upper()}", ""])
            
            # Recommendations
            recommendations = analysis_results.get('recommendations', [])
            if recommendations:
                report_lines.extend([
                    "RECOMMENDATIONS",
                    "=" * 50,
                    ""
                ])
                
                # Group by priority
                high_priority = [r for r in recommendations if r.get('priority') == 'high']
                medium_priority = [r for r in recommendations if r.get('priority') == 'medium']
                low_priority = [r for r in recommendations if r.get('priority') == 'low']
                
                for priority, recs in [("HIGH PRIORITY", high_priority), 
                                      ("MEDIUM PRIORITY", medium_priority), 
                                      ("LOW PRIORITY", low_priority)]:
                    if recs:
                        report_lines.extend([priority, "-" * len(priority), ""])
                        
                        for i, rec in enumerate(recs, 1):
                            report_lines.extend([
                                f"{i}. {rec.get('issue', 'Unknown issue')}",
                                f"   Category: {rec.get('category', 'unknown').replace('_', ' ').title()}",
                                f"   Recommendation: {rec.get('recommendation', 'No recommendation')}",
                                f"   Rationale: {rec.get('rationale', 'No rationale provided')}",
                                ""
                            ])
            
            # Analysis Methodology
            report_lines.extend([
                "ANALYSIS METHODOLOGY",
                "=" * 50,
                "",
                "This report analyzes etcd performance based on industry best practices:",
                "",
                "Critical Thresholds:",
                f"• WAL fsync P99 latency should be < {self.thresholds['wal_fsync_p99_ms']} ms",
                f"• Backend commit P99 latency should be < {self.thresholds['backend_commit_p99_ms']} ms",
                f"• CPU usage should remain below {self.thresholds['cpu_usage_warning']}% (warning) / {self.thresholds['cpu_usage_critical']}% (critical)",
                f"• Memory usage should remain below {self.thresholds['memory_usage_warning']}% (warning) / {self.thresholds['memory_usage_critical']}% (critical)",
                f"• Network peer latency should be < {self.thresholds['peer_latency_warning_ms']} ms",
                "",
                "Performance degradation can be caused by:",
                "1. Slow disk I/O (most common) - upgrade to NVMe SSDs, dedicated storage",
                "2. CPU starvation - increase CPU resources, process isolation",
                "3. Network issues - optimize topology, increase bandwidth",
                "4. Memory pressure - monitor and increase as needed",
                "5. Node resource contention - scale master nodes or redistribute workload",
                "",
                "Data Sources:",
                f"• Metrics collection duration: {duration}",
                f"• Test ID: {test_id}",
                f"• Collection timestamp: {analysis_results.get('timestamp', 'Unknown')}",
                ""
            ])
            
            # Node Usage Insights
            if node_usage_analysis:
                report_lines.extend([
                    "Node Usage Analysis:",
                    "• Master node resource metrics collected from node-level and cgroup-level monitoring",
                    "• Cgroup analysis identifies resource-intensive system services",
                    "• Resource utilization correlated with etcd performance metrics",
                    ""
                ])
            
            # Footer
            report_lines.extend([
                "=" * 100,
                "END OF REPORT",
                "=" * 100
            ])
            
            return "\n".join(report_lines)
            
        except Exception as e:
            self.logger.error(f"Error generating performance report: {e}")
            return f"Error generating performance report: {str(e)}"

    def script_based_root_cause_analysis(self, failed_thresholds: List[Dict[str, Any]],
                                         metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Provide a lightweight, scripted root-cause analysis based on collected metrics.

        Returns a structured summary consumed by the agent, including:
        - disk_io_analysis.disk_performance_assessment.write_throughput.performance_grade
        - network_analysis.network_health_assessment.avg_peer_latency_ms/network_grade
        """
        try:
            data = (metrics_data or {}).get('data', {})

            # ---- Disk I/O assessment (write throughput) ----
            disk_io_data = data.get('disk_io_data', []) or []
            throughput_mb_values: List[float] = []
            for item in disk_io_data:
                metric_name = item.get('metric_name', '')
                if 'throughput' in metric_name:
                    unit = item.get('unit', 'bytes_per_second')
                    avg_val = item.get('avg') or 0.0
                    if unit == 'bytes_per_second':
                        throughput_mb_values.append(float(avg_val) / (1024 * 1024))
                    else:
                        # assume already MB/s
                        throughput_mb_values.append(float(avg_val))

            cluster_avg_mb_s = round(sum(throughput_mb_values) / len(throughput_mb_values), 2) if throughput_mb_values else 0.0

            # Simple grading heuristics for write throughput
            # Tune thresholds as needed for your environment
            if cluster_avg_mb_s >= 200:
                disk_grade = 'excellent'
            elif cluster_avg_mb_s >= 100:
                disk_grade = 'good'
            elif cluster_avg_mb_s >= 50:
                disk_grade = 'warning'
            else:
                disk_grade = 'critical'

            disk_assessment = {
                'disk_performance_assessment': {
                    'write_throughput': {
                        'cluster_avg_mb_s': cluster_avg_mb_s,
                        'performance_grade': disk_grade
                    }
                }
            }

            # ---- Network health assessment (peer latency) ----
            network_data = data.get('network_data', {}) or {}
            pod_metrics = network_data.get('pod_metrics', []) or []
            peer_latency_ms: List[float] = []
            for m in pod_metrics:
                if 'peer2peer_latency' in m.get('metric_name', ''):
                    unit = m.get('unit', 'seconds')
                    avg_val = m.get('avg') or 0.0
                    # Convert seconds to milliseconds if needed
                    peer_latency_ms.append(float(avg_val) * 1000 if unit == 'seconds' else float(avg_val))

            avg_peer_latency_ms = round(sum(peer_latency_ms) / len(peer_latency_ms), 3) if peer_latency_ms else 0.0

            # Grade peer latency with simple thresholds
            if avg_peer_latency_ms <= self.thresholds['peer_latency_warning_ms']:
                network_grade = 'good'
            elif avg_peer_latency_ms <= self.thresholds['peer_latency_critical_ms']:
                network_grade = 'warning'
            else:
                network_grade = 'critical'

            network_assessment = {
                'network_health_assessment': {
                    'avg_peer_latency_ms': avg_peer_latency_ms,
                    'network_grade': network_grade
                }
            }

            # ---- WAL/Backend latency summary for evidence ----
            wal = data.get('wal_fsync_data', []) or []
            backend = data.get('backend_commit_data', []) or []
            wal_ms = [float(x.get('avg') or 0.0) * 1000 if x.get('unit', 'seconds') == 'seconds' else float(x.get('avg') or 0.0)
                    for x in wal if 'p99' in x.get('metric_name', '')]
            backend_ms = [float(x.get('avg') or 0.0) * 1000 if x.get('unit', 'seconds') == 'seconds' else float(x.get('avg') or 0.0)
                        for x in backend if 'p99' in x.get('metric_name', '')]

            wal_avg_ms = round(sum(wal_ms) / len(wal_ms), 3) if wal_ms else 0.0
            backend_avg_ms = round(sum(backend_ms) / len(backend_ms), 3) if backend_ms else 0.0

            evidence = {
                'latency_evidence': {
                    'wal_fsync_p99_avg_ms': wal_avg_ms,
                    'backend_commit_p99_avg_ms': backend_avg_ms
                },
                'failed_thresholds': failed_thresholds or []
            }

            return {
                'disk_io_analysis': disk_assessment,
                'network_analysis': network_assessment,
                'evidence': evidence,
                'timestamp': self.utility.format_timestamp(),
                'status': 'success'
            }
        except Exception as e:
            self.logger.error(f"Script analysis failed: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }

def main():
    """Test function for the performance report analyzer"""
    import json
    
    # Sample test data
    sample_data = {
        "status": "success",
        "data": {
            "wal_fsync_data": [
                {
                    "metric_name": "disk_wal_fsync_seconds_duration_p99",
                    "pod_name": "etcd-test-pod-1",
                    "avg": 0.015,
                    "max": 0.020,
                    "unit": "seconds"
                }
            ],
            "backend_commit_data": [
                {
                    "metric_name": "disk_backend_commit_duration_seconds_p99",
                    "pod_name": "etcd-test-pod-1",
                    "avg": 0.030,
                    "max": 0.035,
                    "unit": "seconds"
                }
            ],
            "general_info_data": [
                {
                    "metric_name": "etcd_pods_cpu_usage",
                    "pod_name": "etcd-test-pod-1",
                    "avg": 75.5,
                    "max": 89.2,
                    "unit": "percent"
                }
            ]
        },
        "duration": "1h",
        "test_id": "test-123"
    }
    
    # Sample node usage data
    sample_node_usage = {
        "status": "success",
        "data": {
            "node_capacities": {
                "master-node-1": {"memory": 125.37}
            },
            "metrics": {
                "cpu_usage": {
                    "status": "success",
                    "nodes": {
                        "master-node-1": {
                            "modes": {
                                "idle": {"avg": 3140.45, "max": 3153.6},
                                "user": {"avg": 34.29, "max": 52.8}
                            },
                            "total": {"avg": 399.46, "max": 3153.6}
                        }
                    }
                },
                "memory_used": {
                    "status": "success",
                    "nodes": {
                        "master-node-1": {
                            "avg": 14.68,
                            "max": 14.84,
                            "total_capacity": 125.37
                        }
                    }
                }
            }
        }
    }
    
    # Test the analyzer
    analyzer = etcdReportAnalyzer()
    analysis_results = analyzer.analyze_performance_metrics(
        sample_data, 
        "test-001",
        sample_node_usage
    )
    report = analyzer.generate_performance_report(analysis_results, "test-001", "1h")
    
    print("Sample Performance Report with Node Usage:")
    print("=" * 80)
    print(report)

if __name__ == "__main__":
    main()                    