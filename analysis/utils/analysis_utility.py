"""
etcd Analyzer Performance Utility Module
Common utility functions for etcd performance analysis
"""

import asyncio
import logging
import uuid
from typing import Dict, List, Any, Optional
from datetime import datetime
import pytz


class etcdAnalyzerUtility:
    """Utility class for etcd performance analysis operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.timezone = pytz.UTC
    
    def generate_test_id(self) -> str:
        """Generate a unique test ID for analysis sessions"""
        return str(uuid.uuid4())
    
    def format_metric_value(self, value: float, unit: str) -> Dict[str, Any]:
        """Format metric value with appropriate unit and readability"""
        if value is None:
            return {"raw": None, "formatted": "N/A"}
        
        formatted_value = {
            "raw": value,
            "formatted": "N/A"
        }
        
        try:
            if unit.lower() in ['milliseconds', 'ms']:
                if value >= 1000:
                    formatted_value["formatted"] = f"{value/1000:.3f}s"
                else:
                    formatted_value["formatted"] = f"{value:.3f}ms"
            elif unit.lower() in ['seconds', 's']:
                if value >= 60:
                    minutes = value / 60
                    if minutes >= 60:
                        hours = minutes / 60
                        formatted_value["formatted"] = f"{hours:.2f}h"
                    else:
                        formatted_value["formatted"] = f"{minutes:.2f}m"
                else:
                    formatted_value["formatted"] = f"{value:.3f}s"
            elif unit.lower() in ['bytes']:
                if value >= 1024**3:
                    formatted_value["formatted"] = f"{value/(1024**3):.2f}GB"
                elif value >= 1024**2:
                    formatted_value["formatted"] = f"{value/(1024**2):.2f}MB"
                elif value >= 1024:
                    formatted_value["formatted"] = f"{value/1024:.2f}KB"
                else:
                    formatted_value["formatted"] = f"{value:.0f}B"
            elif unit.lower() in ['bytes_per_second']:
                if value >= 1024**3:
                    formatted_value["formatted"] = f"{value/(1024**3):.2f}GB/s"
                elif value >= 1024**2:
                    formatted_value["formatted"] = f"{value/(1024**2):.2f}MB/s"
                elif value >= 1024:
                    formatted_value["formatted"] = f"{value/1024:.2f}KB/s"
                else:
                    formatted_value["formatted"] = f"{value:.0f}B/s"
            elif unit.lower() in ['operations_per_second', 'ops/s']:
                formatted_value["formatted"] = f"{value:.3f}/s"
            elif unit.lower() in ['percent', '%']:
                formatted_value["formatted"] = f"{value:.2f}%"
            elif unit.lower() in ['count', 'total']:
                formatted_value["formatted"] = f"{int(value):,}"
            elif unit.lower() in ['gb', 'gigabytes']:
                formatted_value["formatted"] = f"{value:.2f}GB"
            else:
                # Default formatting for unknown units
                if value >= 1000000:
                    formatted_value["formatted"] = f"{value/1000000:.2f}M {unit}"
                elif value >= 1000:
                    formatted_value["formatted"] = f"{value/1000:.2f}K {unit}"
                else:
                    formatted_value["formatted"] = f"{value:.3f} {unit}"
        
        except Exception as e:
            self.logger.warning(f"Error formatting value {value} with unit {unit}: {e}")
            formatted_value["formatted"] = f"{value} {unit}"
        
        return formatted_value
    
    def extract_pod_metrics(self, metric_data: Dict[str, Any], metric_name: str) -> List[Dict[str, Any]]:
        """Extract pod-level metrics from collected data"""
        pod_metrics = []
        
        try:
            if metric_data.get('status') == 'success':
                # Handle different data structures
                pods_data = None
                
                if 'pod_metrics' in metric_data:
                    pods_data = metric_data['pod_metrics']
                elif 'pods' in metric_data:
                    pods_data = metric_data['pods']
                elif 'data' in metric_data and 'pods' in metric_data['data']:
                    pods_data = metric_data['data']['pods']
                
                if pods_data:
                    for pod_name, pod_stats in pods_data.items():
                        pod_metric = {
                            "metric_name": metric_name,
                            "pod_name": pod_name,
                            "avg": pod_stats.get('avg'),
                            "max": pod_stats.get('max'),
                            "unit": metric_data.get('unit', 'unknown')
                        }
                        
                        # Add node information if available
                        if 'node' in pod_stats:
                            pod_metric["node"] = pod_stats['node']
                        
                        pod_metrics.append(pod_metric)
        
        except Exception as e:
            self.logger.error(f"Error extracting pod metrics for {metric_name}: {e}")
        
        return pod_metrics
    
    def extract_node_metrics(self, metric_data: Dict[str, Any], metric_name: str) -> List[Dict[str, Any]]:
        """Extract node-level metrics from collected data"""
        node_metrics = []
        
        try:
            if metric_data.get('status') == 'success':
                nodes_data = None
                
                if 'nodes' in metric_data:
                    nodes_data = metric_data['nodes']
                elif 'data' in metric_data and 'nodes' in metric_data['data']:
                    nodes_data = metric_data['data']['nodes']
                
                if nodes_data:
                    for node_name, node_stats in nodes_data.items():
                        node_metric = {
                            "metric_name": metric_name,
                            "node_name": node_name,
                            "avg": node_stats.get('avg'),
                            "max": node_stats.get('max'),
                            "unit": metric_data.get('unit', 'unknown')
                        }
                        
                        # Add device information if available
                        if 'devices' in node_stats:
                            node_metric["devices"] = node_stats['devices']
                        elif 'device_count' in node_stats:
                            node_metric["device_count"] = node_stats['device_count']
                        
                        # Add capacity information if available
                        if 'total_capacity' in node_stats:
                            node_metric["total_capacity"] = node_stats['total_capacity']
                        
                        # Add mode/cgroup breakdowns if available
                        if 'modes' in node_stats:
                            node_metric["modes"] = node_stats['modes']
                        if 'cgroups' in node_stats:
                            node_metric["cgroups"] = node_stats['cgroups']
                        
                        node_metrics.append(node_metric)
        
        except Exception as e:
            self.logger.error(f"Error extracting node metrics for {metric_name}: {e}")
        
        return node_metrics
    
    def extract_cluster_metrics(self, metric_data: Dict[str, Any], metric_name: str, test_id: str) -> Dict[str, Any]:
        """Extract cluster-level metrics from collected data"""
        cluster_metric = {
            "metric_name": metric_name,
            "test_id": test_id,
            "avg": None,
            "max": None,
            "unit": "unknown",
            "query": None
        }
        
        try:
            if metric_data.get('status') == 'success':
                cluster_metric["avg"] = metric_data.get('avg')
                cluster_metric["max"] = metric_data.get('max')
                cluster_metric["unit"] = metric_data.get('unit', 'unknown')
                cluster_metric["query"] = metric_data.get('query')
        
        except Exception as e:
            self.logger.error(f"Error extracting cluster metrics for {metric_name}: {e}")
        
        return cluster_metric
    
    def analyze_node_resource_utilization(self, node_usage_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze node resource utilization patterns"""
        analysis = {
            "cpu_utilization": {},
            "memory_utilization": {},
            "resource_bottlenecks": [],
            "recommendations": []
        }
        
        try:
            if node_usage_data.get('status') != 'success':
                return analysis
            
            metrics = node_usage_data.get('data', {}).get('metrics', {})
            node_capacities = node_usage_data.get('data', {}).get('node_capacities', {})
            
            # Analyze CPU usage
            cpu_usage = metrics.get('cpu_usage', {})
            if cpu_usage.get('status') == 'success':
                nodes = cpu_usage.get('nodes', {})
                
                for node_name, node_data in nodes.items():
                    total = node_data.get('total', {})
                    avg_cpu = total.get('avg', 0)
                    max_cpu = total.get('max', 0)
                    
                    # CPU is reported as total percentage across all cores
                    # For 40 cores, 100% usage per core = 4000% total
                    # Estimate core count from max idle
                    modes = node_data.get('modes', {})
                    idle_max = modes.get('idle', {}).get('max', 0)
                    estimated_cores = int(idle_max / 100) if idle_max > 0 else 40
                    
                    # Calculate actual utilization percentage
                    avg_utilization = (avg_cpu / estimated_cores) if estimated_cores > 0 else 0
                    max_utilization = (max_cpu / estimated_cores) if estimated_cores > 0 else 0
                    
                    analysis['cpu_utilization'][node_name] = {
                        'estimated_cores': estimated_cores,
                        'avg_utilization_percent': round(avg_utilization, 2),
                        'max_utilization_percent': round(max_utilization, 2),
                        'raw_avg': avg_cpu,
                        'raw_max': max_cpu,
                        'status': self._assess_cpu_status(avg_utilization)
                    }
                    
                    # Check for CPU bottlenecks
                    if avg_utilization > 70:
                        analysis['resource_bottlenecks'].append({
                            'type': 'cpu',
                            'node': node_name,
                            'severity': 'high' if avg_utilization > 85 else 'medium',
                            'description': f'Node {node_name} CPU utilization at {avg_utilization:.1f}%'
                        })
            
            # Analyze memory usage
            memory_used = metrics.get('memory_used', {})
            if memory_used.get('status') == 'success':
                nodes = memory_used.get('nodes', {})
                
                for node_name, node_data in nodes.items():
                    avg_memory = node_data.get('avg', 0)
                    max_memory = node_data.get('max', 0)
                    total_capacity = node_data.get('total_capacity', 0)
                    
                    # Calculate utilization percentage
                    avg_percent = (avg_memory / total_capacity * 100) if total_capacity > 0 else 0
                    max_percent = (max_memory / total_capacity * 100) if total_capacity > 0 else 0
                    
                    analysis['memory_utilization'][node_name] = {
                        'total_capacity_gb': total_capacity,
                        'avg_used_gb': avg_memory,
                        'max_used_gb': max_memory,
                        'avg_utilization_percent': round(avg_percent, 2),
                        'max_utilization_percent': round(max_percent, 2),
                        'status': self._assess_memory_status(avg_percent)
                    }
                    
                    # Check for memory pressure
                    if avg_percent > 70:
                        analysis['resource_bottlenecks'].append({
                            'type': 'memory',
                            'node': node_name,
                            'severity': 'high' if avg_percent > 85 else 'medium',
                            'description': f'Node {node_name} memory utilization at {avg_percent:.1f}%'
                        })
            
            # Generate recommendations
            analysis['recommendations'] = self._generate_node_recommendations(
                analysis['resource_bottlenecks']
            )
        
        except Exception as e:
            self.logger.error(f"Error analyzing node resource utilization: {e}")
            analysis['error'] = str(e)
        
        return analysis
    
    def _assess_cpu_status(self, utilization: float) -> str:
        """Assess CPU utilization status"""
        if utilization > 85:
            return "critical"
        elif utilization > 70:
            return "warning"
        elif utilization > 50:
            return "moderate"
        else:
            return "good"
    
    def _assess_memory_status(self, utilization: float) -> str:
        """Assess memory utilization status"""
        if utilization > 85:
            return "critical"
        elif utilization > 70:
            return "warning"
        elif utilization > 50:
            return "moderate"
        else:
            return "good"
    
    def _generate_node_recommendations(self, bottlenecks: List[Dict[str, Any]]) -> List[str]:
        """Generate recommendations for node resource issues"""
        recommendations = []
        
        cpu_bottlenecks = [b for b in bottlenecks if b['type'] == 'cpu']
        memory_bottlenecks = [b for b in bottlenecks if b['type'] == 'memory']
        
        if cpu_bottlenecks:
            recommendations.extend([
                "Consider increasing CPU allocation for master nodes",
                "Review workload distribution across master nodes",
                "Investigate CPU-intensive processes on affected nodes"
            ])
        
        if memory_bottlenecks:
            recommendations.extend([
                "Consider increasing memory allocation for master nodes",
                "Review memory-intensive workloads and optimize where possible",
                "Monitor for memory leaks in system services"
            ])
        
        if not bottlenecks:
            recommendations.append("Node resource utilization is within acceptable ranges")
        
        return recommendations
    
    def analyze_latency_patterns(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze latency patterns and identify potential bottlenecks"""
        analysis = {
            "potential_bottlenecks": [],
            "latency_analysis": {},
            "recommendations": []
        }
        
        try:
            # Analyze WAL fsync latency
            if 'wal_fsync_data' in metrics_data:
                wal_metrics = metrics_data['wal_fsync_data']
                p99_data = next((m for m in wal_metrics if 'p99' in m.get('metric_name', '')), None)
                
                if p99_data and p99_data.get('avg'):
                    avg_latency = p99_data['avg']
                    max_latency = p99_data.get('max', avg_latency)
                    
                    analysis['latency_analysis']['wal_fsync_p99'] = {
                        "avg_ms": avg_latency * 1000 if avg_latency < 1 else avg_latency,
                        "max_ms": max_latency * 1000 if max_latency < 1 else max_latency,
                        "status": self._assess_latency_status(avg_latency, 0.01, 0.05, 0.1)
                    }
                    
                    if avg_latency > 0.1:
                        analysis['potential_bottlenecks'].append({
                            "type": "disk_io",
                            "description": "High WAL fsync latency indicates disk I/O bottleneck",
                            "severity": "high",
                            "metric": "wal_fsync_p99",
                            "value": f"{avg_latency:.3f}s"
                        })
                    elif avg_latency > 0.05:
                        analysis['potential_bottlenecks'].append({
                            "type": "disk_io",
                            "description": "Elevated WAL fsync latency may indicate disk performance issues",
                            "severity": "medium",
                            "metric": "wal_fsync_p99",
                            "value": f"{avg_latency:.3f}s"
                        })
            
            # Generate recommendations
            analysis['recommendations'] = self._generate_recommendations(analysis['potential_bottlenecks'])
        
        except Exception as e:
            self.logger.error(f"Error analyzing latency patterns: {e}")
            analysis['error'] = str(e)
        
        return analysis
    
    def _assess_latency_status(self, value: float, good_threshold: float, warning_threshold: float, critical_threshold: float) -> str:
        """Assess latency status based on thresholds"""
        if value <= good_threshold:
            return "excellent"
        elif value <= warning_threshold:
            return "good"
        elif value <= critical_threshold:
            return "warning"
        else:
            return "critical"
    
    def _generate_recommendations(self, bottlenecks: List[Dict[str, Any]]) -> List[str]:
        """Generate recommendations based on identified bottlenecks"""
        recommendations = []
        
        disk_issues = [b for b in bottlenecks if b['type'] == 'disk_io']
        network_issues = [b for b in bottlenecks if b['type'] == 'network']
        backend_issues = [b for b in bottlenecks if b['type'] == 'backend_commit']
        
        if disk_issues:
            recommendations.extend([
                "Check disk I/O performance and consider faster storage (NVMe SSD)",
                "Verify etcd data directory is on dedicated disk with sufficient IOPS",
                "Monitor disk utilization and consider storage optimization"
            ])
        
        if network_issues:
            recommendations.extend([
                "Check network connectivity between etcd cluster members",
                "Verify network bandwidth and latency between nodes",
                "Consider network topology optimization for etcd cluster"
            ])
        
        if backend_issues:
            recommendations.extend([
                "Consider etcd database compaction and defragmentation",
                "Monitor database size and optimize key-value operations",
                "Review etcd configuration for performance tuning"
            ])
        
        if not bottlenecks:
            recommendations.append("No significant performance bottlenecks detected")
        
        return recommendations
    
    def create_performance_summary(self, all_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Create a comprehensive performance summary"""
        summary = {
            "timestamp": datetime.now(self.timezone).isoformat(),
            "total_metrics_collected": 0,
            "categories": {
                "general_info": {"count": 0, "status": "unknown"},
                "wal_fsync": {"count": 0, "status": "unknown"},
                "disk_io": {"count": 0, "status": "unknown"},
                "network_io": {"count": 0, "status": "unknown"},
                "backend_commit": {"count": 0, "status": "unknown"},
                "compact_defrag": {"count": 0, "status": "unknown"},
                "node_usage": {"count": 0, "status": "unknown"}
            },
            "performance_indicators": {},
            "overall_health": "unknown"
        }
        
        try:
            # Count metrics by category
            category_alias = {
                'network': 'network_io'
            }
            for category, data in all_metrics.items():
                raw_key = category.replace('_data', '')
                key = category_alias.get(raw_key, raw_key)
                if key not in summary['categories']:
                    self.logger.debug(f"Unknown category '{key}' in summary counting")
                    continue
                if isinstance(data, list):
                    summary['categories'][key]['count'] = len(data)
                    summary['total_metrics_collected'] += len(data)
                elif isinstance(data, dict):
                    if 'pod_metrics' in data:
                        # Network data structure with pod_metrics, node_metrics, cluster_metrics
                        count = len(data.get('pod_metrics', [])) + len(data.get('node_metrics', [])) + len(data.get('cluster_metrics', []))
                        summary['categories'][key]['count'] = count
                        summary['total_metrics_collected'] += count
                    elif 'metrics' in data:
                        # Node usage data structure
                        count = len(data.get('metrics', {}))
                        summary['categories']['node_usage']['count'] = count
                        summary['total_metrics_collected'] += count
                    elif 'status' in data:
                        # Data structure with status (like node_usage_data)
                        if data.get('status') == 'success' and 'metrics' in data:
                            count = len(data.get('metrics', {}))
                            summary['categories']['node_usage']['count'] = count
                            summary['total_metrics_collected'] += count
            
            # Map latency analysis to categories and determine status
            category_status_map = {
                'wal_fsync_p99': 'wal_fsync',
                'backend_commit_p99': 'backend_commit',
                'disk_io': 'disk_io',
                'network': 'network_io',
                'general_info': 'general_info',
                'compact_defrag': 'compact_defrag',
                'node_usage': 'node_usage'
            }
            
            # Assess category status based on latency analysis
            if 'latency_analysis' in all_metrics:
                latency_data = all_metrics['latency_analysis']
                health_scores = []
                category_scores = {}
                
                # Process latency analysis results
                for metric, analysis in latency_data.get('latency_analysis', {}).items():
                    status = analysis.get('status', 'unknown')
                    if status == 'excellent':
                        score = 4
                    elif status == 'good':
                        score = 3
                    elif status == 'warning':
                        score = 2
                    elif status == 'critical':
                        score = 1
                    else:
                        score = 0
                    
                    health_scores.append(score)
                    
                    # Map metric to category
                    for metric_key, category_key in category_status_map.items():
                        if metric_key in metric.lower():
                            if category_key not in category_scores:
                                category_scores[category_key] = []
                            category_scores[category_key].append(score)
                            break
                
                # Set status for each category
                for category_key in summary['categories']:
                    if category_key in category_scores and category_scores[category_key]:
                        # Use average score for category
                        avg_category_score = sum(category_scores[category_key]) / len(category_scores[category_key])
                        if avg_category_score >= 3.5:
                            summary['categories'][category_key]['status'] = 'excellent'
                        elif avg_category_score >= 2.5:
                            summary['categories'][category_key]['status'] = 'good'
                        elif avg_category_score >= 1.5:
                            summary['categories'][category_key]['status'] = 'warning'
                        elif avg_category_score >= 0.5:
                            summary['categories'][category_key]['status'] = 'critical'
                    elif summary['categories'][category_key]['count'] > 0:
                        # If metrics collected but no latency analysis, default to 'good'
                        summary['categories'][category_key]['status'] = 'good'
                    else:
                        # No metrics collected
                        summary['categories'][category_key]['status'] = 'unknown'
                
                # Assess overall health
                if health_scores:
                    avg_score = sum(health_scores) / len(health_scores)
                    if avg_score >= 3.5:
                        summary['overall_health'] = 'excellent'
                    elif avg_score >= 2.5:
                        summary['overall_health'] = 'good'
                    elif avg_score >= 1.5:
                        summary['overall_health'] = 'warning'
                    else:
                        summary['overall_health'] = 'critical'
            else:
                # No latency analysis available, set status based on whether metrics were collected
                for category_key in summary['categories']:
                    if summary['categories'][category_key]['count'] > 0:
                        summary['categories'][category_key]['status'] = 'good'
                    else:
                        summary['categories'][category_key]['status'] = 'unknown'
                
                # Set overall health based on whether any metrics were collected
                if summary['total_metrics_collected'] > 0:
                    summary['overall_health'] = 'good'
                else:
                    summary['overall_health'] = 'unknown'
        
        except Exception as e:
            self.logger.error(f"Error creating performance summary: {e}")
            summary['error'] = str(e)
        
        return summary
    
    def format_timestamp(self, timestamp: Optional[str] = None) -> str:
        """Format timestamp in UTC"""
        if timestamp:
            return timestamp
        return datetime.now(self.timezone).isoformat()
    
    def safe_extract_value(self, data: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
        """Safely extract nested values from dictionary"""
        try:
            current = data
            for key in keys:
                current = current[key]
            return current
        except (KeyError, TypeError):
            return default


