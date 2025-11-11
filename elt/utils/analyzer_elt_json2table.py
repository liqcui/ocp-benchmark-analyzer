"""
Generic ELT Orchestrator for ETCD Analyzer
Provides plugin architecture for easy extension with new metric types
All metric-specific logic is delegated to specialized ELT modules
"""

import logging
from typing import Dict, Any, List, Optional, Union
import json
import pandas as pd
from datetime import datetime

from .analyzer_elt_utility import utilityELT
from ..ocp.analyzer_elt_cluster_apistats import apiStatsELT

logger = logging.getLogger(__name__)


class MetricELTRegistry:
    """Registry for metric-specific ELT modules"""
    
    def __init__(self):
        self._handlers = {}
        self._initialized = False
    
    def register(self, metric_type: str, elt_class, identifier_func=None):
        """
        Register a new metric type handler
        
        Args:
            metric_type: Name of the metric type (e.g., 'cluster_info', 'disk_io')
            elt_class: ELT class to handle this metric type
            identifier_func: Optional function to identify if data matches this type
        """
        self._handlers[metric_type] = {
            'class': elt_class,
            'instance': None,  # Lazy initialization
            'identifier': identifier_func
        }
    
    def get_handler(self, metric_type: str):
        """Get or create handler instance for metric type"""
        if metric_type not in self._handlers:
            return None
        
        handler_info = self._handlers[metric_type]
        if handler_info['instance'] is None:
            handler_info['instance'] = handler_info['class']()
        
        return handler_info['instance']
    
    def identify_metric_type(self, data: Dict[str, Any]) -> Optional[str]:
        """Identify metric type from data structure"""
        for metric_type, handler_info in self._handlers.items():
            identifier = handler_info.get('identifier')
            if identifier and identifier(data):
                return metric_type
        return None
    
    def list_registered_types(self) -> List[str]:
        """List all registered metric types"""
        return list(self._handlers.keys())


# Global registry instance
_registry = MetricELTRegistry()


def register_metric_handler(metric_type: str, elt_class, identifier_func=None):
    """
    Decorator or function to register new metric handlers
    
    Example usage:
        register_metric_handler('my_metric', MyMetricELT, lambda d: 'my_field' in d)
    """
    _registry.register(metric_type, elt_class, identifier_func)


class GenericELT(utilityELT):
    """
    Generic ELT orchestrator that delegates to specialized handlers
    This class should only contain orchestration logic, no metric-specific code
    """
    
    def __init__(self):
        super().__init__()
        self.registry = _registry
        self._ensure_handlers_registered()
    
    def _ensure_handlers_registered(self):
        """Lazy initialization of metric handlers"""
        if self.registry._initialized:
            return

        self.registry._initialized = True  

        # Import and register all metric handlers
        try:
            from ..ocp.analyzer_elt_cluster_info import clusterInfoELT
            register_metric_handler(
                'cluster_info', 
                clusterInfoELT,
                self._is_cluster_info
            )
        except ImportError as e:
            logger.warning(f"Could not import cluster_info handler: {e}")

        try:
            from ..etcd.analyzer_elt_cluster_status import etcdClusterStatusELT
            register_metric_handler(
                'etcd_cluster_status',
                etcdClusterStatusELT,
                self._is_etcd_cluster_status
            )
        except ImportError as e:
            logger.warning(f"Could not import etcd_cluster_status handler: {e}")

        try:
            from ..node.analyzer_elt_node_usage import nodeUsageELT
            register_metric_handler(
                'node_usage',
                nodeUsageELT,
                self._is_node_usage
            )
        except ImportError as e:
            logger.warning(f"Could not import disk_io handler: {e}")

        try:
            from ..pods.analyzer_elt_pods_usage import podsUsageELT
            register_metric_handler(
                'pods_usage',
                podsUsageELT,
                self._is_pods_usage
            )
        except ImportError as e:
            logger.warning(f"Could not import pods_usage handler: {e}")       

        try:
            from ..disk.analyzer_elt_disk_io import diskIOELT
            register_metric_handler(
                'disk_io',
                diskIOELT,
                self._is_disk_io
            )
        except ImportError as e:
            logger.warning(f"Could not import disk_io handler: {e}")

        # Register network_l1 handler
        try:
            from ..net.analyzer_elt_network_l1 import networkL1ELT
            register_metric_handler(
                'network_l1', 
                networkL1ELT,
                self._is_network_l1
            )
        except ImportError as e:
            logger.warning(f"Could not import network_l1 handler: {e}")
        
        # Register network_socket_tcp handler
        try:
            from ..net.analyzer_elt_network_socket4tcp import networkSocketTCPELT
            register_metric_handler(
                'network_socket_tcp',
                networkSocketTCPELT,
                self._is_network_socket_tcp
            )
        except ImportError as e:
            logger.warning(f"Could not import network_socket_tcp handler: {e}")
        
        # Register network_socket_udp handler
        try:
            from ..net.analyzer_elt_network_socket4udp import networkSocketUDPELT
            register_metric_handler(
                'network_socket_udp',
                networkSocketUDPELT,
                self._is_network_socket_udp
            )
        except ImportError as e:
            logger.warning(f"Could not import network_socket_udp handler: {e}")
        
        # Register network_socket_ip handler
        try:
            from ..net.analyzer_elt_network_socket4ip import networkSocketIPELT
            register_metric_handler(
                'network_socket_ip',
                networkSocketIPELT,
                self._is_network_socket_ip
            )
        except ImportError as e:
            logger.warning(f"Could not import network_socket_ip handler: {e}")
        
        # Register network_socket_mem handler - NEW
        try:
            from ..net.analyzer_elt_network_socket4mem import networkSocketMemELT
            register_metric_handler(
                'network_socket_mem',
                networkSocketMemELT,
                self._is_network_socket_mem
            )
        except ImportError as e:
            logger.warning(f"Could not import network_socket_mem handler: {e}")
        
        self.registry._initialized = True

        try:
            from ..net.analyzer_elt_network_socket4softnet import networkSocketSoftnetELT
            register_metric_handler(
                'network_socket_softnet',
                networkSocketSoftnetELT,
                self._is_network_socket_softnet
            )
        except ImportError as e:
            logger.warning(f"Could not import network_socket_softnet handler: {e}")

        try:
            from ..net.analyzer_elt_network_netstat4tcp import networkNetstatTCPELT
            register_metric_handler(
                'network_netstat_tcp',
                networkNetstatTCPELT,
                self._is_network_netstat_tcp
            )
        except ImportError as e:
            logger.warning(f"Could not import network_netstat_tcp handler: {e}")

        # Register network_netstat_udp handler
        try:
            from ..net.analyzer_elt_network_netstat4udp import networkNetstatUDPELT
            register_metric_handler(
                'network_netstat_udp',
                networkNetstatUDPELT,
                self._is_network_netstat_udp
            )
        except ImportError as e:
            logger.warning(f"Could not import network_netstat_udp handler: {e}")

        try:
            from ..net.analyzer_elt_network_io import networkIOELT
            register_metric_handler(
                'network_io',
                networkIOELT,
                self._is_network_io
            )
        except ImportError as e:
            logger.warning(f"Could not import network_io handler: {e}")


        try:
            from ..etcd.analyzer_elt_backend_commit import backendCommitELT
            register_metric_handler(
                'backend_commit',
                backendCommitELT,
                self._is_backend_commit
            )
        except ImportError as e:
            logger.warning(f"Could not import backend_commit handler: {e}")

        try:
            from ..etcd.analyzer_elt_compact_defrag import compactDefragELT
            register_metric_handler(
                'compact_defrag',
                compactDefragELT,
                self._is_compact_defrag
            )
        except ImportError as e:
            logger.warning(f"Could not import compact_defrag handler: {e}")

        try:
            from ..etcd.analyzer_elt_wal_fsync import diskWalFsyncELT
            register_metric_handler(
                'disk_wal_fsync',
                diskWalFsyncELT,
                self._is_disk_wal_fsync
            )
        except ImportError as e:
            logger.warning(f"Could not import disk_wal_fsync handler: {e}")

        try:
            from ..etcd.analyzer_elt_general_info import generalInfoELT
            register_metric_handler(
                'general_info',
                generalInfoELT,
                self._is_general_info
            )
        except ImportError as e:
            logger.warning(f"Could not import general_info handler: {e}")
     

        # Register api_stats BEFORE ovn_latency to ensure correct identification
        # (api_stats has category='api_server' which is more specific)
        try:
            from ..ocp.analyzer_elt_cluster_apistats import apiStatsELT
            register_metric_handler(
                'api_stats',
                apiStatsELT,
                self._is_api_stats
            )
        except ImportError as e:
            logger.warning(f"Could not import api_stats handler: {e}")

        try:
            from ..ovnk.analyzer_elt_latency import ovnkLatencyELT
            register_metric_handler(
                'ovn_latency',
                ovnkLatencyELT,
                self._is_ovn_latency
            )
        except ImportError as e:
            logger.warning(f"Could not import ovn_latency handler: {e}")

        try:
            from ..ovnk.analyzer_elt_ovs import ovsUsageELT
            register_metric_handler(
                'ovs_usage',
                ovsUsageELT,
                self._is_ovs_usage
            )
        except ImportError as e:
            logger.warning(f"Could not import ovs_usage handler: {e}")

        try:
            from ..ovnk.analyzer_elt_kubelet_cni import kubeletCNIELT
            register_metric_handler(
                'kubelet_cni',
                kubeletCNIELT,
                self._is_kubelet_cni
            )
        except ImportError as e:
            logger.warning(f"Could not import kubelet_cni handler: {e}")            

        try:
            from ..ocp.analyzer_elt_cluster_alert import clusterAlertELT
            register_metric_handler(
                'cluster_alert',
                clusterAlertELT,
                self._is_cluster_alert
            )
        except ImportError as e:
            logger.warning(f"Could not import cluster_alert handler: {e}")

    # ============================================================================
    # DATA TYPE IDENTIFICATION
    # ============================================================================
    
    def identify_data_type(self, data: Dict[str, Any]) -> str:
        """Identify the type of data from structure"""
        # Try registered handlers first
        metric_type = self.registry.identify_metric_type(data)
        if metric_type:
            logger.debug(f"Identified data type as: {metric_type}")
            return metric_type
        
        # Fallback to generic
        logger.debug(f"Could not identify data type, falling back to generic. Top-level keys: {list(data.keys())[:10]}")
        return 'generic'
    
    @staticmethod
    def _is_cluster_info(data: Dict[str, Any]) -> bool:
        """Identify cluster info data"""
        # Check for tool identifier
        if 'tool' in data and data.get('tool') == 'get_ocp_cluster_info':
            return True
        
        # Check nested structure
        if 'result' in data and isinstance(data.get('result'), dict):
            nested_data = data['result'].get('data', {})
            if ('cluster_name' in nested_data and 'cluster_version' in nested_data and 
                'master_nodes' in nested_data):
                return True
        
        # Check data nested structure
        if 'data' in data and isinstance(data.get('data'), dict):
            nested_data = data['data']
            if ('cluster_name' in nested_data and 'cluster_version' in nested_data and 
                ('master_nodes' in nested_data or 'total_nodes' in nested_data)):
                return True
        
        # Direct structure
        if ('cluster_name' in data and 'cluster_version' in data and 
            ('master_nodes' in data or 'total_nodes' in data)):
            return True
        
        return False

    @staticmethod
    def _is_etcd_cluster_status(data: Dict[str, Any]) -> bool:
        """Identify etcd cluster status data"""
        if 'etcd_pod' in data and 'cluster_health' in data:
            return True
        
        # Check nested structure
        if 'data' in data and isinstance(data.get('data'), dict):
            inner = data['data']
            if 'etcd_pod' in inner and 'cluster_health' in inner:
                return True
            if 'endpoint_status' in inner and 'member_status' in inner:
                return True
        
        # Check for cluster status indicators
        if ('endpoint_status' in data and 'member_status' in data and 
            'leader_info' in data):
            return True
        
        return False

    @staticmethod
    def _is_node_usage(data: Dict[str, Any]) -> bool:
        """Identify node usage data"""
        if 'category' in data and data.get('category') == 'node_usage':
            return True
        
        # Check nested structure
        if 'data' in data and isinstance(data.get('data'), dict):
            inner = data['data']
            if 'category' in inner and inner.get('category') == 'node_usage':
                return True
            # Check for node_groups structure (new format)
            if 'node_groups' in inner and isinstance(inner.get('node_groups'), dict):
                return True
            # Check for metrics with node usage indicators
            metrics = inner.get('metrics', {})
            if isinstance(metrics, dict):
                if any(k in metrics for k in ['cpu_usage', 'memory_used', 'cgroup_cpu_usage', 'cgroup_rss_usage']):
                    return True
        
        # Direct metrics check
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            if any(k in data['metrics'] for k in ['cpu_usage', 'memory_used', 'cgroup_cpu_usage', 'cgroup_rss_usage']):
                return True
        
        # Check for node_groups at top level (new format)
        if 'node_groups' in data and isinstance(data.get('node_groups'), dict):
            return True
        
        # Check for node_group in query_params
        if 'query_params' in data and isinstance(data.get('query_params'), dict):
            if 'node_group' in data['query_params']:
                return True
        
        return False

    @staticmethod
    def _is_pods_usage(data: Dict[str, Any]) -> bool:
        """Identify pods usage data"""
        if 'category' in data and data.get('category') == 'ovnk_pods_usage':
            return True
        
        # Check nested structure
        if 'data' in data and isinstance(data.get('data'), dict):
            inner = data['data']
            if 'ovnkube_node_containers' in inner:
                return True
            # Check for namespace and metrics structure
            if 'namespace' in inner and 'metrics' in inner:
                metrics = inner.get('metrics', {})
                if any('container_cpu_usage' in k or 'container_memory' in k for k in metrics.keys()):
                    return True
        
        # Direct structure check
        if 'ovnkube_node_containers' in data:
            return True
        
        return False

    @staticmethod
    def _is_disk_io(data: Dict[str, Any]) -> bool:
        """Identify disk I/O data"""
        if 'category' in data and data.get('category') == 'disk_io':
            return True
        
        # Check nested structure
        if 'data' in data and isinstance(data.get('data'), dict):
            inner = data['data']
            if 'category' in inner and inner.get('category') == 'disk_io':
                return True
            # Check for metrics with disk_io prefix
            metrics = inner.get('metrics', {})
            if isinstance(metrics, dict):
                if any('disk_io' in k for k in metrics.keys()):
                    return True
        
        # Direct metrics check
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            if any('disk_io' in k for k in data['metrics'].keys()):
                return True
        
        return False
        
    # Add this static method to GenericELT class after _is_cluster_info
    @staticmethod
    def _is_network_l1(data: Dict[str, Any]) -> bool:
        """Identify network L1 data"""
        if 'category' in data and data.get('category') == 'network_l1':
            return True
        if 'node_groups' in data and isinstance(data.get('node_groups'), dict):
            node_groups = data['node_groups']
            for role_data in node_groups.values():
                if isinstance(role_data, dict) and 'metrics' in role_data:
                    metrics = role_data['metrics']
                    if any('network_l1' in str(k) for k in metrics.keys()):
                        return True
        return False

    @staticmethod
    def _is_network_socket_tcp(data: Dict[str, Any]) -> bool:
        """Identify network socket TCP data"""
        if 'category' in data and data.get('category') == 'network_socket_tcp':
            return True
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            metric_keys = data['metrics'].keys()
            if any('socket_tcp' in k or 'sockstat' in k for k in metric_keys):
                return True
        if 'nodes' in data and isinstance(data.get('nodes'), dict):
            return True
        return False

    @staticmethod
    def _is_network_socket_udp(data: Dict[str, Any]) -> bool:
        """Identify network socket UDP data"""
        if 'category' in data and data.get('category') == 'network_socket_udp':
            return True
        if 'metrics' in data and isinstance(data.get('metrics'), list):
            for metric in data['metrics']:
                if isinstance(metric, dict):
                    metric_name = metric.get('metric', '')
                    if 'udp' in metric_name.lower():
                        return True
        return False

    @staticmethod
    def _is_network_socket_ip(data: Dict[str, Any]) -> bool:
        """Identify network socket IP data"""
        if 'category' in data and data.get('category') in ['network_netstat_ip', 'network_socket_ip']:
            return True
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            metric_keys = data['metrics'].keys()
            if any('netstat_ip' in k or 'Icmp' in k for k in metric_keys):
                return True
        return False

    @staticmethod
    def _is_network_socket_mem(data: Dict[str, Any]) -> bool:
        """Identify network socket memory data - NEW"""
        if 'category' in data and data.get('category') == 'network_socket_mem':
            return True
        if 'metrics' in data and isinstance(data.get('metrics'), list):
            for metric in data['metrics']:
                if isinstance(metric, dict):
                    metric_name = metric.get('metric', '')
                    if any(keyword in metric_name for keyword in ['sockstat', 'TCP_Kernel', 'UDP_Kernel', 'FRAG_memory']):
                        return True
        return False

    @staticmethod
    def _is_network_socket_softnet(data: Dict[str, Any]) -> bool:
        """Identify network socket softnet data"""
        if 'category' in data and data.get('category') == 'network_socket_softnet':
            return True
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            metric_keys = data['metrics'].keys()
            if any('softnet' in k for k in metric_keys):
                return True
        return False

    @staticmethod
    def _is_network_netstat_tcp(data: Dict[str, Any]) -> bool:
        """Identify network TCP netstat data"""
        if 'category' in data and data.get('category') == 'network_netstat_tcp':
            return True
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            metric_keys = data['metrics'].keys()
            if any('netstat_tcp' in k or 'node_netstat_Tcp' in k or 'node_tcp_sync' in k for k in metric_keys):
                return True
        return False

    @staticmethod
    def _is_network_netstat_udp(data: Dict[str, Any]) -> bool:
        """Identify network UDP netstat data - NEW"""
        if 'category' in data and data.get('category') == 'network_netstat_udp':
            return True
        if 'metrics' in data and isinstance(data.get('metrics'), list):
            for metric in data['metrics']:
                if isinstance(metric, dict):
                    metric_name = metric.get('metric', '')
                    if any(keyword in metric_name for keyword in ['udp_error', 'nestat_udp', 'netstat_udp']):
                        return True
        return False

    @staticmethod
    def _is_network_io(data: Dict[str, Any]) -> bool:
        """Identify network IO data"""
        if 'category' in data and data.get('category') == 'network_io':
            return True
        if 'data' in data and isinstance(data.get('data'), dict):
            nested_data = data['data']
            if 'category' in nested_data and nested_data.get('category') == 'network_io':
                return True
            if 'metrics' in nested_data and isinstance(nested_data.get('metrics'), dict):
                metric_keys = nested_data['metrics'].keys()
                if any('network_io' in k for k in metric_keys):
                    return True
        return False

    @staticmethod
    def _is_backend_commit(data: Dict[str, Any]) -> bool:
        """Identify backend commit data"""
        if 'category' in data and data.get('category') == 'disk_backend_commit':
            return True
        
        # Check for nested structure
        if 'data' in data and isinstance(data.get('data'), dict):
            inner = data['data']
            if 'category' in inner and inner.get('category') == 'disk_backend_commit':
                return True
            # Check for pods_metrics or metrics with backend commit metric names
            metrics = inner.get('metrics') or inner.get('pods_metrics')
            if isinstance(metrics, dict):
                if any('backend_commit' in k or 'disk_backend_commit' in k 
                       for k in metrics.keys()):
                    return True
        
        # Direct metrics check
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            if any('backend_commit' in k or 'disk_backend_commit' in k 
                   for k in data['metrics'].keys()):
                return True
        
        if 'pods_metrics' in data and isinstance(data.get('pods_metrics'), dict):
            if any('backend_commit' in k or 'disk_backend_commit' in k 
                   for k in data['pods_metrics'].keys()):
                return True
        
        return False

    @staticmethod
    def _is_disk_wal_fsync(data: Dict[str, Any]) -> bool:
        """Identify disk WAL fsync data"""
        if 'category' in data and data.get('category') == 'disk_wal_fsync':
            return True
        
        if 'data' in data and isinstance(data.get('data'), dict):
            inner = data['data']
            if 'category' in inner and inner.get('category') == 'disk_wal_fsync':
                return True
            metrics = inner.get('metrics', {})
            if isinstance(metrics, dict):
                if any('disk_wal_fsync' in k for k in metrics.keys()):
                    return True
        
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            if any('disk_wal_fsync' in k for k in data['metrics'].keys()):
                return True
        
        return False

    @staticmethod
    def _is_compact_defrag(data: Dict[str, Any]) -> bool:
        """Identify compact defrag data"""
        if 'category' in data and data.get('category') == 'compact_defrag':
            return True
        
        # Check nested structure
        if 'data' in data and isinstance(data.get('data'), dict):
            inner = data['data']
            if 'category' in inner and inner.get('category') == 'compact_defrag':
                return True
            # Check for metrics with compact_defrag indicators
            metrics = inner.get('metrics') or inner.get('pods_metrics')
            if isinstance(metrics, dict):
                if any('compact_defrag' in k or 'compact' in k.lower() or 'defrag' in k.lower()
                       for k in metrics.keys()):
                    return True
        
        # Direct metrics check
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            if any('compact_defrag' in k or 'compact' in k.lower() or 'defrag' in k.lower()
                   for k in data['metrics'].keys()):
                return True
        
        if 'pods_metrics' in data and isinstance(data.get('pods_metrics'), dict):
            if any('compact_defrag' in k or 'compact' in k.lower() or 'defrag' in k.lower()
                   for k in data['pods_metrics'].keys()):
                return True
        
        return False

    @staticmethod
    def _is_general_info(data: Dict[str, Any]) -> bool:
        """Identify general info data"""
        # Check top-level category
        if 'category' in data and data.get('category') == 'general_info':
            return True
        
        # Check nested structure - prioritize checking inner data first
        if 'data' in data and isinstance(data.get('data'), dict):
            inner = data['data']
            # Check category in nested data
            if 'category' in inner and inner.get('category') == 'general_info':
                return True
            # Check for pod_metrics (strong indicator of general_info)
            if 'pod_metrics' in inner and isinstance(inner.get('pod_metrics'), dict):
                # Additional validation: check if pod_metrics contains expected structure
                pod_metrics = inner['pod_metrics']
                # Check for common general_info metric keys
                if any(key in pod_metrics for key in [
                    'etcd_pods_cpu_usage', 'etcd_pods_memory_usage', 
                    'etcd_db_space_used_percent', 'proposal_failure_rate',
                    'apiserver_storage_objects_max_top20'
                ]):
                    return True
                # If pod_metrics exists and is not empty, it's likely general_info
                if pod_metrics:
                    return True
        
        # Direct pod_metrics check
        if 'pod_metrics' in data and isinstance(data.get('pod_metrics'), dict):
            return True
        
        return False

    @staticmethod
    def _is_ovn_latency(data: Dict[str, Any]) -> bool:
        """Identify OVN latency data"""
        # Exclude api_server category - it should be handled by api_stats
        if 'category' in data and data.get('category') == 'api_server':
            return False
        
        if 'category' in data and data.get('category') == 'latency':
            return True
        
        # Check nested structure
        if 'data' in data and isinstance(data.get('data'), dict):
            inner = data['data']
            # Exclude api_server category
            if 'category' in inner and inner.get('category') == 'api_server':
                return False
            if 'category' in inner and inner.get('category') == 'latency':
                return True
            # Check for metrics with latency-specific names (but not API server metrics)
            metrics = inner.get('metrics', {})
            if isinstance(metrics, dict):
                # Check for OVN-specific latency metrics (cni_request, ovn, etc.)
                if any('cni_request' in k or 'ovn' in k.lower() or 
                       ('latency' in k and 'api' not in k.lower()) for k in metrics.keys()):
                    return True
        
        # Direct metrics check
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            # Check for OVN-specific latency metrics
            if any('cni_request' in k or 'ovn' in k.lower() or 
                   ('latency' in k and 'api' not in k.lower()) for k in data['metrics'].keys()):
                return True
        
        return False

    @staticmethod
    def _is_ovs_usage(data: Dict[str, Any]) -> bool:
        """Identify OVS usage data"""
        # Check top-level category (both 'ovs' and 'ovs_metrics')
        if 'category' in data:
            category = data.get('category')
            if category in ['ovs', 'ovs_metrics']:
                return True
        
        # Check nested structure
        if 'data' in data and isinstance(data.get('data'), dict):
            inner = data['data']
            if 'category' in inner:
                inner_category = inner.get('category')
                if inner_category in ['ovs', 'ovs_metrics']:
                    return True
            # Check for metrics with OVS indicators
            metrics = inner.get('metrics', {})
            if isinstance(metrics, dict):
                if any('ovs' in k.lower() or 'ovsdb' in k.lower() 
                    for k in metrics.keys()):
                    return True
        
        # Direct metrics check
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            if any('ovs' in k.lower() or 'ovsdb' in k.lower() 
                for k in data['metrics'].keys()):
                return True
        
        return False

    @staticmethod
    def _is_kubelet_cni(data: Dict[str, Any]) -> bool:
        """Identify kubelet CNI data"""
        if 'category' in data and data.get('category') == 'cni':
            return True
        
        # Check nested structure
        if 'data' in data and isinstance(data.get('data'), dict):
            inner = data['data']
            if 'category' in inner and inner.get('category') == 'cni':
                return True
            # Check for metrics with CNI indicators
            metrics = inner.get('metrics', {})
            if isinstance(metrics, dict):
                if any(k.startswith('cni_') or k.startswith('crio_') or 'container_threads' in k 
                    for k in metrics.keys()):
                    return True
        
        # Direct metrics check
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            if any(k.startswith('cni_') or k.startswith('crio_') or 'container_threads' in k 
                for k in data['metrics'].keys()):
                return True
        
        return False

    @staticmethod
    def _is_api_stats(data: Dict[str, Any]) -> bool:
        """Identify API server stats data"""
        # Check category first (most specific identifier)
        if 'category' in data and data.get('category') == 'api_server':
            return True
        
        # Check nested structure
        if 'data' in data and isinstance(data.get('data'), dict):
            inner = data['data']
            # Check category first - this is the most reliable identifier
            if 'category' in inner and inner.get('category') == 'api_server':
                return True
            # Check for cluster_summary which is unique to API stats
            if 'cluster_summary' in inner and isinstance(inner.get('cluster_summary'), dict):
                cluster_summary = inner['cluster_summary']
                if 'performance_indicators' in cluster_summary:
                    return True
            # Check for metrics with api server indicators
            metrics = inner.get('metrics', {})
            if isinstance(metrics, dict):
                if any('apicalls' in k or 'api_request' in k or 'api_server' in k 
                    for k in metrics.keys()):
                    return True
        
        # Check for cluster_summary at top level (unique to API stats)
        if 'cluster_summary' in data and isinstance(data.get('cluster_summary'), dict):
            cluster_summary = data['cluster_summary']
            if 'performance_indicators' in cluster_summary:
                return True
        
        # Direct metrics check (less reliable, but fallback)
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            if any('apicalls' in k or 'api_request' in k or 'api_server' in k 
                for k in data['metrics'].keys()):
                return True
        
        return False

    @staticmethod
    def _is_cluster_alert(data: Dict[str, Any]) -> bool:
        """Identify cluster alert data"""
        if 'category' in data and data.get('category') == 'alerts':
            return True
        
        # Check nested structure
        if 'data' in data and isinstance(data.get('data'), dict):
            inner = data['data']
            if 'category' in inner and inner.get('category') == 'alerts':
                return True
            # Check for metrics with alert indicators
            metrics = inner.get('metrics', {})
            if isinstance(metrics, dict):
                if 'top_alerts' in metrics:
                    return True
        
        # Direct metrics check
        if 'metrics' in data and isinstance(data.get('metrics'), dict):
            if 'top_alerts' in data['metrics']:
                return True
        
        return False

    # ============================================================================
    # MAIN PROCESSING PIPELINE
    # ============================================================================

    def process_data(self, data: Union[Dict[str, Any], str]) -> Dict[str, Any]:
        """
        Main entry point for processing any metric data
        Returns standardized result dictionary
        """
        try:
            # Parse JSON if string
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError as e:
                    return self._error_response(f"Invalid JSON: {e}")
            
            if not isinstance(data, dict):
                return self._error_response("Input must be dictionary or JSON string")
            
            # Identify data type
            data_type = self.identify_data_type(data)
            logger.debug(f"Data type identified: {data_type}")
            
            # Get appropriate handler
            handler = self.registry.get_handler(data_type)
            
            if handler is None:
                logger.warning(f"No handler found for data_type: {data_type}. Available handlers: {self.registry.list_registered_types()}")
                # No specific handler, use generic processing
                return self._process_generic(data, data_type)
            
            logger.debug(f"Using handler: {type(handler).__name__} for data_type: {data_type}")
            # Delegate to specialized handler
            return self._process_with_handler(data, data_type, handler)
            
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            return self._error_response(str(e))
    
    def _process_with_handler(self, data: Dict[str, Any], data_type: str, handler) -> Dict[str, Any]:
        """Process data using specialized handler"""
        try:
            # Extract nested data if needed
            actual_data = self._extract_actual_data(data, data_type)
            logger.debug(f"Extracted actual_data for {data_type}. Keys: {list(actual_data.keys())[:10]}")
            
            # Use handler's methods with appropriate method names
            if data_type == 'cluster_info':
                structured_data = handler.extract_cluster_info(actual_data) if hasattr(handler, 'extract_cluster_info') else {}
                summary_method = 'summarize_cluster_info'
            elif data_type == 'etcd_cluster_status':  # NEW
                structured_data = handler.extract_cluster_status(actual_data) if hasattr(handler, 'extract_cluster_status') else {}
                summary_method = 'summarize_cluster_status'
            elif data_type == 'node_usage':
                structured_data = handler.extract_node_usage(actual_data) if hasattr(handler, 'extract_node_usage') else {}
                summary_method = 'summarize_node_usage' 
            elif data_type == 'pods_usage':
                structured_data = handler.extract_pods_usage(actual_data) if hasattr(handler, 'extract_pods_usage') else {}
                summary_method = 'summarize_pods_usage'
            elif data_type == 'disk_io':
                structured_data = handler.extract_disk_io(actual_data) if hasattr(handler, 'extract_disk_io') else {}
                summary_method = 'summarize_disk_io'                
            elif data_type == 'network_l1':
                structured_data = handler.extract_network_l1(actual_data) if hasattr(handler, 'extract_network_l1') else {}
                summary_method = 'summarize_network_l1'
            elif data_type == 'network_socket_tcp':
                structured_data = handler.extract_network_socket_tcp(actual_data) if hasattr(handler, 'extract_network_socket_tcp') else {}
                summary_method = 'summarize_network_socket_tcp'
            elif data_type == 'network_socket_udp':
                structured_data = handler.extract_network_socket_udp(actual_data) if hasattr(handler, 'extract_network_socket_udp') else {}
                summary_method = 'summarize_network_socket_udp'
            elif data_type == 'network_socket_ip':
                structured_data = handler.extract_network_socket_ip(actual_data) if hasattr(handler, 'extract_network_socket_ip') else {}
                summary_method = 'summarize_network_socket_ip'
            elif data_type == 'network_socket_mem':
                structured_data = handler.extract_network_socket_mem(actual_data) if hasattr(handler, 'extract_network_socket_mem') else {}
                summary_method = 'summarize_network_socket_mem'
            elif data_type == 'network_socket_softnet':
                structured_data = handler.extract_network_socket_softnet(actual_data) if hasattr(handler, 'extract_network_socket_softnet') else {}
                summary_method = 'summarize_network_socket_softnet'
            elif data_type == 'network_netstat_tcp':
                structured_data = handler.extract_network_netstat_tcp(actual_data) if hasattr(handler, 'extract_network_netstat_tcp') else {}
                summary_method = 'summarize_network_netstat_tcp'
            elif data_type == 'network_netstat_udp':  # NEW
                structured_data = handler.extract_network_netstat_udp(actual_data) if hasattr(handler, 'extract_network_netstat_udp') else {}
                summary_method = 'summarize_network_netstat_udp'
            elif data_type == 'network_io':
                structured_data = handler.extract_network_io(actual_data) if hasattr(handler, 'extract_network_io') else {}
                summary_method = 'summarize_network_io'                              
            elif data_type == 'backend_commit':
                structured_data = handler.extract_backend_commit(actual_data) if hasattr(handler, 'extract_backend_commit') else {}
                summary_method = 'summarize_backend_commit'
            elif data_type == 'compact_defrag':
                structured_data = handler.extract_compact_defrag(actual_data) if hasattr(handler, 'extract_compact_defrag') else {}
                summary_method = 'summarize_compact_defrag' 
            elif data_type == 'disk_wal_fsync':
                structured_data = handler.extract_wal_fsync(actual_data) if hasattr(handler, 'extract_wal_fsync') else {}
                summary_method = 'summarize_wal_fsync'
            elif data_type == 'general_info':
                structured_data = handler.extract_general_info(actual_data) if hasattr(handler, 'extract_general_info') else {}
                summary_method = 'summarize_general_info'
            elif data_type == 'ovn_latency':
                structured_data = handler.extract_ovn_latency(actual_data) if hasattr(handler, 'extract_ovn_latency') else {}
                summary_method = 'summarize_ovn_latency'
            elif data_type == 'ovs_usage':
                structured_data = handler.extract_ovs_usage(actual_data) if hasattr(handler, 'extract_ovs_usage') else {}
                summary_method = 'summarize_ovs_usage'
            elif data_type == 'kubelet_cni':
                structured_data = handler.extract_kubelet_cni(actual_data) if hasattr(handler, 'extract_kubelet_cni') else {}
                summary_method = 'summarize_kubelet_cni'
            elif data_type == 'api_stats':
                structured_data = handler.extract_api_stats(actual_data) if hasattr(handler, 'extract_api_stats') else {}
                summary_method = 'summarize_api_stats'
            elif data_type == 'cluster_alert':
                structured_data = handler.extract_cluster_alert(actual_data) if hasattr(handler, 'extract_cluster_alert') else {}
                summary_method = 'summarize_cluster_alert'
            else:
                # Generic fallback
                structured_data = handler.extract_cluster_info(actual_data) if hasattr(handler, 'extract_cluster_info') else {}
                summary_method = 'summarize_cluster_info'
            
            dataframes = handler.transform_to_dataframes(structured_data) if hasattr(handler, 'transform_to_dataframes') else {}
            html_tables = handler.generate_html_tables(dataframes) if hasattr(handler, 'generate_html_tables') else {}
            
            # Generate summary
            if hasattr(handler, summary_method):
                summary = getattr(handler, summary_method)(structured_data)
            else:
                summary = self._generate_generic_summary(structured_data, data_type)
            
            return {
                'success': True,
                'data_type': data_type,
                'summary': summary,
                'html_tables': html_tables,
                'dataframes': dataframes,
                'structured_data': structured_data,
                'timestamp': data.get('timestamp', data.get('collection_timestamp', datetime.now().isoformat()))
            }
            
        except Exception as e:
            logger.error(f"Error in handler processing: {e}")
            return self._error_response(str(e))

    def _extract_actual_data(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Extract actual data from nested structures"""
        # Handle common nesting patterns
        if data_type == 'cluster_info':
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']
            elif 'data' in data and isinstance(data.get('data'), dict):
                return data['data']

        # Unwrap for etcd_cluster_status - NEW
        if data_type == 'etcd_cluster_status':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        if data_type == 'node_usage':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        if data_type == 'pods_usage':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        if data_type == 'disk_io':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        # Unwrap typical wrappers for network_l1 as well
        if data_type == 'network_l1':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        # Unwrap for network_socket_tcp
        if data_type == 'network_socket_tcp':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']
        
        # Unwrap for network_socket_udp
        if data_type == 'network_socket_udp':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']
        
        # Unwrap for network_socket_ip
        if data_type == 'network_socket_ip':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']
        
        # Unwrap for network_socket_mem
        if data_type == 'network_socket_mem':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']
        
        # Unwrap for network_socket_softnet
        if data_type == 'network_socket_softnet':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        # Unwrap for network_netstat_tcp
        if data_type == 'network_netstat_tcp':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']
        
        # Unwrap for network_netstat_udp - NEW
        if data_type == 'network_netstat_udp':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        if data_type == 'network_io':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        if data_type == 'backend_commit':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        if data_type == 'compact_defrag':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        if data_type == 'disk_wal_fsync':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        if data_type == 'general_info':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        if data_type == 'ovn_latency':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        if data_type == 'ovs_usage':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        if data_type == 'kubelet_cni':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']                

        if data_type == 'api_stats':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']

        if data_type == 'cluster_alert':
            if 'data' in data and isinstance(data.get('data'), dict):
                return data['data']
            if 'result' in data and isinstance(data.get('result'), dict) and 'data' in data['result']:
                return data['result']['data']
        
        return data


    def _process_generic(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Generic processing for unknown data types"""
        try:
            structured_data = self._extract_generic_fields(data)
            df = pd.DataFrame(structured_data)
            
            if not df.empty:
                df = self.limit_dataframe_columns(df)
            
            html_table = self.create_html_table(df, 'generic_data') if not df.empty else ""
            
            return {
                'success': True,
                'data_type': 'generic',
                'summary': f"Generic data with {len(structured_data)} fields",
                'html_tables': {'generic_data': html_table} if html_table else {},
                'dataframes': {'generic_data': df} if not df.empty else {},
                'structured_data': structured_data,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            return self._error_response(f"Generic processing failed: {e}")
    
    def _extract_generic_fields(self, data: Dict[str, Any], max_fields: int = 20) -> List[Dict[str, Any]]:
        """Extract important fields from generic data"""
        fields = []
        
        priority_keys = [
            'name', 'status', 'version', 'timestamp', 'count', 'total',
            'health', 'error', 'message', 'value', 'metric', 'result'
        ]
        
        # Add priority fields first
        for key in priority_keys:
            if key in data:
                fields.append({
                    'Property': key.replace('_', ' ').title(),
                    'Value': self._format_generic_value(data[key])
                })
        
        # Add remaining fields
        remaining_keys = [k for k in data.keys() if k not in priority_keys]
        for key in remaining_keys:
            if len(fields) >= max_fields:
                break
            fields.append({
                'Property': key.replace('_', ' ').title(),
                'Value': self._format_generic_value(data[key])
            })
        
        return fields
    
    def _format_generic_value(self, value: Any) -> str:
        """Format value for generic display"""
        if isinstance(value, dict):
            return f"Dict({len(value)} keys)"
        elif isinstance(value, list):
            return f"List({len(value)} items)"
        else:
            value_str = str(value)
            return value_str[:100] + '...' if len(value_str) > 100 else value_str
    
    def _generate_generic_summary(self, data: Dict[str, Any], data_type: str) -> str:
        """Generate generic summary"""
        summary_parts = [f"Data Type: {data_type.replace('_', ' ').title()}"]
        
        if isinstance(data, dict):
            summary_parts.append(f"Sections: {len(data)}")
            
            # Count total items
            total_items = sum(len(v) if isinstance(v, list) else 1 for v in data.values())
            summary_parts.append(f"Total Items: {total_items}")
        
        return " | ".join(summary_parts)
    
    def _error_response(self, error_msg: str) -> Dict[str, Any]:
        """Standard error response"""
        return {
            'success': False,
            'error': error_msg,
            'data_type': 'error',
            'summary': f"Error: {error_msg}",
            'html_tables': {},
            'dataframes': {},
            'structured_data': {}
        }


# ============================================================================
# PUBLIC API FUNCTIONS
# ============================================================================

def convert_json_to_html_table(json_data: Union[Dict[str, Any], str], 
                               compact: bool = True,
                               two_column: bool = False) -> str:
    """
    Convert JSON data to HTML table format
    
    Args:
        json_data: Dictionary or JSON string
        compact: Use compact display mode
        two_column: Limit to 2-column layout
        
    Returns:
        HTML string with formatted tables
    """
    try:
        elt = GenericELT()
        result = elt.process_data(json_data)
        
        if not result.get('success'):
            return f"<div class='alert alert-danger'>Error: {result.get('error')}</div>"
        
        # Build HTML output
        output_parts = []
        
        # Data type badge
        data_type = result.get('data_type', 'unknown')
        if data_type != 'generic':
            type_display = data_type.replace('_', ' ').title()
            output_parts.append(
                f"<div class='mb-3'>"
                f"<span class='badge badge-info' style='font-size: 1.0rem; font-weight: 600'>"
                f"{type_display}"
                f"</span>"
                f"</div>"
            )
        
        # Summary
        summary = result.get('summary', '')
        if summary:
            summary_decoded = elt.decode_unicode_escapes(str(summary))
            if summary_decoded.strip().startswith('<'):
                output_parts.append(f"<div class='alert alert-light'>{summary_decoded}</div>")
            else:
                def escape_html(s: str) -> str:
                    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                
                safe_summary = escape_html(summary_decoded)
                output_parts.append(f"<div class='alert alert-light'>{safe_summary}</div>")
        
        # Tables
        html_tables = result.get('html_tables', {})
        for table_name, table_html in html_tables.items():
            table_title = table_name.replace('_', ' ').title()
            output_parts.append(f"<h5 class='mt-3'>{table_title}</h5>")
            output_parts.append(table_html)
        
        final_html = ''.join(output_parts)
        final_html = elt.decode_unicode_escapes(final_html)
        
        return final_html
        
    except Exception as e:
        logger.error(f"Error in convert_json_to_html_table: {e}")
        return f"<div class='alert alert-danger'>Error: {str(e)}</div>"


def process_metric_data(metric_data: Union[Dict[str, Any], str], 
                       metric_type: str = None) -> Dict[str, Any]:
    """
    Process metric data with optional type hint
    
    Args:
        metric_data: Dictionary or JSON string containing metric data
        metric_type: Optional type hint (will auto-detect if not provided)
        
    Returns:
        Dictionary with processed results
    """
    elt = GenericELT()
    return elt.process_data(metric_data)


def extract_and_transform_results(results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Legacy API compatibility function
    Extract and transform tool results into tables and summaries
    """
    return process_metric_data(results)


# ============================================================================
# EXTENSION EXAMPLES
# ============================================================================

def register_new_metric_handler_example():
    """
    Example showing how to register a new metric handler
    This would be called during module initialization
    """
    # Example: Register disk I/O handler
    # from .metrics.analyzer_elt_disk_io import diskIOELT
    # 
    # def is_disk_io(data):
    #     return (
    #         'tool' in data and data.get('tool') == 'collect_disk_io_metrics'
    #         or 'category' in data and data.get('category') == 'disk_io'
    #     )
    # 
    # register_metric_handler('disk_io', diskIOELT, is_disk_io)
    pass


__all__ = [
    'GenericELT',
    'MetricELTRegistry',
    'register_metric_handler',
    'convert_json_to_html_table',
    'process_metric_data',
    'extract_and_transform_results',
]