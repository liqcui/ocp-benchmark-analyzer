#!/usr/bin/env python3
"""
OVNK Benchmark Prometheus Utilities (PromQL) - Updated
Enhanced with better node grouping and common utility functions
"""

import asyncio
import json
import subprocess
import os
from typing import Dict, List, Any, Optional, Set
from kubernetes import client
from kubernetes.client.rest import ApiException


class mcpToolsUtility:
    """Utility class for node information and grouping operations"""
    
    def __init__(self, auth_client=None):
        self.auth_client = auth_client
        self.node_cache = {}
        self.cache_timestamp = None
        self.cache_ttl = 300  # 5 minutes cache TTL
        
        # Node role detection labels in priority order
        self.role_labels = [
            'node-role.kubernetes.io/control-plane',
            'node-role.kubernetes.io/master', 
            'node-role.kubernetes.io/infra',
            'node-role.kubernetes.io/worker'
        ]
        
        # Role mapping for consistency
        self.role_mapping = {
            'control-plane': 'controlplane',
            'master': 'controlplane',
            'infra': 'infra',
            'worker': 'worker',
            'workload': 'workload'
        }
    
    # ==================== Common Prometheus Utility Functions ====================
    
    @staticmethod
    def extract_token_from_config(prometheus_config: Dict[str, Any]) -> Optional[str]:
        """Extract bearer token from prometheus configuration
        
        Args:
            prometheus_config: Prometheus configuration dictionary
            
        Returns:
            Bearer token string or None
        """
        cfg = prometheus_config or {}
        token = cfg.get('token') or cfg.get('bearer_token')
        if not token:
            headers = cfg.get('headers') or {}
            auth = headers.get('Authorization') or headers.get('authorization')
            if isinstance(auth, str) and auth.lower().startswith('bearer '):
                token = auth[7:]
        return token
    
    @staticmethod
    def build_prometheus_config(prometheus_config: Dict[str, Any], 
                                auth_client=None) -> Dict[str, Any]:
        """Build normalized Prometheus configuration with auth headers
        
        Args:
            prometheus_config: Base prometheus configuration
            auth_client: Optional auth client with token
            
        Returns:
            Normalized configuration dictionary
        """
        cfg: Dict[str, Any] = dict(prometheus_config or {})
        
        # Get URL from multiple possible sources
        url = cfg.get('url') or cfg.get('base_url') or os.getenv('PROMETHEUS_URL')
        if url:
            cfg['url'] = url.rstrip('/')
        else:
            return {}
        
        # Attach bearer token header if available
        token: Optional[str] = None
        if isinstance(auth_client, dict):
            token = auth_client.get('token') or auth_client.get('bearer_token')
        elif auth_client:
            token = getattr(auth_client, 'token', None) or getattr(auth_client, 'bearer_token', None)
        
        if token:
            headers = dict(cfg.get('headers', {}))
            headers.setdefault('Authorization', f'Bearer {token}')
            cfg['headers'] = headers
        
        return cfg
    
    @staticmethod
    def get_node_pattern(nodes: List[str]) -> str:
        """Generate regex pattern for Prometheus node name matching
        
        For Prometheus regex patterns, we don't escape dots because:
        1. Dots in regex match any character, which is fine for FQDNs
        2. Escaping creates parse errors in PromQL
        3. The alternation (|) provides specificity
        
        Args:
            nodes: List of node names
            
        Returns:
            Regex pattern string for PromQL
        """
        if not nodes:
            return "^$"  # Match nothing
        
        # Don't escape dots - they work fine in PromQL regex patterns
        # Just join with pipe for alternation
        return '|'.join(nodes)
    
    @staticmethod
    def calculate_time_series_stats(values: List[float]) -> Dict[str, float]:
        """Calculate statistics from time series values
        
        Args:
            values: List of numeric values from time series
            
        Returns:
            Dictionary with 'avg' and 'max' keys
        """
        if not values:
            return {'avg': 0.0, 'max': 0.0}
        return {
            'avg': round(sum(values) / len(values), 2),
            'max': round(max(values), 2)
        }
    
    @staticmethod
    def extract_numeric_values(time_series_values: List[tuple]) -> List[float]:
        """Extract numeric values from Prometheus time series data
        
        Args:
            time_series_values: List of (timestamp, value) tuples
            
        Returns:
            List of valid float values
        """
        numeric_values = []
        for ts, val in time_series_values:
            try:
                numeric_values.append(float(val))
            except (ValueError, TypeError):
                pass
        return numeric_values
    
    @staticmethod
    def bytes_to_gb(bytes_value: float) -> float:
        """Convert bytes to gigabytes
        
        Args:
            bytes_value: Value in bytes
            
        Returns:
            Value in GB rounded to 2 decimals
        """
        return round(bytes_value / (1024**3), 2)
    
    @staticmethod
    def extract_cgroup_name(cgroup_id: str) -> str:
        """Extract cgroup name from full cgroup ID path
        
        Args:
            cgroup_id: Full cgroup ID (e.g., '/system.slice/kubelet.service')
            
        Returns:
            Short cgroup name (e.g., 'kubelet.service')
        """
        return cgroup_id.split('/')[-1] if '/' in cgroup_id else cgroup_id
    
    # ==================== Node Resolution Functions ====================
    
    async def resolve_node_from_instance(self, instance: str, prometheus_client=None) -> str:
        """Resolve a Prometheus instance label to a node name
        
        Args:
            instance: Instance label from Prometheus (e.g., "10.0.0.1:9100" or "node-1.example.com:9100")
            prometheus_client: Optional Prometheus client for label lookup
            
        Returns:
            Resolved node name or 'unknown'
        """
        if not instance:
            return 'unknown'
        
        # Remove port if present
        base_instance = instance.split(':')[0]
        
        # Get all node labels to try matching
        node_labels = await self.get_all_node_labels(prometheus_client)
        
        # Try direct match first (exact or short name)
        variations = self.normalize_node_name(base_instance)
        for variation in variations:
            if variation in node_labels:
                return variation
        
        # Try to find node by checking if instance is contained in any node name
        for node_name in node_labels.keys():
            if base_instance in node_name or node_name.startswith(base_instance):
                return node_name
        
        # If it looks like an IP, try to resolve via kube_node_info metric
        if base_instance.replace('.', '').replace(':', '').isdigit():
            resolved = await self._resolve_ip_to_node(base_instance, prometheus_client)
            if resolved:
                return resolved
        
        # Return the base instance if we can't resolve it
        return base_instance if base_instance else 'unknown'
    
    async def _resolve_ip_to_node(self, ip_address: str, prometheus_client=None) -> Optional[str]:
        """Try to resolve IP address to node name using kube_node_info
        
        Args:
            ip_address: IP address string
            prometheus_client: Prometheus client
            
        Returns:
            Node name or None
        """
        if not prometheus_client:
            return None
        
        try:
            # Query kube_node_info which has both node name and internal IP
            query = f'kube_node_info{{internal_ip="{ip_address}"}}'
            result = await prometheus_client.query_instant(query)
            
            if 'result' in result and len(result['result']) > 0:
                metric = result['result'][0].get('metric', {})
                node_name = metric.get('node') or metric.get('kubernetes_node')
                if node_name:
                    return node_name
        except Exception:
            pass
        
        return None
    
    # ==================== Node Label and Role Functions ====================
    
    async def get_node_labels_from_kubernetes(self) -> Dict[str, Dict[str, str]]:
        """Get node labels directly from Kubernetes API"""
        if not self.auth_client or not self.auth_client.kube_client:
            return {}
        
        try:
            v1 = client.CoreV1Api(self.auth_client.kube_client)
            nodes = v1.list_node()
            
            labels_map = {}
            for node in nodes.items:
                node_name = node.metadata.name
                labels = node.metadata.labels or {}
                
                # Store labels for this node and potential name variations
                candidates = [node_name]
                if '.' in node_name:
                    candidates.append(node_name.split('.')[0])
                
                for key in candidates:
                    if key not in labels_map:
                        labels_map[key] = labels
            
            return labels_map
            
        except Exception:
            return {}

    async def get_node_labels_via_oc(self) -> Dict[str, Dict[str, str]]:
        """Get node labels using oc CLI as fallback"""
        try:
            completed = subprocess.run(
                ["oc", "get", "nodes", "-o", "json"],
                check=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            data = json.loads(completed.stdout)
            items = data.get("items", []) if isinstance(data, dict) else []
            labels_map = {}
            
            for node in items:
                metadata = node.get("metadata", {})
                node_name = metadata.get("name", "")
                labels = metadata.get("labels", {}) or {}
                
                candidates = [node_name]
                if '.' in node_name:
                    candidates.append(node_name.split('.')[0])
                    
                for key in candidates:
                    if key and key not in labels_map:
                        labels_map[key] = labels
            
            return labels_map
            
        except Exception:
            return {}
    
    async def get_node_labels_from_prometheus(self, prometheus_client) -> Dict[str, Dict[str, str]]:
        """Get node labels from Prometheus kube_node_labels metric"""
        try:
            result = await prometheus_client.query_instant('kube_node_labels')
            labels_map = {}
            
            if 'result' in result:
                for item in result['result']:
                    if 'metric' not in item:
                        continue
                    metric_labels = item['metric']
                    node_name = (metric_labels.get('node') or 
                                metric_labels.get('kubernetes_node') or 
                                metric_labels.get('nodename'))
                    
                    if not node_name:
                        continue
                    
                    # Keep all labels except __name__ and prometheus-specific labels
                    labels = {k: v for k, v in metric_labels.items() 
                             if k not in ['__name__', 'job', 'instance']}
                    
                    # Store labels for node name variations
                    candidates = [node_name]
                    if '.' in node_name:
                        candidates.append(node_name.split('.')[0])
                    
                    for key in candidates:
                        if key not in labels_map:
                            labels_map[key] = labels
            
            return labels_map
        except Exception:
            return {}
    
    async def get_all_node_labels(self, prometheus_client=None, force_refresh: bool = False) -> Dict[str, Dict[str, str]]:
        """Get node labels with fallback strategies and caching"""
        import time
        
        # Check cache
        if not force_refresh and self.node_cache and self.cache_timestamp:
            if (time.time() - self.cache_timestamp) < self.cache_ttl:
                return self.node_cache
        
        labels_map = {}
        
        # Try Kubernetes API first
        if self.auth_client:
            k8s_labels = await self.get_node_labels_from_kubernetes()
            if k8s_labels:
                labels_map = k8s_labels
        
        # Try oc CLI as fallback
        if not labels_map:
            oc_labels = await self.get_node_labels_via_oc()
            if oc_labels:
                labels_map = oc_labels
        
        # Try Prometheus kube_node_labels as third fallback
        if not labels_map and prometheus_client:
            prom_labels = await self.get_node_labels_from_prometheus(prometheus_client)
            if prom_labels:
                labels_map = prom_labels
        
        # Update cache
        if labels_map:
            self.node_cache = labels_map
            self.cache_timestamp = time.time()
        
        return labels_map
    
    def determine_node_role(self, node_name: str, labels: Dict[str, str], instance: str = "") -> str:
        """Determine node role from labels with improved detection"""
        
        # Check standard Kubernetes node role labels with exact matching
        if 'node-role.kubernetes.io/control-plane' in labels:
            return 'controlplane'
        if 'node-role.kubernetes.io/master' in labels:
            return 'controlplane'
        if 'node-role.kubernetes.io/infra' in labels:
            return 'infra'
        if 'node-role.kubernetes.io/worker' in labels:
            return 'worker'
        
        # Check for older/alternative role labels
        for label_key, label_value in labels.items():
            label_key_lower = label_key.lower()
            
            # Check for role in label keys
            if 'kubernetes.io/role' in label_key_lower:
                if label_value.lower() in ['master', 'control-plane']:
                    return 'controlplane'
                elif label_value.lower() == 'infra':
                    return 'infra'
                elif label_value.lower() == 'worker':
                    return 'worker'
            
            # Check for role indicators in any label
            if any(term in label_key_lower for term in ['master', 'control-plane', 'controlplane']):
                return 'controlplane'
            elif 'infra' in label_key_lower:
                return 'infra'
            elif 'worker' in label_key_lower:
                return 'worker'
            elif 'workload' in label_key_lower:
                return 'workload'
            
            # Check label values for role indicators
            label_value_lower = label_value.lower() if isinstance(label_value, str) else ""
            if any(term in label_value_lower for term in ['master', 'control']):
                return 'controlplane'
            elif 'infra' in label_value_lower:
                return 'infra'
            elif 'worker' in label_value_lower:
                return 'worker'
            elif 'workload' in label_value_lower:
                return 'workload'
        
        # Name-based detection as fallback
        name_lower = node_name.lower()
        instance_lower = instance.lower().split(':')[0] if instance else ""
        
        # Check for master/control-plane patterns
        if any(pattern in name_lower for pattern in ['master', 'control', 'cp-', '-cp']):
            return 'controlplane'
        if instance_lower and any(pattern in instance_lower for pattern in ['master', 'control', 'cp-', '-cp']):
            return 'controlplane'
        
        # Check for infra patterns
        if 'infra' in name_lower or 'infrastructure' in name_lower:
            return 'infra'
        if instance_lower and ('infra' in instance_lower or 'infrastructure' in instance_lower):
            return 'infra'
        
        # Check for workload patterns
        if 'workload' in name_lower:
            return 'workload'
        if instance_lower and 'workload' in instance_lower:
            return 'workload'
        
        # Default to worker for compute nodes
        return 'worker'
    
    async def get_node_groups(self, prometheus_client=None, force_refresh: bool = False) -> Dict[str, List[Dict[str, Any]]]:
        """Get nodes grouped by role (controlplane/worker/infra/workload)"""
        node_labels = await self.get_all_node_labels(prometheus_client, force_refresh)
        
        groups = {
            'controlplane': [],
            'worker': [],
            'infra': [],
            'workload': []
        }
        
        for node_name, labels in node_labels.items():
            role = self.determine_node_role(node_name, labels)
            
            node_info = {
                'name': node_name,
                'role': role,
                'labels': {k: v for k, v in labels.items() if k.startswith('node-role.kubernetes.io/')}
            }
            
            groups[role].append(node_info)
        
        return groups
    
    async def get_node_info(self, node_name: str, prometheus_client=None) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific node"""
        node_labels = await self.get_all_node_labels(prometheus_client)
        
        # Try exact match first
        if node_name in node_labels:
            labels = node_labels[node_name]
            role = self.determine_node_role(node_name, labels)
            
            return {
                'name': node_name,
                'role': role,
                'labels': labels,
                'role_labels': {k: v for k, v in labels.items() if k.startswith('node-role.kubernetes.io/')}
            }
        
        # Try short name lookup
        for full_name, labels in node_labels.items():
            if full_name.startswith(node_name + '.') or full_name.startswith(node_name):
                role = self.determine_node_role(full_name, labels)
                return {
                    'name': full_name,
                    'role': role,
                    'labels': labels,
                    'role_labels': {k: v for k, v in labels.items() if k.startswith('node-role.kubernetes.io/')}
                }
        
        return None
    
    async def get_nodes_by_role(self, role: str, prometheus_client=None) -> List[Dict[str, Any]]:
        """Get all nodes with specified role"""
        groups = await self.get_node_groups(prometheus_client)
        return groups.get(role, [])
    
    async def get_cluster_summary(self, prometheus_client=None) -> Dict[str, Any]:
        """Get cluster node summary"""
        groups = await self.get_node_groups(prometheus_client)
        
        summary = {
            'total_nodes': sum(len(nodes) for nodes in groups.values()),
            'groups': {}
        }
        
        for role, nodes in groups.items():
            summary['groups'][role] = {
                'count': len(nodes),
                'nodes': [node['name'] for node in nodes]
            }
        
        return summary
    
    def normalize_node_name(self, node_name: str) -> Set[str]:
        """Get all possible variations of a node name"""
        variations = {node_name}
        
        # Add short name if FQDN
        if '.' in node_name:
            variations.add(node_name.split('.')[0])
        
        # Remove port if present
        if ':' in node_name:
            base_name = node_name.split(':')[0]
            variations.add(base_name)
            if '.' in base_name:
                variations.add(base_name.split('.')[0])
        
        return variations
    
    async def match_node_to_role(self, node_identifier: str, prometheus_client=None) -> Optional[str]:
        """Match a node identifier (name/instance) to its role"""
        node_labels = await self.get_all_node_labels(prometheus_client)
        
        # Try all variations of the node name
        variations = self.normalize_node_name(node_identifier)
        
        for variation in variations:
            if variation in node_labels:
                labels = node_labels[variation]
                return self.determine_node_role(variation, labels, node_identifier)
        
        # Fallback: check if any known node name contains this identifier
        for full_name, labels in node_labels.items():
            if any(var in full_name for var in variations):
                return self.determine_node_role(full_name, labels, node_identifier)
        
        return 'unknown'
    
    async def get_node_role_mapping(self, prometheus_client=None) -> Dict[str, str]:
        """Get a simple mapping of node_name -> role"""
        node_labels = await self.get_all_node_labels(prometheus_client)
        mapping = {}
        
        for node_name, labels in node_labels.items():
            role = self.determine_node_role(node_name, labels)
            mapping[node_name] = role
            
            # Add short name variant
            if '.' in node_name:
                short_name = node_name.split('.')[0]
                if short_name not in mapping:
                    mapping[short_name] = role
        
        return mapping
    
    def clear_cache(self) -> None:
        """Clear the node labels cache"""
        self.node_cache = {}
        self.cache_timestamp = None
    
    # ==================== Pod Information Functions ====================
    
    def get_pod_node_names_via_oc(self, namespace: str = "openshift-multus", 
                                  label_selector: Optional[str] = None) -> Dict[str, Any]:
        """Get the nodeName for pods in a namespace using oc jsonpath"""
        try:
            cmd = ["oc", "-n", namespace, "get", "pods"]
            if label_selector:
                cmd.extend(["-l", label_selector])
            jsonpath = "{range .items[*]}{.spec.nodeName}{'\\n'}{end}"
            cmd.extend(["-o", f"jsonpath={jsonpath}"])
            completed = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=30)
            lines = [line.strip() for line in completed.stdout.splitlines()]
            node_names = [ln for ln in lines if ln]
            
            return {
                'namespace': namespace,
                'label_selector': label_selector,
                'node_names': node_names,
                'count': len(node_names)
            }
        except Exception as e:
            return {
                'namespace': namespace,
                'label_selector': label_selector,
                'error': str(e),
                'node_names': [],
                'count': 0
            }
    
    def get_pod_to_node_mapping_via_oc(self, namespace: str = "openshift-multus", 
                                       label_selector: Optional[str] = None) -> Dict[str, str]:
        """Get a mapping of pod name -> nodeName via oc jsonpath"""
        try:
            cmd = ["oc", "-n", namespace, "get", "pods"]
            if label_selector:
                cmd.extend(["-l", label_selector])
            jsonpath = "{range .items[*]}{.metadata.name}={.spec.nodeName}{'\\n'}{end}"
            cmd.extend(["-o", f"jsonpath={jsonpath}"])
            completed = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=30)
            mapping: Dict[str, str] = {}
            for line in completed.stdout.splitlines():
                if not line.strip():
                    continue
                if "=" in line:
                    pod, node = line.split("=", 1)
                    pod = pod.strip()
                    node = node.strip()
                    if pod and node:
                        mapping[pod] = node
            return mapping
        except Exception:
            return {}
    
    def get_pod_full_info_via_oc(self, namespace: str = "openshift-multus", 
                                 label_selector: Optional[str] = None) -> Dict[str, Dict[str, str]]:
        """Get full pod info including namespace via oc jsonpath"""
        try:
            cmd = ["oc", "-n", namespace, "get", "pods"]
            if label_selector:
                cmd.extend(["-l", label_selector])
            jsonpath = "{range .items[*]}{.metadata.name}={.spec.nodeName}={.metadata.namespace}{'\\n'}{end}"
            cmd.extend(["-o", f"jsonpath={jsonpath}"])
            completed = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=30)
            
            pod_info: Dict[str, Dict[str, str]] = {}
            for line in completed.stdout.splitlines():
                if not line.strip():
                    continue
                parts = line.split("=")
                if len(parts) >= 3:
                    pod = parts[0].strip()
                    node = parts[1].strip()
                    ns = parts[2].strip()
                    if pod and node:
                        pod_info[pod] = {
                            'node_name': node,
                            'namespace': ns if ns else namespace
                        }
            return pod_info
        except Exception:
            return {}
    
    def get_all_pods_info_across_namespaces(self, namespaces: List[str] = None) -> Dict[str, Dict[str, str]]:
        """Get pod info across multiple namespaces"""
        if namespaces is None:
            namespaces = ['default', 'openshift-ovn-kubernetes', 'kube-system', 
                         'openshift-monitoring', 'openshift-multus']
        
        all_pod_info: Dict[str, Dict[str, str]] = {}
        
        for namespace in namespaces:
            try:
                pod_info = self.get_pod_full_info_via_oc(namespace=namespace)
                all_pod_info.update(pod_info)
            except Exception:
                continue
        
        return all_pod_info
    
    def get_pod_to_node_mapping_with_metadata_via_oc(self, namespace: str = "openshift-multus", 
                                                     label_selector: Optional[str] = None) -> Dict[str, Any]:
        """Get pod-node mapping with metadata in JSON format"""
        try:
            cmd = ["oc", "-n", namespace, "get", "pods"]
            if label_selector:
                cmd.extend(["-l", label_selector])
            jsonpath = "{range .items[*]}{.metadata.name}={.spec.nodeName}{'\\n'}{end}"
            cmd.extend(["-o", f"jsonpath={jsonpath}"])
            completed = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=30)
            
            mapping: Dict[str, str] = {}
            pod_details: List[Dict[str, str]] = []
            
            for line in completed.stdout.splitlines():
                if not line.strip():
                    continue
                if "=" in line:
                    pod, node = line.split("=", 1)
                    pod = pod.strip()
                    node = node.strip()
                    if pod and node:
                        mapping[pod] = node
                        pod_details.append({
                            'pod_name': pod,
                            'node_name': node
                        })
            
            return {
                'namespace': namespace,
                'label_selector': label_selector,
                'pod_node_mapping': mapping,
                'pod_details': pod_details,
                'count': len(mapping)
            }
            
        except Exception as e:
            return {
                'namespace': namespace,
                'label_selector': label_selector,
                'error': str(e),
                'pod_node_mapping': {},
                'pod_details': [],
                'count': 0
            }
    
    
async def get_node_groups(auth_client=None, prometheus_client=None) -> Dict[str, List[Dict[str, Any]]]:
    """Quick access to node groups"""
    utility = mcpToolsUtility(auth_client)
    return await utility.get_node_groups(prometheus_client)


async def get_node_role(node_name: str, auth_client=None, prometheus_client=None) -> str:
    """Quick access to get a node's role"""
    utility = mcpToolsUtility(auth_client)
    node_info = await utility.get_node_info(node_name, prometheus_client)
    return node_info['role'] if node_info else 'unknown'


async def get_cluster_summary(auth_client=None, prometheus_client=None) -> Dict[str, Any]:
    """Quick access to cluster summary"""
    utility = mcpToolsUtility(auth_client)
    return await utility.get_cluster_summary(prometheus_client)


