#!/usr/bin/env python3
"""
OpenShift Cluster Information Collector
Renamed module from ovnk_benchmark_openshift_cluster_info to cluster_info.
"""

import json
import logging
import asyncio
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

from kubernetes import client, config  # noqa: F401  # config may be used by OpenShiftAuth
from kubernetes.client.rest import ApiException  # noqa: F401

from ocauth.openshift_auth import OpenShiftAuth

logger = logging.getLogger(__name__)


@dataclass
class NodeInfo:
    """Node information structure"""
    name: str
    node_type: str  # master, infra, worker
    cpu_capacity: str
    memory_capacity: str
    architecture: str
    kernel_version: str
    container_runtime: str
    kubelet_version: str
    os_image: str
    ready_status: str  # Ready, NotReady, SchedulingDisabled, etc.
    schedulable: bool
    creation_timestamp: str


@dataclass
class ClusterInfo:
    """Complete cluster information structure"""
    cluster_name: str
    cluster_version: str
    platform: str
    api_server_url: str
    total_nodes: int
    master_nodes: List[NodeInfo]
    infra_nodes: List[NodeInfo]
    worker_nodes: List[NodeInfo]
    namespaces_count: int
    pods_count: int
    services_count: int
    secrets_count: int
    configmaps_count: int
    networkpolicies_count: int
    adminnetworkpolicies_count: int
    baselineadminnetworkpolicies_count: int
    egressfirewalls_count: int
    egressips_count: int
    clusteruserdefinednetworks_count: int
    userdefinednetworks_count: int
    unavailable_cluster_operators: List[str]
    mcp_status: Dict[str, str]
    collection_timestamp: str


class ClusterInfoCollector:
    """Collects comprehensive OpenShift cluster information and status"""

    def __init__(self, kubeconfig_path: Optional[str] = None):
        self.kubeconfig_path = kubeconfig_path
        self.auth_manager = None
        self.k8s_client = None

    async def initialize(self):
        """Initialize the collector with authentication"""
        self.auth_manager = OpenShiftAuth(self.kubeconfig_path)
        await self.auth_manager.initialize()
        self.k8s_client = self.auth_manager.kube_client

    async def collect_cluster_info(self) -> ClusterInfo:
        """Collect all cluster information"""
        try:
            if not self.k8s_client:
                await self.initialize()

            logger.info("Starting cluster information collection...")

            # Basic cluster info
            cluster_name = await self._get_cluster_name()
            cluster_version = await self._get_cluster_version()
            platform = await self._get_infrastructure_platform()
            api_server_url = getattr(getattr(self.k8s_client, "configuration", None), "host", "") or ""

            # Node information
            nodes_info = await self._collect_nodes_info()

            # Resource counts
            resource_counts = await self._collect_resource_counts()

            # Cluster operators status
            unavailable_operators = await self._get_unavailable_cluster_operators()

            # Machine config pool status
            mcp_status = await self._get_mcp_status()

            cluster_info = ClusterInfo(
                cluster_name=cluster_name,
                cluster_version=cluster_version,
                platform=platform,
                api_server_url=api_server_url,
                total_nodes=len(nodes_info["all_nodes"]),
                master_nodes=nodes_info["master_nodes"],
                infra_nodes=nodes_info["infra_nodes"],
                worker_nodes=nodes_info["worker_nodes"],
                namespaces_count=resource_counts["namespaces"],
                pods_count=resource_counts["pods"],
                services_count=resource_counts["services"],
                secrets_count=resource_counts["secrets"],
                configmaps_count=resource_counts["configmaps"],
                networkpolicies_count=resource_counts["networkpolicies"],
                adminnetworkpolicies_count=resource_counts["adminnetworkpolicies"],
                baselineadminnetworkpolicies_count=resource_counts["baselineadminnetworkpolicies"],
                egressfirewalls_count=resource_counts["egressfirewalls"],
                egressips_count=resource_counts["egressips"],
                clusteruserdefinednetworks_count=resource_counts["clusteruserdefinednetworks"],
                userdefinednetworks_count=resource_counts["userdefinednetworks"],
                unavailable_cluster_operators=unavailable_operators,
                mcp_status=mcp_status,
                collection_timestamp=datetime.now(timezone.utc).isoformat(),
            )

            logger.info("Cluster information collection completed successfully")
            return cluster_info

        except Exception as e:
            logger.error(f"Failed to collect cluster info: {e}")
            raise

    async def _get_cluster_name(self) -> str:
        """Get cluster name from infrastructure config"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            infrastructure = custom_api.get_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="infrastructures",
                name="cluster",
            )
            return infrastructure.get("status", {}).get("infrastructureName", "unknown")
        except Exception as e:
            logger.warning(f"Could not get cluster name: {e}")
            return "unknown"

    async def _get_cluster_version(self) -> str:
        """Get OpenShift cluster version"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            cluster_version = custom_api.get_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="clusterversions",
                name="version",
            )

            history = cluster_version.get("status", {}).get("history", [])
            if history:
                return history[0].get("version", "unknown")

        except Exception as e:
            logger.warning(f"Could not get cluster version: {e}")

        return "unknown"

    async def _get_infrastructure_platform(self) -> str:
        """Get infrastructure platform (AWS, Azure, GCP, etc.)"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            infrastructure = custom_api.get_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="infrastructures",
                name="cluster",
            )
            return infrastructure.get("status", {}).get("platform", "unknown")
        except Exception as e:
            logger.warning(f"Could not get infrastructure platform: {e}")
            return "unknown"

    async def _collect_nodes_info(self) -> Dict[str, List[NodeInfo]]:
        """Collect detailed information about all nodes"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            nodes = v1.list_node()

            master_nodes = []
            infra_nodes = []
            worker_nodes = []
            all_nodes = []

            for node in nodes.items:
                node_info = self._parse_node_info(node)
                all_nodes.append(node_info)

                # Categorize nodes based on labels
                labels = node.metadata.labels or {}
                if labels.get("node-role.kubernetes.io/master") == "" or \
                   labels.get("node-role.kubernetes.io/control-plane") == "":
                    node_info.node_type = "master"
                    master_nodes.append(node_info)
                elif labels.get("node-role.kubernetes.io/infra") == "":
                    node_info.node_type = "infra"
                    infra_nodes.append(node_info)
                else:
                    node_info.node_type = "worker"
                    worker_nodes.append(node_info)

            return {
                "all_nodes": all_nodes,
                "master_nodes": master_nodes,
                "infra_nodes": infra_nodes,
                "worker_nodes": worker_nodes,
            }

        except Exception as e:
            logger.error(f"Failed to collect nodes info: {e}")
            raise

    def _parse_node_info(self, node) -> NodeInfo:
        """Parse Kubernetes node object into NodeInfo"""
        status = node.status
        spec = node.spec
        metadata = node.metadata

        # Determine node status
        ready_status = False
        for condition in status.conditions or []:
            if condition.type == "Ready" and condition.status == "True":
                ready_status = True
                break

        schedulable = not spec.unschedulable if spec.unschedulable is not None else True

        # Determine overall status string
        if ready_status and schedulable:
            status_str = 'Ready'
        elif ready_status and not schedulable:
            status_str = 'Ready,SchedulingDisabled'
        elif not ready_status and schedulable:
            status_str = 'NotReady'
        elif not ready_status and not schedulable:
            status_str = 'NotReady,SchedulingDisabled'
        else:
            status_str = 'Unknown'

        return NodeInfo(
            name=metadata.name,
            node_type="unknown",  # Will be set by categorization logic
            cpu_capacity=status.capacity.get("cpu", "unknown"),
            memory_capacity=status.capacity.get("memory", "unknown"),
            architecture=status.node_info.architecture,
            kernel_version=status.node_info.kernel_version,
            container_runtime=status.node_info.container_runtime_version,
            kubelet_version=status.node_info.kubelet_version,
            os_image=status.node_info.os_image,
            ready_status=status_str,
            schedulable=schedulable,
            creation_timestamp=metadata.creation_timestamp.isoformat() if metadata.creation_timestamp else "",
        )

    async def _collect_resource_counts(self) -> Dict[str, int]:
        """Collect counts of various cluster resources"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            networking_v1 = client.NetworkingV1Api(self.k8s_client)
            custom_api = client.CustomObjectsApi(self.k8s_client)

            # Basic resources
            namespaces = v1.list_namespace()
            pods = v1.list_pod_for_all_namespaces()
            services = v1.list_service_for_all_namespaces()
            secrets = v1.list_secret_for_all_namespaces()
            configmaps = v1.list_config_map_for_all_namespaces()

            # Network policies
            networkpolicies = networking_v1.list_network_policy_for_all_namespaces()

            # Custom resources
            counts = {
                "namespaces": len(namespaces.items),
                "pods": len(pods.items),
                "services": len(services.items),
                "secrets": len(secrets.items),
                "configmaps": len(configmaps.items),
                "networkpolicies": len(networkpolicies.items),
                "adminnetworkpolicies": 0,
                "baselineadminnetworkpolicies": 0,
                "egressfirewalls": 0,
                "egressips": 0,
                "clusteruserdefinednetworks": 0,
                "userdefinednetworks": 0,
            }

            # Admin network policies
            try:
                admin_policies = custom_api.list_cluster_custom_object(
                    group="policy.networking.k8s.io",
                    version="v1alpha1",
                    plural="adminnetworkpolicies",
                )
                counts["adminnetworkpolicies"] = len(admin_policies.get("items", []))
            except Exception as e:
                logger.warning(f"Could not count admin network policies: {e}")

            # Baseline admin network policies
            try:
                baseline_admin_policies = custom_api.list_cluster_custom_object(
                    group="policy.networking.k8s.io",
                    version="v1alpha1",
                    plural="baselineadminnetworkpolicies",
                )
                counts["baselineadminnetworkpolicies"] = len(baseline_admin_policies.get("items", []))
            except Exception as e:
                logger.warning(f"Could not count baseline admin network policies: {e}")

            # Egress firewalls
            try:
                egress_firewalls = custom_api.list_cluster_custom_object(
                    group="k8s.ovn.org",
                    version="v1",
                    plural="egressfirewalls",
                )
                counts["egressfirewalls"] = len(egress_firewalls.get("items", []))
            except Exception as e:
                logger.warning(f"Could not count egress firewalls: {e}")

            # Egress IPs
            try:
                egress_ips = custom_api.list_cluster_custom_object(
                    group="k8s.ovn.org",
                    version="v1",
                    plural="egressips",
                )
                counts["egressips"] = len(egress_ips.get("items", []))
            except Exception as e:
                logger.warning(f"Could not count egress IPs: {e}")

            # Cluster User Defined Networks (ClusterUDN)
            try:
                cluster_udn_resources = custom_api.list_cluster_custom_object(
                    group="k8s.ovn.org",
                    version="v1",
                    plural="clusteruserdefinednetworks",
                )
                counts["clusteruserdefinednetworks"] = len(cluster_udn_resources.get("items", []))
            except Exception as e:
                logger.warning(f"Could not count cluster user defined networks: {e}")

            # User Defined Networks (UDN)
            try:
                udn_resources = custom_api.list_cluster_custom_object(
                    group="k8s.ovn.org",
                    version="v1",
                    plural="userdefinednetworks",
                )
                counts["userdefinednetworks"] = len(udn_resources.get("items", []))
            except Exception as e:
                logger.warning(f"Could not count user defined networks: {e}")

            return counts

        except Exception as e:
            logger.error(f"Failed to collect resource counts: {e}")
            return {
                "namespaces": 0,
                "pods": 0,
                "services": 0,
                "secrets": 0,
                "configmaps": 0,
                "networkpolicies": 0,
                "adminnetworkpolicies": 0,
                "baselineadminnetworkpolicies": 0,
                "egressfirewalls": 0,
                "egressips": 0,
                "clusteruserdefinednetworks": 0,
                "userdefinednetworks": 0,
            }

    async def _get_unavailable_cluster_operators(self) -> List[str]:
        """Get list of unavailable cluster operators"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            operators = custom_api.list_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="clusteroperators",
            )

            unavailable_operators = []
            for operator in operators.get("items", []):
                name = operator["metadata"]["name"]
                conditions = operator.get("status", {}).get("conditions", [])

                # Check if operator is available
                is_available = False
                for condition in conditions:
                    if condition["type"] == "Available" and condition["status"] == "True":
                        is_available = True
                        break

                if not is_available:
                    unavailable_operators.append(name)

            return unavailable_operators

        except Exception as e:
            logger.warning(f"Could not get cluster operators status: {e}")
            return []

    async def _get_mcp_status(self) -> Dict[str, str]:
        """Get machine config pool status"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            mcp_response = custom_api.list_cluster_custom_object(
                group="machineconfiguration.openshift.io",
                version="v1",
                plural="machineconfigpools",
            )

            mcp_status = {}
            for mcp in mcp_response.get('items', []):
                pool_name = mcp.get('metadata', {}).get('name', 'unknown')

                # Determine pool status
                if 'status' in mcp:
                    conditions = mcp['status'].get('conditions', [])

                    updated = False
                    updating = False
                    degraded = False

                    for condition in conditions:
                        if condition.get('type') == 'Updated' and condition.get('status') == 'True':
                            updated = True
                        elif condition.get('type') == 'Updating' and condition.get('status') == 'True':
                            updating = True
                        elif condition.get('type') == 'Degraded' and condition.get('status') == 'True':
                            degraded = True

                    if degraded:
                        status = 'Degraded'
                    elif updating:
                        status = 'Updating'
                    elif updated:
                        status = 'Updated'
                    else:
                        status = 'Unknown'

                    mcp_status[pool_name] = status

            return mcp_status

        except Exception as e:
            logger.warning(f"Could not get machine config pool status: {e}")
            return {}

    def to_json(self, cluster_info: ClusterInfo) -> str:
        """Convert cluster info to JSON string"""
        return json.dumps(asdict(cluster_info), indent=2, default=str)

    def to_dict(self, cluster_info: ClusterInfo) -> Dict[str, Any]:
        """Convert cluster info to dictionary"""
        return asdict(cluster_info)


# Backward compatibility alias for older imports
# Some modules may still import OpenShiftGeneralInfo; map it to ClusterInfoCollector
OpenShiftGeneralInfo = ClusterInfoCollector


# Convenience functions
async def collect_cluster_information(kubeconfig_path: Optional[str] = None) -> Dict[str, Any]:
    """Collect and return cluster information as dictionary"""
    collector = ClusterInfoCollector(kubeconfig_path)
    cluster_info = await collector.collect_cluster_info()
    return collector.to_dict(cluster_info)


async def get_cluster_info_json(kubeconfig_path: Optional[str] = None) -> str:
    """Collect and return cluster information as JSON string"""
    collector = ClusterInfoCollector(kubeconfig_path)
    cluster_info = await collector.collect_cluster_info()
    return collector.to_json(cluster_info)


if __name__ == "__main__":
    async def main():
        try:
            collector = ClusterInfoCollector()
            cluster_info = await collector.collect_cluster_info()
            print(collector.to_json(cluster_info))
        except Exception as e:
            print(f"Error: {e}")

    asyncio.run(main())



