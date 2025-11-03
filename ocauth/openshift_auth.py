"""
OpenShift Authentication Module
Handles authentication and service discovery for OpenShift/Kubernetes clusters
"""

import os
import base64
import json
import subprocess
from typing import Optional, Dict, Any, List
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import aiohttp
import asyncio
import logging


class OpenShiftAuth:
    """Authentication and service discovery for OpenShift clusters"""
    
    def __init__(self, kubeconfig_path: Optional[str] = None):
        self.kubeconfig_path = kubeconfig_path or os.getenv('KUBECONFIG')
        self.kube_client: Optional[client.ApiClient] = None
        self.prometheus_url: Optional[str] = None
        self.prometheus_token: Optional[str] = None
        self.cluster_info: Dict[str, Any] = {}
        self.prometheus_alt_urls: List[str] = []
        
        # Environment configuration for Prometheus access
        self.prefer_route = os.getenv('OVNK_PROMETHEUS_USE_ROUTE', 'true').lower() in ['1', 'true', 'yes']
        self.prometheus_namespace = os.getenv('OVNK_PROMETHEUS_NAMESPACE', 'openshift-monitoring')
        self.prometheus_sa = os.getenv('OVNK_PROMETHEUS_SA', 'prometheus-k8s')
        
    async def initialize(self) -> None:
        """Initialize authentication and discover services"""
        try:
            # Load kubeconfig
            await self._load_kube_config()
            
            # Create API client
            self.kube_client = client.ApiClient()
            
            # Get cluster information
            await self._get_cluster_info()
            
            # Discover Prometheus
            await self._discover_prometheus()
            
            # Get service account token (multiple strategies)
            await self._get_prometheus_token()
            
            print(f"✅ Successfully authenticated to OpenShift cluster")
            print(f"✅ Prometheus URL: {self.prometheus_url}")
            print(f"✅ Token authentication: {'enabled' if self.prometheus_token else 'disabled'}")
            
        except Exception as e:
            print(f"❌ Failed to initialize authentication: {e}")
            raise
    
    async def _load_kube_config(self) -> None:
        """Load kubeconfig with priority order"""
        try:
            if self.kubeconfig_path and os.path.exists(self.kubeconfig_path):
                print(f"📁 Loading kubeconfig from: {self.kubeconfig_path}")
                config.load_kube_config(config_file=self.kubeconfig_path)
            elif os.path.exists('/var/run/secrets/kubernetes.io/serviceaccount/token'):
                # Running in-cluster
                print("🏗️ Loading in-cluster configuration")
                config.load_incluster_config()
            else:
                # Try default kubeconfig location
                default_kubeconfig = os.path.expanduser('~/.kube/config')
                if os.path.exists(default_kubeconfig):
                    print(f"📁 Loading default kubeconfig from: {default_kubeconfig}")
                    config.load_kube_config()
                else:
                    raise Exception("No valid kubeconfig found")
        except Exception as e:
            print(f"❌ Failed to load kubeconfig: {e}")
            raise
    
    async def _get_cluster_info(self) -> None:
        """Get basic cluster information"""
        try:
            v1 = client.CoreV1Api(self.kube_client)
            
            # Get cluster version (OpenShift specific)
            try:
                custom_api = client.CustomObjectsApi(self.kube_client)
                cluster_version = custom_api.get_cluster_custom_object(
                    group="config.openshift.io",
                    version="v1",
                    plural="clusterversions",
                    name="version"
                )
                self.cluster_info['openshift_version'] = cluster_version.get('status', {}).get('desired', {}).get('version', 'unknown')
                self.cluster_info['is_openshift'] = True
                print(f"🚀 OpenShift version: {self.cluster_info['openshift_version']}")
            except ApiException:
                self.cluster_info['is_openshift'] = False
                print("🔧 Kubernetes cluster (not OpenShift)")
            
            # Get nodes info
            nodes = v1.list_node()
            self.cluster_info['node_count'] = len(nodes.items)
            self.cluster_info['nodes'] = []
            for node in nodes.items:
                labels = node.metadata.labels or {}
                roles_set = set()
                for label_key, label_value in labels.items():
                    if label_key.startswith('node-role.kubernetes.io/'):
                        role_suffix = label_key.split('/', 1)[1]
                        roles_set.add(role_suffix or (label_value or 'unknown'))
                    elif label_key == 'kubernetes.io/role' and label_value:
                        roles_set.add(label_value)

                self.cluster_info['nodes'].append({
                    'name': node.metadata.name,
                    'roles': sorted(list(roles_set)),
                    'version': node.status.node_info.kubelet_version
                })
            
            # Get namespaces
            namespaces = v1.list_namespace()
            self.cluster_info['namespace_count'] = len(namespaces.items)
            print(f"🏷️ Found {self.cluster_info['node_count']} nodes and {self.cluster_info['namespace_count']} namespaces")
            
        except Exception as e:
            print(f"⚠️ Warning: Could not get cluster info: {e}")
    
    async def _discover_prometheus(self) -> None:
        """Discover Prometheus service and construct URLs"""
        try:
            v1 = client.CoreV1Api(self.kube_client)
            
            # Try to find Prometheus in common namespaces
            prometheus_namespaces = [
                self.prometheus_namespace,
                'openshift-monitoring',
                'monitoring',
                'prometheus',
                'kube-system'
            ]
            
            prometheus_service = None
            prometheus_namespace = None
            self.prometheus_alt_urls = []
            
            for namespace in prometheus_namespaces:
                try:
                    services = v1.list_namespaced_service(namespace=namespace)
                    for service in services.items:
                        service_name = service.metadata.name.lower()
                        if ('prometheus' in service_name) or ('thanos-querier' in service_name):
                            prometheus_service = service
                            prometheus_namespace = namespace
                            
                            # Build internal service URLs
                            svc_name = service.metadata.name
                            ports = [p.port for p in (service.spec.ports or [])]
                            
                            # Prefer port 9090, then others
                            preferred_ports = [9090] + [p for p in ports if p != 9090]
                            for port in preferred_ports:
                                url = f"http://{svc_name}.{namespace}.svc.cluster.local:{port}"
                                if url not in self.prometheus_alt_urls:
                                    self.prometheus_alt_urls.append(url)
                            break
                    if prometheus_service:
                        break
                except ApiException:
                    continue
            
            if prometheus_service:
                prometheus_namespace = prometheus_service.metadata.namespace
                
                # Determine primary URL based on preference
                if self.prefer_route and self.cluster_info.get('is_openshift', False):
                    route_url = await self._get_openshift_route_url(
                        prometheus_service.metadata.name,
                        prometheus_namespace
                    )
                    self.prometheus_url = route_url or (self.prometheus_alt_urls[0] if self.prometheus_alt_urls else None)
                else:
                    self.prometheus_url = self.prometheus_alt_urls[0] if self.prometheus_alt_urls else None
                
                print(f"🎯 Found Prometheus service: {prometheus_service.metadata.name} in {prometheus_namespace}")
                print(f"📍 Primary URL: {self.prometheus_url}")
                print(f"🔗 Alternative URLs: {len(self.prometheus_alt_urls)} found")
            else:
                print("⚠️ No Prometheus service found, using default URL")
                self.prometheus_url = "http://prometheus-k8s.openshift-monitoring.svc.cluster.local:9090"
            
        except Exception as e:
            print(f"⚠️ Warning: Could not discover Prometheus: {e}")
            self.prometheus_url = "http://prometheus-k8s.openshift-monitoring.svc.cluster.local:9090"
    
    async def _get_openshift_route_url(self, service_name: str, namespace: str) -> Optional[str]:
        """Get OpenShift route URL for Prometheus"""
        try:
            custom_api = client.CustomObjectsApi(self.kube_client)
            
            # Look for existing route
            routes = custom_api.list_namespaced_custom_object(
                group="route.openshift.io",
                version="v1",
                namespace=namespace,
                plural="routes"
            )
            
            for route in routes.get('items', []):
                route_name = route['metadata']['name'].lower()
                if service_name.lower() in route_name or 'prometheus' in route_name:
                    host = route['spec'].get('host')
                    tls = route['spec'].get('tls')
                    protocol = 'https' if tls else 'http'
                    url = f"{protocol}://{host}"
                    print(f"🌐 Found OpenShift route: {url}")
                    return url
            
            return None
            
        except Exception as e:
            print(f"Could not get OpenShift route: {e}")
            return None
    
    async def _get_prometheus_token_via_oc(self) -> Optional[str]:
        """Get token using oc command (Strategy 1)"""
        try:
            # Set KUBECONFIG if available
            env = os.environ.copy()
            if self.kubeconfig_path:
                env['KUBECONFIG'] = self.kubeconfig_path
            
            # Try 'oc create token' first (newer OpenShift)
            try:
                result = subprocess.run(
                    ['oc', 'create', 'token', self.prometheus_sa, '-n', self.prometheus_namespace],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    env=env
                )
                
                if result.returncode == 0:
                    token = result.stdout.strip()
                    if token:
                        print(f"✅ Retrieved token via 'oc create token'")
                        return token
            except Exception as e:
                print(f"'oc create token' failed: {e}")
            
            # Fallback to 'oc sa new-token' (older OpenShift)
            try:
                result = subprocess.run(
                    ['oc', 'sa', 'new-token', self.prometheus_sa, '-n', self.prometheus_namespace],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    env=env
                )
                
                if result.returncode == 0:
                    token = result.stdout.strip()
                    if token:
                        print(f"✅ Retrieved token via 'oc sa new-token'")
                        return token
            except Exception as e:
                print(f"'oc sa new-token' failed: {e}")
                
        except Exception as e:
            print(f"oc command execution failed: {e}")
        
        return None
    
    async def _get_prometheus_token_via_kube_client(self) -> Optional[str]:
        """Get token using Kubernetes client (Strategy 2)"""
        try:
            # Try TokenRequest API first (K8s 1.20+)
            token = await self._create_token_request()
            if token:
                return token
            
            # Fallback to service account secrets
            token = await self._get_token_from_secrets()
            if token:
                return token
            
            return None
            
        except Exception as e:
            print(f"Kubernetes client token retrieval failed: {e}")
            return None
    
    async def _create_token_request(self) -> Optional[str]:
        """Create token using TokenRequest API"""
        try:
            auth_v1 = client.AuthenticationV1Api(self.kube_client)
            
            # Create TokenRequest object
            token_request = self._create_token_request_object(3600)
            
            if token_request is None:
                return None
            
            if isinstance(token_request, dict):
                # Handle dynamic object creation
                response = self.kube_client.call_api(
                    resource_path=f'/api/v1/namespaces/{self.prometheus_namespace}/serviceaccounts/{self.prometheus_sa}/token',
                    method='POST',
                    body=token_request,
                    header_params={'Content-Type': 'application/json'},
                    response_type='object'
                )
                if response and len(response) > 0:
                    token_data = response[0]
                    if hasattr(token_data, 'status') and hasattr(token_data.status, 'token'):
                        print("✅ Retrieved token via TokenRequest API (dynamic)")
                        return token_data.status.token
                    elif isinstance(token_data, dict) and 'status' in token_data:
                        token = token_data['status'].get('token')
                        if token:
                            print("✅ Retrieved token via TokenRequest API (dict)")
                            return token
            else:
                # Use standard API
                token_response = auth_v1.create_service_account_token(
                    name=self.prometheus_sa,
                    namespace=self.prometheus_namespace,
                    body=token_request
                )
                print("✅ Retrieved token via TokenRequest API (standard)")
                return token_response.status.token
                
        except Exception as e:
            print(f"TokenRequest API failed: {e}")
        
        return None
    
    def _create_token_request_object(self, expiration_seconds: int = 3600):
        """Create a TokenRequest object using dynamic approach"""
        # Try multiple ways to create the TokenRequest object
        
        # Method 1: Direct class access
        try:
            return client.V1TokenRequest(
                spec=client.V1TokenRequestSpec(expiration_seconds=expiration_seconds)
            )
        except (AttributeError, NameError):
            pass
        
        # Method 2: Via client.models
        try:
            return client.models.V1TokenRequest(
                spec=client.models.V1TokenRequestSpec(expiration_seconds=expiration_seconds)
            )
        except (AttributeError, NameError):
            pass
        
        # Method 3: Via authentication_v1 models
        try:
            from kubernetes.client.models import V1TokenRequest, V1TokenRequestSpec
            return V1TokenRequest(
                spec=V1TokenRequestSpec(expiration_seconds=expiration_seconds)
            )
        except ImportError:
            pass
        
        # Method 4: Dynamic object creation (fallback)
        try:
            token_request = {
                'apiVersion': 'authentication.k8s.io/v1',
                'kind': 'TokenRequest',
                'spec': {
                    'expirationSeconds': expiration_seconds
                }
            }
            return token_request
        except Exception:
            pass
        
        return None
    
    async def _get_token_from_secrets(self) -> Optional[str]:
        """Get token from service account secrets"""
        try:
            v1 = client.CoreV1Api(self.kube_client)
            
            # Get service account
            try:
                service_account = v1.read_namespaced_service_account(
                    name=self.prometheus_sa,
                    namespace=self.prometheus_namespace
                )
            except ApiException as e:
                if e.status == 404:
                    print(f"Service account {self.prometheus_sa} not found in {self.prometheus_namespace}")
                    return None
                raise
            
            # Look for token secrets
            if hasattr(service_account, 'secrets') and service_account.secrets:
                for secret_ref in service_account.secrets:
                    try:
                        secret = v1.read_namespaced_secret(
                            name=secret_ref.name,
                            namespace=self.prometheus_namespace
                        )
                        
                        if secret.type == 'kubernetes.io/service-account-token':
                            token_data = secret.data.get('token')
                            if token_data:
                                token = base64.b64decode(token_data).decode('utf-8')
                                print("✅ Retrieved token from service account secret")
                                return token
                    except Exception as e:
                        print(f"Could not read secret {secret_ref.name}: {e}")
                        continue
            
            # Try to find token secrets by label
            secrets = v1.list_namespaced_secret(
                namespace=self.prometheus_namespace,
                label_selector=f"kubernetes.io/service-account.name={self.prometheus_sa}"
            )
            
            for secret in secrets.items:
                if secret.type == 'kubernetes.io/service-account-token':
                    token_data = secret.data.get('token')
                    if token_data:
                        token = base64.b64decode(token_data).decode('utf-8')
                        print("✅ Retrieved token from labeled secret")
                        return token
            
            print(f"No token secrets found for service account {self.prometheus_sa}")
            return None
            
        except Exception as e:
            print(f"Could not get token from secrets: {e}")
            return None
    
    async def _get_prometheus_token(self) -> None:
        """Get service account token for Prometheus access using multiple strategies"""
        try:
            # Strategy 1: Try oc command line tools first
            print("🔐 Attempting to get token via oc command...")
            self.prometheus_token = await self._get_prometheus_token_via_oc()
            
            if self.prometheus_token:
                return
            
            # Strategy 2: Use Kubernetes client API
            print("🔐 Attempting to get token via Kubernetes client...")
            self.prometheus_token = await self._get_prometheus_token_via_kube_client()
            
            if self.prometheus_token:
                return
            
            # Strategy 3: Try to get current context token
            print("🔐 Attempting to get token from current context...")
            if hasattr(self.kube_client.configuration, 'api_key') and self.kube_client.configuration.api_key:
                token = list(self.kube_client.configuration.api_key.values())[0]
                if token.startswith('Bearer '):
                    token = token[7:]
                self.prometheus_token = token
                print("✅ Retrieved token from current context")
                return
            
            # Strategy 4: Try to read token from service account (in-cluster)
            print("🔐 Attempting to read in-cluster service account token...")
            token_path = '/var/run/secrets/kubernetes.io/serviceaccount/token'
            if os.path.exists(token_path):
                with open(token_path, 'r') as f:
                    self.prometheus_token = f.read().strip()
                print("✅ Retrieved in-cluster service account token")
                return
            
            print("⚠️ No token retrieval strategy succeeded")
            
        except Exception as e:
            print(f"⚠️ Warning: Could not get Prometheus token: {e}")
            self.prometheus_token = None
    
    async def test_prometheus_connection(self) -> bool:
        """Test connection to Prometheus with fallback URLs"""
        urls_to_test = [self.prometheus_url] if self.prometheus_url else []
        urls_to_test.extend(self.prometheus_alt_urls)
        
        if not urls_to_test:
            print("❌ No Prometheus URLs to test")
            return False
        
        headers = {}
        if self.prometheus_token:
            headers['Authorization'] = f'Bearer {self.prometheus_token}'
        
        # Disable SSL verification for self-signed certificates
        connector = aiohttp.TCPConnector(ssl=False)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            for url in urls_to_test:
                try:
                    print(f"🔍 Testing Prometheus connection: {url}")
                    
                    async with session.get(
                        f"{url}/api/v1/query",
                        params={'query': 'up'},
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        
                        print(f"📡 Response status: {response.status}")
                        
                        if response.status == 200:
                            try:
                                data = await response.json()
                                if data.get('status') == 'success':
                                    if url != self.prometheus_url:
                                        print(f"🔄 Switching to working URL: {url}")
                                        self.prometheus_url = url
                                    print("✅ Prometheus connection successful")
                                    return True
                            except Exception as e:
                                print(f"Failed to parse response: {e}")
                                continue
                        
                        elif response.status == 403:
                            print("🚫 Access forbidden - token may be invalid or insufficient permissions")
                            
                        elif response.status == 401:
                            print("🔐 Unauthorized - authentication required")
                            
                except Exception as e:
                    print(f"Connection failed for {url}: {e}")
                    continue
        
        print("❌ All Prometheus connection attempts failed")
        return False
    
    async def test_kubeapi_connection(self) -> bool:
        """Test connectivity to the Kubernetes API server"""
        try:
            if not self.kube_client:
                await self._load_kube_config()
                self.kube_client = client.ApiClient()

            v1 = client.CoreV1Api(self.kube_client)
            namespaces = v1.list_namespace(limit=1)
            print("✅ Kubernetes API connection successful")
            return True
        except Exception as e:
            print(f"❌ Kubernetes API connection test failed: {e}")
            return False
    
    async def validate_prometheus_access(self) -> Dict[str, Any]:
        """Validate Prometheus access and return detailed status"""
        validation_result = {
            'prometheus_url': self.prometheus_url,
            'has_token': bool(self.prometheus_token),
            'connection_successful': False,
            'alternative_urls': self.prometheus_alt_urls,
            'error_details': None
        }
        
        try:
            validation_result['connection_successful'] = await self.test_prometheus_connection()
        except Exception as e:
            validation_result['error_details'] = str(e)
        
        return validation_result
    
    def get_prometheus_url(self) -> Optional[str]:
        """Return the Prometheus URL"""
        return self.prometheus_url
    
    def get_prometheus_token(self) -> Optional[str]:
        """Return the Prometheus authentication token"""
        return self.prometheus_token
    
    def get_prometheus_config(self) -> Dict[str, Any]:
        """Return Prometheus URL and token configuration"""
        return {
            'url': self.prometheus_url,
            'token': self.prometheus_token,
            'authenticated': bool(self.prometheus_token),
            'alternative_urls': self.prometheus_alt_urls
        }
    
    def get_cluster_summary(self) -> Dict[str, Any]:
        """Get a summary of cluster information"""
        return {
            'cluster_info': self.cluster_info,
            'prometheus_config': {
                'url': self.prometheus_url,
                'authenticated': bool(self.prometheus_token),
                'alternative_urls': self.prometheus_alt_urls,
                'prefer_route': self.prefer_route,
                'namespace': self.prometheus_namespace,
                'service_account': self.prometheus_sa
            },
            'auth_status': 'authenticated' if self.kube_client else 'not_authenticated'
        }
    
    async def refresh_token(self) -> bool:
        """Refresh the Prometheus access token"""
        try:
            print("🔄 Refreshing Prometheus token...")
            old_token = self.prometheus_token
            self.prometheus_token = None
            
            await self._get_prometheus_token()
            
            if self.prometheus_token and self.prometheus_token != old_token:
                print("✅ Token refreshed successfully")
                return True
            else:
                print("⚠️ Token refresh did not produce a new token")
                return False
                
        except Exception as e:
            print(f"❌ Token refresh failed: {e}")
            return False


# Convenience function for easy initialization
async def initialize_auth(kubeconfig_path: Optional[str] = None) -> OpenShiftAuth:
    """Initialize and return configured OpenShift authentication"""
    auth = OpenShiftAuth(kubeconfig_path)
    await auth.initialize()
    return auth


# Global authenticator instance
auth = OpenShiftAuth()


# Example usage and testing
async def main():
    """Example usage of the OpenShift authentication module"""
    try:
        # Initialize authentication
        await auth.initialize()
        
        # Test connections
        kube_ok = await auth.test_kubeapi_connection()
        prom_ok = await auth.test_prometheus_connection()
        
        print("\n" + "="*50)
        print("CONNECTION STATUS")
        print("="*50)
        print(f"Kubernetes API: {'✅ Connected' if kube_ok else '❌ Failed'}")
        print(f"Prometheus:     {'✅ Connected' if prom_ok else '❌ Failed'}")
        
        # Display cluster summary
        summary = auth.get_cluster_summary()
        print(f"\nCluster Type: {'OpenShift' if summary['cluster_info'].get('is_openshift') else 'Kubernetes'}")
        if summary['cluster_info'].get('openshift_version'):
            print(f"Version: {summary['cluster_info']['openshift_version']}")
        print(f"Nodes: {summary['cluster_info'].get('node_count', 'unknown')}")
        print(f"Namespaces: {summary['cluster_info'].get('namespace_count', 'unknown')}")
        
        # Display configuration
        print(f"\nPrometheus Configuration:")
        print(f"  URL: {auth.get_prometheus_url()}")
        print(f"  Token: {'Available' if auth.get_prometheus_token() else 'Not available'}")
        
        # Get Prometheus config
        prom_config = auth.get_prometheus_config()
        print(f"  Authenticated: {prom_config['authenticated']}")
        if prom_config['alternative_urls']:
            print(f"  Alternatives: {len(prom_config['alternative_urls'])} URLs")
        
    except Exception as e:
        print(f"❌ Initialization failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())