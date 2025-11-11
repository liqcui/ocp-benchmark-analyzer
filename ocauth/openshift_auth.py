"""
OpenShift Authentication Module
Handles authentication and service discovery for OpenShift clusters
"""

import os
import logging
import json
from typing import Optional, Dict, Any
import shlex
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import asyncio
import ssl

# Suppress urllib3 SSL warnings for self-signed certificates
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# Suppress urllib3 connection pool retry warnings (these are expected during transient network issues)
urllib3.disable_warnings(urllib3.exceptions.MaxRetryError)
# Suppress connection reset warnings from urllib3 connection pool
# These warnings occur when urllib3 automatically retries after connection resets, which is expected behavior
urllib3_logger = logging.getLogger('urllib3.connectionpool')
urllib3_logger.setLevel(logging.ERROR)  # Only show errors, not warnings about retries
import requests


class OpenShiftAuth:
    """OpenShift authentication and service discovery"""
    
    def __init__(self, kubeconfig_path: Optional[str] = None):
        self.k8s_client = None
        self.prometheus_url = None
        self.token = None
        self.ca_cert_path = None
        self.logger = logging.getLogger(__name__)
        # Optional kubeconfig path for explicit configuration
        self.kubeconfig_path: Optional[str] = kubeconfig_path
        # Store full discovery details for building fallbacks
        self.prometheus_info: Optional[Dict[str, Any]] = None
        # K8s API TLS verification flag (env overridable)
        self.k8s_verify_ssl = None
        # K8s API CA certificate path if available
        self.k8s_ca_cert_path: Optional[str] = None
        # Minimal cluster information cache for quick health checks
        self.cluster_info: Dict[str, Any] = {}
        
    async def initialize(self) -> bool:
        """Initialize Kubernetes client and discover services"""
        try:
            # Clear any potentially broken CA bundle envs before loading kubeconfig
            # Misconfigured CA bundles are a common root cause of PEM lib errors
            original_ca_bundle = os.environ.pop('REQUESTS_CA_BUNDLE', None)
            original_curl_bundle = os.environ.pop('CURL_CA_BUNDLE', None)
            if original_ca_bundle:
                self.logger.info(f"Temporarily cleared REQUESTS_CA_BUNDLE: {original_ca_bundle}")
            if original_curl_bundle:
                self.logger.info(f"Temporarily cleared CURL_CA_BUNDLE: {original_curl_bundle}")
            
            # Load kubeconfig (use explicit path when provided)
            if self.kubeconfig_path:
                self.logger.info(f"Loading kubeconfig from path: {self.kubeconfig_path}")
                config.load_kube_config(config_file=self.kubeconfig_path)
            else:
                config.load_kube_config()

            # Respect env override for API server TLS verification
            def _env_bool(name: str, default: Optional[bool] = None) -> Optional[bool]:
                val = os.getenv(name)
                if val is None:
                    return default
                val_l = val.strip().lower()
                if val_l in ("true", "1", "yes"): return True
                if val_l in ("false", "0", "no"): return False
                return default

            self.k8s_verify_ssl = (
                _env_bool('K8S_VERIFY', None)
                if _env_bool('K8S_VERIFY', None) is not None
                else _env_bool('OCP_API_VERIFY', None)
            )

            k8s_conf = client.Configuration.get_default_copy()
            if self.k8s_verify_ssl is not None:
                k8s_conf.verify_ssl = self.k8s_verify_ssl
                if not self.k8s_verify_ssl:
                    k8s_conf.assert_hostname = False
                    k8s_conf.ssl_ca_cert = None
                self.logger.info(f"Kubernetes API TLS verify set via env: {self.k8s_verify_ssl}")

            # Capture CA path from configuration if present and valid
            if getattr(k8s_conf, 'ssl_ca_cert', None):
                ca_path = k8s_conf.ssl_ca_cert
                # Validate the CA cert file exists and is readable
                if os.path.isfile(ca_path):
                    try:
                        with open(ca_path, 'r') as f:
                            content = f.read()
                            # Basic validation - check if it looks like a PEM cert
                            if 'BEGIN CERTIFICATE' in content:
                                self.k8s_ca_cert_path = ca_path
                                self.logger.info(f"Using valid CA cert from config: {ca_path}")
                            else:
                                self.logger.warning(f"CA cert file exists but doesn't appear to be valid PEM: {ca_path}")
                    except Exception as e:
                        self.logger.warning(f"CA cert file exists but couldn't be read: {ca_path} - {e}")
                else:
                    self.logger.warning(f"CA cert path from config doesn't exist: {ca_path}")
                    
            # Allow override via env
            env_ca = os.getenv('K8S_CA_CERT')
            if env_ca and os.path.isfile(env_ca):
                self.k8s_ca_cert_path = env_ca
                self.logger.info(f"Using CA cert from K8S_CA_CERT env: {env_ca}")

            # Propagate CA path to Prometheus config default if available
            prom_ca_env = os.getenv('PROMETHEUS_CA_CERT')
            if prom_ca_env and os.path.isfile(prom_ca_env):
                self.ca_cert_path = prom_ca_env
            elif self.k8s_ca_cert_path and not self.ca_cert_path:
                self.ca_cert_path = self.k8s_ca_cert_path

            self.k8s_client = client.ApiClient(configuration=k8s_conf)
            
            # If CA file appears invalid and verification not explicitly forced, disable TLS early
            try:
                ca_from_conf = getattr(k8s_conf, 'ssl_ca_cert', None)
                if ca_from_conf and self.k8s_verify_ssl is not True:
                    if not self._is_ca_file_valid(ca_from_conf):
                        self.logger.warning(
                            f"Detected invalid CA cert at {ca_from_conf}; disabling TLS verification to avoid PEM errors"
                        )
                        k8s_conf_fallback = client.Configuration.get_default_copy()
                        k8s_conf_fallback.verify_ssl = False
                        k8s_conf_fallback.assert_hostname = False
                        k8s_conf_fallback.ssl_ca_cert = None
                        self.k8s_client = client.ApiClient(configuration=k8s_conf_fallback)
                        self.k8s_verify_ssl = False
                
                # Proactively probe API to catch SSL/CA issues and auto-fallback
                self._ensure_k8s_api_connectivity()
            except Exception:
                # _ensure_k8s_api_connectivity will log and handle fallbacks
                raise
            
            # Get authentication details
            await self._get_auth_details()
            
            # Discover Prometheus service
            prometheus_info = await self._discover_prometheus()
            if prometheus_info:
                self.prometheus_info = prometheus_info
                self.prometheus_url = prometheus_info['url']
                self.logger.info(f"Discovered Prometheus at: {self.prometheus_url}")
                return True
            else:
                self.logger.error("Failed to discover Prometheus service")
                return False
            
            # Best-effort populate minimal cluster info for consumers
            try:
                await self._update_basic_cluster_info()
            except Exception as e:
                self.logger.debug(f"Failed to populate basic cluster info: {e}")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize OCP authentication: {e}")
            return False

    @property
    def kube_client(self):
        """Backward compatible alias for k8s_client."""
        return self.k8s_client

    @property
    def prometheus_token(self) -> Optional[str]:
        """Backward compatible alias commonly used by collectors."""
        return self.token

    def _is_ca_file_valid(self, ca_path: str) -> bool:
        """Validate a CA bundle by attempting to load it with ssl.

        Returns False if the file cannot be loaded as a trust store.
        """
        try:
            if not os.path.isfile(ca_path):
                return False
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.load_verify_locations(cafile=ca_path)
            return True
        except Exception as e:
            self.logger.debug(f"CA validation failed for {ca_path}: {e}")
            return False

    def _ensure_k8s_api_connectivity(self) -> None:
        """Probe the Kubernetes API and auto-recover from common SSL/PEM issues.

        If a PEM/SSL verification error is detected (e.g., invalid REQUESTS_CA_BUNDLE,
        broken CA file, or hostname mismatch), automatically disable TLS verification
        as a last-resort fallback to avoid repeated urllib3 retry warnings.
        """
        try:
            # Use a lightweight version check
            version_api = client.VersionApi(self.k8s_client)
            _ = version_api.get_code()
            self.logger.info("Kubernetes API connectivity verified successfully")
            return
        except Exception as probe_err:
            err_text = str(probe_err)
            # Detect common SSL issues seen in logs: PEM lib, certificate verify failed, SSLError
            ssl_error_indicators = (
                "PEM lib",
                "CERTIFICATE_VERIFY_FAILED",
                "SSLError",
                "certificate verify failed",
                "ssl.c:",
                "[X509]",
            )
            if not any(ind in err_text for ind in ssl_error_indicators):
                # Not an SSL-related problem; re-raise for caller
                self.logger.error(f"Kubernetes API connectivity check failed: {err_text}")
                raise

            self.logger.warning(
                "Kubernetes API SSL verification failed during probe: %s. "
                "Disabling TLS verification as a fallback (set K8S_VERIFY=true to force verification).",
                err_text,
            )

            # Rebuild configuration with SSL verification disabled
            k8s_conf_fallback = client.Configuration.get_default_copy()
            k8s_conf_fallback.verify_ssl = False
            k8s_conf_fallback.assert_hostname = False
            k8s_conf_fallback.ssl_ca_cert = None
            self.k8s_client = client.ApiClient(configuration=k8s_conf_fallback)
            self.k8s_verify_ssl = False

            # Verify connectivity with fallback; if it still fails, let the exception propagate
            try:
                version_api = client.VersionApi(self.k8s_client)
                _ = version_api.get_code()
                self.logger.info("Kubernetes API connectivity verified successfully (with TLS verification disabled)")
            except Exception as fallback_err:
                self.logger.error(f"Kubernetes API connectivity failed even with TLS disabled: {fallback_err}")
                raise
    
    async def _get_auth_details(self):
        """Extract authentication details from kubeconfig"""
        # Primary fallback: try to create service account token for Prometheus
        if not self.token:
            self.token = await self._create_prometheus_sa_token()
        
        # Secondary fallback: try 'oc whoami -t' if SA token creation failed
        if not self.token:
            try:
                self.logger.info("Attempting to get token via 'oc whoami -t'")
                proc = await asyncio.create_subprocess_exec(
                    'oc', 'whoami', '-t',
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode == 0:
                    token = stdout.decode().strip()
                    if token and len(token) > 10:
                        self.token = token
                        self.logger.info("Successfully obtained token via 'oc whoami -t'")
                    else:
                        self.logger.warning("Token from 'oc whoami -t' appears invalid")
                else:
                    err = stderr.decode().strip()
                    if err:
                        self.logger.debug(f"'oc whoami -t' failed: {err}")
            except Exception as e:
                self.logger.debug(f"Failed to run 'oc whoami -t' for token fallback: {e}")
        
        # Final fallback: try to get token from service account
        if not self.token:
            try:
                sa_token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
                if os.path.exists(sa_token_path):
                    with open(sa_token_path, 'r') as f:
                        self.token = f.read().strip()
                    self.logger.info("Using service account token")
            except Exception as e:
                self.logger.debug(f"Could not read service account token: {e}")
        
        if not self.token:
            self.logger.warning("No authentication token found - Prometheus access may fail")
        else:
            self.logger.info(f"Authentication token configured (length: {len(self.token)})")

    async def _update_basic_cluster_info(self) -> None:
        """Populate minimal cluster info used by health checks."""
        try:
            if not self.k8s_client:
                return
            v1 = client.CoreV1Api(self.k8s_client)
            nodes = v1.list_node(limit=1)
            # If the API returns fewer than requested, we still can count via full list
            if hasattr(nodes, 'items') and nodes.items is not None and len(nodes.items) <= 1:
                try:
                    nodes_full = v1.list_node()
                    node_count = len(getattr(nodes_full, 'items', []) or [])
                except Exception:
                    node_count = len(nodes.items or [])
            else:
                node_count = len(nodes.items or [])
            self.cluster_info['node_count'] = node_count
        except Exception as e:
            # Non-fatal
            self.logger.debug(f"Basic cluster info update failed: {e}")

    async def test_kubeapi_connection(self) -> bool:
        """Test basic connectivity to the Kubernetes API server."""
        try:
            if not self.k8s_client:
                # Attempt to reuse existing config
                k8s_conf = client.Configuration.get_default_copy()
                self.k8s_client = client.ApiClient(configuration=k8s_conf)
            v1 = client.CoreV1Api(self.k8s_client)
            _ = v1.list_namespace(limit=1)
            return True
        except Exception as e:
            self.logger.debug(f"Kubernetes API connection test failed: {e}")
            return False
    
    async def _create_prometheus_sa_token(self) -> Optional[str]:
        """Create service account token for Prometheus access"""
        sa_configs = [
            {'namespace': 'openshift-monitoring', 'name': 'prometheus-k8s'},
            {'namespace': 'openshift-user-workload-monitoring', 'name': 'prometheus-k8s'},
            {'namespace': 'openshift-monitoring', 'name': 'prometheus'},
            {'namespace': 'monitoring', 'name': 'prometheus'},
        ]
        
        for sa_config in sa_configs:
            namespace = sa_config['namespace']
            sa_name = sa_config['name']
            
            self.logger.info(f"Attempting to create token for SA {sa_name} in namespace {namespace}")
            
            # Try 'oc create token' (newer method, preferred)
            token = await self._try_oc_create_token(namespace, sa_name)
            if token:
                self.logger.info(f"Successfully created token using 'oc create token' for {namespace}/{sa_name}")
                return token
            
            # Try 'oc sa new-token' (older method, fallback)
            token = await self._try_oc_sa_new_token(namespace, sa_name)
            if token:
                self.logger.info(f"Successfully created token using 'oc sa new-token' for {namespace}/{sa_name}")
                return token
            
            self.logger.debug(f"Failed to create token for {namespace}/{sa_name}")
        
        self.logger.warning("Failed to create service account token for any Prometheus service account")
        return None
    
    async def _try_oc_create_token(self, namespace: str, sa_name: str) -> Optional[str]:
        """Try to create token using 'oc create token' command"""
        try:
            env = os.environ.copy()
            # Remove potentially broken CA bundle for subprocess
            env.pop('REQUESTS_CA_BUNDLE', None)
            
            cmd = ['oc', 'create', 'token', sa_name, '-n', namespace, '--duration', '24h']
            
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env
            )
            
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                token = stdout.decode().strip()
                if token and len(token) > 20:
                    return token
                else:
                    self.logger.debug(f"'oc create token' returned invalid token for {namespace}/{sa_name}")
            else:
                stderr_text = stderr.decode().strip()
                if stderr_text:
                    self.logger.debug(f"'oc create token' failed for {namespace}/{sa_name}: {stderr_text}")
                
        except Exception as e:
            self.logger.debug(f"Error running 'oc create token' for {namespace}/{sa_name}: {e}")
        
        return None
    
    async def _try_oc_sa_new_token(self, namespace: str, sa_name: str) -> Optional[str]:
        """Try to create token using 'oc sa new-token' command (legacy)"""
        try:
            env = os.environ.copy()
            # Remove potentially broken CA bundle for subprocess
            env.pop('REQUESTS_CA_BUNDLE', None)
            
            cmd = ['oc', 'sa', 'new-token', sa_name, '-n', namespace]
            
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env
            )
            
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                token = stdout.decode().strip()
                if token and len(token) > 20:
                    return token
                else:
                    self.logger.debug(f"'oc sa new-token' returned invalid token for {namespace}/{sa_name}")
            else:
                stderr_text = stderr.decode().strip()
                if stderr_text:
                    self.logger.debug(f"'oc sa new-token' failed for {namespace}/{sa_name}: {stderr_text}")
                
        except Exception as e:
            self.logger.debug(f"Error running 'oc sa new-token' for {namespace}/{sa_name}: {e}")
        
        return None
    
    async def _discover_prometheus(self) -> Optional[Dict[str, Any]]:
        """Discover Prometheus service in OpenShift monitoring namespace - routes first, then services"""
        try:
            monitoring_namespaces = [
                'openshift-monitoring',
                'openshift-user-workload-monitoring',
                'monitoring',
                'kube-system'
            ]
            
            # Try to find Prometheus through OpenShift routes (preferred)
            self.logger.info("Searching for Prometheus routes in monitoring namespaces...")
            for namespace in monitoring_namespaces:
                try:
                    route_info = await self._find_prometheus_route(namespace)
                    if route_info:
                        self.logger.info(f"✓ Found Prometheus route in namespace '{namespace}': {route_info['url']}")
                        return {
                            'url': route_info['url'],
                            'namespace': namespace,
                            'access_method': 'route',
                            'route_host': route_info['host'],
                            'tls_enabled': route_info['tls_enabled']
                        }
                except Exception as e:
                    self.logger.debug(f"No route found in namespace {namespace}: {e}")
                    continue
            
            # If no routes found, fall back to service discovery
            self.logger.warning("No Prometheus routes found in any namespace, trying service discovery...")
            return await self._discover_prometheus_service()
            
        except Exception as e:
            self.logger.error(f"Error discovering Prometheus: {e}")
            return None
    
    async def _find_prometheus_route(self, namespace: str) -> Optional[Dict[str, Any]]:
        """Find OpenShift route for Prometheus using oc command first, then API call"""
        try:
            # Method 1: Try 'oc get route' command (most reliable)
            route_info = await self._find_route_via_oc_command(namespace)
            if route_info:
                self.logger.info(f"Found Prometheus route via 'oc get route' in {namespace}")
                return route_info
            
            # Method 2: Fall back to direct API call
            self.logger.debug(f"Attempting route discovery via direct API call in {namespace}")
            return self._discover_route_via_requests(namespace)
                
        except Exception as e:
            self.logger.debug(f"Could not find route in namespace {namespace}: {e}")
        
        return None
    
    async def _find_route_via_oc_command(self, namespace: str) -> Optional[Dict[str, Any]]:
        """Find Prometheus route using 'oc get route' command"""
        try:
            env = os.environ.copy()
            # Remove potentially broken CA bundle for subprocess
            env.pop('REQUESTS_CA_BUNDLE', None)
            
            # Get all routes in the namespace
            cmd = ['oc', 'get', 'route', '-n', namespace, '-o', 'json']
            
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env
            )
            
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                import json
                routes_data = json.loads(stdout.decode())
                items = routes_data.get('items', [])
                
                # Look for Prometheus routes by name pattern
                prometheus_patterns = ['prometheus-k8s', 'prometheus']
                
                for route in items:
                    route_name = route.get('metadata', {}).get('name', '').lower()
                    
                    # Check if route name contains prometheus patterns
                    if any(pattern in route_name for pattern in prometheus_patterns):
                        spec = route.get('spec', {})
                        host = spec.get('host')
                        
                        if not host:
                            continue
                        
                        tls = spec.get('tls')
                        tls_enabled = bool(tls)
                        scheme = 'https' if tls_enabled else 'http'
                        
                        self.logger.info(f"Found route '{route.get('metadata', {}).get('name')}' with host: {host}")
                        
                        return {
                            'url': f"{scheme}://{host}",
                            'host': host,
                            'tls_enabled': tls_enabled,
                            'route_name': route.get('metadata', {}).get('name')
                        }
                
                # Also check by labels
                for route in items:
                    labels = route.get('metadata', {}).get('labels', {})
                    
                    if (labels.get('app.kubernetes.io/name') == 'prometheus' or
                        labels.get('app') == 'prometheus' or
                        labels.get('component') == 'prometheus'):
                        
                        spec = route.get('spec', {})
                        host = spec.get('host')
                        
                        if not host:
                            continue
                        
                        tls = spec.get('tls')
                        tls_enabled = bool(tls)
                        scheme = 'https' if tls_enabled else 'http'
                        
                        self.logger.info(f"Found route '{route.get('metadata', {}).get('name')}' by label with host: {host}")
                        
                        return {
                            'url': f"{scheme}://{host}",
                            'host': host,
                            'tls_enabled': tls_enabled,
                            'route_name': route.get('metadata', {}).get('name')
                        }
                
                self.logger.debug(f"No Prometheus routes found via 'oc get route' in {namespace}")
            else:
                stderr_text = stderr.decode().strip()
                if stderr_text:
                    self.logger.debug(f"'oc get route' failed for {namespace}: {stderr_text}")
                
        except json.JSONDecodeError as e:
            self.logger.debug(f"Failed to parse JSON from 'oc get route' in {namespace}: {e}")
        except Exception as e:
            self.logger.debug(f"Error running 'oc get route' for {namespace}: {e}")
        
        return None

    def _discover_route_via_requests(self, namespace: str) -> Optional[Dict[str, Any]]:
        """Directly query the OpenShift Routes API using requests with CA verification"""
        if not self.k8s_client:
            raise RuntimeError("Kubernetes client not initialized")

        base_url = self.k8s_client.configuration.host.rstrip('/')
        headers = {'Accept': 'application/json'}
        if self.token:
            headers['Authorization'] = f"Bearer {self.token}"

        verify: Any
        # Prefer explicit CA path if valid, otherwise obey verify flag, otherwise disable
        if self.k8s_ca_cert_path and os.path.isfile(self.k8s_ca_cert_path):
            verify = self.k8s_ca_cert_path
            self.logger.debug(f"Using CA cert for route discovery: {self.k8s_ca_cert_path}")
        elif self.k8s_verify_ssl is not None:
            verify = self.k8s_verify_ssl
            self.logger.debug(f"Using verify_ssl setting for route discovery: {self.k8s_verify_ssl}")
        else:
            # Default to False if we had SSL issues earlier
            verify = False
            self.logger.debug("Using verify=False for route discovery (SSL issues detected earlier)")

        label_selectors = [
            "app.kubernetes.io/name=prometheus",
            "app=prometheus",
            "component=prometheus"
        ]

        timeout = (5, 20)
        session = requests.Session()
        
        for label_selector in label_selectors:
            try:
                url = f"{base_url}/apis/route.openshift.io/v1/namespaces/{namespace}/routes"
                params = {"labelSelector": label_selector}
                resp = session.get(url, headers=headers, params=params, verify=verify, timeout=timeout)
                
                if resp.status_code == 200:
                    data = resp.json()
                    items = data.get('items') or []
                    if items:
                        route = items[0]
                        spec = route.get('spec', {})
                        host = spec.get('host')
                        tls_enabled = bool(spec.get('tls'))
                        scheme = 'https' if tls_enabled else 'http'
                        return {
                            'url': f"{scheme}://{host}",
                            'host': host,
                            'tls_enabled': tls_enabled,
                            'route_name': (route.get('metadata') or {}).get('name')
                        }
                elif resp.status_code in (401, 403):
                    self.logger.warning(f"Unauthorized to list routes in {namespace}: HTTP {resp.status_code}")
                    return None
            except Exception as e:
                self.logger.debug(f"Error checking routes with selector '{label_selector}' in {namespace}: {e}")
                continue

        # As last resort, list all routes and match by name
        try:
            url = f"{base_url}/apis/route.openshift.io/v1/namespaces/{namespace}/routes"
            resp = session.get(url, headers=headers, verify=verify, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                for route in data.get('items') or []:
                    name = (route.get('metadata') or {}).get('name', '').lower()
                    if any(k in name for k in ['prometheus', 'monitoring']):
                        spec = route.get('spec', {})
                        host = spec.get('host')
                        tls_enabled = bool(spec.get('tls'))
                        scheme = 'https' if tls_enabled else 'http'
                        return {
                            'url': f"{scheme}://{host}",
                            'host': host,
                            'tls_enabled': tls_enabled,
                            'route_name': (route.get('metadata') or {}).get('name')
                        }
        except Exception as e:
            self.logger.debug(f"Error listing all routes in {namespace}: {e}")
            
        return None
    
    async def _discover_prometheus_service(self) -> Optional[Dict[str, Any]]:
        """Discover Prometheus through service discovery"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            
            monitoring_namespaces = [
                'openshift-monitoring',
                'openshift-user-workload-monitoring',
                'monitoring',
                'kube-system'
            ]
            
            for namespace in monitoring_namespaces:
                try:
                    # Look for Prometheus service
                    services = v1.list_namespaced_service(
                        namespace=namespace,
                        label_selector="app.kubernetes.io/name=prometheus"
                    )
                    
                    if not services.items:
                        services = v1.list_namespaced_service(
                            namespace=namespace,
                            label_selector="app=prometheus"
                        )
                    
                    if services.items:
                        service = services.items[0]
                        service_name = service.metadata.name
                        
                        # Get service details
                        port = None
                        for svc_port in service.spec.ports:
                            if svc_port.name in ['web', 'http', 'prometheus'] or svc_port.port in [9090, 9091]:
                                port = svc_port.port
                                break
                        
                        if not port and service.spec.ports:
                            port = service.spec.ports[0].port
                        
                        # Build Prometheus URL based on service type
                        prometheus_url = None
                        access_method = None
                        
                        if service.spec.type == 'LoadBalancer' and service.status.load_balancer.ingress:
                            host = service.status.load_balancer.ingress[0].ip or service.status.load_balancer.ingress[0].hostname
                            prometheus_url = f"http://{host}:{port}"
                            access_method = 'loadbalancer'
                        elif service.spec.type == 'NodePort':
                            nodes = v1.list_node()
                            if nodes.items:
                                node_ip = None
                                for address in nodes.items[0].status.addresses:
                                    if address.type in ['ExternalIP', 'InternalIP']:
                                        node_ip = address.address
                                        break
                                if node_ip:
                                    prometheus_url = f"http://{node_ip}:{service.spec.ports[0].node_port}"
                                    access_method = 'nodeport'
                        
                        if not prometheus_url:
                            prometheus_url = f"http://{service_name}.{namespace}.svc.cluster.local:{port}"
                            access_method = 'cluster_internal'
                        
                        self.logger.info(f"Found Prometheus service in namespace: {namespace}")
                        return {
                            'url': prometheus_url,
                            'namespace': namespace,
                            'service_name': service_name,
                            'port': port,
                            'access_method': access_method
                        }
                        
                except ApiException as e:
                    if e.status != 404:
                        self.logger.warning(f"Error checking namespace {namespace}: {e}")
                    continue
            
            # If no service found, try to find Prometheus pods directly
            self.logger.info("No Prometheus services found, trying pod discovery...")
            return await self._discover_prometheus_pods()
            
        except Exception as e:
            self.logger.error(f"Error discovering Prometheus services: {e}")
            return None
    
    async def _discover_prometheus_pods(self) -> Optional[Dict[str, Any]]:
        """Discover Prometheus through pods if service discovery fails"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            
            monitoring_namespaces = [
                'openshift-monitoring',
                'openshift-user-workload-monitoring', 
                'monitoring'
            ]
            
            for namespace in monitoring_namespaces:
                try:
                    pods = v1.list_namespaced_pod(
                        namespace=namespace,
                        label_selector="app.kubernetes.io/name=prometheus"
                    )
                    
                    if not pods.items:
                        pods = v1.list_namespaced_pod(
                            namespace=namespace,
                            label_selector="app=prometheus"
                        )
                    
                    if pods.items:
                        pod = pods.items[0]
                        pod_ip = pod.status.pod_ip
                        
                        port = 9090
                        for container in pod.spec.containers:
                            if container.ports:
                                for container_port in container.ports:
                                    if container_port.name in ['web', 'http', 'prometheus']:
                                        port = container_port.container_port
                                        break
                        
                        prometheus_url = f"http://{pod_ip}:{port}"
                        
                        self.logger.info(f"Found Prometheus pod in namespace: {namespace}")
                        return {
                            'url': prometheus_url,
                            'namespace': namespace,
                            'pod_name': pod.metadata.name,
                            'port': port,
                            'access_method': 'direct_pod'
                        }
                        
                except ApiException as e:
                    if e.status != 404:
                        self.logger.warning(f"Error checking pods in namespace {namespace}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error discovering Prometheus pods: {e}")
        
        return None
    
    async def get_etcd_endpoints(self) -> Dict[str, Any]:
        """Get etcd endpoints from the cluster"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            
            etcd_namespace = "openshift-etcd"
            pods = v1.list_namespaced_pod(
                namespace=etcd_namespace,
                label_selector="app=etcd"
            )
            
            endpoints = []
            for pod in pods.items:
                if pod.status.phase == "Running":
                    pod_ip = pod.status.pod_ip
                    endpoints.append({
                        'name': pod.metadata.name,
                        'ip': pod_ip,
                        'endpoint': f"{pod_ip}:2379"
                    })
            
            return {
                'namespace': etcd_namespace,
                'endpoints': endpoints,
                'total_members': len(endpoints)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting etcd endpoints: {e}")
            return {'error': str(e)}
    
    async def execute_etcd_command(self, command: str) -> Dict[str, Any]:
        """Execute etcdctl command in etcd pod"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            
            etcd_namespace = "openshift-etcd"
            pods = v1.list_namespaced_pod(
                namespace=etcd_namespace,
                label_selector="app=etcd"
            )
            
            if not pods.items:
                return {'error': 'No etcd pods found'}
            
            pod_name = None
            for pod in pods.items:
                if pod.status.phase == "Running":
                    pod_name = pod.metadata.name
                    break
            
            if not pod_name:
                return {'error': 'No running etcd pods found'}
            
            # Determine container name to exec into
            container_name = None
            try:
                for c in pods.items[0].spec.containers:
                    if c.name == "etcdctl":
                        container_name = c.name
                        break
                if container_name is None:
                    for c in pods.items[0].spec.containers:
                        if c.name == "etcd":
                            container_name = c.name
                            break
                if container_name is None and pods.items[0].spec.containers:
                    container_name = pods.items[0].spec.containers[0].name
            except Exception:
                pass

            from kubernetes.stream import stream
            
            base_cmd = [
                'etcdctl',
                '--cacert=/etc/etcd/tls/etcd-ca/ca.crt',
                '--cert=/etc/etcd/tls/etcd-peer/peer.crt', 
                '--key=/etc/etcd/tls/etcd-peer/peer.key',
                '--endpoints=https://localhost:2379'
            ] + command.split()

            # Build shell-wrapped command to unset conflicting env vars and force ETCDCTL_API=3
            unsafe_envs = [
                'ETCDCTL_KEY','ETCDCTL_CERT','ETCDCTL_CACERT','ETCDCTL_ENDPOINTS','ETCDCTL_USER',
                'ETCDCTL_PASSWORD','ETCDCTL_TOKEN','ETCDCTL_INSECURE_SKIP_TLS_VERIFY'
            ]
            unset_parts = ' '.join([f"-u {name}" for name in unsafe_envs])
            cmd_str = ' '.join(shlex.quote(p) for p in base_cmd)
            shell_cmd = f"env {unset_parts} ETCDCTL_API=3 {cmd_str}"

            exec_command = ['/bin/sh', '-c', shell_cmd]

            resp = stream(
                v1.connect_get_namespaced_pod_exec,
                pod_name,
                etcd_namespace,
                command=exec_command,
                container=container_name,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False
            )
            
            return {
                'pod_name': pod_name,
                'command': ' '.join(exec_command),
                'output': resp
            }
            
        except Exception as e:
            self.logger.error(f"Error executing etcd command: {e}")
            return {'error': str(e)}
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for API requests"""
        headers = {}
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
            self.logger.debug("Authorization header configured")
        else:
            self.logger.warning("No token available for Authorization header")
        return headers
    
    def get_prometheus_config(self) -> Dict[str, Any]:
        """Get Prometheus connection configuration"""
        # Environment overrides
        env_url = os.getenv('PROMETHEUS_URL') or os.getenv('OCP_PROMETHEUS_URL')
        env_token = os.getenv('PROMETHEUS_TOKEN')
        env_verify = os.getenv('PROMETHEUS_VERIFY')
        if env_url:
            self.logger.info(f"Using PROMETHEUS_URL override: {env_url}")
        base_url = env_url or self.prometheus_url
        # Prefer explicit PROMETHEUS_TOKEN when provided; otherwise use discovered token
        token_value: Optional[str] = env_token if env_token else self.token
        verify_value: Any = self.ca_cert_path if self.ca_cert_path else False
        if env_verify is not None:
            env_verify_lower = env_verify.lower()
            if env_verify_lower in ('true', '1', 'yes'):
                verify_value = True
            elif env_verify_lower in ('false', '0', 'no'):
                verify_value = False
            else:
                # Treat as CA file path - validate it exists
                if os.path.isfile(env_verify):
                    verify_value = env_verify
                else:
                    self.logger.warning(f"PROMETHEUS_VERIFY points to non-existent file: {env_verify}")
                    verify_value = False

        # Fallback URLs from env or discovery
        fallback_urls: list[str] = []
        env_fallbacks = os.getenv('PROMETHEUS_FALLBACK_URLS')
        if env_fallbacks:
            fallback_urls.extend([u.strip() for u in env_fallbacks.split(',') if u.strip()])

        fallback_urls.extend(self._build_fallback_urls())

        config = {
            'url': base_url,
            'headers': self.get_auth_headers(),
            'token': token_value,
            'verify': verify_value,
            'fallback_urls': fallback_urls,
        }
        
        # Log configuration for debugging (without sensitive data)
        config_debug = config.copy()
        if 'Authorization' in config_debug.get('headers', {}):
            config_debug['headers'] = config_debug['headers'].copy()
            config_debug['headers']['Authorization'] = 'Bearer [REDACTED]'
        
        self.logger.debug(f"Prometheus config: {config_debug}")
        return config

    def _build_fallback_urls(self) -> list:
        """Construct likely fallback Prometheus URLs when the route is unreachable"""
        fallbacks: list[str] = []
        try:
            info = self.prometheus_info or {}
            namespace = info.get('namespace')
            access_method = info.get('access_method')
            service_name = info.get('service_name') or 'prometheus-k8s'
            port = info.get('port') or 9090

            # If initial access is via route, prefer cluster-internal service DNS
            if namespace:
                fallbacks.append(f"http://{service_name}.{namespace}.svc.cluster.local:{port}")
                fallbacks.append(f"http://{service_name}.{namespace}.svc:{port}")

            # Generic common namespaces if discovery context missing
            if not namespace:
                for ns in ['openshift-monitoring', 'openshift-user-workload-monitoring', 'monitoring']:
                    fallbacks.append(f"http://prometheus-k8s.{ns}.svc.cluster.local:9090")

            # If discovery returned direct pod or nodeport, include that URL too
            if access_method in ('nodeport', 'loadbalancer', 'direct_pod') and info.get('url'):
                fallbacks.append(info['url'])

        except Exception as e:
            self.logger.debug(f"Failed building fallback URLs: {e}")

        # Deduplicate while preserving order
        seen = set()
        unique = []
        for u in fallbacks:
            if u and u not in seen:
                seen.add(u)
                unique.append(u)
        return unique