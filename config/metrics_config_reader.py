"""
Configuration module for OpenShift Benchmark
Enhanced version with explicit metrics file loading - no defaults
"""

import os
import yaml
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field, ConfigDict
from pathlib import Path


class PrometheusConfig(BaseModel):
    """Prometheus configuration"""
    url: Optional[str] = None
    token: Optional[str] = None
    timeout: int = 30
    verify_ssl: bool = False
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


class ReportConfig(BaseModel):
    """Report generation configuration"""
    output_dir: str = "exports"
    formats: List[str] = ["excel", "pdf"]
    template_dir: str = "templates"
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


class Config(BaseModel):
    """Main configuration class - metrics must be explicitly loaded"""
    kubeconfig_path: Optional[str] = Field(default=None)
    metrics_files: List[str] = Field(default_factory=list)
    metrics_directory: str = Field(default="config")
    prometheus: PrometheusConfig = Field(default_factory=PrometheusConfig)
    reports: ReportConfig = Field(default_factory=ReportConfig)
    timezone: str = Field(default="UTC")
    log_level: str = Field(default="INFO")
    
    # Metrics organization - Dict[category, List[metric_dict]]
    metrics_config: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict)
    
    # Metadata tracking
    metrics_metadata: Dict[str, Any] = Field(default_factory=dict)
    
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
    
    def __init__(self, **data):
        super().__init__(**data)
        self._load_environment()
        # NO automatic metrics loading - must be explicit
        self._create_directories()
        # Initialize empty metadata
        self.metrics_metadata = {
            'loaded_files': [],
            'total_metrics': 0,
            'categories': [],
            'metrics_by_file': {}
        }
    
    def _load_environment(self) -> None:
        """Load configuration from environment variables"""
        # Kubeconfig
        if not self.kubeconfig_path:
            self.kubeconfig_path = os.getenv('KUBECONFIG', os.path.expanduser('~/.kube/config'))
        
        # Prometheus
        if prometheus_url := os.getenv('PROMETHEUS_URL'):
            self.prometheus.url = prometheus_url
        if prometheus_token := os.getenv('PROMETHEUS_TOKEN'):
            self.prometheus.token = prometheus_token
        
        # Reports
        if output_dir := os.getenv('REPORT_OUTPUT_DIR'):
            self.reports.output_dir = output_dir
        
        # Logging
        if log_level := os.getenv('LOG_LEVEL'):
            self.log_level = log_level
    
    def _create_directories(self) -> None:
        """Create necessary directories"""
        directories = [
            Path(self.reports.output_dir),
            'storage',
            'exports',
            'logs'
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def load_metrics_file(self, file_path: str, category_filter: Optional[List[str]] = None) -> Dict[str, Any]:
        """Load metrics from a specific file with optional category filtering
        
        Args:
            file_path: Full or relative path to metrics file (e.g., 'config/metrics-etcd.yml')
            category_filter: Optional list of categories to load
        
        Returns:
            Dictionary with loading status and metrics info
        """
        try:
            # Convert to Path object
            metrics_path = Path(file_path)
            
            # Make absolute if needed
            if not metrics_path.is_absolute():
                metrics_path = Path.cwd() / metrics_path
            
            if not metrics_path.exists():
                return {
                    'success': False,
                    'error': f'File not found: {metrics_path}',
                    'metrics_loaded': 0
                }
            
            # Load YAML
            with open(metrics_path, 'r') as f:
                metrics_data = yaml.safe_load(f)
            
            # Handle both formats: {'metrics': [...]} or direct list
            if isinstance(metrics_data, dict) and 'metrics' in metrics_data:
                metrics_list = metrics_data['metrics']
            elif isinstance(metrics_data, list):
                metrics_list = metrics_data
            else:
                return {
                    'success': False,
                    'error': f'Invalid metrics file format in {metrics_path}',
                    'metrics_loaded': 0
                }
            
            loaded_count = 0
            loaded_categories = set()
            
            # Process each metric
            for metric_data in metrics_list:
                if not isinstance(metric_data, dict):
                    continue
                
                category = metric_data.get('category', 'general')
                
                # Apply category filter if specified
                if category_filter and category not in category_filter:
                    continue
                
                # Create metric dict
                metric_dict = {
                    'name': metric_data.get('name', ''),
                    'title': metric_data.get('title', metric_data.get('name', '')),
                    'expr': metric_data.get('expr', ''),
                    'unit': metric_data.get('unit', ''),
                    'description': metric_data.get('description', ''),
                    'category': category,
                    'source_file': str(metrics_path)
                }
                
                # Ensure category exists
                if category not in self.metrics_config:
                    self.metrics_config[category] = []
                
                # Check for duplicates
                metric_name = metric_dict['name']
                if not any(m['name'] == metric_name for m in self.metrics_config[category]):
                    self.metrics_config[category].append(metric_dict)
                    loaded_count += 1
                    loaded_categories.add(category)
            
            # Update metadata
            if str(metrics_path) not in self.metrics_metadata['loaded_files']:
                self.metrics_metadata['loaded_files'].append(str(metrics_path))
            
            self.metrics_metadata['metrics_by_file'][str(metrics_path)] = loaded_count
            self.metrics_metadata['total_metrics'] = self.get_metrics_count()
            self.metrics_metadata['categories'] = self.get_all_categories()
            
            # Add to metrics_files list if not already there
            if str(metrics_path) not in self.metrics_files:
                self.metrics_files.append(str(metrics_path))
            
            return {
                'success': True,
                'file_path': str(metrics_path),
                'file_name': metrics_path.name,
                'metrics_loaded': loaded_count,
                'categories_loaded': sorted(list(loaded_categories)),
                'total_metrics': self.get_metrics_count()
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'file_path': file_path,
                'metrics_loaded': 0
            }
    
    def get_metrics_by_category(self, category: str) -> List[Dict[str, Any]]:
        """Get metrics by category"""
        return self.metrics_config.get(category, [])
    
    def get_all_metrics(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get all metrics organized by category"""
        return self.metrics_config
    
    def get_metric_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a specific metric by name"""
        for category_metrics in self.metrics_config.values():
            for metric in category_metrics:
                if metric.get('name') == name:
                    return metric
        return None
    
    def get_all_categories(self) -> List[str]:
        """Get list of all metric categories"""
        return sorted(list(self.metrics_config.keys()))
    
    def get_metrics_count(self) -> int:
        """Get total count of loaded metrics"""
        return sum(len(metrics) for metrics in self.metrics_config.values())
    
    def get_file_summary(self) -> Dict[str, Dict[str, Any]]:
        """Get summary of all loaded metrics files"""
        summary = {}
        
        for file_path in self.metrics_metadata.get('loaded_files', []):
            metrics_count = self.metrics_metadata.get('metrics_by_file', {}).get(file_path, 0)
            file_name = Path(file_path).name
            
            # Get categories for this file
            categories = set()
            for category, metrics_list in self.metrics_config.items():
                for metric in metrics_list:
                    if metric.get('source_file') == file_path:
                        categories.add(category)
            
            summary[file_path] = {
                'file_name': file_name,
                'metrics_count': metrics_count,
                'categories': sorted(list(categories)),
                'category_count': len(categories)
            }
        
        return summary
    
    def search_metrics(self, search_term: str, search_in: str = 'all') -> List[Dict[str, Any]]:
        """Search metrics by name, description, or category"""
        results = []
        search_term_lower = search_term.lower()
        
        for category_metrics in self.metrics_config.values():
            for metric in category_metrics:
                match = False
                
                if search_in in ['name', 'all']:
                    if search_term_lower in metric.get('name', '').lower():
                        match = True
                
                if search_in in ['description', 'all']:
                    if search_term_lower in metric.get('description', '').lower():
                        match = True
                
                if search_in in ['category', 'all']:
                    if search_term_lower in metric.get('category', '').lower():
                        match = True
                
                if match:
                    results.append(metric)
        
        return results
    
    def validate_config(self) -> Dict[str, Any]:
        """Validate configuration and return status"""
        status = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Check kubeconfig
        if self.kubeconfig_path and not Path(self.kubeconfig_path).exists():
            status['warnings'].append(f"Kubeconfig file not found: {self.kubeconfig_path}")
        
        # Check if we have any metrics loaded
        total_metrics = self.get_metrics_count()
        if total_metrics == 0:
            status['warnings'].append("No metrics loaded - use load_metrics_file() to load metrics")
        
        return status


# Singleton instance for etcd config
_etcd_config_instance = None


def get_etcd_config():
    """
    Get or create the singleton Config instance for etcd metrics.
    This function lazy-loads the etcd metrics configuration on first call.
    
    Returns:
        Config: Singleton Config instance with etcd metrics loaded
    """
    global _etcd_config_instance
    
    if _etcd_config_instance is None:
        _etcd_config_instance = Config()
        # Load etcd metrics configuration
        project_root = Path(__file__).parent.parent
        etcd_metrics_path = project_root / 'config' / 'metrics-etcd.yml'
        
        if etcd_metrics_path.exists():
            result = _etcd_config_instance.load_metrics_file(str(etcd_metrics_path))
            if not result.get('success'):
                # Log warning but continue
                import logging
                logging.warning(f"Failed to load etcd metrics: {result.get('error')}")
    
    return _etcd_config_instance


# Example usage
if __name__ == "__main__":
    # Initialize empty config
    config = Config()
    
    # Explicitly load metrics file
    result = config.load_metrics_file('config/metrics-etcd.yml')
    print(f"Load result: {result}")
    
    # Show summary
    print(f"\nTotal metrics: {config.get_metrics_count()}")
    print(f"Categories: {config.get_all_categories()}")