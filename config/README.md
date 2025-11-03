# Metrics Configuration Reader

A flexible, feature-rich configuration management system for OpenShift OVN-Kubernetes benchmarking metrics. This module provides dynamic metrics loading from multiple YAML files with support for hot-reloading, validation, and organized metric categorization.

## Table of Contents

- [Features](#-features)
- [Requirements](#-requirements)
- [Quick Start](#-quick-start)
- [Configuration](#-configuration)
- [Metrics File Format](#-metrics-file-format)
- [API Reference](#-api-reference)
- [Usage Scenarios](#-usage-scenarios)
- [Best Practices](#-best-practices)
- [Performance](#-performance)
- [Testing](#-testing)

---

## 🌟 Features

### Core Capabilities
- **Multi-file Support**: Load metrics from single or multiple YAML files simultaneously
- **Explicit File Loading**: No default files - you specify exactly which files to load
- **Dynamic Loading**: Load metrics files at initialization or dynamically at runtime
- **Category Organization**: Automatically organizes metrics into logical categories for easy access
- **Environment Variables**: Full configuration via environment variables for container/cloud deployments
- **Validation**: Built-in configuration validation with detailed error and warning reporting
- **Flexible Format**: Supports multiple YAML formats (list or dict with 'metrics' key)
- **Auto-discovery**: Optional auto-discovery of metrics files in a directory

### Integration Features
- **Prometheus Integration**: Configurable URL, authentication, SSL, and timeout settings
- **Report Generation**: Multiple output formats (Excel, PDF) with configurable directories
- **Type Safety**: Pydantic-based validation and serialization
- **Container-Ready**: 12-factor app compliant, perfect for Kubernetes/Docker
- **CI/CD Compatible**: Validation and testing support for automated deployments
- **Logging Integration**: Configurable logging with structured messages

### Performance & Safety
- **Lazy Loading**: Metrics loaded only when needed
- **Efficient Storage**: O(1) category lookup with minimal memory overhead
- **Error Handling**: Comprehensive error handling with graceful degradation
- **Self-Documenting**: Metrics contain their own documentation

---

## 📋 Requirements

```bash
pip install pydantic pyyaml
```

---

## 🚀 Quick Start

### Method 1: Load Files at Initialization

```python
from metrics_config_reader import Config

# Initialize with specific metrics files
config = Config(
    metrics_files=['metrics-net.yml', 'metrics-node.yml'],
    metrics_directory='config'
)

# Get all metrics organized by category
all_metrics = config.get_all_metrics()

# Get metrics by category
network_metrics = config.get_metrics_by_category("network_io")

# Get specific metric by name
cpu_metric = config.get_metric_by_name("node_cpu_usage")

# Get statistics
print(f"Total metrics: {config.get_metrics_count()}")
print(f"Categories: {config.get_all_categories()}")
```

### Method 2: Load Files Separately

```python
# Initialize empty config
config = Config()

# Load files one by one
config.load_metrics_file('metrics-net.yml')
config.load_metrics_file('metrics-node.yml')
config.load_metrics_file('metrics-etcd.yml')

print(f"Loaded {config.get_metrics_count()} metrics")
```

### Method 3: Load Multiple Files at Once

```python
# Initialize empty config
config = Config()

# Load multiple files in one call
result = config.load_multiple_metrics_files([
    'metrics-net.yml',
    'metrics-node.yml',
    'metrics-etcd.yml'
])

print(f"Success: {result['success']}")
print(f"Files loaded: {result['files_loaded']}")
print(f"Total metrics: {result['total_metrics_loaded']}")
```

---

## 🔧 Configuration

### Via Constructor

```python
from metrics_config_reader import Config, PrometheusConfig, ReportConfig

# Method 1: Specify files at initialization
config = Config(
    metrics_files=['metrics-net.yml', 'metrics-node.yml'],
    metrics_directory='config',
    kubeconfig_path="~/.kube/config",
    prometheus=PrometheusConfig(
        url="https://prometheus.example.com",
        token="your-token-here",
        timeout=60,
        verify_ssl=True
    ),
    reports=ReportConfig(
        output_dir="reports",
        formats=["excel", "pdf"],
        template_dir="templates"
    ),
    timezone="America/New_York",
    log_level="DEBUG"
)

# Method 2: Start empty and load later
config = Config(
    kubeconfig_path="~/.kube/config",
    metrics_directory='config'
)
config.load_metrics_file('metrics-net.yml')
config.load_metrics_file('metrics-node.yml')
```

### Via Environment Variables

```bash
# Kubeconfig
export KUBECONFIG=/path/to/kubeconfig

# Prometheus
export PROMETHEUS_URL=https://prometheus.example.com
export PROMETHEUS_TOKEN=your-token-here

# Metrics files (comma-separated)
export METRICS_FILES=metrics-net.yml,metrics-node.yml,metrics-etcd.yml

# Reports
export REPORT_OUTPUT_DIR=./reports

# Logging
export LOG_LEVEL=DEBUG
```

Then initialize:

```python
# Loads from environment variables
config = Config()
```

---

## 📊 Metrics File Format

### Supported YAML Formats

**Format 1: List with 'metrics' key** (Recommended)
```yaml
metrics:
  - name: node_cpu_usage
    title: "Node CPU Usage"
    expr: 'sum by (instance)(irate(node_cpu_seconds_total[5m])) * 100'
    unit: percent
    description: "CPU usage per node"
    category: node_usage
    
  - name: node_memory_used
    title: "Node Memory Used"
    expr: 'node_memory_MemTotal_bytes - node_memory_MemFree_bytes'
    unit: bytes
    description: "Memory used per node"
    category: node_usage
```

**Format 2: Direct list**
```yaml
- name: network_io_rx
  title: "Network RX"
  expr: 'rate(node_network_receive_bytes_total[5m])'
  unit: bytes_per_second
  description: "Network receive rate"
  category: network_io
```

### Metric Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique metric identifier |
| `title` | string | No | Display title (defaults to name) |
| `expr` | string | Yes | Prometheus query expression |
| `unit` | string | No | Unit of measurement |
| `description` | string | No | Metric description |
| `category` | string | No | Metric category (defaults to 'general') |

### Supported Units

- `percent` - Percentage values (0-100)
- `bytes` - Byte measurements
- `bits_per_second` - Network throughput/bandwidth
- `packets_per_second` - Packet rates
- `bytes_per_second` - Byte transfer rates
- `count` - Simple counters
- `count_per_second` - Rate counters
- `eps` - Events per second

### Common Categories

Example categories you might use:
- `node_usage` - Node resource utilization
- `network_l1` - Physical network layer
- `network_socket_tcp` - TCP socket statistics
- `network_socket_udp` - UDP socket statistics
- `network_socket_mem` - Socket memory usage
- `network_socket_softnet` - Softnet processing
- `network_socket_ip` - IP layer statistics
- `network_netstat_tcp` - TCP netstat metrics
- `network_netstat_udp` - UDP netstat metrics
- `network_io` - Network I/O metrics

---

## 📚 API Reference

### Initialization & Loading

#### `Config(metrics_files=[], metrics_directory='config', ...)`
Initialize configuration.

```python
# Empty initialization (load files later)
config = Config()

# With specific files
config = Config(
    metrics_files=['metrics-net.yml', 'metrics-node.yml'],
    metrics_directory='config'
)
```

#### `load_metrics_file(file_name, category_filter=None)`
Load metrics from a specific file.

```python
# Load entire file
result = config.load_metrics_file('metrics-net.yml')

# Load with category filter
result = config.load_metrics_file(
    'metrics-net.yml',
    category_filter=['network_netstat_tcp', 'network_io']
)

# Load from custom path
result = config.load_metrics_file('custom/path/metrics-custom.yml')

print(f"Loaded {result['metrics_loaded']} metrics")
print(f"Categories: {result['categories_loaded']}")
```

**Returns:**
```python
{
    'success': True,
    'file_path': 'config/metrics-net.yml',
    'file_name': 'metrics-net.yml',
    'metrics_loaded': 95,
    'categories_loaded': ['network_l1', 'network_socket_tcp', ...],
    'total_metrics': 150
}
```

#### `load_multiple_metrics_files(file_names)`
Load multiple files at once.

```python
result = config.load_multiple_metrics_files([
    'metrics-net.yml',
    'metrics-node.yml',
    'metrics-etcd.yml',
    'metrics-storage.yml'
])

print(f"Files loaded: {result['files_loaded']}")
print(f"Files failed: {result['files_failed']}")
print(f"Total metrics: {result['total_metrics_loaded']}")

# Check details for each file
for detail in result['details']:
    print(f"{detail['file_name']}: {detail['metrics_loaded']} metrics")
```

**Returns:**
```python
{
    'success': True,
    'files_loaded': 3,
    'files_failed': 1,
    'total_metrics_loaded': 250,
    'details': [
        {'success': True, 'file_name': 'metrics-net.yml', ...},
        {'success': True, 'file_name': 'metrics-node.yml', ...},
        {'success': False, 'error': 'File not found', ...}
    ]
}
```

### Querying Metrics

#### `get_all_metrics()`
Get all metrics organized by category.

```python
all_metrics = config.get_all_metrics()
# Returns: Dict[category, List[metric_dict]]

for category, metrics in all_metrics.items():
    print(f"\n{category}:")
    for metric in metrics:
        print(f"  - {metric['name']}: {metric['description']}")
```

#### `get_metrics_by_category(category)`
Get all metrics in a specific category.

```python
network_metrics = config.get_metrics_by_category("network_io")

for metric in network_metrics:
    print(f"{metric['name']}: {metric['expr']}")
```

#### `get_metrics_by_file(file_name)`
Get all metrics from a specific source file.

```python
net_metrics = config.get_metrics_by_file('metrics-net.yml')
print(f"Found {len(net_metrics)} metrics from metrics-net.yml")
```

#### `get_metric_by_name(name)`
Get a specific metric by its unique name.

```python
metric = config.get_metric_by_name("node_cpu_usage")
if metric:
    print(f"Name: {metric['name']}")
    print(f"Expression: {metric['expr']}")
    print(f"Unit: {metric['unit']}")
    print(f"Category: {metric['category']}")
    print(f"Source: {metric['source_file']}")
```

#### `get_categories_by_file(file_name)`
Get categories defined in a specific file.

```python
categories = config.get_categories_by_file('metrics-net.yml')
print(f"Categories in metrics-net.yml: {categories}")
```

#### `get_files_by_category(category)`
Get source files containing a specific category.

```python
files = config.get_files_by_category('network_io')
print(f"Files with network_io metrics: {files}")
```

#### `get_all_categories()`
Get list of all metric categories.

```python
categories = config.get_all_categories()
print(f"Available categories: {categories}")
```

#### `get_metrics_count()`
Get total count of loaded metrics.

```python
total = config.get_metrics_count()
print(f"Total metrics loaded: {total}")
```

#### `search_metrics(search_term, search_in='all')`
Search metrics by term.

```python
# Search in all fields
tcp_metrics = config.search_metrics('tcp')

# Search only in names
tcp_names = config.search_metrics('tcp', search_in='name')

# Search in descriptions
error_metrics = config.search_metrics('error', search_in='description')

# Search in categories
network_cats = config.search_metrics('network', search_in='category')
```

#### `filter_metrics(category=None, file_name=None, name_contains=None)`
Filter metrics by multiple criteria.

```python
# Filter by category and file
filtered = config.filter_metrics(
    category='network_io',
    file_name='metrics-net.yml'
)

# Filter by name pattern
tcp_filtered = config.filter_metrics(name_contains='tcp')

# Combined filters
specific = config.filter_metrics(
    category='network_netstat_tcp',
    file_name='metrics-net.yml',
    name_contains='error'
)
```

### File Management

#### `get_loaded_files()`
Get list of currently loaded files.

```python
files = config.get_loaded_files()
print(f"Loaded files: {files}")
```

#### `get_file_summary()`
Get detailed summary of all loaded files.

```python
summary = config.get_file_summary()

for file_path, info in summary.items():
    print(f"\nFile: {info['file_name']}")
    print(f"  Metrics: {info['metrics_count']}")
    print(f"  Categories: {info['categories']}")
```

**Returns:**
```python
{
    'config/metrics-net.yml': {
        'file_name': 'metrics-net.yml',
        'metrics_count': 95,
        'categories': ['network_l1', 'network_socket_tcp', ...],
        'category_count': 9
    },
    ...
}
```

#### `list_available_files()`
List all loaded files with details.

```python
files = config.list_available_files()

for file_info in files:
    print(f"\nFile: {file_info['file_name']}")
    print(f"  Path: {file_info['file_path']}")
    print(f"  Metrics: {file_info['metrics_count']}")
    print(f"  Categories: {file_info['categories']}")
    print(f"  Exists: {file_info['exists']}")
```

#### `remove_metrics_file(file_name, reload=True)`
Remove a metrics file from configuration.

```python
# Remove and reload
result = config.remove_metrics_file('metrics-old.yml')
if result['success']:
    print(f"File removed, now have {result['new_count']} metrics")

# Remove without reload
result = config.remove_metrics_file('metrics-old.yml', reload=False)
```

#### `reload_metrics(clear_existing=False)`
Reload metrics from configured files.

```python
# Merge new metrics with existing
result = config.reload_metrics(clear_existing=False)
print(f"Added {result['added_metrics']} new metrics")

# Complete reload (clear and reload)
result = config.reload_metrics(clear_existing=True)
print(f"Reloaded {result['new_count']} metrics")
```

### Validation & Metadata

#### `validate_config()`
Validate configuration and return status.

```python
status = config.validate_config()

if status['valid']:
    print("✅ Configuration is valid")
else:
    print("❌ Configuration has errors:")
    for error in status['errors']:
        print(f"  - {error}")

if status['warnings']:
    print("⚠️  Warnings:")
    for warning in status['warnings']:
        print(f"  - {warning}")
```

#### `get_metrics_metadata()`
Get metadata about loaded metrics.

```python
metadata = config.get_metrics_metadata()

print(f"Total metrics: {metadata['total_metrics']}")
print(f"Categories: {metadata['categories']}")
print(f"Loaded files: {metadata['loaded_files']}")
print(f"Metrics by file: {metadata['metrics_by_file']}")
```

#### `print_summary()`
Print formatted summary of configuration.

```python
config.print_summary()

# Output:
# ============================================================
# METRICS CONFIGURATION SUMMARY
# ============================================================
# 
# 📁 Loaded Files: 2
#    • metrics-net.yml: 95 metrics
#    • metrics-node.yml: 5 metrics
# 
# 📊 Total Metrics: 100
# 📂 Total Categories: 10
# 
# 📋 Categories:
#    • network_io: 25 metrics
#      Sources: metrics-net.yml
#    • node_usage: 5 metrics
#      Sources: metrics-node.yml
# ...
```

---

## 🎯 Usage Scenarios

### Scenario 1: Simple Setup - Load Specific Files

```python
from metrics_config_reader import Config

# Load only the metrics you need
config = Config(
    metrics_files=['metrics-net.yml', 'metrics-node.yml'],
    metrics_directory='config'
)

# Use metrics
print(f"Loaded {config.get_metrics_count()} metrics")
for category in config.get_all_categories():
    metrics = config.get_metrics_by_category(category)
    print(f"{category}: {len(metrics)} metrics")
```

### Scenario 2: Dynamic Service Discovery

```python
import os
from kubernetes import client, config as k8s_config

# Start with empty config
config = Config(metrics_directory='config')

# Detect available services and load relevant metrics
k8s_config.load_incluster_config()
v1 = client.CoreV1Api()
services = v1.list_service_for_all_namespaces()

for svc in services.items:
    if svc.metadata.name == "etcd":
        config.load_metrics_file('metrics-etcd.yml')
    elif svc.metadata.name == "ovn-kubernetes":
        config.load_metrics_file('metrics-ovnk.yml')
    elif "prometheus" in svc.metadata.name:
        config.load_metrics_file('metrics-prometheus.yml')

print(f"Loaded {config.get_metrics_count()} metrics for detected services")
```

### Scenario 3: Development with Hot Reload

```python
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class MetricsWatcher(FileSystemEventHandler):
    def __init__(self, config):
        self.config = config
        self.last_reload = time.time()
    
    def on_modified(self, event):
        if not event.src_path.endswith('.yml'):
            return
        
        # Debounce: wait 2 seconds between reloads
        if time.time() - self.last_reload < 2:
            return
        
        print(f"\n🔄 Detected change in {event.src_path}")
        
        # Reload the specific file
        file_name = os.path.basename(event.src_path)
        result = self.config.load_metrics_file(file_name)
        
        if result['success']:
            print(f"✅ Reloaded {file_name}: {result['metrics_loaded']} metrics")
        
        self.last_reload = time.time()

# Setup with specific files
config = Config(
    metrics_files=['metrics-net.yml', 'metrics-node.yml'],
    metrics_directory='config'
)

watcher = MetricsWatcher(config)
observer = Observer()
observer.schedule(watcher, "config/", recursive=True)
observer.start()

print("👁️  Watching for metrics file changes...")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
    observer.join()
```

### Scenario 5: Multi-Environment Configuration

```python
import os

ENV = os.getenv("ENVIRONMENT", "dev")

env_configs = {
    "dev": {
        "files": ['metrics-node.yml'],  # Only node metrics for dev
        "prometheus_url": "http://localhost:9090",
        "log_level": "DEBUG"
    },
    "staging": {
        "files": ['metrics-node.yml', 'metrics-net.yml'],
        "prometheus_url": "https://prometheus.staging.example.com",
        "log_level": "INFO"
    },
    "prod": {
        "files": ['metrics-node.yml', 'metrics-net.yml', 'metrics-etcd.yml'],  # All metrics in production
        "prometheus_url": "https://prometheus.prod.example.com",
        "log_level": "WARNING"
    }
}

env_cfg = env_configs[ENV]

config = Config(
    metrics_files=env_cfg["files"],
    metrics_directory='config',
    prometheus=PrometheusConfig(url=env_cfg["prometheus_url"]),
    log_level=env_cfg["log_level"]
)

print(f"🌍 Environment: {ENV}")
print(f"📊 Loaded {config.get_metrics_count()} metrics")
```

### Scenario 6: Feature Flag Driven Metrics

```python
class FeatureFlags:
    ENABLE_ADVANCED_NETWORK = True
    ENABLE_ETCD_METRICS = True
    ENABLE_STORAGE_METRICS = False

# Start with base metrics
config = Config(
    metrics_files=['metrics-node.yml'],
    metrics_directory='config'
)

# Conditionally load additional metrics
if FeatureFlags.ENABLE_ADVANCED_NETWORK:
    result = config.load_metrics_file('metrics-net.yml')
    print(f"✅ Network metrics: {result['metrics_loaded']}")

if FeatureFlags.ENABLE_ETCD_METRICS:
    result = config.load_metrics_file('metrics-etcd.yml')
    print(f"✅ ETCD metrics: {result['metrics_loaded']}")

if FeatureFlags.ENABLE_STORAGE_METRICS:
    result = config.load_metrics_file('metrics-storage.yml')
    print(f"✅ Storage metrics: {result['metrics_loaded']}")

print(f"\n📊 Total active metrics: {config.get_metrics_count()}")
```

### Scenario 7: Batch Loading with Error Handling

```python
# Load multiple files and handle errors gracefully
config = Config(metrics_directory='config')

files_to_load = [
    'metrics-net.yml',
    'metrics-node.yml',
    'metrics-etcd.yml',
    'metrics-storage.yml',
    'metrics-missing.yml'  # This file doesn't exist
]

result = config.load_multiple_metrics_files(files_to_load)

print(f"Successfully loaded: {result['files_loaded']} files")
print(f"Failed to load: {result['files_failed']} files")
print(f"Total metrics: {result['total_metrics_loaded']}")

# Check individual results
for detail in result['details']:
    if detail['success']:
        print(f"  ✅ {detail.get('file_name', 'unknown')}: {detail['metrics_loaded']} metrics")
    else:
        print(f"  ❌ {detail.get('file_path', 'unknown')}: {detail['error']}")
```

### Scenario 8: Category-Filtered Loading

```python
# Load only specific categories from a file
config = Config()

# Load only TCP-related metrics from network file
result = config.load_metrics_file(
    'metrics-net.yml',
    category_filter=['network_netstat_tcp', 'network_socket_tcp']
)

print(f"Loaded {result['metrics_loaded']} TCP metrics")
print(f"Categories: {result['categories_loaded']}")

# Load only node usage metrics
result = config.load_metrics_file(
    'metrics-node.yml',
    category_filter=['node_usage']
)

print(f"Total metrics: {config.get_metrics_count()}")
```

### Scenario 9: REST API Service

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Metrics Configuration API")

# Initialize with specific metrics files
config = Config(
    metrics_files=['metrics-net.yml', 'metrics-node.yml', 'metrics-etcd.yml'],
    metrics_directory='config'
)

class LoadFileRequest(BaseModel):
    file_name: str
    category_filter: list = None

@app.get("/metrics")
async def get_all_metrics():
    return config.get_all_metrics()

@app.get("/metrics/categories")
async def get_categories():
    return {"categories": config.get_all_categories()}

@app.get("/metrics/category/{category}")
async def get_metrics_by_category(category: str):
    metrics = config.get_metrics_by_category(category)
    if not metrics:
        raise HTTPException(status_code=404, detail="Category not found")
    return {"category": category, "metrics": metrics}

@app.get("/metrics/files")
async def list_files():
    return config.list_available_files()

@app.post("/metrics/load")
async def load_file(request: LoadFileRequest):
    result = config.load_metrics_file(
        request.file_name,
        category_filter=request.category_filter
    )
    if not result['success']:
        raise HTTPException(status_code=400, detail=result['error'])
    return result

@app.get("/health")
async def health_check():
    status = config.validate_config()
    if not status['valid']:
        raise HTTPException(status_code=503, detail=status)
    return {
        "status": "healthy",
        "metrics_count": config.get_metrics_count(),
        "files_loaded": len(config.get_loaded_files())
    }
```
