# Metrics Configuration Reader

A flexible, feature-rich configuration management system for OpenShift OVN-Kubernetes benchmarking metrics. This module provides dynamic metrics loading from multiple YAML files with support for hot-reloading, validation, and organized metric categorization.

## Table of Contents

- [Features](#-features)
- [Requirements](#-requirements)
- [Quick Start](#-quick-start)
- [Configuration](#-configuration)
- [Metrics File Format](#-metrics-file-format)
- [Available Metrics Files](#-available-metrics-files)
- [API Reference](#-api-reference)
- [Usage Scenarios](#-usage-scenarios)
- [Best Practices](#-best-practices)
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
- **Category Filtering**: Load only specific categories from metrics files

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
- **Singleton Support**: Built-in singleton pattern for etcd configuration

---

## 📋 Requirements

```bash
pip install pydantic pyyaml
```

**Python Version**: 3.7+

---

## 🚀 Quick Start

### Method 1: Load Files at Initialization

```python
from metrics_config_reader import Config

# Initialize with specific metrics files
config = Config(
    metrics_files=['metrics-net.yml', 'metrics-etcd.yml'],
    metrics_directory='config'
)

# Get all metrics organized by category
all_metrics = config.get_all_metrics()

# Get metrics by category
network_metrics = config.get_metrics_by_category("network_io")

# Get specific metric by name
cpu_metric = config.get_metric_by_name("etcd_pods_cpu_usage")

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
config.load_metrics_file('metrics-etcd.yml')
config.load_metrics_file('metrics-ovs.yml')

print(f"Loaded {config.get_metrics_count()} metrics")
```

### Method 3: Load Multiple Files at Once

```python
# Initialize empty config
config = Config()

# Load multiple files in one call
result = config.load_multiple_metrics_files([
    'metrics-net.yml',
    'metrics-etcd.yml',
    'metrics-ovs.yml',
    'metrics-api.yml'
])

print(f"Success: {result['success']}")
print(f"Files loaded: {result['files_loaded']}")
print(f"Total metrics: {result['total_metrics_loaded']}")
```

### Method 4: Using the Singleton Pattern (for etcd)

```python
from metrics_config_reader import get_etcd_config

# Get singleton instance (auto-loads metrics-etcd.yml)
config = get_etcd_config()

# Access etcd metrics
etcd_metrics = config.get_metrics_by_category("general_info")
```

---

## 🔧 Configuration

### Via Constructor

```python
from metrics_config_reader import Config, PrometheusConfig, ReportConfig

# Method 1: Specify files at initialization
config = Config(
    metrics_files=['metrics-net.yml', 'metrics-etcd.yml'],
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
config.load_metrics_file('metrics-etcd.yml')
```

### Via Environment Variables

```bash
# Kubeconfig
export KUBECONFIG=/path/to/kubeconfig

# Prometheus
export PROMETHEUS_URL=https://prometheus.example.com
export PROMETHEUS_TOKEN=your-token-here

# Metrics files (loaded from metrics_directory)
export METRICS_FILES=metrics-net.yml,metrics-etcd.yml,metrics-ovs.yml

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
  - name: etcd_pods_cpu_usage
    title: "etcd_pods_cpu_usage"
    expr: 'sum(irate(container_cpu_usage_seconds_total{namespace="openshift-etcd"}[2m])) by (pod) * 100'
    unit: percent
    description: "CPU usage percentage for etcd containers"
    category: general_info
    
  - name: etcd_db_physical_size
    title: "etcd_db_physical_size"
    expr: 'etcd_mvcc_db_total_size_in_bytes{namespace="openshift-etcd"} / 1024 / 1024'
    unit: MB
    description: "Physical size of etcd database in megabytes"
    category: general_info
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
- `MB` - Megabytes
- `bits_per_second` - Network throughput/bandwidth
- `packets_per_second` - Packet rates
- `bytes_per_second` - Byte transfer rates
- `operations_per_second` - Operation rates
- `count` - Simple counters
- `count_per_second` - Rate counters
- `eps` - Events per second
- `seconds` - Time duration
- `milliseconds` - Time duration in milliseconds
- `per_second` - Generic rate measurement
- `per_day` - Daily rate measurement
- `boolean` - Boolean values (0/1)
- `flows` - Network flow counts
- `errors_per_second` - Error rates
- `iops` - I/O operations per second
- `threads` - Thread counts
- `packets` - Packet counts
- `faults` - Page faults
- `faults/s` - Page fault rate
- `requests_per_second` - Request rates

---

## 📁 Available Metrics Files

The following metrics files are available in the `config/` directory:

### 1. **metrics-alert.yml**
- **Category**: `alerts`
- **Metrics**: 1
- **Description**: Alert monitoring and tracking
- Example: `top_alerts` - Top 15 alerts by severity

### 2. **metrics-api.yml**
- **Categories**: `api_server`
- **Metrics**: 15
- **Description**: Kubernetes API Server performance metrics
- Covers:
  - API call latency (read-only and mutating)
  - Request rates and errors
  - In-flight requests
  - etcd request duration
  - Priority and Fairness metrics

### 3. **metrics-cni.yml**
- **Category**: `cni`
- **Metrics**: 18
- **Description**: Container Network Interface (CNI) and CRI-O metrics
- Covers:
  - CPU and memory usage for kubelet and CRI-O
  - Network usage, drops, and errors
  - Container and control plane threads
  - Disk I/O operations (IOPS)

### 4. **metrics-disk.yml**
- **Category**: `disk_io`
- **Metrics**: 8
- **Description**: Disk I/O performance metrics
- Covers:
  - Container disk writes
  - Node disk throughput (read/write)
  - Disk IOPS (read/write)
  - Disk operation latency

### 5. **metrics-etcd.yml**
- **Categories**: `general_info`, `disk_compact_defrag`, `disk_wal_fsync`, `disk_backend_commit`, `network_io`
- **Metrics**: 51
- **Description**: Comprehensive etcd cluster metrics
- Covers:
  - CPU and memory usage
  - Database size and space utilization
  - Proposal processing (commit, apply, failure rates)
  - Leader elections and cluster health
  - Disk compaction and defragmentation
  - WAL fsync performance
  - Backend commit operations
  - Network I/O and peer communication
  - gRPC streams (watch, lease)

### 6. **metrics-latency.py** (actually .yml format)
- **Category**: `latency`
- **Metrics**: 18
- **Description**: Network and pod lifecycle latency metrics
- Covers:
  - CNI request latency (ADD/DEL operations)
  - Pod annotation processing
  - Pod creation stages (LSP creation, port binding)
  - Service synchronization latency
  - Network configuration application
  - Controller and node ready duration

### 7. **metrics-net.yml**
- **Categories**: `network_l1`, `network_socket_tcp`, `network_socket_udp`, `network_socket_mem`, `network_socket_softnet`, `network_socket_ip`, `network_netstat_ip`, `network_netstat_tcp`, `network_netstat_udp`, `network_io`
- **Metrics**: 95
- **Description**: Comprehensive network stack metrics
- Covers:
  - Layer 1: Network operational status, speed, MTU, ARP
  - Socket statistics: TCP/UDP connections, memory usage
  - Softnet processing and drops
  - IP layer statistics
  - TCP/UDP netstat metrics (errors, retransmissions)
  - Network I/O: throughput, packets, drops, errors
  - Connection tracking

### 8. **metrics-node.yml**
- **Category**: `node_usage`
- **Metrics**: 5
- **Description**: Node resource utilization metrics
- Covers:
  - CPU usage by mode
  - Memory usage and cache/buffer
  - Cgroup CPU and RSS usage

### 9. **metrics-ovn.yml**
- **Category**: `ovn`
- **Metrics**: 2
- **Description**: OVN database size metrics
- Covers:
  - Northbound database size
  - Southbound database size

### 10. **metrics-ovs.yml**
- **Category**: `ovs`
- **Metrics**: 18
- **Description**: Open vSwitch (OVS) performance metrics
- Covers:
  - CPU and memory usage for vswitchd and ovsdb-server
  - Flow table statistics (datapath, br-int, br-ex)
  - Connection and stream management
  - Megaflow cache performance
  - Datapath packet processing and errors
  - Network interface statistics

### 11. **metrics-pods.yml**
- **Category**: `pods`
- **Metrics**: 6
- **Description**: Pod and container metrics
- Covers:
  - Container CPU usage
  - Container memory (RSS, working set)
  - Pod status by phase
  - Namespace status
  - Pod distribution across nodes

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
    metrics_files=['metrics-net.yml', 'metrics-etcd.yml'],
    metrics_directory='config'
)
```

#### `load_metrics_file(file_name, category_filter=None)`
Load metrics from a specific file with optional category filtering.

```python
# Load entire file
result = config.load_metrics_file('metrics-net.yml')

# Load with category filter
result = config.load_metrics_file(
    'metrics-net.yml',
    category_filter=['network_netstat_tcp', 'network_io']
)

# Load from custom path
result = config.load_metrics_file('config/metrics-etcd.yml')

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
    'metrics-etcd.yml',
    'metrics-ovs.yml',
    'metrics-api.yml'
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
        {'success': True, 'file_name': 'metrics-etcd.yml', ...},
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
metric = config.get_metric_by_name("etcd_pods_cpu_usage")
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
# Output: ['network_l1', 'network_socket_tcp', 'network_socket_udp', ...]
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
#    • metrics-etcd.yml: 51 metrics
# 
# 📊 Total Metrics: 146
# 📂 Total Categories: 15
# 
# 📋 Categories:
#    • network_io: 25 metrics
#      Sources: metrics-net.yml
#    • general_info: 31 metrics
#      Sources: metrics-etcd.yml
# ...
```

### Singleton Pattern

#### `get_etcd_config()`
Get or create singleton Config instance for etcd metrics.

```python
from metrics_config_reader import get_etcd_config

# Get singleton instance (auto-loads metrics-etcd.yml)
config = get_etcd_config()

# Access etcd metrics
etcd_metrics = config.get_metrics_by_category("general_info")

# Subsequent calls return the same instance
config2 = get_etcd_config()
assert config is config2  # True
```

---

## 🎯 Usage Scenarios

### Scenario 1: Load All Available Metrics

```python
from metrics_config_reader import Config

# Load all available metrics files
config = Config(metrics_directory='config')

all_files = [
    'metrics-alert.yml',
    'metrics-api.yml',
    'metrics-cni.yml',
    'metrics-disk.yml',
    'metrics-etcd.yml',
    'metrics-latency.py',
    'metrics-net.yml',
    'metrics-node.yml',
    'metrics-ovn.yml',
    'metrics-ovs.yml',
    'metrics-pods.yml'
]

result = config.load_multiple_metrics_files(all_files)
print(f"Loaded {result['total_metrics_loaded']} metrics from {result['files_loaded']} files")
```

### Scenario 2: Load Only Network Metrics

```python
# Focus on network monitoring
config = Config(metrics_directory='config')

network_files = [
    'metrics-net.yml',
    'metrics-ovn.yml',
    'metrics-ovs.yml'
]

config.load_multiple_metrics_files(network_files)

# Get all network I/O metrics
network_io = config.get_metrics_by_category('network_io')
print(f"Monitoring {len(network_io)} network I/O metrics")
```

### Scenario 3: Load Specific Categories

```python
# Load only TCP-related metrics from network file
config = Config()

result = config.load_metrics_file(
    'metrics-net.yml',
    category_filter=['network_netstat_tcp', 'network_socket_tcp']
)

print(f"Loaded {result['metrics_loaded']} TCP metrics")
print(f"Categories: {result['categories_loaded']}")
```

### Scenario 4: Dynamic Service Discovery

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
    if "etcd" in svc.metadata.name:
        config.load_metrics_file('metrics-etcd.yml')
    elif "ovn" in svc.metadata.name:
        config.load_metrics_file('metrics-ovn.yml')
        config.load_metrics_file('metrics-ovs.yml')
    elif "prometheus" in svc.metadata.name:
        config.load_metrics_file('metrics-api.yml')

print(f"Loaded {config.get_metrics_count()} metrics for detected services")
```

### Scenario 5: Development with Hot Reload

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

# Setup
config = Config(
    metrics_files=['metrics-net.yml', 'metrics-etcd.yml'],
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

### Scenario 6: Multi-Environment Configuration

```python
import os

ENV = os.getenv("ENVIRONMENT", "dev")

env_configs = {
    "dev": {
        "files": ['metrics-node.yml', 'metrics-pods.yml'],
        "prometheus_url": "http://localhost:9090",
        "log_level": "DEBUG"
    },
    "staging": {
        "files": ['metrics-node.yml', 'metrics-net.yml', 'metrics-pods.yml'],
        "prometheus_url": "https://prometheus.staging.example.com",
        "log_level": "INFO"
    },
    "prod": {
        "files": [
            'metrics-alert.yml',
            'metrics-api.yml',
            'metrics-cni.yml',
            'metrics-disk.yml',
            'metrics-etcd.yml',
            'metrics-latency.py',
            'metrics-net.yml',
            'metrics-node.yml',
            'metrics-ovn.yml',
            'metrics-ovs.yml',
            'metrics-pods.yml'
        ],
        "prometheus_url": "https://prometheus.prod.example.com",
        "log_level": "WARNING"
    }
}

env_cfg = env_configs[ENV]

config = Config(
    metrics_directory='config',
    prometheus=PrometheusConfig(url=env_cfg["prometheus_url"]),
    log_level=env_cfg["log_level"]
)

config.load_multiple_metrics_files(env_cfg["files"])

print(f"🌍 Environment: {ENV}")
print(f"📊 Loaded {config.get_metrics_count()} metrics")
```

### Scenario 7: REST API Service

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Metrics Configuration API")

# Initialize with core metrics
config = Config(
    metrics_files=['metrics-net.yml', 'metrics-etcd.yml', 'metrics-api.yml'],
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

@app.get("/metrics/summary")
async def get_summary():
    return {
        "total_metrics": config.get_metrics_count(),
        "categories": config.get_all_categories(),
        "files": config.get_file_summary()
    }
```

### Scenario 8: Monitoring Dashboard Builder

```python
# Build custom dashboards by selecting specific metrics
config = Config(metrics_directory='config')

# Load all metrics
config.load_multiple_metrics_files([
    'metrics-etcd.yml',
    'metrics-net.yml',
    'metrics-ovs.yml'
])

# Create dashboard sections
dashboard = {
    "etcd_health": {
        "title": "ETCD Cluster Health",
        "metrics": []
    },
    "network_performance": {
        "title": "Network Performance",
        "metrics": []
    }
}

# Select specific metrics for etcd health
etcd_health_metrics = [
    "etcd_has_leader",
    "etcd_pods_cpu_usage",
    "etcd_pods_memory_usage",
    "proposal_pending_total"
]

for metric_name in etcd_health_metrics:
    metric = config.get_metric_by_name(metric_name)
    if metric:
        dashboard["etcd_health"]["metrics"].append(metric)

# Select network metrics with high error rates
network_errors = config.search_metrics("error", search_in="name")
for metric in network_errors[:10]:  # Top 10 error metrics
    if metric['category'].startswith('network'):
        dashboard["network_performance"]["metrics"].append(metric)

print(f"Dashboard built with {sum(len(s['metrics']) for s in dashboard.values())} metrics")
```

### Scenario 9: Category-Based Analysis

```python
# Analyze metrics by category
config = Config(metrics_directory='config')
config.load_metrics_file('metrics-etcd.yml')

# Get all etcd categories
categories = config.get_all_categories()

print("ETCD Metrics Analysis:")
print("=" * 60)

for category in categories:
    metrics = config.get_metrics_by_category(category)
    
    print(f"\n{category.upper()} ({len(metrics)} metrics):")
    print("-" * 60)
    
    # Group by unit
    units = {}
    for metric in metrics:
        unit = metric.get('unit', 'unknown')
        if unit not in units:
            units[unit] = []
        units[unit].append(metric['name'])
    
    for unit, metric_names in units.items():
        print(f"  {unit}: {len(metric_names)} metrics")
        for name in metric_names[:3]:  # Show first 3
            print(f"    - {name}")
        if len(metric_names) > 3:
            print(f"    ... and {len(metric_names) - 3} more")
```

### Scenario 10: Batch Processing with Error Handling

```python
# Load multiple files and handle errors gracefully
config = Config(metrics_directory='config')

files_to_load = [
    'metrics-net.yml',
    'metrics-etcd.yml',
    'metrics-ovs.yml',
    'metrics-missing.yml'  # This file doesn't exist
]

result = config.load_multiple_metrics_files(files_to_load)

print(f"Successfully loaded: {result['files_loaded']} files")
print(f"Failed to load: {result['files_failed']} files")
print(f"Total metrics: {result['total_metrics_loaded']}")

# Check individual results
for detail in result['details']:
    if detail['success']:
        print(f"  ✅ {detail['file_name']}: {detail['metrics_loaded']} metrics")
    else:
        print(f"  ❌ {detail['file_path']}: {detail['error']}")
```

---

## 🎓 Best Practices

### 1. **Explicit File Loading**
Always specify which metrics files to load. The library does not automatically load any files.

```python
# Good: Explicit loading
config = Config(metrics_directory='config')
config.load_metrics_file('metrics-etcd.yml')

# Also good: Load at initialization
config = Config(
    metrics_files=['metrics-etcd.yml', 'metrics-net.yml'],
    metrics_directory='config'
)
```

### 2. **Use Category Filters for Large Files**
When you only need specific metrics, use category filtering to reduce memory usage.

```python
# Load only TCP metrics from the large network file
config.load_metrics_file(
    'metrics-net.yml',
    category_filter=['network_netstat_tcp', 'network_socket_tcp']
)
```

### 3. **Handle Loading Errors**
Always check the result of file loading operations.

```python
result = config.load_metrics_file('metrics-etcd.yml')

if not result['success']:
    logging.error(f"Failed to load metrics: {result['error']}")
    # Handle error appropriately
else:
    logging.info(f"Loaded {result['metrics_loaded']} metrics")
```

### 4. **Use Metadata for Discovery**
Leverage metadata methods to understand what's loaded.

```python
# Check what's available
print(f"Total metrics: {config.get_metrics_count()}")
print(f"Categories: {config.get_all_categories()}")

# Get detailed summary
summary = config.get_file_summary()
for file_path, info in summary.items():
    print(f"{info['file_name']}: {info['metrics_count']} metrics")
```

### 5. **Environment-Specific Configuration**
Use environment variables for different deployment environments.

```bash
# Production
export PROMETHEUS_URL=https://prometheus.prod.example.com
export METRICS_FILES=metrics-alert.yml,metrics-api.yml,metrics-etcd.yml

# Development
export PROMETHEUS_URL=http://localhost:9090
export METRICS_FILES=metrics-etcd.yml
```

### 6. **Validate Configuration**
Always validate configuration before using it in production.

```python
status = config.validate_config()

if not status['valid']:
    for error in status['errors']:
        logging.error(f"Configuration error: {error}")
    sys.exit(1)

if status['warnings']:
    for warning in status['warnings']:
        logging.warning(f"Configuration warning: {warning}")
```

### 7. **Use Singleton for Common Configurations**
For frequently accessed configurations like etcd, use the singleton pattern.

```python
from metrics_config_reader import get_etcd_config

# Always returns the same instance
config = get_etcd_config()
```

### 8. **Search Before Filtering**
Use search to discover available metrics before implementing filters.

```python
# Find all error-related metrics
error_metrics = config.search_metrics('error', search_in='description')

# Extract unique categories
error_categories = set(m['category'] for m in error_metrics)
print(f"Error metrics found in: {error_categories}")
```

### 9. **Organize Metrics by Use Case**
Structure your metric loading based on monitoring use cases.

```python
# Load metrics by monitoring focus
monitoring_profiles = {
    "basic": ['metrics-node.yml', 'metrics-pods.yml'],
    "network": ['metrics-net.yml', 'metrics-ovn.yml', 'metrics-ovs.yml'],
    "storage": ['metrics-etcd.yml', 'metrics-disk.yml'],
    "full": [
        'metrics-alert.yml', 'metrics-api.yml', 'metrics-cni.yml',
        'metrics-disk.yml', 'metrics-etcd.yml', 'metrics-latency.py',
        'metrics-net.yml', 'metrics-node.yml', 'metrics-ovn.yml',
        'metrics-ovs.yml', 'metrics-pods.yml'
    ]
}

profile = os.getenv("MONITORING_PROFILE", "basic")
config.load_multiple_metrics_files(monitoring_profiles[profile])
```

### 10. **Document Custom Categories**
When creating new metrics files, use clear, hierarchical category names.

```python
# Good category naming
"network_io"          # Top level
"network_netstat_tcp" # Specific protocol
"disk_backend_commit" # Subsystem + operation

# Avoid generic names
"metrics"
"data"
"general"
```

---

## 🧪 Testing

### Unit Testing Example

```python
import unittest
from metrics_config_reader import Config

class TestMetricsConfig(unittest.TestCase):
    
    def setUp(self):
        """Set up test config"""
        self.config = Config(metrics_directory='config')
    
    def test_load_etcd_metrics(self):
        """Test loading etcd metrics file"""
        result = self.config.load_metrics_file('metrics-etcd.yml')
        
        self.assertTrue(result['success'])
        self.assertGreater(result['metrics_loaded'], 0)
        self.assertIn('general_info', result['categories_loaded'])
    
    def test_load_with_category_filter(self):
        """Test category filtering"""
        result = self.config.load_metrics_file(
            'metrics-net.yml',
            category_filter=['network_io']
        )
        
        self.assertTrue(result['success'])
        self.assertEqual(result['categories_loaded'], ['network_io'])
    
    def test_get_metric_by_name(self):
        """Test retrieving specific metric"""
        self.config.load_metrics_file('metrics-etcd.yml')
        
        metric = self.config.get_metric_by_name('etcd_pods_cpu_usage')
        
        self.assertIsNotNone(metric)
        self.assertEqual(metric['name'], 'etcd_pods_cpu_usage')
        self.assertEqual(metric['category'], 'general_info')
    
    def test_search_metrics(self):
        """Test metric search functionality"""
        self.config.load_metrics_file('metrics-net.yml')
        
        results = self.config.search_metrics('tcp', search_in='name')
        
        self.assertGreater(len(results), 0)
        for metric in results:
            self.assertIn('tcp', metric['name'].lower())
    
    def test_load_multiple_files(self):
        """Test loading multiple files"""
        files = ['metrics-etcd.yml', 'metrics-net.yml']
        result = self.config.load_multiple_metrics_files(files)
        
        self.assertTrue(result['success'])
        self.assertEqual(result['files_loaded'], 2)
        self.assertGreater(result['total_metrics_loaded'], 50)
    
    def test_invalid_file(self):
        """Test handling of non-existent file"""
        result = self.config.load_metrics_file('non-existent.yml')
        
        self.assertFalse(result['success'])
        self.assertIn('error', result)
    
    def test_validation(self):
        """Test configuration validation"""
        status = self.config.validate_config()
        
        self.assertIn('valid', status)
        self.assertIn('errors', status)
        self.assertIn('warnings', status)
    
    def test_metrics_count(self):
        """Test metrics counting"""
        initial_count = self.config.get_metrics_count()
        
        self.config.load_metrics_file('metrics-etcd.yml')
        new_count = self.config.get_metrics_count()
        
        self.assertGreater(new_count, initial_count)

if __name__ == '__main__':
    unittest.main()
```

### Integration Testing Example

```python
import pytest
from metrics_config_reader import Config, get_etcd_config

@pytest.fixture
def config():
    """Fixture for config instance"""
    return Config(metrics_directory='config')

def test_full_workflow(config):
    """Test complete workflow"""
    # Load files
    files = ['metrics-etcd.yml', 'metrics-net.yml', 'metrics-ovs.yml']
    result = config.load_multiple_metrics_files(files)
    
    assert result['success']
    assert result['files_loaded'] == 3
    
    # Validate
    status = config.validate_config()
    assert status['valid']
    
    # Query metrics
    categories = config.get_all_categories()
    assert len(categories) > 0
    
    # Search
    error_metrics = config.search_metrics('error')
    assert len(error_metrics) > 0

def test_singleton_pattern():
    """Test etcd config singleton"""
    config1 = get_etcd_config()
    config2 = get_etcd_config()
    
    assert config1 is config2
    assert config1.get_metrics_count() > 0

def test_category_coverage(config):
    """Test that all expected categories are present"""
    config.load_metrics_file('metrics-etcd.yml')
    
    expected_categories = [
        'general_info',
        'disk_compact_defrag',
        'disk_wal_fsync',
        'disk_backend_commit',
        'network_io'
    ]
    
    actual_categories = config.get_all_categories()
    
    for category in expected_categories:
        assert category in actual_categories

def test_metric_structure(config):
    """Test that metrics have required fields"""
    config.load_metrics_file('metrics-etcd.yml')
    
    all_metrics = config.get_all_metrics()
    
    for category, metrics in all_metrics.items():
        for metric in metrics:
            assert 'name' in metric
            assert 'expr' in metric
            assert 'category' in metric
            assert 'source_file' in metric
```

---

## 📖 Additional Resources

### File Locations
- Metrics files: `config/*.yml`
- Python module: `metrics_config_reader.py`
- Exports directory: `exports/`
- Reports directory: `reports/` (configurable)
- Logs directory: `logs/`

### Common Issues and Solutions

**Issue**: `FileNotFoundError` when loading metrics
```python
# Solution: Verify file path and metrics_directory
config = Config(metrics_directory='config')  # Ensure correct directory
result = config.load_metrics_file('metrics-etcd.yml')
if not result['success']:
    print(f"Error: {result['error']}")
```

**Issue**: No metrics loaded despite successful file load
```python
# Solution: Check if file format is correct
# Ensure YAML has 'metrics:' key or is a direct list
```

**Issue**: Category not found
```python
# Solution: List available categories first
categories = config.get_all_categories()
print(f"Available: {categories}")
```

### Performance Considerations

- **Memory Usage**: Each metric is stored as a dictionary. Loading all 11 files results in approximately 230+ metrics.
- **Loading Time**: File loading is fast (< 100ms per file) but can be optimized by loading only needed files.
- **Query Performance**: Category lookups are O(1), name lookups are O(n) where n is total metrics.

### Contributing

When adding new metrics files:
1. Follow the standard YAML format with 'metrics' key
2. Include all required fields (name, expr, category)
3. Use clear, descriptive category names
4. Add unit information for all metrics
5. Document the metrics file in this README

---

## 📝 License

Please refer to your project's license file for licensing information.

---

## 🆘 Support

For issues, questions, or contributions:
1. Check this README for common solutions
2. Review the API Reference section
3. Examine the usage scenarios for similar use cases
4. Check metric files in `config/` directory for available metrics

---

**Version**: 1.0.0  
**Last Updated**: 2024