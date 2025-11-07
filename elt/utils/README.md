# ETCD Analyzer ELT Code Refactoring Guide

## Overview

This document describes the optimized architecture for the ETCD Analyzer ELT (Extract, Load, Transform) system. The refactoring separates concerns into three main components:

1. **Pure Utilities** (`analyzer_elt_utility.py`)
2. **Generic Orchestrator** (`analyzer_elt_json2table.py`)
3. **Metric-Specific Handlers** (e.g., `analyzer_elt_cluster_info.py`)

---

## Architecture Principles

### 1. Separation of Concerns
- **Utilities** contain only reusable formatting/processing functions
- **Orchestrator** handles data routing and provides plugin architecture
- **Handlers** contain metric-specific extraction logic

### 2. Easy Extension
- New metrics can be added by:
  1. Creating a new ELT handler class
  2. Implementing 4 required methods
  3. Registering with one function call

### 3. No File I/O
- All functions work with in-memory data structures
- Results are returned as dictionaries/DataFrames
- HTML generation is for display only

### 4. Critical & Top 1 Highlighting
- Critical values highlighted in **red** with ⚠️
- Warning values highlighted in **orange**
- Top 1 values highlighted with 🏆 and blue background

---

## File Structure

```
elt/
├── utils/
│   ├── analyzer_elt_utility.py       # Pure utility functions
│   └── analyzer_elt_json2table.py    # Generic orchestrator
└── ocp/
    └── analyzer_elt_cluster_info.py  # Cluster info handler
└── metrics/
    ├── analyzer_elt_disk_io.py       # Disk I/O handler (future)
    ├── analyzer_elt_wal_fsync.py     # WAL fsync handler (future)
    └── ...                            # Other metric handlers
```

---

## Component Details

### 1. `analyzer_elt_utility.py` (Pure Utilities)

**Purpose:** Reusable formatting and processing functions

**Key Functions:**
```python
# Text formatting
truncate_text(text, max_length)
truncate_url(url, max_length)
truncate_node_name(name, max_length)

# Capacity parsing
parse_cpu_capacity(cpu_str) -> int
parse_memory_capacity(memory_str) -> float
parse_db_size(size_str) -> float

# HTML generation
create_status_badge(status, value)
create_html_table(df, table_name)
decode_unicode_escapes(text)

# Value highlighting
highlight_critical_values(value, thresholds, unit, is_top)

# DataFrame utilities
limit_dataframe_columns(df, max_cols, table_name)
calculate_totals_from_nodes(nodes)
```

**Rules:**
- ✅ Generic formatting functions
- ✅ HTML generation utilities
- ✅ Data type conversions
- ❌ No metric-specific logic
- ❌ No data extraction logic
- ❌ No file I/O

---

### 2. `analyzer_elt_json2table.py` (Generic Orchestrator)

**Purpose:** Route data to appropriate handlers, provide plugin architecture

**Key Classes:**
```python
class MetricELTRegistry:
    """Registry for metric-specific handlers"""
    def register(metric_type, elt_class, identifier_func)
    def get_handler(metric_type)
    def identify_metric_type(data)

class GenericELT:
    """Main orchestrator"""
    def process_data(data) -> Dict
    def identify_data_type(data) -> str
```

**Key Functions:**
```python
# Main API
convert_json_to_html_table(json_data) -> str
process_metric_data(metric_data) -> Dict

# Extension API
register_metric_handler(metric_type, elt_class, identifier_func)
```

**Data Flow:**
```
Input JSON/Dict
    ↓
identify_data_type()
    ↓
get_handler(metric_type)
    ↓
handler.extract_*()
    ↓
handler.transform_to_dataframes()
    ↓
handler.generate_html_tables()
    ↓
Output: {summary, html_tables, dataframes}
```

---

### 3. Metric-Specific Handler (e.g., `analyzer_elt_cluster_info.py`)

**Required Methods:**

```python
class clusterInfoELT(utilityELT):
    
    def extract_cluster_info(self, data: Dict) -> Dict:
        """Extract and structure raw data"""
        # Extract specific fields
        # Apply metric-specific logic
        # Return structured dictionary
        
    def transform_to_dataframes(self, structured_data: Dict) -> Dict[str, pd.DataFrame]:
        """Convert structured data to DataFrames"""
        # Create one DataFrame per table section
        # Apply column limiting
        # Decode unicode
        
    def generate_html_tables(self, dataframes: Dict) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        # Use self.create_html_table() from utilityELT
        # Return dict of table_name -> html_string
        
    def summarize_cluster_info(self, data: Dict) -> str:
        """Generate summary text/HTML"""
        # Create brief summary for display
        # Return HTML or plain text
```

---

## Adding a New Metric Handler

### Step 1: Create Handler Class

```python
# elt/metrics/analyzer_elt_disk_io.py

from ..utils.etcd_analyzer_elt_utility import utilityELT

class diskIOELT(utilityELT):
    
    def extract_disk_io(self, data: Dict) -> Dict:
        """Extract disk I/O metrics"""
        structured = {
            'metrics_overview': [],
            'node_performance': [],
            'throughput_metrics': []
        }
        
        # Extract metrics from data
        metrics = data.get('metrics', {})
        
        # Process each metric
        for metric_name, metric_data in metrics.items():
            # Extract and highlight top values
            avg_value = metric_data.get('avg_value', 0)
            
            # Identify top 1
            is_top = (metric_name == self._find_top_metric(metrics))
            
            # Apply thresholds
            thresholds = {'critical': 1000, 'warning': 500}
            formatted_value = self.highlight_critical_values(
                avg_value, 
                thresholds,
                ' MB/s',
                is_top=is_top
            )
            
            structured['metrics_overview'].append({
                'Metric': metric_name,
                'Avg Value': formatted_value,
                'Category': self._categorize_metric(metric_name)
            })
        
        return structured
    
    def _find_top_metric(self, metrics: Dict) -> str:
        """Find metric with highest value"""
        return max(metrics.items(), 
                  key=lambda x: x[1].get('avg_value', 0))[0]
    
    def transform_to_dataframes(self, structured_data: Dict) -> Dict[str, pd.DataFrame]:
        dataframes = {}
        for key, value in structured_data.items():
            if isinstance(value, list) and value:
                df = pd.DataFrame(value)
                if not df.empty:
                    # Decode unicode
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            df[col] = df[col].astype(str).apply(self.decode_unicode_escapes)
                    dataframes[key] = df
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict) -> Dict[str, str]:
        html_tables = {}
        for table_name, df in dataframes.items():
            if not df.empty:
                html_tables[table_name] = self.create_html_table(df, table_name)
        return html_tables
    
    def summarize_disk_io(self, data: Dict) -> str:
        """Generate summary"""
        metrics_count = len(data.get('metrics_overview', []))
        return f"<div>Disk I/O Analysis: {metrics_count} metrics collected</div>"
```

### Step 2: Create Identifier Function

```python
def is_disk_io(data: Dict) -> bool:
    """Identify if data is disk I/O metrics"""
    if 'tool' in data and data.get('tool') == 'collect_disk_io_metrics':
        return True
    
    if 'category' in data and data.get('category') == 'disk_io':
        return True
    
    # Check for disk I/O specific fields
    if 'metrics' in data:
        metrics = data['metrics']
        disk_io_metrics = ['container_disk_writes', 'node_disk_throughput_read']
        if any(metric in metrics for metric in disk_io_metrics):
            return True
    
    return False
```

### Step 3: Register Handler

```python
# In analyzer_elt_json2table.py -> _ensure_handlers_registered()

try:
    from .metrics.analyzer_elt_disk_io import diskIOELT, is_disk_io
    register_metric_handler('disk_io', diskIOELT, is_disk_io)
except ImportError as e:
    logger.warning(f"Could not import disk_io handler: {e}")
```

### Step 4: Use It

```python
from elt.utils.analyzer_elt_json2table import convert_json_to_html_table

# Your disk I/O data
disk_io_data = {
    'tool': 'collect_disk_io_metrics',
    'metrics': {
        'container_disk_writes': {'avg_value': 850},
        'node_disk_throughput_read': {'avg_value': 1200}
    }
}

# Convert to HTML - automatically routes to diskIOELT
html_output = convert_json_to_html_table(disk_io_data)
```

---

## Highlighting Examples

### Critical Value Highlighting

```python
# In your extract method:

# Define thresholds for this metric
thresholds = {'critical': 1000, 'warning': 500}

# Check if this is the top value
is_top = (current_value == max_value_in_dataset)

# Apply highlighting
formatted_value = self.highlight_critical_values(
    value=850,              # The actual value
    thresholds=thresholds,  # Warning/critical levels
    unit=' MB/s',           # Unit to append
    is_top=is_top          # True for top 1
)

# Results:
# is_top=True  → "🏆 850 MB/s" (blue background)
# value >= 1000 → "⚠️ 850 MB/s" (red, bold)
# value >= 500  → "850 MB/s" (orange, bold)
# else          → "850 MB/s" (normal)
```

### Finding Top Values

```python
# Example: Find top CPU node

# Collect all values with indices
cpu_values = [
    (i, self.parse_cpu_capacity(node.get('cpu_capacity', '0')))
    for i, node in enumerate(nodes)
]

# Find top index
top_cpu_idx = max(cpu_values, key=lambda x: x[1])[0] if cpu_values else -1

# Apply when processing nodes
for i, node in enumerate(nodes):
    cpu_cores = self.parse_cpu_capacity(node.get('cpu_capacity', '0'))
    
    cpu_display = self.highlight_critical_values(
        cpu_cores,
        {'critical': 64, 'warning': 32},
        ' cores',
        is_top=(i == top_cpu_idx)  # Top 1 gets trophy
    )
```

---

## Best Practices

### DO ✅
- Keep utilities pure and reusable
- Use clear metric type names
- Implement all 4 required methods
- Highlight critical and top values
- Decode unicode in DataFrames
- Return structured dictionaries
- Use type hints
- Add logging for errors
- Make thresholds configurable

### DON'T ❌
- Write files in ELT modules
- Mix metric-specific logic in utilities
- Add scoring/analysis logic (AI does this)
- Hardcode table titles in handlers
- Skip unicode decoding
- Return incomplete data structures
- Ignore error handling
- Duplicate formatting code

---

## Testing New Handlers

```python
# Test script example
import json
from elt.utils.analyzer_elt_json2table import process_metric_data

# Load test data
with open('test_data.json', 'r') as f:
    test_data = json.load(f)

# Process
result = process_metric_data(test_data)

# Verify
assert result['success'] == True
assert result['data_type'] == 'disk_io'
assert 'metrics_overview' in result['html_tables']
assert len(result['dataframes']) > 0

# Check highlighting
html = result['html_tables']['metrics_overview']
assert '🏆' in html  # Top value marked
assert '⚠️' in html or 'text-danger' in html  # Critical values marked

print("✓ All tests passed")
```

---

## Summary

### Key Changes
1. **Moved** all generic utilities to `analyzer_elt_utility.py`
2. **Created** plugin architecture in `analyzer_elt_json2table.py`
3. **Isolated** cluster_info logic to its own module
4. **Enabled** easy extension for new metrics

### Benefits
1. Clean separation of concerns
2. Easy to add new metric types
3. Reusable utility functions
4. No code duplication
5. Consistent HTML output
6. Clear extension path

### Adding New Metrics
1. Create handler class (4 methods)
2. Create identifier function
3. Register handler (1 line)
4. Done! ✓

---

## Quick Reference

### Required Handler Methods
```python
extract_<metric_type>(data: Dict) -> Dict
transform_to_dataframes(structured_data: Dict) -> Dict[str, pd.DataFrame]
generate_html_tables(dataframes: Dict) -> Dict[str, str]
summarize_<metric_type>(data: Dict) -> str
```

### Registration Template
```python
register_metric_handler(
    metric_type='my_metric',
    elt_class=MyMetricELT,
    identifier_func=lambda d: 'my_field' in d
)
```

### Highlighting Template
```python
self.highlight_critical_values(
    value=actual_value,
    thresholds={'critical': 100, 'warning': 50},
    unit=' units',
    is_top=(value == max_value)
)
```