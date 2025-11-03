#!/usr/bin/env python3
"""
Debug script to test metrics loading from metrics-node.yml
"""

import sys
from pathlib import Path
import yaml
import logging

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from config.metrics_config_reader import Config


def test_direct_yaml_load():
    """Test loading YAML file directly"""
    print("\n" + "="*80)
    print("TEST 1: Direct YAML Loading")
    print("="*80)
    
    metrics_file = Path('config/metrics-node.yml')
    
    if not metrics_file.exists():
        print(f"❌ File not found: {metrics_file}")
        return None
    
    print(f"✓ File found: {metrics_file}")
    
    with open(metrics_file, 'r') as f:
        data = yaml.safe_load(f)
    
    print(f"✓ YAML loaded successfully")
    print(f"  Root keys: {list(data.keys())}")
    
    if 'metrics' in data:
        metrics_list = data['metrics']
        print(f"  Metrics count: {len(metrics_list)}")
        print(f"\n  First 3 metrics:")
        for i, metric in enumerate(metrics_list[:3]):
            print(f"    {i+1}. name: {metric.get('name')}, category: {metric.get('category')}")
    
    return data


def test_config_loader():
    """Test loading via Config class"""
    print("\n" + "="*80)
    print("TEST 2: Config Class Loading")
    print("="*80)
    
    config = Config()
    
    metrics_file = Path('config/metrics-node.yml')
    
    if not metrics_file.exists():
        print(f"❌ File not found: {metrics_file}")
        return None
    
    print(f"✓ Config instance created")
    print(f"  Loading from: {metrics_file}")
    
    result = config.load_metrics_file(str(metrics_file))
    
    print(f"\n  Load result:")
    print(f"    success: {result.get('success')}")
    print(f"    metrics_loaded: {result.get('metrics_loaded')}")
    print(f"    categories_loaded: {result.get('categories_loaded')}")
    
    if not result.get('success'):
        print(f"    ❌ error: {result.get('error')}")
        return None
    
    print(f"\n  Config state after loading:")
    print(f"    Total metrics: {config.get_metrics_count()}")
    print(f"    Categories: {config.get_all_categories()}")
    
    # Test individual metric retrieval
    print(f"\n  Testing metric retrieval:")
    test_metrics = ['node_cpu_usage', 'node_memory_used', 'node_memory_cache_buffer', 
                    'cgroup_cpu_usage', 'cgroup_rss_usage']
    
    for metric_name in test_metrics:
        metric = config.get_metric_by_name(metric_name)
        if metric:
            print(f"    ✓ {metric_name}: found in category '{metric.get('category')}'")
        else:
            print(f"    ❌ {metric_name}: NOT FOUND")
    
    # Show all metrics by category
    print(f"\n  All metrics by category:")
    all_metrics = config.get_all_metrics()
    for category, metrics_list in all_metrics.items():
        print(f"    {category}:")
        for metric in metrics_list:
            print(f"      - {metric.get('name')}")
    
    return config


def test_metrics_structure():
    """Test the structure of loaded metrics"""
    print("\n" + "="*80)
    print("TEST 3: Metrics Structure Validation")
    print("="*80)
    
    config = Config()
    metrics_file = Path('config/metrics-node.yml')
    
    result = config.load_metrics_file(str(metrics_file))
    
    if not result.get('success'):
        print(f"❌ Failed to load: {result.get('error')}")
        return
    
    required_fields = ['name', 'title', 'expr', 'unit', 'description', 'category']
    
    all_metrics = config.get_all_metrics()
    
    print(f"Checking {config.get_metrics_count()} metrics...")
    
    issues = []
    for category, metrics_list in all_metrics.items():
        for metric in metrics_list:
            metric_name = metric.get('name', 'UNNAMED')
            missing_fields = [field for field in required_fields if field not in metric or not metric[field]]
            
            if missing_fields:
                issues.append({
                    'metric': metric_name,
                    'category': category,
                    'missing': missing_fields
                })
    
    if issues:
        print(f"\n❌ Found {len(issues)} metrics with issues:")
        for issue in issues:
            print(f"  - {issue['metric']} (category: {issue['category']})")
            print(f"    Missing fields: {issue['missing']}")
    else:
        print(f"✓ All metrics have required fields")


def main():
    """Run all tests"""
    print("\n" + "="*80)
    print("METRICS LOADING DIAGNOSTIC TESTS")
    print("="*80)
    
    # Test 1: Direct YAML load
    yaml_data = test_direct_yaml_load()
    
    # Test 2: Config class loading
    config = test_config_loader()
    
    # Test 3: Structure validation
    if config:
        test_metrics_structure()
    
    print("\n" + "="*80)
    print("TESTS COMPLETE")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()