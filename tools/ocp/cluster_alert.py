"""
OVNK Kubelet CNI Alert Collector Module
Collects alert metrics for OVNK monitoring from metrics-alert.yml
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pytz
import os
import sys

# Ensure project root on sys.path for utils imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ocauth.openshift_auth import OpenShiftAuth as OCPAuth
from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility
from config.metrics_config_reader import Config


class ocpAlertCollector:
    """Collector for OCP alert metrics"""
    
    def __init__(self, ocp_auth: OCPAuth, duration: str = "1h", 
                 start_time: Optional[str] = None, end_time: Optional[str] = None,
                 metrics_file_path: str = None):
        """
        Initialize ocpAlertCollector
        
        Args:
            ocp_auth: OpenShift authentication object
            duration: Duration string (e.g., '1h', '5m') - used if start_time/end_time not provided
            start_time: Optional start time in ISO format (UTC)
            end_time: Optional end time in ISO format (UTC)
            metrics_file_path: Optional path to metrics configuration file
        """
        self.ocp_auth = ocp_auth
        self.duration = duration
        self.start_time = start_time
        self.end_time = end_time
        self.logger = logging.getLogger(__name__)
        self.utility = mcpToolsUtility(ocp_auth)
        
        # Load metrics configuration
        if metrics_file_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metrics_file_path = os.path.join(project_root, 'config', 'metrics-alert.yml')
        
        self.config = Config()
        load_result = self.config.load_metrics_file(metrics_file_path)
        
        # Get category-specific metrics
        self.category = "alerts"
        self.alert_metrics = self.config.get_metrics_by_category(self.category)
        
        # Log metrics loading
        if load_result.get('success'):
            metrics_count = len(self.alert_metrics)
            self.logger.info(f"✅ Loaded {metrics_count} metrics from {self.category} category (ocpAlertCollector)")
        else:
            self.logger.warning(f"⚠️ No {self.category} metrics found in configuration")
    
    def _get_time_params(self, prom: PrometheusBaseQuery) -> tuple:
        """
        Get time parameters for queries based on initialization parameters
        
        Returns:
            Tuple of (start_time, end_time) as ISO strings
        """
        if self.start_time and self.end_time:
            # Use provided time range
            return (self.start_time, self.end_time)
        else:
            # Use duration from now
            return prom.get_time_range_from_duration(self.duration, self.end_time)
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all alert metrics and return comprehensive results"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                # Test connection first
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': "Prometheus connection failed",
                        'timestamp': datetime.now(pytz.UTC).isoformat()
                    }
                
                # Get time parameters
                start, end = self._get_time_params(prom)
                
                # Collect results for all metrics
                results = {
                    'status': 'success',
                    'timestamp': datetime.now(pytz.UTC).isoformat(),
                    'query_time_range': {
                        'start': start,
                        'end': end,
                        'duration': self.duration if not (self.start_time and self.end_time) else 'custom'
                    },
                    'category': 'alerts',
                    'metrics': {}
                }
                
                # Process each metric dynamically from config
                for metric_config in self.alert_metrics:
                    metric_name = metric_config['name']
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        # Route to appropriate collection method based on metric name
                        if metric_name == 'top_alerts':
                            metric_result = await self.collect_top_alerts(prom, start, end)
                        else:
                            # Generic collector for other metrics
                            metric_result = await self._collect_generic_metric(prom, metric_config, start, end)
                        
                        results['metrics'][metric_name] = metric_result
                        
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        results['metrics'][metric_name] = {
                            'status': 'error',
                            'error': str(e)
                        }
                
                # Add summary
                successful_metrics = sum(1 for m in results['metrics'].values() if m.get('status') == 'success')
                total_metrics = len(results['metrics'])
                
                results['summary'] = {
                    'total_metrics': total_metrics,
                    'successful_metrics': successful_metrics,
                    'failed_metrics': total_metrics - successful_metrics
                }
                
                return results
                
        except Exception as e:
            self.logger.error(f"Error in collect_all_metrics: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(pytz.UTC).isoformat()
            }
    
    async def collect_top_alerts(self, prom: PrometheusBaseQuery = None, 
                                 start: str = None, end: str = None) -> Dict[str, Any]:
        """
        Collect top 15 alerts by severity with separated avg and max values per alertname
        
        Args:
            prom: Optional PrometheusBaseQuery instance (created if not provided)
            start: Optional start time (calculated if not provided)
            end: Optional end time (calculated if not provided)
        
        Returns:
            Dictionary with top alerts information including per-alertname avg/max statistics
        """
        self.logger.info("Collecting top alerts with per-alertname avg and max")
        
        # Get metric config
        metric_config = self.config.get_metric_by_name('top_alerts')
        if not metric_config:
            return {
                'status': 'error',
                'error': 'top_alerts metric not found in configuration'
            }
        
        alerts_query = metric_config['expr']
        
        try:
            # Handle prometheus client creation if not provided
            should_close = False
            if prom is None:
                prometheus_config = self.ocp_auth.get_prometheus_config()
                prom = PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token'))
                await prom._ensure_session()
                should_close = True
            
            # Calculate time range if not provided
            if start is None or end is None:
                start, end = self._get_time_params(prom)
            
            try:
                # Use range query to get time series data
                raw_result = await prom.query_range(alerts_query, start, end, step='15s')
                
                # Extract and process all time series
                alertname_groups = {}
                all_alert_entries = []
                
                if 'result' in raw_result:
                    for item in raw_result['result']:
                        labels = item.get('metric', {})
                        alertname = labels.get('alertname', 'unknown')
                        severity = labels.get('severity', 'unknown')
                        
                        # Extract numeric values from time series
                        values = []
                        for ts, val in item.get('values', []):
                            try:
                                v = float(val)
                                if v != float('inf') and v != float('-inf'):
                                    values.append(v)
                            except (ValueError, TypeError):
                                continue
                        
                        if not values:
                            continue
                        
                        # Calculate statistics for this series
                        avg_count = sum(values) / len(values)
                        max_count = max(values)
                        
                        # Store in alertname groups for statistics
                        if alertname not in alertname_groups:
                            alertname_groups[alertname] = {
                                'values': [],
                                'severity': severity,
                                'avg': avg_count,
                                'max': max_count
                            }
                        else:
                            # Update with max of maxes and avg of avgs
                            alertname_groups[alertname]['values'].extend(values)
                            alertname_groups[alertname]['max'] = max(alertname_groups[alertname]['max'], max_count)
                        
                        # Add to all entries for sorting
                        all_alert_entries.append({
                            'alert_name': alertname,
                            'severity': severity,
                            'count': max_count,  # Use max for sorting
                            'timestamp': datetime.now(pytz.UTC).isoformat()
                        })
                
                # Sort alerts by count descending
                sorted_alerts = sorted(all_alert_entries, key=lambda x: x['count'], reverse=True)[:15]
                
                # Calculate final statistics per alertname
                alertname_statistics = {}
                for alertname, data in alertname_groups.items():
                    if data['values']:
                        alertname_statistics[alertname] = {
                            "avg_count": round(sum(data['values']) / len(data['values']), 2),
                            "max_count": round(data['max'], 2)
                        }
                
                return {
                    "status": "success",
                    "metric_name": "top_alerts",
                    "query": alerts_query,
                    "time_range": {
                        "start": start,
                        "end": end
                    },
                    "alerts": sorted_alerts,
                    "total_alert_types": len(sorted_alerts),
                    "alertname_statistics": alertname_statistics
                }
                
            finally:
                if should_close:
                    await prom.close()
            
        except Exception as e:
            self.logger.error(f"Error collecting alerts: {e}")
            return {
                "status": "error",
                "metric_name": "top_alerts",
                "error": str(e),
                "alerts": [],
                "total_alert_types": 0,
                "alertname_statistics": {}
            }
    
    async def _collect_generic_metric(self, prom: PrometheusBaseQuery, 
                                      metric_config: Dict[str, Any],
                                      start: str, end: str) -> Dict[str, Any]:
        """Generic method to collect any alert metric"""
        query = metric_config['expr']
        metric_name = metric_config['name']
        
        try:
            result = await self._query_with_stats(prom, query, start, end)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error')}
            
            # Process results
            metric_data = {}
            for series in result.get('series_data', []):
                labels = series['labels']
                stats = series['statistics']
                
                # Use alertname as key if available
                key = labels.get('alertname') or labels.get('__name__') or 'unknown'
                
                metric_data[key] = {
                    'avg': round(stats.get('avg', 0), 2),
                    'max': round(stats.get('max', 0), 2),
                    'labels': labels
                }
            
            return {
                'status': 'success',
                'metric': metric_name,
                'title': metric_config.get('title', metric_name.replace('_', ' ').title()),
                'unit': metric_config.get('unit', 'unknown'),
                'description': metric_config.get('description', f'Alert metric: {metric_name}'),
                'data': metric_data,
                'overall_stats': result.get('overall_statistics', {})
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting metric {metric_name}: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def _query_with_stats(self, prom_client: PrometheusBaseQuery, 
                                query: str, start: str, end: str) -> Dict[str, Any]:
        """Execute a range query and compute basic statistics per series and overall."""
        try:
            data = await prom_client.query_range(query, start, end, step='15s')

            series_data: List[Dict[str, Any]] = []
            all_values: List[float] = []

            for item in data.get('result', []) if isinstance(data, dict) else []:
                metric_labels = item.get('metric', {})
                values = []
                for ts, val in item.get('values', []) or []:
                    try:
                        v = float(val)
                    except (ValueError, TypeError):
                        continue
                    if v != float('inf') and v != float('-inf'):
                        values.append(v)

                stats: Dict[str, Any] = {}
                if values:
                    avg_v = sum(values) / len(values)
                    max_v = max(values)
                    min_v = min(values)
                    stats = {
                        'avg': avg_v,
                        'max': max_v,
                        'min': min_v,
                        'count': len(values),
                        'latest': values[-1]
                    }
                    all_values.extend(values)

                series_data.append({'labels': metric_labels, 'statistics': stats})

            overall_statistics: Dict[str, Any] = {}
            if all_values:
                overall_statistics = {
                    'avg': sum(all_values) / len(all_values),
                    'max': max(all_values),
                    'min': min(all_values),
                    'count': len(all_values)
                }

            return {
                'status': 'success',
                'series_data': series_data,
                'overall_statistics': overall_statistics
            }
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    async def get_metric_by_name(self, metric_name: str) -> Dict[str, Any]:
        """Get a specific metric by name"""
        try:
            metric_config = self.config.get_metric_by_name(metric_name)
            if not metric_config:
                return {
                    'status': 'error',
                    'error': f'Metric {metric_name} not found in configuration'
                }
            
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config.get('url'), prometheus_config.get('token')) as prom:
                # Test connection first
                connection_ok = await prom.test_connection()
                if not connection_ok:
                    return {
                        'status': 'error',
                        'error': "Prometheus connection failed"
                    }
                
                # Get time parameters
                start, end = self._get_time_params(prom)
                
                # Route to appropriate method
                if metric_name == 'top_alerts':
                    return await self.collect_top_alerts(prom, start, end)
                else:
                    return await self._collect_generic_metric(prom, metric_config, start, end)
                
        except Exception as e:
            self.logger.error(f"Error getting metric {metric_name}: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }


# Convenience functions for individual metric collection
async def collect_alert_metrics(ocp_auth: OCPAuth, duration: str = "1h",
                                start_time: Optional[str] = None,
                                end_time: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to collect all alert metrics"""
    collector = ocpAlertCollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_all_metrics()


async def get_top_alerts(ocp_auth: OCPAuth, duration: str = "1h",
                        start_time: Optional[str] = None,
                        end_time: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to get top alerts"""
    collector = ocpAlertCollector(ocp_auth, duration, start_time, end_time)
    return await collector.collect_top_alerts()


async def get_specific_alert_metric(ocp_auth: OCPAuth, metric_name: str, 
                                    duration: str = "1h",
                                    start_time: Optional[str] = None,
                                    end_time: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to get a specific alert metric"""
    collector = ocpAlertCollector(ocp_auth, duration, start_time, end_time)
    return await collector.get_metric_by_name(metric_name)