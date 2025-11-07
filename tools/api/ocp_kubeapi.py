"""
Kubernetes API Server Metrics Module
Queries and processes Kubernetes API server performance metrics
"""

import asyncio
from datetime import datetime, timezone
import math
from typing import Dict, List, Any, Optional
from .ovnk_benchmark_prometheus_basequery import PrometheusBaseQuery


class kubeAPICollector:
    """Handles Kubernetes API server metrics queries"""
    
    def __init__(self, prometheus_client: PrometheusBaseQuery, metrics_config: Dict[str, Any]):
        self.prometheus_client = prometheus_client
        self.metrics_config = metrics_config
        self.api_server_metrics = metrics_config.get('api_server', [])
    
    async def get_all_metrics(self, duration: str = "5m", start_time: Optional[str] = None, 
                              end_time: Optional[str] = None) -> Dict[str, Any]:
        """
        Get all API server metrics from metrics.yml configuration
        
        Returns:
            JSON result with avg, max and essential data for all api_server metrics
        """
        result = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'duration': duration,
            'metrics': {},
            'errors': []
        }
        
        try:
            # Create tasks for all metrics
            tasks = []
            metric_names = []
            
            for metric in self.api_server_metrics:
                metric_name = metric.get('name')
                if metric_name:
                    task = self._query_metric(metric, duration, start_time, end_time)
                    tasks.append(task)
                    metric_names.append(metric_name)
            
            # Execute all queries concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for i, metric_result in enumerate(results):
                metric_name = metric_names[i]
                if isinstance(metric_result, Exception):
                    result['errors'].append({
                        'metric': metric_name,
                        'error': str(metric_result)
                    })
                else:
                    result['metrics'][metric_name] = metric_result
            
        except Exception as e:
            result['errors'].append({
                'error': f"Failed to get API server metrics: {str(e)}"
            })
        
        return result
    
    async def _query_metric(self, metric: Dict[str, Any], duration: str,
                           start_time: Optional[str], end_time: Optional[str]) -> Dict[str, Any]:
        """Query a single metric and calculate statistics"""
        metric_name = metric.get('name')
        query_expr = metric.get('expr', '')
        unit = metric.get('unit', 'count')
        
        # Replace duration placeholder
        query_expr = query_expr.replace('[5m:]', f'[{duration}:]')
        
        try:
            # Execute query
            if start_time and end_time:
                data = await self.prometheus_client.query_range(
                    query_expr, start_time, end_time, step='15s'
                )
            else:
                data = await self.prometheus_client.query_instant(query_expr)
            
            # Process and calculate statistics
            stats = self._calculate_statistics(data)
            
            return {
                'name': metric_name,
                'unit': unit,
                'query': query_expr,
                'avg': stats['avg'],
                'max': stats['max'],
                'count': stats['count'],
                'top_5': stats['top_5']
            }
            
        except Exception as e:
            return {
                'name': metric_name,
                'error': str(e)
            }
    
    def _calculate_statistics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate avg, max and top 5 from query result"""
        stats = {
            'avg': 0.0,
            'max': 0.0,
            'count': 0,
            'top_5': []
        }
        
        if 'result' not in data:
            return stats
        
        all_values = []
        series_stats = []
        
        for result_item in data['result']:
            labels = result_item.get('metric', {})
            values = []
            
            # Extract values
            if 'value' in result_item:
                _, value = result_item['value']
                num_val = self._to_number(value)
                if num_val is not None:
                    values.append(num_val)
            elif 'values' in result_item:
                for _, value in result_item['values']:
                    num_val = self._to_number(value)
                    if num_val is not None:
                        values.append(num_val)
            
            if values:
                all_values.extend(values)
                series_avg = sum(values) / len(values)
                series_max = max(values)
                
                # Create label string
                label_str = self._format_labels(labels)
                
                series_stats.append({
                    'label': label_str,
                    'avg': round(series_avg, 6),
                    'max': round(series_max, 6)
                })
        
        # Calculate overall statistics
        if all_values:
            finite_values = [v for v in all_values if self._is_finite(v)]
            if finite_values:
                stats['avg'] = round(sum(finite_values) / len(finite_values), 6)
                stats['max'] = round(max(finite_values), 6)
                stats['count'] = len(finite_values)
        
        # Get top 5 by max value
        series_stats.sort(key=lambda x: x['max'], reverse=True)
        stats['top_5'] = series_stats[:5]
        
        return stats
    
    def _format_labels(self, labels: Dict[str, str]) -> str:
        """Format labels into a readable string"""
        parts = []
        priority_keys = ['resource', 'verb', 'scope', 'instance', 'operation', 
                        'type', 'service', 'group', 'kind']
        
        for key in priority_keys:
            if key in labels:
                parts.append(labels[key])
        
        # Add any remaining labels
        for key, value in labels.items():
            if key not in priority_keys and key != '__name__':
                parts.append(f"{key}={value}")
        
        return ':'.join(parts) if parts else 'unknown'
    
    def _to_number(self, value: Any) -> Optional[float]:
        """Convert value to finite float"""
        try:
            num = float(value)
            if self._is_finite(num):
                return num
            return None
        except (ValueError, TypeError):
            return None
    
    def _is_finite(self, num: float) -> bool:
        """Check if number is finite"""
        return not math.isnan(num) and not math.isinf(num)
    
    # Individual metric functions
    async def get_avg_ro_apicalls_latency(self, duration: str = "5m", 
                                          start_time: Optional[str] = None,
                                          end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get average read-only API calls latency"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'avg_ro_apicalls_latency'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)
    
    async def get_max_ro_apicalls_latency(self, duration: str = "5m",
                                          start_time: Optional[str] = None,
                                          end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get maximum read-only API calls latency"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'max_ro_apicalls_latency'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)
    
    async def get_avg_mutating_apicalls_latency(self, duration: str = "5m",
                                                start_time: Optional[str] = None,
                                                end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get average mutating API calls latency"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'avg_mutating_apicalls_latency'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)
    
    async def get_max_mutating_apicalls_latency(self, duration: str = "5m",
                                                start_time: Optional[str] = None,
                                                end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get maximum mutating API calls latency"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'max_mutating_apicalls_latency'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)
    
    async def get_api_request_rate(self, duration: str = "5m",
                                   start_time: Optional[str] = None,
                                   end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get API server request rate"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'api_request_rate'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)
    
    async def get_api_request_errors(self, duration: str = "5m",
                                    start_time: Optional[str] = None,
                                    end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get API server request errors"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'api_request_errors'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)
    
    async def get_api_server_current_inflight_requests(self, duration: str = "5m",
                                                       start_time: Optional[str] = None,
                                                       end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get current inflight requests"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'api_server_current_inflight_requests'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)
    
    async def get_etcd_request_duration(self, duration: str = "5m",
                                       start_time: Optional[str] = None,
                                       end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get etcd request duration"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'etcd_request_duration'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)
    
    async def get_request_latency_p99_by_verb(self, duration: str = "5m",
                                              start_time: Optional[str] = None,
                                              end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get 99th percentile request latency by verb"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'request_latency_p99_by_verb'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)
    
    async def get_request_duration_p99_by_resource(self, duration: str = "5m",
                                                   start_time: Optional[str] = None,
                                                   end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get 99th percentile request duration by resource"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'request_duration_p99_by_resource'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)
    
    async def get_pf_request_wait_duration_p99(self, duration: str = "5m",
                                               start_time: Optional[str] = None,
                                               end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get Priority and Fairness request wait duration"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'pf_request_wait_duration_p99'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)
    
    async def get_pf_request_execution_duration_p99(self, duration: str = "5m",
                                                    start_time: Optional[str] = None,
                                                    end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get Priority and Fairness request execution duration"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'pf_request_execution_duration_p99'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)
    
    async def get_pf_request_dispatch_rate(self, duration: str = "5m",
                                          start_time: Optional[str] = None,
                                          end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get Priority and Fairness request dispatch rate"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'pf_request_dispatch_rate'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)
    
    async def get_pf_request_in_queue(self, duration: str = "5m",
                                     start_time: Optional[str] = None,
                                     end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get Priority and Fairness requests in queue"""
        metric = next((m for m in self.api_server_metrics 
                      if m.get('name') == 'pf_request_in_queue'), None)
        if not metric:
            return {'error': 'Metric not found in configuration'}
        
        return await self._query_metric(metric, duration, start_time, end_time)


async def create_collector(prometheus_url: str, token: Optional[str] = None,
                           metrics_config: Optional[Dict[str, Any]] = None) -> kubeAPICollector:
    """
    Factory function to create kubeAPICollector with authentication
    
    Args:
        prometheus_url: Prometheus server URL
        token: Optional authentication token
        metrics_config: Metrics configuration dictionary
        
    Returns:
        Configured kubeAPICollector instance
    """
    # Create Prometheus client
    prometheus_client = PrometheusBaseQuery(prometheus_url, token)
    
    # Load metrics config if not provided
    if metrics_config is None:
        from .metrics_config_reader import Config
        config = Config()
        metrics_config = config.get_all_metrics()
    
    # Create and return collector
    return kubeAPICollector(prometheus_client, metrics_config)