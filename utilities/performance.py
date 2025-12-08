"""
Performance Monitoring Utilities
================================

Tools for monitoring and tracking scraper performance metrics.
"""

import time
import psutil
import os
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict
from datetime import datetime
from contextlib import contextmanager


@dataclass
class PerformanceMetrics:
    """Container for performance tracking data"""
    
    start_time: float = field(default_factory=time.time)
    pages_processed: int = 0
    documents_processed: int = 0
    pdfs_generated: int = 0
    html_fallbacks: int = 0
    errors: int = 0
    network_requests: int = 0
    total_bytes_downloaded: int = 0
    avg_page_load_time: float = 0.0
    avg_document_process_time: float = 0.0
    
    # Resource usage
    peak_memory_mb: float = 0.0
    avg_cpu_percent: float = 0.0
    
    def get_duration(self) -> float:
        """Get total runtime in seconds"""
        return time.time() - self.start_time
    
    def get_docs_per_minute(self) -> float:
        """Calculate documents processed per minute"""
        duration_minutes = self.get_duration() / 60
        return self.documents_processed / duration_minutes if duration_minutes > 0 else 0
    
    def get_success_rate(self) -> float:
        """Calculate success rate as percentage"""
        if self.documents_processed == 0:
            return 100.0
        return round((self.documents_processed - self.errors) / self.documents_processed * 100, 2)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary"""
        data = asdict(self)
        data['duration_seconds'] = round(self.get_duration(), 2)
        data['docs_per_minute'] = round(self.get_docs_per_minute(), 2)
        data['success_rate'] = self.get_success_rate()
        return data


class MetricsCollector:
    """
    Collects and aggregates performance metrics over time.
    """
    
    def __init__(self):
        self.metrics = PerformanceMetrics()
        self.operation_times: Dict[str, List[float]] = {}
        self.resource_samples: List[Dict[str, float]] = []
        self._process = psutil.Process(os.getpid())
    
    def record_page_processed(self):
        """Record that a page was processed"""
        self.metrics.pages_processed += 1
    
    def record_document_processed(self, method: str = "html", file_size: int = 0):
        """Record that a document was processed"""
        self.metrics.documents_processed += 1
        
        if method == "pdf":
            self.metrics.pdfs_generated += 1
        else:
            self.metrics.html_fallbacks += 1
        
        if file_size:
            self.metrics.total_bytes_downloaded += file_size
    
    def record_error(self):
        """Record an error occurred"""
        self.metrics.errors += 1
    
    def record_network_request(self, response_size: int = 0):
        """Record a network request"""
        self.metrics.network_requests += 1
        if response_size:
            self.metrics.total_bytes_downloaded += response_size
    
    def record_operation_time(self, operation: str, duration: float):
        """Record the duration of an operation"""
        if operation not in self.operation_times:
            self.operation_times[operation] = []
        self.operation_times[operation].append(duration)
        
        # Update averages
        if operation == "page_load":
            times = self.operation_times[operation]
            self.metrics.avg_page_load_time = sum(times) / len(times)
        elif operation == "document_process":
            times = self.operation_times[operation]
            self.metrics.avg_document_process_time = sum(times) / len(times)
    
    def sample_resource_usage(self):
        """Sample current resource usage"""
        try:
            memory_mb = self._process.memory_info().rss / 1024 / 1024
            cpu_percent = self._process.cpu_percent(interval=0.1)
            
            self.resource_samples.append({
                'timestamp': time.time(),
                'memory_mb': memory_mb,
                'cpu_percent': cpu_percent,
            })
            
            # Update peak memory
            if memory_mb > self.metrics.peak_memory_mb:
                self.metrics.peak_memory_mb = memory_mb
            
            # Update average CPU
            if self.resource_samples:
                avg_cpu = sum(s['cpu_percent'] for s in self.resource_samples) / len(self.resource_samples)
                self.metrics.avg_cpu_percent = round(avg_cpu, 2)
            
        except Exception:
            pass  # Ignore errors in resource monitoring
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics as dictionary"""
        return self.metrics.to_dict()
    
    def get_detailed_stats(self) -> Dict[str, Any]:
        """Get detailed statistics including operation breakdowns"""
        stats = self.get_metrics()
        
        # Add operation time statistics
        stats['operation_stats'] = {}
        for operation, times in self.operation_times.items():
            if times:
                stats['operation_stats'][operation] = {
                    'count': len(times),
                    'avg': round(sum(times) / len(times), 3),
                    'min': round(min(times), 3),
                    'max': round(max(times), 3),
                }
        
        # Add resource usage history
        if self.resource_samples:
            stats['resource_history'] = {
                'samples': len(self.resource_samples),
                'memory_peak_mb': round(self.metrics.peak_memory_mb, 2),
                'cpu_avg_percent': self.metrics.avg_cpu_percent,
            }
        
        return stats


class PerformanceMonitor:
    """
    High-level performance monitoring interface with context managers.
    """
    
    def __init__(self, collector: Optional[MetricsCollector] = None):
        self.collector = collector or MetricsCollector()
    
    @contextmanager
    def measure_operation(self, operation_name: str):
        """
        Context manager to measure operation duration.
        
        Usage:
            with monitor.measure_operation("page_load"):
                # ... operation code ...
        """
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.collector.record_operation_time(operation_name, duration)
    
    def record_page(self):
        """Record page processed"""
        self.collector.record_page_processed()
        self.collector.sample_resource_usage()
    
    def record_document(self, method: str = "html", file_size: int = 0):
        """Record document processed"""
        self.collector.record_document_processed(method, file_size)
        self.collector.sample_resource_usage()
    
    def record_error(self):
        """Record error"""
        self.collector.record_error()
    
    def get_summary(self) -> Dict[str, Any]:
        """Get performance summary"""
        return self.collector.get_metrics()
    
    def get_detailed_report(self) -> Dict[str, Any]:
        """Get detailed performance report"""
        return self.collector.get_detailed_stats()
    
    def print_summary(self):
        """Print a human-readable performance summary"""
        metrics = self.get_summary()
        
        print("\n" + "=" * 60)
        print("PERFORMANCE SUMMARY")
        print("=" * 60)
        print(f"Duration:              {metrics['duration_seconds']:.2f}s")
        print(f"Pages processed:       {metrics['pages_processed']}")
        print(f"Documents processed:   {metrics['documents_processed']}")
        print(f"PDFs generated:        {metrics['pdfs_generated']}")
        print(f"HTML fallbacks:        {metrics['html_fallbacks']}")
        print(f"Errors:                {metrics['errors']}")
        print(f"Success rate:          {metrics['success_rate']:.2f}%")
        print(f"Docs/minute:           {metrics['docs_per_minute']:.2f}")
        print(f"Network requests:      {metrics['network_requests']}")
        print(f"Data downloaded:       {metrics['total_bytes_downloaded'] / 1024 / 1024:.2f} MB")
        print(f"Avg page load:         {metrics['avg_page_load_time']:.3f}s")
        print(f"Avg doc process:       {metrics['avg_document_process_time']:.3f}s")
        print(f"Peak memory:           {metrics['peak_memory_mb']:.2f} MB")
        print(f"Avg CPU:               {metrics['avg_cpu_percent']:.2f}%")
        print("=" * 60 + "\n")
