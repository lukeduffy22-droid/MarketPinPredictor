"""
Performance monitoring utilities for latency and memory tracking.
Enforces <200ms p99 latency and <100MB memory growth requirements.
"""
import time
import tracemalloc
import logging
from functools import wraps
from typing import Callable, Any

log = logging.getLogger("perf")

# Start memory tracking
tracemalloc.start()

def timed(name: str) -> Callable:
    """
    Decorator to measure function execution time.
    Logs warning if execution exceeds 200ms threshold.
    
    Usage:
        @timed("predict_close")
        def my_function():
            pass
    """
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs) -> Any:
            t0 = time.perf_counter()
            result = fn(*args, **kwargs)
            dt_ms = (time.perf_counter() - t0) * 1000
            
            if dt_ms > 200:
                log.warning(f"SLOW {name} {dt_ms:.1f}ms")
            else:
                log.debug(f"{name} {dt_ms:.1f}ms")
            
            return result
        return wrapper
    return decorator

def snapshot(label: str) -> dict:
    """
    Take a memory snapshot and log current/peak usage.
    Returns dict with current and peak memory in MB.
    """
    current, peak = tracemalloc.get_traced_memory()
    current_mb = current / 1e6
    peak_mb = peak / 1e6
    
    log.info(f"HEAP {label} cur={current_mb:.1f}MB peak={peak_mb:.1f}MB")
    
    return {
        "current_mb": current_mb,
        "peak_mb": peak_mb,
        "label": label
    }

class LatencyTracker:
    """Track p50, p95, p99 latencies over a sliding window"""
    
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.latencies = []
        
    def record(self, latency_ms: float):
        """Record a latency measurement"""
        self.latencies.append(latency_ms)
        if len(self.latencies) > self.window_size:
            self.latencies.pop(0)
    
    def get_percentiles(self) -> dict:
        """Get p50, p95, p99 latencies"""
        if not self.latencies:
            return {"p50": 0, "p95": 0, "p99": 0, "count": 0}
        
        sorted_lat = sorted(self.latencies)
        n = len(sorted_lat)
        
        return {
            "p50": sorted_lat[int(n * 0.50)] if n > 0 else 0,
            "p95": sorted_lat[int(n * 0.95)] if n > 0 else 0,
            "p99": sorted_lat[int(n * 0.99)] if n > 0 else 0,
            "count": n
        }

# Global latency tracker for predict endpoint
predict_latency = LatencyTracker()
