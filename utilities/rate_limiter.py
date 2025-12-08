"""
Rate Limiting Utilities
=======================

Tools for controlling request rates to avoid overwhelming target servers.
"""

import time
import asyncio
from typing import Optional
from collections import deque


class RateLimiter:
    """
    Token bucket rate limiter for controlling request rates.
    
    This implementation uses the token bucket algorithm to ensure
    that requests don't exceed a specified rate.
    """
    
    def __init__(
        self,
        max_requests: int = 10,
        time_window: float = 60.0,
        burst_size: Optional[int] = None
    ):
        """
        Initialize rate limiter.
        
        Args:
            max_requests: Maximum number of requests allowed per time window
            time_window: Time window in seconds
            burst_size: Maximum burst size (defaults to max_requests)
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.burst_size = burst_size or max_requests
        
        self.tokens = self.burst_size
        self.last_update = time.time()
        self.request_times = deque(maxlen=max_requests * 2)
        
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1):
        """
        Acquire tokens for making a request.
        
        This method will block until enough tokens are available.
        
        Args:
            tokens: Number of tokens to acquire (default: 1)
        """
        async with self._lock:
            while True:
                self._refill_tokens()
                
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    self.request_times.append(time.time())
                    return
                
                # Calculate wait time
                wait_time = self._calculate_wait_time(tokens)
                await asyncio.sleep(wait_time)
    
    def _refill_tokens(self):
        """Refill tokens based on elapsed time"""
        now = time.time()
        elapsed = now - self.last_update
        
        # Calculate tokens to add
        tokens_to_add = (elapsed / self.time_window) * self.max_requests
        self.tokens = min(self.burst_size, self.tokens + tokens_to_add)
        self.last_update = now
    
    def _calculate_wait_time(self, tokens_needed: int) -> float:
        """Calculate minimum wait time to acquire tokens"""
        tokens_deficit = tokens_needed - self.tokens
        wait_time = (tokens_deficit / self.max_requests) * self.time_window
        return max(0.1, wait_time)  # Minimum 100ms wait
    
    def get_current_rate(self) -> float:
        """
        Get current request rate (requests per second).
        
        Returns:
            Current rate based on recent requests
        """
        if len(self.request_times) < 2:
            return 0.0
        
        now = time.time()
        recent_requests = [t for t in self.request_times if now - t < self.time_window]
        
        if len(recent_requests) < 2:
            return 0.0
        
        time_span = recent_requests[-1] - recent_requests[0]
        if time_span == 0:
            return 0.0
        
        return len(recent_requests) / time_span
    
    def get_tokens_available(self) -> int:
        """Get number of tokens currently available"""
        self._refill_tokens()
        return int(self.tokens)
    
    def reset(self):
        """Reset the rate limiter"""
        self.tokens = self.burst_size
        self.last_update = time.time()
        self.request_times.clear()


class AdaptiveRateLimiter(RateLimiter):
    """
    Adaptive rate limiter that adjusts based on server responses.
    
    This rate limiter can automatically slow down if it detects
    rate limiting responses (429, 503, etc.) or speed up if
    the server is responding well.
    """
    
    def __init__(
        self,
        initial_rate: int = 10,
        time_window: float = 60.0,
        min_rate: int = 1,
        max_rate: int = 100,
        adjustment_factor: float = 0.5
    ):
        """
        Initialize adaptive rate limiter.
        
        Args:
            initial_rate: Initial requests per time window
            time_window: Time window in seconds
            min_rate: Minimum requests per time window
            max_rate: Maximum requests per time window
            adjustment_factor: Factor for rate adjustments (0-1)
        """
        super().__init__(initial_rate, time_window)
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.adjustment_factor = adjustment_factor
        self.consecutive_successes = 0
        self.consecutive_failures = 0
    
    def record_success(self):
        """Record a successful request"""
        self.consecutive_successes += 1
        self.consecutive_failures = 0
        
        # Increase rate after sustained success
        if self.consecutive_successes >= 10:
            self._increase_rate()
            self.consecutive_successes = 0
    
    def record_failure(self, status_code: Optional[int] = None):
        """
        Record a failed request.
        
        Args:
            status_code: HTTP status code if applicable
        """
        self.consecutive_failures += 1
        self.consecutive_successes = 0
        
        # Immediate rate reduction for rate limit errors
        if status_code in (429, 503):
            self._decrease_rate(aggressive=True)
        elif self.consecutive_failures >= 3:
            self._decrease_rate(aggressive=False)
            self.consecutive_failures = 0
    
    def _increase_rate(self):
        """Gradually increase the rate limit"""
        new_rate = min(
            self.max_rate,
            int(self.max_requests * (1 + self.adjustment_factor))
        )
        if new_rate != self.max_requests:
            self.max_requests = new_rate
            self.tokens = min(self.tokens, new_rate)
    
    def _decrease_rate(self, aggressive: bool = False):
        """Decrease the rate limit"""
        factor = 0.5 if aggressive else (1 - self.adjustment_factor)
        new_rate = max(
            self.min_rate,
            int(self.max_requests * factor)
        )
        if new_rate != self.max_requests:
            self.max_requests = new_rate
            self.tokens = min(self.tokens, new_rate)
