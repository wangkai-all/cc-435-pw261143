"""令牌桶限速器：限制采集端对站点的请求速率。"""
from __future__ import annotations

import threading
import time


class RateLimiter:
    """阻塞式令牌桶。time_func/sleep_func 可注入，便于测试。"""

    def __init__(self, rate_per_sec: float, burst: float = 1.0,
                 time_func=time.monotonic, sleep_func=time.sleep):
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec 必须为正数")
        self.rate = float(rate_per_sec)
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.time_func = time_func
        self.sleep_func = sleep_func
        self._lock = threading.Lock()
        self._last = self.time_func()

    def acquire(self) -> float:
        """获取一个令牌，返回实际等待的秒数。"""
        with self._lock:
            now = self.time_func()
            self.tokens = min(self.capacity, self.tokens + (now - self._last) * self.rate)
            self._last = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return 0.0
            wait = (1.0 - self.tokens) / self.rate
            self.tokens = 0.0
        self.sleep_func(wait)
        with self._lock:
            self._last = self.time_func()
        return wait
