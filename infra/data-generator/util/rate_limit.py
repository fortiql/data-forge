"""Token Bucket – SRP. OCP: pluggable traffic curve.

a small tool that does one thing well.
"""
from __future__ import annotations

import math
from time import monotonic
from typing import Callable


def diurnal_multiplier() -> float:
    """Return 0.4..1.4 multiplier based on local hour. (Pure function)"""
    from datetime import datetime

    hour = datetime.now().hour
    return 0.9 + 0.5 * math.sin((hour - 3) / 24 * 2 * math.pi)


class TokenBucket:
    """ISP: minimal surface – add tokens by time, try consume.

    - rate: events/sec
    - curve: function multiplier for dynamic load (default: diurnal)
    """

    def __init__(self, rate: float, curve: Callable[[], float] = diurnal_multiplier) -> None:
        self.rate = rate
        self.curve = curve
        self._tokens = 0.0
        self._last = monotonic()

    def refill(self) -> None:
        now = monotonic()
        self._tokens += self.rate * self.curve() * (now - self._last)
        self._last = now

    def try_consume(self, n: float = 1.0) -> bool:
        if self._tokens >= n:
            self._tokens -= n
            return True
        return False

