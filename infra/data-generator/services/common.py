"""Common Utilities – SRP: IDs, time, and simple caches.

ISP: tiny helpers the generators share.
"""
from __future__ import annotations

import random
import string
from collections import deque
from datetime import datetime, timezone
from typing import Deque


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def rid(prefix: str, n: int = 10) -> str:
    return prefix + "_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


class HotCache:
    """SRP: encapsulate recent IDs. Small and explicit.

    Liskov-friendly: a dict of deques is still a HotCache.
    """

    def __init__(self) -> None:
        self.users: Deque[str] = deque(maxlen=1000)
        self.products: Deque[str] = deque(maxlen=1000)
        self.orders: Deque[str] = deque(maxlen=3000)
        self.warehouses: Deque[str] = deque(maxlen=50)
        self.suppliers: Deque[str] = deque(maxlen=100)
        self.sessions: Deque[tuple[str, str]] = deque(maxlen=200)

    def pick_user(self) -> str:
        if self.users and random.random() < 0.95:
            return random.choice(list(self.users))
        uid = rid("u", 8)
        self.users.append(uid)
        return uid

    def get_or_create_session(self, user_id: str) -> str:
        if random.random() < 0.4 or not self.sessions:
            sid = rid("sess", 12)
            self.sessions.append((user_id, sid))
            return sid
        options = [s for u, s in self.sessions if u == user_id]
        if options:
            return random.choice(options)
        sid = rid("sess", 12)
        self.sessions.append((user_id, sid))
        return sid

    def pick_product(self) -> str:
        if self.products:
            return random.choice(list(self.products))
        return rid("p", 8)

    def pick_warehouse(self) -> str:
        return random.choice(list(self.warehouses)) if self.warehouses else rid("wh", 6)

    def pick_supplier(self) -> str:
        return random.choice(list(self.suppliers)) if self.suppliers else rid("sup", 6)


def ensure_str(v, fallback: str) -> str:
    return v if isinstance(v, str) and v != "" else fallback


def ensure_int(v, fallback: int) -> int:
    try:
        return int(v)
    except Exception:
        return int(fallback)


def ensure_float(v, fallback: float) -> float:
    try:
        return float(v)
    except Exception:
        return float(fallback)


def clamp_enum(v, allowed: list[str], fallback: str) -> str:
    return v if v in allowed else fallback
