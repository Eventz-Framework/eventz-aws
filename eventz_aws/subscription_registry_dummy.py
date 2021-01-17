import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, TypedDict

from eventz.protocols import SubscriptionRegistryProtocol

log = logging.getLogger(__name__)
log.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class Registration(TypedDict):
    subscription: str
    time: datetime


class SubscriptionRegistryDummy(SubscriptionRegistryProtocol[str]):
    def __init__(self):
        self._registry: Dict[str, List[Registration]] = defaultdict(list)

    def register(
        self, aggregate_id: str, subscription: str, time: Optional[datetime] = None
    ) -> None:
        time = time or datetime.now(timezone.utc)
        if not any(s["subscription"] == subscription for s in self._registry[aggregate_id]):
            self._registry[aggregate_id].append(
                {"subscription": subscription, "time": time,}
            )

    def fetch(self, aggregate_id: str) -> Tuple[str]:
        sorted_items = sorted(self._registry[aggregate_id], key=lambda i: i["time"])
        return tuple(i["subscription"] for i in sorted_items)
