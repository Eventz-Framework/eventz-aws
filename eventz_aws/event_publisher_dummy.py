from typing import List, Any, Optional, Tuple

from eventz.messages import Event
from eventz.packets import Packet
from eventz.protocols import MarshallProtocol

from eventz_aws.types import EventPublisherProtocol, Payload


class EventPublisherDummy(EventPublisherProtocol):
    def __init__(self, marshall: MarshallProtocol):
        self.events: List[str] = []
        self._marshall: MarshallProtocol = marshall

    def publish(self, packet: Packet) -> None:
        if isinstance(packet.payload, list):
            for item in packet.payload:
                self._try_event(item)
            return
        self._try_event(packet.payload)

    def _try_event(self, item: Any) -> None:
        if isinstance(item, Event):
            self.events.append(self._marshall.to_json(item))
