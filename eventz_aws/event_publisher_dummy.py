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
        self.events.append(packet)
