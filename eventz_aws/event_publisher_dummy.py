from typing import List

from eventz.packets import Packet
from eventz.protocols import MarshallProtocol

from eventz_aws.types import EventPublisherProtocol


class EventPublisherDummy(EventPublisherProtocol):
    def __init__(self, marshall: MarshallProtocol):
        self.events: List[Packet] = []
        self._marshall: MarshallProtocol = marshall

    def publish(self, packet: Packet) -> None:
        self.events.append(packet)
