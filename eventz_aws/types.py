from typing import Protocol, Union, Dict, Tuple

from eventz.messages import Event
from eventz.packets import Packet

Payload = Union[Dict, Event, Tuple[Event]]


class EventPublisherProtocol(Protocol):
    def publish(self, packet: Packet) -> None:
        ...


class SocketClientProtocol(Protocol):
    def send(self, connection_id: str, packet: Packet) -> None:
        ...
