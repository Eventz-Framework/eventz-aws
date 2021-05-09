from typing import List, Protocol, Union, Dict, Tuple

from eventz.messages import Event
from eventz.packets import Packet

Payload = Union[Dict, Event, Tuple[Event]]


class SocketClientProtocol(Protocol):
    def send(self, connection_id: str, packet: Packet) -> None:
        ...


class SocketStatsProtocol(Protocol):
    @property
    def total_packets_sent(self) -> int:
        ...

    def get_packets_sent_to_subscriber(self, subscriber_id: str) -> Tuple[Packet, ...]:
        ...

    def get_packet(self, msgid: str) -> Packet:
        ...

    def get_packet_subscribers(self, msgid: str) -> List[str]:
        ...
