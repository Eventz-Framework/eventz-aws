import functools
import operator
from collections import defaultdict
from typing import Dict, List, Tuple

from eventz.packets import Packet

from eventz_aws.errors import PacketNotFound
from eventz_aws.types import SocketStatsProtocol


class SocketClientBase(SocketStatsProtocol):
    def __init__(self):
        self._packets: Dict[str, List[Packet]] = defaultdict(list)

    def _register(self, subscriber_id: str, packet: Packet) -> None:
        """
        Provides a bunch of useful statistics about the Packets handled by the socket.
        Can be useful for log generation and other kinds of debug processes.
        """
        self._packets[subscriber_id].append(packet)

    @property
    def total_packets_sent(self) -> int:
        return functools.reduce(
            operator.add,
            [len(packets) for _, packets in self._packets.items()]
        )

    def get_packets_sent_to_subscriber(self, subscriber_id: str) -> Tuple[Packet, ...]:
        return tuple(self._packets[subscriber_id])

    def get_packet(self, msgid: str) -> Packet:
        for subscriber_id, packets in self._packets.items():
            for packet in packets:
                if packet.msgid == msgid:
                    return packet
        err = f"Could not find a packet in this client with mesgid={msgid}"
        raise PacketNotFound(err)

    def get_packet_subscribers(self, msgid: str) -> List[str]:
        subscribers = []
        for subscriber_id, packets in self._packets.items():
            for packet in packets:
                if packet.msgid == msgid:
                    subscribers.append(subscriber_id)
                    break
        return subscribers
