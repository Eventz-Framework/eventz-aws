from eventz.packets import Packet

from eventz_aws.socket_client_base import SocketClientBase
from eventz_aws.types import SocketClientProtocol, SocketStatsProtocol


class SocketClientDummy(SocketClientBase, SocketClientProtocol, SocketStatsProtocol):
    def __init__(self):
        super().__init__()

    def send(self, connection_id: str, packet: Packet) -> None:
        super()._register(connection_id, packet)
