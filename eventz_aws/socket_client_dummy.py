from eventz.packets import Packet

from eventz_aws.types import SocketClientProtocol


class SocketClientDummy(SocketClientProtocol):
    def send(self, connection_id: str, packet: Packet) -> None:
        pass
