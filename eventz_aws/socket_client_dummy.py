from eventz_aws.types import SocketClientProtocol


class SocketClientDummy(SocketClientProtocol):
    def send(
        self,
        message_type: str,
        connection_id: str,
        route: str,
        msgid: str,
        dialog: str,
        seq: int,
    ) -> None:
        pass
