from typing import Optional, List

from eventz_aws.types import SocketClientProtocol, Payload


class SocketClientDummy(SocketClientProtocol):
    def send(
        self,
        message_type: str,
        connection_id: str,
        route: str,
        msgid: str,
        dialog: str,
        seq: int,
        options: Optional[List[str]] = None,
        payload: Optional[Payload] = None,
    ) -> None:
        pass
