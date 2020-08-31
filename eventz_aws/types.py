from typing import Protocol

from eventz.messages import Event


class EventPublisherProtocol(Protocol):
    def publish(
        self,
        connection_id: str,
        route: str,
        msgid: str,
        dialog: str,
        seq: int,
        event: Event,
    ) -> None:
        ...


class SocketClientProtocol(Protocol):
    def send(
        self, message_type: str, connection_id: str, route: str, msgid: str, dialog: str, seq: int
    ) -> None:
        ...
