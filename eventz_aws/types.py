from typing import Protocol, Union, Dict, Optional, Tuple

from eventz.messages import Event

Payload = Union[Dict, Event, Tuple[Event]]


class EventPublisherProtocol(Protocol):
    def publish(
            self,
            subscribers: Tuple[str],
            message_type: str,
            route: str,
            msgid: str,
            dialog: str,
            seq: int,
            options: Optional[Tuple[str]],
            payload: Payload,
    ) -> None:
        ...


class SocketClientProtocol(Protocol):
    def send(
            self,
            message_type: str,
            connection_id: str,
            route: str,
            msgid: str,
            dialog: str,
            seq: int,
            options: Optional[Tuple[str]],
            payload: Optional[Payload],
    ) -> None:
        ...
