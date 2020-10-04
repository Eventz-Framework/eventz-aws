from typing import List, Any, Optional, Tuple

from eventz.messages import Event
from eventz.protocols import MarshallProtocol

from eventz_aws.types import EventPublisherProtocol, Payload


class EventPublisherDummy(EventPublisherProtocol):
    def __init__(self, marshall: MarshallProtocol):
        self.events: List[str] = []
        self._marshall: MarshallProtocol = marshall

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
        if isinstance(payload, list):
            for item in payload:
                self._try_event(item)
            return
        self._try_event(payload)

    def _try_event(self, item: Any) -> None:
        if isinstance(item, Event):
            self.events.append(self._marshall.to_json(item))
