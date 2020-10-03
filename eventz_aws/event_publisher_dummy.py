import json
from typing import List

from eventz.messages import Event
from eventz.protocols import MarshallProtocol

from eventz_aws.types import EventPublisherProtocol


class EventPublisherDummy(EventPublisherProtocol):
    def __init__(self, marshall: MarshallProtocol):
        self.events: List[str] = []
        self._marshall: MarshallProtocol = marshall

    def publish(
        self,
        connection_id: str,
        route: str,
        msgid: str,
        dialog: str,
        seq: int,
        event: Event,
    ) -> None:
        self.events.append(json.dumps(event))

    def _try_event(self, item: Any) -> None:
        if isinstance(item, Event):
            self.events.append(self._marshall.to_json(item))
