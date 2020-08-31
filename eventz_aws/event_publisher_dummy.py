from typing import List

from eventz.messages import Event

from eventz_aws.types import EventPublisherProtocol


class EventPublisherDummy(EventPublisherProtocol):
    def __init__(self):
        self.events: List[Event] = []

    def publish(
        self,
        connection_id: str,
        route: str,
        msgid: str,
        dialog: str,
        seq: int,
        event: Event,
    ) -> None:
        self.events.append(event)
