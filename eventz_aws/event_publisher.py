import json
from typing import List, Optional, Tuple

import boto3
from eventz.protocols import MarshallProtocol

from eventz_aws.types import EventPublisherProtocol, Payload


class EventPublisher(EventPublisherProtocol):
    def __init__(self, arn: str, marshall: MarshallProtocol):
        self._arn: str = arn
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
        options = options or []
        client = boto3.client("sns")
        message = {
            "transport": {
                "subscribers": List[str],
                "type": message_type,
                "route": route,
                "msgid": msgid,
                "dialog": dialog,
                "seq": seq,
            },
            "options": options,
            "payload": payload,
        }
        client.publish(
            TargetArn=self._arn,
            Message=json.dumps({"default": self._marshall.to_json(message)}),
            MessageStructure="json",
        )
