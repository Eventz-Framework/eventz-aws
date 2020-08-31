import json

import boto3
from eventz.messages import Event

from eventz_aws.types import EventPublisherProtocol


class EventPublisher(EventPublisherProtocol):
    def __init__(self, arn: str):
        self._arn: str = arn

    def publish(
        self,
        connection_id: str,
        route: str,
        msgid: str,
        dialog: str,
        seq: int,
        event: Event,
    ) -> None:
        client = boto3.client("sns")
        message = {
            "transport": {
                "connection_id": connection_id,
                "route": route,
                "msgid": msgid,
                "dialog": dialog,
                "seq": seq,
            },
            "event": event,
        }
        client.publish(
            TargetArn=self._arn,
            Message=json.dumps({"default": json.dumps(message)}),
            MessageStructure="json",
        )
