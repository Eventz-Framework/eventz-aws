import json
from typing import List

import boto3
from eventz.protocols import MarshallProtocol
from eventz.packets import Packet

from eventz_aws.types import EventPublisherProtocol


class EventPublisher(EventPublisherProtocol):
    def __init__(self, arn: str, marshall: MarshallProtocol):
        self._arn: str = arn
        self._marshall: MarshallProtocol = marshall

    def publish(self, packet: Packet) -> None:
        client = boto3.client("sns")
        message = {
            "transport": {
                "subscribers": List[str],
                "type": packet.message_type,
                "route": packet.route,
                "msgid": packet.msgid,
                "dialog": packet.dialog,
                "seq": packet.seq,
            },
            "options": packet.options,
        }
        if packet.payload:
            message["payload"] = packet.payload
        client.publish(
            TargetArn=self._arn,
            Message=json.dumps({"default": self._marshall.to_json(message)}),
            MessageStructure="json",
        )
