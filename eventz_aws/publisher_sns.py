import json
import logging
import os

from boto3_type_annotations.sns import Client as SnsClient
from eventz.protocols import MarshallProtocol
from eventz.packets import Packet

from eventz.protocols import PublisherProtocol

log = logging.getLogger(__name__)
log.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class PublisherSns(PublisherProtocol):
    def __init__(self, arn: str, marshall: MarshallProtocol, client: SnsClient):
        self._arn: str = arn
        self._marshall: MarshallProtocol = marshall
        self._client = client

    def publish(self, packet: Packet) -> None:
        message = {
            "transport": {
                "subscribers": packet.subscribers,
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
        log.info(f"EventPublisher.publish message: {message}")
        self._client.publish(
            TargetArn=self._arn,
            Message=json.dumps({"default": self._marshall.to_json(message)}),
            MessageStructure="json",
        )
