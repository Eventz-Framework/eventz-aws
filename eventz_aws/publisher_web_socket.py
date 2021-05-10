import logging
import os

from boto3_type_annotations.apigatewaymanagementapi import Client as WebSocketClient
from eventz.packets import Packet
from eventz.protocols import MarshallProtocol, PublisherProtocol

log = logging.getLogger(__name__)
log.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class PublisherWebSocket(PublisherProtocol):
    def __init__(self, marshall: MarshallProtocol, client: WebSocketClient):
        super().__init__()
        self._marshall: MarshallProtocol = marshall
        self._client: WebSocketClient = client

    def publish(self, packet: Packet) -> None:
        message = {
            "type": packet.message_type,
            "route": packet.route,
            "msgid": packet.msgid,
            "dialog": packet.dialog,
            "seq": packet.seq,
        }
        if packet.options:
            message["options"] = packet.options
        if packet.payload:
            message["payload"] = packet.payload
        log.info(f"message = {message}")
        for connection_id in packet.subscribers:
            log.info(f"PublisherWebSocket.send connection_id={connection_id} packet={packet}")
            self._client.post_to_connection(
                ConnectionId=connection_id, Data=bytes(self._marshall.to_json(message), "utf-8")
            )
