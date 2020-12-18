import json
import logging
import os

import boto3
from eventz.packets import Packet

from eventz_aws.socket_client_base import SocketClientBase
from eventz_aws.types import SocketClientProtocol, SocketStatsProtocol

log = logging.getLogger(__name__)
log.setLevel(os.getenv("LOG_LEVEL", "DEBUG"))


class SocketClient(SocketClientBase, SocketClientProtocol, SocketStatsProtocol):
    def __init__(self, api_id: str, region: str, stage: str):
        super().__init__()
        self._api_id: str = api_id
        self._region: str = region
        self._stage: str = stage

    def send(self, connection_id: str, packet: Packet) -> None:
        log.debug(f"SocketClient.send connection_id={connection_id} packet={packet}")
        endpoint_url = f"https://{self._api_id}.execute-api.{self._region}.amazonaws.com/{self._stage}"
        log.debug(f"endpoint_url={endpoint_url}")
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
        gateway = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint_url)
        log.debug(f"message = {message}")
        gateway.post_to_connection(
            ConnectionId=connection_id, Data=bytes(json.dumps(message), "utf-8")
        )
