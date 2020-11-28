import json

import boto3
from eventz.packets import Packet

from eventz_aws.types import SocketClientProtocol


class SocketClient(SocketClientProtocol):
    def __init__(self, api_id: str, region: str, stage: str):
        self._api_id: str = api_id
        self._region: str = region
        self._stage: str = stage

    def send(self, connection_id: str, packet: Packet) -> None:
        endpoint_url = f"https://{self._api_id}.execute-api.{self._region}.amazonaws.com/{self._stage}"
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
        gateway.post_to_connection(
            ConnectionId=connection_id, Data=bytes(json.dumps(message), "utf-8")
        )
