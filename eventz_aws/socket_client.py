import json
from typing import Optional, List

import boto3

from eventz_aws.types import SocketClientProtocol, Payload


class SocketClient(SocketClientProtocol):
    def __init__(self, api_id: str, region: str, stage: str):
        self._api_id: str = api_id
        self._region: str = region
        self._stage: str = stage

    def send(
        self,
        message_type: str,
        connection_id: str,
        route: str,
        msgid: str,
        dialog: str,
        seq: int,
        options: Optional[List[str]] = None,
        payload: Optional[Payload] = None,
    ) -> None:
        endpoint_url = f"https://{self._api_id}.execute-api.{self._region}.amazonaws.com/{self._stage}"
        message = {
            "type": message_type,
            "route": route,
            "msgid": msgid,
            "dialog": dialog,
            "seq": seq,
        }
        if options:
            message["options"] = options
        if payload:
            message["payload"] = payload
        gateway = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint_url)
        gateway.post_to_connection(
            ConnectionId=connection_id, Data=bytes(json.dumps(message), "utf-8")
        )
