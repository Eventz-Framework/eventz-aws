import json

import boto3

from eventz_aws.types import SocketClientProtocol


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
    ) -> None:
        endpoint_url = f"https://{self._api_id}.execute-api.{self._region}.amazonaws.com/{self._stage}"
        message = json.dumps(
            {
                "type": message_type,
                "route": route,
                "msgid": msgid,
                "dialog": dialog,
                "seq": seq,
            }
        )
        gateway = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint_url)
        gateway.post_to_connection(
            ConnectionId=connection_id, Data=bytes(message, "utf-8")
        )
