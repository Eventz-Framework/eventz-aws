import json
from typing import Any, Dict, TypedDict

from eventz.packets import Packet
from eventz.protocols import EventParserProtocol


class RequestContext(TypedDict):
    routeKey: str
    messageId: str
    eventType: str
    extendedRequestId: str
    requestTime: str
    messageDirection: str
    stage: str
    connectedAt: str
    requestTimeEpoch: str
    identity: Dict[str, str]
    requestId: str
    domainName: str
    connectionId: str
    apiId: str


class AwsWebSocketEvent(TypedDict):
    requestContext: RequestContext
    body: str
    isBase64Encoded: bool


class EventParser(EventParserProtocol[AwsWebSocketEvent]):
    def get_command_packet(self, event: AwsWebSocketEvent) -> Packet:
        body: Dict[str, Any] = json.loads(event["body"])
        return Packet(
            subscribers=(event["requestContext"]["connectionId"],),
            message_type="COMMAND",
            route=body.get("route", "UnknownService"),
            msgid=body.get("msgid", "UnknownMsgid"),
            dialog=body.get("dialog", "UnknownDialog"),
            seq=body.get("seq", 1),
            options=None,
            payload=body.get("payload")
        )
