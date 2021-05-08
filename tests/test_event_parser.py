from eventz.packets import Packet

from eventz_aws.event_parser import EventParser


def test_incomning_aws_event_translated_to_command_packet():
    event_parser = EventParser()
    event = {
        "requestContext": {
            "routeKey": "$default",
            "messageId": "ew0FgeFbrPECE2g=",
            "eventType": "MESSAGE",
            "extendedRequestId": "ew0FgHvYLPEFffg=",
            "requestTime": "03/May/2021:17:32:44 +0000",
            "messageDirection": "IN",
            "stage": "Prod",
            "connectedAt": 1620063164506,
            "requestTimeEpoch": 1620063164762,
            "identity": {
                "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:87.0) Gecko/20100101 Firefox/87.0",
                "sourceIp": "82.38.11.131",
            },
            "requestId": "ew0FgHvYLPEFffg=",
            "domainName": "lbhk207o91.execute-api.eu-west-2.amazonaws.com",
            "connectionId": "ew0FeeFarPECE2g=",
            "apiId": "lbhk207o91",
        },
        "body": '{"type":"COMMAND","route":"GameService","msgid":"3b203c12-8e8e-44fc-b4e8-d6acddeaf8dc",'
        '"dialog":"3b203c12-8e8e-44fc-b4e8-d6acddeaf8dc","seq":1,"payload":{"__fqn__":"commands.tarot.CreateGame","__version__":1,"__msgid__":"741e00f2-85c3-4c9f-9d38-886711579c34","__timestamp__":"2021-05-03T17:32:44.404Z","aggregateId":"3c55c40c-3603-4928-9e1b-f9b1d4232c14","template":"SeasonsSimple"}}',
        "isBase64Encoded": False,
    }
    assert event_parser.get_command_packet(event=event) == Packet(
        subscribers=("ew0FeeFarPECE2g=",),
        message_type="COMMAND",
        route="GameService",
        msgid="3b203c12-8e8e-44fc-b4e8-d6acddeaf8dc",
        dialog="3b203c12-8e8e-44fc-b4e8-d6acddeaf8dc",
        seq=1,
        options=None,
        payload={
            "__fqn__": "commands.tarot.CreateGame",
            "__version__": 1,
            "__msgid__": "741e00f2-85c3-4c9f-9d38-886711579c34",
            "__timestamp__": "2021-05-03T17:32:44.404Z",
            "aggregateId": "3c55c40c-3603-4928-9e1b-f9b1d4232c14",
            "template": "SeasonsSimple"
        },
    )
