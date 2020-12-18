from uuid import uuid4

from eventz.packets import Packet

from eventz_aws.socket_client_dummy import SocketClientDummy

subscriber_id_1 = str(uuid4())
subscriber_id_2 = str(uuid4())
dialog_1 = str(uuid4())
packet_msgid_1 = str(uuid4())
packet_msgid_2 = str(uuid4())
packet_msgid_3 = str(uuid4())
event_msgid_1 = str(uuid4())
event_msgid_2 = str(uuid4())
event_msgid_3 = str(uuid4())
packet_1 = Packet(
    subscribers=[subscriber_id_1, subscriber_id_2],
    message_type="EVENT",
    route="ExampleService",
    msgid=packet_msgid_1,
    dialog=dialog_1,
    seq=3,
    options=None,
    payload={
        "__fqn__": "events.example.Thing1",
        "__msgid__": event_msgid_1,
        "__timestamp__": {
            "__codec__": "codecs.eventz.Datetime",
            "params": {"timestamp": "2020-12-17T01:02:03.345Z"},
        },
        "__version__": 1,
        "value": "One"
    },
)
packet_2 = Packet(
    subscribers=[subscriber_id_1, subscriber_id_2],
    message_type="EVENT",
    route="ExampleService",
    msgid=packet_msgid_2,
    dialog=dialog_1,
    seq=3,
    options=None,
    payload={
        "__fqn__": "events.example.Thing1",
        "__msgid__": event_msgid_2,
        "__timestamp__": {
            "__codec__": "codecs.eventz.Datetime",
            "params": {"timestamp": "2020-12-17T01:02:03.345Z"},
        },
        "__version__": 1,
        "value": "One"
    },
)
packet_3 = Packet(
    subscribers=[subscriber_id_1, subscriber_id_2],
    message_type="EVENT",
    route="ExampleService",
    msgid=packet_msgid_3,
    dialog=dialog_1,
    seq=3,
    options=None,
    payload={
        "__fqn__": "events.example.Thing1",
        "__msgid__": event_msgid_3,
        "__timestamp__": {
            "__codec__": "codecs.eventz.Datetime",
            "params": {"timestamp": "2020-12-17T01:02:03.345Z"},
        },
        "__version__": 1,
        "value": "One"
    },
)


def test_sent_packets_are_remembered_correctly():
    client = SocketClientDummy()
    client.send(connection_id=subscriber_id_1, packet=packet_1)
    client.send(connection_id=subscriber_id_2, packet=packet_1)
    client.send(connection_id=subscriber_id_1, packet=packet_2)
    client.send(connection_id=subscriber_id_2, packet=packet_2)
    client.send(connection_id=subscriber_id_1, packet=packet_3)
    assert client.total_packets_sent == 5
    assert client.get_packets_sent_to_subscriber(subscriber_id_1) == (
        packet_1,
        packet_2,
        packet_3,
    )
    assert client.get_packets_sent_to_subscriber(subscriber_id_2) == (
        packet_1,
        packet_2,
    )
    assert client.get_packet(packet_1.msgid) == packet_1
    assert client.get_packet(packet_2.msgid) == packet_2
    assert client.get_packet(packet_3.msgid) == packet_3
    assert client.get_packet_subscribers(packet_1.msgid) == [subscriber_id_1, subscriber_id_2]
    assert client.get_packet_subscribers(packet_2.msgid) == [subscriber_id_1, subscriber_id_2]
    assert client.get_packet_subscribers(packet_3.msgid) == [subscriber_id_1]
