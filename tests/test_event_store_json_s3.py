import json
from datetime import datetime

import boto3
import pytest
from eventz.aggregate import Aggregate
from eventz.marshall import Marshall, DatetimeCodec
from moto import mock_s3

from eventz_aws.event_store_json_s3 import EventStoreJsonS3
from tests.example.child import Child
from tests.example.children import Children
from tests.example.parent import ParentCreated, ChildChosen

parent_id1 = Aggregate.make_id()
msgid1 = "11111111-1111-1111-1111-111111111111"
msgid2 = "22222222-2222-2222-2222-222222222222"
dt_iso1 = "2020-01-02T03:04:05.123456"
dt_iso2 = "2020-01-02T03:04:06.123456"
dt1 = datetime(2020, 1, 2, 3, 4, 5, 123456)
dt2 = datetime(2020, 1, 2, 3, 4, 6, 123456)


@mock_s3
def test_sequence_of_events_can_be_read(
    json_events, parent_created_event, child_chosen_event
):
    bucket_name = "TestBucket"
    region = "us-east-1"
    store = EventStoreJsonS3(
        bucket_name=bucket_name,
        region=region,
        marshall=Marshall({"eventz.marshall.DatetimeCodec": DatetimeCodec()}),
        recreate_storage=True,
    )
    # insert fixture data into the storage
    client = boto3.client("s3", region_name=region)
    client.put_object(Bucket=bucket_name, Key=parent_id1, Body=json.dumps(json_events))
    # run test and make assertion
    assert store.fetch(parent_id1) == (parent_created_event, child_chosen_event)


@mock_s3
def test_new_sequence_of_events_can_be_persisted(
    json_events, parent_created_event, child_chosen_event
):
    bucket_name = "TestBucket"
    region = "us-east-1"
    store = EventStoreJsonS3(
        bucket_name=bucket_name,
        region=region,
        marshall=Marshall({"eventz.marshall.DatetimeCodec": DatetimeCodec()}),
        recreate_storage=True,
    )
    assert store.fetch(parent_id1) == ()
    store.persist(parent_id1, [parent_created_event, child_chosen_event])

    client = boto3.client("s3", region_name=region)
    obj = client.get_object(Bucket=bucket_name, Key=parent_id1)
    json_string = obj.get("Body").read().decode("utf-8")
    persisted_json = json.loads(json_string)
    assert persisted_json == json_events


@pytest.fixture
def parent_created_event():
    return ParentCreated(
        parent_id=parent_id1,
        children=Children(
            name="Group One",
            items=[
                Child(name="Child One"),
                Child(name="Child Two"),
                Child(name="Child Three"),
            ],
        ),
        msgid=msgid1,
        timestamp=dt1,
    )


@pytest.fixture
def child_chosen_event():
    return ChildChosen(
        parent_id=parent_id1,
        child=Child(name="Child Three"),
        msgid=msgid2,
        timestamp=dt2,
    )


@pytest.fixture()
def json_events():
    return [
        {
            "__fcn__": "tests.example.parent.ParentCreated",
            "__version__": 1,
            "msgid": msgid1,
            "timestamp": {
                "__codec__": "eventz.marshall.DatetimeCodec",
                "params": {"timestamp": dt_iso1},
            },
            "parent_id": parent_id1,
            "children": {
                "__fcn__": "tests.example.children.Children",
                "name": "Group One",
                "items": [
                    {"__fcn__": "tests.example.child.Child", "name": "Child One",},
                    {"__fcn__": "tests.example.child.Child", "name": "Child Two",},
                    {"__fcn__": "tests.example.child.Child", "name": "Child Three",},
                ],
            },
        },
        {
            "__fcn__": "tests.example.parent.ChildChosen",
            "__version__": 1,
            "msgid": msgid2,
            "timestamp": {
                "__codec__": "eventz.marshall.DatetimeCodec",
                "params": {"timestamp": dt_iso2},
            },
            "parent_id": parent_id1,
            "child": {"__fcn__": "tests.example.child.Child", "name": "Child Three",},
        },
    ]
