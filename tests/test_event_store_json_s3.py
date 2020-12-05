import json
from datetime import datetime, timezone

import boto3
import pytest
from eventz.aggregate import Aggregate
from eventz.marshall import Marshall, FqnResolver
from eventz.codecs.datetime import Datetime
from moto import mock_s3

from eventz_aws.event_store_json_s3 import EventStoreJsonS3
from tests.example.child import Child
from tests.example.children import Children
from tests.example.parent import ParentCreated, ChildChosen

parent_id1 = Aggregate.make_id()
msgid1 = "11111111-1111-1111-1111-111111111111"
msgid2 = "22222222-2222-2222-2222-222222222222"
dt_iso1 = "2020-01-02T03:04:05.123Z"
dt_iso2 = "2020-01-02T03:04:06.123Z"
dt1 = datetime(2020, 1, 2, 3, 4, 5, 123000, tzinfo=timezone.utc)
dt2 = datetime(2020, 1, 2, 3, 4, 6, 123000, tzinfo=timezone.utc)
bucket_name = "TestBucket"
region = "eu-west-1"
marshall = Marshall(
    fqn_resolver=FqnResolver(
        fqn_map={
            "tests.Child": "tests.example.child.Child",
            "tests.Children": "tests.example.children.Children",
            "tests.ChildChosen": "tests.example.parent.ChildChosen",
            "tests.Parent": "tests.example.parent.Parent",
            "tests.ParentCreated": "tests.example.parent.ParentCreated",
        }
    ),
    codecs={"codecs.eventz.Datetime": Datetime()},
)


@mock_s3
def test_sequence_of_events_can_be_read(
    json_events, parent_created_event, child_chosen_event
):
    store = EventStoreJsonS3(
        bucket_name=bucket_name,
        region=region,
        marshall=marshall,
        recreate_storage=True,
    )
    # insert fixture data into the storage
    client = boto3.client("s3", region_name=region)
    client.put_object(
        Bucket=bucket_name,
        Key=parent_id1,
        Body=json.dumps(json_events, sort_keys=True, separators=(",", ":")),
    )
    # run test and make assertion
    assert store.fetch(parent_id1) == (parent_created_event, child_chosen_event)


@mock_s3
def test_new_sequence_of_events_can_be_persisted(
    json_events, parent_created_event, child_chosen_event
):
    store = EventStoreJsonS3(
        bucket_name=bucket_name,
        region=region,
        marshall=marshall,
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
        __msgid__=msgid1,
        __timestamp__=dt1,
    )


@pytest.fixture
def child_chosen_event():
    return ChildChosen(
        parent_id=parent_id1,
        child=Child(name="Child Three"),
        __msgid__=msgid2,
        __timestamp__=dt2,
    )


@pytest.fixture()
def json_events():
    return [
        {
            "__fqn__": "tests.ParentCreated",
            "__version__": 1,
            "__msgid__": msgid1,
            "__timestamp__": {
                "__codec__": "codecs.eventz.Datetime",
                "params": {"timestamp": dt_iso1},
            },
            "parent_id": parent_id1,
            "children": {
                "__fqn__": "tests.Children",
                "name": "Group One",
                "items": [
                    {"__fqn__": "tests.Child", "name": "Child One",},
                    {"__fqn__": "tests.Child", "name": "Child Two",},
                    {"__fqn__": "tests.Child", "name": "Child Three",},
                ],
            },
        },
        {
            "__fqn__": "tests.ChildChosen",
            "__version__": 1,
            "__msgid__": msgid2,
            "__timestamp__": {
                "__codec__": "codecs.eventz.Datetime",
                "params": {"timestamp": dt_iso2},
            },
            "parent_id": parent_id1,
            "child": {"__fqn__": "tests.Child", "name": "Child Three",},
        },
    ]
