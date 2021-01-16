import json

import boto3
from moto import mock_s3

from eventz_aws.event_store_json_s3 import EventStoreJsonS3
from tests.conftest import parent_id1

bucket_name = "TestBucket"
region = "eu-west-1"


@mock_s3
def test_sequence_of_events_can_be_read(
    marshall, json_events, parent_created_event_1, child_chosen_event_1
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
    assert store.fetch(parent_id1) == (
        parent_created_event_1.sequence(1),
        child_chosen_event_1.sequence(2),
    )
    assert store.fetch(parent_id1, seq=2) == (
        child_chosen_event_1.sequence(2),
    )


@mock_s3
def test_new_sequence_of_events_can_be_persisted(
    marshall, json_events, parent_created_event_1, child_chosen_event_1
):
    store = EventStoreJsonS3(
        bucket_name=bucket_name,
        region=region,
        marshall=marshall,
        recreate_storage=True,
    )
    assert store.fetch(parent_id1) == ()
    persisted_events = store.persist(parent_id1, (parent_created_event_1, child_chosen_event_1,))
    assert persisted_events[0].__seq__ == 1
    assert persisted_events[1].__seq__ == 2
    client = boto3.client("s3", region_name=region)
    obj = client.get_object(Bucket=bucket_name, Key=parent_id1)
    json_string = obj.get("Body").read().decode("utf-8")
    persisted_json = json.loads(json_string)
    assert persisted_json == json_events


@mock_s3
def test_two_batches_of_events_can_be_persisted(
    marshall, json_events, parent_created_event_1, child_chosen_event_1, parent_created_event_2, child_chosen_event_2
):
    store = EventStoreJsonS3(
        bucket_name=bucket_name,
        region=region,
        marshall=marshall,
        recreate_storage=True,
    )
    assert store.fetch(parent_id1) == ()
    persisted_events = store.persist(parent_id1, (parent_created_event_1, child_chosen_event_1,))
    assert persisted_events[0].__seq__ == 1
    assert persisted_events[1].__seq__ == 2
    fetched_events = store.fetch(parent_id1)
    assert len(fetched_events) == 2
    assert fetched_events[0].__seq__ == 1
    assert fetched_events[1].__seq__ == 2
    persisted_events = store.persist(parent_id1, (parent_created_event_2, child_chosen_event_2,))
    assert persisted_events[0].__seq__ == 3
    assert persisted_events[1].__seq__ == 4
    fetched_events = store.fetch(parent_id1)
    assert len(fetched_events) == 4
    assert fetched_events[0].__seq__ == 1
    assert fetched_events[1].__seq__ == 2
    assert fetched_events[2].__seq__ == 3
    assert fetched_events[3].__seq__ == 4
