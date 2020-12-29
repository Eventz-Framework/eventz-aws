import json

import boto3
from eventz.marshall import Marshall, FqnResolver
from eventz.codecs.datetime import Datetime
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
    assert store.fetch(parent_id1) == (parent_created_event_1, child_chosen_event_1)


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
    store.persist(parent_id1, [parent_created_event_1, child_chosen_event_1])

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
    store.persist(parent_id1, [parent_created_event_1, child_chosen_event_1])
    assert len(store.fetch(parent_id1)) == 2
    store.persist(parent_id1, [parent_created_event_2, child_chosen_event_2])
    assert len(store.fetch(parent_id1)) == 4
