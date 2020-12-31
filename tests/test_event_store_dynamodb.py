from typing import Tuple, cast

from eventz_aws.event_store_dynamodb import EventStoreDynamodb
from tests.conftest import dynamodb_events_table_name, parent_id1
from tests.example.children import Children
from tests.example.parent import ParentCreated


def test_sequence_of_events_can_be_read(
    dynamodb_connection_with_initial_events,
    marshall,
    json_events,
    parent_created_event_1,
    child_chosen_event_1,
):
    store = EventStoreDynamodb(
        aggregate="parent",
        table_name=dynamodb_events_table_name,
        connection=dynamodb_connection_with_initial_events,
        marshall=marshall,
    )
    # run test and make assertion
    assert store.fetch(parent_id1) == (parent_created_event_1, child_chosen_event_1)


def test_new_sequence_of_events_can_be_persisted(
        dynamodb_connection_with_empty_events_table, marshall, json_events, parent_created_event_1, child_chosen_event_1
):
    store = EventStoreDynamodb(
        aggregate="parent",
        table_name=dynamodb_events_table_name,
        connection=dynamodb_connection_with_empty_events_table,
        marshall=marshall,
    )
    assert store.fetch(parent_id1) == ()
    store.persist(parent_id1, [parent_created_event_1, child_chosen_event_1])
    response = dynamodb_connection_with_empty_events_table.query(
        TableName=dynamodb_events_table_name,
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={
            ":pk": {"S": f"parent-{parent_id1}"}
        },
        ScanIndexForward=False,
        ConsistentRead=True,
    )
    assert len(response["Items"]) == 2


def test_two_batches_of_events_can_be_persisted(
        dynamodb_connection_with_empty_events_table, marshall, json_events, parent_created_event_1, child_chosen_event_1, parent_created_event_2, child_chosen_event_2
):
    store = EventStoreDynamodb(
        aggregate="parent",
        table_name=dynamodb_events_table_name,
        connection=dynamodb_connection_with_empty_events_table,
        marshall=marshall,
    )
    assert store.fetch(parent_id1) == ()
    store.persist(parent_id1, [parent_created_event_1, child_chosen_event_1])
    assert len(store.fetch(parent_id1)) == 2
    store.persist(parent_id1, [parent_created_event_2, child_chosen_event_2])
    assert len(store.fetch(parent_id1)) == 4


def test_ordering_with_more_than_ten_items(
    dynamodb_connection_with_empty_events_table,
    marshall,
    json_events,
    parent_created_event_1,
    child_chosen_event_1,
):
    store = EventStoreDynamodb(
        aggregate="parent",
        table_name=dynamodb_events_table_name,
        connection=dynamodb_connection_with_empty_events_table,
        marshall=marshall,
    )
    assert store.fetch(parent_id1) == ()
    events = [
        ParentCreated(
            parent_id=parent_id1, children=Children(name=f"Group{str(i)}", items=[],),
        )
        for i in range(1, 12)
    ]
    store.persist(parent_id1, events)
    fetched_items: Tuple[ParentCreated] = cast(
        Tuple[ParentCreated], store.fetch(parent_id1)
    )
    assert len(fetched_items) == 11
    assert fetched_items[0].children.name == "Group1"
    assert fetched_items[1].children.name == "Group2"
    assert fetched_items[2].children.name == "Group3"
    assert fetched_items[3].children.name == "Group4"
    assert fetched_items[4].children.name == "Group5"
    assert fetched_items[5].children.name == "Group6"
    assert fetched_items[6].children.name == "Group7"
    assert fetched_items[7].children.name == "Group8"
    assert fetched_items[8].children.name == "Group9"
    assert fetched_items[9].children.name == "Group10"
    assert fetched_items[10].children.name == "Group11"
