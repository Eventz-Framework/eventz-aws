from eventz_aws.event_store_dynamodb import EventStoreDynamodb
from tests.conftest import dynamodb_events_table_name, parent_id1


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
