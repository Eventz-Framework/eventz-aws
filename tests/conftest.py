from datetime import datetime, timezone

import boto3
import pytest
from eventz.aggregate import Aggregate
from eventz.codecs.datetime import Datetime
from eventz.marshall import FqnResolver, Marshall
from moto.dynamodb2 import mock_dynamodb2

from tests.example.child import Child
from tests.example.children import Children
from tests.example.parent import ParentCreated, ChildChosen

parent_id1 = Aggregate.make_id()
msgid1 = "11111111-1111-1111-1111-111111111111"
msgid2 = "22222222-2222-2222-2222-222222222222"
msgid3 = "33333333-3333-3333-3333-333333333333"
msgid4 = "44444444-4444-4444-4444-444444444444"
dt_iso1 = "2020-01-02T03:04:05.123Z"
dt_iso2 = "2020-01-02T03:04:06.123Z"
dt_iso3 = "2020-01-02T03:04:07.123Z"
dt_iso4 = "2020-01-02T03:04:08.123Z"
dt1 = datetime(2020, 1, 2, 3, 4, 5, 123000, tzinfo=timezone.utc)
dt2 = datetime(2020, 1, 2, 3, 4, 6, 123000, tzinfo=timezone.utc)
dt3 = datetime(2020, 1, 2, 3, 4, 7, 123000, tzinfo=timezone.utc)
dt4 = datetime(2020, 1, 2, 3, 4, 8, 123000, tzinfo=timezone.utc)
dynamodb_events_table_name = "Eventz"
dynamodb_subscriptions_table_name = "Subscriptions"


@pytest.fixture
def parent_created_event_1():
    return ParentCreated(
        aggregate_id=parent_id1,
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
        __seq__=None,
    )


@pytest.fixture
def child_chosen_event_1():
    return ChildChosen(
        aggregate_id=parent_id1,
        child=Child(name="Child Three"),
        __msgid__=msgid2,
        __timestamp__=dt2,
        __seq__=None,
    )


@pytest.fixture
def parent_created_event_2():
    return ParentCreated(
        aggregate_id=parent_id1,
        children=Children(
            name="Group Two",
            items=[
                Child(name="Child Four"),
                Child(name="Child Five"),
                Child(name="Child TSix"),
            ],
        ),
        __msgid__=msgid3,
        __timestamp__=dt3,
    )


@pytest.fixture
def child_chosen_event_2():
    return ChildChosen(
        aggregate_id=parent_id1,
        child=Child(name="Child Three"),
        __msgid__=msgid4,
        __timestamp__=dt4,
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
            "__seq__": 1,
            "aggregateId": parent_id1,
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
            "__seq__": 2,
            "aggregateId": parent_id1,
            "child": {"__fqn__": "tests.Child", "name": "Child Three",},
        },
    ]


@pytest.fixture()
def marshall():
    return Marshall(
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


@pytest.fixture()
def dynamodb_connection():
    with mock_dynamodb2():
        connection = boto3.client("dynamodb", region_name="eu-west-3")
        yield connection


@pytest.fixture()
def dynamodb_connection_with_empty_events_table(
    dynamodb_connection, json_events, marshall
):
    dynamodb_connection.create_table(
        TableName=dynamodb_events_table_name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield dynamodb_connection
    dynamodb_connection.delete_table(TableName=dynamodb_events_table_name,)


@pytest.fixture()
def dynamodb_connection_with_initial_events(
    dynamodb_connection_with_empty_events_table, json_events, marshall
):
    for idx, event in enumerate(json_events):
        dynamodb_connection_with_empty_events_table.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": dynamodb_events_table_name,
                        "Item": {
                            "pk": {"S": f"parent-{event['aggregateId']}"},
                            "sk": {"N": str(event['__seq__'])},
                            "msgid": {"S": event["__msgid__"]},
                            "timestamp": {"S": event['__timestamp__']['params']['timestamp']},
                            "event": {"S": marshall.to_json(event)},
                        },
                        "ConditionExpression": "attribute_not_exists(pk) AND attribute_not_exists(sk)",
                    }
                },
                {  # Ensure msgid is always unique in the table
                    "Put": {
                        "TableName": dynamodb_events_table_name,
                        "Item": {
                            "pk": {"S": f"msgid#{event['__msgid__']}"},
                            "sk": {"S": "1"},
                        },
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }
                }
            ]
        )
    yield dynamodb_connection_with_empty_events_table


@pytest.fixture()
def dynamodb_connection_with_empty_subscriptions_table(dynamodb_connection):
    dynamodb_connection.create_table(
        TableName=dynamodb_subscriptions_table_name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "sk-index",
                "KeySchema": [
                    {"AttributeName": "sk", "KeyType": "HASH"},
                ],
                "Projection": {
                    "ProjectionType": "KEYS_ONLY",
                },
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield dynamodb_connection
    dynamodb_connection.delete_table(TableName=dynamodb_subscriptions_table_name,)
