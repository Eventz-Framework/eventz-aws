import logging
import os
from typing import Optional, Sequence, Tuple

from boto3_type_annotations.dynamodb import Client as DynamoClient
from eventz.event_store import EventStore
from eventz.messages import Event
from eventz.protocols import Events, MarshallProtocol, EventStoreProtocol

log = logging.getLogger(__name__)
log.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class EventStoreDynamodb(EventStore, EventStoreProtocol):
    def __init__(
        self, aggregate: str, connection: DynamoClient, table_name: str, marshall: MarshallProtocol,
    ):
        log.info(
            f"EventStoreDynamodb.init aggregate={aggregate} connection={connection}, marshall={marshall}"
        )
        self._aggregate: str = aggregate
        self._connection: DynamoClient = connection
        self._table_name: str = table_name
        self._marshall: MarshallProtocol = marshall

    def fetch(self, aggregate_id: str, seq: Optional[int] = None) -> Tuple[Event, ...]:
        log.info(
            f"EventStoreDynamodb.fetch with aggregate={self._aggregate} aggregate_id={aggregate_id}"
        )
        key_condition_expression = "pk = :pk"
        expression_attribute_values = {":pk": {"S": f"{self._aggregate}-{aggregate_id}"}}
        if seq:
            key_condition_expression += " AND sk >= :sk"
            expression_attribute_values[":sk"] = {"N": f"{str(seq)}"}
        response = self._connection.query(
            TableName=self._table_name,
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ScanIndexForward=True,
            ConsistentRead=True,
        )
        log.info(f"Query response:")
        log.info(response)
        return tuple(
            self._marshall.from_json(item["event"]["S"]) for item in response["Items"]
        )

    def persist(self, aggregate_id: str, events: Events) -> Events:
        log.info(
            f"EventStoreDynamodb.persist with aggregate={self._aggregate} aggregate_id={aggregate_id} events:"
        )
        log.info(events)
        sk = self._get_current_sk(aggregate_id)
        log.info(f"Current sk is '{sk}'")
        new_events = []
        for event in events:
            sk += 1
            self._connection.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": self._table_name,
                            "Item": {
                                "pk": {"S": f"{self._aggregate}-{aggregate_id}"},
                                "sk": {"N": str(sk)},
                                "msgid": {"S": event.__msgid__},
                                "timestamp": {"S": str(event.__timestamp__)},
                                "event": {"S": self._marshall.to_json(event.sequence(sk))},
                            },
                            "ConditionExpression": "attribute_not_exists(pk) AND attribute_not_exists(sk)",
                        }
                    },
                    {   # Ensure msgid is always unique in the table
                        "Put": {
                            "TableName": self._table_name,
                            "Item": {
                                "pk": {"S": f"msgid#{event.__msgid__}"},
                                "sk": {"N": "1"},
                            },
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    }
                ]
            )
            new_events.append(event.sequence(sk))
        log.info("Data put to dynamodb without error.")
        return tuple(new_events)

    def _get_current_sk(self, aggregate_id: str) -> int:
        log.info(
            f"EventStoreDynamodb._get_next_sk with aggregate={self._aggregate} aggregate_id={aggregate_id}"
        )
        response = self._connection.query(
            TableName=self._table_name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={
                ":pk": {"S": f"{self._aggregate}-{aggregate_id}"}
            },
            ScanIndexForward=False,
            ConsistentRead=True,
            Limit=1
        )
        if not response["Items"]:
            return 0  # first event
        return int(response["Items"][0]["sk"]["N"])
