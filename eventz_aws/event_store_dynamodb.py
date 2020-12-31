import logging
import os
from typing import Optional, Sequence, Tuple

from boto3_type_annotations.dynamodb import Client as DynamoClient
from eventz.event_store import EventStore
from eventz.messages import Event
from eventz.protocols import MarshallProtocol, EventStoreProtocol

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

    def fetch(self, aggregate_id: str, msgid: Optional[str] = None) -> Tuple[Event, ...]:
        log.info(
            f"EventStoreDynamodb.fetch with aggregate={self._aggregate} aggregate_id={aggregate_id}"
        )
        response = self._connection.query(
            TableName=self._table_name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={
                ":pk": {"S": f"{self._aggregate}-{aggregate_id}"}
            },
            ScanIndexForward=True,
            ConsistentRead=True,
        )
        log.info(f"Query response:")
        log.info(response)
        return tuple(
            self._marshall.from_json(item["event"]["S"]) for item in response["Items"]
        )

    def persist(self, aggregate_id: str, events: Sequence[Event]) -> None:
        log.info(
            f"EventStoreDynamodb.persist with aggregate={self._aggregate} aggregate_id={aggregate_id} events:"
        )
        log.info(events)
        sk = self._get_current_sk(aggregate_id)
        log.info(f"Current sk is '{sk}'")
        for event in events:
            sk += 1
            self._connection.put_item(
                TableName=self._table_name,
                Item={
                    "pk": {"S": f"{self._aggregate}-{aggregate_id}"},
                    "sk": {"N": str(sk)},
                    "event": {"S": self._marshall.to_json(event)},
                    "msgid": {"S": event.__msgid__},  # @TODO ensure unique in the table
                },
                ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
            )
        log.info("Data put to dynamodb without error.")

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
