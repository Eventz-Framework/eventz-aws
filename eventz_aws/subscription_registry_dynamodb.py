import logging
import os
from datetime import datetime, timezone
from typing import Optional, Tuple

from boto3_type_annotations.dynamodb import Client as DynamoClient
from eventz.protocols import SubscriptionRegistryProtocol

log = logging.getLogger(__name__)
log.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class SubscriptionRegistryDynamodb(SubscriptionRegistryProtocol[str]):
    def __init__(self, connection: DynamoClient, table_name: str):
        self._connection: DynamoClient = connection
        self._table_name: str = table_name

    def register(
        self, aggregate_id: str, subscription: str, time: Optional[datetime] = None
    ) -> None:
        time = time or datetime.now(timezone.utc)
        log.info(
            f"SubscriptionRegistryDynamodb.register with game_id={aggregate_id} "
            f"subscription={subscription} time={time}"
        )
        self._connection.put_item(
            TableName=self._table_name,
            Item={
                "pk": {"S": f"subscription-{aggregate_id}"},
                "sk": {"S": subscription},
                "datetime": {"S": time.isoformat()},
            },
        )
        log.info("Data put to dynamodb without error.")

    def deregister(
        self, subscription: str
    ) -> None:
        log.info(
            f"SubscriptionRegistryDynamodb.deregister with subscription={subscription=}"
        )
        log.info(
            f"Querying index=sk-index with sk={subscription}"
        )
        response = self._connection.query(
            TableName=self._table_name,
            IndexName='sk-index',
            KeyConditionExpression="sk = :sk",
            ExpressionAttributeValues={
                ":sk": {"S": subscription}
            },
            ScanIndexForward=False,
        )
        log.info(
            f"Response obtained was {response=}"
        )
        for item in response["Items"]:
            log.info(f"Deleting subscription with {item=}")
            self._connection.delete_item(
                TableName=self._table_name,
                Key={
                    "pk": {"S": item["pk"]["S"]},
                    "sk": {"S": item["sk"]["S"]},
                },
            )
            log.info("Data deleted from dynamodb without error.")

    def fetch(self, aggregate_id: str) -> Tuple[str]:
        log.info(
            f"SubscriptionRegistryDynamodb.fetch with game_id={aggregate_id}"
        )
        response = self._connection.query(
            TableName=self._table_name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={
                ":pk": {"S": f"subscription-{aggregate_id}"}
            },
            ScanIndexForward=False,
            ConsistentRead=True,
        )
        log.info(f"Query response:")
        log.info(response)
        sorted_items = sorted(response["Items"], key=lambda i: i["datetime"]["S"])
        return tuple(str(item["sk"]["S"]) for item in sorted_items)
