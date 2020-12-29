import logging
import os
from datetime import datetime, timezone
from typing import Optional, Tuple

from boto3_type_annotations.dynamodb import Client as DynamoClient
from eventz.protocols import SubscriptionRegistryProtocol

log = logging.getLogger(__name__)
log.setLevel(os.getenv("LOG_LEVEL", "DEBUG"))


class SubscriptionRegistryDynamodb(SubscriptionRegistryProtocol[str]):
    def __init__(self, connection: DynamoClient, table_name: str):
        self._connection: DynamoClient = connection
        self._table_name: str = table_name

    def register(
        self, game_id: str, subscription: str, time: Optional[datetime] = None
    ) -> None:
        time = time or datetime.now(timezone.utc)
        log.debug(
            f"SubscriptionRegistryDynamodb.register with game_id={game_id} "
            f"subscription={subscription} time={time}"
        )
        self._connection.put_item(
            TableName=self._table_name,
            Item={
                "pk": {"S": f"subscription-{game_id}"},
                "sk": {"S": subscription},
                "datetime": {"S": time.isoformat()},
            },
        )
        log.debug("Data put to dynamodb without error.")

    def fetch(self, game_id: str) -> Tuple[str]:
        log.debug(
            f"SubscriptionRegistryDynamodb.fetch with game_id={game_id}"
        )
        response = self._connection.query(
            TableName=self._table_name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={
                ":pk": {"S": f"subscription-{game_id}"}
            },
            ScanIndexForward=False,
            ConsistentRead=True,
        )
        log.debug(f"Query response:")
        log.debug(response)
        sorted_items = sorted(response["Items"], key=lambda i: i["datetime"]["S"])
        return tuple(str(item["sk"]["S"]) for item in sorted_items)
