import logging
import os
from typing import Sequence, Tuple

import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError
from eventz.event_store import EventStore
from eventz.messages import Event
from eventz.protocols import MarshallProtocol, EventStoreProtocol

log = logging.getLogger(__name__)
log.setLevel(os.getenv("LOG_LEVEL", "DEBUG"))


class EventStoreJsonS3(EventStore, EventStoreProtocol):
    def __init__(
        self,
        bucket_name: str,
        region: str,
        marshall: MarshallProtocol,
        recreate_storage: bool = True,
    ):
        log.debug((
            f"EventStoreJsonS3.init bucket_name={bucket_name}, "
            f"region={region}, marshall={marshall} recreate_storage={recreate_storage}"
        ))
        self._bucket_name: str = bucket_name
        client_config = Config(region_name=region,)
        self._resource = boto3.resource("s3", region_name=region, config=client_config,)
        self._client = boto3.client("s3", region_name=region, config=client_config,)
        self._marshall = marshall
        bucket_exists = self._bucket_exists()
        if not bucket_exists:
            log.debug("Bucket does not exist.")
            self._resource.create_bucket(
                Bucket=self._bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
            log.debug("Bucket created without error.")
        elif bucket_exists and recreate_storage:
            log.debug("Bucket exists.")
            bucket = self._resource.Bucket(self._bucket_name)
            bucket.objects.all().delete()
            log.debug("Bucket contents deleted.")
            bucket.delete()
            log.debug("Bucket deleted.")
            self._resource.create_bucket(
                Bucket=self._bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
            log.debug("Bucket created without error.")

    def fetch(self, aggregate_id: str) -> Tuple[Event, ...]:
        log.debug(f"EventStoreJsonS3.fetch with aggregate_id={aggregate_id}")
        try:
            obj = self._client.get_object(Bucket=self._bucket_name, Key=aggregate_id)
            log.debug(f"Bucket object obtained: {obj}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return ()
            raise e
        json_string = obj.get("Body").read().decode("utf-8")
        log.debug(f"JSON read:")
        log.debug(json_string)
        deserialized_json_data = self._marshall.from_json(json_string)
        log.debug(f"deserialized_json_data:")
        log.debug(deserialized_json_data)
        return tuple(deserialized_json_data)

    def persist(self, aggregate_id: str, events: Sequence[Event]) -> None:
        log.debug(f"EventStoreJsonS3.persist with aggregate_id={aggregate_id} events:")
        log.debug(events)
        existing_events = self.fetch(aggregate_id)
        log.debug(f"existing_events:")
        log.debug(existing_events)
        json_string = self._marshall.to_json(existing_events + tuple(events))
        log.debug(f"Combined event data as JSON:")
        log.debug(json_string)
        self._client.put_object(
            Bucket=self._bucket_name, Key=aggregate_id, Body=json_string
        )
        log.debug("Data put to bucket without error.")

    def _bucket_exists(self) -> bool:
        try:
            self._resource.meta.client.head_bucket(Bucket=self._bucket_name)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise e
            return False
