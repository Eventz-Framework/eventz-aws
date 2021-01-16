import logging
import os
from typing import Optional, Sequence, Tuple

import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError
from eventz.event_store import EventStore
from eventz.messages import Event
from eventz.protocols import Events, MarshallProtocol, EventStoreProtocol

log = logging.getLogger(__name__)
log.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class EventStoreJsonS3(EventStore, EventStoreProtocol):
    def __init__(
        self,
        bucket_name: str,
        region: str,
        marshall: MarshallProtocol,
        recreate_storage: bool = True,
    ):
        log.info((
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
            log.info("Bucket does not exist.")
            self._resource.create_bucket(
                Bucket=self._bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
            log.info("Bucket created without error.")
        elif bucket_exists and recreate_storage:
            log.info("Bucket exists.")
            bucket = self._resource.Bucket(self._bucket_name)
            bucket.objects.all().delete()
            log.info("Bucket contents deleted.")
            bucket.delete()
            log.info("Bucket deleted.")
            self._resource.create_bucket(
                Bucket=self._bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
            log.info("Bucket created without error.")

    def fetch(self, aggregate_id: str, seq: Optional[int] = None) -> Events:
        log.info(f"EventStoreJsonS3.fetch with aggregate_id={aggregate_id}")
        try:
            obj = self._client.get_object(Bucket=self._bucket_name, Key=aggregate_id)
            log.info(f"Bucket object obtained: {obj}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return ()
            raise e
        json_string = obj.get("Body").read().decode("utf-8")
        log.info(f"JSON read:")
        log.info(json_string)
        deserialized_json_data = self._marshall.from_json(json_string)
        log.info(f"deserialized_json_data:")
        log.info(deserialized_json_data)
        events = [
            e.sequence(idx + 1)
            for idx, e in enumerate(deserialized_json_data)
        ]
        slice_idx = self._get_slice_index(seq)
        return tuple(
            events[slice_idx:]
        )

    def _get_slice_index(self, seq: Optional[int]) -> int:
        slice_index = 0 if seq is None else seq - 1
        if slice_index < 0:
            return 0
        return slice_index

    def persist(self, aggregate_id: str, events: Events) -> Events:
        log.info(f"EventStoreJsonS3.persist with aggregate_id={aggregate_id} events:")
        log.info(events)
        existing_events = self.fetch(aggregate_id)
        log.info(f"existing_events:")
        log.info(existing_events)
        seq = len(existing_events)
        new_events = [
            e.sequence(seq + idx + 1)
            for idx, e in enumerate(events)
        ]
        json_string = self._marshall.to_json(existing_events + tuple(new_events))
        log.info(f"Combined event data as JSON:")
        log.info(json_string)
        self._client.put_object(
            Bucket=self._bucket_name, Key=aggregate_id, Body=json_string
        )
        log.info("Data put to bucket without error.")
        return tuple(new_events)

    def _bucket_exists(self) -> bool:
        try:
            self._resource.meta.client.head_bucket(Bucket=self._bucket_name)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise e
            return False
