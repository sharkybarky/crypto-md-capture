import logging
from queue import Empty

from google.cloud.pubsublite.cloudpubsub import PublisherClient
from google.cloud.pubsublite.types import (
    CloudRegion,
    CloudZone,
    MessageMetadata,
    TopicPath,
)

from trade import Trade

log = logging.getLogger(__name__)


class GcpRePublisherLite:
    def __init__(self):
        self.project_number = 692233547485
        self.cloud_region = "europe-west2"
        self.zone_id = "a"
        self.topic_id = "luno_topic"
        regional = False

        if regional:
            self.location = CloudRegion(self.cloud_region)
        else:
            self.location = CloudZone(CloudRegion(self.cloud_region), self.zone_id)

        self.topic_path = TopicPath(self.project_number, self.location, self.topic_id)

    def consume_and_republish(self, trade_queue, shutdown_event):
        while not shutdown_event.is_set() or trade_queue.qsize() > 0:
            try:
                message: Trade = trade_queue.get(timeout=3)
                log.info(f"GcpRePublisher read message off queue: {message.to_json()=}, queue-size: {trade_queue.qsize()}")
            except Empty as e:
                # force a timeout so that the blocking call to queue.get() will jump out every so often and
                # re-evaluate the shutdown_event Event
                log.debug(f"GcpRePublisher consume loop continuing: {shutdown_event.is_set()=}")
                continue

            # start publishing JSON, move to protobuf later to make messages smaller?
            with PublisherClient() as publisher_client:
                api_future = publisher_client.publish(self.topic_path, message.to_json().encode("utf-8"))
                # result() blocks. To resolve API futures asynchronously, use add_done_callback().
                message_id = api_future.result()
                # message_id = api_future.add_done_callback()
                message_metadata = MessageMetadata.decode(message_id)
                log.info(f"Published a message to {self.topic_path} with partition {message_metadata.partition.value} "
                         f"and offset {message_metadata.cursor.offset}.")

        log.info(f"GcpRePublisherLite exiting")
