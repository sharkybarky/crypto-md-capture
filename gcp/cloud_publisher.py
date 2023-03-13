import logging
from queue import Empty

from google.cloud import pubsub_v1

from trade import Trade

log = logging.getLogger(__name__)


class GcpRePublisher:
    def __init__(self):
        self.project_id = "692233547485"
        self.topic_id = "luno_topic_full_fat"
        self.ordering_key = "luno"
        self.publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
        self.publisher = pubsub_v1.PublisherClient(publisher_options=self.publisher_options)
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

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
            api_future = self.publisher.publish(topic=self.topic_path,
                                                data=message.to_json().encode("utf-8"),
                                                ordering_key=self.ordering_key)
            # result() blocks, but this is okay as we're pulling off our internal buffer.
            # If we wanted to resolve API futures asynchronously, could use add_done_callback().
            message_id = api_future.result()
            # message_id = api_future.add_done_callback()
            log.info(f"Published a message to {self.topic_path} with ordering_key {self.ordering_key}, "
                     f"message_id = {message_id}")

        log.info(f"GcpRePublisher exiting")
