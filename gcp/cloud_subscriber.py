from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import logging

from utils.utils import setup_logging

setup_logging()
log = logging.getLogger(__name__)


project_id = "692233547485"
subscription_id = "luno_test_subscription"
# Number of seconds the subscriber should listen for messages
# timeout = 5.0

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier in the form
# `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    message_data = message.data.decode("utf-8")
    log.info(f"Received {message_data=}, {message.ordering_key=}.")
    message.ack()


streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
log.info(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely, unless an exception is encountered first.
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.



