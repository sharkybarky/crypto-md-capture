from google.cloud.pubsublite.cloudpubsub import PublisherClient
from google.cloud.pubsublite.types import (
    CloudRegion,
    CloudZone,
    MessageMetadata,
    TopicPath,
)

project_number = 692233547485
cloud_region = "europe-west2"
zone_id = "a"
topic_id = "luno_topic"
regional = False

if regional:
    location = CloudRegion(cloud_region)
else:
    location = CloudZone(CloudRegion(cloud_region), zone_id)

topic_path = TopicPath(project_number, location, topic_id)

# PublisherClient() must be used in a `with` block or have __enter__() called before use.
with PublisherClient() as publisher_client:
    data = "Hello world!"
    api_future = publisher_client.publish(topic_path, data.encode("utf-8"))
    # result() blocks. To resolve API futures asynchronously, use add_done_callback().
    message_id = api_future.result()
    # message_id = api_future.add_done_callback()
    message_metadata = MessageMetadata.decode(message_id)
    print(
        f"Published a message to {topic_path} with partition {message_metadata.partition.value} and offset {message_metadata.cursor.offset}."
    )
