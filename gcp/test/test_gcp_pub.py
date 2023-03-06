from google.cloud import pubsub_v1

project_id = "692233547485"
topic_id = "luno_topic_full_fat"

publisher_options = pubsub_v1.types.PublisherOptions(
    enable_message_ordering=True
)
publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
# The `topic_path` method creates a fully qualified identifier in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

for n in range(1, 10):
    data_str = f"Hello World - Message number {n}"
    # Data must be a bytestring
    data = data_str.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic=topic_path, data=data, ordering_key="luno")
    print(future.result())

print(f"Published messages to {topic_path}.")
