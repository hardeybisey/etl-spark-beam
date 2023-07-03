import os
from google.cloud import pubsub_v1

# credentials_path = ""
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path


PROJECT_ID = "triple-baton-382908"
producer = pubsub_v1.PublisherClient()
topic_path = producer.topic_path(PROJECT_ID, "my-topic")

i = 0
while True:
    data = "Message number {}".format(i)
    data = data.encode("utf-8")
    attributes = {"origin": "python-sample"}
    future = producer.publish(topic_path, data=data, **attributes)
    print("published message with message ID {}".format(future.result()))