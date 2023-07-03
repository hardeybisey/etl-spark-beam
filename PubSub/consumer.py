import os
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

# credentials_path = ""
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

PROJECT_ID = "triple-baton-382908"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, "my-sub")


def callback(message):
    print("data: {}".format(message.data))
    
    if message.attributes:
        print("attributes:")
        for key in message.attributes:
            value = message.attributes.get(key)
            print("{}: {}".format(key, value))
    message.ack()
    
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print("Listening for messages on {}..\n".format(subscription_path))

with subscriber:
    try:
        streaming_pull_future.result(timeout=5)
    except TimeoutError:
        streaming_pull_future.cancel()