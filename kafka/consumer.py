import time
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka import DeserializingConsumer

import json

# Parse the command line.
parser = ArgumentParser()
parser.add_argument('config_file', type=FileType('r'))
parser.add_argument('--reset', action='store_true')
args = parser.parse_args()


# Define the deserialization function
def deserializer(value, key):
    return json.loads(value.decode())


# Parse the configuration.
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
config_parser = ConfigParser()
config_parser.read_file(args.config_file)
config = dict(config_parser['default'])
config.update(config_parser['consumer'])
config.update({"value.deserializer": deserializer})
# Create Consumer instance
consumer = DeserializingConsumer(config)


# Set up a callback to handle the '--reset' flag.
def reset_offset(consumer, partitions):
    if args.reset:
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)


# Subscribe to topic
topic = "twitter"
consumer.subscribe([topic], on_assign=reset_offset)

# Poll for new messages from Kafka and print them.
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            print("Waiting...")
        elif msg.error():
            print("ERROR: %s".format(msg.error()))
        else:
            # Extract the (optional) key and value, and print.
            if msg.value()['truncated'] == True:
                print(msg.value()['extended_tweet']['full_text'])
                print("-------------------------")
            else:
                print(msg.value()['text'])
                print("-------------------------")
        time.sleep(2)
except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    consumer.close()