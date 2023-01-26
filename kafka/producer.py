import time

import tweepy
from confluent_kafka import Producer
import logging
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

# Parse the command line.
parser = ArgumentParser()
parser.add_argument('config_file', type=FileType('r'))
args = parser.parse_args()

config_parser = ConfigParser()
config_parser.read_file(args.config_file)
config = dict(config_parser['default'])

# Twitter API credentials
consumerKey = ''
consumerSecret = ''
accessToken = ''
accessTokenSecret = ''

# Kafka producer
producer = Producer(config)
search_term = 'Bitcoin'
topic_name = 'twitter'


def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


def twitterAuth():
    # create the authentication object
    authenticate = tweepy.OAuthHandler(consumerKey, consumerSecret)
    # set the access token and the access token secret
    authenticate.set_access_token(accessToken, accessTokenSecret)
    # create the API object
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    return api


class TweetListener(tweepy.Stream):

    def on_data(self, raw_data):
        print("Producing data to Kafka -----------------------------------")
        logging.info(raw_data)
        producer.produce(topic_name, value=raw_data, callback=delivery_callback)
        time.sleep(2)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

    def start_streaming_tweets(self, search_term):
        self.filter(track=search_term, stall_warnings=True, languages=["en"])


if __name__ == '__main__':
    twitter_stream = TweetListener(consumerKey, consumerSecret, accessToken, accessTokenSecret)
    twitter_stream.start_streaming_tweets(search_term)
