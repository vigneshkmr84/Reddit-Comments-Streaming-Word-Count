import os
import sys
import argparse
import praw
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--kafka-topic"
                        , help="Kafka topic to push messages"
                        , type=str
                        , required=True)

    parser.add_argument("--bootstrap-server"
                        , help="Kafka bootstrap server"
                        , type=str
                        , required=True)

    parser.add_argument("--subreddit"
                        , help="Subreddit to stream comments from"
                        , type=str
                        , default="AskReddit")
    
    parser.add_argument("--user-name"
                        , help="Reddit user name"
                        , type=str)
    
    parser.add_argument("--password"
                        , help="Reddit password"
                        , type=str)
    
    parser.add_argument("--client-id"
                        , help="Reddit Client Id"
                        , type=str)
    
    parser.add_argument("--secret"
                        , help="Reddit Client Secret"
                        , type=str)

    args = parser.parse_args()
    
    if os.path.isfile(".env"):
        print('Loading secrets from .env file')
        load_dotenv()
        args.user_name = os.getenv("user_name")
        args.password = os.getenv("password")
        args.client_id = os.getenv("client_id")
        args.secret = os.getenv("secret")
    
    if args.user_name is None or args.password is None or args.client_id is None or args.secret is None:
        print('Missing Reddit secret. Please pass user_name, password, client_id and secret in .env file or via CLI arguments.')
        sys.exit(1)
    
    return args

# Function to push message from reddit to Kafka topic
def push_to_kafka(data, bootstrap_server, kafka_topic):
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server])

    producer.send(kafka_topic, str.encode(data))
    producer.flush()
    producer.close()

# Using praw, stream comments from subreddit & push message to kafka topic
def process_comments_from_reddit(client_id, secret, user_name, password, bootstrap_server, kafka_topic, subreddit):
    reddit = praw.Reddit(client_id = client_id,
                        client_secret= secret,
                        username = user_name,
                        user_agent = 'project for bigdata spark streaming',
                        password = password)

    sub_reddit = reddit.subreddit(subreddit)
    # keep all new comments from all sub-reddit
    for comment in sub_reddit.stream.comments(skip_existing=True):
        print(comment.body)
        push_to_kafka(comment.body, bootstrap_server, kafka_topic)

# Retrieve reddit token with client_id, secret, user_name & password
def get_reddit_token(client_id, secret, user_name, password):
    try:
        auth = requests.auth.HTTPBasicAuth(client_id, secret)
        data = {'grant_type': 'password', 'username': user_name, 'password': password}

        headers = {'User-Agent': 'project for bigdata spark streaming'}
        end_point = 'https://www.reddit.com/api/v1/access_token'
        res = requests.post(end_point, headers=headers, data=data, auth=auth, timeout=20)
        if res.status_code != 200:
            return None
        return res.json()["access_token"]
    except Exception as login_exception:
        print("Login Exception : " + str(login_exception))
        return None


if __name__ == '__main__':

    args = parse_args()
    bootstrap_server = args.bootstrap_server
    topic = args.kafka_topic
    subreddit = args.subreddit

    load_dotenv()
    user_name = os.getenv("user_name")
    password = os.getenv("password")
    client_id = os.getenv("client_id")
    secret = os.getenv("secret")
    token = get_reddit_token(client_id, secret, user_name, password)

    if token:
        process_comments_from_reddit(client_id, secret, user_name, password, bootstrap_server, topic, subreddit)
    else:
        print("Error in retreiving token.")
        sys.exit(1)
