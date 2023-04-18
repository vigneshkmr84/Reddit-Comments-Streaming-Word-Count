import os
import sys
# import time
import praw
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer


# Function to push message from reddit to Kafka topic
def push_to_kafka(data, bootstrap_server, kafka_topic):
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server])

    producer.send(kafka_topic, str.encode(data))
    producer.flush()
    producer.close()

# Using praw, stream comments from subreddit & push message to kafka topic
def process_comments_from_reddit(client_id, secret, user_name, password, bootstrap_server, kafka_topic):
    reddit = praw.Reddit(client_id = client_id,
                        client_secret= secret,
                        username = user_name,
                        user_agent = 'project for bigdata spark streaming',
                        password = password)

    sub_reddit = reddit.subreddit("all")
    # keep all new comments from all sub-reddit
    for comment in sub_reddit.stream.comments(skip_existing=True):
        print(comment.body)
        push_to_kafka(comment.body, bootstrap_server, kafka_topic)
        #time.sleep(3)

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
        print("Login Exception : " + login_exception)
        return None
    

if __name__ == '__main__':
    args = sys.argv
    TOPIC = args[1]
    bootstrap_server = args[2]

    load_dotenv()
    user_name = os.getenv("user_name")
    password = os.getenv("password")
    client_id = os.getenv("client_id")
    secret = os.getenv("secret")
    token = get_reddit_token(client_id, secret, user_name, password)
    if token:
        # print("Token ", token)
        process_comments_from_reddit(client_id, secret, user_name, password, bootstrap_server, TOPIC)
    else:
        print("Error in retreiving token.")
        sys.exit(1)
