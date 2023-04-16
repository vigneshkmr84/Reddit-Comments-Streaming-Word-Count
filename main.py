import os
import praw
import requests
from dotenv import load_dotenv
import json
from kafka import KafkaProducer
import time


def push_to_kafka(data):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    producer.send('reddit-comments', str.encode(data))
    producer.flush()
    producer.close()
    print('Pushed')

# default get all details from
def get_comments_from_reddit(client_id, secret, user_name, password):
    reddit = praw.Reddit(client_id = client_id,
                         client_secret= secret, 
                         username = user_name, 
                         user_agent = 'project for bigdata spark streaming',
                         password = password)

    sub_reddit = reddit.subreddit("all")
    # keep all new comments from all sub-reddit
    for comment in sub_reddit.stream.comments(skip_existing=True):
        print(comment.body)
        push_to_kafka(comment.body)
        # time.sleep(3)


def get_reddit_token(client_id, secret, user_name, password):
    try:
        auth = requests.auth.HTTPBasicAuth(client_id, secret)
        # print(user_name, password, client_id, secret)

        data = {'grant_type': 'password', 'username': user_name, 'password': password}

        headers = {'User-Agent': 'project for bigdata spark streaming'}
        end_point = 'https://www.reddit.com/api/v1/access_token'
        res = requests.post(end_point, headers=headers, data=data, auth=auth)
        if res.status_code != 200:
            return None
        else:
            return res.json()["access_token"]
    except Exception as ex:
        print("Login Exception")
        return None
    


if __name__ == '__main__':
    load_dotenv()
    user_name = os.getenv("user_name")
    password = os.getenv("password")
    client_id = os.getenv("client_id")
    secret = os.getenv("secret")
    token = get_reddit_token(client_id, secret, user_name, password)
    print("Token ", token)
    get_comments_from_reddit(client_id, secret, user_name, password)
