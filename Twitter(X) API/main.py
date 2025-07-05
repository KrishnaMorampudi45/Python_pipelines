import tweepy
import configparser
from database import conn

token=configparser.ConfigParser(interpolation=None)
token.read('config.ini')
bearer_token = token['twitter']['bearer']

client = tweepy.Client(bearer_token=bearer_token)

username = "PhaniRo45"


user = client.get_user(username=username)
user_id = user.data.id
fetch=conn()
cursor1=fetch.cursor()
cursor1.execute("SELECT MAX(id) FROM tweets")
last_id = cursor1.fetchone()[0]
tweets = client.get_users_tweets(id=user_id,since_id=last_id, max_results=5,tweet_fields=['created_at'])

conn=conn()
cursor=conn.cursor()

for tweet in tweets.data:
    tweet_id=tweet.id
    tweet_text=tweet.text
    tweet_date=tweet.created_at

    cursor.execute("insert into tweets (id,content,post_time) values (?,?,?)",tweet_id,tweet_text,tweet_date)
conn.commit()
conn.close()
print("twets updated")


