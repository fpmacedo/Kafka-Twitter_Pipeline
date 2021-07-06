import configparser
import tweepy

from time import sleep
from kafka import KafkaProducer
from json import dumps



#reading configuration file
config = configparser.ConfigParser()
config.read_file(open('twitter.cfg'))

#setting up twitter variables
CONSUMER_KEY = config.get('CONSUMER','CONSUMER_KEY')
CONSUMER_SECRET = config.get('CONSUMER','CONSUMER_SECRET')
ACCESS_TOKEN = config.get('ACCESS','ACCESS_TOKEN')
ACCESS_TOKEN_SCRET = config.get('ACCESS','ACCESS_TOKEN_SCRET')


#setting up twitter auth
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SCRET)
api = tweepy.API(auth)


public_tweets = api.home_timeline()

producer = KafkaProducer(
  value_serializer = lambda m: dumps(m).encode("utf-8"),
  bootstrap_servers=['localhost:29092'])

class MyStreamListener(tweepy.StreamListener):

    # Streaming API. Streaming API fetches live tweets
    def on_status(self, status):
        print(status.id)
        print(status.text)
        producer.send("twitter", value={"id" : status.id ,"text" : status.text })
     
     # To print the status if an error happens
    def on_error(self,status):
        print(status)


myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
myStream.filter(track=['python'])
