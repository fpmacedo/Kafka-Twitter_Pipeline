import configparser
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

topic_name = "netflix"

for i in range(1, 100):
  producer.send("t1", value={"hello" : i})
  sleep(0.001)