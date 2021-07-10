import configparser
import tweepy

from datetime import datetime
from kafka import KafkaProducer
from json import dumps




def twitter_auth(config):

  """ Get the keys and tokens from cfg file and create a twitter
      API authentication using tweepy.
      
      Parameters:
        - config: the configurations from .cfg file
      Returns:
        - api: the API authenticated metod
  """

  #setting up twitter variables
  CONSUMER_KEY = config.get('CONSUMER','CONSUMER_KEY')
  CONSUMER_SECRET = config.get('CONSUMER','CONSUMER_SECRET')
  ACCESS_TOKEN = config.get('ACCESS','ACCESS_TOKEN')
  ACCESS_TOKEN_SCRET = config.get('ACCESS','ACCESS_TOKEN_SCRET')

  #setting up twitter auth
  auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
  auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SCRET)
  api = tweepy.API(auth)

  return api



def kafka_producer(bootstrap_server):
  """ Create a Kafka producer serializing the data in JSON UTF-8.
      
      Parameters:
        - bootstrap_server: the Kafka bootstrap server adress
      Returns:
        - producer: the Kafka Producer
  """

  producer = KafkaProducer(
  value_serializer = lambda m: dumps(m).encode("utf-8"),
  bootstrap_servers=[bootstrap_server])

  return producer



def start_streaming(producer, api, word):

  """ Create a streaming process using the passed word.
      
      Parameters:
        - producer: the Kafka Producer;
        - api: the API authenticated metod;
        - word: the word to filter the tweets
  """

  class MyStreamListener(tweepy.StreamListener):
    
    # Streaming API. Streaming API fetches live tweets
    def on_status(self, status):

        #Print user info
        print("User Info:")
        print(status.user.id)
        print(status.user.name)
        print(status.user.verified)
        print(status.user.followers_count)
        print(status.user.friends_count)
        print(datetime.timestamp(status.user.created_at))
        #Print tweet info
        print("Tweet Info:")
        print(status.id)
        print(status.text)
        print(datetime.timestamp(status.created_at))
        #print(status.entities)
        print("-------------------------------------------------------------")

        producer.send("twitter",
         value={
                "user_id" : status.user.id,
                "user_name" : status.user.name,
                "screen_name": status.user.screen_name,
                "verified" : status.user.verified,
                "followers_count" : status.user.followers_count,
                "friends_count" : status.user.friends_count,
                "user_created_at" : datetime.timestamp(status.user.created_at),
                "tweet_id" : status.id,
                "tweet_message" : status.text,
                "tweet_created_at" : datetime.timestamp(status.created_at)
                 })
        
    # To print the status if an error happens
    def on_error(self,status):
        print(status)  

  #create the twitter stream listener
  myStreamListener = MyStreamListener()
  myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
  #select the word to filter the tweets
  myStream.filter(track=[word])


def main():

  #reading configuration file
  config = configparser.ConfigParser()
  config.read_file(open('twitter.cfg'))

  #twitter authentication
  api = twitter_auth(config)
  
  #create a kafka producer
  producer = kafka_producer('localhost:29092')

  #start the streaming process
  start_streaming(producer ,api , 'python')


if __name__ == "__main__":
    main()