import configparser
import tweepy

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


#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        print(status.text)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

print(myStream.filter(track=['python']))

