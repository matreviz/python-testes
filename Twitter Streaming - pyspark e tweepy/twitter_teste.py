import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

# Set up your credentials from http://apps.twitter.com
consumer_key    = 'vHoPSHjSmNcJ5PV0sgmanz46i'
consumer_secret = '1xcHTuHwO9LnFiTYNkzRdg2VbS4HwN4qPBrQ0Vg3StFsK6L5WJ'
access_token    = '1268705033337651200-iCmApCYwLefGws2aOIHyR0ZaM7f3sa'
access_secret   = '4UNU43yeevTrfC7yBdvmVfbe13nQqblARc4lc3JKz7ms3'

class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads( data )
          print(msg['text'].encode('utf-8'))
          self.client_socket.send((str(msg['text']) + "\n").encode('utf-8'))
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)

  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['champions'])

if __name__ == "__main__":
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # Create a socket object
  host = "127.0.0.1"     # Get local machine name
  port = 5555                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print("Received request from: " + str(addr))

  sendData(c)