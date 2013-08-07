#!/usr/bin/env python
from maxtweety.tasks import StreamWatcherListener

import os
import sys
import argparse
import ConfigParser

import pymongo
import tweepy

import logging

# CONFIG
max_server_url = 'https://max.upc.edu'
# max_server_url = 'https://sneridagh.upc.es'

twitter_generator_name = 'Twitter'
debug_hashtag = 'debugmaxupcnet'
logging_file = '/var/pyramid/maxserver/var/log/twitter-listener.log'
if not os.path.exists(logging_file):  # pragma: no cover
    logging_file = '/tmp/twitter-listener.log'
logger = logging.getLogger("tweeterlistener")
fh = logging.FileHandler(logging_file, encoding="utf-8")
formatter = logging.Formatter('%(asctime)s %(message)s')
logger.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)


def main(argv=sys.argv, quiet=False):  # pragma: no cover
    # command = MaxTwitterRulesRunnerTest(argv, quiet)
    command = MaxTwitterListenerRunner(argv, quiet)
    return command.run()


class MaxTwitterListenerRunner(object):  # pragma: no cover
    verbosity = 1  # required
    description = "Max rules runner."
    usage = "usage: %prog [options]"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c', '--config',
                      dest='configfile',
                      type=str,
                      help=("Configuration file"))

    def __init__(self, argv, quiet=False):
        self.quiet = quiet
        self.options = self.parser.parse_args()
        self.config = ConfigParser.ConfigParser()
        self.config.read(self.options.configfile)

        if not self.options.configfile:
            logging.error('You must provide a valid configuration .ini file.')
            sys.exit(1)

        try:
            self.consumer_key = self.config.get('twitter', 'consumer_key')
            self.consumer_secret = self.config.get('twitter', 'consumer_secret')
            self.access_token = self.config.get('twitter', 'access_token')
            self.access_token_secret = self.config.get('twitter', 'access_token_secret')

        except:
            logging.error('You must provide a valid configuration .ini file.')
            sys.exit(1)

    def get_twitter_enabled_contexts(self):
        req = requests.get('{}/contexts'.format())


    def run(self):

        contexts_with_twitter_username = db.contexts.find({"twitterUsernameId": {"$exists": True}})
        follow_list = [users_to_follow.get('twitterUsernameId') for users_to_follow in contexts_with_twitter_username]
        contexts_with_twitter_username.rewind()
        readable_follow_list = [users_to_follow.get('twitterUsername') for users_to_follow in contexts_with_twitter_username]

        # Prompt for login credentials and setup stream object
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)

        # auth = tweepy.auth.BasicAuthHandler(self.options.username, self.options.password)
        stream = tweepy.Stream(auth, StreamWatcherListener(), timeout=None)

        # Hardcoded global hashtag(s)
        track_list = ['#upc', '#%s' % debug_hashtag]

        logging.warning("Listening to this Twitter hashtags: %s" % str(track_list))
        logging.warning("Listening to this Twitter userIds: %s" % str(readable_follow_list))

        stream.filter(follow=follow_list, track=track_list)


# For testing purposes only
class MaxTwitterRulesRunnerTest(object):  # pragma: no cover
    verbosity = 1  # required
    description = "Max rules runner."
    usage = "usage: %prog [options]"
    parser = argparse.ArgumentParser(usage, description=description)
    parser.add_argument('-c', '--config',
                      dest='configfile',
                      type=str,
                      help=("Configuration file"))

    def __init__(self, argv, quiet=False):
        self.quiet = quiet
        self.options = self.parser.parse_args()

        logging.warning("Running first time!")

    def run(self):
        while True:
            import time
            time.sleep(2)
            from maxrules.tasks import processTweet
            processTweet('sneridagh', u'Twitejant com un usuari de twitter assignat a un contexte')
            time.sleep(2)
            processTweet('maxupcnet', u'Twitejant amb el hashtag #upc #gsxf')

if __name__ == '__main__':  # pragma: no cover
    try:
        main()
    except KeyboardInterrupt:
        logging.warning('\nGoodbye!')
