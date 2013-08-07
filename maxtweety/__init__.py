from maxtweety.listener import StreamWatcherListener
from maxtweety.utils import setup_logging

import os
import sys
import argparse
import ConfigParser
import json
import requests
import tweepy

import logging

# CONFIG
# max_server_url = 'https://max.upc.edu'
# max_server_url = 'https://sneridagh.upc.es'

# twitter_generator_name = 'Twitter'
debug_hashtag = u'debugmaxupcnet'
# logging_file = '/var/pyramid/maxserver/var/log/twitter-listener.log'
# if not os.path.exists(logging_file):  # pragma: no cover
#     logging_file = '/tmp/twitter-listener.log'
logger = logging.getLogger(__name__)
# fh = logging.FileHandler(logging_file, encoding="utf-8")
# formatter = logging.Formatter('%(asctime)s %(message)s')
# logger.setLevel(logging.DEBUG)
# fh.setFormatter(formatter)
# logger.addHandler(fh)


def main(argv=sys.argv, quiet=False):  # pragma: no cover
    # command = MaxTwitterRulesRunnerTest(argv, quiet)
    command = MaxTwitterListenerRunner(argv, quiet)
    return command.run()


class MaxTwitterListenerRunner(object):  # pragma: no cover
    verbosity = 1  # required
    description = "Max Twitter listener runner."
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

            self.maxservers_settings = [maxserver for maxserver in self.config.sections() if maxserver.startswith('max_')]

        except:
            logging.error('You must provide a valid configuration .ini file.')
            sys.exit(1)

    def get_twitter_enabled_contexts(self):
        contexts = {}
        for max_settings in self.maxservers_settings:
            max_url = self.config.get(max_settings, 'server')
            req = requests.get('{}/contexts'.format(max_url), headers=self.oauth2Header(self.restricted_username, self.restricted_token))
            context_follow_list = [users_to_follow.get('twitterUsernameId') for users_to_follow in req.json().get('items') if users_to_follow.get('twitterUsernameId')]
            context_readable_follow_list = [users_to_follow.get('twitterUsername') for users_to_follow in req.json().get('items') if users_to_follow.get('twitterUsername')]
            contexts.setdefault(max_settings, {})['ids'] = context_follow_list
            contexts[max_settings]['readable'] = context_readable_follow_list

        self.users_id_to_follow = contexts

    def flatten_users_id_to_follow(self):
        flat_list = []
        for maxserver in self.users_id_to_follow.keys():
            id_list = self.users_id_to_follow.get(maxserver).get('ids')
            flat_list = flat_list + id_list

        return flat_list

    def get_max_global_hashtags(self):
        self.global_hashtags = []
        for max_settings in self.maxservers_settings:
            self.global_hashtags.append(self.config.get(max_settings, 'hashtag'))

    def load_settings(self):
        settings_file = '{}/.max_restricted'.format(self.config.get('general', 'config_directory'))
        if os.path.exists(settings_file):
            settings = json.loads(open(settings_file).read())
        else:
            settings = {}

        if 'token' not in settings or 'username' not in settings:
            logger.info("Unable to load MAX settings, please execute init_maxpush script.")
            sys.exit(1)

        self.restricted_username = settings.get('username')
        self.restricted_token = settings.get('token')

    def oauth2Header(self, username, token, scope="widgetcli"):
        return {
            "X-Oauth-Token": token,
            "X-Oauth-Username": username,
            "X-Oauth-Scope": scope}

    def run(self):
        setup_logging(self.options.configfile)
        self.load_settings()
        self.get_max_global_hashtags()
        self.get_twitter_enabled_contexts()

        # Prompt for login credentials and setup stream object
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)

        # auth = tweepy.auth.BasicAuthHandler(self.options.username, self.options.password)
        stream = tweepy.Stream(auth, StreamWatcherListener(self.config.get('rabbitmq', 'server')), timeout=None)

        # Add the debug hashtag
        self.global_hashtags.append(debug_hashtag)

        logging.warning("Listening to this Twitter hashtags: \n{}".format(json.dumps(self.global_hashtags, indent=4, sort_keys=True)))
        logging.warning("Listening to this Twitter userIds: \n{}".format(json.dumps(self.users_id_to_follow, indent=4, sort_keys=True)))

        stream.filter(follow=self.flatten_users_id_to_follow(), track=self.global_hashtags)


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
