from maxtweety.listener import StreamWatcherListener
from maxtweety.utils import setup_logging

import argparse
import ConfigParser
import json
import logging
import multiprocessing
import os
import pika
import requests
import sys
import tweepy
import arrow
import time
import datetime

debug_hashtag = u'debugmaxupcnet'
logger = logging.getLogger('tweety')


class RestartClock(object):
    def __init__(self, seconds_between_restarts=10):
        self.reset()
        self.delay = seconds_between_restarts

    def reset(self):
        """
            Resets the timer to the current time
        """
        self.last = arrow.now()

    def ready(self):
        """
            Returns True if enough time has elapsed since last reset
        """
        elapsed = arrow.now() - self.last
        elapsed_seconds = elapsed.seconds
        return elapsed_seconds >= self.delay

    def remaining(self):
        """
            Time remaining to be ready again
        """
        elapsed = arrow.now() - self.last
        elapsed_seconds = elapsed.seconds
        time_to_wait = self.delay - elapsed_seconds
        return time_to_wait

    def wait(self):
        time.sleep(self.remaining())

    def in_time(self, restart_request):
        return restart_request is None or restart_request > self.last


def main(argv=sys.argv, quiet=False):  # pragma: no cover
    clock = RestartClock()
    tweety = MaxTwitterListenerRunner(argv, quiet)
    tweety.spawn_process()

    connection = pika.BlockingConnection(
        pika.URLParameters(tweety.config.get('rabbitmq', 'server'))
    )

    channel = connection.channel()

    logger.info('[*] Waiting for restart signal.')

    def callback(ch, method, properties, body):

        logger.info("[x] Received restart from MAX server")
        try:
            restart_request_time = arrow.get(datetime.datetime.utcfromtimestamp(float(body)))
        except:
            restart_request_time = None
        ch.basic_ack(delivery_tag=method.delivery_tag)

        if clock.in_time(restart_request_time):
            if not clock.ready():
                logger.info("[x] Delaying restart {} seconds".format(clock.remaining()))
                clock.wait()
            tweety.restart()
            logger.info("[x] Restarted")
            clock.reset()
        else:
            logger.info("[x] Discarding restart")

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback,
                          queue='tweety_restart')
    channel.start_consuming()


class MaxTwitterListenerRunner(object):  # pragma: no cover
    process = None

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

        setup_logging(self.options.configfile)

        try:
            self.consumer_key = self.config.get('twitter', 'consumer_key')
            self.consumer_secret = self.config.get('twitter', 'consumer_secret')
            self.access_token = self.config.get('twitter', 'access_token')
            self.access_token_secret = self.config.get('twitter', 'access_token_secret')

            self.maxservers_settings = [maxserver for maxserver in self.config.sections() if maxserver.startswith('max_')]

        except:
            logging.error('You must provide a valid configuration .ini file.')
            sys.exit(1)

    def spawn_process(self, *args, **kwargs):
        self.process = multiprocessing.Process(target=self.run, args=args, kwargs=kwargs)
        self.process.start()

    def restart(self):
        if self.process.is_alive():
            self.process.terminate()
            self.spawn_process()

    def get_twitter_enabled_contexts(self):
        contexts = {}
        for max_settings in self.maxservers_settings:
            max_url = self.config.get(max_settings, 'server')
            req = requests.get('{}/contexts'.format(max_url), params={"twitter_enabled": True}, headers=self.oauth2Header(self.restricted_users[max_settings]['username'], self.restricted_users[max_settings]['token']))
            context_follow_list = [users_to_follow.get('twitterUsernameId') for users_to_follow in req.json() if users_to_follow.get('twitterUsernameId')]
            context_readable_follow_list = [users_to_follow.get('twitterUsername') for users_to_follow in req.json() if users_to_follow.get('twitterUsername')]
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

    def load_restricted_users(self):
        self.restricted_users = {}
        for max_settings in self.maxservers_settings:
            settings_file = '{}/.max_restricted'.format(self.config.get(max_settings, 'config_directory'))

            if os.path.exists(settings_file):
                settings = json.loads(open(settings_file).read())
            else:
                settings = {}

            if 'token' not in settings or 'username' not in settings:
                logger.info("Unable to load MAX settings, please execute initialization script for MAX server {}.".format(self.config.get(max_settings, 'server')))
                sys.exit(1)

            self.restricted_users.setdefault(max_settings, {})['username'] = settings.get('username')
            self.restricted_users.setdefault(max_settings, {})['token'] = settings.get('token')

    def oauth2Header(self, username, token, scope="widgetcli"):
        return {
            "X-Oauth-Token": token,
            "X-Oauth-Username": username,
            "X-Oauth-Scope": scope}

    def run(self):
        self.load_restricted_users()
        self.get_max_global_hashtags()
        self.get_twitter_enabled_contexts()

        # Prompt for login credentials and setup stream object
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)

        # auth = tweepy.auth.BasicAuthHandler(self.options.username, self.options.password)
        stream = tweepy.Stream(auth, StreamWatcherListener(self.config.get('rabbitmq', 'server')), timeout=None)

        # Add the debug hashtag
        self.global_hashtags.append(debug_hashtag)

        logger.info("Listening to this Twitter hashtags: \n{}".format(json.dumps(self.global_hashtags, indent=4, sort_keys=True)))
        logger.info("Listening to this Twitter userIds: \n{}".format(json.dumps(self.users_id_to_follow, indent=4, sort_keys=True)))

        stream.filter(follow=self.flatten_users_id_to_follow(), track=self.global_hashtags)
