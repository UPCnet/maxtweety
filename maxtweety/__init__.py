from maxtweety.listener import StreamWatcherListener
from maxtweety.utils import setup_logging

import argparse
import ConfigParser
import json
import logging
import multiprocessing
import pika
import requests
import sys
import tweepy
import arrow
import time
import datetime

debug_hashtag = u'#debugmaxupcnet'
logger = logging.getLogger('tweety')


class RestartClock(object):
    def __init__(self, seconds_between_restarts=20):
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
    tweety = MaxTwitterListenerRunner(argv, quiet)
    tweety.spawn_process()

    connection = pika.BlockingConnection(
        pika.URLParameters(tweety.common.get('rabbitmq', 'server'))
    )

    channel = connection.channel()

    logger.info('[*] Waiting for restart signal.')

    try:
        restart_delay = int(tweety.config.get('main', 'minimum_restart_delay'))
    except:
        restart_delay = 300

    clock = RestartClock(seconds_between_restarts=restart_delay)

    def callback(ch, method, properties, body):

        received_restart_msg = "[*] Received restart from MAX Server -->"
        try:
            restart_request_time = arrow.get(datetime.datetime.utcfromtimestamp(float(body)))
        except:
            restart_request_time = None
        ch.basic_ack(delivery_tag=method.delivery_tag)

        if clock.in_time(restart_request_time):
            if not clock.ready():
                remaining = clock.remaining()
                next_restart = arrow.now().replace(seconds=remaining).strftime('%H:%M:%S')
                logger.info("{} Delayed until {}".format(received_restart_msg, next_restart))
                clock.wait()
            tweety.restart()
            logger.info("{} Restarted".format(received_restart_msg))
            clock.reset()
        else:
            logger.info("{} Discarding ancient restart".format(received_restart_msg))

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
    parser.add_argument(
        '-c', '--config',
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

        common_config_file = self.config.get('main', 'common')
        cloudapis_config_file = self.config.get('main', 'cloudapis')
        instances_config_file = self.config.get('main', 'instances')

        self.common = ConfigParser.ConfigParser()
        self.common.read(common_config_file)

        self.cloudapis = ConfigParser.ConfigParser()
        self.cloudapis.read(cloudapis_config_file)

        self.instances = ConfigParser.ConfigParser()
        self.instances.read(instances_config_file)

        try:
            self.consumer_key = self.cloudapis.get('twitter', 'consumer_key')
            self.consumer_secret = self.cloudapis.get('twitter', 'consumer_secret')
            self.access_token = self.cloudapis.get('twitter', 'access_token')
            self.access_token_secret = self.cloudapis.get('twitter', 'access_token_secret')

            self.maxservers_settings = [maxserver for maxserver in self.instances.sections() if maxserver.startswith('max_')]

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

    def send_restart(self):
        connection = pika.BlockingConnection(
            pika.URLParameters(self.rabbit_server)
        )
        channel = connection.channel()
        restart_request_time = datetime.datetime.now().strftime('%s.%f')
        channel.basic_publish(
            exchange='',
            routing_key='tweety_restart',
            body=restart_request_time)

    def get_twitter_enabled_contexts(self):
        contexts = {}
        for max_settings in self.maxservers_settings:
            max_url = self.instances.get(max_settings, 'server')
            max_restricted_user = self.instances.get(max_settings, 'restricted_user')
            max_restricted_user_token = self.instances.get(max_settings, 'restricted_user_token')
            req = requests.get('{}/contexts'.format(max_url), params={"twitter_enabled": True, "limit": 0}, headers=self.oauth2Header(max_restricted_user, max_restricted_user_token))
            if req.status_code == 200:
                context_follow_list = [users_to_follow.get('twitterUsernameId') for users_to_follow in req.json() if users_to_follow.get('twitterUsernameId')]
                context_readable_follow_list = [users_to_follow.get('twitterUsername') for users_to_follow in req.json() if users_to_follow.get('twitterUsername')]
                contexts.setdefault(max_settings, {})['ids'] = context_follow_list
                contexts[max_settings]['readable'] = context_readable_follow_list
            else:
                logging.error('Failed to get contexts from "{}" at {}'.format(max_settings, max_url))

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
            self.global_hashtags.append('#{}'.format(self.instances.get(max_settings, 'hashtag')))

    def oauth2Header(self, username, token, scope="widgetcli"):
        return {
            "X-Oauth-Token": token,
            "X-Oauth-Username": username,
            "X-Oauth-Scope": scope}

    def run(self):
        self.get_max_global_hashtags()
        self.get_twitter_enabled_contexts()

        # Prompt for login credentials and setup stream object
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)

        # auth = tweepy.auth.BasicAuthHandler(self.options.username, self.options.password)
        self.rabbit_server = self.common.get('rabbitmq', 'server')
        stream = tweepy.Stream(auth, StreamWatcherListener(self.rabbit_server), timeout=None)

        # Add the debug hashtag
        self.global_hashtags.append(debug_hashtag)

        logger.info("Listening to this Twitter hashtags: {}".format(', '.join(self.global_hashtags)))
        logger.info("Listening to this Twitter users: \n{}".format(json.dumps(dict([(k, ['{:<15} ({})'.format(*a) for a in zip(v['readable'], v['ids'])]) for k, v in self.users_id_to_follow.items()]), indent=4)))

        try:
            stream.filter(follow=self.flatten_users_id_to_follow(), track=self.global_hashtags)
        except:
            logger.info('Stream listener unexpectedly exited, sending auto-restart signal.')
            self.send_restart()
        return
