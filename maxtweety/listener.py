from textwrap import TextWrapper

import json
import logging
import pika
import tweepy

logger = logging.getLogger('twitterlistener')


class StreamWatcherListener(tweepy.StreamListener):  # pragma: no cover

    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')

    def __init__(self, rabbit_server, api=None):
        super(StreamWatcherListener, self).__init__(api)
        self.rabbit_server = rabbit_server

    def on_status(self, status):
        # Initialize Rabbit connection
        self.connection = pika.BlockingConnection(
            pika.URLParameters(self.rabbit_server)
        )
        self.channel = self.connection.channel()

        try:
            logger.info('Got tweet %d from %s via %s with content: %s' % (status.id, status.author.screen_name, status.source, status.text))
            # Insert the data of the new tweet into the Rabbit queue
            message = dict(author=status.author.screen_name.lower(), message=status.text, stid=status.id)
            self.channel.basic_publish(
                exchange='twitter',
                routing_key='',
                body=json.dumps(message)
            )
            self.connection.close()
            # from maxrules.tasks import processTweet
            # processTweet.delay(status.author.screen_name.lower(), status.text, status.id)
        except:
            # Catch any unicode errors while printing to console
            # and just ignore them to avoid breaking application.
            pass

    def on_error(self, status_code):
        logging.error('An error has occured! Status code = %s' % status_code)
        return True  # keep stream alive

    def on_timeout(self):
        logging.warning('Snoozing Zzzzzz')
