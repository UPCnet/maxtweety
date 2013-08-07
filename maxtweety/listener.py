from textwrap import TextWrapper

import logging
import tweepy

logger = logging.getLogger("tweeterlistener")


class StreamWatcherListener(tweepy.StreamListener):  # pragma: no cover

    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')

    def on_status(self, status):
        try:
            logger.info('Got tweet %d from %s via %s with content: %s' % (status.id, status.author.screen_name, status.source, status.text))
            # Insert the new data in MAX
            from maxrules.tasks import processTweet
            processTweet.delay(status.author.screen_name.lower(), status.text, status.id)
        except:
            # Catch any unicode errors while printing to console
            # and just ignore them to avoid breaking application.
            pass

    def on_error(self, status_code):
        logging.error('An error has occured! Status code = %s' % status_code)
        return True  # keep stream alive

    def on_timeout(self):
        logging.warning('Snoozing Zzzzzz')
