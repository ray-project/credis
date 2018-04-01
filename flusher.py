"""Usage:

See bench-flusher.sh.
"""
import argparse
import logging
import time

import redis

parser = argparse.ArgumentParser(description="Flusher.")
parser.add_argument(
    "-t",
    "--ckptflush-interval",
    default=1,
    type=float,
    help="Seconds to sleep between each (ckpt,flush) pair.")
args = parser.parse_args()

# Set up logging.
logging.basicConfig(format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# Singleton chain.
PORT = 6370

# # Every this many seconds, either checkpoint or flush.
# CKPT_FLUSH_INTERVAL_SECS = 1

# SLEEP_SECS = 1
# BEFORE_FLUSH_SLEEP_SECS = 1


def Main():
    head_client = redis.StrictRedis('127.0.0.1', PORT)
    try:
        num_ckpted = 0
        num_flushed = 0
        log.info('Start.')
        while True:
            time.sleep(args.ckptflush_interval)

            r = head_client.execute_command('TAIL.CHECKPOINT')
            log.info('ckpt: %s' % r)
            num_ckpted += r

            # if BEFORE_FLUSH_SLEEP_SECS > 0:
            #     time.sleep(BEFORE_FLUSH_SLEEP_SECS)
            r = head_client.execute_command('HEAD.FLUSH')
            log.info('flush: %s' % r)
            num_flushed += r
    except Exception as e:
        log.info("Exiting: checkpointed %d flushed %d" % (num_ckpted,
                                                          num_flushed))


if __name__ == '__main__':
    Main()
