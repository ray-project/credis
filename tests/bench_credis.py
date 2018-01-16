import multiprocessing
import time

import numpy as np

import redis
from tests import common
from tests.common import *
from tests.demo import SeqPut


if __name__ == '__main__':
    # 1-node chain.
    BenchCredis(num=1)
