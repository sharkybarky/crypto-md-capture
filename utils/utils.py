import logging
import sys


class LessThanFilter(logging.Filter):
    def __init__(self, exclusive_maximum, name=""):
        super(LessThanFilter, self).__init__(name)
        self.max_level = exclusive_maximum

    def filter(self, record):
        # non-zero return means we log this message
        return 1 if record.levelno < self.max_level else 0


def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.NOTSET)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.addFilter(LessThanFilter(logging.WARNING))
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d:%(filename)s:%(lineno)s:[%(levelname)s] - %(message)s',
                                  datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    root.addHandler(handler)

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.WARNING)
    formatter_red = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d:%(filename)s:%(lineno)s:[%(levelname)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")

    handler.setFormatter(formatter_red)
    root.addHandler(handler)
