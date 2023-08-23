import logging
import sys


def __configure_print_logger() -> logging.Logger:
    logger = logging.getLogger("logger1")
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.propagate = False
    return logger


__print_logger = __configure_print_logger()


def print_cmd(msg):
    __print_logger.info(msg)


