import logging
import sys


# Production logger config will be done in a separate config file and will be more complex.
# Left out of the scope of the task.

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
    # Logging to the highest level possible
    __print_logger.error(msg)
