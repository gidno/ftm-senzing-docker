import os
import logging
from logging.handlers import RotatingFileHandler
import sys
import socket
import pika

from libs.conf import RABBIT_USER, RABBIT_PASS, RABBIT_URL

sys.path.append(".")


def get_logger():
    log_dir = os.path.join(os.path.dirname(__file__), '../logs')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    logger = logging.getLogger("senzing")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s  %(levelname)s  %(filename)s  %(lineno)s  %(message)s')
    log_file = os.path.join(log_dir, 'senzing.log')
    handler = RotatingFileHandler(
        log_file, maxBytes=100 * 1024 * 1024, backupCount=5)
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addHandler(stream)
    return logger


def get_q_manager(q, q_err):
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(RABBIT_URL,
                                       5672,
                                       '/',
                                       credentials=credentials)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=q, durable=True)
    channel.queue_declare(queue=q_err, durable=True)
    return channel


def get_host_name():
    try:
        name = socket.gethostname()
        ip = socket.gethostbyname(name)
    except Exception:
        name = os.popen('hostname').read().strip()
        ip = os.popen('hostname --ip-address').read().strip()
    return name, ip
