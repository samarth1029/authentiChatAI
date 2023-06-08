# set up the Google Cloud Logging python client library
# use Python’s standard logging library to send logs to GCP
import logging

import google.cloud.logging


class CloudLogger:
    def __init__(self, msg: str, level: str = None):
        client = google.cloud.logging.Client()
        client.setup_logging()
        if level == "warn":
            logging.warning(msg)
        elif level == "error":
            logging.error(msg)
        else:
            logging.info(msg)
