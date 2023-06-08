import logging


def get_logger(logger_name, create_file=False):
    # create logger for prd_ci
    log = logging.getLogger(logger_name)
    log.setLevel(level=logging.INFO)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # create console handler for logger.
    ch = logging.StreamHandler()
    ch.setLevel(level=logging.DEBUG)
    ch.setFormatter(formatter)
    log.addHandler(ch)

    if create_file:
        # create file handler for logger.
        fh = logging.FileHandler('SPOT.log')
        fh.setLevel(level=logging.DEBUG)
        fh.setFormatter(formatter)
        log.addHandler(fh)

    return log
