import logging

LOGGER_FORMAT = '%(levelname)s %(asctime)s %(name)s: %(message)s'


def get_logger(name, log_level: str = 'INFO') -> logging.Logger:
    logging.basicConfig(level=log_level, format=LOGGER_FORMAT)
    logger = logging.getLogger(name)
    return logger
