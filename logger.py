import logging


class Logger:
    def __init__(self, logger_name, filename):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        logger = logging.FileHandler(filename)
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger.setFormatter(formatter)
        self.logger.addHandler(logger)

