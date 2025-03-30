import sys
import logging
from logging import Logger
import datetime as dt
import configparser
from cfg.resource_paths import LOG_CONF_PATH

class ApplicationLogger:
    def __init__(self,
                 log_app_name: str = 'data_pipeline_app',
                 log_conf_section: str = 'DEFAULT'):
        
        self.log_conf_section = log_conf_section
        self.set_log_conf()
        self.log_app_name = log_app_name
        self.setup_logging()

    def set_log_conf(self) -> None:
        cfg_parser = configparser.ConfigParser(interpolation=None)
        cfg_parser.read_file(open(LOG_CONF_PATH))

        self.log_app_name = cfg_parser[self.log_conf_section]['LOG_APP_NAME']
        self.log_level = getattr(logging, cfg_parser[self.log_conf_section]['LOG_LEVEL'], logging.INFO)
        self.fmt = cfg_parser[self.log_conf_section]['FORMAT']
        self.date_fmt = cfg_parser[self.log_conf_section]['DATE_FORMAT']
        self.log_group = cfg_parser[self.log_conf_section]['LOG_GROUP']

    def setup_logging(self) -> None:

        logger = logging.getLogger(self.log_app_name)
        self.logger = logger

        logging.basicConfig(level=self.log_level, format=self.fmt, datefmt=self.date_fmt)
        self.logger.setLevel(self.log_level)

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        if self.log_group:

            curr_dt = dt.datetime.strftime(dt.datetime.now(), '%Y%m%d_%H%M%S')
            log_path = f'logs/{self.log_group}/{curr_dt}.log'

            formatter = logging.Formatter(self.fmt, datefmt=self.date_fmt)
            file_handler = logging.FileHandler(log_path)
            file_handler.setLevel(self.log_level)
            file_handler.setFormatter(formatter)

            strm_handler = logging.StreamHandler(stream=sys.stdout)
            strm_handler.setLevel(self.log_level)
            strm_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(strm_handler)

    def get_log_app_name(self) -> str:
        return self.log_app_name

    def get_logger(self) -> Logger:
        return self.logger

if __name__ == '__main__':
    app_logger = ApplicationLogger()
    logger = app_logger.get_logger()
    logger.info(f'Created application logger for application {app_logger.get_log_app_name()}')
