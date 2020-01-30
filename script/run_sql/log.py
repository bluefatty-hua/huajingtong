# coding=utf8

import logging
import logging.handlers
from multi_process_logger import MultiProcessRotatingFileHandler


def init_logging(conf):
    log_format = conf.get('log_format', '%(asctime)s %(levelname)-5s %(message)s -- %(filename)s %(lineno)d %(funcName)s')
    log_level = conf.get('log_level', logging.DEBUG)
    console_log_level = conf.get('console_log_level', logging.ERROR)
    file_log_level = conf.get('file_log_level', logging.INFO)
    scribe_log_level = conf.get('scribe_log_level', log_level)
    log_file = conf.get('log_file', None)
    if isinstance(scribe_log_level, basestring):
        scribe_log_level = eval(scribe_log_level)
    if isinstance(console_log_level, basestring):
        console_log_level = eval(console_log_level)
    if isinstance(log_level, basestring):
        log_level = eval(log_level)

    logger = logging.getLogger()
    logger.setLevel(log_level)

    # 因为有些库的中会对 handler 处理，所以这里需要先清空
    logger.handlers = []
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(log_format))
    ch.setLevel(console_log_level)
    logger.addHandler(ch)

    if log_file:
        fh = MultiProcessRotatingFileHandler(log_file, 'midnight', backupCount=conf.get('backup_count', 0))
        fh.setFormatter(logging.Formatter(log_format))
        fh.setLevel(file_log_level)
        logger.addHandler(fh)


if __name__ == '__main__':
    init_logging({'console_log_level': logging.DEBUG, 'log_file': './tmp.log'})
    logging.debug('aaaa')
    logging.info('bbb')
