#!/usr/bin/python

import logging
import logging.handlers

LOGFORMAT = '%(name)s[%(process)s]: %(message)s'
daemonname = 'argo-egi-consumer'

class AbstractLogger(object):
    def error(self, msg):
        pass

    def info(self, msg):
        pass

    def warning(self, msg):
        pass


class MsgLogger(AbstractLogger):
    def __init__(self):
        logging.basicConfig()
        formatter = logging.Formatter(LOGFORMAT)
        self.mylog = logging.getLogger(daemonname)
        self.mylog.setLevel(logging.DEBUG)
        handler = logging.handlers.SysLogHandler('/dev/log')
        handler.setFormatter(formatter)
        self.mylog.addHandler(handler)

    def error(self, msg):
        self.mylog.error(msg)

    def info(self, msg):
        self.mylog.info(msg)

    def warning(self, msg):
        self.mylog.warning(msg)


class ProxyMsgLogger(AbstractLogger):
    def __init__(self):
        if not getattr(self.__class__, 'shared_object', None):
            self.__class__.shared_object = MsgLogger()

    def error(self, msg):
        self.__class__.shared_object.error(msg)

    def info(self, msg):
        self.__class__.shared_object.info(msg)

    def warning(self, msg):
        self.__class__.shared_object.warning(msg)
