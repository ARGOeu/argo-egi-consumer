import ConfigParser
import os, re, errno
import logging
from argo_egi_consumer.log import ProxyMsgLogger

class AbstractConsumerConf(object):
    def get_option(self, opt, optional=False):
        pass


class ConsumerConf(AbstractConsumerConf):
    def __init__(self, confile):
        self.log = ProxyMsgLogger()
        self._options = {}
        self._args = {'Output': ['Directory', 'Filename', 'ErrorFilename', 'WritePlaintext'],
                      'General': ['AvroSchema', 'Debug', 'LogFaultyTimestamps', 'ReportWritMsgEveryHours'],
                      'Subscription': ['Destinations', 'IdleMsgTimeout'],
                      'Authentication': ['HostKey', 'HostCert'],
                      'STOMP': ['TCPKeepAliveIdle', 'TCPKeepAliveInterval',
                                'TCPKeepAliveProbes', 'ReconnectAttempts', 'UseSSL'],
                      'Brokers': ['Server']}
        self._filename = confile

    def parse(self):
        config = ConfigParser.ConfigParser()
        if not os.path.exists(self._filename):
            self.log.error(repr(self.__class__) + ' Could not find %s ' % self._filename)
            raise SystemExit(1)
        config.read(self._filename)

        try:
            for sect, opts in self._args.items():
                for opt in opts:
                    for section in config.sections():
                        if section.lower().startswith(sect.lower()):
                            for o in config.options(section):
                                if o.startswith(opt.lower()):
                                    optget = config.get(section, o)
                                    self._options.update({(sect+o).lower(): optget})
        except ConfigParser.NoOptionError as e:
            self.log.error(repr(self.__class__) + " No option '%s' in section: '%s' " % (e.args[0], e.args[1]))
            raise SystemExit(1)
        except ConfigParser.NoSectionError as e:
            self.log.error(repr(self.__class__) + "No section '%s' defined" % (e.args[0]))
            raise SystemExit(1)

    def get_option(self, opt, optional=False):
        if not self._options:
            self.parse()
        try:
            if 'brokers' in opt:
                sortbrokers, tupleserv = [], []
                bn = [serv for serv in self._options.keys() if 'brokers' in serv]
                if len(bn) > 1:
                    try:
                        sortbrokers = sorted(bn, key=lambda s:
                                            int(re.search("(server)([0-9]*)", s).group(2)))
                    except ValueError, IndexError:
                        self.log.error(repr(self.__class__) + " List of broker servers should be enumerated")
                        raise SystemExit(1)
                else:
                    sortbrokers = bn

                for brokopt in sortbrokers:
                    value = self._options[brokopt]
                    if ':' not in value:
                        self.log.error(repr(self.__class__) + " Port should be specified for %s" % value)
                        port = 6163
                        server = value
                    else:
                        (server, port) = value.split(':')
                    tupleserv.append((server, int(port)))

                return tupleserv

            else:
                return self._options[opt]

        except KeyError as e:
            if not optional:
                self.log.error(repr(self.__class__) + " No option %s defined" % e)
                raise SystemExit(1)
            else:
                return None


class ProxyConsumerConf(AbstractConsumerConf):
    def __init__(self, config):
        if not getattr(self.__class__, 'shared_object', None):
            self.__class__.shared_object = ConsumerConf(config)

    def parse(self):
        return self.__class__.shared_object.parse()

    def get_option(self, opt, optional=False):
        return self.__class__.shared_object.get_option(opt, optional)