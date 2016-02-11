import ConfigParser
import os, re, errno
import logging
from argo_egi_consumer.shared import SingletonShared as Shared

sh = Shared()

class ConsumerConf:
    def __init__(self, confile):
        self._options = {}
        self._args = {'Output': ['Directory', 'Filename', 'ErrorFilename', 'WritePlaintext'],
                      'General': ['LogName', 'AvroSchema', 'Debug', 'LogMsgOutAllowedTime', 'ReportWritMsgEveryHours'],
                      'MsgRetention': ['PastDaysOk', 'FutureDaysOk'],
                      'Subscription': ['Destinations', 'IdleMsgTimeout'],
                      'Authentication': ['HostKey', 'HostCert'],
                      'STOMP': ['TCPKeepAliveIdle', 'TCPKeepAliveInterval',
                                'TCPKeepAliveProbes', 'ReconnectAttempts', 'UseSSL'],
                      'Brokers': ['Server']}
        self._filename = confile

    def parse(self):
        config = ConfigParser.ConfigParser()
        if not os.path.exists(self._filename):
            sh.Logger.error(repr(self.__class__) + ' Could not find %s ' % self._filename)
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
            sh.Logger.error(repr(self.__class__) + " No option '%s' in section: '%s' " % (e.args[0], e.args[1]))
            raise SystemExit(1)
        except ConfigParser.NoSectionError as e:
            sh.Logger.error(repr(self.__class__) + "No section '%s' defined" % (e.args[0]))
            raise SystemExit(1)

    def get_option(self, opt, optional=False):
        if not self._options:
            self.parse()
        try:
            if opt.startswith('Broker'.lower()):
                sortbrokers, tupleserv = [], []
                bn = [serv for serv in self._options.keys() if 'brokers' in serv]
                if len(bn) > 1:
                    try:
                        sortbrokers = sorted(bn, key=lambda s:
                                            int(re.search("(server)([0-9]*)", s).group(2)))
                    except ValueError, IndexError:
                        sh.Logger.error(repr(self.__class__) + " List of broker servers should be enumerated")
                        raise SystemExit(1)
                else:
                    sortbrokers = bn

                for brokopt in sortbrokers:
                    value = self._options[brokopt]
                    if ':' not in value:
                        sh.Logger.error(repr(self.__class__) + " Port should be specified for %s" % value)
                        port = 6163
                        server = value
                    else:
                        (server, port) = value.split(':')
                    tupleserv.append((server, int(port)))

                return tupleserv

            elif opt.startswith('OutputDirectory'.lower()):
                return self._options[opt] + '/' if self._options[opt][-1] != '/' else self._options[opt]

            elif opt.startswith('OutputFilename'.lower()) or \
                 opt.startswith('OutputErrorFilename'.lower()):
                if not re.search(r'DATE(.\w+)$', self._options[opt]):
                    sh.Logger.error('No DATE placeholder in %s', opt)
                    raise SystemExit(1)
                else:
                    return self._options[opt]

            elif opt.startswith('GeneralLogMsgOutAllowedTime'.lower()) or \
                 opt.startswith('OutputWritePlaintext'.lower()) or \
                 opt.startswith('STOMPUseSSL'.lower()):
                return eval(self._options[opt])

            elif opt.startswith('SubscriptionIdleMsgTimeout'.lower()) or \
                 opt.startswith('STOMPTCPKeepAliveIdle'.lower()) or \
                 opt.startswith('STOMPTCPKeepAliveInterval'.lower()) or \
                 opt.startswith('STOMPTCPKeepAliveProbes'.lower()) or \
                 opt.startswith('MsgRetentionFutureDaysOK'.lower()) or \
                 opt.startswith('MsgRetentionPastDaysOK'.lower()) or \
                 opt.startswith('STOMPReconnectAttempts'.lower()) or \
                 opt.startswith('SubscriptionIdleMsgTimeout'.lower()):
                return int(self._options[opt])

            elif opt.startswith('SubscriptionDestinations'.lower()):
                return [t.strip() for t in self._options[opt].split(',')]

            else:
                return self._options[opt]

        except KeyError as e:
            if not optional:
                sh.Logger.error(repr(self.__class__) + " No option %s defined" % e)
                raise SystemExit(1)
            else:
                return None
