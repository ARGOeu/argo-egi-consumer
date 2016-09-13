import ConfigParser
import os, re, errno, sys
import logging
from argo_egi_consumer.shared import SingletonShared as Shared

sh = Shared()

class ConsumerConf:
    def __init__(self, confile):
        self._options = {}
        self._args = {'MsgFile': ['Directory', 'Filename', 'ErrorFilename', 'WritePlaintext', 'BulkSize'],
                      'General': ['LogName', 'AvroSchema', 'Debug',
                                  'LogWrongFormat', 'ReportWritMsgEveryHours',
                                  'WriteMsgFile', 'WriteMsgIngestion'],
                      'MsgIngestion': ['Host', 'Token', 'Tenant', 'BulkSize'],
                      'MsgRetention': ['PastDaysOk', 'FutureDaysOk', 'LogMsgOutAllowedTime'],
                      'Subscription': ['Destinations', 'IdleMsgTimeout'],
                      'Authentication': ['HostKey', 'HostCert'],
                      'STOMP': ['TCPKeepAliveIdle', 'TCPKeepAliveInterval',
                                'TCPKeepAliveProbes', 'ReconnectAttempts', 'UseSSL'],
                      'Brokers': ['Server']}
        self._filename = confile

    def _module_class_name(self, obj):
        name = repr(obj.__module__) + '.' + repr(obj.__class__.__name__)
        return name.replace("'",'')

    def parse(self):
        config = ConfigParser.ConfigParser()
        if not os.path.exists(self._filename):
            sys.stderr.write(self._module_class_name(self)  + ': Could not find %s \n' % self._filename)
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
            sys.stderr.write(self._module_class_name(self) + ": No option '%s' in section: '%s' \n" % (e.args[0], e.args[1]))
            raise SystemExit(1)
        except ConfigParser.NoSectionError as e:
            sys.stderr.write(self._module_class_name(self) + ": No section '%s' defined\n" % (e.args[0]))
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
                        sys.stderr.write(self._module_class_name(self) + ": List of broker servers should be enumerated\n")
                        raise SystemExit(1)
                else:
                    sortbrokers = bn

                for brokopt in sortbrokers:
                    value = self._options[brokopt]
                    if ':' not in value:
                        sys.stderr.write(self._module_class_name(self) + ": Port should be specified for %s\n" % value)
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
                if '.' not in self._options[opt]:
                    sys.stderr.write(self._module_class_name(self) + ': %s should have an extension\n' % opt)
                    raise SystemExit(1)
                if not re.search(r'DATE(.\w+)$', self._options[opt]):
                    sys.stderr.write(self._module_class_name(self) + ': No DATE placeholder in %s\n' % opt)
                    raise SystemExit(1)
                else:
                    return self._options[opt]

            elif opt.startswith('MsgRetentionLogMsgOutAllowedTime'.lower()) or \
                 opt.startswith('GeneralLogWrongFormat'.lower()) or \
                 opt.startswith('GeneralWriteMsgFile'.lower()) or \
                 opt.startswith('GeneralWriteMsgIngestion'.lower()) or \
                 opt.startswith('MsgFileWritePlaintext'.lower()) or \
                 opt.startswith('GeneralWriteMsgFile'.lower()) or \
                 opt.startswith('GeneralWriteMsgIngestion'.lower()) or \
                 opt.startswith('STOMPUseSSL'.lower()):
                return eval(self._options[opt])

            elif opt.startswith('SubscriptionIdleMsgTimeout'.lower()) or \
                 opt.startswith('MsgFileBulkSize'.lower()) or \
                 opt.startswith('MsgIngestionBulkSize'.lower()) or \
                 opt.startswith('MsgRetentionFutureDaysOK'.lower()) or \
                 opt.startswith('MsgRetentionPastDaysOK'.lower()) or \
                 opt.startswith('STOMPReconnectAttempts'.lower()) or \
                 opt.startswith('STOMPTCPKeepAliveIdle'.lower()) or \
                 opt.startswith('STOMPTCPKeepAliveInterval'.lower()) or \
                 opt.startswith('STOMPTCPKeepAliveProbes'.lower()) or \
                 opt.startswith('SubscriptionIdleMsgTimeout'.lower()):
                return int(self._options[opt])

            elif opt.startswith('SubscriptionDestinations'.lower()):
                return [t.strip() for t in self._options[opt].split(',')]

            else:
                return self._options[opt]

        except KeyError as e:
            if not optional:
                sys.stderr.write(self._module_class_name(self) + ": No option %s defined\n" % e)
                raise SystemExit(1)
            else:
                return None
