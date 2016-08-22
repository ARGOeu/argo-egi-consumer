
# Copyright (c) 2013 GRNET S.A., SRCE, IN2P3 CNRS Computing Centre
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS
# IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language
# governing permissions and limitations under the License.
#
# The views and conclusions contained in the software and
# documentation are those of the authors and should not be
# interpreted as representing official policies, either expressed
# or implied, of either GRNET S.A., SRCE or IN2P3 CNRS Computing
# Centre
#
# The work represented by this source file is partially funded by
# the EGI-InSPIRE project through the European Commission's 7th
# Framework Programme (contract # INFSO-RI-261323)

import avro.schema
import datetime
import json
import logging
import os
import pprint
import stomp
import sys
import threading
import time
import re

from argo_egi_consumer.shared import SingletonShared as Shared
from avro.datafile import DataFileReader
from avro.datafile import DataFileWriter
from avro.io import DatumReader
from avro.io import DatumWriter
from os import path

defaultFileLogPastDays = 1
defaultFileLogFutureDays = 1
LOGFORMAT = '%(name)s[%(process)s]: %(levelname)s %(message)s'

sh = Shared()

class MsgLogger:
    def __init__(self, name):
        formatter = logging.Formatter(LOGFORMAT)
        self.mylog = logging.getLogger(name)
        self.mylog.setLevel(logging.DEBUG)
        handler = logging.handlers.SysLogHandler('/dev/log')
        handler.setFormatter(formatter)
        self.mylog.addHandler(handler)
        self.mylog.propagate = False

        self.rootlog = logging.getLogger('')
        self.rootlog.setLevel(logging.WARNING)
        self.rootlog.addHandler(handler)
        self.rootlog.propagate = False

    def error(self, msg):
        self.mylog.error(msg)

    def info(self, msg):
        self.mylog.info(msg)

    def warning(self, msg):
        self.mylog.warning(msg)

    def addHandler(self, hdlr):
        self.mylog.addHandler(hdlr)

    def removeHandler(self, hdlr):
        self.mylog.removeHandler(hdlr)


class MessageWriter:
    def __init__(self):
        self.load()

    def load(self):
        sh.ConsumerConf.parse()
        self.avroSchema = sh.ConsumerConf.get_option('GeneralAvroSchema'.lower())
        self.dateFormat = '%Y-%m-%dT%H:%M:%SZ'
        self.errorFilenameTemplate = sh.ConsumerConf.get_option('MsgFileErrorFilename'.lower())
        self.fileDirectory = sh.ConsumerConf.get_option('MsgFileDirectory'.lower())
        self.filenameTemplate = sh.ConsumerConf.get_option('MsgFileFilename'.lower())
        self.futureDaysOk = sh.ConsumerConf.get_option('MsgRetentionFutureDaysOk'.lower())
        self.logOutAllowedTime = sh.ConsumerConf.get_option('MsgRetentionLogMsgOutAllowedTime'.lower())
        self.logWrongFormat = sh.ConsumerConf.get_option('GeneralLogWrongFormat'.lower())
        self.pastDaysOk = sh.ConsumerConf.get_option('MsgRetentionPastDaysOk'.lower())
        self.txtOutput = sh.ConsumerConf.get_option('MsgFileWritePlaintext'.lower())

    def _write_to_ptxt(self, log, fields, exten):
        try:
            filename = '.'.join(log.split('.')[:-1]) + '.%s' % exten
            plainfile = open(filename, 'a+')
            plainfile.write(json.dumps(fields) + '\n')
            plainfile.close()
        except (IOError, OSError) as e:
            sh.Logger.error(e)
            raise SystemExit(1)

    def _write_to_avro(self, log, fields):
        msglist = []
        msg, tags = {}, {}

        msg = {'service': fields['serviceType'],
               'timestamp': fields['timestamp'],
               'hostname': fields['hostName'],
               'metric': fields['metricName'],
               'status': fields['metricStatus']}
        msgattrmap = {'detailsData': 'message',
                      'summaryData': 'summary',
                      'nagios_host': 'monitoring_host'}
        for attr in msgattrmap.keys():
            if attr in fields:
                msg[msgattrmap[attr]] = fields[attr]

        tagattrmap = {'ROC': 'roc', 'voName': 'voName', 'voFqan': 'voFqan'}
        for attr in tagattrmap.keys():
            tags[tagattrmap[attr]] = fields.get(attr, None)
        if tags:
            msg['tags'] = tags

        if ',' in fields['serviceType']:
            servtype = fields['serviceType'].split(',')
            msg['service'] = servtype[0].strip()
            msglist.append(msg)
            copymsg = msg.copy()
            copymsg['service'] = servtype[1].strip()
            msglist.append(copymsg)
        else:
            msglist.append(msg)

        sh.thlock.acquire(True)
        try:
            schema = avro.schema.parse(open(self.avroSchema).read())
            if path.exists(log):
                avroFile = open(log, 'a+')
                writer = DataFileWriter(avroFile, DatumWriter())
            else:
                avroFile = open(log, 'w+')
                writer = DataFileWriter(avroFile, DatumWriter(), schema)

            for m in msglist:
                writer.append(m)

            writer.close()
            avroFile.close()

        except (IOError, OSError) as e:
            sh.Logger.error(e)
            raise SystemExit(1)

        finally:
            sh.thlock.release()

    def _is_validmsg(self, msgfields):
        keys = set(msgfields.keys())
        mandatory_fields = set(['serviceType', 'timestamp', 'hostName', 'metricName', 'metricStatus'])

        if keys >= mandatory_fields:
            return True
        else:
            sh.Logger.error('Message %s has no mandatory fields: %s' % (msgfields['message-id'],
                                                                        str([e for e in mandatory_fields.difference(keys)])))
            return False

    def _is_ininterval(self, msgid, timestamp, now):
        inint = False

        try:
            msgTime = datetime.datetime.strptime(timestamp, self.dateFormat).date()
            nowTime = datetime.datetime.utcnow().date()
        except ValueError as e:
            sh.Logger.error('Message %s %s' % (msgid, e))
            return inint

        timeDiff = nowTime - msgTime
        if timeDiff.days == 0:
            inint = True
        elif timeDiff.days > 0 and timeDiff.days <= self.pastDaysOk:
            inint = True
        elif timeDiff.days < 0 and -timeDiff.days <= self.futureDaysOk:
            inint = True

        return inint

    def writeMessage(self, fields):
        now = datetime.datetime.utcnow().date()

        if self._is_validmsg(fields):
            if self._is_ininterval(fields['message-id'], fields['timestamp'], now):
                filename = self.createLogFilename(fields['timestamp'][:10])
                self._write_to_avro(filename, fields)
                if self.txtOutput:
                    self._write_to_ptxt(filename, fields, 'PLAINTEXT')

            elif self.logOutAllowedTime:
                filename = self.createErrorLogFilename(str(now))
                self._write_to_avro(filename, fields)
                if self.txtOutput:
                    self._write_to_ptxt(filename, fields, 'PLAINTEXT')
        elif self.logWrongFormat:
            filename = self.createErrorLogFilename(str(now))
            self._write_to_ptxt(filename, fields, 'WRONGFORMAT')

    def createLogFilename(self, timestamp):
        return self.fileDirectory + self.filenameTemplate.replace('DATE', timestamp)

    def createErrorLogFilename(self, timestamp):
        return self.fileDirectory + self.errorFilenameTemplate.replace('DATE', timestamp)
