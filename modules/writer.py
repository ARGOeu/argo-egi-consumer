
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
        self.dateFormat = '%Y-%m-%dT%H:%M:%SZ'
        self.fileDirectory = sh.ConsumerConf.get_option('OutputDirectory'.lower())
        self.filenameTemplate =  sh.ConsumerConf.get_option('OutputFilename'.lower())
        self.errorFilenameTemplate =  sh.ConsumerConf.get_option('OutputErrorFilename'.lower())
        self.avroSchema = sh.ConsumerConf.get_option('GeneralAvroSchema'.lower())
        self.txtOutput = eval(sh.ConsumerConf.get_option('OutputWritePlaintext'.lower()))
        self.fileLogPastDays = defaultFileLogPastDays
        self.fileLogFutureDays = defaultFileLogFutureDays
        self.errorLogFaultyTimestamps = eval(sh.ConsumerConf.get_option('GeneralLogFaultyTimestamps'.lower()))

    def writeMessage(self, fields):
        msgOk = False
        nowTime = datetime.datetime.utcnow().date()
        msgTime = nowTime
        if 'timestamp' in fields:
            msgTime = datetime.datetime.strptime(fields['timestamp'], self.dateFormat).date()
            timeDiff = nowTime - msgTime;
            if timeDiff.days == 0:
                msgOk = True
            elif timeDiff.days > 0 and timeDiff.days <= self.fileLogPastDays:
                msgOk = True
            elif timeDiff.days < 0 and -timeDiff.days <= self.fileLogFutureDays:
                msgOk = True

        logMsg = False
        if msgOk:
            filename = self.createLogFilename(fields['timestamp'][:10])
            logMsg = True
        elif self.errorLogFaultyTimestamps:
            filename = self.createErrorLogFilename(fields['timestamp'][:10])
            logMsg = True

        if logMsg:
            # lines
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
                if attr in fields:
                    tags[tagattrmap[attr]] = fields[attr]
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
                if path.exists(filename):
                    avroFile = open(filename, 'a+')
                    writer = DataFileWriter(avroFile, DatumWriter())
                else:
                    avroFile = open(filename, 'w+')
                    writer = DataFileWriter(avroFile, DatumWriter(), schema)

                for m in msglist:
                    writer.append(m)

                writer.close()
                avroFile.close()

                if self.txtOutput:
                    plainfile = open(filename+'.TXT', 'a+')
                    plainfile.write(json.dumps(m)+'\n')
                    plainfile.close()

            except (IOError, OSError) as e:
                sh.Logger.error(e)
                raise SystemExit(1)

            finally:
                sh.thlock.release()


    def createLogFilename(self, timestamp):
        if self.fileDirectory[-1] != '/':
            self.fileDirectory = self.fileDirectory + '/'
        return self.fileDirectory + self.filenameTemplate % timestamp

    def createErrorLogFilename(self, timestamp):
        if self.fileDirectory[-1] != '/':
            self.fileDirectory = self.fileDirectory + '/'
        return self.fileDirectory + self.errorFilenameTemplate % timestamp
