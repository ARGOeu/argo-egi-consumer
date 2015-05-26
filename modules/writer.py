
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

import time
import sys
import json
import logging
import pprint
import threading
import stomp
import datetime
import json
import os
from os import path
from argo_egi_consumer.log import ProxyMsgLogger
from argo_egi_consumer.config import ProxyConsumerConf
import avro.schema
from avro.datafile import DataFileReader
from avro.datafile import DataFileWriter
from avro.io import DatumReader
from avro.io import DatumWriter

defaultFileLogPastDays = 1
defaultFileLogFutureDays = 1

class MessageWriter:
    def __init__(self, config, thlock):
        self.log = ProxyMsgLogger()
        self.conf = ProxyConsumerConf(config)
        self._thlock = thlock
        self.load()

    def load(self):
        self.conf.parse()
        self.dateFormat = '%Y-%m-%dT%H:%M:%SZ'
        self.fileDirectory = self.conf.get_option('OutputDirectory'.lower())
        self.filenameTemplate =  self.conf.get_option('OutputFilename'.lower())
        self.errorFilenameTemplate =  self.conf.get_option('OutputErrorFilename'.lower())
        self.avroSchema = self.conf.get_option('GeneralAvroSchema'.lower())
        self.debugOutput = eval(self.conf.get_option('GeneralDebug'.lower()))
        self.fileLogPastDays = defaultFileLogPastDays
        self.fileLogFutureDays = defaultFileLogFutureDays
        self.errorLogFaultyTimestamps = eval(self.conf.get_option('GeneralLogFaultyTimestamps'.lower()))

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

            self._thlock.acquire(True)
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

                if self.debugOutput:
                    plainfile = open(filename+'.DEBUG', 'a+')
                    plainfile.write(json.dumps(m)+'\n')
                    plainfile.close()

            except (IOError, OSError) as e:
                self.log.error(e)
                raise SystemExit(1)

            finally:
                self._thlock.release()


    def createLogFilename(self, timestamp):
        if self.fileDirectory[-1] != '/':
            self.fileDirectory = self.fileDirectory + '/'
        return self.fileDirectory + self.filenameTemplate % timestamp

    def createErrorLogFilename(self, timestamp):
        if self.fileDirectory[-1] != '/':
            self.fileDirectory = self.fileDirectory + '/'
        return self.fileDirectory + self.errorFilenameTemplate % timestamp
