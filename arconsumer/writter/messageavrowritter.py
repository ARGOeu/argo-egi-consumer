
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
import logging
import pprint
import stomp
import sys
import datetime
import json
import os
from os import path
from messagewritter import MessageWritter

import avro.schema
from avro.datafile import DataFileReader
from avro.datafile import DataFileWriter
from avro.io import DatumReader
from avro.io import DatumWriter

defaultFileDirectory = '/var/lib/ar-consumer'
defaultFilenameTemplate = 'ar-consumer_log_%s.avro'
defaultErrorFilenameTemplate = 'ar-consumer_error_log_%s.avro'
defaultAvroSchema = 'argo.avsc'
defaultSplitFields = 'serviceType'
defaultMessageFields = ['timestamp', 'metricName', 'serviceType', 'hostName', 'metricStatus', 'ROC', 'voName', 'voFqan']
defaultFileFields = ['timestamp', 'metric', 'service', 'hostname', 'status', 'roc', 'vo', 'vo_fqan']
defaultMessageTagFields = ['ROC', 'voName', 'voFqan']
defaultFileTagFields = ['roc', 'vo', 'vo_fqan']
defaultFileLogPastDays = 1
defaultFileLogFutureDays = 1
defaultErrorLogFaultyTimestamps = 0

class MessageAvroWritter(MessageWritter):

    def __init__(self):
        MessageWritter.__init__(self) 
        self.fileDiectory = defaultFileDirectory;
        self.filenameTemplate =  defaultFilenameTemplate
        self.errorFilenameTemplate = defaultErrorFilenameTemplate 
        self.avroSchema = defaultAvroSchema
        self.splitFields = defaultSplitFields
        self.messageFields = defaultMessageFields
        self.fileFields = defaultFileFields
        self.messageTagFields = defaultMessageTagFields
        self.fileTagFields = defaultFileTagFields
        self.fileLogPastDays = defaultFileLogPastDays
        self.fileLogFutureDays = defaultFileLogFutureDays
        self.errorLogFaultyTimestamps = defaultErrorLogFaultyTimestamps

    def loadConfig(self, configFileName):
        configFile = None
        configFields = dict()
        self.configFile = configFileName
        if os.path.isfile(self.configFile):
            configFile = open(self.configFile, 'r')
            lines = configFile.readlines()

            for line in lines:
                if line[0] == '#':
                    continue
                splitLine = line.split('=')
                if len(splitLine) > 1:
                    key = splitLine[0].strip()
                    value = splitLine[1].strip()
                    value = value.decode('string_escape')
                    if value[0] == "'":
                        if value [-1] == "'":
                            value = value[1:-1]
                        else:
                            continue
                    elif value[0] == '"':
                        if value [-1] == '"':
                            value = value[1:-1]
                        else:
                            continue
                    else:
                        value = int(value)
                    configFields[key] = value

            configFile.close()

        #apply config
        if 'fileDirectory' in configFields:
            self.fileDirectory = configFields['fileDirectory']
        if 'filenameTemplate' in configFields:
            self.filenameTemplate = configFields['filenameTemplate']
        if 'errorFilenameTemplate ' in configFields:
            self.errorFilenameTemplate = configFields['errorFilenameTemplate']
        if 'avroSchema' in configFields:
            self.avroSchema = configFields['avroSchema']
        if 'splitFields' in configFields:
            self.splitFields = configFields['splitFields']
        if 'messageFields' in configFields:
            self.messageFields = configFields['messageFields'].split(';')
        if 'fileFields' in configFields:
            self.fileFields = configFields['fileFields'].split(';')
        if 'fileLogPastDays' in configFields:
            self.fileLogPastDays = configFields['fileLogPastDays']
        if 'fileLogFutureDays' in configFields:
            self.fileLogFutureDays = configFields['fileLogFutureDays']
        if 'errorLogFaultyTimestamps' in configFields:
            self.errorLogFaultyTimestamps = configFields['errorLogFaultyTimestamps']

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
            lines = list()
            lines.append(dict())
            tags = dict()

            # message fields
            for field in self.messageFields:
                if field in fields:
                    fieldSplit = fields[field]
                    fileField = self.fileFields[self.messageFields.index(field)]
                    # serviceType can contain ',' and it has to be split to two lines 
                    if field in self.splitFields and ',' in fieldSplit:
                        newLines = list();
                        fieldSplit = fieldSplit.split(',')              
                        for split in fieldSplit:
                            #new lines
                            for idx in range(0,len(lines)):
                                newLine = lines[idx].copy()
                                newLine[fileField] = split
                                newLines.append(newLine)
                        lines = newLines
                    else:
                        for idx in range(0,len(lines)):
                            lines[idx][fileField] = fieldSplit

            # tags
            lines[idx]['tags'] = lines[idx]

            #for tag in self.messageTagFields:
            #    if tag in fields:
            #        fileTag = self.fileTagFields[self.messageTagFields.index(tag)]
            #        tags[fileTag] = fields[tag]
            #if len(tags) > 0:
            #    jsonTags = json.dumps(tags)
            #    for idx in range(0,len(lines)):
            #        lines[idx]['tags'] = jsonTags
            
            schema = avro.schema.parse(open(self.avroSchema).read())
            if path.exists(filename):
                avroFile = open(filename, 'a+')
                writer = DataFileWriter(avroFile, DatumWriter())
            else:
                avroFile = open(filename, 'w+')
                writer = DataFileWriter(avroFile, DatumWriter(), schema)

            for line in lines:
                # TODO write to file
                writer.append(line)

            writer.close()
            avroFile.close()

    def createLogFilename(self, timestamp):
        if self.fileDirectory[-1] != '/':
            self.fileDirectory = self.fileDirectory + '/'
        return self.fileDirectory + self.filenameTemplate % timestamp

    def createErrorLogFilename(self, timestamp):
        if self.fileDirectory[-1] != '/':
            self.fileDirectory = self.fileDirectory + '/'
        return self.fileDirectory + self.errorFilenameTemplate % timestamp
