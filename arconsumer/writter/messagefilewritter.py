
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
import stomp
import sys
import datetime
import os
from os import path
from messagewritter import MessageWritter

defaultFileDirectory = '/var/lib/ar-consumer'
defaultFilenamePrefix = 'ar-consumer_log_%s.txt'
defaultFileFields = ['timestamp', 'metricName', 'serviceType', 'hostName', 'metricStatus', 'voName']
defaultFileHeader = ''
defaultFileFieldHeader = ''
defaultFileFieldFormat = '%s\001'
defaultFileFieldNotAvaliable = ''
defaultFileFieldFooter = '\n'
defaultFileFooter = ''
defaultFileLogPastDays = 1
defaultFileLogFutureDays = 0
defaultErrorLogFilenamePrefix = 'ar-consumer_error_%s.txt'
defaultErrorLogFaultyTimestamps = 0

class MessageFileWritter(MessageWritter):
    
	def __init__(self):
		MessageWritter.__init__(self) 
		# log file
		self.fileDirectory = ''
		self.filenamePrefix = ''	
		self.fileFields = []
		self.fileHeader = ''
		self.fileFieldHeader = ''
		self.fileFieldFormat = ''
		self.fileFieldNotAvaliable = ''
		self.fileFieldFooter = ''
		self.fileFooter = ''
		self.fileLogPastDays = 0
		self.fileLogFutureDays = 0	
		self.errorLogFilenamePrefix = ''
		self.errorLogFaultyTimestamps = 0

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
		else:
			self.fileDirectory = defaultFileDirectory

		if 'filenamePrefix' in configFields:
			self.filenamePrefix = configFields['filenamePrefix']
		else:
			self.filenamePrefix = defaultFilenamePrefix

		if 'fileFields' in configFields:
			self.fileFields = configFields['fileFields'].split(';')
		else:
			self.fileFields = defaultFileFields
			
		if 'fileHeader' in configFields:
			self.fileHeader = configFields['fileHeader']
		else:
			self.fileHeader = defaultFileHeader

		if 'fileFieldHeader' in configFields:
			self.fileFieldHeader = configFields['fileFieldHeader']
		else:
			self.fileFieldHeader = defaultFileFieldHeader

		if 'fileFieldFormat' in configFields:
			self.fileFieldFormat = configFields['fileFieldFormat']
		else:
			self.fileFieldFormat = defaultFileFieldFormat

		if 'fileFieldNotAvaliable' in configFields:
			self.fileFieldNotAvaliable = configFields['fileFieldNotAvaliable']
		else:
			self.fileFieldNotAvaliable = defaultFileFieldNotAvaliable

		if 'fileFieldFooter' in configFields:
			self.fileFieldFooter = configFields['fileFieldFooter']
		else:
			self.fileFieldFooter = defaultFileFieldFooter

		if 'fileFooter' in configFields:
			self.fileFooter = configFields['fileFooter']
		else:
			self.fileFooter = defaultFileFooter

		if 'fileLogPastDays' in configFields:
                        self.fileLogPastDays = configFields['fileLogPastDays']
                else:
                        self.fileLogPastDays = defaultFileLogPastDays

		if 'fileLogFutureDays' in configFields:
                        self.fileLogFutureDays = configFields['fileLogFutureDays']
                else:
                        self.fileLogFutureDays = defaultFileLogFutureDays
	
		if 'errorLogFilenamePrefix' in configFields:
                        self.errorLogFilenamePrefix = configFields['errorLogFilenamePrefix']
                else:
                        self.errorLogFilenamePrefix = defaultErrorLogFilenamePrefix

		if 'errorLogFaultyTimestamps' in configFields:
                        self.errorLogFaultyTimestamps = configFields['errorLogFaultyTimestamps']
                else:
                        self.errorLogFaultyTimestamps = defaultErrorLogFaultyTimestamps

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
			addFileHeader = not path.exists(filename)
			msgFile = open(filename, 'a')

			if addFileHeader:
                		msgFile.write(self.fileHeader)

			# lines
			# msgFile.write(self.fileFieldHeader)
			lines = [self.fileFieldHeader]

			for field in self.fileFields:
				if field in fields:
					fieldSplit = fields[field]
					# serviceType can contain ',' and it has to be split to two lines 
					if field == 'serviceType' and ',' in fieldSplit:
						newLines = list();
						fieldSplit = fieldSplit.split(',')				
						# offset = 0
						for split in fieldSplit:
							#new lines
							for idx in range(0,len(lines)):
								newLines.append(lines[idx] + (self.fileFieldFormat % split))
							# offset += len(lines)
						lines = newLines
					else:
						for idx in range(0,len(lines)):
                                                	lines[idx] = lines[idx] + (self.fileFieldFormat % fieldSplit)
					# msgFile.write(self.fileFieldFormat % fields[field])
				else:
					for idx in range(0,len(lines)):
						lines[idx] = lines[idx] + (self.fileFieldFormat % self.fileFieldNotAvaliable)
					# msgFile.write(self.fileFieldFormat % self.fileFieldNotAvaliable)

			for idx in range(0,len(lines)):
				lines[idx] = lines[idx] + (self.fileFieldFooter)
			# msgFile.write(self.fileFieldFooter)

			for line in lines:
                                msgFile.write(line)

			msgFile.close();	


	def createLogFilename(self, timestamp):
		if self.fileDirectory[-1] != '/':
			self.fileDirectory = self.fileDirectory + '/'
		return self.fileDirectory + self.filenamePrefix % timestamp

	def createErrorLogFilename(self, timestamp):
		if self.fileDirectory[-1] != '/':
			self.fileDirectory = self.fileDirectory + '/'
		return self.fileDirectory + self.errorLogFilenamePrefix % timestamp

