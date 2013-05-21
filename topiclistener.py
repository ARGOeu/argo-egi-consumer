import time
import sys
import logging
import stomp
import sys
import datetime
from os import path

class TopicListener(stomp.ConnectionListener):
    
    def __init__(self): 
	# connection
        self.connected = False
        self.connectedCounter = 100
        # topic
	self.topic = None
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
	# output
	self.debugOutput = 0 
        # date format
        self.dateFormat = '%Y-%m-%dT%H:%M:%SZ'

    def createLogFilename(self, timestamp):
        if self.fileDirectory[-1] != '/':
            self.fileDirectory = self.fileDirectory + '/'
        return self.fileDirectory + self.filenamePrefix % timestamp

    def createErrorLogFilename(self, timestamp):
        if self.fileDirectory[-1] != '/':
            self.fileDirectory = self.fileDirectory + '/'
        return self.fileDirectory + self.errorLogFilenamePrefix % timestamp

    def createLogEntry(self, msg):
	return datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + ' --> ' + msg + '\n'

    def on_connected(self,headers,body):
        sys.stdout.write(self.createLogEntry("Listener connected: %s\n" % body))
        sys.stdout.flush()
        self.connected = True
        self.connectedCounter = 100

    def on_connecting(self,host_and_port):
        sys.stdout.write(self.createLogEntry("Listener connecting to: %s:%d" % host_and_port))
        sys.stdout.flush()

    def on_disconnected(self):
        sys.stderr.write(self.createLogEntry("Listener disconnected"))
        sys.stderr.flush()
        self.connected = False

    def on_error(self, headers, message):
	sys.stderr.write(self.createLogEntry("Received error %s" % message))

    def on_message(self, headers, message):
        lines = message.split('\n')
        fields = dict()
        for line in lines:
            splitLine = line.split(': ')
            if len(splitLine) > 1:
                key = splitLine[0]
                value = splitLine[1]
                fields[key] = value

	if self.debugOutput:
            sys.stdout.write(self.createLogEntry('-' * 20))
            sys.stdout.write('Message:\n %s' % message)
            sys.stdout.flush()

        msgTime = datetime.datetime.strptime(fields['timestamp'], self.dateFormat).date();
        nowTime = datetime.datetime.utcnow().date()

	timeDiff = nowTime - msgTime;
     
        if self.debugOutput:
            sys.stdout.write('Msg time: %r\n' % msgTime)
            sys.stdout.write('Now: %r\n' % nowTime)
            sys.stdout.write('Diff time: %r\n' % timeDiff)
            sys.stdout.flush()	

        msgOk = False
        if timeDiff.days == 0:
            msgOk = True
        elif timeDiff.days > 0 and timeDiff.days <= self.fileLogFutureDays:
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
	
            msgFile.write(self.fileFieldHeader)
            for field in self.fileFields:
                if field in fields:
                    msgFile.write(self.fileFieldFormat % fields[field])
                else:
                    msgFile.write(self.fileFieldFormat % self.fileFieldNotAvaliable)
            msgFile.write(self.fileFieldFooter)

            msgFile.close();

        if self.debugOutput:
            if logMsg:
                sys.stdout.write('msg added to file\n\n')
            else:
                sys.stdout.write('msg NOT added to file\n\n')
            sys.stdout.flush()
