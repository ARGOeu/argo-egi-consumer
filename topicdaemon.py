#!/usr/bin/env python

import sys
import os
import time
import stomp
import logging
from daemon import Daemon
from topiclistener import TopicListener

# topis deamon defaults
defaultMsgServer = 'msg.cro-ngi.hr'
defaultMsgServerPort = 6163
defaultDaemonConfigFile = os.path.dirname(os.path.abspath(__file__)) + '/topicdaemon.conf' 
defaultDaemonPIDFile = os.path.dirname(os.path.abspath(__file__)) + '/topicdaemon.pid'
defaultDaemonStdIn = '/dev/null'
defaultDaemonStdOut = os.path.dirname(os.path.abspath(__file__)) + '/std.out'
defaultDaemonStdErr = os.path.dirname(os.path.abspath(__file__)) + '/std.err'
defaultDaemonName = 'topicdaemon'

defaultTopic = '/topic/grid.probe.metricOutput.EGEE.ngi.*'
defaultFileDirectory = os.path.dirname(os.path.abspath(__file__))
defaultFilenamePrefix = 'log_%s.txt'
defaultFileFields = ['timestamp', 'metricName', 'serviceType', 'hostName', 'metricStatus', 'voName']
defaultFileHeader = ''
defaultFileFieldHeader = ''
defaultFileFieldFormat = '%s\001'
defaultFileFieldNotAvaliable = ''
defaultFileFieldFooter = '\n'
defaultFileFooter = ''
defaultFileLogPastDays = 1
defaultFileLogFutureDays = 0
defaultErrorLogFilenamePrefix = 'error_log_%s.txt'
defualtErrorLogFaultyTimestamps = 0

defaultDebugOutput = 0

class TopicDaemon(Daemon):
	def run(self):
		
		logging.basicConfig()
	
		#load config
		configFile = None
		configFields = dict()
		if os.path.isfile(defaultDaemonConfigFile):
			configFile = open(defaultDaemonConfigFile, 'r')
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

		# create listener
                listener = TopicListener()

		#apply config
		if 'topic' in configFields:
                	listener.topic = configFields['topic']
            	else:
                	listener.topic = defaultTopic
 
		if 'fileDirectory' in configFields:
                        listener.fileDirectory = configFields['fileDirectory']
                else:
                        listener.fileDirectory = defaultFileDirectory
		
		if 'filenamePrefix' in configFields:
                        listener.filenamePrefix = configFields['filenamePrefix']
                else:
                        listener.filenamePrefix = defaultFilenamePrefix

		if 'fileFields' in configFields:
                        listener.fileFields = configFields['fileFields'].split(';')
                else:
                        listener.fileFields = defaultFileFields

		if 'fileHeader' in configFields:
                        listener.fileHeader = configFields['fileHeader']
                else:
                        listener.fileHeader = defaultFileHeader

		if 'fileFieldHeader' in configFields:
                        listener.fileFieldHeader = configFields['fileFieldHeader']
                else:
                        listener.fileFieldHeader = defaultFileFieldHeader

		if 'fileFieldFormat' in configFields:
                        listener.fileFieldFormat = configFields['fileFieldFormat']
                else:
                        listener.fileFieldFormat = defaultFileFieldFormat

		if 'fileFieldNotAvaliable' in configFields:
                        listener.fileFieldNotAvaliable = configFields['fileFieldNotAvaliable']
                else:
                        listener.fileFieldNotAvaliable = defaultFileFieldNotAvaliable

		if 'fileFieldFooter' in configFields:
                        listener.fileFieldFooter = configFields['fileFieldFooter']
                else:
                        listener.fileFieldFooter = defaultFileFieldFooter

		if 'fileFooter' in configFields:
                        listener.fileFooter = configFields['fileFooter']
                else:
                        listener.fileFooter = defaultFileFooter

                if 'debugOutput' in configFields:
                        listener.debugOutput = configFields['debugOutput']
                else:
                        listener.debugOutput = defaultDebugOutput


                # sys.stdout.write("Config fields:\n%r\n" % configFields)
                # sys.stdout.flush()

		# create stomp connection
		msgServer = defaultMsgServer
		if 'msgServer' in configFields:
                        msgServer = configFields['msgServer']
		msgServerPort = defaultMsgServerPort
		if 'msgServerPort' in configFields:
                        msgServerPort = int(configFields['msgServerPort'])		

		conn = stomp.Connection([(msgServer,msgServerPort)])

		# loop
		while True:
			if not listener.connected:
				listener.connectedCounter -= 1
				if listener.connectedCounter <= 0:
					# remove listeners
					# conn.remove_listener('topiclistener')

					# start connection
					conn.set_listener('topiclistener', listener)
                			conn.start()
                			conn.connect()				
					conn.subscribe(destination=listener.topic, ack='auto')
	
                                        listener.connectedCounter = 100
			time.sleep(1)
		
		sys.stdout.write("%s ended\n" % self.name)
                sys.stdout.flush()

if __name__ == "__main__":
	daemon = TopicDaemon(defaultDaemonPIDFile, defaultDaemonStdIn, defaultDaemonStdOut, defaultDaemonStdErr, defaultDaemonName)
	if len(sys.argv) == 2:
		if 'start' == sys.argv[1]:
			daemon.start()
		elif 'stop' == sys.argv[1]:
			daemon.stop()
		elif 'restart' == sys.argv[1]:
			daemon.restart()
		elif 'status' == sys.argv[1]:
                        daemon.status()
		else:
			print "Unknown command"
			sys.exit(2)
		sys.exit(0)
	else:
		print "usage: %s start|stop|restart|status" % sys.argv[0]
		sys.exit(2)
