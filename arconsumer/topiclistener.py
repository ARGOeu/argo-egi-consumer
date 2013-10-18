import time
import sys
import logging
import stomp
import sys
import datetime
from os import path
from writter import MessageWritter

class TopicListener(stomp.ConnectionListener):
    
    def __init__(self): 
	# connection
        self.connected = False
        self.connectedCounter = 100
        # topic
	self.topic = None
	# output
	self.debugOutput = 0 
        # meassage writter
        self.messageWritter = None;
        self.messagesWritten = 0;
    
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
        sys.stdout.write(self.createLogEntry("Listener disconnected"))
        sys.stdout.flush()
        self.connected = False

    def on_error(self, headers, message):
	sys.stdout.write(self.createLogEntry("Received error %s" % message))
        sys.stdout.flush()

    def on_message(self, headers, message):
        lines = message.split('\n')
        fields = dict()
	
	#header fields
	fields.update(headers)

	# body fields
        for line in lines:
            splitLine = line.split(': ')
            if len(splitLine) > 1:
                key = splitLine[0]
                value = splitLine[1]
                fields[key] = value

	if self.debugOutput:
            sys.stdout.write(self.createLogEntry('-' * 20))
            sys.stdout.write('Message Header:\n %s' % headers)
            sys.stdout.write('Message Body:\n %s' % message)
            sys.stdout.flush()
	
        if self.messageWritter is not None:
            try:
	        self.messageWritter.writeMessage(fields);
                self.messagesWritten = self.messagesWritten + 1 
            except:
                self.connectedCounter = -1
                self.connected = False
                sys.stdout.write('--- Error parsing Message ---\nHeaders:\n%s\nBody:\n%s\n---\n' % (headers, message))
                sys.stdout.flush()

        if self.debugOutput:
            sys.stdout.write('msg sent to writter\n\n')
            sys.stdout.flush()
