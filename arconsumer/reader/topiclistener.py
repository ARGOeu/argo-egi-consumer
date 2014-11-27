
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
from os import path
#from writter import MessageWritter

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
        self.messageWritters = [];
        self.messagesWritten = 0;
    
    def createLogEntry(self, msg):
       return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' ' + msg + '\n'

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
            splitLine = line.split(': ', 1)
            if len(splitLine) > 1:
                key = splitLine[0]
                value = splitLine[1]
                fields[key] = value

        if self.debugOutput:
            sys.stdout.write(self.createLogEntry('-' * 20))
            sys.stdout.write('Message Header:\n %s' % headers)
            sys.stdout.write('Message Body:\n %s' % message)
            sys.stdout.flush()
        
        try:
            for messageWritter in self.messageWritters:
                messageWritter.writeMessage(fields);
            self.messagesWritten = self.messagesWritten + 1 
        
        except Exception as inst:
            self.connectedCounter = -1
            self.connected = False
            sys.stderr.write(self.createLogEntry('--- Error parsing Message ---\nHeaders:\n%s\nBody:\n%s\n---\n' % (headers, message)))
            sys.stderr.flush()
        
        if self.debugOutput:
            sys.stdout.write(self.createLogEntry('msg sent to writter\n\n'))
            sys.stdout.flush()
