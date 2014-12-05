
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

import sys
import os
import time
import stomp
import logging
import datetime
import threading
from messagereader import MessageReader
from topiclistener import TopicListener

defaultMsgServers = ['msg.cro-ngi.hr']
defaultMsgServerPort = 6163
defaultListenerIdleTimeout = 0
defaultServerReconnectCycle = 1
defaultTopics = ['/topic/grid.probe.metricOutput.EGEE.ngi.*']
defaultDebugOutput = False
defaultUseSSL = True
defaultSSLCertificate = '/etc/grid-security/hostcert.pem'
defaultSSLKey = '/etc/grid-security/hostkey.pem'

class MessageActiveMQReader(MessageReader):

    def __init__(self):
        self.msgServer = ''
        self.msgServers = defaultMsgServers
        self.msgServerPort = defaultMsgServerPort
        self.listenerIdleTimeout = defaultListenerIdleTimeout
        self.serverReconnectCycle = defaultServerReconnectCycle
        self.topics = defaultTopics
        self.debugOutput = defaultDebugOutput
        self.useSSL = defaultUseSSL
        self.SSLCertificate = defaultSSLCertificate
        self.SSLKey = defaultSSLKey
        self.writters = list()
        self.thread = None

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

        # Apply config
        if 'topics' in configFields:
            self.topics = configFields['topics'].split(';')
        if 'msgServers' in configFields:
            self.msgServers = configFields['msgServers'].split(';')
        if 'msgServerPort' in configFields:
            self.msgServerPort = int(configFields['msgServerPort'])
        if 'debugOutput' in configFields:
            self.debugOutput = configFields['debugOutput']
        if 'listenerIdleTimeout' in configFields:
            self.listenerIdleTimeout = configFields['listenerIdleTimeout']
        if 'serverReconnectCycle' in configFields:
            self.serverReconnectCycle = configFields['serverReconnectCycle'] 
                
        # SSL               
        if 'useSSL' in configFields:
            self.useSSL = configFields['useSSL'] == 1
        if 'SSLCertificate' in configFields:
            self.SSLCertificate = configFields['SSLCertificate']
        if 'SSLKey' in configFields:
            self.SSLKey = configFields['SSLKey']


    def addWritter(self, writter):
        self.writters.append(writter)
        """Set message writter"""


    def start(self):
        self.thread = threading.Thread(target=self.run, args=())
        self.thread.start()


    def run(self):
        # create listener
        listener = TopicListener()
        listener.topics = self.topics

        # message writters
        for writter in self.writters:
            listener.messageWritters.append(writter)

        # message server
        self.msgServer = self.msgServers[0]

        # loop
        retryCount = 0
        listener.connectedCounter = 0
        listener.debugOutput = self.debugOutput
        loopCount = 0
        serverReconnect = self.serverReconnectCycle + 1
        while True:

            reconnect = False
        
            #check connection
            if not listener.connected:
                listener.connectedCounter -= 1
                if listener.connectedCounter <= 0:              
                    reconnect = True
            
            else:

                ###########################################################
                # RECONNECT ON no new messages in a while
                # If the listener does not receive a new message in the
                # defined timeout (listenerIdleTimeout) the application
                # will reconnect.
                # If this setting is set to 0 this check will not apply.
                ###########################################################

                if self.listenerIdleTimeout > 0 and loopCount >= self.listenerIdleTimeout:
                                        
                    if not listener.messagesWritten > 0:
                        reconnect = True
                        loopCount = 0
                        listener.messagesWritten = 0

            if reconnect:
                # cycle msg server
                if serverReconnect >= self.serverReconnectCycle:

                    ###########################################################
                    # STOMP connection
                    # Have to set maual options for keepalive and reconnect attempts
                    # if not set defaults are used.
                    #
                    # KEEPALIVE
                    # keepalive = ( 'linux', TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT )
                    #   'linux' - hase to be 'linux' - stomp.py requirements
                    #   TCP_KEEPALIVE - idle time in between keep-alives when there is a response from the peer
                    #   TCP_KEEPINTVL - interval between keep-alives when there is no response from the peer, this is done to probe the peer until there is a response.
                    #   TCP_KEEPCNT - number of times keep-alives are repeated before a close when there is no response
                    #
                    # RECONNECT_ATTEMPTS_MAX
                    # reconnect_attempts_max=1000
                    #   Max number of reconnect attempts
                    ###########################################################

                    msgServerIdx = self.msgServers.index(self.msgServer)
                    msgServerIdx = (msgServerIdx+1) % len(self.msgServers)
                    self.msgServer = self.msgServers[msgServerIdx]
                    conn = stomp.Connection([(self.msgServer,self.msgServerPort)], keepalive=('linux', 20, 5, 10), reconnect_attempts_max=10, use_ssl=self.useSSL, ssl_key_file=self.SSLKey, ssl_cert_file=self.SSLCertificate)
                    serverReconnect = 0
                    retryCount = 0                       
                    sys.stdout.write(self.createLogEntry("Cycle to broker: %s\n" % self.msgServer))
                    sys.stdout.flush()

                # message writters
                for writter in self.writters:
                    listener.messageWritters.append(writter)
    
                # start connection
                serverReconnect +=1
                try:
                    conn.disconnect()
                except:
                    # do notihng
                conn.set_listener('topiclistener', listener)
                try:
                    conn.start()
                    conn.connect()
                    for topic in listener.topics:           
                        conn.subscribe(destination=topic, ack='auto')
                    retryCount = 0
                    listener.connectedCounter = 100
                except:
                    sys.stdout.write(self.createLogEntry("Reconnect %i FAILED\n" % retryCount))
                    sys.stdout.flush()

                    retryCount += 1
                    listener.connectedCounter = 10
                                            
            loopCount = loopCount + 1
            time.sleep(1) 


    def isRunning(self):
        self.thread.isAlive()

    def createLogEntry(self, msg):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' ' + msg + '\n'








