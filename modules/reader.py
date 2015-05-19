
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

import datetime
import decimal
import logging
import os
import stomp
import sys
import time
import traceback
import threading
from collections import deque
from argo_egi_consumer.writer import MessageWriter
from argo_egi_consumer.log import ProxyMsgLogger
from argo_egi_consumer.config import ProxyConsumerConf


class TopicListener(stomp.ConnectionListener):
    def __init__(self):
        self._log = ProxyMsgLogger()
        self.conf = ProxyConsumerConf()
        self.connected = False
        self.connectedCounter = 100
        self.topic = None
        self.writer = MessageWriter()
        self.messagesWriten = 0
        self.load()

    def load(self):
        hours = self.conf.get_option('GeneralReportWritMsgEveryHours'.lower(), optional=True)
        self._infowrittmsg_everysec = 60*60*int(hours) if hours else 60*60*24

    def _deferwritmsgreport(self):
        while True:
            time.sleep(self._infowrittmsg_everysec)
            if self.connected:
                self._log.info('Written %i messages in hours' % (self.messagesWriten))
                self.messagesWriten = 0

    def on_connected(self, headers, body):
        self._log.info("TopicListener connected, session %s" % headers['session'])
        self.connected = True
        self.connectedCounter = 100
        t = threading.Thread(target=self._deferwritmsgreport)
        t.start()

    def on_disconnected(self):
        self._log.warning("TopicListener disconnected")
        self.connected = False

    def on_error(self, headers, message):
        self._log.error("Received error %s" % message)

    def on_message(self, headers, message):
        lines = message.split('\n')
        fields = dict()

        try:
            #header fields
            fields.update(headers)
            # body fields
            for line in lines:
                splitLine = line.split(': ', 1)
                if len(splitLine) > 1:
                    key = splitLine[0]
                    value = splitLine[1]
                    fields[key] = value.decode('utf-8', 'replace')
        except Exception as inst:
            self.connectedCounter = -1
            self.connected = False
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback)
            self._log.error('Error parsing message: HEADERS: %s BODY: %s' % (headers, message))

        self.writer.writeMessage(fields)
        self.messagesWriten += 1


class MessageReader:
    def __init__(self):
        self.log = ProxyMsgLogger()
        self.conf = ProxyConsumerConf()
        self.listener = TopicListener()
        self.reconnect = False
        self.load()

    def load(self):
        self.conf.parse()
        self.msgServers = deque(self.conf.get_option('BrokerServer'.lower()))
        self.listenerIdleTimeout = int(self.conf.get_option('SubscriptionIdleMsgTimeout'.lower()))
        ltopics = self.conf.get_option('SubscriptionDestinations'.lower()).split(',')
        self.topics = [t.strip() for t in ltopics]
        self.useSSL = eval(self.conf.get_option('STOMPUseSSL'.lower()))
        self.keepaliveidle = int(self.conf.get_option('STOMPTCPKeepAliveIdle'.lower()))
        self.keepaliveint = int(self.conf.get_option('STOMPTCPKeepAliveInterval'.lower()))
        self.keepaliveprobes = int(self.conf.get_option('STOMPTCPKeepAliveProbes'.lower()))
        self.reconnects = int(self.conf.get_option('STOMPReconnectAttempts'.lower()))
        self.SSLCertificate = self.conf.get_option('AuthenticationHostKey'.lower())
        self.SSLKey = self.conf.get_option('AuthenticationHostCert'.lower())

    def connect(self):
        # cycle msg server
        server = self.msgServers[0]
        self.conn = stomp.Connection([server],
                            keepalive=('linux',
                                        self.keepaliveidle,
                                        self.keepaliveint,
                                        self.keepaliveprobes),
                            reconnect_attempts_max=self.reconnects,
                            use_ssl=self.useSSL,
                            ssl_key_file=self.SSLKey,
                            ssl_cert_file=self.SSLCertificate)
        self.log.info("Cycle to broker %s:%i" % (self.msgServers[0][0], self.msgServers[0][1]))
        self.msgServers.rotate(-1)

        self.conn.set_listener('TopicListener', self.listener)

        try:
            self.conn.start()
            self.conn.connect()
            for topic in self.listener.topics:
                self.conn.subscribe(destination=topic, ack='auto')
            self.listener.connectedCounter = 100
        except:
            self.log.error('Connection to broker %s:%i failed after %i retries' % (server[0], server[1],
                                                                            self.reconnects))
            self.listener.connectedCounter = 10


    def run(self):
        self.listener.topics = self.topics

        # loop
        self.listener.connectedCounter = 0
        loopCount = 0

        while True:
            self.reconnect = False

            #check connection
            if not self.listener.connected:
                self.listener.connectedCounter -= 1
                if self.listener.connectedCounter <= 0:
                    self.reconnect = True

            else:
                if self.listenerIdleTimeout > 0 and loopCount >= self.listenerIdleTimeout:
                    if not self.listener.messagesWriten > 0:
                        self.reconnect = True
                        loopCount = 0
                        self.listener.messagesWriten = 0
                        self.log.info('TopicListener did not receive any message in %s seconds' % self.listenerIdleTimeout)

            if self.reconnect:
                self.connect()

            loopCount += 1
            time.sleep(1)

