
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
import socket
import signal
import sys
import time
import traceback
import threading
from collections import deque
from argo_egi_consumer.writer import MessageWriter
from argo_egi_consumer.log import ProxyMsgLogger
from argo_egi_consumer.config import ProxyConsumerConf

thlock = threading.Lock()

class DestListener(stomp.ConnectionListener):
    def __init__(self, config):
        self._log = ProxyMsgLogger()
        self.conf = ProxyConsumerConf(config)
        self.connected = False
        self.connectedCounter = 100
        self.writer = MessageWriter(config, thlock)
        self.messagesWriten = 0
        self.load()

    def load(self):
        pass

    def on_connected(self, headers, body):
        self._log.info('Listener connected, session %s' % headers['session'])
        self.connected = True
        self.connectedCounter = 100

    def on_disconnected(self):
        self._log.warning("Listener disconnected")
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

            self.writer.writeMessage(fields)
            self.messagesWriten += 1

        except Exception as inst:
            self._log.error('Error parsing message: HEADERS: %s BODY: %s' % (headers, message))


class MessageReader:
    def __init__(self, config):
        self.log = ProxyMsgLogger()
        self.conf = ProxyConsumerConf(config)
        self.listener = DestListener(config)
        self._listconns = []
        self.reconnect = False
        self._ths = []
        self.load()

    def load(self):
        self.conf.parse()
        self.msgServers = deque(self.conf.get_option('BrokerServer'.lower()))
        self.listenerIdleTimeout = int(self.conf.get_option('SubscriptionIdleMsgTimeout'.lower()))
        ldest = self.conf.get_option('SubscriptionDestinations'.lower()).split(',')
        self.destinations = [t.strip() for t in ldest]
        self.useSSL = eval(self.conf.get_option('STOMPUseSSL'.lower()))
        self.keepaliveidle = int(self.conf.get_option('STOMPTCPKeepAliveIdle'.lower()))
        self.keepaliveint = int(self.conf.get_option('STOMPTCPKeepAliveInterval'.lower()))
        self.keepaliveprobes = int(self.conf.get_option('STOMPTCPKeepAliveProbes'.lower()))
        self.reconnects = int(self.conf.get_option('STOMPReconnectAttempts'.lower()))
        self.SSLCertificate = self.conf.get_option('AuthenticationHostKey'.lower())
        self.SSLKey = self.conf.get_option('AuthenticationHostCert'.lower())
        self._hours = self.conf.get_option('GeneralReportWritMsgEveryHours'.lower(), optional=True)
        self._infowrittmsg_everysec = 60*60*int(self._hours) if self._hours else 60*60*24


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
        self.log.info("Cycle to broker %s:%i" % (server[0], server[1]))
        self._listconns.append(self.conn)
        self.msgServers.rotate(-1)

        self.conn.set_listener('DestListener', self.listener)

        try:
            self.conn.start()
            self.conn.connect()
            for dest in self.destinations:
                self.conn.subscribe(destination=dest, ack='auto')
            self.log.info('Subscribed to %s' % repr(self.destinations))
            self.listener.connectedCounter = 100
        except:
            self.log.error('Connection to broker %s:%i failed after %i retries' % (server[0], server[1],
                                                                            self.reconnects))
            self.listener.connectedCounter = 10

    def _deferwritmsgreport(self, e):
        while True:
            time.sleep(self._infowrittmsg_everysec)
            if self.listener.connected and not e.isSet():
                self.log.info('Written %i messages in %s hours' %
                              (self.listener.messagesWriten, self._hours))
                self.listener.messagesWriten = 0
            else:
                break

    def run(self):
        # loop
        self.listener.connectedCounter = 0
        loopCount = 0

        while True:
            self.reconnect = False

            #check connection
            if not self.listener.connected:
                self.listener.connectedCounter -= 1
                self.reconnect = True

            else:
                if self.listenerIdleTimeout > 0 and loopCount >= self.listenerIdleTimeout:
                    if not self.listener.messagesWriten > 0:
                        self.reconnect = True
                        loopCount = 0
                        self.listener.messagesWriten = 0
                        self.log.info('Listener did not receive any message in %s seconds' % self.listenerIdleTimeout)

            if self.reconnect:
                if self._ths:
                    self._e.set()
                self._e = threading.Event()
                t = threading.Thread(target=self._deferwritmsgreport, args=(self._e,))
                t.daemon = True
                t.start()
                self._ths.append(t)

                if self._listconns:
                    for conn in self._listconns:
                        try:
                            conn.stop()
                            conn.disconnect()
                        except (socket.error, stomp.exception.NotConnectedException):
                            self.listener.connected = False
                    self._listconns = []
                self.connect()

            loopCount += 1
            time.sleep(1)
