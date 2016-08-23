
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
from argo_egi_consumer.writer import MessageWriterFile
from argo_egi_consumer.shared import SingletonShared as Shared

sh = Shared()

class DestListener(stomp.ConnectionListener):
    def __init__(self):
        self.connected = False
        self.connectedCounter = 100
        self.writer = MessageWriterFile()

    def load(self):
        pass

    def on_connected(self, headers, body):
        sh.Logger.info('Listener connected, session %s' % headers['session'])
        self.connected = True
        self.connectedCounter = 100

    def on_disconnected(self):
        sh.Logger.warning("Listener disconnected")
        self.connected = False

    def on_error(self, headers, message):
        sh.Logger.error("Received error %s" % message)

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
                fields[key] = value.decode('utf-8', 'replace')

        self.writer.writeMessage(fields)
        sh.nummsg += 1

class MessageReader:
    def __init__(self):
        self.listener = DestListener()
        self._listconns = []
        self._ths = []
        self._wastupleserv = None
        self._reconnconfreload = False
        self.load()

    def load(self):
        sh.ConsumerConf.parse()

        tupleserv = sh.ConsumerConf.get_option('BrokerServer'.lower())
        if self._wastupleserv and tupleserv != self._wastupleserv:
            self._reconnconfreload = True
        else:
            self._reconnconfreload = False
        self._wastupleserv = tupleserv
        self.msgServers = deque(tupleserv)

        self.listenerIdleTimeout = sh.ConsumerConf.get_option('SubscriptionIdleMsgTimeout'.lower())
        self.destinations = sh.ConsumerConf.get_option('SubscriptionDestinations'.lower())
        self.useSSL = sh.ConsumerConf.get_option('STOMPUseSSL'.lower())
        self.keepaliveidle = sh.ConsumerConf.get_option('STOMPTCPKeepAliveIdle'.lower())
        self.keepaliveint = sh.ConsumerConf.get_option('STOMPTCPKeepAliveInterval'.lower())
        self.keepaliveprobes = sh.ConsumerConf.get_option('STOMPTCPKeepAliveProbes'.lower())
        self.reconnects = sh.ConsumerConf.get_option('STOMPReconnectAttempts'.lower())
        self.SSLCertificate = sh.ConsumerConf.get_option('AuthenticationHostKey'.lower())
        self.SSLKey = sh.ConsumerConf.get_option('AuthenticationHostCert'.lower())
        self._hours = sh.ConsumerConf.get_option('GeneralReportWritMsgEveryHours'.lower(), optional=True)
        self._nummsgs_evsec = 3600*float(self._hours) if self._hours else 3600*24

    def connect(self):
        # cycle msg server
        self.server = self.msgServers[0]
        self.conn = stomp.Connection([self.server],
                            keepalive=('linux',
                                        self.keepaliveidle,
                                        self.keepaliveint,
                                        self.keepaliveprobes),
                            reconnect_attempts_max=self.reconnects,
                            use_ssl=self.useSSL,
                            ssl_key_file=self.SSLKey,
                            ssl_cert_file=self.SSLCertificate)
        sh.Logger.info("Cycle to broker %s:%i" % (self.server[0], self.server[1]))
        self._listconns.append(self.conn)
        self.msgServers.rotate(-1)
        self.wasserver = self.server

        self.conn.set_listener('DestListener', self.listener)

        try:
            self.deststr = ''
            self.conn.start()
            self.conn.connect()
            for dest in self.destinations:
                self.conn.subscribe(destination=dest, ack='auto')
                self.deststr = self.deststr + dest + ', '
            sh.Logger.info('Subscribed to %s' % (self.deststr[:len(self.deststr) - 2]))
            self.listener.connectedCounter = 100
            self.tconn = time.time()
        except:
            sh.Logger.error('Connection to broker %s:%i failed after %i retries' % (self.server[0], self.server[1],
                                                                            self.reconnects))
            self.listener.connectedCounter = 10

    def _deferwritmsgreport(self):
        s = 0
        while True:
            if sh.eventusr1.isSet():
                now = time.time()
                dur = now - sh.stime
                sh.Logger.info('Connected to %s:%i for %.2f hours' % (self.server[0], self.server[1], (now - self.tconn)/3600))
                sh.Logger.info('Subscribed to %s' % (self.deststr[:len(self.deststr) - 2]))
                sh.Logger.info('Written %i messages in %.2f hours' %
                            (sh.nummsg, dur/3600 if dur/3600 < float(self._hours) else float(self._hours)))
                sh.eventusr1.clear()
            if sh.eventterm.isSet():
                dur = time.time() - sh.stime
                sh.Logger.info('Written %i messages in %.2f hours' %
                            (sh.nummsg, dur/3600 if dur/3600 < float(self._hours) else float(self._hours)))
                break
            if s < self._nummsgs_evsec:
                sh.eventterm.wait(2.0)
                s += 2
            else:
                if self.listener.connected:
                    sh.Logger.info('Written %i messages in %.2f hours' %
                                (sh.nummsg, float(self._hours)))
                    sh.nummsg, s = 0, 0
                    sh.stime = time.time()

    def run(self):
        # loop
        self.listener.connectedCounter = 0
        loopCount = 0

        self.th = threading.Thread(target=self._deferwritmsgreport, name='msgwritreport_thread')
        self.th.start()

        while True:
            self.reconnect = False

            #check connection
            if not self.listener.connected:
                self.listener.connectedCounter -= 1
                self.reconnect = True

            else:
                if self.listenerIdleTimeout > 0 and loopCount >= self.listenerIdleTimeout:
                    if not sh.nummsg > 0:
                        self.reconnect = True
                        loopCount = 0
                        sh.nummsg = 0
                        sh.Logger.info('Listener did not receive any message in %s seconds' % self.listenerIdleTimeout)

            if self.reconnect or self._reconnconfreload:
                if self._listconns:
                    for conn in self._listconns:
                        try:
                            conn.stop()
                            conn.disconnect()
                        except (socket.error, stomp.exception.NotConnectedException):
                            sh.Logger.info('Disconnected: %s:%i' % (self.wasserver[0], self.wasserver[1]))
                            self.listener.connected = False
                            self._reconnconfreload = False
                    self._listconns = []
                self.connect()

            loopCount += 1
            time.sleep(1)
