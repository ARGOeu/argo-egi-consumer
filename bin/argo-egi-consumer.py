#!/usr/bin/python

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

from argo_egi_consumer.reader import MessageReader
from argo_egi_consumer.writer import MessageWriter
from argo_egi_consumer.log import ProxyMsgLogger
from argo_egi_consumer.config import ProxyConsumerConf

import argparse
import signal
import stomp
import pwd
import datetime
import logging, pprint
import sys, os, time, atexit

# topis deamon defaults
pidfile = '/var/run/argo-egi-consumer.pid'
daemonname = 'argo-egi-consumer'
user = 'arstats'
log = None

class Daemon:
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null',
                 stderr='/dev/null', name=daemonname):
        self.name = name
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile

    def _daemonize(self, nofork):
        if not nofork:
            try:
                pid = os.fork()
                if pid > 0:
                    raise SystemExit(0)
            except OSError, e:
                log.error("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
                raise SystemExit(1)

            os.chdir("/")
            os.umask(0)
            try:
                # decouple from parent environment
                os.setsid()
            except OSError as e:
                log.error('%s %s' % (str(self.__class__), e))
                raise SystemExit(1)

            # do second fork
            try:
                pid = os.fork()
                if pid > 0:
                    raise SystemExit(0)
            except OSError, e:
                log.error("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
                sys.exit(1)

            # redirect standard file descriptors
            sys.stdout.flush()
            sys.stderr.flush()
            si = file(self.stdin, 'r')
            so = file(self.stdout, 'a+')
            se = file(self.stderr, 'a+', 0)
            os.dup2(si.fileno(), sys.stdin.fileno())
            os.dup2(so.fileno(), sys.stdout.fileno())
            os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        atexit.register(self._delpid)
        pid = str(os.getpid())

        try:
            file(self.pidfile,'w+').write("%s\n" % pid)
        except (IOError, OSError) as e:
            log.error('%s %s' % (str(self.__class__), e))
            sys.exit(1)

        try:
            uinfo = pwd.getpwnam(user)
            os.chown(self.pidfile, uinfo.pw_uid, uinfo.pw_gid)
            os.setegid(uinfo.pw_gid)
            os.seteuid(uinfo.pw_uid)
        except (OSError, IOError) as e:
            log.error('%s %s' % (str(self.__class__), e))
            raise SystemExit(1)

    def _delpid(self):
        try:
            os.seteuid(0)
            os.setegid(0)
            os.remove(self.pidfile)
        except (IOError, OSError) as e:
            log.error('%s %s' % (str(self.__class__), e))
            raise SystemExit(1)

    def _is_pid_running(self, pid):
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def _setup_sighandlers(self):
        def sigtermcleanup(signum, frame):
            log.info('Caught SIGTERM')
            try:
                self.reader.conn.stop()
                self.reader.conn.disconnect()
            except stomp.exception.NotConnectedException:
                log.info('Ended')
                try:
                    while 1:
                        raise SystemExit(0)
                        time.sleep(0.1)
                except OSError, err:
                    err = str(err)
                    if err.find("No such process") > 0:
                        if os.path.exists(self.pidfile):
                            os.remove(self.pidfile)
                    else:
                        self.log.error(err)
                        raise SystemExit(1)

        signal.signal(signal.SIGTERM, sigtermcleanup)

        def sighupcleanup(signum, frame):
            log.info('Caught SIGHUP')
            self.reader.load()
            self.reader.listener.load()
            self.reader.listener.writer.load()
            log.info('Config reload')
            try:
                self.reader.conn.stop()
                self.reader.conn.disconnect()
            except stomp.exception.NotConnectedException:
                self.reader.conn.start()
                self.reader.conn.connect()
                log.info('Subscribed to %s' % repr(self.reader.topics))
                for topic in self.reader.topics:
                    self.reader.conn.subscribe(destination=topic, ack='auto')

        signal.signal(signal.SIGHUP, sighupcleanup)

    def start(self, nofork):
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            if self._is_pid_running(pid):
                message = "pidfile %s already exist. Daemon already running?\n"
                log.error(message)
                sys.exit(1)
            else:
                self._delpid

        log.info("Started")
        self._setup_sighandlers()
        # Start the daemon
        self._daemonize(nofork)
        self._run()

    def stop(self):
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            log.error(message)
            return # not an error in a restart
        else:
            os.kill(pid, signal.SIGTERM)

    def restart(self, nofork):
        self.stop()
        self.start(nofork)

    def status(self):
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            if self._is_pid_running(pid):
                log.info("%i is running..." % (pid))
                return 0
            else:
                log.info("Stopped")
                return 3

        else:
            log.info("Stopped")
            return 3

    def _run(self):
        self.reader = MessageReader()
        self.reader.run()

def main():
    daemon = Daemon(pidfile, name=daemonname)
    global log
    log = ProxyMsgLogger()

    parser = argparse.ArgumentParser()
    parser.add_argument('--start', action='store_true')
    parser.add_argument('--stop', action='store_true')
    parser.add_argument('--restart', action='store_true')
    parser.add_argument('--nofork', action='store_true')
    parser.add_argument('--status', action='store_true')
    args = parser.parse_args()

    if args.start:
        daemon.start(args.nofork)
    elif args.stop:
        daemon.stop()
    elif args.restart:
        daemon.restart(args.nofork)
    elif args.status:
        daemon.status()
    else:
        print parser.print_help()
        raise SystemExit(2)

    raise SystemExit(0)

main()
