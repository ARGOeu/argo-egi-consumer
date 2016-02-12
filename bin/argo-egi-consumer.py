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
from argo_egi_consumer.writer import MsgLogger
from argo_egi_consumer.shared import SingletonShared as Shared
from argo_egi_consumer.config import ConsumerConf

import argparse
import datetime
import hashlib
import logging, logging.handlers
import pwd
import signal
import socket
import stomp
import sys, os, time, atexit
import threading

# topic deamon defaults
pidfile = '/var/run/argo-egi-consumer-%s.pid'
daemonname = 'argo-egi-consumer'
user = 'arstats'
conf, log = None, None
sh = Shared()

class Daemon:
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null',
                 stderr='/dev/null', name=daemonname, nofork=True):
        self.name = name
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self._nofork = nofork

    def _daemonize(self):
        handler = logging.StreamHandler()
        sh.Logger.addHandler(handler)
        if not self._nofork:
            try:
                pid = os.fork()
                if pid > 0:
                    raise SystemExit(0)
            except OSError, e:
                sh.Logger.error("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
                sh.Logger.removeHandler(handler)
                raise SystemExit(1)

            os.chdir("/")
            os.umask(0)
            try:
                # decouple from parent environment
                os.setsid()
            except OSError as e:
                sh.Logger.error('%s %s' % (str(self.__class__), e))
                sh.Logger.removeHandler(handler)
                raise SystemExit(1)

            # do second fork
            try:
                pid = os.fork()
                if pid > 0:
                    raise SystemExit(0)
            except OSError, e:
                sh.Logger.error("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
                sh.Logger.removeHandler(handler)
                raise SystemExit(1)

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
            sh.Logger.error('%s %s' % (str(self.__class__), e))
            sh.Logger.removeHandler(handler)
            raise SystemExit(1)

        try:
            uinfo = pwd.getpwnam(user)
            os.chown(self.pidfile, uinfo.pw_uid, uinfo.pw_gid)
            os.setegid(uinfo.pw_gid)
            os.seteuid(uinfo.pw_uid)
        except (OSError, IOError) as e:
            sh.Logger.error('%s %s' % (str(self.__class__), e))
            sh.Logger.removeHandler(handler)
            raise SystemExit(1)

    def _delpid(self):
        handler = logging.StreamHandler()
        sh.Logger.addHandler(handler)
        try:
            os.seteuid(0)
            os.setegid(0)
            os.remove(self.pidfile)
        except (IOError, OSError) as e:
            sh.Logger.error('%s %s' % (str(self.__class__), e))
            sh.Logger.removeHandler(handler)
            raise SystemExit(1)

    def _is_pid_running(self, pid):
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def _setup_sighandlers(self):
        def sigtermcleanup(signum, frame):
            sh.Logger.info('Caught SIGTERM')
            sh.eventterm.set()
            try:
                self.reader.conn.stop()
                self.reader.conn.disconnect()
            except stomp.exception.NotConnectedException:
                sh.Logger.info('Disconnected: %s:%i' % (self.reader.server[0], self.reader.server[1]))
                if os.path.exists(self.pidfile):
                    sh.Logger.info('Removing pidfile: %s' % self.pidfile)
                    os.remove(self.pidfile)
                sh.Logger.info('Ended')
                os._exit(0)

        signal.signal(signal.SIGTERM, sigtermcleanup)

        def sigintcleanup(signum, frame):
            sh.Logger.info('Caught SIGINT')
            try:
                self.reader.conn.stop()
                self.reader.conn.disconnect()
            except stomp.exception.NotConnectedException:
                sh.Logger.info('Disconnected: %s:%i' % (self.reader.server[0], self.reader.server[1]))
                if os.path.exists(self.pidfile):
                    sh.Logger.info('Removing pidfile: %s' % self.pidfile)
                    os.remove(self.pidfile)
                sh.Logger.info('Ended')
                os._exit(0)

        signal.signal(signal.SIGINT, sigintcleanup)

        def sigusr1handle(signum, frame):
            sh.Logger.info('Caught SIGUSR1')
            sh.eventusr1.set()

        signal.signal(signal.SIGUSR1, sigusr1handle)

        def sighuphandle(signum, frame):
            sh.Logger.info('Caught SIGHUP')
            self.reader.load()
            self.reader.listener.load()
            self.reader.listener.writer.load()
            sh.Logger.info('Config reload')

        signal.signal(signal.SIGHUP, sighuphandle)

    def start(self):
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            if self._is_pid_running(pid):
                message = "pidfile %s already exist. Daemon already running?\n" % self.pidfile
                sh.Logger.error(message)
                raise SystemExit(1)
            else:
                self._delpid

        self._setup_sighandlers()
        # Start the daemon
        self._daemonize()
        self._run()

    def stop(self):
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n" % self.pidfile
            sh.Logger.error(message)
            return
        else:
            os.kill(pid, signal.SIGTERM)

    def restart(self):
        self.stop()
        self.start()

    def status(self):
        # Get the pid from the pidfile
        handler = logging.StreamHandler()
        global log
        sh.Logger.addHandler(handler)
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            if self._is_pid_running(pid):
                sh.Logger.info("%i is running..." % (pid))
                sh.Logger.removeHandler(handler)
                return 0
            else:
                sh.Logger.info("Stopped")
                sh.Logger.removeHandler(handler)
                return 3

        else:
            sh.Logger.info("Stopped")
            sh.Logger.removeHandler(handler)
            return 3

    def _run(self):
        self.reader = MessageReader()
        sh.Logger.info("Started")
        sh.seta('stime', time.time())
        self.reader.run()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', action='store_true')
    parser.add_argument('--stop', action='store_true')
    parser.add_argument('--restart', action='store_true')
    parser.add_argument('--config', nargs=1, required=True)
    parser.add_argument('--nofork', action='store_true')
    parser.add_argument('--status', action='store_true')
    args = parser.parse_args()

    sh.seta('ConsumerConf', ConsumerConf(args.config[0]))
    sh.ConsumerConf.parse()
    sh.seta('eventusr1', threading.Event())
    sh.seta('eventterm', threading.Event())
    sh.seta('thlock', threading.Lock())
    clname = sh.ConsumerConf.get_option('GeneralLogName'.lower(), optional=True)
    sh.seta('Logger', MsgLogger(clname if clname else os.path.basename(sys.argv[0])))
    sh.seta('nummsg', 0)
    md = hashlib.md5()
    md.update(args.config[0])
    daemon = Daemon(pidfile % md.hexdigest(), name=daemonname, nofork=args.nofork)

    if args.start:
        daemon.start()
    elif args.stop:
        daemon.stop()
    elif args.restart:
        daemon.restart()
    elif args.status:
        daemon.status()
    else:
        print parser.print_help()
        raise SystemExit(2)

    raise SystemExit(0)

main()
