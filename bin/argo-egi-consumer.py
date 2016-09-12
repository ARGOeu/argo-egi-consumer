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

from argo_egi_consumer.reader import StompConn
from argo_egi_consumer.writer import MsgLogger, LOGFORMAT, MessageWriterFile, MessageWriterIngestion
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

from collections import deque

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
        self._hours = sh.ConsumerConf.get_option('GeneralReportWritMsgEveryHours'.lower(), optional=True)
        self._nummsgs_evsec = 3600*float(self._hours) if self._hours else 3600*24

    def _report(self):
        def msgs_stat(dur):
            sh.Logger.info('StompConn', 'Received %i messages in %.2f hours' %
                        (sh.nummsgrecv, dur/3600 if dur/3600 < float(self._hours) else float(self._hours)))
            if sh.ConsumerConf.get_option('GeneralWriteMsgFile'.lower()):
                sh.Logger.info('MessageWriterFile', 'Written %i messages in %.2f hours' %
                            (sh.nummsgfile, dur/3600 if dur/3600 < float(self._hours) else float(self._hours)))
            if sh.ConsumerConf.get_option('GeneralWriteMsgIngestion'.lower()):
                sh.Logger.info('MessageWriterIngestion', 'Sent %i messages in %.2f hours' %
                            (sh.nummsging, dur/3600 if dur/3600 < float(self._hours) else float(self._hours)))

        s = 0.0
        while True:
            if sh.eventusr1.isSet():
                now = time.time()
                dur = now - sh.stime
                sh.Logger.info('StompConn', 'Connected to %s:%i for %.2f hours' % (sh.server[0], sh.server[1], (now - sh.tconn)/3600))
                sh.Logger.info('StompConn', 'Subscribed to %s' % (sh.deststr[:len(sh.deststr) - 2]))
                msgs_stat(dur)
                sh.eventusr1.clear()

            if sh.eventterm.isSet():
                dur = time.time() - sh.stime
                for t in sh.writers:
                    t.join()
                msgs_stat(dur)
                break

            if s < self._nummsgs_evsec:
                sh.eventterm.wait(0.2)
                s += 0.2

            else:
                if self.stomp.listener.connected:
                    dur = time.time() - sh.stime
                    sh.Logger.info(self, 'Report every %.2f hour' % float(self._hours))
                    msgs_stat(dur)
                    sh.Logger.info(self, 'Counters reset')
                    sh.nummsgrecv, sh.nummsgfile, sh.nummsging, s = 0, 0, 0, 0
                    sh.stime = time.time()

    def _postdaemon(self):
        thr = threading.Thread(target=self._report, name='report')
        thr.start()
        if sh.ConsumerConf.get_option('GeneralWriteMsgIngestion'.lower()):
            thi = MessageWriterIngestion(100)
            thi.daemon = True
            thi.start()
            sh.writers.append(thi)

        if sh.ConsumerConf.get_option('GeneralWriteMsgFile'.lower()):
            thf = MessageWriterFile(50)
            thf.daemon = True
            thf.start()
            sh.writers.append(thf)

    def _writepid(self, pid):
        pf = file(self.pidfile, 'w+')
        try:
            pf.write("%s\n" % pid)
        except (IOError, OSError) as e:
            sh.Logger.error(self, e)
            sh.Logger.removeHandler(handler)
            raise SystemExit(1)
        finally:
            pf.close()

    def _getpid(self):
        pid = None
        if os.path.exists(self.pidfile):
            with open(self.pidfile, 'r') as con:
                pid = int(con.read().strip())
        return pid

    def _delpid(self):
        handler = logging.StreamHandler()
        sh.Logger.addHandler(handler)
        try:
            os.seteuid(0)
            os.setegid(0)
            sh.Logger.info(self, 'Removing pidfile: %s' % self.pidfile)
            os.remove(self.pidfile)
        except (IOError, OSError) as e:
            sh.Logger.error(self, e)
            sh.Logger.removeHandler(handler)
            raise SystemExit(1)

    def _daemonize(self):
        handler = logging.StreamHandler()
        formatter = logging.Formatter(LOGFORMAT)
        handler.setFormatter(formatter)
        sh.Logger.addHandler(handler)
        if not self._nofork:
            try:
                pid = os.fork()
                if pid > 0:
                    raise SystemExit(0)
            except OSError, e:
                sh.Logger.error(self, "fork #1 failed: %d (%s)" % (e.errno, e.strerror))
                sh.Logger.removeHandler(handler)
                raise SystemExit(1)

            os.chdir("/")
            os.umask(0)
            try:
                # decouple from parent environment
                os.setsid()
            except OSError as e:
                sh.Logger.error(self, e)
                sh.Logger.removeHandler(handler)
                raise SystemExit(1)

            # do second fork
            try:
                pid = os.fork()
                if pid > 0:
                    raise SystemExit(0)
            except OSError, e:
                sh.Logger.error(self, "fork #2 failed: %d (%s)" % (e.errno, e.strerror))
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

        # register exit handler
        atexit.register(self._delpid)

        # write pid file
        self._writepid(str(os.getpid()))

        try:
            uinfo = pwd.getpwnam(user)
            os.chown(self.pidfile, uinfo.pw_uid, uinfo.pw_gid)
            os.setegid(uinfo.pw_gid)
            os.seteuid(uinfo.pw_uid)
        except (OSError, IOError) as e:
            sh.Logger.error(self, e)
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
            sh.Logger.info(self, 'Caught SIGTERM')
            sh.eventterm.set()
            for t in sh.writers:
                if len(sh.msgqueues[t.name]['queue']) > 0:
                    t.write_msg(sh.msgqueues[t.name]['queue'])
                sh.eventtermwrit[t.name].set()

            try:
                self.stomp.conn.stop()
                self.stomp.conn.disconnect()
            except stomp.exception.NotConnectedException:
                sh.Logger.info('StompConn' , 'Disconnected: %s:%i' % (sh.server[0], sh.server[1]))
                raise SystemExit(3)

        signal.signal(signal.SIGTERM, sigtermcleanup)

        def sigintcleanup(signum, frame):
            sh.Logger.info(self, 'Caught SIGINT')
            sh.eventterm.set()
            try:
                self.stomp.conn.stop()
                self.stomp.conn.disconnect()
            except stomp.exception.NotConnectedException:
                sh.Logger.info(self, 'Disconnected: %s:%i' % (sh.server[0], sh.server[1]))
                raise SystemExit(3)

        signal.signal(signal.SIGINT, sigintcleanup)

        def sigusr1handle(signum, frame):
            sh.Logger.info(self, 'Caught SIGUSR1')
            sh.eventusr1.set()

        signal.signal(signal.SIGUSR1, sigusr1handle)

        def sighuphandle(signum, frame):
            sh.Logger.info(self, 'Caught SIGHUP')
            self.stomp.load()
            self.stomp.listener.load()
            for w in sh.writers:
                w.load()
            sh.Logger.info(self, 'Config reload')

        signal.signal(signal.SIGHUP, sighuphandle)

    def start(self, isrestart):
        if not isrestart:
            sh.Logger.warning(self, "Starting...")

        pid = self._getpid()
        if pid:
            if self._is_pid_running(pid):
                message = "pidfile %s already exist. Daemon already running?\n" % self.pidfile
                sh.Logger.error(self, message)
                raise SystemExit(3)
            else:
                self._delpid()

        self._setup_sighandlers()
        # Start the daemon
        self._daemonize()
        self._postdaemon()
        self._run()

    def stop(self, isrestart):
        if not isrestart:
            sh.Logger.warning(self, "Stopping...")

        pid = self._getpid()
        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n" % self.pidfile
            sh.Logger.error(self, message)
            raise SystemExit(3)
        else:
            os.kill(pid, signal.SIGTERM)

    def reload(self):
        pid = self._getpid()
        if pid:
            os.kill(self._getpid(), signal.SIGHUP)

    def restart(self):
        sh.Logger.warning(self, "Restarting...")
        self.stop(True)
        time.sleep(1)
        self.start(True)

    def status(self):
        # Get the pid from the pidfile
        handler = logging.StreamHandler()
        global log
        sh.Logger.addHandler(handler)
        pid = self._getpid()

        if pid:
            if self._is_pid_running(pid):
                sh.Logger.info(self, "%i is running..." % (pid))
                os.kill(self._getpid(), signal.SIGUSR1)
                sh.Logger.removeHandler(handler)
                raise SystemExit(0)
            else:
                sh.Logger.info(self, "Stopped")
                sh.Logger.removeHandler(handler)
                raise SystemExit(3)
        else:
            sh.Logger.info(self, "Stopped")
            sh.Logger.removeHandler(handler)
            raise SystemExit(3)

    def _run(self):
        self.stomp = StompConn()
        sh.Logger.info(self, "Started")
        sh.seta('stime', time.time())
        self.stomp.run()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', action='store_true')
    parser.add_argument('--stop', action='store_true')
    parser.add_argument('--reload', action='store_true')
    parser.add_argument('--restart', action='store_true')
    parser.add_argument('--config', nargs=1, required=True)
    parser.add_argument('--nofork', action='store_true')
    parser.add_argument('--status', action='store_true')
    args = parser.parse_args()

    sh.seta('ConsumerConf', ConsumerConf(args.config[0]))
    sh.ConsumerConf.parse()
    sh.seta('eventusr1', threading.Event())
    sh.seta('eventterm', threading.Event())
    sh.seta('eventtermwrit', {})
    sh.seta('eventwrite', threading.Event())
    sh.seta('msgqueues', {})
    sh.seta('cond', {})
    sh.seta('thlock', threading.Lock())
    clname = sh.ConsumerConf.get_option('GeneralLogName'.lower(), optional=True)
    sh.seta('Logger', MsgLogger(clname if clname else os.path.basename(sys.argv[0])))

    if not sh.ConsumerConf.get_option('GeneralWriteMsgFile'.lower()) and \
            not sh.ConsumerConf.get_option('GeneralWriteMsgIngestion'.lower()):
        sys.stderr.write('%s: At least one writer should be enabled\n' % clname)
        raise SystemExit(1)

    sh.seta('nummsgfile', 0)
    sh.seta('nummsging', 0)
    sh.seta('nummsgrecv', 0)
    sh.seta('server', [])
    sh.seta('writers', [])
    sh.seta('deststr', '')
    sh.seta('tconn', 0)
    md = hashlib.md5()
    md.update(args.config[0])
    daemon = Daemon(pidfile % md.hexdigest(), name=daemonname, nofork=args.nofork)

    if args.start:
        daemon.start(False)
    elif args.stop:
        daemon.stop(False)
    elif args.restart:
        daemon.restart()
    elif args.reload:
        daemon.reload()
    elif args.status:
        daemon.status()
    else:
        print parser.print_help()
        raise SystemExit(2)

    raise SystemExit(0)

main()
