
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

import avro.schema
import datetime
import json
import logging
import os
import pprint
import re
import requests
import stomp
import sys
import threading
import time

from argo_egi_consumer.shared import SingletonShared as Shared
from avro.datafile import DataFileReader
from avro.datafile import DataFileWriter
from avro.io import BinaryEncoder
from avro.io import DatumReader
from avro.io import DatumWriter
from io import BytesIO
from os import path
from base64 import b64encode

LOGFORMAT = '%(name)s[%(process)s]: %(levelname)s %(message)s'

sh = Shared()

class MsgLogger:
    def __init__(self, name):
        formatter = logging.Formatter(LOGFORMAT)
        self.mylog = logging.getLogger(name)
        self.mylog.setLevel(logging.DEBUG)
        handler = logging.handlers.SysLogHandler('/dev/log')
        handler.setFormatter(formatter)
        self.mylog.addHandler(handler)
        self.mylog.propagate = False

        self.rootlog = logging.getLogger('')
        self.rootlog.setLevel(logging.WARNING)
        self.rootlog.addHandler(handler)
        self.rootlog.propagate = False

    def _module_class_name(self, obj):
        # name = repr(obj.__module__) + '.' + repr(obj.__class__.__name__)
        if isinstance(obj, str):
            return obj
        else:
            name = repr(obj.__class__.__name__)
            return name.replace("'",'')

    def error(self, obj, msg):
        self.mylog.error(self._module_class_name(obj) + ': ' + str(msg))

    def info(self, obj, msg):
        self.mylog.info(self._module_class_name(obj) + ': ' + str(msg))

    def warning(self, obj, msg):
        self.mylog.warning(self._module_class_name(obj) + ': ' + str(msg))

    def addHandler(self, hdlr):
        self.mylog.addHandler(hdlr)

    def removeHandler(self, hdlr):
        self.mylog.removeHandler(hdlr)

class MessageBaseWriter(object):
    def load(self):
        sh.ConsumerConf.parse()
        self.date_format = '%Y-%m-%dT%H:%M:%SZ'
        self.futuredays_ok = sh.ConsumerConf.get_option('MsgRetentionFutureDaysOk'.lower())
        self.log_out_allowedtime_msg = sh.ConsumerConf.get_option('MsgRetentionLogMsgOutAllowedTime'.lower())
        self.log_wrong_formatted_msg = sh.ConsumerConf.get_option('GeneralLogWrongFormat'.lower())
        self.pastdays_ok = sh.ConsumerConf.get_option('MsgRetentionPastDaysOk'.lower())

        try:
            avsc = open(sh.ConsumerConf.get_option('GeneralAvroSchema'.lower()))
            self.schema = avro.schema.parse(avsc.read())
        except (OSError, IOError) as e:
            sh.Logger.error(self, e)
            sh.Logger.removeHandler(handler)
            raise SystemExit(1)
        finally:
            avsc.close()

    def _split_in_two(self, msg, fields):
        msglist = []

        servtype = fields['serviceType'].split(',')
        msg['service'] = servtype[0].strip()
        msglist.append(msg)

        copymsg = msg.copy()
        copymsg['service'] = servtype[1].strip()
        msglist.append(copymsg)

        return msglist

    def construct_msg(self, fields):
        msglist = []
        msg, tags = {}, {}

        msg = {'service': fields['serviceType'],
               'timestamp': fields['timestamp'],
               'hostname': fields['hostName'],
               'metric': fields['metricName'],
               'status': fields['metricStatus']}
        msgattrmap = {'detailsData': 'message',
                      'summaryData': 'summary',
                      'nagios_host': 'monitoring_host'}
        for attr in msgattrmap.keys():
            if attr in fields:
                msg[msgattrmap[attr]] = fields[attr]

        tagattrmap = {'ROC': 'roc', 'voName': 'voName', 'voFqan': 'voFqan'}
        for attr in tagattrmap.keys():
            tags[tagattrmap[attr]] = fields.get(attr, None)
        if tags:
            msg['tags'] = tags

        if ',' in fields['serviceType']:
            msglist += self._split_in_two(msg, fields)
        else:
            msglist.append(msg)

        return msglist

    def is_validmsg(self, msgfields):
        keys = set(msgfields.keys())
        mandatory_fields = set(['serviceType', 'timestamp', 'hostName', 'metricName', 'metricStatus'])

        if keys >= mandatory_fields:
            return True
        else:
            sh.Logger.error(self, 'Message %s has no mandatory fields: %s' % (msgfields['message-id'],
                                                                                  str([e for e in mandatory_fields.difference(keys)])))
            return False

    def is_ininterval(self, msgid, timestamp, now):
        inint = False

        try:
            msgTime = datetime.datetime.strptime(timestamp, self.date_format).date()
            nowTime = datetime.datetime.utcnow().date()
        except ValueError as e:
            sh.Logger.error(self, 'Message %s %s' % (msgid, e))
            return inint

        timeDiff = nowTime - msgTime
        if timeDiff.days == 0:
            inint = True
        elif timeDiff.days > 0 and timeDiff.days <= self.pastdays_ok:
            inint = True
        elif timeDiff.days < 0 and -timeDiff.days <= self.futuredays_ok:
            inint = True

        return inint

    def write_msg(self, fields):
        pass

class MessageWriterIngestion(MessageBaseWriter):
    def __init__(self):
        self.load()

    def load(self):
        super(MessageWriterIngestion, self).load()
        self.partition_date_format ='%Y-%m-%d'
        self.host = sh.ConsumerConf.get_option('MsgIngestionHost'.lower())
        self.token = sh.ConsumerConf.get_option('MsgIngestionToken'.lower())
        self.tenat = sh.ConsumerConf.get_option('MsgIngestionTenant'.lower())
        self.urlapi = "/v1/projects/%s/topics/metric_data:publish?key=%s" % (self.tenat, self.token)

        self.avro_writer = DatumWriter(self.schema)
        self.bytesio = BytesIO()
        self.encoder = BinaryEncoder(self.bytesio)

    def _b64enc_msg(self, msg):
        try:
            self.avro_writer.write(msg, self.encoder)
            raw_bytes = self.bytesio.getvalue()

            return b64encode(raw_bytes)

        except (IOError, OSError) as e:
            sh.Logger.error(self, e)
            raise SystemExit(1)

    def construct_msg(self, fields):
        msglist = super(MessageWriterIngestion, self).construct_msg(fields)
        json_msgs = []

        for m in msglist:
            ingest_msg = {"messages": [{"attributes": {"type": "metric_data",
                                                       "partition_date": datetime.datetime.now().strftime(self.partition_date_format)},
                                        "data": self._b64enc_msg(m)}]}
            json_msgs.append(json.dumps(ingest_msg))

        return json_msgs

    def write_msg(self, fields):
        now = datetime.datetime.utcnow().date()

        if self.is_validmsg(fields):
            if self.is_ininterval(fields['message-id'], fields['timestamp'], now):
                msglist = self.construct_msg(fields)
                for b64jsonmsg in msglist:
                    try:
                        response = requests.post('https://' + self.host + self.urlapi,
                                                 data=b64jsonmsg,
                                                 verify=False)
                        response.raise_for_status()
                        sh.nummsging += 1
                    except requests.exceptions.RequestException as e:
                        if isinstance(e, requests.exceptions.HTTPError):
                            if e.response.status_code >= 400:
                                # TODO: disable writer
                                pass
                            sh.Logger.error(self, repr(e) + ' - ' + str(response.json()))
                        else:
                            sh.Logger.error(self, repr(e))

class MessageWriterFile(MessageBaseWriter):
    def __init__(self):
        self.load()

    def load(self):
        super(MessageWriterFile, self).load()
        self.write_plaintxt = sh.ConsumerConf.get_option('MsgFileWritePlaintext'.lower())
        self.errorfilename_template = sh.ConsumerConf.get_option('MsgFileErrorFilename'.lower())
        self.filedir = sh.ConsumerConf.get_option('MsgFileDirectory'.lower())
        self.filename_template = sh.ConsumerConf.get_option('MsgFileFilename'.lower())

    def _write_to_ptxt(self, log, msglist, exten):
        try:
            filename = '.'.join(log.split('.')[:-1]) + '.%s' % exten
            plainfile = open(filename, 'a+')
            plainfile.write(json.dumps(msglist) + '\n')
            plainfile.close()
        except (IOError, OSError) as e:
            sh.Logger.error(self, e)
            raise SystemExit(1)

    def _write_to_avro(self, log, msglist):
        sh.thlock.acquire(True)
        try:
            if path.exists(log):
                avroFile = open(log, 'a+')
                writer = DataFileWriter(avroFile, DatumWriter())
            else:
                avroFile = open(log, 'w+')
                writer = DataFileWriter(avroFile, DatumWriter(), self.schema)

            for m in msglist:
                writer.append(m)
            sh.nummsgfile += len(msglist)

        except (IOError, OSError) as e:
            sh.Logger.error(self, e)
            raise SystemExit(1)

        finally:
            writer.close()
            avroFile.close()
            sh.thlock.release()

    def _create_log_filename(self, timestamp):
        return self.filedir + self.filename_template.replace('DATE', timestamp)

    def _create_error_log_filename(self, timestamp):
        return self.filedir + self.errorfilename_template.replace('DATE', timestamp)

    def write_msg(self, fields):
        now = datetime.datetime.utcnow().date()
        msglist = self.construct_msg(fields)

        if self.is_validmsg(fields):
            if self.is_ininterval(fields['message-id'], fields['timestamp'], now):
                filename = self._create_log_filename(fields['timestamp'][:10])
                self._write_to_avro(filename, msglist)
                if self.write_plaintxt:
                    self._write_to_ptxt(filename, msglist, 'PLAINTEXT')

            elif self.log_out_allowedtime_msg:
                filename = self._create_error_log_filename(str(now))
                self._write_to_avro(filename, msglist)
                if self.write_plaintxt:
                    self._write_to_ptxt(filename, msglist, 'PLAINTEXT')
        elif self.log_wrong_formatted_msg:
            filename = self._create_error_log_filename(str(now))
            self._write_to_ptxt(filename, msglist, 'WRONGFORMAT')
