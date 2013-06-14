import time
import sys
import logging
import stomp
import sys
import datetime
from os import path

class MessageWritter(object):
    
    def __init__(self): 
	# log file
	self.configFile = ''
        # date format
        self.dateFormat = '%Y-%m-%dT%H:%M:%SZ'

    def loadConfig(self, configFile):
	"""Load writter configuration from file"""

    def writeMessage(self, message):
        """Write message"""
