class SingletonShared(object):
    def __new__(cls, *args, **kwargs):
        if getattr(cls, 'sharedobj', None):
            return cls.sharedobj
        else:
            setattr(cls, 'sharedobj', object.__new__(cls))
            return cls.sharedobj

    def __init__(self):
        for attr in ['ConsumerConf', 'Logger', 'thlock', 'eventterm', 'stime', 'eventusr1']:
            if getattr(self.__class__, attr, None):
                code = """self.%s = self.__class__.%s""" % (attr, attr)
                exec code

    def seta(self, attr, value):
        setattr(self.__class__, attr, value)
