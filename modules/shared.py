class SingletonShared:
    def __init__(self):
        for attr in ['ConsumerConf', 'Logger', 'thlock']:
            if getattr(self.__class__, attr, None):
                code = """self.%s = self.__class__.%s""" % (attr, attr)
                exec code

    def seta(self, attr, value):
        setattr(self.__class__, attr, value)
