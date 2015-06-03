from distutils.core import setup
import glob, sys

NAME='argo-egi-consumer'

def get_ver():
    try:
        for line in open(NAME+'.spec'):
            if "Version:" in line:
                return line.split()[1]
    except IOError:
        print "Make sure that %s is in directory"  % (NAME+'.spec')
        sys.exit(1)

setup(
    name=NAME,
    version=get_ver(),
    author='SRCE',
    author_email='dvrcic@srce.hr, lgjenero@srce.hr',
    package_dir={'argo_egi_consumer': 'modules/'},
    packages=['argo_egi_consumer'],
    url='https://github.com/ARGOeu/argo-egi-consumer',
    description='argo-egi-consumer fetchs metric result messages from brokers',
    data_files=[('/etc/argo-egi-consumer', ['etc/consumer.conf', 'etc/metric_data.avsc']),
                ('/usr/bin/', ['bin/argo-egi-consumer.py']),
                ('/etc/init.d/', ['init.d/argo-egi-consumer'])]
)
