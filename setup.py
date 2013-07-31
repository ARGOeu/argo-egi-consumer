from distutils.core import setup

setup(
    name='ARConsumer',
    version='1.0.0',
    author='L. Gjenero',
    author_email='lgjenero@srce.hr',
    packages=['arconsumer', 'arconsumer.writter'],
    scripts=['bin/ar-consumer'],
    url='https://code.grnet.gr/projects/ar-ng',
    license='LICENSE.txt',
    description='AR consumer.',
    long_description=open('README.txt').read(),
    install_requires=[
        "stomppy >= 3.0.3",
    ],
)
