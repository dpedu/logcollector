#!/usr/bin/env python3
from setuptools import setup

from irclogtools import __version__

setup(name='irclogtools',
      version=__version__,
      description='tools for doing various things with IRC logs',
      url='http://gitlab.davepedu.com/dave/irclogtools',
      author='dpedu',
      author_email='dave@davepedu.com',
      packages=['irclogtools'],
      entry_points={'console_scripts': ['ilogarchive=irclogtools.archive:main']})
