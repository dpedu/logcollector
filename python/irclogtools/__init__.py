
import re
from collections import namedtuple

logfile_pattern = re.compile(r'((?P<network>[^_]+)_)?(?P<channel>.+)_(?P<date>[0-9]+)\.log')
LogFile = namedtuple("LogFile", "filename network channel date")

__version__ = "0.0.0"
