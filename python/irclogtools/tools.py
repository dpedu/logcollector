
import os
from irclogtools.containers import LogFile


def discover_logfiles(path):
    """
    Given a path, return a list of LogFile objects representing the contents
    """
    root = os.path.abspath(os.path.normpath(path))
    logs = []
    for fname in os.listdir(path):
        fabspath = os.path.join(root, fname)
        if os.path.isfile(fabspath):
            logs.append(LogFile.create(fabspath))
    return logs
