import os
import datetime
import json

from irclogtools import logfile_pattern


class CombinedLogfile(object):
    HEADER = "#$$$COMBINEDLOG"
    PORTIONHEADER = "#$$$BEGINPORTION"
    ENDPORTIONHEADER = "#$$$ENDPORTION"

    def __init__(self, fpath):
        self.path = fpath
        self.data = []
        if os.path.exists(self.path):
            self._parse()
        # TODO maybe an interface to limit the date range of added stuff?

    def _parse(self):
        """
        Open the logfile and load each section into memory
        """
        with open(self.path, "rb") as f:
            # Read the magic header
            header = f.readline().decode("UTF-8")
            assert self.HEADER and header[0:len(self.HEADER)] == self.HEADER, "Invalid header!"

            channel = None
            network = None

            meta = None
            portion = None

            while True:
                line = f.readline()
                if not line:
                    break

                if line.startswith(self.PORTIONHEADER.encode("UTF-8")):
                    assert portion is None and meta is None, "Started portion while already in portion?"
                    meta = json.loads(line.decode("UTF-8").split(" ", 1)[1].strip())
                    portion = b''
                    if not channel:
                        channel = meta["channel"]
                    if not network:
                        network = meta["network"]
                    assert channel == meta["channel"], "Portion does not match first portion's channel"
                    # assert network == meta["network"], "Portion does not match first portion's network"

                elif line.startswith(self.ENDPORTIONHEADER.encode("UTF-8")):
                    assert portion is not None and meta is not None, "Ended portion while not in portion?"
                    self.data.append(VirtualLogFile(meta["name"], portion))
                    portion = None
                    meta = None

                else:
                    portion += line

            assert portion is None and meta is None, "Unexpected EOF during open portion"

    def write(self, target_path=None, raw=False):
        """
        Write the in-memory contents to disk. A log archive is a UTF-8 text file and is described below.

        The files start with a header, containing only the channel name:  #TODO number of portions + check on parsing

        #$$$COMBINEDLOG '#chan'

        Then sorted repeating units of:

        #$$$BEGINPORTION {"channel": "#chan", "date": "20140119", "name": "#chan_20140119.log", "network": null}
        newline-separated UTF-8 log messages
        #$$$ENDPORTION #hcsmp_20140119.log

        the metadata is json and must be sorted by key. network may be null but no other fields may be. date must be
        formatted as above and name, the original file name, must match by irclogtools.logfile_pattern.
        """
        if not target_path:
            target_path = self.path

        channel = self.data[0].channel
        print("{}: writing {}{} portions".format(target_path, len(self.data), " raw" if raw else ''))

        with open(target_path, "wb") as f:
            # Write the magic header
            if not raw:
                f.write("{} '{}'\n".format(self.HEADER, channel).encode("UTF-8"))

            # Put portions in order
            self.sort()

            # Write each portion
            for section in self.data:
                if not raw:
                    meta = {"name": section.name,
                            "network": section.network,
                            "channel": section.channel,
                            "date": section.date.strftime("%Y%m%d"),
                            "lines": section.lines(),
                            "size": section.bytes()}
                    f.write("{} {}\n".format(self.PORTIONHEADER, json.dumps(meta, sort_keys=True)).encode("UTF-8"))
                contents = section.contents()
                f.write(contents)
                if not raw:
                    if not contents.endswith(b"\n"):
                        f.write(b"\n")
                    f.write("{} {}\n".format(self.ENDPORTIONHEADER, section.name).encode("UTF-8"))

    def sort(self):
        self.data.sort(key=lambda x: x.date)

    def add_section(self, section):
        """
        Add a portion (as a LogFile object) to the log file. If a portion with matching dates exists, it will
        be replaced
        """
        for s in self.data:
            assert section.channel == s.channel
            if s.date == section.date:
                return
        self.data.append(section)

    def get_range(self):
        """
        Return (start, end) datetime tuple of sections
        """
        start = self.data[0].date
        end = self.data[0].date

        for item in self.data:
            if item.date > end:
                end = item.date
            if item.date < start:
                start = item.date

        return (start, end, )

    def limit(self, end=None, start=None):
        """
        Drop all portions newer than end or older than start
        """
        assert end or start, "Need an start, end, or both"
        for item in self.data[:]:
            if (end and item.date > end) or (start and item.date < start):
                self.data.remove(item)


class LogFile(object):

    def __init__(self, fname, root=None):
        self.dir = root
        self.name = fname
        self.network = None
        self.channel = None
        self.date = None  # datetime object for this channel
        self._parse()

    def _parse(self):
        matches = logfile_pattern.match(self.name).groupdict()
        self.network = matches["network"]
        self.channel = matches["channel"]
        date = matches["date"]
        self.date = datetime.datetime.strptime(date, '%Y%m%d')

    def contents(self):
        """
        Return log contents
        """
        with open(os.path.join(self.dir, self.name), "rb") as f:
            return f.read()

    def lines(self):
        """
        Return line count
        """
        lines = 0
        with open(os.path.join(self.dir, self.name), "rb") as f:
            for _ in f.readlines():
                lines += 1
        return lines

    def bytes(self):
        return os.path.getsize(os.path.join(self.dir, self.name))

    @staticmethod
    def create(fname):
        return LogFile(os.path.basename(fname), root=os.path.dirname(fname))

    def __str__(self):
        return "<__main__.LogFile '{}'>".format(self.name)

    __repr__ = __str__


class VirtualLogFile(LogFile):
    def __init__(self, fname, contents):
        super().__init__(fname)
        self.data = contents

    def contents(self):
        return self.data

    def lines(self):
        return len(self.data.split(b'\n'))

    def bytes(self):
        return len(self.data)
