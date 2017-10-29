#!/usr/bin/env python3

import os
import re
import datetime
from collections import defaultdict
from tabulate import tabulate
import json

logfile_pattern = re.compile(r'(?P<network>[^_]+)_(?P<channel>.+)_(?P<date>[0-9]+)\.log')


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
        Write the in-memory contents to disk
        """
        if not target_path:
            target_path = self.path

        channel = self.data[0].channel
        print("Writing {}{} portions for {} to {}".format(len(self.data), " raw" if raw else '', channel, target_path))

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
                            "date": section.date.strftime("%Y%m%d")}
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
        Add a portion (as a LogFile object) to the log file. If a portion with matching dates exists, it will be replaced
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
        self.network, self.channel, date = logfile_pattern.findall(self.name)[0]
        self.date = datetime.datetime.strptime(date, '%Y%m%d')

    def contents(self):
        """
        Return log contents
        """
        with open(os.path.join(self.dir, self.name), "rb") as f:
            return f.read()

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

def main():
    """
    Tool for archiving IRC logs (in ZNC's log format: Network_#channel_20170223.log). In testing, inputs and outputs
    always match sha256 sums.
    import:
        given the path to a directory containing many znc logs under one network, combine the logs into 1 log archive
        per channel, placed in the output dir.
    inspect:
        print some stats about the contents of a log archive
    slice:
        given an input log archive, create a new log archive containing a subset of the contents sliced by date range
    split:
        given an input log archive, reproduce the original input logs
    """
    import argparse
    parser = argparse.ArgumentParser("manipulate irc log archives")
    subparser_action = parser.add_subparsers(dest='action', help='action to take')

    parser_import = subparser_action.add_parser('import', help='Import raw ZNC logfiles into a log archive')
    parser_import.add_argument("-d", "--dir", required=True, help="dir containing log files")
    parser_import.add_argument("-o", "--output", required=True, help="output dir")

    parser_inspect = subparser_action.add_parser('inspect', help='Inspect log archives')
    parser_inspect.add_argument("-f", "--file", required=True, help="log archive file to inspect")
    parser_inspect.add_argument("--detail", action="store_true", help="show more detail")

    parser_inspect = subparser_action.add_parser('slice', help='Extract date range to new file')
    parser_inspect.add_argument("-s", "--src", required=True, help="source log archive path")
    parser_inspect.add_argument("-d", "--dest", required=True, help="source log archive path")
    parser_inspect.add_argument("--start", help="start timestamp such as 2016-1-1")
    parser_inspect.add_argument("--end", help="end timestamp such as 2016-1-1")
    parser_inspect.add_argument("--raw", action="store_true", help="write raw lines instead of log archive")

    parser_split = subparser_action.add_parser('split', help='Split a log archive back into original logfiles')
    parser_split.add_argument("-s", "--src", required=True, help="source log archive path")
    parser_split.add_argument("-d", "--dest", required=True, help="dir to dump logs into")

    args = parser.parse_args()

    if args.action == "import":
        os.makedirs(args.output, exist_ok=True)

        logs = discover_logfiles(args.dir)

        by_channel = defaultdict(list)
        for log in logs:
            by_channel[log.channel].append(log)

        print(tabulate([[k, len(v)] for k, v in by_channel.items()], headers=["channel", "num logs"]) + "\n")

        for channel, logfiles in by_channel.items():
            fout = os.path.join(args.output, "{}.log".format(channel))
            print(fout)
            log = CombinedLogfile(fout)
            for item in logfiles:
                log.add_section(item)
            log.write()

    elif args.action == "inspect":
        log = CombinedLogfile(args.file)

        drange = log.get_range()

        info = [["portions", len(log.data)],
                ["start", drange[0].strftime('%Y-%m-%d')],
                ["end", drange[1].strftime('%Y-%m-%d')]]

        print(tabulate(info, headers=["property", "value"]) + "\n")

        if args.detail:
            info = []
            total = 0
            for portion in log.data:
                data = portion.contents()
                size = len(data)
                total += size
                lines = len(data.split(b"\n"))
                info.append([portion.name,
                             portion.network,
                             portion.channel,
                             portion.date.strftime('%Y-%m-%d'),
                             lines,
                             "{:,}".format(size)])
            info.append(['', '', '', 'total size:', '', "{:,} B".format(total)])

            print(tabulate(info, headers=["portion file", "network", "channel", "date", "lines", "bytes"]) + "\n")

    elif args.action == "slice":
        src = CombinedLogfile(args.src)

        limstart = args.start and datetime.datetime.strptime(args.start, '%Y-%m-%d')
        limend = args.end and datetime.datetime.strptime(args.end, '%Y-%m-%d')

        src.limit(start=limstart, end=limend)
        src.write(args.dest, raw=args.raw)

    elif args.action == "split":
        src = CombinedLogfile(args.src)

        for portion in src.data:
            with open(os.path.join(args.dest, portion.name), "wb") as f:
                f.write(portion.contents())

if __name__ == '__main__':
    main()
