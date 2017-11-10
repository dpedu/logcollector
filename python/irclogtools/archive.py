#!/usr/bin/env python3

import os
import datetime
import argparse
from collections import defaultdict
from tabulate import tabulate
from concurrent.futures import ProcessPoolExecutor

from irclogtools.containers import CombinedLogfile
from irclogtools.tools import discover_logfiles


def archiveit(output_dir, _channel, _logfiles):
    fout = os.path.join(output_dir, "{}.log".format(_channel))
    log = CombinedLogfile(fout)
    for item in _logfiles:
        log.add_section(item)
    log.write()


def by_totalsize(logfiles):
    """
    Given a list of `LogFile`s, return the total reported size, in bytes, of the log
    """
    return sum([i.bytes() for i in logfiles])


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
    parser = argparse.ArgumentParser("manipulate irc log archives")
    subparser_action = parser.add_subparsers(dest='action', help='action to take')

    parser_import = subparser_action.add_parser('import', help='Import raw ZNC logfiles into a log archive')
    parser_import.add_argument("-d", "--dir", required=True, help="dir containing log files")
    parser_import.add_argument("-o", "--output", required=True, help="output dir")
    parser_import.add_argument("--all", action="store_true", help="ingest all log files, not just channels")

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
            if not args.all and not log.channel.startswith("#"):
                continue
            by_channel[log.channel].append(log)

        _display = [[k, len(v)] for k, v in by_channel.items()]
        print(tabulate(sorted(_display, key=lambda x: x[0].lower()), headers=["channel", "num logs"]) + "\n")

        with ProcessPoolExecutor(max_workers=os.cpu_count() * 2) as tp:
            for channel, logfiles in sorted(by_channel.items(), key=lambda x: by_totalsize(x[1]), reverse=True):
                tp.submit(archiveit, args.output, channel, logfiles)

    elif args.action == "inspect":
        log = CombinedLogfile(args.file)

        drange = log.get_range()

        info = [["portions", len(log.data)],
                ["start", drange[0].strftime('%Y-%m-%d')],
                ["end", drange[1].strftime('%Y-%m-%d')]]

        print(tabulate(info, headers=["property", "value"]) + "\n")

        if args.detail:
            info = []
            total_bytes = 0
            total_lines = 0
            for portion in log.data:
                data = portion.contents()
                size = len(data)
                total_bytes += size
                lines = len(data.split(b"\n"))
                total_lines += lines
                info.append([portion.name,
                             portion.network,
                             portion.channel,
                             portion.date.strftime('%Y-%m-%d'),
                             lines,
                             "{:,}".format(size)])
            info.append([])
            info.append(['', '', '', 'total:', "{:,}".format(total_lines), "{:,} B".format(total_bytes)])

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
