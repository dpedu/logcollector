#!/usr/bin/env python3
import logging
import datetime
import os
import sys

from time import sleep
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import paramiko

#import logfile_pattern, LogFile

import re

from collections import namedtuple

logfile_pattern = re.compile(r'(?P<network>[^_]+)_(?P<channel>[^_]+)_(?P<date>[0-9]+)\.log')
LogFile = namedtuple("LogFile", "filename network channel date")
FutureTrack = namedtuple("FutureTrack", "username channel future")


class ZNCLogFetcher(object):
    def __init__(self, host, user, ssh_key, znc_user_dir, output_dir, keep_days, ignore_map={}):
        """
        :param host: host to ssh to
        :param user: user to ssh as
        :param ssh_key: path to ssh private key file to authenticate with
        :param znc_user_dir: remote path to znc's user dir
        :param output_dir: local dir to output files to. will be created
        :param keep_days: leave behind log files created within this many days of now
        :param ignore_map: dict of username->[channel] to ignore
        """
        self.host = host
        self.user = user
        self.ssh_key = paramiko.RSAKey.from_private_key_file(ssh_key)
        logging.debug("Loaded ssh key: {}".format(ssh_key))
        self.znc_user_dir = znc_user_dir
        self.output_dir = output_dir
        self.keep_days = keep_days
        self.ignore_map = ignore_map

        self.download_workers = 50

    def get_transport(self):
        """
        Create and return a new ssh connection
        """
        logging.debug("Connecting using {}@{}".format(self.user, self.host))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.WarningPolicy())
        client.connect(self.host, username=self.user, pkey=self.ssh_key, compress=True)
        return client

    def discover_znc_users(self):
        """
        Return a list of znc users by listing dirs in znc's user directory
        """
        with self.get_transport() as c:
            sftp = c.open_sftp()
            sftp.chdir(self.znc_user_dir)
            users = sftp.listdir()
        # users = ["voice_of_reason"]  # ["xMopxShell2", "voice_of_reason"]
        logging.info("Found users: {}".format(users))
        return users

    def discover_user_logs(self, username, max_age=None):
        """
        Lists items in user's ZNC log dir. Returns a dict organized by channel
        """
        logging.info("Discovering logs for {}".format(username))
        with self.get_transport() as c:
            sftp = c.open_sftp()
            userlogdir = os.path.join(self.znc_user_dir, username, 'moddata/log')

            try:
                stat = sftp.stat(userlogdir) # NOQA
            except:
                print("User {} has no logdir".format(username))
                return []
            sftp.chdir(userlogdir)

            by_channel = defaultdict(list)
            logsiter = sftp.listdir_iter(userlogdir)
            for logfile in logsiter:
                try:
                    network, channel, date = logfile_pattern.findall(logfile.filename)[0]
                    log_date = datetime.datetime.strptime(date, '%Y%m%d')
                    if not max_age or log_date < max_age:
                        by_channel[channel].append(LogFile(logfile.filename, network, channel, log_date))
                except:
                    print("Could not parse: {}".format(logfile.filename))
        logging.info("Discover logs: found {} channels for {}".format(len(by_channel.keys()), username))

        by_channel = dict(by_channel)

        if username in self.ignore_map:
            for channel in self.ignore_map[username]:
                if channel in by_channel:
                    del by_channel[channel]
                    logging.info("Ignored {} for user {}".format(channel, username))

        return by_channel

    def run(self):
        users = self.discover_znc_users()

        oldest_log = datetime.datetime.now() - datetime.timedelta(days=args.keep_days)

        futures = []
        jobs_added = 0
        is_clean = True
        with ThreadPoolExecutor(max_workers=self.download_workers) as tp:
            for zncuser in users:
                user_logs_by_channel = self.discover_user_logs(zncuser, max_age=oldest_log)
                logging.info("Queuing jobs for {}".format(zncuser))
                for channel, logfiles in user_logs_by_channel.items():

                    # Sort by date
                    logfiles = sorted(logfiles, key=lambda item: item.date)

                    # make output dir
                    for d in [
                        os.path.join(self.output_dir),
                        os.path.join(self.output_dir, zncuser),
                        os.path.join(self.output_dir, zncuser, logfiles[0].network)
                    ]:
                        try:
                            os.mkdir(d)
                        except:
                            pass

                    logging.info("Queuing {}:{}".format(zncuser, channel))
                    futures.append(FutureTrack(username=zncuser,
                                               channel=channel,
                                               future=tp.submit(self.download_channel, zncuser, channel, logfiles)))
                    jobs_added += 1
                    if jobs_added < self.download_workers * 3:
                        sleep(0.1)  # Prevents swarm of ssh connections
                logging.info("Finished queuing jobs")
            for future in futures:
                try:
                    assert future.future.result()
                except Exception as e:
                    is_clean = False
                    logging.critical("FAILED TO DOWNLOAD: {}: {}({})".format(future, e.__class__.__name__, str(e)))
        return is_clean

    def download_channel(self, username, channel, logfiles):
        """
        Download a single channels logs and condense into monthly logs
        """
        logging.info("Starting channel {} for {}. {} files to download".format(channel, username, len(logfiles)))
        with self.get_transport() as c:
            sftp = c.open_sftp()
            sftp.chdir(os.path.join(self.znc_user_dir, username, 'moddata/log'))

            month_file = None
            current_month = -1
            for f in logfiles:
                logging.debug("Downloading {}".format(f.filename))
                if f.date.month != current_month:
                    if month_file:
                        month_file.close()
                    month_file = open(os.path.join(args.output, username, f.network,
                                                   "{}_{}{:02}.log".format(f.channel, f.date.year, f.date.month)),
                                      'wb')
                    current_month = f.date.month
                fh = sftp.open(f.filename)
                month_file.write("# BEGIN FILE '{}'\n".format(f.filename).encode("UTF-8"))
                while True:
                    data = fh.read(4096 * 1024)
                    if not data:
                        break
                    month_file.write(data)
                fh.close()
                month_file.write("# END FILE '{}'\n".format(f.filename).encode("UTF-8"))
            sftp.close()
        logging.info("Finished channel {} for {}".format(channel, username))
        return True


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    for mute_me in ["paramiko"]:
        logging.getLogger(mute_me).setLevel(logging.CRITICAL)

    import argparse
    parser = argparse.ArgumentParser("Asdf")
    parser.add_argument("-a", "--host", required=True, help="host to fetch from")
    parser.add_argument("-u", "--user", required=True, help="ssh user")
    parser.add_argument("-k", "--key", required=True, help="ssh key")

    parser.add_argument("-d", "--dir", required=True, help="user dir to find logs from")
    parser.add_argument("-o", "--output", required=True, help="local output dir")

    parser.add_argument("-l", "--keep-days", type=int, help="number of days to keep", default=90)

    parser.add_argument("-x", "--ignore", nargs="+", help="username:#channelname pairs to ignore",  default=[])

    args = parser.parse_args()

    ignore_dict = defaultdict(list)
    for item in args.ignore:
        user, channel = item.split(":")
        ignore_dict[user].append(channel)

    f = ZNCLogFetcher(host=args.host, user=args.user, ssh_key=args.key, znc_user_dir=args.dir, output_dir=args.output,
                      keep_days=args.keep_days, ignore_map=dict(ignore_dict))
    sys.exit(0 if f.run() else 1)
