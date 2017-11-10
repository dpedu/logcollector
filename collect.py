#!/usr/bin/env python3
import re
import datetime
import os
import sys
import traceback
from time import sleep
from collections import defaultdict, namedtuple
from concurrent.futures import ThreadPoolExecutor

import paramiko


logfile_pattern = re.compile(r'(?P<network>[^_]+)_(?P<channel>.+)_(?P<date>[0-9]+)\.log')

LogFile = namedtuple("LogFile", "filename network channel date")


class ZNCLogFetcher(object):
    def __init__(self, host, user, ssh_key, znc_user_dir, output_dir, keep_days):
        """
        :param host: host to ssh to
        :param user: user to ssh as
        :param ssh_key: path to ssh private key file to authenticate with
        :param znc_user_dir: remote path to znc's user dir
        :param output_dir: local dir to output files to. will be created
        :param keep_days: leave behind log files created within this many days of now
        """
        self.host = host
        self.user = user
        self.ssh_key = paramiko.RSAKey.from_private_key_file(ssh_key)
        self.znc_user_dir = znc_user_dir
        self.output_dir = output_dir
        self.keep_days = keep_days

    def get_transport(self):
        """
        Create and return a new ssh connection
        """
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.WarningPolicy())
        client.connect(self.host, username=self.user, pkey=self.ssh_key)
        return client

    def discover_znc_users(self):
        """
        Return a list of znc users by listing dirs in znc's user directory
        """
        with self.get_transport() as c:
            sftp = c.open_sftp()
            sftp.chdir(self.znc_user_dir)
            users = sftp.listdir()
        return users

    def discover_user_logs(self, username, max_age=None):
        """
        Lists items in user's ZNC log dir. Returns a dict organized by channel
        """
        with self.get_transport() as c:
            sftp = c.open_sftp()
            userlogdir = os.path.join(self.znc_user_dir, username, 'moddata/log')

            try:
                stat = sftp.stat(userlogdir)  # NOQA
            except:
                print("User {} has no logdir".format(username))
                return {}
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
        return dict(by_channel)

    def run(self):
        users = self.discover_znc_users()

        oldest_log = datetime.datetime.now() - datetime.timedelta(days=args.keep_days)

        with ThreadPoolExecutor(max_workers=50) as tp:
            for zncuser in users:
                user_logs_by_channel = self.discover_user_logs(zncuser, max_age=oldest_log)
                for channel, logfiles in user_logs_by_channel.items():

                    # Sort by date
                    logfiles = sorted(logfiles, key=lambda item: item.date)

                    # make output dir
                    for d in [
                        os.path.join(self.output_dir),
                        os.path.join(self.output_dir, zncuser),
                        os.path.join(self.output_dir, zncuser, logfiles[0].network.lower())
                    ]:
                        try:
                            os.mkdir(d)
                        except:
                            pass

                    tp.submit(self.download_channel, zncuser, channel, logfiles)
                    sys.stdout.write("+")
                    sys.stdout.flush()
                    sleep(0.2)  # Prevents swarm of ssh connections

    def download_channel(self, username, channel, logfiles):
        """
        Download a single channels logs and condense into monthly logs
        """
        with self.get_transport() as c:
            try:
                sftp = c.open_sftp()
                sftp.chdir(os.path.join(self.znc_user_dir, username, 'moddata/log'))

                month_file = None
                current_month = -1
                for f in logfiles:
                    sys.stdout.write(".")
                    sys.stdout.flush()
                    if f.date.month != current_month:
                        if month_file:
                            month_file.close()
                        month_file = open(os.path.join(args.output, username, f.network.lower(), "{}_{}{:02}.log"
                                          .format(f.channel, f.date.year, f.date.month)), 'wb')
                        current_month = f.date.month
                    fh = sftp.open(f.filename)
                    month_file.write("# BEGIN FILE '{}'\n".format(f.filename).encode("UTF-8"))
                    while True:
                        data = fh.read(4 * 1024 * 1024)
                        if not data:
                            break
                        month_file.write(data)
                    fh.close()
                    month_file.write("# END FILE '{}'\n".format(f.filename).encode("UTF-8"))
                if month_file:
                    month_file.close()
                sftp.close()
                sys.stdout.write("finished {}".format(channel))
                sys.stdout.flush()
            except:
                print(traceback.format_exc())


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser("Asdf")
    parser.add_argument("-a", "--host", required=True, help="host to fetch from")
    parser.add_argument("-u", "--user", required=True, help="ssh user")
    parser.add_argument("-k", "--key", required=True, help="ssh key")

    parser.add_argument("-d", "--dir", required=True, help="user dir to find logs from")
    parser.add_argument("-o", "--output", required=True, help="local output dir")

    parser.add_argument("-l", "--keep-days", type=int, help="number of days to keep", default=90)  # dont fetch anything newer than x days ago (?)

    args = parser.parse_args()
    f = ZNCLogFetcher(host=args.host,
                      user=args.user,
                      ssh_key=args.key,
                      znc_user_dir=args.dir,
                      output_dir=args.output,
                      keep_days=args.keep_days)
    f.run()
