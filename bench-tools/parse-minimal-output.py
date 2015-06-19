#!/usr/bin/env python
# -*- coding: utf-8 -*-#
# @(#)parse-minimal-output.py
#
#
# Copyright (C) 2015, GC3, University of Zurich. All rights reserved.
#
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

__docformat__ = 'reStructuredText'
__author__ = 'Antonio Messina <antonio.s.messina@gmail.com>'

import argparse
import glob
import os
import pandas as pd
import re
import sys

re_host = re.compile('(.*/)?fio-test(-[0-9a-z-\.]*)?.(?P<hostname>(osd|node|vhp)-[kl][0-9]-(01-)?[0-9]+).*')
re_fiofile = re.compile('.*fio-test.(p:(?P<pool>[^.]+).)?'
                        'bs:(?P<bs>[0-9]+[a-z]).'
                        'iodepth:(?P<iodepth>[0-9]+).'
                        '(?P<test>read|randread|write|randwrite).'
                        '(?P<cache>cache|nocache).(fio.)?out')

fio_columns = [
    "terse output version",
    "fio version",
    "jobname",
    "groupid",
    "error",
    "read io KB",
    "read bandwidth (KB/sec)",
    "read iops",
    "read runtime (msec)",
    "read submission latency min",
    "read submission latency max",
    "read submission latency mean",
    "read submission latency dev (usec)",
    "read completion latency min",
    "read completion latency max",
    "read completion latency mean",
    "read completion latency dev (usec)",
    "read completion latency 1.000000 perc",
    "read completion latency 5.000000 perc",
    "read completion latency 10.000000 perc",
    "read completion latency 20.000000 perc",
    "read completion latency 30.000000 perc",
    "read completion latency 40.000000 perc",
    "read completion latency 50.000000 perc",
    "read completion latency 60.000000 perc",
    "read completion latency 70.000000 perc",
    "read completion latency 80.000000 perc",
    "read completion latency 90.000000 perc",
    "read completion latency 95.000000 perc",
    "read completion latency 99.000000 perc",
    "read completion latency 99.500000 perc",
    "read completion latency 99.900000 perc",
    "read completion latency 99.950000 perc",
    "read completion latency 99.990000 perc",
    "read completion latency 0 perc=0",
    "read completion latency 0 perc=0.1",
    "read completion latency 0 perc=0.2",
    "read Total latency min",
    "read Total latency max",
    "read Total latency mean",
    "read Total latency deviation (usec)",
    "read bw (KB/s) min",
    "read bw (KB/s) max",
    "read bw aggr percentage of total",
    "read bw mean",
    "read bw deviation",
    "write io KB",
    "write bandwidth (KB/sec)",
    "write iops",
    "write runtime (msec)",
    "write submission latency min",
    "write submission latency max",
    "write submission latency mean",
    "write submission latency dev (usec)",
    "write completion latency min",
    "write completion latency max",
    "write completion latency mean",
    "write completion latency dev (usec)",
    "write completion latency 1.00 perc",
    "write completion latency 5.00 perc",
    "write completion latency 10.00 perc",
    "write completion latency 20.00 perc",
    "write completion latency 30.00 perc",
    "write completion latency 40.00 perc",
    "write completion latency 50.00 perc",
    "write completion latency 60.00 perc",
    "write completion latency 70.00 perc",
    "write completion latency 80.00 perc",
    "write completion latency 90.00 perc",
    "write completion latency 95.00 perc",
    "write completion latency 99.00 perc",
    "write completion latency 99.50 perc",
    "write completion latency 99.90 perc",
    "write completion latency 99.95 perc",
    "write completion latency 99.99 perc",
    "write completion latency 0 perc.1",
    "write completion latency 0 perc.2",
    "write completion latency 0 perc.3",
    "write Total latency min",
    "write Total latency max",
    "write Total latency mean",
    "write Total latency deviation (usec)",
    "write bw (KB/s) min",
    "write bw (KB/s) max",
    "write bw aggr percentage of total",
    "write bw mean",
    "write bw deviation",
    "CPU user",
    "CPU system",
    "CPU context switches",
    "CPU major faults",
    "CPU minor faults",
    "IO depths 1",
    "IO depths 2",
    "IO depths 4",
    "IO depths 8",
    "IO depths 16",
    "IO depths 32",
    "IO depths 64",
    "IO latencies microseconds <=2",
    "IO latencies microseconds 4",
    "IO latencies microseconds 10",
    "IO latencies microseconds 20",
    "IO latencies microseconds 50",
    "IO latencies microseconds 100",
    "IO latencies microseconds 250",
    "IO latencies microseconds 500",
    "IO latencies microseconds 750",
    "IO latencies microseconds 1000",
    "IO latencies milliseconds <=2",
    "IO latencies milliseconds 4",
    "IO latencies milliseconds 10",
    "IO latencies milliseconds 20",
    "IO latencies milliseconds 50",
    "IO latencies milliseconds 100",
    "IO latencies milliseconds 250",
    "IO latencies milliseconds 500",
    "IO latencies milliseconds 750",
    "IO latencies milliseconds 1000",
    "IO latencies milliseconds 2000",
    "IO latencies milliseconds >=2000",
    "Disk utilization disk name",
    "Disk utilization read ios",
    "Disk utilization write ios",
    "Disk utilization read merges",
    "Disk utilization write merges",
    "Disk utilization read ticks",
    "Disk utilization write ticks",
    "Disk utilization time spent in queue",
    "Disk utilization disk utilization percentage",
]

column_names = ['hostname', 'pool', 'bs', 'iodepth', 'test', 'cache', 'ctime', 'mtime'] + fio_columns

def strtok(s):
    if s[-1] == 'k':
        return int(s[:-1])
    elif s[-1] == 'm':
        return int(s[:-1])*1024


def walk_directory(path, data):
    try:
        hostname = re_host.search(path).group('hostname')
    except:
        print("Ignoring directory %s as doesn't match pattern %s" % (path, re_host.pattern))
        return data

    for root, dirs, files in os.walk(path):
        for outfile in files:
            outfile = os.path.join(root, outfile)
            fmatch = re_fiofile.search(outfile)
            if not fmatch:
                continue
            print("Parsing file %s" % outfile)
            fmatch = re_fiofile.search(outfile)
            if not fmatch:
                print("Ignoring file %s" % outfile)
                continue
            # usecols is used to ignore any extra data.
            # Extra data is usually any disk after the first.
            try:
                tmp = pd.read_csv(outfile, names=fio_columns, delimiter=';', usecols=range(len(fio_columns)))
                tmp['hostname'] = hostname
                tmp['pool'] = fmatch.group('pool')
                tmp['bs'] = strtok(fmatch.group('bs'))
                tmp['iodepth'] = fmatch.group('iodepth')
                tmp['test'] = fmatch.group('test')
                tmp['cache'] = fmatch.group('cache')
                tmp['ctime'] = os.path.getctime(outfile)
                tmp['mtime'] = os.path.getmtime(outfile)
                data = pd.concat((data, tmp))
            except Exception as ex:
                print("ERROR parsing file %s: %s" % (outfile, ex))
    return data

# Why I have to save and reload???
# Otherwise I'll get an error:
# Traceback (most recent call last):
#   File "../parse-output-minimal.py", line 205, in <module>
#     data.loc[(data.test =='write')|(data.test == 'randwrite'), 'bw'] = data['write bw mean']
#   File "/usr/lib/python2.7/dist-packages/pandas/core/indexing.py", line 98, in __setitem__
#     self._setitem_with_indexer(indexer, value)
#   File "/usr/lib/python2.7/dist-packages/pandas/core/indexing.py", line 203, in _setitem_with_indexer
#     self._setitem_with_indexer(new_indexer, value)
#   File "/usr/lib/python2.7/dist-packages/pandas/core/indexing.py", line 287, in _setitem_with_indexer
#     value = self._align_series(indexer, value)
#   File "/usr/lib/python2.7/dist-packages/pandas/core/indexing.py", line 482, in _align_series
#     return ser.reindex(new_ix).values
#   File "/usr/lib/python2.7/dist-packages/pandas/core/series.py", line 2058, in reindex
#     return super(Series, self).reindex(index=index, **kwargs)
#   File "/usr/lib/python2.7/dist-packages/pandas/core/generic.py", line 1565, in reindex
#     takeable=takeable).__finalize__(self)
#   File "/usr/lib/python2.7/dist-packages/pandas/core/generic.py", line 1602, in _reindex_axes
#     fill_value=fill_value, limit=limit, copy=copy)
#   File "/usr/lib/python2.7/dist-packages/pandas/core/generic.py", line 1691, in _reindex_with_indexers
#     allow_dups=allow_dups)
#   File "/usr/lib/python2.7/dist-packages/pandas/core/internals.py", line 3241, in reindex_indexer
#     raise ValueError("cannot reindex from a duplicate axis")
# ValueError: cannot reindex from a duplicate axis


def postprocess_and_write_data(data, fullcsvpath, smallcsvpath):
    data = data[column_names]
    data.to_csv(fullcsvpath, index=False)

    data = pd.read_csv(fullcsvpath)
    # Add convenience columns

    data.loc[(data.test =='write')|(data.test == 'randwrite'), 'iops'] = data['write iops']
    data.loc[(data.test =='read')|(data.test == 'randread'), 'iops'] = data['read iops']

    data.loc[(data.test =='write')|(data.test == 'randwrite'), 'bw'] = data['write bw mean']
    data.loc[(data.test =='read')|(data.test == 'randread'), 'bw'] = data['read bw mean']

    data.loc[(data.test =='read')|(data.test == 'randread'), 'lat_usec'] = data['read Total latency mean']
    data.loc[(data.test =='write')|(data.test == 'randwrite'), 'lat_usec'] = data['write Total latency mean']
    data['lat'] = data['lat_usec']/1000

    data['bw_m'] = data['bw']/1024

    perc_columns = [ 'CPU user',
                     'CPU system',
                     'IO depths 1',
                     'IO depths 2',
                     'IO depths 4',
                     'IO depths 8',
                     'IO depths 16',
                     'IO depths 32',
                     'IO depths 64',
                     'Disk utilization disk utilization percentage',
    ]
    for col in perc_columns:
        data[col + ' %'] = data[col].apply(lambda x: float(x[:-1]))

    for key in [
            "terse output version",
            "fio version",
            "jobname",
            "groupid",
            "error",
            "read io KB",
            "read bandwidth (KB/sec)",
            "read iops",
            "read submission latency min",
            "read submission latency max",
            "read submission latency mean",
            "read submission latency dev (usec)",
            "read completion latency min",
            "read completion latency max",
            "read completion latency mean",
            "read completion latency dev (usec)",
            "read completion latency 1.000000 perc",
            "read completion latency 5.000000 perc",
            "read completion latency 10.000000 perc",
            "read completion latency 20.000000 perc",
            "read completion latency 30.000000 perc",
            "read completion latency 40.000000 perc",
            "read completion latency 50.000000 perc",
            "read completion latency 60.000000 perc",
            "read completion latency 70.000000 perc",
            "read completion latency 80.000000 perc",
            "read completion latency 90.000000 perc",
            "read completion latency 95.000000 perc",
            "read completion latency 99.000000 perc",
            "read completion latency 99.500000 perc",
            "read completion latency 99.900000 perc",
            "read completion latency 99.950000 perc",
            "read completion latency 99.990000 perc",
            "read completion latency 0 perc=0",
            "read completion latency 0 perc=0.1",
            "read completion latency 0 perc=0.2",
            "read bw aggr  percentage of total",
            "read bw mean",
            "read bw deviation",
            "write io KB",
            "write bandwidth (KB/sec)",
            "write iops",
            "write submission latency min",
            "write submission latency max",
            "write submission latency mean",
            "write submission latency dev (usec)",
            "write completion latency min",
            "write completion latency max",
            "write completion latency mean",
            "write completion latency dev (usec)",
            "write completion latency 1.00 perc",
            "write completion latency 5.00 perc",
            "write completion latency 10.00 perc",
            "write completion latency 20.00 perc",
            "write completion latency 30.00 perc",
            "write completion latency 40.00 perc",
            "write completion latency 50.00 perc",
            "write completion latency 60.00 perc",
            "write completion latency 70.00 perc",
            "write completion latency 80.00 perc",
            "write completion latency 90.00 perc",
            "write completion latency 95.00 perc",
            "write completion latency 99.00 perc",
            "write completion latency 99.50 perc",
            "write completion latency 99.90 perc",
            "write completion latency 99.95 perc",
            "write completion latency 99.99 perc",
            "write completion latency 0 perc.1",
            "write completion latency 0 perc.2",
            "write completion latency 0 perc.3",
            "write bw aggr percentage of total",
            "write bw mean",
            "write bw deviation",
            "CPU context switches",
            "CPU major faults",
            "CPU minor faults",
            "IO depths 1",
            "IO depths 2",
            "IO depths 4",
            "IO depths 8",
            "IO depths 16",
            "IO depths 32",
            "IO depths 64",
            "IO depths 1 perc",
            "IO depths 2 perc",
            "IO depths 4 perc",
            "IO depths 8 perc",
            "IO depths 16 perc",
            "IO depths 32 perc",
            "IO depths 64 perc",
            "IO latencies microseconds <=2",
            "IO latencies microseconds 4",
            "IO latencies microseconds 10",
            "IO latencies microseconds 20",
            "IO latencies microseconds 50",
            "IO latencies microseconds 100",
            "IO latencies microseconds 250",
            "IO latencies microseconds 500",
            "IO latencies microseconds 750",
            "IO latencies microseconds 1000",
            "IO latencies milliseconds <=2",
            "IO latencies milliseconds 4",
            "IO latencies milliseconds 10",
            "IO latencies milliseconds 20",
            "IO latencies milliseconds 50",
            "IO latencies milliseconds 100",
            "IO latencies milliseconds 250",
            "IO latencies milliseconds 500",
            "IO latencies milliseconds 750",
            "IO latencies milliseconds 1000",
            "IO latencies milliseconds 2000",
            "IO latencies milliseconds >=2000",
    ]:
        if key in data:
            del data[key]

    for key in data:
        for rw in ['read ', 'write ']:
            if key.startswith(rw):
                newkey = key[len(rw):]
                data.loc[(data.test ==rw.strip())|(data.test == 'rand' + rw.strip()), newkey] = data[key]
                del data[key]
    data.to_csv(smallcsvpath, index=False)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-f','--full', default='full.csv', help='Path to csv file containing "raw" data. Default: %(default)s')
    parser.add_argument('-t', '--terse', default='terse.csv', help='Path to csv file containing "terse" data. Default: %(default)s')
    parser.add_argument('dirs', nargs="+", help="Path to directories where log files are found")

    cfg = parser.parse_args()

    data = pd.DataFrame(columns = column_names)
    for path in cfg.dirs:
        data = walk_directory(path, data)

    postprocess_and_write_data(data, cfg.full, cfg.terse)
