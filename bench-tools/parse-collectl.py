#!/usr/bin/env python
# -*- coding: utf-8 -*-#
# @(#)parse-collectl.py
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
import csv
import gzip
import math
import os
import pandas as pd
import re
import sys

from cStringIO import StringIO
from collections import defaultdict
from matplotlib import colors as mplcolors
from matplotlib import pylab as plt
from matplotlib.dates import DateFormatter


# re_host = re.compile('(.*/)?fio-test(-[0-9\.]*)?.(?P<hostname>(osd|node|vhp)-[kl][0-9]-(01-)?[0-9]+).*')
re_collectl = re.compile('fio-test.(p:(?P<pool>[^.]+).)?'
                        'bs:(?P<bs>[0-9]+[a-z]).'
                        'iodepth:(?P<iodepth>[0-9]+).'
                        '(?P<test>read|randread|write|randwrite).'
                         '(?P<cache>cache|nocache).collectl[.-]*(?P<hostname>(osd|node|vhp)-[kl][0-9]-(01-)?[0-9]+).*')

def convert_collectl(input, output):
    output_csv = csv.writer(output)
    numlines = 0
    for line in input:
        line = line.strip()
        if line=='' or line.startswith('# ') or line.startswith('##'):
            continue
        elif line.startswith('#'):
            line = line[1:]
            nfields = len(line.split())
        numlines += 1
        parts = line.split(' ', nfields-1)
        output_csv.writerow(parts)
    return numlines


def plot_data(columns, plottype, labelfmt):
    # cm = plt.get_cmap('Set3')
    cm = plt.get_cmap('jet')
    cmnorm = mplcolors.Normalize(vmin=0,vmax=len(columns))
    maxhosts = max(len(v.keys()) for v in tests.values())
    yplots = int(math.sqrt(maxhosts))
    xplots = int(math.ceil(float(maxhosts)/yplots))
    print("%d x %d" % (xplots, yplots))
    for test in tests.keys():
        print("Processing test %s, type %s" % ((test,), plottype))
        pool, bs, iodepth, rw = test
        plotfname = '%s.pool=%s.bs=%s.io=%d.%s.svg' % (plottype, pool, bs, iodepth, rw)

        fig, axes = plt.subplots(xplots, yplots, sharex='all', sharey='all')
        for n, (host, datasets) in enumerate(tests[test].items()):
            for data in datasets:
                if not set(columns).issubset(data.columns):
                    continue
                import pdb; pdb.set_trace()
                data['time'] = data.Date_Time.apply(lambda x: x.time())
                x = (n % xplots)
                y = (n / xplots)
                ax = axes[x, y]
                ax.set_title('%s Usage for host %s' % (plottype, host))
                #data.plot(data.Date_Time, cpucols_pct, lw=2, colormap='jet', ax=ax, rot=90)
                for idx, col in enumerate(columns):
                    ax.plot(data.time, data[col], label=labelfmt(col), color=cm(cmnorm(idx)))
                if n == 0:
                    ax.legend(loc='upper left')
        fig.autofmt_xdate()
        fig.set_size_inches(14*3, 10*3)

        plt.savefig(plotfname)
        plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser('parse collectl output files and produce plots')
    parser.add_argument("files", nargs='+', help='collectl files')

    cfg = parser.parse_args()

    tests = defaultdict(lambda: defaultdict(list))
    for fname in cfg.files:
        outfname = fname+'.csv'
        if not os.path.isfile(outfname):
            print("Preprocessing file %s" % fname)
            with gzip.open(fname, 'r') as input, open(outfname, 'w') as output:
                lines = convert_collectl(input, output)
            if lines < 2:
                os.remove(outfname)
                print("empty file, skipping")
                continue
        basefile = os.path.basename(fname)
        match = re_collectl.search(basefile)
        if not match:
            print("Error parsing filename %s" % fname)
            continue
        pool = match.group('pool')
        bs = match.group('bs')
        iodepth = int(match.group('iodepth'))
        rw = match.group('test')
        host = match.group('hostname')

        try:
            tests[(pool,bs,iodepth,rw)][host].append(pd.read_csv(outfname, parse_dates=[[0,1]]))
        except Exception as ex:
            print("Skipping file %s: %s" % (outfname, ex))

    # plot data.
    print("Now plotting data")

    for title, cols, replacement in (
            ('CPU percent aggregate', [
                '[CPU]User%',
                # '[CPU]Nice%',
                '[CPU]Sys%',
                '[CPU]Wait%',
                '[CPU]Irq%',
                # '[CPU]Soft%',
                #'[CPU]Steal%',
                #'[CPU]Idle%',
                '[CPU]Totl%'
            ], lambda x: x.replace('[CPU]','')),

            ('CPU abs aggregate', [
                '[CPU]Intrpt/sec',
                '[CPU]Ctx/sec',
                '[CPU]Proc/sec',
                '[CPU]ProcQue',
                '[CPU]ProcRun',
            ], lambda x: x.replace('[CPU]','')),

            ('Disk aggregate', [
                '[DSK]ReadTot',
                '[DSK]WriteTot',
                '[DSK]OpsTot',
                '[DSK]ReadMrgTot',
                '[DSK]WriteMrgTot',
                '[DSK]MrgTot',
            ], lambda x: x.replace('[DSK]','')),

            ('Disk aggregate BW', [
                '[DSK]ReadKBTot',
                '[DSK]WriteKBTot',
                '[DSK]KbTot',
            ], lambda x: x.replace('[DSK]','')),

            ('Net vlan618', [
                #'[NET:vlan618]Name',
                '[NET:vlan618]RxPkt',
                '[NET:vlan618]TxPkt',
                '[NET:vlan618]RxKB',
                '[NET:vlan618]TxKB',
                '[NET:vlan618]RxErr',
                '[NET:vlan618]RxDrp',
                '[NET:vlan618]RxFifo',
                '[NET:vlan618]RxFra',
                '[NET:vlan618]RxCmp',
                '[NET:vlan618]RxMlt',
                '[NET:vlan618]TxErr',
                '[NET:vlan618]TxDrp',
                '[NET:vlan618]TxFifo',
                '[NET:vlan618]TxColl',
                '[NET:vlan618]TxCar',
                '[NET:vlan618]TxCmp',
                '[NET:vlan618]RxErrs',
                '[NET:vlan618]TxErrs',
            ], lambda x: x.replace('[NET:vlan618]', 'vlan618:')),

            ('Net vlan619', [
                #'[NET:vlan619]Name',
                '[NET:vlan619]RxPkt',
                '[NET:vlan619]TxPkt',
                '[NET:vlan619]RxKB',
                '[NET:vlan619]TxKB',
                '[NET:vlan619]RxErr',
                '[NET:vlan619]RxDrp',
                '[NET:vlan619]RxFifo',
                '[NET:vlan619]RxFra',
                '[NET:vlan619]RxCmp',
                '[NET:vlan619]RxMlt',
                '[NET:vlan619]TxErr',
                '[NET:vlan619]TxDrp',
                '[NET:vlan619]TxFifo',
                '[NET:vlan619]TxColl',
                '[NET:vlan619]TxCar',
                '[NET:vlan619]TxCmp',
                '[NET:vlan619]RxErrs',
                '[NET:vlan619]TxErrs',
            ], lambda x: x.replace('[NET:vlan619]', 'vlan619:')),
         ):
        print("Plotting %s ..." % title)
        name = title.lower().replace(' ', '_')
        plot_data(cols, name, replacement)
