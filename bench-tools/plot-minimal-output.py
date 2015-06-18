#!/usr/bin/env python
# -*- coding: utf-8 -*-#
# @(#)plot-minimal-output.py
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

from matplotlib import colors as mplcolors
from matplotlib import pylab as plt
import itertools
import numpy as np
import os
import pandas as pd
import sys

# expected_perf = {
#     'cinder': {'osds': 430, 'read_iops': 370, 'write_iops': 270, 'read_bw': 120, 'write_bw': 80}
#     'cinder-l': {'osds': 192, 'read_iops': 370, 'write_iops': 270, 'read_bw': 120, 'write_bw': 80}
# }
# data = pd.read_csv('jbod.3.csv')

# def expected_iops(pool, bs, iodepth):
#     return data
# bw iops             bw iops  bw iops
# pool  test      bs   iodepth                                                 
# local randread  4    32         1494.248816  373    1494.248816  373 NaN  NaN
# 64         1527.643912  381    1527.643912  381 NaN  NaN
# 128  32        36079.281651  281   36079.281651  281 NaN  NaN
# 64        36097.815114  281   36097.815114  281 NaN  NaN
# 4096 32       123186.287205   30  123186.287205   30 NaN  NaN
# randwrite 4    32         1082.091543  270    1082.091543  270 NaN  NaN
# 64         1097.795029  274    1097.795029  274 NaN  NaN
# 128  32        28132.613490  219   28132.613490  219 NaN  NaN
# 64        28700.485578  223   28700.485578  223 NaN  NaN
# 4096 32        81300.212696   19   81300.212696   19 NaN  NaN
# 64        81315.291303   19   81315.291303   19 NaN  NaN


data = None

if len(sys.argv) < 2:
    print("Usage: %s fname fname fname ..." % sys.argv[0])
    sys.exit(0)

for name in sys.argv[1:]:
    try:
        x = pd.read_csv(name)
        x['name'] = name
        if data is None:
            data = x
        else:
            data = data.append(x)
    except Exception as ex:
        print("ERROR parsing file %s: %s" % (name, ex))

data = data.fillna(0)
data['mb/s'] = data['bw']/1024

# Playing with pivot_table
# pivot = pd.pivot_table(data, index=['name', 'pool', 'test', 'bs', 'iodepth'], values=['iops','bw','mb/s'], aggfunc=[np.sum,np.mean,np.std])



def plot_pool(data, pool, what, wlabel=None, tests=None, testlabel='', func=lambda x: x.mean(), funclabel='mean plus std deviation'):
    wlabel = wlabel or what
    xp = data[data.pool == pool]
    alltests = {name:[] for name in data.name.unique()}
    labels = []
    if not tests:
        tests = data.test.unique()
    for test in tests:
        x1 = xp[xp.test == test]
        for iodepth in sorted(data.iodepth.unique()):
            x2 = x1[x1.iodepth == iodepth]
            for bs in sorted(data.bs.unique()):
                x = x2[x2.bs == bs]
                label = 'io:%d\nbs:%d\n%s' % (iodepth, bs, test)
                labels.append(label)
                for testname in sorted(alltests.keys()):
                    curtest_mean = func(x[x.name == testname][what])
                    curtest_dev = x[x.name == testname][what].std()
                    alltests[testname].append((curtest_mean, curtest_dev))

    N = len(labels)
    ind = np.arange(N)
    width = 1.0/(len(alltests)+1)

    fig, ax = plt.subplots()
    ax.set_ylabel(wlabel)
    # ax.set_yscale('log')
    if pool != 'local':
        # parallel = [str(i) for i in sorted(data.groupby(['pool','name','test', 'iodepth', 'bs']).hostname.count().unique())]
        ax.set_title('%s for pool %s (%s)' % (wlabel, pool, funclabel))
    else:
        ax.set_title('%s for pool %s (mean plus std deviation)' % (wlabel, pool))
    ax.set_xticks(ind+len(alltests)/2*width)
    ax.set_xticklabels(labels)

    plots = []
    plotname = []
    idx=0

    # cm = plt.get_cmap('Set2')
    # cm = plt.get_cmap('gist_earth')
    cm = plt.get_cmap('Set3')
    # cm = plt.get_cmap('jet')
    # cm = plt.get_cmap('Dark2')
    # cm = plt.get_cmap('Blues_r')
    
    cmnorm = mplcolors.Normalize(vmin=0,vmax=len(alltests))
    for name in sorted(alltests):
        values = alltests[name]
        t = [i[0] for i in values]
        terr = [i[1] for i in values]
        # plot = ax.bar(ind+idx*width, t, width, color=colors[idx], yerr=terr)
        plot = ax.bar(ind+idx*width, t, width, color=cm(cmnorm(idx)), yerr=terr)
        plots.append(plot)
        name = os.path.basename(name)
        parallel = data[data.name == name].groupby(['pool','iodepth','bs', 'test']).hostname.count()
        maxparallel = parallel.max()
        if name.endswith('.csv'):
            name = name[:-4]
        if name.startswith('ceph.'):
            name = name[5:]
        name += " (%d)" % maxparallel
        plotname.append(name)
        idx += 1

    # Ugly fix
    if what == 'iops':
        ax.legend(plots, plotname, loc='upper right', ncol=2)
    else:
        ax.legend(plots, plotname, loc='upper left', ncol=2)

    fig = plt.gcf()
    fig.set_size_inches(14, 10)
    outfile = ('%s.%s%s.png' % (pool,what, testlabel)).replace('/','')
    print("Saving file %s" % outfile)
    plt.savefig(outfile)
    plt.close()


# plot only randwrite and randread
data = data[(data.test == 'randread') | (data.test == 'randwrite')]
for pool in data.pool.unique():
    if pool not in  ['cinder', 'vhp', 'local', 'cinder-l', 'vhp-l']:
        continue
    for what, wlabel in (('iops', 'iops'),('mb/s', 'bandwidth (mb/s)')):
        for test in data.test.unique():
            plot_pool(data, pool, what, wlabel, tests=[test], testlabel='.'+test)
            plot_pool(data, pool, what, wlabel, tests=[test], testlabel='.aggr.'+test, func=lambda x: x.sum(), funclabel='aggregate')
    for what, wlabel in (('lat', 'latency (ms)'),):
        for test in data.test.unique():
            plot_pool(data, pool, what, wlabel, tests=[test], testlabel='.'+test)
        
