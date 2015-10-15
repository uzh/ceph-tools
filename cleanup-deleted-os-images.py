#!/usr/bin/env python
# -*- coding: utf-8 -*-#
#
#
# Copyright (C) 2015, S3IT, University of Zurich. All rights reserved.
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
"""
This program is used to delete rbd volumes created using OpenStack and
"soft-deleted".

It is intended to work on an OpenStack Kilo installation with the
following patch applied: https://review.openstack.org/#/c/125963/

In current kilo version (2015.1.1) when a snapshot is created the
image is downloaded on the compute node and then uploaded to
glance. The mentioned patch instead uses the internal ceph "cloning"
feature, to directly clone the image within ceph, without the need of
downloading the image on the compute node. This allows creation of
snapshots from instances with a root disk which is bigger than the
local disk of the compute node.

However, when a snapshot or ephemeral disk is deleted but another rbd
volumes is cloned from it, instead of deleting the image this is
renamed by appending `to_be_deleted_by_glance`. For instance: if you
create a VM, create a snapshot from it and then delete the VM disk,
its rbd volume is renamed, and *never* deleted by OpenStack.

This tool allows to find all volumes that have been deleted and
depends on other volumes that can be safely deleted. In other words,
considering the graph of all the rbd volumes and their snapshots and
clones, it builds all the connected components of the graph and delets
those that only contains rbd volumes that were deleted by OpenStack
but not from ceph.

"""
__docformat__ = 'reStructuredText'
__author__ = 'Antonio Messina <antonio.s.messina@gmail.com>'

import argparse
import cPickle as pickle
import networkx as nx
import rados
import rbd

def cluster_connect(pool, conffile, rados_id):
    cluster = rados.Rados(conffile=conffile, rados_id=rados_id)
    cluster.connect()
    ioctx = cluster.open_ioctx(pool)
    return ioctx


def build_layering_graph(ioctx, pool, filter_volumes = lambda x: True):
    """Returns a netowrkx DAG of all the rbd volumes and snapshots"""
    rbd_inst = rbd.RBD()

    # List all "interesting" volumes. By default, all volumes.
    volumenames = [vol for vol in rbd_inst.list(ioctx) if filter_volumes(vol)]

    # These are used for caching
    snapshots = []
    volumes = {}

    # Build an empty graph
    graph = nx.DiGraph()

    # To compute a graph of all the interesting rbd volumes, we need first
    # to list all the volumes. For each one of them, we find all the
    # snapshots. Later on, we will see the "children" of all the
    # snapshots, if any.

    # Analyze rbd volumes and collect their snapshots, and add edges
    # volume -> snapshot
    for name in volumenames:
        print("Checking volume %s" % name)
        volume = rbd.Image(ioctx, name, read_only=True)
        volumes[name] = volume
        color='black'
        graph.add_node(name)

        for snap in volume.list_snaps():
            snapname = volume.name + '@' + snap['name']
            snapshots.append({'volume': name,
                              'snap': snap['name']})
            graph.add_node(snapname)
            graph.add_edge(name, snapname)

    # Analyze snapshots and add edges snapshot -> volume
    for snap in snapshots:
        vol = snap['volume']
        snapname = snap['snap']
        print("Checking snapshot %s@%s" % (vol, snapname))

        volume = rbd.Image(ioctx, vol, snapshot=snapname, read_only=True)
        volumes['%s@%s' % (vol, snapname)] = volume

        for volpool, name in volume.list_children():
            if volpool != pool:
                print("WARNING: Image %s@%s has clone on a different pool: %s"
                      % (vol, snapname, volpool))
            graph.add_edge('%s@%s' % (volume.name, snapname), name)
    return graph


def find_connected_components(origgraph):
    graph = origgraph.copy()
    subgraphs = []

    # Find subgraphs (connected components)
    for g in nx.topological_sort(graph):
        if g in graph:
            nodes = nx.shortest_path(graph, source=g).keys()
            sub = graph.subgraph(nodes)
            subgraphs.append(sub)
            graph.remove_nodes_from(sub)
    return subgraphs


def graph_can_be_deleted(graph):
    to_be_deleted = []
    for n in graph:
        # volume/snapshot cannot be deleted, thus the graph cannot be deleted
        if 'to_be_deleted' not in n:
            return False
        if '@' in n:
            volume, snapshot = n.split('@')
            if False in ['to_be_deleted' in i for i in (volume, snapshot)]:
                # this is a snapshot, but either the image or the
                # snapshot cannot be deleted.
                return False
        # both volume and snapshot are to be deleted Loop over all the
        # other nodes until you find a volume/snapshot that cannot be
        # deleted, or...

    # *all* volumes and snapshots can be deleted, delete the whole
    # graph.
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--pool',
                        default='cinder',
                        help='Ceph pool to use. Default: %(default)s')
    parser.add_argument('-c', '--conf', metavar='FILE',
                        default='/etc/ceph/ceph.conf',
                        help='Ceph configuration file. '
                        'Default: %(default)s')
    parser.add_argument('-u', '--user',
                        default='cinder',
                        help='Ceph user to use to connect. '
                        'Default: %(default)s')
    parser.add_argument('-s', '--save', metavar='FILE',
                        default=None,
                        help='Save graph using Pickle to file, for '
                        'further analysis. Default: do not save.')
    parser.add_argument('--load', metavar='FILE',
                        default=None,
                        help='Instead of building the graph from ceph, '
                        'use the supplied file.')
    parser.add_argument('-f', '--force',
                        action='store_true',
                        help='Instead of printing rbd commands to cleanup '
                        'deleted images, actually delete them.')
    cfg = parser.parse_args()
    ioctx = cluster_connect(cfg.pool, cfg.conf, cfg.user)
    rbd_inst = rbd.RBD()

    # Build the graph of volumes/snapshots
    if cfg.load:
        graph = pickle.load(open(cfg.load))
    else:
        def _filter_volumes_and_disks(x):
            return not x.startswith('volume-') and not x.endswith('_disk')
        graph = build_layering_graph(ioctx, cfg.pool,
                                     filter_volumes=_filter_volumes_and_disks)

    # Save the graph, for later postprocessing
    if cfg.save:
        pickle.dump(graph, open(cfg.save, 'w'))

    # Now we have a big graph, but we actually want to find the connected
    # components. Main goal: find connected components made of images that
    # can be deleted (thus, the whole connected component can be deleted)
    subgraphs = find_connected_components(graph)

    to_delete = []
    for sub in subgraphs:
        if graph_can_be_deleted(sub):
            to_delete.append(sub)


    # Cleanup connected components
    for g in to_delete:
        for n in reversed(nx.topological_sort(g)):
            if not cfg.force:
                if '@' in n:
                    print("rbd -p %s snap unprotect %s" % (cfg.pool, n))
                else:
                    print("rbd -p %s snap purge %s" % (cfg.pool, n))
                    print("rbd -p %s rm %s" % (cfg.pool, n))
                    print("")
            else:
                if '@' not in n:
                    yn = raw_input("Confirm deletion of image %s "
                                   "and all its snapshots [yN]: " % n)
                    if yn.lower() in ['y', 'yes']:
                        print("Deleting image %s and all its snapshots" % n)
                        image = rbd.Image(ioctx, n)
                        for snap in image.list_snaps():
                            image.unprotect_snap(snap)
                            image.remove_snap(snap)
                        del image
                        rbd_inst.remove(ioctx, n)
                    else:
                        print("Skipping")
