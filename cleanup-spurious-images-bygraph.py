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
"""
__docformat__ = 'reStructuredText'
__author__ = 'Hanieh Rajabi <hanieh.rajabi@gmail.com>'

import os
import argparse
import cPickle as pickle
import rados
import rbd
import sys
import re
import sys
import logging
from keystoneclient.auth.identity import v3
from keystoneclient import session
from keystoneclient.v3 import client as keystone_client
from novaclient import client as nova_client
from cinderclient import client as cinder_client
import cinderclient.exceptions as cex

log = logging.getLogger()
log.addHandler(logging.StreamHandler())
volume_re = re.compile('^volume-(?P<uuid>\w{8}-\w{4}-\w{4}-\w{4}-\w{12})')
#image_re = re.compile('(?P<uuid>\w{8}-\w{4}-\w{4}-\w{4}-\w{12})-disk$')
image_re = re.compile('(?P<uuid>\w{8}-\w{4}-\w{4}-\w{4}-\w{12})$')

class EnvDefault(argparse.Action):
    # This is took from
    # http://stackoverflow.com/questions/10551117/setting-options-from-environment-variables-when-using-argparse
    def __init__(self, envvar, required=True, default=None, **kwargs):
        if envvar and envvar in os.environ:
            default = os.environ[envvar]
        if required and default:
            required = False
        super(EnvDefault, self).__init__(default=default, required=required,
                                         **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


def make_session(opts):
    """Create a Keystone session"""
    auth = v3.Password(auth_url=opts.os_auth_url,
                       username=opts.os_username,
                       password=opts.os_password,
                       project_name=opts.os_project_name,
                       user_domain_name=opts.os_user_domain_name,
                       project_domain_name=opts.os_project_domain_name)
    sess = session.Session(auth=auth)
    return sess


def cluster_connect(pool, conffile, rados_id):
    cluster = rados.Rados(conffile=conffile, rados_id=rados_id)
    cluster.connect()
    ioctx = cluster.open_ioctx(pool)
    return ioctx

def volume_lookup(cclient,nclient,volumenames):
    log.info("Got information about %d volumes", len(volumenames))
    # Inizializza una lista
    to_delete= []
    snap_to_delete= []
    snap_vol_to_delete = []
    inst_to_delete = []
    snapshots = []
    volumes = {}
    for name in volumenames:
        uuid = volume_re.search(name).group('uuid')
        log.debug("Checking if cinder volume %s exists", uuid)
        try:
            cclient.volumes.get(uuid)
            log.debug("Volume %s exists.", uuid)
        except cex.NotFound:
            log.debug("This %s rbd image should be deleted", uuid)
            
            #Check if there is snapshots for the volume
            volume = rbd.Image(ioctx, name, read_only=True)
            volumes[name] = volume
            
            #iterate on snapshots (in dict format)
            for snap in volume.list_snaps():
                snapname = name + '@' + snap['name']
                snapshots.append({'volume': name,
                              'snap': snap['name']})
                snap_to_delete.append("This rbd %s has %s" %(name,snap['name']) )
            
            #iterate on snapshot list to find any child
            for snap in snapshots:
                vol = snap['volume']
                snapname = snap['snap']
                print("Checking snapshot %s@%s" % (vol, snapname))

                snap_volumes = rbd.Image(ioctx, vol, snapshot=snapname, read_only=True)
                volumes['%s@%s' % (vol, snapname)] = snap_volume

                for volpool, name in volume.list_children():
                    if volpool != cfg.pool:
                    print("WARNING: Image %s@%s has clone on a different pool: %s"
                          % (vol, snapname, volpool))
                    snap_vol_to_delete.append("rbd -p %s flatten %s " %(cfg.pool,snapname))
                snap_vol_to_delete.append("rbd -p %s snap unprotect %s@%s " %(cfg.pool,vol,snapname))
                snap_vol_to_delete.append("rbd -p %s snap rm  %s@%s " %(cfg.pool,vol,snapname))
            vol_instance = cclient.volumes.get(uuid)
            if vol_instance.bootable:
                if vol_instance.attachments[0]['device'] == '/dev/vda':
                    inst_to_delete.append("there is %s instance booting from %s volume" 
                        % (vol_instance.attachments[0]['server_id'],uuid))
                
            to_delete.append("rbd -p %s rm %s" % (cfg.pool, name))
    print "This is the list of commnads you should issue"
    print str.join('\n', to_delete)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--os-username',
                        action=EnvDefault,
                        envvar="OS_USERNAME",
                        help='OpenStack administrator username. If not supplied, the value of the '
                        '"OS_USERNAME" environment variable is used.')
    parser.add_argument('--os-password',
                        action=EnvDefault,
                        envvar="OS_PASSWORD",
                        help='OpenStack administrator password. If not supplied, the value of the '
                        '"OS_PASSWORD" environment variable is used.')
    parser.add_argument('--os-project-name',
                        action=EnvDefault,
                        envvar="OS_PROJECT_NAME",
                        help='OpenStack administrator project name. If not supplied, the value of the '
                        '"OS_PROJECT_NAME" environment variable is used.')
    parser.add_argument('--os-auth-url',
                        action=EnvDefault,
                        envvar="OS_AUTH_URL",
                        help='OpenStack auth url endpoint. If not supplied, the value of the '
                        '"OS_AUTH_URL" environment variable is used.')
    parser.add_argument('--os-user-domain-name',
                        action=EnvDefault,
                        envvar="OS_USER_DOMAIN_NAME",
                        default='default')
    parser.add_argument('--os-project-domain-name',
                        action=EnvDefault,
                        envvar="OS_PROJECT_DOMAIN_NAME",
                        default='default')
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

    parser.add_argument('-v', '--verbose', action='count', default=0,
                        help='Increase verbosity')
   
    cfg = parser.parse_args()
    # Set verbosity
    verbosity = max(0, 3-cfg.verbose) * 10
    log.setLevel(verbosity)

    ioctx = cluster_connect(cfg.pool, cfg.conf, cfg.user)
    rbd_inst = rbd.RBD()
    sess = make_session(cfg)
    cclient = cinder_client.Client('2', session=sess)
    nclient = nova_client.Client('2',session=sess)
    volumenames = [vol for vol in rbd_inst.list(ioctx) if volume_re.match(vol)]
    imagenames = [ img for img in rbd_inst.list(ioctx) if image_re.match(img)]
    
    if volumenames:
        volume_lookup(cclient,nclient,volumenames)
    if imagenames:
        image_lookup(os_client,imagenames)
#    log.info("Got information about %d images",len(imagenames))







