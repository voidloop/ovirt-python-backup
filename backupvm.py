#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright (c) 2017 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os
import sys
import time
import uuid
import glob
import subprocess

import ovirtsdk4 as sdk
import ovirtsdk4.types as types

try:
    import ConfigParser as configparser
except ImportError:
    import configparser

config = configparser.ConfigParser()
config.read('/root/.ovirtshellrc')

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s', 
    datefmt='%Y/%m/%d %H:%M:%S', 
    level=logging.INFO, 
    filename='/var/log/ovirt-backup.log'
)

# Number of backup (default = 3)
NUM_BACKUP = 3
if len(sys.argv) >= 3:
    NUM_BACKUP = int(sys.argv[2])

# The connection details:
API_URL = config.get('ovirt-shell', 'url')
API_USER = config.get('ovirt-shell', 'username')
API_PASSWORD = config.get('ovirt-shell', 'password')

# The file containing the certificat of the CA used by the server. In
# an usual installation it will be in the file '/etc/pki/ovirt-engine/ca.pem'.
# API_CA_FILE = '/usr/local/backup/ca.pem'

# The name of the application, to be used as the 'origin' of events
# sent to the audit log:
APPLICATION_NAME = 'ov-backup'

# The name and the ID of the virtual machine that contains the data 
# that we want to back-up:
DATA_VM_NAME = sys.argv[1]
# DATA_VM_ID = sys.argv[2]

# The IDs of the disks that we want to back-up:
# DATA_VM_DISKS = sys.argv[3:]

BACKUP_DIR = '/mnt/ovirt-backup/%s' % DATA_VM_NAME
BACKUP_DATE = time.strftime("%Y%m%d%H%M")

# The name of the virtual machine where we will attach the disks in
# order to actually back-up them. This virtual machine will usually have
# some kind of back-up software installed.
AGENT_VM_NAME = 'ov-backup'

# Connect to the server:
connection = sdk.Connection(
    url=API_URL,
    username=API_USER,
    password=API_PASSWORD,
    # ca_file=API_CA_FILE,
    insecure=True,
    debug=True,
    log=logging.getLogger(),
)
logging.info('Connected to the server.')

# Get the reference to the root of the services tree:
system_service = connection.system_service()

# Get the reference to the service that we will use to send events to
# the audit log:
events_service = system_service.events_service()

# In order to send events we need to also send unique integer ids. These
# should usually come from an external database, but in this example we
# will just generate them from the current time in seconds since Jan 1st
# 1970.
event_id = int(time.time())

# Get the reference to the service that manages the virtual machines:
vms_service = system_service.vms_service()

# Find the virtual machine that we want to back up. Note that we need to
# use the 'all_content' parameter to retrieve the retrieve the OVF, as
# it isn't retrieved by default:
data_vm_list = vms_service.list(
    search='name=%s' % DATA_VM_NAME,
    all_content=True,
)

if len(data_vm_list) == 0: 
    msg = 'Could not find data virtual machine \'%s\'.' % DATA_VM_NAME
    logging.error(msg)
    print(msg, file=sys.stderr)
    sys.exit(1)

data_vm = data_vm_list[0]

logging.info(
    'Found data virtual machine \'%s\', the id is \'%s\'.',
    data_vm.name, data_vm.id,
)

# If virtual machine is not up, skip backup. 
if data_vm.status != types.VmStatus.UP:
    logging.info(
        'Backup skipped: data virtual machine \'%s\' is not \'up\'.',
        data_vm.name,
    )
    connection.close()
    sys.exit(0)

# Find the virtual machine were we will attach the disks in order to do
# the backup:
agent_vm = vms_service.list(
    search='name=%s' % AGENT_VM_NAME,
)[0]
logging.info(
    'Found agent virtual machine \'%s\', the id is \'%s\'.',
    agent_vm.name, agent_vm.id,
)

# Find the services that manage the data and agent virtual machines:
data_vm_service = vms_service.vm_service(data_vm.id)
agent_vm_service = vms_service.vm_service(agent_vm.id)

# Create an unique description for the snapshot, so that it is easier
# for the administrator to identify this snapshot as a temporary one
# created just for backup purposes:
snap_description = '%s-backup-%s' % (data_vm.name, uuid.uuid4())

# Send an external event to indicate to the administrator that the
# backup of the virtual machine is starting. Note that the description
# of the event contains the name of the virtual machine and the name of
# the temporary snapshot, this way, if something fails, the administrator
# will know what snapshot was used and remove it manually.
events_service.add(
    event=types.Event(
        vm=types.Vm(
          id=data_vm.id,
        ),
        origin=APPLICATION_NAME,
        severity=types.LogSeverity.NORMAL,
        custom_id=event_id,
        description=(
            'Backup of virtual machine \'%s\' using snapshot \'%s\' is '
            'starting.' % (data_vm.name, snap_description)
        ),
    ),
)
event_id += 1


# Create directory where stored backup
backup_dir_date = '%s/%s' % (BACKUP_DIR, BACKUP_DATE)
os.makedirs(backup_dir_date)

# Save the OVF to a file, so that we can use to restore the virtual
# machine later. The name of the file is the name of the virtual
# machine, followed by a dash and the identifier of the virtual machine,
# to make it unique:
ovf_data = data_vm.initialization.configuration.data
ovf_file = '%s/%s-%s.ovf' % (backup_dir_date, data_vm.name, data_vm.id)
with open(ovf_file, 'w') as ovs_fd:
    ovs_fd.write(ovf_data)

logging.info('Wrote OVF to file \'%s\'.', os.path.abspath(ovf_file))

# Send the request to create the snapshot. Note that this will return
# before the snapshot is completely created, so we will later need to
# wait till the snapshot is completely created.
snaps_service = data_vm_service.snapshots_service()
snap = snaps_service.add(
    snapshot=types.Snapshot(
        description=snap_description,
        persist_memorystate=False,
    ),
)
logging.info(
    'Sent request to create snapshot \'%s\', the id is \'%s\'.',
    snap.description, snap.id,
)

# Poll and wait till the status of the snapshot is 'ok', which means
# that it is completely created:
snap_service = snaps_service.snapshot_service(snap.id)
logging.info(
    'Waiting till the snapshot is created, the status is now \'%s\'.',
    snap.snapshot_status,
)
while snap.snapshot_status != types.SnapshotStatus.OK:
    time.sleep(1)
    snap = snap_service.get()
logging.info('The snapshot is now complete.')

time.sleep(5)

agent_host = agent_vm_service.get().host
data_host = data_vm_service.get().host

if agent_host.id != data_host.id:
    name = agent_vm_service.get().name

    data_cluster = data_vm_service.get().cluster

    logging.info("Migrating VM '%s' to '%s'." % (name, data_host.id))

    agent_vm_service.migrate(cluster=data_cluster, host=data_host, wait=True)
    
    while agent_vm_service.get().status == types.VmStatus.MIGRATING:
        time.sleep(10)

    logging.info("The VM '%s' is migrated." % name)

time.sleep(5)

# Retrieve the descriptions of the disks of the snapshot:
snap_disks_service = snap_service.disks_service()
snap_disks = snap_disks_service.list()

# Attach all the disks of the snapshot to the agent virtual machine, and
# save the resulting disk attachments in a list so that we can later
# detach them easily:
attachments_service = agent_vm_service.disk_attachments_service()
attachments = []

for snap_disk in snap_disks:

    a = types.DiskAttachment(
        disk=types.Disk(
            id=snap_disk.id, 
            snapshot=types.Snapshot(
                id=snap.id,
            ),
        ),
        active=True,
        bootable=False,
        interface=types.DiskInterface.VIRTIO,
    )

    attachment = attachments_service.add(
        attachment=a, 
    )

    attachments.append(attachment)

    logging.info(
        'Attached disk \'%s\' to the agent virtual machine.',
        attachment.disk.id,
    )

# Now the disks are attached to the virtual agent virtual machine, we
# can then ask that virtual machine to perform the backup. Doing that
# requires a mechanism to talk to the backup software that runs inside the
# agent virtual machine. That is outside of the scope of the SDK. But if
# the guest agent is installed in the virtual machine then we can
# provide useful information, like the identifiers of the disks that have
# just been attached.

# for attachment in attachments:
#     if attachment.logical_name is not None:
#         logging.info(
#             'Logical name for disk \'%s\' is \'%s\'.',
#             attachment.disk.id, attachment.logicalname,
#         )
#     else:
#         logging.info(
#             'The logical name for disk \'%s\' isn\'t available. Is the '
#             'guest agent installed?',
#             attachment.disk.id,
#         )

# Insert here the code to contact the backup agent and do the actual
# backup ...
logging.info('Doing the actual backup ...')

########################################################################
########################################################################
########################################################################

transfer_completed = True

try:
    for attachment in attachments:
        inputfile = glob.glob('/dev/disk/by-id/*%s' % attachment.disk.id[:20])[0]
        if_arg = "if=%s" % inputfile
        of_arg = "of=%s/%s" % (backup_dir_date, attachment.disk.id)
        cmd_args = ['dd', if_arg, of_arg]
        logging.info('Executing command: \'%s\'', subprocess.list2cmdline(cmd_args))
        dd_process = subprocess.Popen(cmd_args, stderr=subprocess.PIPE)
        (out, err) = dd_process.communicate()

        for line in err.splitlines():
            msg = 'Command output: %s' % line.rstrip()
            logging.info(msg)

        if dd_process.returncode != 0:
            msg = 'Copy data from \'%s\' to \'%s/%s\' failed' % (inputfile, backup_dir_date, attachment.disk.id)
            logging.error(msg)
            print(msg, file=sys.stderr)
            transfer_completed = False
            break

except Exception as e:
    logging.error(str(e),)

# Rotate backups:
if transfer_completed:
    dir_list = sorted(glob.glob(BACKUP_DIR + "/*"), key=str.lower)
    while len(dir_list) > NUM_BACKUP:
        logging.info('Rotating backup directories (oldest directory will be removed)')
        # oldest backup is the top of the list
        cmd = 'rm -rf ' + dir_list.pop(0)
        logging.info('Executing command: \'%s\'', cmd)
        os.system(cmd)
 
########################################################################
########################################################################
########################################################################

# time.sleep(60)

# Detach the disks from the agent virtual machine:
for attachment in attachments:
    attachment_service = attachments_service.attachment_service(attachment.id)
    attachment_service.remove()
    logging.info(
        'Detached disk \'%s\' to from the agent virtual machine.',
        attachment.disk.id,
    )

# Remove the snapshot:
try:
    snap_service.remove()
    logging.info('Removed the snapshot \'%s\'.', snap.description)
except sdk.Error as e:
    logging.info('Error during remove the snapshot \'%s\'.', snap.description)
    print('%s: %s' % (DATA_VM_NAME, e))

# Send an external event to indicate to the administrator that the
# backup of the virtual machine is completed:
events_service.add(
    event=types.Event(
        vm=types.Vm(
          id=data_vm.id,
        ),
        origin=APPLICATION_NAME,
        severity=types.LogSeverity.NORMAL,
        custom_id=event_id,
        description=(
            'Backup of virtual machine \'%s\' using snapshot \'%s\' is '
            'completed.' % (data_vm.name, snap_description)
        ),
    ),
)

# Close the connection to the server:
connection.close()
