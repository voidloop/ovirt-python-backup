#!/usr/bin/python3

# Python2 compatibility
from __future__ import print_function

import glob
from datetime import datetime

import subprocess
from ovirtsdk4 import types
import logging
import os
import ovirtsdk4 as sdk
import shutil
import sys
import time
import argparse

try:
    import ConfigParser as configparser
except ImportError:
    import configparser

logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    filename=os.path.expanduser("~") + "/log/ovirt_backup.log"
)


class BackupError(Exception):
    pass

class SnapshotError(Exception):
    pass


class AutoSnapshotService:
    def __init__(self, snapshots_service, snapshot):
        self._snapshots_service = snapshots_service
        self._snapshot = snapshot

    def __enter__(self):
        self._snapshot_service = self._snapshots_service.snapshot_service(self._snapshot.id)
        # Poll and wait till the status of the snapshot is 'ok', which means
        # that it is completely created:
        maxtries = 50
        tries = 0
        snapstatus = types.SnapshotStatus.LOCKED
        while snapstatus != types.SnapshotStatus.OK:
            if tries > maxtries:
                raise SnapshotError("Timeout creating snapshot")
            logging.info("Waiting until the snapshot is created, current status is '{}', try {}".format(snapstatus, tries))

            try:
                snapstatus = self._snapshot_service.get().snapshot_status
            except Exception as e:
                logging.error("Error getting snapshot status: '{}'".format(e))

            tries = tries + 1
            time.sleep(2)

        logging.info("Snapshot created, status '{}'".format(snapstatus))
        self._creation_time = datetime.now()
        return self._snapshot_service

    def __exit__(self, *args, **kwargs):
        logging.info("Sending request to remove snapshot '{}'".format(self._snapshot.description))
        self._snapshot_service.remove(wait=True)
        snap = self._snapshot_service.get()
        # Check if the snapshot is removed, try every 10 seconds until this is the case.
        while snap.snapshot_status == types.SnapshotStatus.LOCKED:
            try:
                snap = self._snapshot_service.get()
            except:
                break
            logging.info("Still removing snapshot '{}'.".format(self._snapshot.description))
            time.sleep(10)

class AutoAttachmentService:
    def __init__(self, attachments_service, attachment):
        self._attachments_service = attachments_service
        self._attachment = attachment

    def __enter__(self):
        #time.sleep(60)
        logging.info('Attaching disk {}'.format(self._attachment.disk.id))
        self._attachment_service = self._attachments_service.attachment_service(self._attachment.id)

    def __exit__(self, *args, **kwargs):
        logging.info('Detaching disk {}'.format(self._attachment.disk.id))
        self._attachment_service.remove(wait=True)
        # Call udev to settle the event queue.
        udev_process = subprocess.Popen(['/usr/bin/udevadm', 'settle'], stderr=subprocess.PIPE)
        logging.info('Detached disk {}'.format(self._attachment.disk.id))

class Backup:
    def __init__(self, data_vm_name, versions, agent, export_domain, basedir, migrate):
        self._system_service = self._get_system_service()
        self._events_service = self._system_service.events_service()

        # Set parameters:
        self._data_vm_name = data_vm_name
        self._num_backups = versions
        self._agent_vm_name = agent
        self._export_domain = export_domain
        self._base_backup_dir = basedir
        self._migrate = migrate

        self._data_vm_service = self._get_vm_service(self._data_vm_name)
        self._agent_vm_service = self._get_vm_service(self._agent_vm_name)

    def run(self):
        backup_time = datetime.now()
        self._add_event("Starting backup for {}".format(self._data_vm_name))

        backup_vm_dir = os.path.join(self._base_backup_dir, self._data_vm_name)
        backup_vm_date_dir = os.path.join(backup_vm_dir, backup_time.strftime('%Y%m%d%H%M'))

        os.makedirs(backup_vm_date_dir)

        try:
            self._save_ovf(self._data_vm_service.get(all_content=True), backup_vm_date_dir)
        except AttributeError:
            pass

        snapshots_service = self._data_vm_service.snapshots_service()
        snapshot = snapshots_service.add(
            snapshot=types.Snapshot(
                description='Backup {}'.format(str(backup_time)),
                persist_memorystate=False
            )
        )

        logging.info("Sent request to create snapshot '{}' ({})".format(snapshot.description, snapshot.id))

        try:
            with AutoSnapshotService(snapshots_service, snapshot) as snapshot_service:
                # If the migrate option is specified, try to migrate the agentVM to the same host as the backupVM.
                if self._migrate:
                    try:
                        self._migrate_agent_vm()
                    except Exception as e:
                        # Catch the exception, failure to migrate should not cause the back-up to fail.
                        logging.error('Could not migrate VM, error: {}'.format(e))

                # Create the snapshotted disks.
                self._backup_snapshot_disks(snapshot_service, backup_vm_date_dir)
 
            # Remove old back-ups.
            self._remove_old_backups(backup_vm_dir)
            
            try:
                # Only create symlinks if self._export_domain is defined.
                if self._export_domain:
                    self._create_symlinks(backup_vm_date_dir, os.path.join(self._base_backup_dir,self._export_domain))
                else:
                    logging.info('No export UID defined, not creating symlinks.')
            except Exception as e:
                sys.stderr.write('error creating symlinks:\n\n{}\n'.format(e))
                
            # Write OK file to indicate the backup was completed succesfully.
            ok_file = backup_vm_date_dir + '.OK'
            logging.info("Writing 'OK' file '{}'.".format(ok_file))
            with open(ok_file, 'w') as ok:
                ok.write('backup completed succesfully, {}'.format(time.time()))

            # Let the world know about our great success.
            self._add_event("Backup of VM '{}' completed succesfully.".format(self._data_vm_name))

        except (sdk.Error, BackupError) as err:
            logging.exception(err)
            logging.info("Backup VM '{}' failed! Current backup directory will be removed".format(self._data_vm_name))
            self._remove_dir(backup_vm_date_dir)
            raise

    def _add_event(self, event):
        logging.info("Add event: '{}'".format(event))
        try:
            self._events_service.add(
                    event=types.Event(origin='image backup',
                    severity=types.LogSeverity.NORMAL,
                    # use current time as unique event id.
                    custom_id=int(time.time()),
                    # Add the event to the data VM.
                    vm = self._data_vm_service.get(),
                    description=event)
            )
        except Exception as e:
            logging.error("Error creating event: {}".format(e))

    def _remove_old_backups(self, backup_vm_dir):
        # Search for successful back-ups (those that have a "OK" file).
        backups_ok = sorted(glob.glob(backup_vm_dir + "/*.OK"), key=str.lower)
        logging.info('Rotating backup directories (oldest directories will be removed)')
        while len(backups_ok) > self._num_backups:
            # oldest backup is the top of the list
            delfile = backups_ok.pop(0)
            # Remove OK file.
            self._remove_file(delfile)
            # Remove .OK extension to get the directory name.
            deldir = delfile[:-3]
            # Remove directory.
            self._remove_dir(deldir)

    @staticmethod
    def _remove_dir(directory):
        logging.info("Removing directory: '{}'".format(directory))
        shutil.rmtree(directory)
    
    @staticmethod
    def _remove_file(delfile):
        logging.info("Removing file: '{}'".format(delfile))
        os.unlink(delfile)

    def _create_symlinks(self,source,target):
        image_source_dir = os.path.join(source, 'images')
        image_target_dir = os.path.join(target, 'images')

        logging.info("Creating symlinks: from '{}' to '{}'".format(source,target))

        # First link the disk images.
        for image in os.listdir(image_source_dir):
            link_target = os.path.join(image_target_dir, image)

            # We want to create relative symlinks which will also work when the export domain is mounted
            # under a different mountpoint.
            link_source = os.path.relpath(os.path.join(image_source_dir, image),image_target_dir)

            # Unlink the target (if it already exists as a symlink).
            if os.path.islink(link_target):
                os.unlink(link_target)

            logging.info("Symlink '{}' to '{}'".format(link_source,link_target))
            os.symlink(link_source, link_target)

        # Link the OVF.
        ovf_source_dir = os.path.join(source, 'ovf')
        ovf_target_dir = os.path.join(target, 'master/vms')

        for ovf in os.listdir(ovf_source_dir):
            link_target = os.path.join(ovf_target_dir, ovf)
            # Use a relative source for the symlink to make the export robust in case of different mountpoints.
            link_source = os.path.relpath(os.path.join(ovf_source_dir, ovf),ovf_target_dir)
            if os.path.islink(link_target):
                os.unlink(link_target)

            logging.info("Symlink '{}' to '{}'".format(link_source,link_target))
            os.symlink(link_source, link_target)

    def _migrate_agent_vm(self):
        agent_vm = self._agent_vm_service.get()
        data_vm = self._data_vm_service.get()

        if agent_vm.host.id != data_vm.host.id:
            logging.info("Migrating VM '{}' from '{}' to '{}'".format(agent_vm.name, agent_vm.host.id, data_vm.host.id))
            self._agent_vm_service.migrate(cluster=data_vm.cluster, host=data_vm.host, wait=True)
            while self._agent_vm_service.get().status == types.VmStatus.MIGRATING:
                time.sleep(10)

    @staticmethod
    def _save_ovf(data_vm, backup_dir):
        # Save the OVF to a file, so that we can use to restore the virtual
        # machine later.
        ovf_data = data_vm.initialization.configuration.data
        ovf_dir = os.path.join(backup_dir, 'ovf', format(data_vm.id))
        os.makedirs(ovf_dir)
        ovf_file = os.path.join(ovf_dir, '{}.ovf'.format(data_vm.id))

        with open(ovf_file, 'w') as ovs_fd:
            ovs_fd.write(ovf_data)

        logging.info("Wrote OVF to file '{}'".format(os.path.abspath(ovf_file)))

    @staticmethod
    def _get_system_service():
        ovirt_shell_config = os.path.expanduser("~") + "/.ovirtshellrc"

        config = configparser.ConfigParser()
        config.read(ovirt_shell_config)

        connection = sdk.Connection(url=config.get('ovirt-shell', 'url'),
                                    username=config.get('ovirt-shell', 'username'),
                                    password=config.get('ovirt-shell', 'password'),
                                    # ca_file=API_CA_FILE,
                                    insecure=True,
                                    debug=True,
                                    log=logging.getLogger())
        logging.info('Connected to the server')
        return connection.system_service()

    def _get_vm_service(self, vm_name):
        vm_list = self._vms_service.list(search='name={}'.format(vm_name))

        try:
            return self._vms_service.vm_service(vm_list[0].id)
        except IndexError:
            raise BackupError("VM '{}' doesn't exist!".format(vm_name))

    @property
    def _vms_service(self):
        return self._system_service.vms_service()

    def _backup_snapshot_disks(self, snapshot_service, backup_vm_date_dir):
        disks_service = snapshot_service.disks_service()
        snapshot = snapshot_service.get()

        attachments_service = self._agent_vm_service.disk_attachments_service()
        for snapshot_disk in disks_service.list():
            attachment = attachments_service.add(
                attachment=types.DiskAttachment(
                    disk=types.Disk(
                        id=snapshot_disk.id,
                        snapshot=types.Snapshot(id=snapshot.id)
                    ),
                    active=True,
                    bootable=False,
                    interface=types.DiskInterface.VIRTIO
                )
            )
            image_id = snapshot_disk.image_id
            image_size = snapshot_disk.provisioned_size
            image_description = snapshot_disk.description if snapshot_disk.description else 'None'
            logging.info('Attach disk {}'.format(attachment.disk.id))

            with AutoAttachmentService(attachments_service, attachment):
                    self._copy_disk(attachment, backup_vm_date_dir, image_id, image_size, image_description)

    def _copy_disk(self, attachment, directory, image_id, image_size, image_description):
        input_file = self._find_data_disk(attachment)
        # Wait for device te settle.
        logging.info('Waiting for device {} to settle'.format(input_file))
        time.sleep(5)
        udev_process = subprocess.Popen(['/usr/bin/udevadm', 'settle'], stderr=subprocess.PIPE)
        time.sleep(5)

        output_dir = os.path.join(directory,'images', attachment.disk.id)
        os.makedirs(output_dir)

        output_file = os.path.join(output_dir, image_id)

        cmd_args = ['dd', 'bs=1M','iflag=nocache', 'conv=sparse', 'if={}'.format(input_file), 'of={}'.format(output_file)]
        logging.info('Executing command: {}'.format(subprocess.list2cmdline(cmd_args)))

        dd_process = subprocess.Popen(cmd_args, stderr=subprocess.PIPE)
        _, err = dd_process.communicate()

        for line in err.splitlines():
            logging.info('Command output: {}'.format(line.decode().rstrip()))

        if dd_process.returncode != 0:
            raise BackupError('Copy failed! (exit code {})'.format(dd_process.returncode))

        # Set a fake value for export_domain if it is not defined.
        export_domain = self._export_domain or '00000000-0000-0000-0000-000000000000'
        
        meta_out = output_file + '.meta'
        logging.info('Writing metafile: {}'.format(meta_out))

        with open( meta_out, 'w' ) as metafile:
            metafile.write('''CTIME={}
DESCRIPTION={}
DISKTYPE=DATA
DOMAIN={}
FORMAT=RAW
GEN=0
IMAGE={}
LEGALITY=LEGAL
MTIME=0
PUUID=00000000-0000-0000-0000-000000000000
SIZE={}
TYPE=PREALLOCATED
VOLTYPE=LEAF
EOF
'''.format(int(time.time()),image_description, export_domain, attachment.disk.id, int(image_size/512)))


    @staticmethod
    def _find_data_disk(attachment):
        maxtries = 6
        tries = 0
        while tries < maxtries:
            logging.info('Searching for attached disk: {}'.format(attachment.disk.id))
            tries = tries + 1
            for path in glob.glob('/sys/block/*/serial'):
                with open(path, 'r') as file:
                   serial = file.read()
                if serial == attachment.disk.id[:20]:
                    # path.split('/') == ('', 'sys', 'block', disk, 'serial')
                    disk = path.split('/')[3]
                    logging.info('Disk {} found, device = {}.'.format(attachment.disk.id, disk))
                    return '/dev/{}'.format(disk)
            logging.info('Disk {} not found yet, sleep and retry.'.format(attachment.disk.id))
            time.sleep(5)
        raise BackupError('Cannot find any usable disk for attachment id {}'.format(attachment.disk.id))

# TODO: create a class to move data
# TODO: add events to ovirt console

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("vmname", type=str,
            help="Basedir for backups. (MANDATORY)")
    parser.add_argument("-b", "--basedir", type=str, dest="basedir",
            help="Basedir for backups. (MANDATORY)")
    parser.add_argument("-a", "--agentvm", type=str, dest="agentvm",
            help="Name of the VM to attach the disks to (normally the VM where this script is running). (MANDATORY)")
    parser.add_argument("-e", "--export-domain", type=str, dest="export_domain",
            help="UID of the export domain to link the backup to for easy restores, this should be a directory in the root of the basedir. (OPTIONAL)")
    parser.add_argument("-n", "--numkeep", type=int, dest="versions", default=7,
            help="Number of versions to keep. (OPTIONAL, default = 7)")
    parser.add_argument("-m", "--migrate-vm", action='store_const', dest="migrate", const=1,
            help="Try to migrate the agent VM to the same host as the VM to back-up, the VMs have to be in the same oVirt cluster.")

    args = parser.parse_args()

    # Check if required options are valid and specified.
    if not ( args.vmname and args.basedir and args.agentvm ):
        exit(parser.print_help())
       
    vm_name = args.vmname

    try:
        b = Backup(vm_name, args.versions, args.agentvm, args.export_domain, args.basedir, args.migrate)
        b.run()
    except (sdk.Error, BackupError) as err:
        print("Backup of the virtual machine '{}' failed. "
              "See '/var/log/ovirt_backup.log' for details.".format(vm_name),
              file=sys.stderr)
        print("Error: {}".format(err), file=sys.stderr)


if __name__ == '__main__':
    main()
