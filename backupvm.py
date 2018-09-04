#!/usr/local/bin/python3
import glob
from datetime import datetime

import subprocess
from ovirtsdk4 import types
import logging
import os
import ovirtsdk4 as sdk
import sys
import time

try:
    import ConfigParser as configparser
except ImportError:
    import configparser

logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y/%m/%d %H:%M:%S',
    level=logging.INFO,
    filename='/var/log/ovirt_backup.log'
)


class BackupError(Exception):
    pass


def delayed(seconds):
    def decorator(f):
        def wrapper(*args, **kwargs):
            time.sleep(seconds)
            return f(*args, **kwargs)
        return wrapper
    return decorator


class AutoSnapshotService:
    def __init__(self, snapshots_service, snapshot):
        self._snapshots_service = snapshots_service
        self._snapshot = snapshot

    def __enter__(self):
        self._snapshot_service = self._snapshots_service.snapshot_service(self._snapshot.id)
        # Poll and wait till the status of the snapshot is 'ok', which means
        # that it is completely created:
        logging.info("Waiting till the snapshot is created, current status is '{}'"
                     .format(self._snapshot.snapshot_status))
        while self._snapshot_service.get().snapshot_status != types.SnapshotStatus.OK:
            time.sleep(1)
        self._creation_time = datetime.now()
        return self._snapshot_service

    @delayed(seconds=30)
    def __exit__(self, *args, **kwargs):
        self._snapshot_service.remove(wait=True)
        logging.info("Sent request to remove snapshot '{}'".format(self._snapshot.description))


class AutoAttachmentService:
    def __init__(self, attachments_service, attachment):
        self._attachments_service = attachments_service
        self._attachment = attachment

    @delayed(seconds=60)
    def __enter__(self):
        self._attachment_service = self._attachments_service.attachment_service(self._attachment.id)

    @delayed(seconds=30)
    def __exit__(self, *args, **kwargs):
        logging.info('Detaching disk {}'.format(self._attachment.disk.id))
        self._attachment_service.remove(wait=True)


class Backup:
    _base_backup_dir = '/mnt/ovirt-backup'
    _application_name = 'ovirt_backup'
    _agent_vm_name = 'ov-backup'
    _num_backups = 3

    def __init__(self, data_vm_name):
        self._system_service = self._get_system_service()
        self._events_service = self._system_service.events_service()

        self._data_vm_name = data_vm_name
        self._data_vm_service = self._get_vm_service(data_vm_name)
        self._agent_vm_service = self._get_vm_service(self._agent_vm_name)

    def run(self):
        backup_time = datetime.now()

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
                description=str(backup_time),
                persist_memorystate=False
            )
        )

        logging.info("Sent request to create snapshot '{}' ({})".format(snapshot.description, snapshot.id))

        try:
            with AutoSnapshotService(snapshots_service, snapshot) as snapshot_service:
                self._migrate_agent_vm()
                self._backup_snapshot_disks(snapshot_service, backup_vm_date_dir)
            self._remove_old_backups(backup_vm_dir)
            logging.info("Backup VM '{}' finished".format(self._data_vm_name))
        except BackupError as err:
            logging.exception(err)
            logging.info("Backup VM '{}' failed! Current backup directory will be removed".format(self._data_vm_name))
            self._remove_dir(backup_vm_date_dir)
            raise

    def _remove_old_backups(self, backup_vm_dir):
        dir_list = sorted(glob.glob(backup_vm_dir + "/*"), key=str.lower)
        logging.info('Rotating backup directories (oldest directories will be removed)')
        while len(dir_list) > self._num_backups:
            # oldest backup is the top of the list
            self._remove_dir(dir_list.pop(0))

    @staticmethod
    def _remove_dir(directory):
        cmd = 'rm -rf {}'.format(directory)
        logging.info("Removing directory: '{}'".format(directory))
        os.system(cmd)

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
        # machine later. The name of the file is the name of the virtual
        # machine, followed by an underscore and the identifier of the virtual machine,
        # to make it unique:
        ovf_data = data_vm.initialization.configuration.data
        ovf_file = os.path.join(backup_dir, '{}_{}.ovf'.format(data_vm.name, data_vm.id))

        with open(ovf_file, 'w') as ovs_fd:
            ovs_fd.write(ovf_data)

        logging.info("Wrote OVF to file '{}'".format(os.path.abspath(ovf_file)))

    @staticmethod
    def _get_system_service():
        config = configparser.ConfigParser()
        config.read('/root/.ovirtshellrc')

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

            logging.info('Attaching disk {}'.format(attachment.disk.id))

            with AutoAttachmentService(attachments_service, attachment):
                    self._copy_disk(attachment, backup_vm_date_dir)

    @classmethod
    def _copy_disk(cls, attachment, directory):
        input_file = cls._find_data_disk(attachment)
        output_file = os.path.join(directory, attachment.disk.id)

        cmd_args = ['dd', 'if={}'.format(input_file), 'of={}'.format(output_file)]
        logging.info('Executing command: {}'.format(subprocess.list2cmdline(cmd_args)))

        dd_process = subprocess.Popen(cmd_args, stderr=subprocess.PIPE)
        _, err = dd_process.communicate()

        for line in err.splitlines():
            logging.info('Command output: {}'.format(line.decode().rstrip()))

        if dd_process.returncode != 0:
            raise BackupError('Copy failed! (exit code {})'.format(dd_process.returncode))

    @staticmethod
    def _find_data_disk(attachment):
        for path in glob.glob('/sys/block/*/serial'):
            with open(path, 'r') as file:
                serial = file.read()
            if serial == attachment.disk.id[:20]:
                # path.split('/') == ('', 'sys', 'block', disk, 'serial')
                disk = path.split('/')[3]
                return '/dev/{}'.format(disk)
        else:
            raise BackupError('Cannot find any usable disk for attachment id {}'.format(attachment.disk.id))


# TODO: create a class to move data
# TODO: use argparse
# TODO: add events to ovirt console


def main():
    vm_name = sys.argv[1]
    try:
        b = Backup(vm_name)
        b.run()
    except BackupError as err:
        print("Backup of the virtual machine '{}' failed. "
              "See '/var/log/ovirt_backup.log' for details.".format(vm_name),
              file=sys.stderr)
        print("Error: {}".format(err), file=sys.stderr)


if __name__ == '__main__':
    main()
