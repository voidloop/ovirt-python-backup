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
    format='%(asctime)s %(levelname)s:%(message)s',
    datefmt='%Y/%m/%d %H:%M:%S',
    level=logging.INFO,
    filename='/var/log/ovirt-backup.log'
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
        logging.info("Waiting till the snapshot is created, the status is now '%s'.", self._snapshot.snapshot_status)
        while self._snapshot_service.get().snapshot_status != types.SnapshotStatus.OK:
            time.sleep(1)
        self._creation_time = datetime.now()
        return self._snapshot_service

    @delayed(seconds=30)
    def __exit__(self, *args, **kwargs):
        logging.info("Sending request to remove snapshot '%s'.", self._snapshot.description)
        self._snapshot_service.remove(wait=True)


class AutoAttachmentService:
    def __init__(self, attachments_service, attachment):
        self._attachments_service = attachments_service
        self._attachment = attachment

    @delayed(seconds=60)
    def __enter__(self):
        self._attachment_service = self._attachments_service.attachment_service(self._attachment.id)

    @delayed(seconds=30)
    def __exit__(self, *args, **kwargs):
        logging.info("Detaching disk '%s'", self._attachment.disk.id)
        self._attachment_service.remove(wait=True)


class Backup:
    _base_backup_dir = '/mnt/ovirt-backup'
    _application_name = 'ovirt-backup'
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

        logging.info("Sent request to create snapshot '%s' (%s).", snapshot.description, snapshot.id)

        with AutoSnapshotService(snapshots_service, snapshot) as snapshot_service:
            self._migrate_agent_vm()
            self._backup_snapshot_disks(snapshot_service, backup_vm_date_dir)
            self._remove_old_backups(backup_vm_dir)

    def _remove_old_backups(self, backup_vm_dir):
        dir_list = sorted(glob.glob(backup_vm_dir + "/*"), key=str.lower)
        while len(dir_list) > self._num_backups:
            logging.info('Rotating backup directories (oldest directory will be removed)')
            # oldest backup is the top of the list
            cmd = 'rm -rf ' + dir_list.pop(0)
            logging.info("Executing command: '%s'", cmd)
            os.system(cmd)

    def _migrate_agent_vm(self):
        agent_vm = self._agent_vm_service.get()
        data_vm = self._data_vm_service.get()

        if agent_vm.host.id != data_vm.host.id:
            logging.info("Migrating VM '%s' from '%s' to '%s'", agent_vm.name, agent_vm.host.id, data_vm.host.id)
            self._agent_vm_service.migrate(cluster=data_vm.cluster, host=data_vm.host, wait=True)
            while self._agent_vm_service.get().status == types.VmStatus.MIGRATING:
                time.sleep(10)

    @staticmethod
    def _save_ovf(data_vm, backup_dir):
        # Save the OVF to a file, so that we can use to restore the virtual
        # machine later. The name of the file is the name of the virtual
        # machine, followed by a dash and the identifier of the virtual machine,
        # to make it unique:
        ovf_data = data_vm.initialization.configuration.data
        ovf_file = os.path.join(backup_dir, '{}-{}.ovf'.format(data_vm.name, data_vm.id))

        with open(ovf_file, 'w') as ovs_fd:
            ovs_fd.write(ovf_data)

        logging.info("Wrote OVF to file '%s'.", os.path.abspath(ovf_file))

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
        logging.info('Connected to the server.')
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

            logging.info("Attaching disk '%s'", attachment.disk.id)

            with AutoAttachmentService(attachments_service, attachment):
                    self._copy_disk(attachment, backup_vm_date_dir)

    @classmethod
    def _copy_disk(cls, attachment, directory):
        logging.info('Doing the actual backup ...')

        input_file = cls._find_data_device(attachment)
        output_file = os.path.join(directory, attachment.disk.id)

        cmd_args = ['dd', 'if={}'.format(input_file), 'of={}'.format(output_file)]
        logging.info("Executing command: '%s'", subprocess.list2cmdline(cmd_args))

        dd_process = subprocess.Popen(cmd_args, stderr=subprocess.PIPE)
        _, err = dd_process.communicate()

        for line in err.splitlines():
            logging.info('Command output: {}'.format(line.decode().rstrip()))

        if dd_process.returncode != 0:
            logging.error('dd command failed! (exit code {})'.format(dd_process.returncode))

    @staticmethod
    def _find_data_device(attachment):
        files = glob.glob('/dev/disk/by-id/*{}*'.format(attachment.disk.id[:20]))
        if len(files) == 0:
            raise BackupError('Cannot find any usable device for attachment id %s', attachment.disk.id)

        disk_by_id = files[0]
        lsblk = subprocess.Popen(['lsblk', '-sln', '-o', 'name', disk_by_id],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        out, err = lsblk.communicate()
        if lsblk.returncode != 0:
            raise BackupError(err.decode())

        lines = out.splitlines()
        dev = lines[-1].strip().decode()
        return '/dev/{}'.format(dev)


# TODO: create a class to move data
# TODO: use argparse


if __name__ == '__main__':
    vm = sys.argv[1]
    b = Backup(vm)
    b.run()
