# Python Ovirt backup script

A script to backup Ovirt virtual machines.

# Requirements
* python 3 (though python2 might keep working for a while)
* ovirtsdk4 python library (python-ovirt-engine-sdk4 on Centos, other distributions may use different package names)

# Setup
* Set up a VM to do the backups, give the VM a path to backup to (external storage??)
* Clone this repository
* Open the backupvm.py file, change some settings:
* `_base_backup_dir` has the storage location for the backups
* `_agent_vm_name` is the name of the VM doing the backup
* `_num_backups` is the amount of backups to stora (for rotation)
* Set up `/root/.ovirtshellrc`, an example is at `example.ovirtshellrc` ( https://access.redhat.com/documentation/en-us/red_hat_virtualization/4.0/html/rhevm_shell_guide/ovirtshellrc_configuration ), at least the url, username and password (under ovirt-shell)


